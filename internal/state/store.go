// Package state durably records callback deduplication and write outcomes.
package state

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

type Proposal struct {
	ID        string                 `json:"id"`
	Digest    string                 `json:"digest"`
	Policy    string                 `json:"policy"`
	Request   apiclient.WriteRequest `json:"request"`
	Plan      apiclient.Plan         `json:"plan"`
	Created   int64                  `json:"created_at"`
	Expires   int64                  `json:"expires_at"`
	Status    string                 `json:"status"`
	RemoteID  string                 `json:"remote_id,omitempty"`
	ErrorCode string                 `json:"error_code,omitempty"`
}

type Snapshot struct {
	ID                string                    `json:"ledger_id,omitempty"`
	Scope             string                    `json:"scope,omitempty"`
	NextEventSequence int64                     `json:"next_event_sequence"`
	Version           int                       `json:"version"`
	Bot               string                    `json:"bot_id"`
	Events            map[string]callback.Event `json:"events"`
	Proposals         map[string]Proposal       `json:"proposals"`
}

type Store struct {
	mu       sync.Mutex
	dir      string
	lock     *os.File
	snapshot Snapshot
}

func failure() *e.CLIError {
	return e.New(e.CategoryConfig, "STATE_IO", "Cannot safely read or persist broker state.").WithHint("Ask the operator to check storage permissions, capacity and backups; do not delete the ledger to retry a write.")
}

// CheckDir rejects symlinks, foreign-owned final directories and writable parents.
// Sticky system temporary directories are allowed for isolated test fixtures.
func CheckDir(path string, private bool) error {
	return securefs.Directory(path, private, false)
}

func Open(dir, bot string, scope ...string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, failure()
	}
	if err := CheckDir(dir, true); err != nil {
		return nil, err
	}
	fd, err := unix.Open(filepath.Join(dir, "broker.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, failure()
	}
	f := os.NewFile(uintptr(fd), "broker.lock")
	if unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		f.Close()
		return nil, e.New(e.CategoryConflict, "BROKER_RUNNING", "Another broker owns this state directory.")
	}
	s := &Store{dir: dir, lock: f, snapshot: Snapshot{Version: 1, Bot: bot, Events: map[string]callback.Event{}, Proposals: map[string]Proposal{}}}
	bound := ""
	if len(scope) > 0 {
		bound = scope[0]
	}
	s.snapshot.Scope = bound
	path := filepath.Join(dir, "ledger.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := s.ensureID(); err != nil {
			s.Close()
			return nil, err
		}
		return s, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		s.Close()
		return nil, failure()
	}
	f2, err := os.Open(path)
	if err != nil {
		s.Close()
		return nil, failure()
	}
	b, err := io.ReadAll(io.LimitReader(f2, 64<<20+1))
	f2.Close()
	s.snapshot = Snapshot{}
	if err != nil || len(b) > 64<<20 || safejson.Decode(b, &s.snapshot, true) != nil || s.snapshot.Version != 1 || s.snapshot.Bot != bot || s.snapshot.Events == nil || s.snapshot.Proposals == nil {
		s.Close()
		return nil, failure()
	}
	if s.snapshot.Scope != bound {
		s.Close()
		return nil, e.New(e.CategoryConflict, "STATE_SCOPE", "This ledger belongs to a different security context. Preserve it and select the correct directory.")
	}
	if err := s.ensureID(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.lock.Close() }

func clone(s Snapshot) Snapshot {
	b, _ := json.Marshal(s)
	var out Snapshot
	_ = json.Unmarshal(b, &out)
	return out
}

func (s *Store) View() Snapshot { s.mu.Lock(); defer s.mu.Unlock(); return clone(s.snapshot) }

// Update publishes in-memory changes only after the atomic on-disk commit.
func (s *Store) Update(fn func(*Snapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := clone(s.snapshot)
	if err := fn(&next); err != nil {
		return err
	}
	b, err := json.Marshal(next)
	if err != nil || len(b) > 64<<20 {
		return failure()
	}
	f, err := os.CreateTemp(s.dir, ".ledger-*")
	if err != nil {
		return failure()
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return failure()
	}
	if os.Rename(name, filepath.Join(s.dir, "ledger.json")) != nil {
		return failure()
	}
	// Rename has committed even if directory fsync fails. Keep that state in
	// memory so a caller never replays a potentially committed transition.
	s.snapshot = next
	d, err := os.Open(s.dir)
	if err != nil {
		return failure()
	}
	defer d.Close()
	if d.Sync() != nil {
		return failure()
	}
	return nil
}

func (s *Store) AddEvent(v callback.Event, now time.Time, retention time.Duration, max int) error {
	return s.Update(func(st *Snapshot) error {
		if old, ok := st.Events[v.ID]; ok {
			// A duplicate is acknowledged, but a changed identity under the same
			// message ID must not replace the original authenticated fact.
			if old.Channel != v.Channel || old.Post != v.Post || old.Reply != v.Reply || old.Sender != v.Sender || old.Kind != v.Kind {
				return e.New(e.CategoryConflict, "EVENT_COLLISION", "A callback ID conflicts with an earlier event.")
			}
			return nil
		}
		for id, ev := range st.Events {
			if ev.Received < now.Add(-retention).Unix() {
				delete(st.Events, id)
			}
		}
		if len(st.Events) >= max {
			return e.New(e.CategoryRateLimit, "INBOX_FULL", "The event inbox is full; ask the operator to inspect retention and capacity.")
		}
		st.NextEventSequence++
		v.Sequence = st.NextEventSequence
		st.Events[v.ID] = v
		return nil
	})
}

func Events(s Snapshot) []callback.Event {
	out := make([]callback.Event, 0, len(s.Events))
	for _, v := range s.Events {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence == out[j].Sequence {
			return out[i].ID < out[j].ID
		}
		return out[i].Sequence < out[j].Sequence
	})
	return out
}

func (s *Store) ensureID() error {
	if s.snapshot.ID != "" {
		if len(s.snapshot.ID) != 32 {
			return failure()
		}
		return nil
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return failure()
	}
	return s.Update(func(st *Snapshot) error { st.ID = hex.EncodeToString(bytes[:]); return nil })
}

// Inbox copies only immutable event values, without repeatedly cloning proposals.
func (s *Store) Inbox() (string, int64, []callback.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]callback.Event, 0, len(s.snapshot.Events))
	for _, v := range s.snapshot.Events {
		events = append(events, v)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
	return s.snapshot.ID, s.snapshot.NextEventSequence, events
}
