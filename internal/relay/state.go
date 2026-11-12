package relay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

const maxStateBytes = 16 << 20

// Record deliberately decodes into Event: unknown message bodies never persist.
type Record struct {
	Trust   string           `json:"trust"`
	Warning string           `json:"warning"`
	Source  apiclient.Source `json:"source"`
	Data    callback.Event   `json:"data"`
}

type Thread struct {
	Post   string   `json:"post_id"`
	Events []Record `json:"events"`
}

type Batch struct {
	Version  int                  `json:"version"`
	ID       string               `json:"id"`
	Identity broker.WatchIdentity `json:"identity"`
	Coverage broker.WatchCoverage `json:"coverage"`
	Threads  []Thread             `json:"threads"`
}

type Delivery struct {
	Batch       Batch  `json:"batch"`
	Attempts    int    `json:"attempts"`
	NextAttempt int64  `json:"next_attempt_at"`
	Status      string `json:"status,omitempty"`
	LastAttempt int64  `json:"last_attempt_at,omitempty"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	Evidence    string `json:"evidence,omitempty"`
	Failure     string `json:"terminal_error,omitempty"`
}

type Receipt struct {
	Batch      Batch  `json:"batch"`
	ID         string `json:"batch_id"`
	Status     string `json:"status"`
	Events     int    `json:"events"`
	Attempts   int    `json:"attempts"`
	AcceptedAt int64  `json:"accepted_at"`
	Evidence   string `json:"evidence,omitempty"`
}

type State struct {
	Version         int                  `json:"relay_version"`
	Consumer        string               `json:"consumer"`
	Identity        broker.WatchIdentity `json:"identity"`
	Binding         string               `json:"binding"`
	After           int64                `json:"collected_sequence"`
	FromNow         bool                 `json:"from_now,omitempty"`
	Coverage        broker.WatchCoverage `json:"coverage"`
	Pending         []Record             `json:"pending"`
	PendingSince    int64                `json:"pending_since"`
	Inflight        *Delivery            `json:"inflight,omitempty"`
	Collected       int64                `json:"collected_events"`
	Filtered        int64                `json:"filtered_events"`
	Acknowledged    int64                `json:"acknowledged_events"`
	AckedBatches    int64                `json:"acknowledged_batches"`
	LastScan        int64                `json:"last_scan_at"`
	LastAck         int64                `json:"last_ack_at"`
	ScanFailures    int                  `json:"scan_failures"`
	NextScan        int64                `json:"next_scan_at"`
	DeliveryMode    string               `json:"delivery_mode,omitempty"`
	AcceptedEvents  int64                `json:"accepted_events,omitempty"`
	AcceptedBatches int64                `json:"accepted_batches,omitempty"`
	LastAccepted    int64                `json:"last_accepted_at,omitempty"`
	Receipts        []Receipt            `json:"receipts,omitempty"`
	NextDelivery    int64                `json:"next_delivery_at,omitempty"`
}

func (s State) Depth() int {
	n := len(s.Pending)
	if s.Inflight != nil {
		for _, t := range s.Inflight.Batch.Threads {
			n += len(t.Events)
		}
	}
	return n
}

func (s *State) Collect(p broker.ScanPage, c Config, now time.Time) error {
	// Decode the entire page before changing progress. The CLI validates binding,
	// sequence and trust metadata before calling this method.
	var accepted []Record
	filtered := int64(0)
	for _, raw := range p.Items {
		b, err := json.Marshal(raw)
		var r Record
		if err != nil || safejson.Decode(b, &r, true) != nil || r.Data.Feedback != "" {
			return stateError()
		}
		if c.Accept(r.Data) {
			accepted = append(accepted, r)
		} else {
			filtered++
		}
	}
	if s.Depth()+len(accepted) > c.QueueLimit {
		return e.New(e.CategoryRateLimit, "RELAY_QUEUE_FULL", "The relay queue is full; restore the receiver before collecting more events. No cursor advanced.")
	}
	if len(s.Pending) == 0 && len(accepted) > 0 {
		s.PendingSince = now.UnixMilli()
	}
	s.Pending = append(s.Pending, accepted...)
	s.Identity, s.Binding, s.After, s.Coverage = p.Identity, p.Binding, p.After, p.Coverage
	s.FromNow = false
	s.Collected += int64(len(p.Items))
	s.Filtered += filtered
	s.LastScan = now.UnixMilli()
	s.ScanFailures, s.NextScan = 0, 0
	return nil
}

// Seal freezes the payload before its first attempt. New observations never alter
// a batch whose acknowledgment might have been lost.
func (s *State) Seal(c Config, now time.Time) bool {
	if s.Inflight != nil || len(s.Pending) == 0 || (len(s.Pending) < c.BatchSize && now.UnixMilli() < s.PendingSince+int64(c.BatchWindowSeconds)*1000) {
		return false
	}
	n := min(len(s.Pending), c.BatchSize)
	b := Batch{Version: 1, Identity: s.Identity, Coverage: s.Coverage}
	indices := map[string]int{}
	for _, r := range s.Pending[:n] {
		i, ok := indices[r.Data.Post]
		if !ok {
			i = len(b.Threads)
			indices[r.Data.Post] = i
			b.Threads = append(b.Threads, Thread{Post: r.Data.Post})
		}
		b.Threads[i].Events = append(b.Threads[i].Events, r)
	}
	b.ID = batchID(s.Consumer, b)
	s.Inflight = &Delivery{Batch: b}
	s.Pending = append([]Record(nil), s.Pending[n:]...)
	if len(s.Pending) == 0 {
		s.PendingSince = 0
	}
	return true
}

func batchID(consumer string, b Batch) string {
	b.ID = ""
	return apiclient.Digest(struct {
		Consumer string
		Batch    Batch
	}{consumer, b})
}

func (s *State) Ack(now time.Time) {
	for _, t := range s.Inflight.Batch.Threads {
		s.Acknowledged += int64(len(t.Events))
	}
	s.AckedBatches++
	s.LastAck = now.UnixMilli()
	s.Inflight = nil
}

// Accept records a documented HTTP acceptance, never processing completion.
func (s *State) Accept(now time.Time, evidence string) error {
	if s.Inflight == nil || len(s.Receipts) >= 10000 {
		return e.New(e.CategoryRateLimit, "RELAY_RECEIPTS_FULL", "The bounded delivery receipt history is full. Stop and archive/reconcile it before provisioning a new relay; no pending batch was discarded.")
	}
	n := 0
	for _, t := range s.Inflight.Batch.Threads {
		n += len(t.Events)
	}
	s.Receipts = append(s.Receipts, Receipt{Batch: s.Inflight.Batch, ID: s.Inflight.Batch.ID, Status: "accepted", Events: n, Attempts: s.Inflight.Attempts, AcceptedAt: now.UnixMilli(), Evidence: evidence})
	s.AcceptedEvents += int64(n)
	s.AcceptedBatches++
	s.LastAccepted = now.UnixMilli()
	s.Inflight = nil
	return nil
}

type Store struct {
	Path   string
	lock   *os.File
	digest string
}

func stateError() error {
	return e.New(e.CategoryConfig, "RELAY_STATE", "Relay state is invalid, unsafe or could not be persisted. Preserve it and restore its original configuration; do not delete it to bypass a delivery conflict.")
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, stateError()
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, stateError()
	}
	if err = securefs.Directory(filepath.Dir(abs), true, false); err != nil {
		return nil, err
	}
	fd, err := unix.Open(abs+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, stateError()
	}
	f := os.NewFile(uintptr(fd), "relay.lock")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || int(st.Uid) != os.Geteuid() || st.Mode&0077 != 0 {
		f.Close()
		return nil, stateError()
	}
	if unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		f.Close()
		return nil, e.New(e.CategoryConflict, "RELAY_IN_USE", "Another relay owns this state file.")
	}
	s := &Store{Path: abs, lock: f}
	b, err := securefs.ReadPrivate(abs, maxStateBytes, false)
	if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, stateError()
	}
	if err == nil {
		s.digest = apiclient.Digest(string(b))
	}
	return s, nil
}

func (s *Store) Close() {
	if s != nil && s.lock != nil {
		_ = s.lock.Close()
	}
}

func Read(path string) (State, bool, error) {
	var s State
	abs, err := filepath.Abs(path)
	if err != nil || path == "" {
		return s, false, stateError()
	}
	if err = securefs.Directory(filepath.Dir(abs), true, false); err != nil {
		return s, false, err
	}
	b, err := securefs.ReadPrivate(abs, maxStateBytes, false)
	if os.IsNotExist(err) {
		return s, false, nil
	}
	if err != nil || safejson.Decode(b, &s, true) != nil || s.Validate() != nil {
		return s, false, stateError()
	}
	return s, true, nil
}

func (s State) Validate() error {
	if s.Version != 1 || len(s.Consumer) != 64 || s.After < 0 || s.Depth() > 10000 || s.ScanFailures < 0 || s.ScanFailures > 21 || s.Filtered < 0 || s.Acknowledged < 0 || s.Collected != s.Filtered+s.Acknowledged+s.AcceptedEvents+int64(s.Depth()) || s.AckedBatches < 0 || (len(s.Pending) > 0 && s.PendingSince <= 0) {
		return stateError()
	}
	if s.DeliveryMode != "" && s.DeliveryMode != HTTPAccepted {
		return stateError()
	}
	if (s.DeliveryMode == "" && (s.AcceptedEvents != 0 || s.AcceptedBatches != 0 || s.LastAccepted != 0 || len(s.Receipts) != 0)) || len(s.Receipts) > 10000 || s.AcceptedBatches != int64(len(s.Receipts)) || s.AcceptedEvents < 0 || s.NextDelivery < 0 {
		return stateError()
	}
	accepted := int64(0)
	receipts := map[string]bool{}
	for _, r := range s.Receipts {
		if r.Batch.ID != r.ID || r.Batch.Identity != s.Identity || r.Batch.Validate() != nil || r.Batch.ID != batchID(s.Consumer, r.Batch) || len(r.ID) != 64 || receipts[r.ID] || r.Status != "accepted" || r.Events < 1 || r.Events > 100 || r.Attempts < 1 || r.Attempts > 21 || r.AcceptedAt <= 0 {
			return stateError()
		}
		count := 0
		for _, thread := range r.Batch.Threads {
			count += len(thread.Events)
		}
		if count != r.Events {
			return stateError()
		}
		receipts[r.ID] = true
		accepted += int64(r.Events)
	}
	if accepted != s.AcceptedEvents {
		return stateError()
	}
	// Persist startup retry budgets before the first successful broker handshake.
	// No work may be dispatched until a complete identity/binding is established.
	if s.Binding == "" {
		if s.Identity != (broker.WatchIdentity{}) || s.Depth() != 0 || s.Inflight != nil || s.Collected != 0 || s.AckedBatches != 0 || s.AcceptedBatches != 0 || (s.FromNow && s.After != 0) {
			return stateError()
		}
		return nil
	}
	if s.FromNow || len(s.Binding) != 64 || len(s.Identity.Ledger) != 32 || !config.ValidContextName(s.Identity.Context) || !apiclient.Identifier(s.Identity.Bot, 256) || !apiclient.Identifier(s.Identity.Channel, 128) {
		return stateError()
	}
	seen := map[int64]bool{}
	check := func(r Record) bool {
		v := r.Data
		if v.Sequence < 1 || v.Sequence > s.After || seen[v.Sequence] || v.Bot != s.Identity.Bot || v.Channel != s.Identity.Channel || !v.ObservationOnly || (v.Kind != "topic_event" && v.Kind != "topic_reply_event") || !apiclient.Identifier(v.ID, 256) || !apiclient.Identifier(v.Post, 128) || v.Feedback != "" || r.Trust != "untrusted" || r.Warning != apiclient.Warning || r.Source.Context != s.Identity.Context || r.Source.Channel != v.Channel || r.Source.Service != "wecom" {
			return false
		}
		seen[v.Sequence] = true
		return true
	}
	for _, r := range s.Pending {
		if !check(r) {
			return stateError()
		}
	}
	if s.Inflight != nil {
		d := s.Inflight
		if receipts[d.Batch.ID] || (s.DeliveryMode == "" && d.Status != "") || (d.Status != "" && d.Status != "sending" && d.Status != "unknown" && d.Status != "rejected") || (d.Status != "" && d.Attempts < 1) {
			return stateError()
		}
		if d.Attempts < 0 || d.Attempts > 21 || d.Batch.Version != 1 || d.Batch.Identity != s.Identity || len(d.Batch.Threads) == 0 || d.Batch.ID != batchID(s.Consumer, d.Batch) {
			return stateError()
		}
		for _, t := range d.Batch.Threads {
			if len(t.Events) == 0 {
				return stateError()
			}
			for _, r := range t.Events {
				if r.Data.Post != t.Post || !check(r) {
					return stateError()
				}
			}
		}
	}
	return nil
}

func (s *Store) Save(v State) error {
	if v.Validate() != nil {
		return stateError()
	}
	return s.save(v)
}

// save shares the existing atomic snapshot protocol with worker-owned receipts.
// Callers validate their own schema before entering this persistence boundary.
func (s *Store) save(v any) error {
	old, err := securefs.ReadPrivate(s.Path, maxStateBytes, false)
	if (err != nil && !os.IsNotExist(err)) || (s.digest != "" && (err != nil || apiclient.Digest(string(old)) != s.digest)) || (s.digest == "" && err == nil) {
		return stateError()
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > maxStateBytes {
		return stateError()
	}
	f, err := os.CreateTemp(filepath.Dir(s.Path), ".relay-*")
	if err != nil {
		return stateError()
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil || os.Rename(f.Name(), s.Path) != nil {
		return stateError()
	}
	// Rename has committed even if directory fsync subsequently fails. The caller
	// stops on any persistence error before making another network request.
	s.digest = apiclient.Digest(string(b))
	dir, err := os.Open(filepath.Dir(s.Path))
	if err != nil {
		return stateError()
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return stateError()
	}
	return nil
}
