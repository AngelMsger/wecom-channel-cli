package monitor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

const (
	maxProbeStateBytes = 64 << 10
	maxEventBytes      = 64 << 10
	MaxOutboxEvents    = 1000
)

// ProbeStateStore holds an exclusive advisory lock on one probe state file.
type ProbeStateStore struct {
	Path string
	lock *os.File
}

// OpenProbeState verifies the private parent directory, acquires an exclusive
// non-blocking lock on <path>.lock, and returns the store plus any existing state.
func OpenProbeState(path string) (*ProbeStateStore, ProbeState, bool, error) {
	if path == "" {
		return nil, ProbeState{}, false, e.New(e.CategoryUsage, "BAD_PROBE_STATE", "Alert state path is required.")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, ProbeState{}, false, e.New(e.CategoryConfig, "PROBE_STATE", "Invalid alert state path.")
	}
	if err := securefs.Directory(filepath.Dir(abs), true, false); err != nil {
		return nil, ProbeState{}, false, err
	}
	lockFile, err := securefs.LockPrivate(abs + ".lock")
	if err != nil {
		return nil, ProbeState{}, false, e.New(e.CategoryInternal, "PROBE_STATE_LOCKED", "Alert state is locked or inaccessible.")
	}
	store := &ProbeStateStore{Path: abs, lock: lockFile}
	raw, err := securefs.ReadPrivateFile(abs, maxProbeStateBytes, false, true)
	if err != nil {
		if os.IsNotExist(err) {
			return store, ProbeState{}, false, nil
		}
		store.Close()
		return nil, ProbeState{}, false, err
	}
	var st ProbeState
	if err := safejson.Decode(raw, &st, true); err != nil || !st.Validate() {
		store.Close()
		return nil, ProbeState{}, false, e.New(e.CategoryConfig, "PROBE_STATE_INVALID", "Alert state file is invalid or corrupted.")
	}
	return store, st, true, nil
}

// Save atomically persists ProbeState with 0600 permissions.
func (s *ProbeStateStore) Save(st ProbeState) error {
	if !st.Validate() {
		return e.New(e.CategoryInternal, "PROBE_STATE_INVALID", "Refusing to save invalid alert state.")
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return e.New(e.CategoryInternal, "PROBE_STATE", "Could not serialize alert state.")
	}
	raw = append(raw, '\n')
	return securefs.WriteAtomic(s.Path, raw)
}

// Close releases the probe state lock.
func (s *ProbeStateStore) Close() {
	if s != nil && s.lock != nil {
		_ = s.lock.Close()
		s.lock = nil
	}
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

func eventFileName(ev NotificationEvent) string {
	return fmt.Sprintf("%020d-%s.json", ev.Sequence, ev.Transition)
}

func listOutboxFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		files = append(files, name)
	}
	sort.Strings(files)
	return files, nil
}

// EnqueueOutbox durably writes one immutable edge event file into dir so slow
// or stopped notifiers still observe every alert and recovery in FIFO order.
func EnqueueOutbox(dir string, ev NotificationEvent) (string, error) {
	if !ev.Validate() {
		return "", e.New(e.CategoryInternal, "OUTBOX_EVENT_INVALID", "Refusing to enqueue invalid alert event.")
	}
	if dir == "" {
		return "", e.New(e.CategoryUsage, "BAD_OUTBOX", "Outbox directory is required.")
	}
	resolved, err := securefs.EnsurePrivateDir(dir, true)
	if err != nil {
		return "", err
	}
	existing, err := listOutboxFiles(resolved)
	if err != nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_READ", "Could not inspect alert outbox.")
	}
	if len(existing) >= MaxOutboxEvents {
		return "", e.New(e.CategoryRateLimit, "OUTBOX_FULL", "The alert outbox reached its capacity limit; repair the notification unit before new events can queue.")
	}
	target := filepath.Join(resolved, eventFileName(ev))
	if _, err := os.Lstat(target); err == nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_EXISTS", "Refusing to overwrite an undelivered alert event.")
	} else if !os.IsNotExist(err) {
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not inspect target outbox event path.")
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not serialize alert event.")
	}
	raw = append(raw, '\n')
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not allocate temporary alert event file.")
	}
	tmp := filepath.Join(resolved, ".tmp-"+hex.EncodeToString(nonce))
	fd, err := unix.Open(tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not create temporary alert event file.")
	}
	f := os.NewFile(uintptr(fd), tmp)
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not write alert event file.")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not sync alert event file.")
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not close alert event file.")
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not commit alert event into outbox.")
	}
	if err := syncDir(resolved); err != nil {
		return "", e.New(e.CategoryInternal, "OUTBOX_WRITE", "Could not sync alert outbox directory.")
	}
	return target, nil
}

// DrainResult summarizes one FIFO drain pass of the alert outbox.
type DrainResult struct {
	Delivered int                 `json:"delivered"`
	Remaining int                 `json:"remaining"`
	Events    []NotificationEvent `json:"events"`
}

// DrainOutbox delivers queued event files in strict sequence order and removes
// each file only after deliver returns nil. On the first delivery failure it
// stops immediately without removing the failed or subsequent events.
//
// The lock file must live outside dir so systemd DirectoryNotEmpty= remains
// accurate when the outbox drains to zero events.
func DrainOutbox(ctx context.Context, dir, lockPath string, maxEvents int, deliver func(context.Context, NotificationEvent, []byte) error) (DrainResult, error) {
	result := DrainResult{Events: []NotificationEvent{}}
	if dir == "" {
		return result, e.New(e.CategoryUsage, "BAD_OUTBOX", "Outbox directory is required.")
	}
	if maxEvents <= 0 || maxEvents > MaxOutboxEvents {
		return result, e.New(e.CategoryUsage, "BAD_LIMIT", "max-events must be between 1 and 1000.")
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return result, e.New(e.CategoryConfig, "UNSAFE_PATH", "Invalid outbox directory path.")
	}
	if lockPath == "" {
		lockPath = filepath.Join(filepath.Dir(absDir), ".notify.lock")
	}
	absLock, err := filepath.Abs(lockPath)
	if err != nil {
		return result, e.New(e.CategoryConfig, "UNSAFE_PATH", "Invalid notify lock path.")
	}
	if absLock == absDir || strings.HasPrefix(absLock, absDir+string(filepath.Separator)) {
		return result, e.New(e.CategoryUsage, "BAD_LOCK_PATH", "Notify lock file must live outside the outbox directory so DirectoryNotEmpty stays accurate.")
	}
	if err := securefs.Directory(filepath.Dir(absLock), true, false); err != nil {
		return result, err
	}
	lockFile, err := securefs.LockPrivate(absLock)
	if err != nil {
		return result, e.New(e.CategoryInternal, "NOTIFY_LOCKED", "Another notification drain is already running or the lock file is inaccessible.")
	}
	defer func() { _ = lockFile.Close() }()

	if _, err := os.Lstat(absDir); os.IsNotExist(err) {
		if err := securefs.Directory(filepath.Dir(absDir), true, false); err != nil {
			return result, err
		}
		return result, nil
	}
	if err := securefs.Directory(absDir, true, false); err != nil {
		return result, err
	}
	files, err := listOutboxFiles(absDir)
	if err != nil {
		return result, e.New(e.CategoryInternal, "OUTBOX_READ", "Could not list alert outbox.")
	}
	result.Remaining = len(files)
	limit := min(len(files), maxEvents)
	for i := 0; i < limit; i++ {
		if ctx.Err() != nil {
			return result, e.New(e.CategoryInternal, "NOTIFY_CANCELLED", "Notification drain was interrupted before finishing.")
		}
		entryPath := filepath.Join(absDir, files[i])
		raw, err := securefs.ReadPrivateFile(entryPath, maxEventBytes, false, true)
		if err != nil {
			return result, err
		}
		var ev NotificationEvent
		if err := safejson.Decode(raw, &ev, true); err != nil || !ev.Validate() {
			return result, e.New(e.CategoryParse, "OUTBOX_EVENT_INVALID", "Queued alert event file is invalid.")
		}
		if err := deliver(ctx, ev, raw); err != nil {
			return result, err
		}
		if err := os.Remove(entryPath); err != nil {
			return result, e.New(e.CategoryInternal, "OUTBOX_REMOVE", "Delivered event could not be removed from outbox.")
		}
		if err := syncDir(absDir); err != nil {
			return result, e.New(e.CategoryInternal, "OUTBOX_REMOVE", "Could not sync alert outbox after delivery.")
		}
		result.Delivered++
		result.Remaining--
		result.Events = append(result.Events, ev)
	}
	return result, nil
}

// ExecDeliverer returns a deliver function that pipes each sanitized JSON event
// to a fixed absolute notifier executable with a bounded per-event timeout.
// WeCom bot and socket variables are stripped while the notification unit's own
// alert-transport environment (for example from its systemd EnvironmentFile) is
// preserved.
func ExecDeliverer(execPath string, args []string, timeout time.Duration) (func(context.Context, NotificationEvent, []byte) error, error) {
	if !filepath.IsAbs(execPath) {
		return nil, e.New(e.CategoryUsage, "BAD_EXEC", "--exec must be an absolute executable path.")
	}
	if timeout <= 0 || timeout > 2*time.Minute {
		return nil, e.New(e.CategoryUsage, "BAD_TIMEOUT", "--exec-timeout must be greater than zero and at most two minutes.")
	}
	env := filteredNotifyEnv(os.Environ())
	return func(ctx context.Context, _ NotificationEvent, raw []byte) error {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		cmd := exec.CommandContext(childCtx, execPath, args...)
		cmd.Env = env
		payload := raw
		if len(payload) == 0 || payload[len(payload)-1] != '\n' {
			payload = append(append([]byte(nil), payload...), '\n')
		}
		cmd.Stdin = bytes.NewReader(payload)
		if err := cmd.Run(); err != nil {
			if childCtx.Err() == context.DeadlineExceeded {
				return e.New(e.CategoryNetwork, "NOTIFY_TIMEOUT", "Notification executable timed out before acknowledging delivery.")
			}
			return e.New(e.CategoryInternal, "NOTIFY_FAILED", "Notification executable exited unsuccessfully; queued event was retained.")
		}
		return nil
	}, nil
}

func filteredNotifyEnv(environ []string) []string {
	var out []string
	hasPath, hasLang := false, false
	for _, entry := range environ {
		k, _, _ := strings.Cut(entry, "=")
		switch k {
		case "WECOM_CHANNEL_BOT_SECRET", "WECOM_CHANNEL_BOT_ID", "WECOM_CHANNEL_SOCKET", "WECOM_CHANNEL_CONTEXT", "WECOM_CHANNEL_CONFIG":
			continue
		case "PATH":
			hasPath = true
		case "LANG":
			hasLang = true
		}
		out = append(out, entry)
	}
	if !hasPath {
		out = append(out, "PATH=/usr/bin:/bin")
	}
	if !hasLang {
		out = append(out, "LANG=C")
	}
	return out
}
