package app

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

type checkpointFile struct {
	path string
	lock *os.File
}

func checkpointError() error {
	return e.New(e.CategoryConfig, "WATCH_CHECKPOINT", "Use a protected checkpoint file in an existing agent-owned directory; unrelated files and symlinks are never overwritten.")
}
func openWatchCheckpoint(path string, lock bool) (*checkpointFile, error) {
	f := &checkpointFile{}
	if path == "" {
		return f, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, checkpointError()
	}
	f.path = absolute
	if err = securefs.Directory(filepath.Dir(absolute), false, false); err != nil {
		return nil, err
	}
	if lock {
		f.lock, err = securefs.LockPrivate(absolute + ".lock")
		if err != nil {
			if err == unix.EWOULDBLOCK {
				return nil, e.New(e.CategoryConflict, "WATCH_IN_USE", "Another watch owns this checkpoint.")
			}
			return nil, checkpointError()
		}
	}
	return f, nil
}
func (f *checkpointFile) close() {
	if f != nil && f.lock != nil {
		_ = f.lock.Close()
	}
}
func (f *checkpointFile) read() (watchCheckpoint, bool, error) {
	var cp watchCheckpoint
	if f.path == "" {
		return cp, false, nil
	}
	i, err := os.Lstat(f.path)
	if os.IsNotExist(err) {
		return cp, false, nil
	}
	if err != nil || !i.Mode().IsRegular() || i.Mode().Perm()&0077 != 0 {
		return cp, false, checkpointError()
	}
	var st unix.Stat_t
	if unix.Lstat(f.path, &st) != nil || int(st.Uid) != os.Geteuid() {
		return cp, false, checkpointError()
	}
	file, err := os.Open(f.path)
	if err != nil {
		return cp, false, checkpointError()
	}
	defer file.Close()
	b, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(b) > 16384 || safejson.Decode(b, &cp, true) != nil || cp.Version != 1 || len(cp.Binding) != 64 || len(cp.Scope) != 64 || cp.After < 0 {
		return cp, false, checkpointError()
	}
	return cp, true, nil
}
func (f *checkpointFile) write(cp watchCheckpoint) error {
	if f.path == "" {
		return nil
	}
	old, exists, err := f.read()
	if err != nil {
		return err
	}
	if exists && (old.Socket != cp.Socket || old.Scope != cp.Scope || old.Binding != cp.Binding || old.Identity != cp.Identity) {
		return e.New(e.CategoryConflict, "WATCH_CHECKPOINT_CHANGED", "The checkpoint was replaced with a different watch while running; it was not overwritten.")
	}
	if exists && old == cp {
		return nil
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return checkpointError()
	}
	if err := securefs.WriteAtomic(f.path, b); err != nil {
		return checkpointError()
	}
	return nil
}
