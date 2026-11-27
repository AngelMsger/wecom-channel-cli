package securefs

import (
	"os"
	"path/filepath"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"golang.org/x/sys/unix"
)

// LockPrivate returns an advisory, process-owned lock in a previously
// verified user directory. Closing the file releases the lock.
func LockPrivate(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "private-lock")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || int(st.Uid) != os.Geteuid() || st.Mode&0077 != 0 {
		f.Close()
		return nil, os.ErrPermission
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// WriteAtomic replaces a file in an already verified private directory.
func WriteAtomic(path string, b []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".private-*")
	if err != nil {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not atomically save private state.")
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(b); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil || closeErr != nil {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not atomically save private state.")
	}
	if os.Rename(name, path) != nil {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not atomically save private state.")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not atomically save private state.")
	}
	defer dir.Close()
	return dir.Sync()
}
