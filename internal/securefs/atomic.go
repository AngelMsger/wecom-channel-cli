package securefs

import (
	"os"
	"path/filepath"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"golang.org/x/sys/unix"
)

// LockPrivate returns an advisory, process-owned lock in a verified directory.
// Closing the returned file releases the lock.
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

// ProbeLock reports whether another process currently holds an exclusive flock
// on path. It never creates the lock file or alters ownership.
func ProbeLock(path string) (bool, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer unix.Close(fd)
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if err == unix.EWOULDBLOCK {
			return true, nil
		}
		return false, err
	}
	_ = unix.Flock(fd, unix.LOCK_UN)
	return false, nil
}

// WriteAtomic replaces a file atomically with 0600 permissions inside an already
// verified directory, refusing to overwrite existing symlinks or non-regular files.
func WriteAtomic(path string, b []byte) error {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Refusing to overwrite symlink or non-regular file.")
		}
	} else if !os.IsNotExist(err) {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not inspect destination file.")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".private-*")
	if err != nil {
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not atomically save private state.")
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return e.New(e.CategoryConfig, "PRIVATE_FILE_WRITE", "Could not secure private state file.")
	}
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

// EnsurePrivateDir ensures dir exists with 0700 permissions owned by the
// current identity and returns its canonical absolute path.
//
// In strict mode, every ancestor up to / is checked by Directory (for
// multi-user Linux host isolation). In default mode, existing symlinked
// ancestors (such as macOS /var -> /private/var or container mounts) are
// resolved while the leaf directory is still enforced to be a non-symlink 0700
// directory owned by the current user.
func EnsurePrivateDir(dir string, strict bool) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot resolve private directory path.")
	}
	if strict {
		parent := abs
		for {
			_, err := os.Lstat(parent)
			if err == nil {
				break
			}
			if !os.IsNotExist(err) {
				return "", err
			}
			parent = filepath.Dir(parent)
		}
		if err := Directory(parent, false, true); err != nil {
			return "", err
		}
		if err := os.MkdirAll(abs, 0700); err != nil {
			return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot create private directory.")
		}
		if err := Directory(abs, true, false); err != nil {
			return "", err
		}
		return abs, nil
	}

	// Never follow a symlinked leaf directory.
	leafExisted := false
	if info, err := os.Lstat(abs); err == nil {
		leafExisted = true
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Private directory must be a real directory, not a symlink.")
		}
	} else if !os.IsNotExist(err) {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot inspect directory path.")
	}

	// Resolve symlinks only on ancestors above the leaf so container/macOS
	// symlinked parents work without weakening leaf checks.
	parent := filepath.Dir(abs)
	missing := []string{filepath.Base(abs)}
	cur := parent
	for {
		if _, err := os.Lstat(cur); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot inspect directory path.")
		}
		missing = append(missing, filepath.Base(cur))
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	resolved, err := filepath.EvalSymlinks(cur)
	if err != nil {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot resolve parent directory.")
	}
	for i := len(missing) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, missing[i])
	}
	if !leafExisted {
		if err := os.MkdirAll(resolved, 0700); err != nil {
			return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Cannot create private directory.")
		}
		_ = os.Chmod(resolved, 0700)
	}
	info, err := os.Lstat(resolved)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Private directory must be a real directory, not a symlink.")
	}
	var st unix.Stat_t
	if unix.Lstat(resolved, &st) != nil || int(st.Uid) != os.Geteuid() {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Private directory must be owned by the current user.")
	}
	if info.Mode().Perm()&0077 != 0 {
		return "", e.New(e.CategoryConfig, "UNSAFE_PATH", "Private directory must not be accessible to group or others.")
	}
	return resolved, nil
}
