package securefs

import (
	"os"
	"path/filepath"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"golang.org/x/sys/unix"
)

// Database checks the private directory and SQLite's main/sidecar files before
// opening SQLite. Its service identity is trusted; other identities cannot swap
// files within the checked directory. Hard links and special files are refused.
func Database(path string, create bool) error {
	if err := Directory(filepath.Dir(path), true, false); err != nil {
		return err
	}
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
		if suffix == "" && create {
			flags = flags | unix.O_CREAT
		}
		fd, err := unix.Open(path+suffix, flags, 0600)
		if err != nil {
			if suffix != "" && os.IsNotExist(err) {
				continue
			}
			if os.IsNotExist(err) {
				return err
			}
			return e.New(e.CategoryConfig, "UNSAFE_DATABASE", "Use a private service-owned SQLite file and sidecars, without symlinks or hard links.")
		}
		var st unix.Stat_t
		bad := unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || int(st.Uid) != os.Geteuid() || st.Nlink != 1
		unix.Close(fd)
		if bad {
			return e.New(e.CategoryConfig, "UNSAFE_DATABASE", "Use a private service-owned SQLite file and sidecars, without symlinks or hard links.")
		}
	}
	return nil
}
