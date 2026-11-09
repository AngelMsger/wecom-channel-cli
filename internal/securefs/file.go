package securefs

import (
	"io"
	"os"
	"path/filepath"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"golang.org/x/sys/unix"
)

// ReadPrivate reads a bounded regular file without following a final symlink.
// Root-owned configuration is allowed only when requested by the caller.
func ReadPrivate(path string, limit int64, allowRoot bool) ([]byte, error) {
	if err := Directory(filepath.Dir(path), false, allowRoot); err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "private-state")
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || (int(st.Uid) != os.Geteuid() && !(allowRoot && st.Uid == 0)) {
		return nil, e.New(e.CategoryConfig, "UNSAFE_FILE", "Use a private regular file owned by this service or its operator.")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		return nil, e.New(e.CategoryConfig, "FILE_LIMIT", "The private file could not be read within its size limit.")
	}
	return b, nil
}
