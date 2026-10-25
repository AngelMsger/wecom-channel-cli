// Package securefs validates the broker's OS-controlled filesystem boundary.
package securefs

import (
	"os"
	"path/filepath"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"golang.org/x/sys/unix"
)

// Directory checks every ancestor, not just the target's permission bits.
// Trusted owners are root and the current service identity. Sticky system temp
// directories are permitted for test deployments; a foreign-owned child is not.
func Directory(path string, private, allowRoot bool) error {
	bad := func() error {
		return e.New(e.CategoryConfig, "UNSAFE_PATH", "The directory or an ancestor is symlinked, writable by others, or owned by an untrusted identity.")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return bad()
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return bad()
		}
		var st unix.Stat_t
		if unix.Lstat(p, &st) != nil {
			return bad()
		}
		own := int(st.Uid) == os.Geteuid()
		root := st.Uid == 0
		if !own && !root {
			return bad()
		}
		if info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0 {
			return bad()
		}
		if p == path {
			if !own && !(allowRoot && root) {
				return bad()
			}
			if private && info.Mode().Perm()&0077 != 0 {
				return bad()
			}
		}
		if p == "/" {
			return nil
		}
	}
}
