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
	bad := func(p, reason string) error {
		err := e.Newf(e.CategoryConfig, "UNSAFE_PATH", "Directory check failed at %q: %s.", p, reason)
		err.Details = map[string]string{"path": p, "reason": reason}
		return err.WithHint("Inspect this path and its ancestors as the service identity; repair deployment ownership or permissions, or use an operator-provisioned directory. Do not bypass checks or chmod shared parents blindly.")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return bad(path, "path_not_absolute_or_clean")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				return bad(p, "directory_missing")
			}
			return bad(p, "directory_inspection_failed")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return bad(p, "symlink")
		}
		if !info.IsDir() {
			return bad(p, "not_directory")
		}
		var st unix.Stat_t
		if unix.Lstat(p, &st) != nil {
			return bad(p, "directory_inspection_failed")
		}
		own := int(st.Uid) == os.Geteuid()
		root := st.Uid == 0
		if !own && !root {
			return bad(p, "untrusted_owner")
		}
		if info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0 {
			return bad(p, "group_or_world_writable")
		}
		if p == path {
			if !own && !(allowRoot && root) {
				return bad(p, "leaf_owner_mismatch")
			}
			if private && info.Mode().Perm()&0077 != 0 {
				return bad(p, "private_directory_permissions")
			}
		}
		if p == "/" {
			return nil
		}
	}
}
