package securefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseRefusesUnsafeFilesAndSidecars(t *testing.T) {
	for _, kind := range []string{"public", "symlink", "hardlink", "directory", "sidecar", "ancestor"} {
		t.Run(kind, func(t *testing.T) {
			dir, _ := filepath.EvalSymlinks(t.TempDir())
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "inbox.sqlite")
			if err := Database(path, true); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal(info, err)
			}
			switch kind {
			case "public":
				err = os.Chmod(path, 0644)
			case "symlink":
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(filepath.Join(dir, "elsewhere"), path)
			case "hardlink":
				err = os.Link(path, filepath.Join(dir, "linked"))
			case "directory":
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
				err = os.Mkdir(path, 0700)
			case "sidecar":
				err = os.Symlink(filepath.Join(dir, "elsewhere"), path+"-journal")
			case "ancestor":
				err = os.Chmod(dir, 0777)
			}
			if err != nil {
				t.Fatal(err)
			}
			if Database(path, true) == nil {
				t.Fatal("unsafe database accepted")
			}
		})
	}
}
