package securefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAncestorChecks(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	parent := filepath.Join(dir, "parent")
	child := filepath.Join(parent, "private")
	if err := os.MkdirAll(child, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Directory(child, true, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0777); err != nil {
		t.Fatal(err)
	}
	if Directory(child, true, false) == nil {
		t.Fatal("writable ancestor accepted")
	}
	_ = os.Chmod(parent, 0700)
	alias := filepath.Join(dir, "alias")
	_ = os.Symlink(parent, alias)
	if Directory(filepath.Join(alias, "private"), true, false) == nil {
		t.Fatal("symlink ancestor accepted")
	}
}
