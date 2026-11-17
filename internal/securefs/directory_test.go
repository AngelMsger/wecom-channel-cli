package securefs

import (
	"os"
	"path/filepath"
	"testing"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
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
	assertPathFailure(t, Directory(child, true, false), parent, "group_or_world_writable")
	_ = os.Chmod(parent, 0700)
	alias := filepath.Join(dir, "alias")
	_ = os.Symlink(parent, alias)
	assertPathFailure(t, Directory(filepath.Join(alias, "private"), true, false), alias, "symlink")
}

func assertPathFailure(t *testing.T, err error, path, reason string) {
	t.Helper()
	if err == nil {
		t.Fatal("unsafe path accepted")
	}
	ce := e.AsCLIError(err)
	details, ok := ce.Payload().Error.Details.(map[string]string)
	if ce.Code != "UNSAFE_PATH" || ce.Category != e.CategoryConfig || !ok || details["path"] != path || details["reason"] != reason || ce.Hint == "" {
		t.Fatalf("unexpected diagnostic: %+v", ce.Payload())
	}
}

func TestDirectoryDiagnostics(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	assertPathFailure(t, Directory("relative", true, false), "relative", "path_not_absolute_or_clean")
	missing := filepath.Join(dir, "missing")
	assertPathFailure(t, Directory(missing, true, false), missing, "directory_missing")
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("synthetic-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	assertPathFailure(t, Directory(file, true, false), file, "not_directory")
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	assertPathFailure(t, Directory(dir, true, false), dir, "private_directory_permissions")
	if err := Directory(dir, false, false); err != nil {
		t.Fatal(err)
	}
}
