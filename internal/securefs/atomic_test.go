package securefs

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestEnsurePrivateDirRelaxedAndStrict(t *testing.T) {
	realRoot := t.TempDir()
	if err := os.Chmod(realRoot, 0700); err != nil {
		t.Fatal(err)
	}
	linkParent := filepath.Join(t.TempDir(), "symroot")
	if err := os.Symlink(realRoot, linkParent); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(linkParent, "nested", "data")

	// Strict mode must reject the symlinked ancestor.
	if _, err := EnsurePrivateDir(target, true); err == nil {
		t.Fatal("expected strict mode to reject symlinked ancestor")
	}

	// Relaxed mode resolves the symlinked ancestor and creates a 0700 leaf.
	resolved, err := EnsurePrivateDir(target, false)
	if err != nil {
		t.Fatalf("relaxed EnsurePrivateDir failed: %v", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("expected 0700 directory, got %v", info.Mode())
	}

	// Even relaxed mode must reject a symlink leaf and group/world permissions.
	symLeaf := filepath.Join(realRoot, "leaf-link")
	if err := os.Symlink(resolved, symLeaf); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsurePrivateDir(symLeaf, false); err == nil {
		t.Fatal("expected relaxed mode to reject symlinked leaf")
	}
	wideDir := filepath.Join(realRoot, "wide")
	if err := os.Mkdir(wideDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(wideDir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsurePrivateDir(wideDir, false); err == nil {
		t.Fatal("expected relaxed mode to reject group/world accessible leaf")
	}
}

func TestLockPrivateProbeLockAndWriteAtomic(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(dir, "serve.lock")

	held, err := ProbeLock(lockPath)
	if err != nil || held {
		t.Fatalf("expected absent lock to probe false, got held=%v err=%v", held, err)
	}

	f, err := LockPrivate(lockPath)
	if err != nil {
		t.Fatalf("LockPrivate failed: %v", err)
	}

	if _, err := LockPrivate(lockPath); err != unix.EWOULDBLOCK {
		t.Fatalf("expected second LockPrivate to return EWOULDBLOCK, got %v", err)
	}
	held, err = ProbeLock(lockPath)
	if err != nil || !held {
		t.Fatalf("expected active lock to probe true, got held=%v err=%v", held, err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	held, err = ProbeLock(lockPath)
	if err != nil || held {
		t.Fatalf("expected closed lock to probe false, got held=%v err=%v", held, err)
	}

	filePath := filepath.Join(dir, "state.json")
	if err := WriteAtomic(filePath, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("WriteAtomic failed: %v", err)
	}
	info, err := os.Lstat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 file, got %o", info.Mode().Perm())
	}
	got, err := ReadPrivateFile(filePath, 1024, false, false)
	if err != nil || string(got) != `{"ok":true}` {
		t.Fatalf("ReadPrivateFile got %q err=%v", string(got), err)
	}

	// Refuse symlink targets on WriteAtomic and ReadPrivateFile.
	symTarget := filepath.Join(dir, "sym.json")
	if err := os.Symlink(filePath, symTarget); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(symTarget, []byte(`{"bad":true}`)); err == nil {
		t.Fatal("expected WriteAtomic to reject symlink destination")
	}
	if _, err := ReadPrivateFile(symTarget, 1024, false, false); err == nil {
		t.Fatal("expected ReadPrivateFile to reject symlink")
	}
}
