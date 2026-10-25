package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

func TestPrivateStoreLockAndRetention(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	s, err := Open(dir, "bot")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if other, err := Open(dir, "bot"); err == nil {
		other.Close()
		t.Fatal("second writer accepted")
	}
	now := time.Now()
	v := callback.Event{ID: "one", Channel: "c", Created: now.Unix(), Received: now.Unix()}
	if err = s.AddEvent(v, now, 24*time.Hour, 1); err != nil {
		t.Fatal(err)
	}
	v.ID = "two"
	if s.AddEvent(v, now, 24*time.Hour, 1) == nil {
		t.Fatal("capacity not enforced")
	}
	if len(s.View().Events) != 1 {
		t.Fatal("failed update changed memory")
	}
	v.Received = now.Add(25 * time.Hour).Unix()
	if s.AddEvent(v, now.Add(25*time.Hour), 24*time.Hour, 1) != nil {
		t.Fatal("retention failed")
	}
	i, err := os.Stat(filepath.Join(dir, "ledger.json"))
	if err != nil || i.Mode().Perm() != 0600 {
		t.Fatal("unsafe ledger")
	}
	s.Close()
	s, err = Open(dir, "bot")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.View().Events["two"]; !ok {
		t.Fatal("state did not persist")
	}
	s.Close()
	if s, err = Open(dir, "other-bot"); err == nil {
		s.Close()
		t.Fatal("cross-bot state accepted")
	}
}

func TestUnsafeStoreRejected(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0755)
	if s, err := Open(dir, "bot"); err == nil {
		s.Close()
		t.Fatal("public state directory accepted")
	}
	_ = os.Chmod(dir, 0700)
	target := filepath.Join(dir, "actual")
	_ = os.WriteFile(target, []byte("x"), 0600)
	_ = os.Symlink(target, filepath.Join(dir, "ledger.json"))
	if s, err := Open(dir, "bot"); err == nil {
		s.Close()
		t.Fatal("symlink state accepted")
	}
}
