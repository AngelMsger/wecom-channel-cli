package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
)

func TestSocketSeparationAndShutdown(t *testing.T) {
	b, api := fixture(t)
	// Short paths also fit Darwin's Unix-domain socket address limit.
	dir, err := os.MkdirTemp("/private/tmp", "wcc-")
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", "wcc-")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"agent", "operator"} {
		if err = os.Mkdir(filepath.Join(dir, d), 0700); err != nil {
			t.Fatal(err)
		}
	}
	b.Config.AgentSocket = filepath.Join(dir, "agent", "agent.sock")
	b.Config.AdminSocket = filepath.Join(dir, "operator", "admin.sock")
	p := proposal(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- b.Serve(ctx, func() { close(ready) }) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("not ready")
	}
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("broker did not shut down")
		}
	}()
	for path, mode := range map[string]os.FileMode{b.Config.AgentSocket: 0660, b.Config.AdminSocket: 0600} {
		i, err := os.Stat(path)
		if err != nil || i.Mode().Perm() != mode {
			t.Fatal("socket permissions", err)
		}
	}
	r := Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}
	_, err = Call(context.Background(), b.Config.AgentSocket, r)
	assertCode(t, err, "POLICY_DENIED")
	out, err := Call(context.Background(), b.Config.AdminSocket, r)
	if err != nil || out.Item == nil || api.sends != 1 {
		t.Fatal("operator approval failed", err)
	}
	_, err = Call(context.Background(), b.Config.AgentSocket, Request{Operation: "post.list", Query: apiclient.Query{Channel: "channel-a"}})
	assertCode(t, err, "POLICY_DENIED")
}

func TestStaleSocketRecoveryAndFileProtection(t *testing.T) {
	dir, err := os.MkdirTemp("/private/tmp", "wcc-stale-")
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", "wcc-stale-")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Create a stale Unix socket (listening socket whose fd is closed without unlinking).
	stalePath := filepath.Join(dir, "stale.sock")
	l, err := unixListener(stalePath, true, true)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the file on disk while closing the listener (simulating SIGKILL).
	f, err := os.Create(filepath.Join(dir, "not-a-socket"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := unixListener(filepath.Join(dir, "not-a-socket"), true, true); err == nil {
		t.Fatal("overwrote non-socket regular file")
	}
	// Active socket must still be refused.
	if _, err := unixListener(stalePath, true, true); err == nil {
		t.Fatal("replaced active listener socket")
	}
	_ = l.Close()

	// Create a genuinely orphaned socket path via unix bind + syscall close (without Go unlink).
	stalePath2 := filepath.Join(dir, "orphan.sock")
	l2, err := unixListener(stalePath2, true, true)
	if err != nil {
		t.Fatal(err)
	}
	// UnlinkOnClose(false) leaves the socket node on disk after Close(), matching SIGKILL.
	if ul, ok := l2.(interface{ SetUnlinkOnClose(bool) }); ok {
		ul.SetUnlinkOnClose(false)
	}
	_ = l2.Close()
	if _, err := os.Lstat(stalePath2); err != nil {
		t.Fatalf("expected stale socket node on disk: %v", err)
	}
	rebound, err := unixListener(stalePath2, true, true)
	if err != nil {
		t.Fatalf("failed to recover stale socket: %v", err)
	}
	_ = rebound.Close()
}
