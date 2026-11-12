package app

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/inbox"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
)

func TestCLIProcessInboxReceiverRestart(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "inbox.sqlite")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	st, page, cfg := relayFixture()
	if err = st.Collect(page, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !st.Seal(cfg, time.Now()) {
		t.Fatal("not sealed")
	}
	t.Setenv("WECOM_TEST_RECEIVER_TOKEN", "synthetic-receiver-token")
	cfg.URL, cfg.TokenEnv = "http://"+address+"/batches", "WECOM_TEST_RECEIVER_TOKEN"
	hook, err := relay.NewWebhook(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, binary, "inbox", "serve", "--db", path, "--listen", address, "--token-env", "WECOM_TEST_RECEIVER_TOKEN")
		cmd.Env = os.Environ()
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		if err = cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		ready := false
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			conn, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
			if dialErr == nil {
				conn.Close()
				ready = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !ready {
			cancel()
			cmd.Wait()
			t.Fatal("receiver did not start", stderr.String())
		}
		if err = hook.Send(ctx, st.Inflight.Batch); err != nil {
			cancel()
			cmd.Wait()
			t.Fatal(err, stderr.String())
		}
		if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
			cancel()
			cmd.Wait()
			t.Fatal(err)
		}
		err = cmd.Wait()
		cancel()
		if err != nil || out.Len() != 0 || strings.Contains(stderr.String(), "synthetic-receiver-token") {
			t.Fatal(err, out.String(), stderr.String())
		}
	}
	db, err := inbox.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, err := db.Status(context.Background())
	if err != nil || v.ReceivedBatches != 1 || v.ReceivedEvents != 2 || v.States[inbox.Received] != 1 {
		t.Fatal(v, err)
	}
}
