package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

type noWatchNetwork struct{}

func (noWatchNetwork) Read(context.Context, apiclient.Query) (apiclient.Page, error) {
	panic("watch attempted remote content retrieval")
}
func (noWatchNetwork) Send(context.Context, apiclient.WriteRequest) (apiclient.SendResult, error) {
	panic("watch attempted a remote write")
}

func TestCLIProcessWatch(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	dir, err := os.MkdirTemp("/private/tmp", "wcw-")
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", "wcw-")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, _ = filepath.EvalSymlinks(dir)
	for _, name := range []string{"agent", "operator", "state"} {
		if err = os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.Bot = "bot"
	cfg.Name = "engineering"
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.AgentSocket = filepath.Join(dir, "agent", "a.sock")
	cfg.AdminSocket = filepath.Join(dir, "operator", "a.sock")
	cfg.CallbackListen = "127.0.0.1:0"
	cfg.Channels = []config.Channel{{ID: "channel", ReadAll: true, ObserveAll: true}}
	cfg.Events = []string{"topic_event", "topic_reply_event"}
	st, err := state.Open(cfg.StateDir, cfg.Bot, "engineering\x00channel")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	verifier, _ := callback.New("token", "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C", "")
	b := broker.New(cfg, st, noWatchNetwork{}, verifier)
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
			t.Error("broker still running")
		}
	}()
	add := func(id, post string) {
		t.Helper()
		now := time.Now()
		if err := st.AddEvent(callback.Event{ID: id, Bot: "bot", Channel: "channel", Post: post, Sender: "another-bot", SenderKind: "bot", Kind: "topic_reply_event", Created: now.Unix(), Received: now.Unix()}, now, 48*time.Hour, 100); err != nil {
			t.Fatal(err)
		}
	}
	runBinary := func(args ...string) (string, string, int) {
		t.Helper()
		bounded, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		cmd := exec.CommandContext(bounded, binary, args...)
		cmd.Env = []string{"WECOM_CHANNEL_SOCKET=" + cfg.AgentSocket, "WECOM_CHANNEL_SKILL_LOADED=0.1.4"}
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		code := 0
		if err != nil {
			if exit, ok := err.(*exec.ExitError); ok {
				code = exit.ExitCode()
			} else {
				t.Fatal(err)
			}
		}
		return out.String(), stderr.String(), code
	}
	add("one", "other")
	add("two", "selected")
	checkpoint := filepath.Join(dir, "watch.json")
	args := []string{"event", "watch", "--post", "selected", "--duration", "1s", "--interval", "1s", "--max-events", "1", "--checkpoint", checkpoint}
	out, stderr, code := runBinary(append(append([]string{}, args...), "--after-sequence", "0")...)
	if code != 0 || !json.Valid([]byte(out)) || !strings.Contains(out, `"sequence":2`) || !strings.Contains(out, `"observation_only":true`) {
		t.Fatal(code, out, stderr)
	}
	add("three", "other")
	add("four", "selected")
	out, stderr, code = runBinary(args...)
	if code != 0 || !strings.Contains(out, `"sequence":4`) || strings.Contains(out, `"sequence":2`) {
		t.Fatal(code, out, stderr)
	}
	out, stderr, code = runBinary("event", "watch", "--whole-channel", "--checkpoint", checkpoint, "--duration", "1s")
	if code != 11 || out != "" || !strings.Contains(stderr, "WATCH_BINDING_CHANGED") {
		t.Fatal(code, out, stderr)
	}
	preview := filepath.Join(dir, "preview.json")
	out, stderr, code = runBinary("event", "watch", "--whole-channel", "--checkpoint", preview, "--dry-run")
	if code != 0 || !strings.Contains(out, `"dry_run": true`) {
		t.Fatal(code, out, stderr)
	}
	for _, path := range []string{preview, preview + ".lock"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("dry-run wrote files")
		}
	}
	start := time.Now()
	out, stderr, code = runBinary("event", "watch", "--whole-channel", "--duration", "150ms")
	if code != 0 || out != "" || time.Since(start) > 2*time.Second || !strings.Contains(stderr, `"state":"stopped"`) {
		t.Fatal(code, out, stderr)
	}
	// Exercise the actual process signal path after the watch reports ready.
	bounded, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	process := exec.CommandContext(bounded, binary, "event", "watch", "--whole-channel", "--duration", "1m")
	process.Env = []string{"WECOM_CHANNEL_SOCKET=" + cfg.AgentSocket, "WECOM_CHANNEL_SKILL_LOADED=0.1.4"}
	pipe, err := process.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	process.Stdout = &stream
	if err = process.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(pipe)
	started := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), `"state":"started"`) {
			started = true
			break
		}
	}
	if !started {
		_ = process.Wait()
		t.Fatal("watch did not report readiness before signal")
	}
	if err = process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	for scanner.Scan() {
	}
	if err = process.Wait(); err != nil || stream.Len() != 0 {
		t.Fatal("signal did not stop watch cleanly", err, stream.String())
	}
	if err = st.Update(func(s *state.Snapshot) error { s.ID = "different-ledger-identity"; return nil }); err != nil {
		t.Fatal(err)
	}
	out, stderr, code = runBinary(args...)
	if code != 11 || out != "" || !strings.Contains(stderr, "WATCH_BINDING_CHANGED") {
		t.Fatal(code, out, stderr)
	}
}
