package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestCLIProcessRelayScanDeadlineRestartAndRecovery(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	dir, err := os.MkdirTemp("/tmp", "wcs-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "b.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var healthy atomic.Bool
	_, page := streamFixture()
	page.Items, page.After, page.HighWatermark = nil, 0, 0
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if healthy.Load() {
			_ = json.NewEncoder(w).Encode(broker.Response{Scan: &page})
			return
		}
		<-r.Context().Done()
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { _ = server.Close(); <-done }()
	cfg := relay.DefaultConfig()
	cfg.URL = "http://127.0.0.1:1/never-called"
	configPath, statePath := filepath.Join(dir, "config.json"), filepath.Join(dir, "state.json")
	b, _ := json.Marshal(cfg)
	if err := os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, string, int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = []string{"WECOM_CHANNEL_SOCKET=" + socket, "WECOM_CHANNEL_NO_SKILL_HINT=1"}
		var out, diag bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diag
		err := cmd.Run()
		code := 0
		if err != nil {
			if exit, ok := err.(*exec.ExitError); ok {
				code = exit.ExitCode()
			} else {
				t.Fatal(err)
			}
		}
		return out.String(), diag.String(), code
	}
	args := []string{"--timeout", "50ms", "relay", "run", "--whole-channel", "--state", statePath, "--config", configPath, "--duration", "1s"}
	start := time.Now()
	_, diag, code := run(args...)
	if code != e.ExitCode(e.New(e.CategoryConflict, "x", "x")) || !strings.Contains(diag, "RELAY_RETRIES_EXHAUSTED") || !strings.Contains(diag, "BROKER_UNREACHABLE") || time.Since(start) > 3*time.Second {
		t.Fatal(code, diag)
	}
	_, diag, code = run(args...)
	if code == 0 || !strings.Contains(diag, "BROKER_UNREACHABLE") || calls.Load() != 1 {
		t.Fatal(code, diag, calls.Load())
	}
	out, diag, code := run("relay", "status", "--state", statePath)
	if code != 0 || !strings.Contains(out, "BROKER_UNREACHABLE") || !strings.Contains(out, `"attempt": 1`) {
		t.Fatal(code, out, diag)
	}
	// Recovery is explicit; a successful empty scan clears counters but keeps history.
	healthy.Store(true)
	args[len(args)-1] = "100ms"
	_, diag, code = run(append(append([]string{}, args...), "--retry-failed")...)
	if code != 0 {
		t.Fatal(code, diag)
	}
	saved, _, err := relay.Read(statePath)
	if err != nil || saved.ScanFailures != 0 || saved.NextScan != 0 || saved.LastScanError == nil || saved.LastScan <= saved.LastScanError.At || saved.Depth() != 0 {
		t.Fatal(err, saved)
	}
	// The shorter session deadline cancels a blocked request without spending retries.
	healthy.Store(false)
	cancelPath := filepath.Join(dir, "cancel.json")
	_, diag, code = run("--timeout", "2s", "relay", "run", "--whole-channel", "--state", cancelPath, "--config", configPath, "--duration", "50ms")
	if code != 0 || !strings.Contains(diag, "stopped") {
		t.Fatal(code, diag)
	}
	saved, _, err = relay.Read(cancelPath)
	if err != nil || saved.ScanFailures != 0 || saved.LastScanError != nil || saved.After != 0 {
		t.Fatal(err, saved)
	}
}
