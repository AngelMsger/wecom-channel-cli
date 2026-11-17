package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

func TestCLIProcessRelay(t *testing.T) {
	for _, mode := range []string{relay.ExplicitAck, relay.HTTPAccepted} {
		t.Run(mode, func(t *testing.T) { testCLIProcessRelay(t, mode) })
	}
}

func testCLIProcessRelay(t *testing.T, mode string) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	dir, err := os.MkdirTemp("/private/tmp", "wcr-")
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", "wcr-")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, _ = filepath.EvalSymlinks(dir)
	for _, n := range []string{"agent", "operator", "broker", "relay"} {
		if err = os.Mkdir(filepath.Join(dir, n), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.Bot, cfg.Name = "bot", "engineering"
	cfg.StateDir, cfg.AgentSocket, cfg.AdminSocket = filepath.Join(dir, "broker"), filepath.Join(dir, "agent", "a.sock"), filepath.Join(dir, "operator", "a.sock")
	cfg.CallbackListen = "127.0.0.1:0"
	cfg.Channels = []config.Channel{{ID: "channel", ReadAll: true, ObserveAll: true}}
	cfg.Events = []string{"topic_event", "topic_reply_event"}
	ledger, err := state.Open(cfg.StateDir, cfg.Bot, "engineering\x00channel")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	// Generate a process-local callback key; this test does not need a known-answer vector.
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	verifier, err := callback.New("token", base64.RawStdEncoding.EncodeToString(key), "")
	if err != nil {
		t.Fatal(err)
	}
	b := broker.New(cfg, ledger, noWatchNetwork{}, verifier)
	ctx, cancel := context.WithCancel(context.Background())
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() { done <- b.Serve(ctx, func() { close(ready) }) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("broker failed to stop")
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("broker not ready")
	}
	add := func(id, sender, senderKind string) {
		t.Helper()
		now := time.Now()
		v := callback.Event{ID: id, Bot: "bot", Channel: "channel", Post: "post", Reply: id, Kind: "topic_reply_event", Sender: sender, SenderKind: senderKind, Change: 1, Created: now.Unix(), Received: now.Unix()}
		if err := ledger.AddEvent(v, now, 48*time.Hour, 100); err != nil {
			t.Fatal(err)
		}
	}
	add("one", "human", "user")
	add("two", "human", "user")
	var mu sync.Mutex
	var bodies []string
	uncertain := false
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(data))
		attempt := len(bodies)
		loseResponse := uncertain
		mu.Unlock()
		var batch relay.Batch
		if json.Unmarshal(data, &batch) != nil || batch.ID == "" || r.Header.Get("Idempotency-Key") != batch.ID {
			t.Error("invalid batch request")
		}
		if loseResponse {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		if attempt == 1 {
			w.WriteHeader(503)
			return
		}
		if mode == relay.HTTPAccepted {
			if r.Header.Get("Authorization") != "Bearer synthetic" {
				t.Error("missing bearer")
			}
			w.WriteHeader(200)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"ack": batch.ID})
	}))
	defer receiver.Close()
	c := relay.DefaultConfig()
	c.URL, c.BatchSize, c.RetryLimit, c.RetryBaseSeconds = receiver.URL, 2, 1, 1
	if mode == relay.HTTPAccepted {
		c.DeliveryMode = mode
		c.TokenEnv = "TEST_GROK_TOKEN"
	}
	configPath, statePath := filepath.Join(dir, "relay.json"), filepath.Join(dir, "relay", "state.json")
	data, _ := json.Marshal(c)
	if err = os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	runRelay := func(extraEnv []string, args ...string) (string, string, int) {
		t.Helper()
		bounded, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		process := exec.CommandContext(bounded, binary, args...)
		process.Env = append([]string{"WECOM_CHANNEL_SOCKET=" + cfg.AgentSocket, "WECOM_CHANNEL_NO_SKILL_HINT=1", "TEST_GROK_TOKEN=synthetic"}, extraEnv...)
		var out, stderr bytes.Buffer
		process.Stdout, process.Stderr = &out, &stderr
		err := process.Run()
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
	args := []string{"relay", "run", "--whole-channel", "--config", configPath, "--state", statePath, "--duration", "200ms", "--max-batches", "1"}
	preview := append(append([]string{}, args...), "--dry-run")
	out, stderr, code := runRelay([]string{"WECOM_CHANNEL_CLI_READ_ONLY=true"}, preview...)
	if code != 0 || !strings.Contains(out, `"dry_run": true`) {
		t.Fatal(code, out, stderr)
	}
	for _, path := range []string{statePath, statePath + ".lock"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("dry-run wrote state")
		}
	}
	out, stderr, code = runRelay([]string{"WECOM_CHANNEL_CLI_READ_ONLY=true"}, args...)
	if code == 0 || out != "" || !strings.Contains(stderr, "READONLY_BLOCKED") {
		t.Fatal(code, out, stderr)
	}
	out, stderr, code = runRelay(nil, append(append([]string{}, args...), "--after-sequence", "0")...)
	if code != 0 || out != "" || !strings.Contains(stderr, "delivery_retry") {
		t.Fatal(code, out, stderr)
	}
	queued, exists, err := relay.Read(statePath)
	if err != nil || !exists || queued.Depth() != 2 || queued.After != 2 || queued.Acknowledged != 0 {
		t.Fatal(queued, err)
	}
	args[len(args)-3] = "3s" // duration value, before --max-batches 1
	out, stderr, code = runRelay(nil, args...)
	if code != 0 || out != "" || !strings.Contains(stderr, "max_batches") {
		t.Fatal(code, out, stderr)
	}
	mu.Lock()
	requests := append([]string(nil), bodies...)
	mu.Unlock()
	if len(requests) != 2 || requests[0] != requests[1] {
		t.Fatal("restart changed immutable payload", requests)
	}
	var batch relay.Batch
	_ = json.Unmarshal([]byte(requests[1]), &batch)
	if len(batch.Threads) != 1 || len(batch.Threads[0].Events) != 2 || batch.Threads[0].Events[0].Trust != "untrusted" {
		t.Fatal("lost grouping/trust")
	}
	out, stderr, code = runRelay(nil, "relay", "status", "--state", statePath)
	counter := `"acknowledged_events": 2`
	if mode == relay.HTTPAccepted {
		counter = `"accepted_events": 2`
	}
	if code != 0 || !strings.Contains(out, counter) || !strings.Contains(out, `"queued_events": 0`) {
		t.Fatal(code, out, stderr)
	}
	if !strings.Contains(out, `"last_delivery_error"`) || !strings.Contains(out, `"code": "RELAY_REJECTED"`) || !strings.Contains(out, `"http_status": 503`) {
		t.Fatal("delivery error history lost after restart and success", out)
	}
	// A retained self callback is skipped by the broker before reaching the relay.
	add("self", "bot", "bot")
	args[len(args)-3] = "150ms"
	out, stderr, code = runRelay(nil, args...)
	if code != 0 || out != "" {
		t.Fatal(code, out, stderr)
	}
	queued, _, err = relay.Read(statePath)
	if err != nil || queued.After != 3 || queued.Filtered != 0 || queued.Depth() != 0 {
		t.Fatal(queued, err)
	}
	mu.Lock()
	count := len(bodies)
	mu.Unlock()
	if count != 2 {
		t.Fatal("empty/filtered scan dispatched", count)
	}

	if mode == relay.HTTPAccepted {
		// Worker uses its own file, without sender config or credential access.
		payloadPath := filepath.Join(dir, "batch.json")
		_ = os.WriteFile(payloadPath, []byte(requests[1]), 0600)
		workerPath := filepath.Join(dir, "worker.json")
		claimArgs := []string{"relay", "claim", "--consumer-state", workerPath, "--batch-file", payloadPath}
		out, stderr, code = runRelay(nil, append(append([]string{}, claimArgs...), "--dry-run")...)
		if code != 0 {
			t.Fatal(out, stderr)
		}
		if _, err := os.Stat(workerPath + ".lock"); !os.IsNotExist(err) {
			t.Fatal("dry claim wrote lock")
		}
		out, stderr, code = runRelay(nil, claimArgs...)
		var claimed relay.ClaimResult
		if code != 0 || json.Unmarshal([]byte(out), &claimed) != nil || !claimed.Claimed || claimed.Token == "" {
			t.Fatal(out, stderr)
		}
		out, stderr, code = runRelay(nil, claimArgs...)
		if code != 0 || !strings.Contains(out, `"claimed": false`) || strings.Contains(out, "claim_id") {
			t.Fatal("duplicate granted work", out, stderr)
		}
		consumeArgs := []string{"relay", "consume", "--consumer-state", workerPath, "--batch-id", batch.ID, "--claim-id", claimed.Token, "--result", "processed"}
		for i := 0; i < 2; i++ {
			out, stderr, code = runRelay(nil, consumeArgs...)
			if code != 0 {
				t.Fatal(out, stderr)
			}
		}
		out, stderr, code = runRelay(nil, "relay", "status", "--consumer-state", workerPath, "--batch-id", batch.ID)
		if code != 0 || !strings.Contains(out, `"status": "processed"`) || strings.Contains(out, claimed.Token) {
			t.Fatal(out, stderr)
		}
		// Restart with memory-only state establishes the current head quietly.
		memoryArgs := []string{"relay", "run", "--whole-channel", "--config", configPath, "--ephemeral", "--duration", "100ms"}
		for i := 0; i < 2; i++ {
			out, stderr, code = runRelay(nil, memoryArgs...)
			if code != 0 {
				t.Fatal(out, stderr)
			}
		}
		mu.Lock()
		count = len(bodies)
		uncertain = true
		mu.Unlock()
		if count != 2 {
			t.Fatal("memory startup replayed history")
		}
		unknownPath := filepath.Join(dir, "relay", "unknown.json")
		unknownArgs := []string{"relay", "run", "--whole-channel", "--config", configPath, "--state", unknownPath, "--duration", "1s"}
		out, stderr, code = runRelay(nil, append(append([]string{}, unknownArgs...), "--after-sequence", "0")...)
		if code == 0 || !strings.Contains(stderr, "RELAY_OUTCOME_UNKNOWN") {
			t.Fatal(out, stderr)
		}
		unknown, _, err := relay.Read(unknownPath)
		if err != nil || unknown.Inflight == nil || unknown.Inflight.Status != "unknown" {
			t.Fatal(unknown, err)
		}
		for _, retry := range []bool{false, true} {
			attemptArgs := append([]string{}, unknownArgs...)
			if retry {
				attemptArgs = append(attemptArgs, "--retry-failed")
			}
			out, stderr, code = runRelay(nil, attemptArgs...)
			if code == 0 || !strings.Contains(stderr, "RELAY_OUTCOME_UNKNOWN") {
				t.Fatal(out, stderr)
			}
		}
		mu.Lock()
		count = len(bodies)
		mu.Unlock()
		if count != 3 {
			t.Fatal("unknown result replayed", count)
		}
		id := unknown.Inflight.Batch.ID
		out, stderr, code = runRelay(nil, "relay", "resolve", "--state", unknownPath, "--batch-id", id, "--outcome", "accepted", "--evidence", "synthetic-host-run")
		if code != 0 {
			t.Fatal(out, stderr)
		}
		unknown, _, err = relay.Read(unknownPath)
		if err != nil || unknown.Inflight != nil || unknown.AcceptedBatches != 1 || unknown.AckedBatches != 0 {
			t.Fatal(unknown, err)
		}
	}
	// Destination/filter changes cannot reuse the queue, even after it drains.
	c.Changes = []int{2}
	data, _ = json.Marshal(c)
	_ = os.WriteFile(configPath, data, 0600)
	out, stderr, code = runRelay(nil, args...)
	if code != 11 || out != "" || !strings.Contains(stderr, "RELAY_BINDING_CHANGED") {
		t.Fatal(code, out, stderr)
	}
}
