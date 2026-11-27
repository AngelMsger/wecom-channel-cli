package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/angelmsger/agent-cli-kit/credstore"
	kittransport "github.com/angelmsger/agent-cli-kit/transport"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

type memProfileCreds struct {
	mu      sync.Mutex
	secrets map[string]string
}

func newMemProfileCreds() *memProfileCreds {
	return &memProfileCreds{secrets: map[string]string{}}
}

func (m *memProfileCreds) Load(account string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.secrets[account]
	if !ok {
		return "", credstore.ErrSecretNotFound
	}
	return v, nil
}

func (m *memProfileCreds) Save(account, secret string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.secrets[account] = secret
	return "memory", nil
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func runRootWithRuntime(ctx context.Context, rt profileRuntime, args ...string) (string, string, error) {
	root := newRootCmd(rt)
	var out, errout bytes.Buffer
	root.SetIn(strings.NewReader(""))
	root.SetOut(&out)
	root.SetErr(&errout)
	root.SetArgs(args)
	err := root.ExecuteContext(ctx)
	return out.String(), errout.String(), err
}

// TestConnectRealHTTPClientDecodingAndSelection exercises `connect` through
// the real apiclient.HTTPClient (where record.Data is a JSON-redacted
// map[string]any, NOT a typed apiclient.Topic), pinning multi-page discovery,
// channel rename preservation, partial failure preservation, proxy persistence,
// secret hygiene, and duplicate-name disambiguation.
func TestConnectRealHTTPClientDecodingAndSelection(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	creds := newMemProfileCreds()

	var mu sync.Mutex
	renameFirst := false
	failPage2 := false

	jsonResp := func(status int, payload any) *http.Response {
		b, _ := json.Marshal(payload)
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(b)),
		}
	}

	fakeWeCom := doerFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/cgi-bin/aibot/get_token":
			return jsonResp(http.StatusOK, map[string]any{
				"errcode":      0,
				"errmsg":       "ok",
				"access_token": "tok-live",
				"expires_in":   7200,
			}), nil
		case "/cgi-bin/aibot/topic/get_topic_list":
			var body struct {
				Cursor string `json:"cursor"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			renamed := renameFirst
			fail2 := failPage2
			mu.Unlock()
			if body.Cursor == "" {
				firstName := "Engineering"
				if renamed {
					firstName = "Engineering-Renamed"
				}
				return jsonResp(http.StatusOK, map[string]any{
					"errcode": 0,
					"errmsg":  "ok",
					"topic_list": []map[string]any{
						{"topic_id": "top-eng", "topic_name": firstName, "create_time": 1700000001},
						{"topic_id": "top-dup-1", "topic_name": "Alerts", "create_time": 1700000002},
					},
					"has_more":    true,
					"next_cursor": "page-2",
				}), nil
			}
			if fail2 {
				return jsonResp(http.StatusInternalServerError, map[string]any{"errcode": 500}), nil
			}
			return jsonResp(http.StatusOK, map[string]any{
				"errcode": 0,
				"errmsg":  "ok",
				"topic_list": []map[string]any{
					{"topic_id": "top-dup-2", "topic_name": "Alerts", "create_time": 1700000003},
					{"topic_id": "top-zh", "topic_name": "研发公告", "create_time": 1700000004},
				},
				"has_more": false,
			}), nil
		default:
			return jsonResp(http.StatusNotFound, map[string]any{"errcode": 404}), nil
		}
	})

	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client: func(bot, secret string, tcfg transport.Config) (apiclient.Client, error) {
			return apiclient.NewWithOptions(bot, secret, apiclient.Options{
				Transport: tcfg,
				HTTP:      kittransport.New(kittransport.Options{Doer: fakeWeCom, MaxRetries: 0}),
			})
		},
	}

	const secretVal = "super-secret-bot-key-9876"
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", secretVal)

	out, errout, err := runRootWithRuntime(context.Background(), rt,
		"--config-dir", dir,
		"connect", "--bot-id", "bot-alpha",
		"--proxy", "socks5h://127.0.0.1:1080",
		"--upstream-timeout", "60s",
	)
	if err != nil {
		t.Fatalf("connect failed: %v\nstderr=%s", err, errout)
	}
	if strings.Contains(out, secretVal) || strings.Contains(errout, secretVal) {
		t.Fatal("connect leaked bot secret to output")
	}
	profileBytes, err := os.ReadFile(config.ProfilePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(profileBytes), secretVal) {
		t.Fatal("connect leaked bot secret into profile.yaml")
	}
	savedSecret, err := creds.Load("bot-alpha")
	if err != nil || savedSecret != secretVal {
		t.Fatalf("expected verified secret saved in credstore, got %q err=%v", savedSecret, err)
	}

	p, exists, err := config.LoadProfile(dir, false)
	if err != nil || !exists {
		t.Fatalf("LoadProfile failed: exists=%v err=%v", exists, err)
	}
	if len(p.Channels) != 4 || p.Network.Proxy != "socks5h://127.0.0.1:1080" || p.Network.TimeoutSeconds != 60 {
		t.Fatalf("unexpected profile after connect: %+v", p)
	}

	// Verify duplicate channel name returns CHANNEL_NAME_AMBIGUOUS, while topic_id
	// and --pick select cleanly.
	_, _, err = runRootWithRuntime(context.Background(), rt, "--config-dir", dir, "listen", "--channel", "Alerts", "--dry-run")
	if e.Safe(err).Code != "CHANNEL_NAME_AMBIGUOUS" {
		t.Fatalf("expected CHANNEL_NAME_AMBIGUOUS, got %v", err)
	}
	out, _, err = runRootWithRuntime(context.Background(), rt, "--config-dir", dir, "listen", "--channel", "top-dup-2", "--dry-run")
	if err != nil || !strings.Contains(out, `"selected_count": 1`) {
		t.Fatalf("expected topic_id selection dry-run to succeed, out=%s err=%v", out, err)
	}

	// Persist selection of Engineering, then rename it remotely and re-run connect
	// using the saved credstore secret (empty env var).
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "")
	for i := range p.Channels {
		if p.Channels[i].ID == "top-eng" {
			p.Channels[i].Selected = true
		}
	}
	if err := config.SaveProfile(dir, p, false); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	renameFirst = true
	mu.Unlock()
	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", dir, "connect"); err != nil {
		t.Fatalf("re-connect using saved credstore failed: %v", err)
	}
	p2, _, err := config.LoadProfile(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	selected := p2.SelectedChannels()
	if len(selected) != 1 || selected[0].ID != "top-eng" || selected[0].Name != "Engineering-Renamed" {
		t.Fatalf("expected selection preserved across rename, got %+v", selected)
	}

	// Partial discovery failure on page 2 must leave the previously saved catalog untouched.
	mu.Lock()
	failPage2 = true
	mu.Unlock()
	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", dir, "connect"); err == nil {
		t.Fatal("expected partial discovery failure on page 2")
	}
	p3, _, err := config.LoadProfile(dir, false)
	if err != nil || len(p3.Channels) != 4 {
		t.Fatalf("expected saved catalog preserved on partial discovery failure, got %+v err=%v", p3.Channels, err)
	}
}

type scriptPollAPI struct {
	mu      sync.Mutex
	calls   int
	failN   int
	replies []apiclient.Message
}

func (a *scriptPollAPI) Read(_ context.Context, q apiclient.Query) (apiclient.Page, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if q.Operation == "post.list" {
		a.calls++
		if a.calls > 1 && a.calls <= 1+a.failN {
			switch a.calls % 5 {
			case 0:
				return apiclient.Page{}, e.New(e.CategoryRateLimit, "RATE_LIMITED", "rate limited").WithDetails(apiclient.FailureMetadata{RetryAfterSeconds: 2})
			case 1:
				return apiclient.Page{}, e.New(e.CategoryServer, "SCAN_TIMEOUT", "timeout").WithDetails(apiclient.FailureMetadata{NetworkReason: "timeout"})
			case 2:
				return apiclient.Page{}, e.New(e.CategoryServer, "RESPONSE_INTERRUPTED", "eof").WithDetails(apiclient.FailureMetadata{NetworkReason: "eof"})
			case 3:
				return apiclient.Page{}, e.New(e.CategoryParse, "BAD_RESPONSE", "waf html")
			default:
				return apiclient.Page{}, e.New(e.CategoryServer, "REQUEST_UNCERTAIN", "reset").WithDetails(apiclient.FailureMetadata{NetworkReason: "reset"})
			}
		}
		return apiclient.Page{Items: []apiclient.Record{
			apiclient.Untrusted(apiclient.Message{
				PostID:             "p-1",
				Creator:            &apiclient.User{UserID: "user-a"},
				Created:            1700000100,
				Modified:           1700000100,
				Content:            "initial post",
				AttachmentsOmitted: true,
			}, q.Channel, "p-1", ""),
		}}, nil
	}
	if q.Operation == "reply.list" {
		items := make([]apiclient.Record, 0, len(a.replies))
		for _, rep := range a.replies {
			items = append(items, apiclient.Untrusted(rep, q.Channel, rep.PostID, ""))
		}
		return apiclient.Page{Items: items}, nil
	}
	return apiclient.Page{}, nil
}

func (a *scriptPollAPI) Send(context.Context, apiclient.WriteRequest) (apiclient.SendResult, error) {
	return apiclient.SendResult{}, e.New(e.CategoryPermission, "READONLY_BLOCKED", "read-only")
}

// TestListenResilientUnlimitedRecoveryAndCheckpoint verifies that `listen`
// survives >25 consecutive transient cross-ocean failures with exponential
// backoff, emits degraded/recovered notices, recovers automatically, and
// preserves checkpoints across operational interval changes without duplicating
// emitted observations.
func TestListenResilientUnlimitedRecoveryAndCheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		p := config.DefaultProfile("bot-1")
		p.Polling.IntervalSeconds = 10
		p.Channels = []config.ProfileChannel{{ID: "ch-1", Name: "Engineering", Selected: true}}
		if err := config.SaveProfile(dir, p, false); err != nil {
			t.Fatal(err)
		}

		api := &scriptPollAPI{failN: 25}
		creds := newMemProfileCreds()
		_, _ = creds.Save("bot-1", "secret-1")
		rt := profileRuntime{
			credentials: func(string) profileCredentials { return creds },
			client:      func(string, string, transport.Config) (apiclient.Client, error) { return api, nil },
		}

		ctx, cancel := context.WithCancel(context.Background())
		var out, errout bytes.Buffer
		done := make(chan error, 1)
		go func() {
			root := newRootCmd(rt)
			root.SetIn(strings.NewReader(""))
			root.SetOut(&out)
			root.SetErr(&errout)
			root.SetArgs([]string{"--config-dir", dir, "listen"})
			done <- root.ExecuteContext(ctx)
		}()

		// Let baseline succeed, then add r-1 while the next 25 polls fail with
		// jittered exponential backoff; poll 27 is the first post-recovery poll.
		for range 400 {
			time.Sleep(30 * time.Second)
			synctest.Wait()
			api.mu.Lock()
			calls := api.calls
			if calls >= 1 && len(api.replies) == 0 {
				api.replies = append(api.replies, apiclient.Message{
					PostID:             "p-1",
					ReplyID:            "r-1",
					Creator:            &apiclient.User{UserID: "user-b"},
					Created:            1700000200,
					Modified:           1700000200,
					Content:            "reply one",
					AttachmentsOmitted: true,
				})
			}
			api.mu.Unlock()
			if calls >= 27 {
				break
			}
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("listen exited with unexpected error: %v\nstderr=%s", err, errout.String())
		}

		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(lines) != 1 || !strings.Contains(lines[0], `"r-1"`) || !strings.Contains(lines[0], `"trust":"untrusted"`) {
			t.Fatalf("expected 1 untrusted NDJSON event for r-1, got %q", out.String())
		}
		stderrStr := errout.String()
		if !strings.Contains(stderrStr, `"consecutive_failures":25`) || !strings.Contains(stderrStr, `"state":"recovered"`) {
			t.Fatalf("expected 25 degraded failures followed by recovery notice, got:\n%s", stderrStr)
		}

		// Tune interval_seconds from 10 -> 5 (operational tuning) and inject r-2.
		// Restarting listen must NOT fail with WATCH_BINDING_CHANGED and must NOT
		// re-emit r-1.
		api.mu.Lock()
		api.failN = 0
		api.replies = append(api.replies, apiclient.Message{
			PostID:             "p-1",
			ReplyID:            "r-2",
			Creator:            &apiclient.User{UserID: "user-c"},
			Created:            1700000300,
			Modified:           1700000300,
			Content:            "reply two",
			AttachmentsOmitted: true,
		})
		api.mu.Unlock()

		ctx2, cancel2 := context.WithCancel(context.Background())
		var out2, err2 bytes.Buffer
		done2 := make(chan error, 1)
		go func() {
			root := newRootCmd(rt)
			root.SetIn(strings.NewReader(""))
			root.SetOut(&out2)
			root.SetErr(&err2)
			root.SetArgs([]string{"--config-dir", dir, "listen", "--interval", "5s"})
			done2 <- root.ExecuteContext(ctx2)
		}()
		time.Sleep(15 * time.Second)
		synctest.Wait()
		cancel2()
		if err := <-done2; err != nil {
			t.Fatalf("restarted listen failed: %v\nstderr=%s", err, err2.String())
		}
		lines2 := strings.Split(strings.TrimSpace(out2.String()), "\n")
		if len(lines2) != 1 || !strings.Contains(lines2[0], `"r-2"`) || strings.Contains(out2.String(), `"r-1"`) {
			t.Fatalf("expected only new event r-2 after checkpoint resume, got %q", out2.String())
		}
	})
}

// TestServeUnifiedEndToEndSocketsWebhookAndStatus verifies the full unified
// single-process `serve` daemon:
//   - real `status` liveness via serve.lock and `status --check`
//   - auto-synced config.yaml client context + live Unix socket CLI reads (`post list`)
//   - quiet initial baseline (0 webhook calls on empty scan)
//   - instant webhook delivery on new replies (Grok http_accepted mode)
//   - non-deadlocking recovery from post-send webhook timeout under on_uncertain=skip
//   - clean stopped detection after shutdown
func TestServeUnifiedEndToEndSocketsWebhookAndStatus(t *testing.T) {
	// Use a short base dir so Unix sockets stay well under macOS/Linux 100-byte limits.
	base, err := os.MkdirTemp("", "wccs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	cfgDir := filepath.Join(base, "cfg")
	dataDir := filepath.Join(base, "d")

	var hookMu sync.Mutex
	var receivedBatches []string
	var hookHang atomic.Bool
	hookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-grok-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if hookHang.Load() {
			// Exceed the 1s webhook timeout so the sender observes a post-send
			// timeout (uncertain outcome in http_accepted mode).
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
			return
		}
		hookMu.Lock()
		receivedBatches = append(receivedBatches, string(body))
		hookMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer hookSrv.Close()

	t.Setenv("TEST_GROK_KEY", "test-grok-key")

	zero := 0
	p := config.DefaultProfile("bot-serve")
	p.DataDir = dataDir
	p.Polling.IntervalSeconds = 5
	p.Webhook = config.ProfileWebhook{
		URL:                hookSrv.URL,
		TokenEnv:           "TEST_GROK_KEY",
		DeliveryMode:       "http_accepted",
		OnUncertain:        "skip",
		BatchWindowSeconds: 1,
		MinIntervalSeconds: &zero,
		TimeoutSeconds:     1,
	}
	p.Channels = []config.ProfileChannel{{ID: "ch-eng", Name: "Engineering", Selected: true}}
	if err := config.SaveProfile(cfgDir, p, false); err != nil {
		t.Fatal(err)
	}

	api := &scriptPollAPI{}
	creds := newMemProfileCreds()
	_, _ = creds.Save("bot-serve", "secret-serve")
	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client:      func(string, string, transport.Config) (apiclient.Client, error) { return api, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	var serveOut, serveErr bytes.Buffer
	serveDone := make(chan error, 1)
	go func() {
		root := newRootCmd(rt)
		root.SetIn(strings.NewReader(""))
		root.SetOut(&serveOut)
		root.SetErr(&serveErr)
		root.SetArgs([]string{"--config-dir", cfgDir, "serve"})
		serveDone <- root.ExecuteContext(ctx)
	}()

	// Poll `status` until the daemon is running and baseline poll is healthy.
	deadline := time.Now().Add(10 * time.Second)
	var statusReport map[string]any
	for time.Now().Before(deadline) {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		if err == nil && json.Unmarshal([]byte(out), &statusReport) == nil {
			if running, _ := statusReport["running"].(bool); running {
				if chs, ok := statusReport["channels"].([]any); ok && len(chs) == 1 {
					if m, ok := chs[0].(map[string]any); ok && m["state"] == "healthy" {
						break
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if statusReport == nil || statusReport["running"] != true || statusReport["healthy"] != true {
		cancel()
		<-serveDone
		t.Fatalf("serve did not become healthy: report=%v stderr=%s", statusReport, serveErr.String())
	}

	// `status --check` must pass while healthy.
	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status", "--check"); err != nil {
		cancel()
		<-serveDone
		t.Fatalf("status --check failed while healthy: %v", err)
	}

	// Verify auto-synced config.yaml context works for normal CLI reads over the live Unix socket.
	postOut, postErr, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "post", "list")
	if err != nil || !strings.Contains(postOut, `"p-1"`) || !strings.Contains(postOut, `"untrusted"`) {
		cancel()
		<-serveDone
		t.Fatalf("post list via auto-synced context failed: out=%s err=%s runErr=%v", postOut, postErr, err)
	}

	// Baseline scan must send zero webhook calls.
	hookMu.Lock()
	initialHooks := len(receivedBatches)
	hookMu.Unlock()
	if initialHooks != 0 {
		cancel()
		<-serveDone
		t.Fatalf("expected 0 webhook calls during empty baseline, got %d", initialHooks)
	}

	// Inject reply #1 and wait for webhook delivery.
	api.mu.Lock()
	api.replies = append(api.replies, apiclient.Message{
		PostID:             "p-1",
		ReplyID:            "r-101",
		Creator:            &apiclient.User{UserID: "user-alice"},
		Created:            1700001001,
		Modified:           1700001001,
		Content:            "first reply",
		AttachmentsOmitted: true,
	})
	api.mu.Unlock()

	waitFor(t, 12*time.Second, func() bool {
		hookMu.Lock()
		defer hookMu.Unlock()
		return len(receivedBatches) >= 1
	}, "first webhook batch delivery")

	hookMu.Lock()
	batch1 := receivedBatches[0]
	hookMu.Unlock()
	if !strings.Contains(batch1, `"r-101"`) || !strings.Contains(batch1, `"untrusted"`) {
		t.Fatalf("unexpected webhook payload: %s", batch1)
	}

	// Inject reply #2 while webhook hangs -> uncertain outcome recorded & skipped (no deadlock).
	hookHang.Store(true)
	api.mu.Lock()
	api.replies = append(api.replies, apiclient.Message{
		PostID:             "p-1",
		ReplyID:            "r-102",
		Creator:            &apiclient.User{UserID: "user-bob"},
		Created:            1700001002,
		Modified:           1700001002,
		Content:            "second reply",
		AttachmentsOmitted: true,
	})
	api.mu.Unlock()

	waitFor(t, 15*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		if err != nil {
			return false
		}
		return strings.Contains(out, `"uncertain_batches": 1`)
	}, "uncertain batch to be recorded and skipped")

	// Restore webhook and inject reply #3 -> must STILL deliver without manual repair!
	hookHang.Store(false)
	api.mu.Lock()
	api.replies = append(api.replies, apiclient.Message{
		PostID:             "p-1",
		ReplyID:            "r-103",
		Creator:            &apiclient.User{UserID: "user-carol"},
		Created:            1700001003,
		Modified:           1700001003,
		Content:            "third reply",
		AttachmentsOmitted: true,
	})
	api.mu.Unlock()

	waitFor(t, 12*time.Second, func() bool {
		hookMu.Lock()
		defer hookMu.Unlock()
		return len(receivedBatches) >= 2
	}, "subsequent webhook batch delivery after uncertain skip")

	hookMu.Lock()
	batch2 := receivedBatches[1]
	hookMu.Unlock()
	if !strings.Contains(batch2, `"r-103"`) {
		t.Fatalf("expected r-103 in post-recovery batch, got %s", batch2)
	}

	// Stop daemon and verify status detects stopped state and --check fails cleanly.
	cancel()
	if err := <-serveDone; err != nil {
		t.Fatalf("serve returned error on shutdown: %v", err)
	}
	stoppedOut, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
	if err != nil || !strings.Contains(stoppedOut, `"running": false`) || !strings.Contains(stoppedOut, `"stopped"`) {
		t.Fatalf("expected stopped status after shutdown, got out=%s err=%v", stoppedOut, err)
	}
	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status", "--check"); e.Safe(err).Code != "SERVICE_UNHEALTHY" {
		t.Fatalf("expected status --check to return SERVICE_UNHEALTHY when stopped, got %v", err)
	}
}

func TestServeWebhookFatalFailureSurfacesInStatusCheck(t *testing.T) {
	base, err := os.MkdirTemp("", "wccf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	cfgDir := filepath.Join(base, "cfg")
	dataDir := filepath.Join(base, "d")

	hookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer hookSrv.Close()
	t.Setenv("TEST_FAIL_KEY", "bad-key")

	zero := 0
	p := config.DefaultProfile("bot-fail")
	p.DataDir = dataDir
	p.Polling.IntervalSeconds = 5
	p.Webhook = config.ProfileWebhook{
		URL:                hookSrv.URL,
		TokenEnv:           "TEST_FAIL_KEY",
		DeliveryMode:       "http_accepted",
		OnUncertain:        "skip",
		BatchWindowSeconds: 1,
		MinIntervalSeconds: &zero,
		TimeoutSeconds:     5,
	}
	p.Channels = []config.ProfileChannel{{ID: "ch-1", Name: "Ops", Selected: true}}
	if err := config.SaveProfile(cfgDir, p, false); err != nil {
		t.Fatal(err)
	}

	api := &scriptPollAPI{}
	creds := newMemProfileCreds()
	_, _ = creds.Save("bot-fail", "secret-fail")
	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client:      func(string, string, transport.Config) (apiclient.Client, error) { return api, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, runErr := runRootWithRuntime(ctx, rt, "--config-dir", cfgDir, "serve")
		done <- runErr
	}()

	waitFor(t, 8*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		return err == nil && strings.Contains(out, `"state": "healthy"`)
	}, "initial healthy baseline")

	// Inject an event so the webhook hits terminal 401 Unauthorized.
	api.mu.Lock()
	api.replies = append(api.replies, apiclient.Message{
		PostID:             "p-1",
		ReplyID:            "r-unauth",
		Creator:            &apiclient.User{UserID: "user-x"},
		Created:            1700001005,
		Modified:           1700001005,
		Content:            "trigger 401",
		AttachmentsOmitted: true,
	})
	api.mu.Unlock()

	waitFor(t, 12*time.Second, func() bool {
		_, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status", "--check")
		return e.Safe(err).Code == "SERVICE_UNHEALTHY" && strings.Contains(err.Error(), "webhook_failed")
	}, "status --check to surface fatal webhook failure")

	cancel()
	<-done
}

func TestCLIProcessUnifiedPreviewAndStatus(t *testing.T) {
	bin := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if bin == "" {
		t.Skip("set WECOM_CHANNEL_TEST_BINARY for process tests")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}

	// connect --dry-run succeeds without credentials.
	cmd := exec.Command(bin, "--config-dir", dir, "connect", "--bot-id", "bot-cli", "--dry-run")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), `"dry_run": true`) {
		t.Fatalf("connect --dry-run failed: err=%v out=%s", err, string(out))
	}

	// WECOM_CHANNEL_CLI_READ_ONLY=true blocks real connect.
	cmd = exec.Command(bin, "--config-dir", dir, "connect", "--bot-id", "bot-cli")
	cmd.Env = append(os.Environ(), "WECOM_CHANNEL_CLI_READ_ONLY=true")
	out, err = cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), `"READONLY_BLOCKED"`) {
		t.Fatalf("expected READONLY_BLOCKED on connect under read-only mode, err=%v out=%s", err, string(out))
	}

	// status --check exits non-zero when no daemon is running.
	cmd = exec.Command(bin, "--config-dir", dir, "status", "--check")
	out, err = cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), `"SERVICE_UNHEALTHY"`) {
		t.Fatalf("expected SERVICE_UNHEALTHY from status --check, err=%v out=%s", err, string(out))
	}

	// Save a profile and verify listen --dry-run and serve --dry-run work offline.
	p := config.DefaultProfile("bot-cli")
	p.Channels = []config.ProfileChannel{{ID: "ch-1", Name: "General", Selected: true}}
	if err := config.SaveProfile(dir, p, false); err != nil {
		t.Fatal(err)
	}
	for _, sub := range [][]string{
		{"--config-dir", dir, "listen", "--dry-run"},
		{"--config-dir", dir, "serve", "--dry-run"},
	} {
		cmd = exec.Command(bin, sub...)
		out, err = cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), `"dry_run": true`) {
			t.Fatalf("%v failed: err=%v out=%s", sub, err, string(out))
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}
