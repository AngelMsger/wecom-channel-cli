package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kittransport "github.com/angelmsger/agent-cli-kit/transport"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

func shortPrivateDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/private/tmp", prefix)
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", prefix)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestRelayLegacyConsumerUpgradePreservesQueue pins that a v0.5.0 two-event
// relay queue with legacy consumer digest and legacy broker binding dispatches
// its sealed batch without RELAY_BINDING_CHANGED or WATCH_BINDING_CHANGED.
func TestRelayLegacyConsumerUpgradePreservesQueue(t *testing.T) {
	root := shortPrivateDir(t, "wcc-up-")
	stateDir := filepath.Join(root, "bstate")
	agentDir := filepath.Join(root, "agent")
	adminDir := filepath.Join(root, "admin")
	relayDir := filepath.Join(root, "relay")
	for _, d := range []string{stateDir, agentDir, adminDir, relayDir} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}

	bcfg := config.Default()
	bcfg.Name = "eng"
	bcfg.Bot = "bot-up"
	bcfg.StateDir = stateDir
	bcfg.AgentSocket = filepath.Join(agentDir, "a.sock")
	bcfg.AdminSocket = filepath.Join(adminDir, "o.sock")
	bcfg.WatchBackend = "poll"
	bcfg.Events = []string{"topic_event", "topic_reply_event"}
	bcfg.Channels = []config.Channel{{ID: "ch-up", ReadAll: true, ObserveAll: true}}

	st, err := state.Open(stateDir, bcfg.Bot, "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().Unix()
	if err := st.Update(func(sn *state.Snapshot) error {
		for i, id := range []string{"ev-1", "ev-2"} {
			seq := int64(i + 1)
			sn.Events[id] = callback.Event{
				ID: id, Sequence: seq, Bot: bcfg.Bot, Channel: "ch-up", Post: "p-1",
				Kind: "topic_event", Change: 1, Transport: "poll", ObservationOnly: true,
				ResourceCreator: "alice", ResourceCreatorKind: "user", SenderKind: "unknown",
				Received: now, Created: now,
			}
			sn.NextEventSequence = seq
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	b := broker.New(bcfg, st, &scriptPollAPI{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	srvDone := make(chan error, 1)
	go func() {
		srvDone <- broker.ServeContexts(ctx, []*broker.Broker{b}, "", func() { close(ready) })
	}()
	<-ready

	var gotBatchID string
	var gotMu sync.Mutex
	hookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch relay.Batch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotMu.Lock()
		gotBatchID = batch.ID
		gotMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ack": batch.ID})
	}))
	defer hookSrv.Close()

	rcfg := relay.DefaultConfig()
	rcfg.URL = hookSrv.URL
	rcfg.BatchWindowSeconds = 1
	rcfgPath := filepath.Join(relayDir, "relay.json")
	cfgBytes, _ := json.Marshal(rcfg)
	if err := securefs.WriteAtomic(rcfgPath, cfgBytes); err != nil {
		t.Fatal(err)
	}

	// Build a persisted v0.5.0 state with legacy consumer digest, legacy watch
	// binding, and a pre-sealed 2-event inflight batch whose ID depends on the
	// legacy consumer digest.
	scan, err := broker.NormalizeScan(broker.ScanRequest{WholeChannel: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	legacyConsumer := rcfg.LegacyConsumerDigest(bcfg.AgentSocket, watchScope(scan))
	ledger, _, _ := st.Inbox()
	identity := broker.WatchIdentity{Context: bcfg.Name, Bot: bcfg.Bot, Channel: "ch-up", Ledger: ledger}
	legacyBinding := apiclient.Digest(struct {
		Identity broker.WatchIdentity
		Policy   string
		Whole    bool
		Posts    []string
		Partial  bool
		Filter   string
	}{identity, apiclient.Digest(bcfg), true, []string{}, false, "exclude-self-v1"})

	resp, err := b.Handle(context.Background(), broker.Request{Operation: "event.scan", Scan: &broker.ScanRequest{WholeChannel: true, Limit: 100, Binding: legacyBinding}, DryRun: true}, false)
	if err != nil || len(resp.Scan.Items) != 2 {
		t.Fatalf("failed preparing legacy batch: resp=%+v err=%v", resp, err)
	}
	legacyState := relay.State{Version: 1, Consumer: legacyConsumer}
	if err := legacyState.Collect(*resp.Scan, rcfg, time.Now().Add(-10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !legacyState.Seal(rcfg, time.Now()) || legacyState.Inflight == nil {
		t.Fatal("expected sealed 2-event legacy batch")
	}
	wantBatchID := legacyState.Inflight.Batch.ID

	rstatePath := filepath.Join(relayDir, "state.json")
	rstore, err := relay.Open(rstatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := rstore.Save(legacyState); err != nil {
		rstore.Close()
		t.Fatal(err)
	}
	rstore.Close()

	_, stderr, err := runRootWithRuntime(context.Background(), newProfileRuntime(),
		"--socket", bcfg.AgentSocket,
		"relay", "run", "--whole-channel",
		"--config", rcfgPath, "--state", rstatePath,
		"--max-batches", "1", "--duration", "5s",
	)
	if err != nil {
		t.Fatalf("legacy relay upgrade failed: %v\nstderr=%s", err, stderr)
	}
	gotMu.Lock()
	deliveredID := gotBatchID
	gotMu.Unlock()
	if deliveredID != wantBatchID {
		t.Fatalf("expected original sealed batch ID %s delivered, got %s", wantBatchID, deliveredID)
	}
	cancel()
	<-srvDone
}

// TestServeRetryFailedHonorsOnUncertainSkip verifies that `serve --retry-failed`
// never replays an interrupted `sending` batch under `on_uncertain: skip`; the
// uncertainty handler records it as uncertain instead of duplicating the POST.
func TestServeRetryFailedHonorsOnUncertainSkip(t *testing.T) {
	base := shortPrivateDir(t, "wcc-skip-")
	cfgDir := filepath.Join(base, "cfg")
	dataDir := filepath.Join(base, "d")

	var hookCalls atomic.Int32
	hookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hookCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer hookSrv.Close()
	t.Setenv("TEST_SKIP_KEY", "k1")

	zero := 0
	p := config.DefaultProfile("bot-skip")
	p.DataDir = dataDir
	p.Polling.IntervalSeconds = 30
	p.Webhook = config.ProfileWebhook{
		URL:                hookSrv.URL,
		TokenEnv:           "TEST_SKIP_KEY",
		DeliveryMode:       "http_accepted",
		OnUncertain:        "skip",
		BatchWindowSeconds: 1,
		MinIntervalSeconds: &zero,
		TimeoutSeconds:     5,
	}
	p.Channels = []config.ProfileChannel{{ID: "ch-1", Name: "Ops", Context: "ops", Selected: true}}
	if err := config.SaveProfile(cfgDir, p, false); err != nil {
		t.Fatal(err)
	}

	// Pre-populate ledger + delivery.json with a batch stuck in `sending`.
	slug := config.ChannelStateSlug(p.Bot, "ch-1")
	chDir := filepath.Join(dataDir, "channels", slug)
	st, err := state.OpenRelaxed(chDir, p.Bot, "ch-1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if err := st.Update(func(sn *state.Snapshot) error {
		sn.Events["ev-1"] = callback.Event{
			ID: "ev-1", Sequence: 1, Bot: p.Bot, Channel: "ch-1", Post: "p-1",
			Kind: "topic_event", Change: 1, Transport: "poll", ObservationOnly: true,
			ResourceCreator: "alice", ResourceCreatorKind: "user", SenderKind: "unknown",
			Received: now, Created: now,
		}
		sn.NextEventSequence = 1
		return nil
	}); err != nil {
		st.Close()
		t.Fatal(err)
	}
	bcfg := buildChannelBrokerConfig(p, p.Channels[0], "ops", chDir, "", "")
	b := broker.New(bcfg, st, &scriptPollAPI{}, nil)
	resp, err := b.Handle(context.Background(), broker.Request{Operation: "event.scan", Scan: &broker.ScanRequest{WholeChannel: true, Limit: 100}, DryRun: true}, false)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	st.Close()

	rcfg, err := buildServeRelayConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	consumer := rcfg.ConsumerDigest("ops", watchScope(broker.ScanRequest{WholeChannel: true, Limit: 100}))
	rst := relay.State{Version: 1, Consumer: consumer, DeliveryMode: relay.HTTPAccepted}
	if err := rst.Collect(*resp.Scan, rcfg, time.Now().Add(-10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !rst.Seal(rcfg, time.Now()) {
		t.Fatal("failed to seal batch")
	}
	rst.Inflight.Attempts = 1
	rst.Inflight.Status = "sending"
	rst.Inflight.LastAttempt = time.Now().UnixMilli()

	deliveryPath := filepath.Join(chDir, "delivery.json")
	rstore, err := relay.OpenRelaxed(deliveryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := rstore.Save(rst); err != nil {
		rstore.Close()
		t.Fatal(err)
	}
	rstore.Close()

	creds := newMemProfileCreds()
	_, _ = creds.Save("bot-skip", "sec")
	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client:      func(string, string, transport.Config) (apiclient.Client, error) { return &scriptPollAPI{}, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, runErr := runRootWithRuntime(ctx, rt, "--config-dir", cfgDir, "serve", "--retry-failed")
		done <- runErr
	}()

	waitFor(t, 8*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		return err == nil && strings.Contains(out, `"uncertain_batches": 1`)
	}, "sending batch skipped as uncertain rather than re-sent")

	cancel()
	<-done
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("expected 0 duplicate webhook sends for interrupted sending batch under skip, got %d", got)
	}
}

// TestServePreservesForeignContextsAndKeepsOneRunOverridesEphemeral covers:
// - foreign context name collision preservation (both auto-avoidance and explicit CONTEXT_CONFLICT)
// - one-run --webhook-url overrides not saved to profile.yaml without --save
// - --strict-fs creating generated socket directories on a fresh data_dir
func TestServePreservesForeignContextsOverridesAndStrictFS(t *testing.T) {
	base := shortPrivateDir(t, "wcc-iso-")
	cfgDir := filepath.Join(base, "cfg")
	if err := os.Mkdir(cfgDir, 0700); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(base, "d")

	// Pre-populate config.yaml with a foreign `engineering` context owned by another bot.
	foreignSock := filepath.Join(base, "foreign.sock")
	if err := config.SaveClients(cfgDir, config.ClientFile{
		Current: "engineering",
		Contexts: []config.ClientContext{{
			Name: "engineering", Server: apiclient.Origin, Socket: foreignSock, Bot: "bot-other", Channel: "ch-other",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Profile for bot-mine with an auto-named channel also called "Engineering".
	p := config.DefaultProfile("bot-mine")
	p.DataDir = dataDir
	p.Polling.IntervalSeconds = 30
	p.Channels = []config.ProfileChannel{{ID: "ch-mine", Name: "Engineering", Selected: false}}
	if err := config.SaveProfile(cfgDir, p, false); err != nil {
		t.Fatal(err)
	}

	creds := newMemProfileCreds()
	_, _ = creds.Save("bot-mine", "sec-mine")
	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client:      func(string, string, transport.Config) (apiclient.Client, error) { return &scriptPollAPI{}, nil },
	}

	// 1. One-run --webhook-url with missing token env (NO --save) must fail AND
	// must NOT persist webhook URL into profile.yaml.
	_, _, err := runRootWithRuntime(context.Background(), rt,
		"--config-dir", cfgDir, "serve",
		"--channel", "Engineering",
		"--webhook-url", "https://ephemeral.example.invalid/hook",
		"--webhook-token-env", "UNSET_WEBHOOK_TOKEN_FOR_TEST",
	)
	if err == nil {
		t.Fatal("expected serve to fail on missing webhook token env")
	}
	loaded, _, err := config.LoadProfile(cfgDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Webhook.URL != "" {
		t.Fatalf("one-run --webhook-url leaked into saved profile.yaml without --save: %q", loaded.Webhook.URL)
	}

	// 2. Run `serve --strict-fs --channel Engineering` on a fresh data_dir:
	//    - must create strict socket dirs cleanly on first startup
	//    - must NOT overwrite foreign `engineering` context in config.yaml
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, runErr := runRootWithRuntime(ctx, rt, "--config-dir", cfgDir, "serve", "--strict-fs", "--channel", "Engineering")
		done <- runErr
	}()

	waitFor(t, 8*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		return err == nil && strings.Contains(out, `"state": "healthy"`)
	}, "serve --strict-fs healthy on fresh data_dir")

	clients, err := config.ReadClients(cfgDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(clients.Contexts) != 2 {
		t.Fatalf("expected 2 distinct contexts (foreign + mine), got %+v", clients.Contexts)
	}
	for _, c := range clients.Contexts {
		if c.Name == "engineering" && (c.Bot != "bot-other" || c.Channel != "ch-other" || c.Socket != foreignSock) {
			t.Fatalf("foreign engineering context was hijacked: %+v", c)
		}
	}
	cancel()
	<-done

	// 3. Explicit colliding context name in profile.yaml must fail with CONTEXT_CONFLICT.
	p.Channels[0].Selected = true
	p.Channels[0].Context = "engineering"
	if err := config.SaveProfile(cfgDir, p, false); err != nil {
		t.Fatal(err)
	}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	if _, _, err := runRootWithRuntime(ctx3, rt, "--config-dir", cfgDir, "serve"); e.Safe(err).Code != "CONTEXT_CONFLICT" {
		t.Fatalf("expected CONTEXT_CONFLICT on explicit foreign context collision, got %v", err)
	}
}

// TestConnectRenamePreservesStableContextAndDeliveryBinding verifies that
// renaming a channel remotely and re-running `connect` keeps its assigned
// context name so `serve` resumes `delivery.json` without RELAY_BINDING_CHANGED.
func TestConnectRenamePreservesStableContextAndDeliveryBinding(t *testing.T) {
	base := shortPrivateDir(t, "wcc-ren-")
	cfgDir := filepath.Join(base, "cfg")
	dataDir := filepath.Join(base, "d")

	var mu sync.Mutex
	topicName := "Alpha"
	jsonResp := func(payload any) *http.Response {
		b, _ := json.Marshal(payload)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}
	}
	fakeWeCom := doerFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/cgi-bin/aibot/get_token":
			return jsonResp(map[string]any{"errcode": 0, "access_token": "tok"}), nil
		case "/cgi-bin/aibot/topic/get_topic_list":
			mu.Lock()
			name := topicName
			mu.Unlock()
			return jsonResp(map[string]any{
				"errcode": 0, "has_more": false,
				"topic_list": []map[string]any{{"topic_id": "top-1", "topic_name": name, "create_time": 1700000001}},
			}), nil
		default:
			return jsonResp(map[string]any{"errcode": 0, "has_more": false, "post_list": []any{}, "reply_list": []any{}}), nil
		}
	})

	hookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer hookSrv.Close()
	t.Setenv("TEST_REN_KEY", "k1")
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "secret-ren")

	creds := newMemProfileCreds()
	rt := profileRuntime{
		credentials: func(string) profileCredentials { return creds },
		client: func(bot, secret string, tcfg transport.Config) (apiclient.Client, error) {
			return apiclient.NewWithOptions(bot, secret, apiclient.Options{
				Transport: tcfg,
				HTTP:      kittransport.New(kittransport.Options{Doer: fakeWeCom, MaxRetries: 0}),
			})
		},
	}

	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "connect", "--bot-id", "bot-ren"); err != nil {
		t.Fatalf("initial connect failed: %v", err)
	}
	p, _, err := config.LoadProfile(cfgDir, false)
	if err != nil || len(p.Channels) != 1 || p.Channels[0].Context != "alpha" {
		t.Fatalf("expected persisted context alpha after connect, got %+v err=%v", p.Channels, err)
	}

	// Start serve once with webhook to create delivery.json bound to "alpha".
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() {
		_, _, runErr := runRootWithRuntime(ctx1, rt, "--config-dir", cfgDir, "serve",
			"--channel", "Alpha", "--data-dir", dataDir,
			"--webhook-url", hookSrv.URL, "--webhook-token-env", "TEST_REN_KEY", "--save",
		)
		done1 <- runErr
	}()
	waitFor(t, 8*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		return err == nil && strings.Contains(out, `"state": "healthy"`)
	}, "initial serve healthy")
	cancel1()
	<-done1

	// Rename the channel remotely and refresh with `connect`.
	mu.Lock()
	topicName = "Alpha Renamed"
	mu.Unlock()
	if _, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "connect"); err != nil {
		t.Fatalf("refresh connect after rename failed: %v", err)
	}
	p2, _, err := config.LoadProfile(cfgDir, false)
	if err != nil || p2.Channels[0].Name != "Alpha Renamed" || p2.Channels[0].Context != "alpha" {
		t.Fatalf("expected stable context alpha preserved across rename, got %+v err=%v", p2.Channels, err)
	}

	// Restart serve: must resume delivery.json without RELAY_BINDING_CHANGED!
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		_, _, runErr := runRootWithRuntime(ctx2, rt, "--config-dir", cfgDir, "serve")
		done2 <- runErr
	}()
	waitFor(t, 8*time.Second, func() bool {
		out, _, err := runRootWithRuntime(context.Background(), rt, "--config-dir", cfgDir, "status")
		return err == nil && strings.Contains(out, `"state": "healthy"`)
	}, "restarted serve healthy after channel rename")
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("serve failed after channel rename: %v", err)
	}
}

// TestStatusCheckFailsOnOverdueOrCorruptSnapshot verifies that a held lock
// without a timely or valid status.json fails `status --check`.
func TestStatusCheckFailsOnOverdueOrCorruptSnapshot(t *testing.T) {
	base := shortPrivateDir(t, "wcc-st-")
	cfgDir := filepath.Join(base, "cfg")
	dataDir := filepath.Join(base, "d")
	if err := os.Mkdir(cfgDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}

	lockPath := filepath.Join(dataDir, "serve.lock")
	lock, err := securefs.LockPrivate(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	// 1. Missing status.json with old serve.lock (> startup grace) -> status_overdue.
	old := time.Now().Add(-5 * time.Minute)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatal(err)
	}
	_, _, err = runRootWithRuntime(context.Background(), newProfileRuntime(), "--config-dir", cfgDir, "status", "--data-dir", dataDir, "--check")
	if e.Safe(err).Code != "SERVICE_UNHEALTHY" || !strings.Contains(err.Error(), "status_overdue") {
		t.Fatalf("expected status_overdue when snapshot missing past grace, got %v", err)
	}

	// 2. Corrupt status.json -> status_unreadable.
	statusPath := filepath.Join(dataDir, "status.json")
	if err := os.WriteFile(statusPath, []byte("{corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err = runRootWithRuntime(context.Background(), newProfileRuntime(), "--config-dir", cfgDir, "status", "--data-dir", dataDir, "--check")
	if e.Safe(err).Code != "SERVICE_UNHEALTHY" || !strings.Contains(err.Error(), "status_unreadable") {
		t.Fatalf("expected status_unreadable on corrupt snapshot, got %v", err)
	}

	// 3. Valid status.json whose updated_at stopped advancing -> status_stale.
	stale := ServiceStatus{
		Version:   1,
		PID:       os.Getpid(),
		Bot:       "bot-1",
		StartedAt: time.Now().Add(-20 * time.Minute).UnixMilli(),
		UpdatedAt: time.Now().Add(-15 * time.Minute).UnixMilli(),
	}
	b, _ := json.Marshal(stale)
	if err := os.WriteFile(statusPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err = runRootWithRuntime(context.Background(), newProfileRuntime(), "--config-dir", cfgDir, "status", "--data-dir", dataDir, "--check", "--max-stale", "10m")
	if e.Safe(err).Code != "SERVICE_UNHEALTHY" || !strings.Contains(err.Error(), "status_stale") {
		t.Fatalf("expected status_stale on overdue UpdatedAt, got %v", err)
	}
}
