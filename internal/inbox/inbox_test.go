package inbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func batch(ids ...int) relay.Batch {
	b := relay.Batch{Version: 1, Identity: broker.WatchIdentity{Context: "daily-ai", Bot: "bot", Channel: "channel", Ledger: strings.Repeat("a", 32)}, Coverage: broker.WatchCoverage{Backend: "callback"}}
	t := relay.Thread{Post: "post"}
	for _, id := range ids {
		v := callback.Event{ID: fmt.Sprint(id), Sequence: int64(id), Bot: "bot", Channel: "channel", Post: "post", Kind: "topic_event", Change: 1, ObservationOnly: true, Sender: "human", SenderKind: "user"}
		t.Events = append(t.Events, relay.Record{Trust: "untrusted", Warning: apiclient.Warning, Source: apiclient.Source{Service: "wecom", Context: "daily-ai", Channel: "channel", Post: "post", Event: v.ID}, Data: v})
	}
	b.Threads = []relay.Thread{t}
	b.ID = apiclient.Digest(b)
	return b
}

func openTest(t *testing.T) (*SQLite, string) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "inbox.sqlite")
	s, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestReceiptDeduplicationRestartAndIsolation(t *testing.T) {
	ctx := context.Background()
	s, path := openTest(t)
	b := batch(1, 2)
	if err := s.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	// A different batch overlaps existing event IDs; only event 3 is work.
	overlap := batch(2, 3)
	if err = s.Accept(ctx, overlap); err != nil {
		t.Fatal(err)
	}
	v, err := s.Get(ctx, overlap.ID)
	if err != nil || v.NewEvents != 1 || len(v.Batch.Threads[0].Events) != 1 || v.Batch.Threads[0].Events[0].Data.ID != "3" || len(v.Original.Threads[0].Events) != 2 {
		t.Fatal(v, err)
	}
	allDuplicate := batch(1)
	if err = s.Accept(ctx, allDuplicate); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, allDuplicate.ID, false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	status, err := s.Status(ctx)
	if err != nil || status.ReceivedBatches != 3 || status.ReceivedEvents != 3 || status.States[Received] != 2 || status.States[Duplicate] != 1 {
		t.Fatal(status, err)
	}
	// Batch ID reuse with a different payload must never confirm success.
	b.Coverage.Note = "changed"
	if err = s.Accept(ctx, b); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	changed := batch(1)
	changed.Threads[0].Events[0].Data.Change = 2
	changed.ID = apiclient.Digest(changed)
	if err = s.Accept(ctx, changed); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	other := batch(4)
	other.Identity.Context, other.Identity.Channel = "knowledge-lab", "other-channel"
	other.Threads[0].Events[0].Data.Channel = "other-channel"
	other.Threads[0].Events[0].Source.Context, other.Threads[0].Events[0].Source.Channel = "knowledge-lab", "other-channel"
	if err = s.Accept(ctx, other); e.Safe(err).Code != "INBOX_BINDING_CHANGED" {
		t.Fatal(err)
	}
	second, _ := openTest(t)
	if err = second.Accept(ctx, other); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if got, err := ro.Status(ctx); err != nil || got.ReceivedEvents != 3 {
		t.Fatal(got, err)
	}
	items, more, err := ro.List(ctx, "", 0, 2)
	if err != nil || !more || len(items) != 2 {
		t.Fatal(items, more, err)
	}
	items, more, err = ro.List(ctx, "", items[1].Sequence, 2)
	if err != nil || more || len(items) != 1 || items[0].Status != Duplicate {
		t.Fatal(items, more, err)
	}
}

func TestCommitFailureRollsBackReceiptAndDedupKeys(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	// Fail after inserting the batch, inside the same transaction as event keys.
	_, err := s.db.Exec("CREATE TRIGGER fail_insert BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END")
	if err != nil {
		t.Fatal(err)
	}
	b := batch(1)
	if s.Accept(ctx, b) == nil {
		t.Fatal("confirmed failed storage")
	}
	v, err := s.Status(ctx)
	if err != nil || v.ReceivedBatches != 0 || v.Identity != nil {
		t.Fatal(v, err)
	}
	if _, err = s.db.Exec("DROP TRIGGER fail_insert"); err != nil {
		t.Fatal(err)
	}
	if err = s.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Queue saturation leaves the next batch pending at the relay, with no receipt.
	if _, err = s.db.Exec("UPDATE batches SET new_events=?", maxPendingEvents); err != nil {
		t.Fatal(err)
	}
	if err = s.Accept(ctx, batch(2)); e.Safe(err).Code != "INBOX_FULL" {
		t.Fatal(err)
	}
	if err = s.Accept(ctx, b); err != nil {
		t.Fatal("full inbox rejected existing receipt", err)
	}
	if _, err = s.Get(ctx, batch(2).ID); e.Safe(err).Code != "INBOX_MISSING" {
		t.Fatal(err)
	}
}

func TestConcurrentReceiptsAndClaimsNeverRepeatHostWork(t *testing.T) {
	s, path := openTest(t)
	other, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx := context.Background()
	b := batch(1)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := s
			if i%2 == 0 {
				db = other
			}
			errs <- db.Accept(ctx, b)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan Item, 2)
	for _, db := range []*SQLite{s, other} {
		wg.Add(1)
		go func(db *SQLite) {
			defer wg.Done()
			v, err := db.Claim(ctx, b.ID, false)
			if err == nil {
				results <- v
			} else if e.Safe(err).Code != "INBOX_CONFLICT" {
				t.Error(err)
			}
		}(db)
	}
	wg.Wait()
	close(results)
	if len(results) != 1 {
		t.Fatal("multiple host claims", len(results))
	}
	claim := <-results
	if claim.Attempt == "" || claim.Status != Dispatching {
		t.Fatal(claim)
	}
	s.Close()
	other.Close()
	restarted, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err = restarted.Claim(ctx, b.ID, false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal("restart reclaimed ambiguous attempt", err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, "stale", Submitted, "job-1", false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, claim.Attempt, Uncertain, "timeout-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Claim(ctx, b.ID, false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, claim.Attempt, "not_submitted", "host-audit-1", false); err != nil {
		t.Fatal(err)
	}
	newClaim, err := restarted.Claim(ctx, b.ID, false)
	if err != nil || newClaim.Attempt == claim.Attempt {
		t.Fatal(newClaim, err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, newClaim.Attempt, Submitted, "job-2", false); err != nil {
		t.Fatal(err)
	}
	status, _ := restarted.Status(ctx)
	if status.States[Submitted] != 1 || status.States[Completed] != 0 {
		t.Fatal("acceptance became completion", status)
	}
	if _, err = restarted.Resolve(ctx, b.ID, newClaim.Attempt, "not_submitted", "unverified", false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal(err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, newClaim.Attempt, Completed, "completion-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Resolve(ctx, b.ID, newClaim.Attempt, Completed, "completion-2", false); err != nil {
		t.Fatal("result retry not idempotent", err)
	}
	if err = restarted.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Claim(ctx, b.ID, false); e.Safe(err).Code != "INBOX_CONFLICT" {
		t.Fatal("receipt reset completed work", err)
	}
}

func TestDryTransitionDoesNotWrite(t *testing.T) {
	s, path := openTest(t)
	ctx := context.Background()
	b := batch(1)
	if err := s.Accept(ctx, b); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if v, err := ro.Claim(ctx, b.ID, true); err != nil || v.Status != Dispatching || v.Attempt != "" {
		t.Fatal(v, err)
	}
	if v, _ := s.Get(ctx, b.ID); v.Status != Received {
		t.Fatal("dry-run changed state", v)
	}
	claim, err := s.Claim(ctx, b.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ro.Resolve(ctx, b.ID, claim.Attempt, Submitted, "job", true); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get(ctx, b.ID); v.Status != Dispatching {
		t.Fatal("dry-run changed state", v)
	}
}

func TestHTTPReceiptProtocolAndFailure(t *testing.T) {
	s, path := openTest(t)
	h, err := Handler(s, "synthetic-secret")
	if err != nil {
		t.Fatal(err)
	}
	b := batch(1)
	raw, _ := json.Marshal(b)
	request := func(handler http.Handler, body []byte, key, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/batches", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for i := 0; i < 2; i++ {
		w := request(h, raw, b.ID, "synthetic-secret")
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"ack":"`+b.ID+`"}` {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		raw        []byte
		key, token string
		code       int
	}{
		{raw, b.ID, "wrong", 401}, {raw, "mismatch", "synthetic-secret", 400},
		{bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), b.ID, "synthetic-secret", 400},
		{bytes.Replace(raw, []byte(`"trust":"untrusted"`), []byte(`"trust":"trusted"`), 1), b.ID, "synthetic-secret", 400},
		{append(raw, []byte(`{}`)...), b.ID, "synthetic-secret", 400},
		{bytes.Repeat([]byte("x"), maxBatchBytes+1), b.ID, "synthetic-secret", 413},
	} {
		w := request(h, tc.raw, tc.key, tc.token)
		if w.Code != tc.code || strings.Contains(w.Body.String(), "ack") || strings.Contains(w.Body.String(), "synthetic-secret") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	ro, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	readOnlyHandler, _ := Handler(ro, "synthetic-secret")
	b2 := batch(2)
	raw2, _ := json.Marshal(b2)
	w := request(readOnlyHandler, raw2, b2.ID, "synthetic-secret")
	if w.Code != 503 || strings.Contains(w.Body.String(), "ack") {
		t.Fatal("acknowledged failed persistence", w.Code, w.Body.String())
	}
}

func TestLostHTTPAckCanReplayAfterReceiverRestart(t *testing.T) {
	s, path := openTest(t)
	b := batch(1)
	dropped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := s.Accept(r.Context(), b); err != nil {
			t.Error(err)
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	cfg := relay.DefaultConfig()
	cfg.URL = dropped.URL
	hook, err := relay.NewWebhook(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = hook.Send(context.Background(), b); e.Safe(err).Code != "RELAY_UNREACHABLE" {
		t.Fatal(err)
	}
	dropped.Close()
	s.Close()
	restarted, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	h, _ := Handler(restarted, "synthetic-secret")
	server := httptest.NewServer(h)
	defer server.Close()
	t.Setenv("TEST_INBOX_TOKEN", "synthetic-secret")
	cfg.URL, cfg.TokenEnv = server.URL+"/batches", "TEST_INBOX_TOKEN"
	hook, err = relay.NewWebhook(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = hook.Send(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	v, err := restarted.Status(context.Background())
	if err != nil || v.ReceivedBatches != 1 || v.ReceivedEvents != 1 || v.States[Received] != 1 {
		t.Fatal(v, err)
	}
}

func TestCrashRecovery(t *testing.T) {
	if path := os.Getenv("WECOM_TEST_INBOX_CRASH"); path != "" {
		s, err := Open(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("PRAGMA cache_size=1"); err != nil {
			t.Fatal(err)
		}
		tx, err := s.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		// Spill dirty pages to force a hot rollback journal, then exit without
		// rollback/close. Recovery must restore the previously acknowledged batch.
		if _, err = tx.Exec("UPDATE batches SET original=zeroblob(1000000)"); err != nil {
			t.Fatal(err)
		}
		os.Exit(99)
	}
	s, path := openTest(t)
	b := batch(1)
	if err := s.Accept(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	s.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
	cmd.Env = append(os.Environ(), "WECOM_TEST_INBOX_CRASH="+path)
	out, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
		t.Fatalf("child: %v %s", err, out)
	}
	recovered, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	v, err := recovered.Get(context.Background(), b.ID)
	if err != nil || v.NewEvents != 1 || v.Original.ID != b.ID {
		t.Fatal(v, err)
	}
	if err = recovered.Accept(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}
