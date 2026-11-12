package relay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestHTTPAcceptanceContract(t *testing.T) {
	for _, status := range []int{200, 201, 202, 204, 302, 401, 429, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("Idempotency-Key") != "batch" {
				t.Error("headers")
			}
			w.Header().Set("Content-Length", "1000000")
			w.WriteHeader(status)
			_, _ = w.Write([]byte("not an acknowledgment"))
		}))
		c := DefaultConfig()
		c.URL = srv.URL
		c.TokenEnv = "TEST_GROK_TOKEN"
		c.DeliveryMode = HTTPAccepted
		t.Setenv(c.TokenEnv, "synthetic")
		h, err := NewWebhook(c)
		if err != nil {
			t.Fatal(err)
		}
		err = h.Send(context.Background(), Batch{ID: "batch"})
		srv.Close()
		if status == 200 {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil || e.Safe(err).HTTPStatus != status {
			t.Fatalf("status %d: %v", status, err)
		}
	}
	// A server can start a run and close the connection before returning headers.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		c, _, _ := w.(http.Hijacker).Hijack()
		_ = c.Close()
	}))
	defer srv.Close()
	c := DefaultConfig()
	c.URL = srv.URL
	c.TokenEnv = "TEST_GROK_TOKEN"
	c.DeliveryMode = HTTPAccepted
	t.Setenv(c.TokenEnv, "synthetic")
	h, _ := NewWebhook(c)
	err := h.Send(context.Background(), Batch{ID: "batch"})
	if err == nil || e.Safe(err).Code != "RELAY_OUTCOME_UNKNOWN" || Retryable(err) || calls.Load() != 1 {
		t.Fatal(err, calls.Load())
	}
}

func TestLegacyConfigDigestIsStable(t *testing.T) {
	c := DefaultConfig()
	c.URL = "https://example.invalid/events"
	b, _ := json.Marshal(c)
	const legacy = `{"version":1,"webhook_url":"https://example.invalid/events","kinds":["topic_event","topic_reply_event"],"changes":[1,2,3],"exclude_self":true,"batch_window_seconds":5,"batch_size":50,"queue_limit":1000,"timeout_seconds":30,"retry_limit":0,"retry_base_seconds":5}`
	if string(b) != legacy || c.Mode() != ExplicitAck {
		t.Fatal(string(b))
	}
	c.DeliveryMode = "arbitrary"
	if c.Validate() == nil {
		t.Fatal("unknown delivery mode")
	}
	c.DeliveryMode = HTTPAccepted
	if c.Validate() == nil {
		t.Fatal("missing bearer source")
	}
}

func TestWorkerClaimDedupCompletionAndRecovery(t *testing.T) {
	st, p, c := fixture()
	_ = st.Collect(p, c, time.Now())
	st.Seal(c, time.Now().Add(time.Minute))
	b := st.Inflight.Batch
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	path := filepath.Join(dir, "worker.json")
	worker, _, err := ReadConsumer(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.ClaimBatch(b, time.Now())
	if err != nil || !result.Claimed || result.Token == "" || result.Work == nil {
		t.Fatal(result, err)
	}
	if err = store.SaveConsumer(worker); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path); err == nil {
		other.Close()
		t.Fatal("concurrent claim lock")
	}
	store.Close()
	worker, _, err = ReadConsumer(path)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := worker.ClaimBatch(b, time.Now())
	if err != nil || repeat.Claimed || repeat.Token != "" || repeat.Work != nil || repeat.Status != "claimed" {
		t.Fatal(repeat, err)
	}
	if err = worker.Complete(b.ID, "wrong", "processed", time.Now()); err == nil {
		t.Fatal("wrong claimant")
	}
	if err = worker.Complete(b.ID, result.Token, "processed", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = worker.Complete(b.ID, result.Token, "processed", time.Now()); err != nil {
		t.Fatal("completion not idempotent", err)
	}
	if worker.Complete(b.ID, result.Token, "failed", time.Now()) == nil {
		t.Fatal("rewrote completion")
	}
	// Another batch containing the same immutable events creates no business work.
	overlap := b
	overlap.ID = apiclient.Digest("overlap")
	duplicate, err := worker.ClaimBatch(overlap, time.Now())
	if err != nil || duplicate.Claimed || duplicate.Status != "duplicate" {
		t.Fatal(duplicate, err)
	}
	// Contexts cannot share a consumption file, even when IDs match.
	cross := b
	cross.Identity.Context = "other"
	for i := range cross.Threads {
		cross.Threads[i].Events = append([]Record(nil), cross.Threads[i].Events...)
		for j := range cross.Threads[i].Events {
			cross.Threads[i].Events[j].Source.Context = "other"
		}
	}
	if _, err = worker.ClaimBatch(cross, time.Now()); err == nil {
		t.Fatal("context rebound")
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.SaveConsumer(worker); err != nil {
		t.Fatal(err)
	}
	// Failed persistence is not a successful claim, and cannot overwrite corruption.
	_ = os.WriteFile(path, []byte(`{}`), 0600)
	if store.SaveConsumer(worker) == nil {
		t.Fatal("overwrote concurrent modification")
	}
}

func TestAcceptedReceiptRetainsRecoverablePayload(t *testing.T) {
	st, p, c := fixture()
	st.DeliveryMode = HTTPAccepted
	_ = st.Collect(p, c, time.Now())
	st.Seal(c, time.Now().Add(time.Minute))
	st.Inflight.Attempts = 1
	st.Inflight.Status = "sending"
	batch := st.Inflight.Batch
	if err := st.Accept(time.Now(), "http_200"); err != nil {
		t.Fatal(err)
	}
	if err := st.Validate(); err != nil {
		t.Fatal(err)
	}
	if st.Acknowledged != 0 || st.Depth() != 0 || st.AcceptedEvents != 3 || apiclient.Digest(st.Receipts[0].Batch) != apiclient.Digest(batch) {
		t.Fatal("accepted conflated with completion")
	}
	st.Receipts[0].Batch.ID = strings.Repeat("0", 64)
	if st.Validate() == nil {
		t.Fatal("corrupted receipt")
	}
}

func TestConsumerConflictFailureAndCapacityNeverGrantWork(t *testing.T) {
	st, p, c := fixture()
	_ = st.Collect(p, c, time.Now())
	st.Seal(c, time.Now().Add(time.Minute))
	batch := st.Inflight.Batch
	worker := ConsumerState{Version: 1, Batches: map[string]Claim{}, Events: map[string]string{}}
	claim, err := worker.ClaimBatch(batch, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Complete(batch.ID, claim.Token, "failed", time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err := worker.ClaimBatch(batch, time.Now())
	if err != nil || result.Claimed || result.Status != "failed" {
		t.Fatal(result, err)
	}
	original := apiclient.Digest(worker)
	batch.Coverage.Note = "conflicting body under same ID"
	if _, err = worker.ClaimBatch(batch, time.Now()); err == nil || apiclient.Digest(worker) != original {
		t.Fatal("mutated conflicting claim")
	}
	batch.ID = apiclient.Digest("new batch")
	for len(worker.Batches) < 10000 {
		id := apiclient.Digest(len(worker.Batches))
		worker.Batches[id] = Claim{Digest: id, Status: "duplicate", ClaimedAt: time.Now().UnixMilli()}
	}
	original = apiclient.Digest(worker)
	if _, err = worker.ClaimBatch(batch, time.Now()); err == nil || e.Safe(err).Code != "RELAY_CONSUMER_FULL" || apiclient.Digest(worker) != original {
		t.Fatal("capacity discarded deduplication", err)
	}
}

func TestAcceptedWebhookDoesNotReplayOnReusedConnection(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		if calls.Load() == 2 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := DefaultConfig()
	c.DeliveryMode = HTTPAccepted
	c.URL = srv.URL
	c.TokenEnv = "TEST_GROK_TOKEN"
	t.Setenv(c.TokenEnv, "synthetic")
	h, err := NewWebhook(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Send(context.Background(), Batch{ID: "first"}); err != nil {
		t.Fatal(err)
	}
	err = h.Send(context.Background(), Batch{ID: "second"})
	if err == nil || e.Safe(err).Code != "RELAY_OUTCOME_UNKNOWN" || calls.Load() != 2 {
		t.Fatalf("POST replayed by HTTP transport: calls=%d err=%v", calls.Load(), err)
	}
}
