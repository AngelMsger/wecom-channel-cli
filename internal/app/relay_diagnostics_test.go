package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestRelayScanExhaustionDiagnosticsSurviveDiskAndRestart(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.json")
	store, err := relay.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st := relay.State{Version: 1, Consumer: apiclient.Digest("consumer"), FromNow: true}
	cfg := relay.DefaultConfig()
	cause := e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "secret-message").WithDetails(map[string]any{"context": "secret-context"})
	notices := 0
	notice := func(reason string, s relay.State, code string) {
		b, _ := json.Marshal(relayStatus(s))
		if reason != "failed" || code != "RELAY_RETRIES_EXHAUSTED" || !strings.Contains(string(b), "BROKER_UNREACHABLE") || strings.Contains(string(b), "secret") {
			t.Fatal(reason, code, string(b))
		}
		notices++
	}
	err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return broker.ScanPage{}, cause },
		func(context.Context, relay.Batch) error { t.Fatal("sent without scan"); return nil }, store.Save, notice)
	store.Close()
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" {
		t.Fatal(err)
	}
	details, _ := json.Marshal(e.Safe(err).Details)
	if !strings.Contains(string(details), `"phase":"scan"`) || !strings.Contains(string(details), "BROKER_UNREACHABLE") {
		t.Fatal(string(details))
	}
	saved, exists, err := relay.Read(path)
	if err != nil || !exists || saved.ScanFailures != 1 || saved.LastScanError == nil {
		t.Fatal(saved, err)
	}
	r := saved.LastScanError
	if r.Code != "BROKER_UNREACHABLE" || r.Attempt != 1 || r.RetryLimit != 0 || !r.Retryable || r.At <= 0 || saved.NextScan <= r.At {
		t.Fatal(r, saved.NextScan)
	}
	err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			t.Fatal("restart reset scan budget")
			return broker.ScanPage{}, nil
		}, nil, nil, notice)
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" || notices != 2 {
		t.Fatal(err, notices)
	}
	var out, diag bytes.Buffer
	if code := Execute([]string{"relay", "status", "--state", path}, strings.NewReader(""), &out, &diag); code != 0 || !strings.Contains(out.String(), "BROKER_UNREACHABLE") || strings.Contains(out.String(), "secret") {
		t.Fatal(code, out.String(), diag.String())
	}
}

func TestRelayIndependentScanAndDeliveryBudgets(t *testing.T) {
	for _, phase := range []string{"scan", "delivery"} {
		for _, retries := range []int{0, 1} {
			t.Run(phase+string(rune('0'+retries)), func(t *testing.T) {
				st, p, cfg := relayFixture()
				if phase == "scan" {
					st = relay.State{Version: 1, Consumer: apiclient.Digest("consumer"), FromNow: true}
				}
				cfg.RetryLimit = 20 // An explicit zero must override the inherited budget.
				zero := 0
				cfg.ScanRetryLimit, cfg.DeliveryRetryLimit = &zero, &zero
				if phase == "scan" {
					cfg.ScanRetryLimit = &retries
				} else {
					cfg.DeliveryRetryLimit = &retries
				}
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
				defer cancel()
				var saved relay.State
				retried := false
				err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
					func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
						if phase == "scan" {
							return broker.ScanPage{}, e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "offline")
						}
						return p, nil
					},
					func(context.Context, relay.Batch) error {
						return e.New(e.CategoryServer, "RELAY_REJECTED", "busy").WithHTTPStatus(503)
					},
					func(s relay.State) error { saved = copyRelayState(t, s); return s.Validate() },
					func(reason string, _ relay.State, _ string) { retried = retried || reason == phase+"_retry" })
				if retries == 0 && e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" || retries == 1 && err != nil || retried != (retries == 1) {
					t.Fatal(err, retried)
				}
				r := saved.LastScanError
				if phase == "delivery" {
					r = saved.LastDeliveryError
				}
				if r == nil || r.RetryLimit != retries || r.Attempt != 1 {
					t.Fatal(r)
				}
			})
		}
	}
}

func TestRelayScanThrottlePersistsAndCancellationBoundsWait(t *testing.T) {
	st := relay.State{Version: 1, Consumer: apiclient.Digest("consumer"), FromNow: true}
	cfg := relay.DefaultConfig()
	limit := 1
	cfg.ScanRetryLimit = &limit
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	calls := 0
	var saved relay.State
	err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			calls++
			return broker.ScanPage{}, e.New(e.CategoryServer, "HTTP_REJECTED", "rate limited").WithHTTPStatus(429).WithDetails(apiclient.FailureMetadata{RetryAfterSeconds: 900})
		}, nil, func(s relay.State) error { saved = copyRelayState(t, s); return s.Validate() }, func(string, relay.State, string) {})
	if err != nil || calls != 1 || saved.NextScan-saved.LastScanError.At < int64((15*time.Minute)/time.Millisecond) {
		t.Fatal(err, calls, saved)
	}
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	err = pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			t.Fatal("restart ignored throttle")
			return broker.ScanPage{}, nil
		}, nil, nil, func(string, relay.State, string) {})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRelayQueueAgeSurvivesSealingAndRetry(t *testing.T) {
	st, page, cfg := relayFixture()
	enqueued := time.Now().Add(-time.Hour)
	if err := st.Collect(page, cfg, enqueued); err != nil {
		t.Fatal(err)
	}
	if got := relayStatus(st)["oldest_queued_at"]; got != enqueued.UnixMilli() {
		t.Fatal(got)
	}
	if !st.Seal(cfg, time.Now()) {
		t.Fatal("batch not sealed")
	}
	st.Inflight.Attempts, st.Inflight.LastAttempt = 1, time.Now().UnixMilli()
	st = copyRelayState(t, st)
	if got := relayStatus(st)["oldest_queued_at"]; got != enqueued.UnixMilli() {
		t.Fatal("retry reset queue age", got)
	}
	st.Ack(time.Now())
	if got := relayStatus(st)["oldest_queued_at"]; got != int64(0) {
		t.Fatal(got)
	}
}

func TestRelayRecoversTransientScanWithoutLosingQueuedWork(t *testing.T) {
	for _, code := range []string{"BROKER_UNREACHABLE", "BUSY", "POLL_UNSTABLE"} {
		t.Run(code, func(t *testing.T) {
			st, page, cfg := relayFixture()
			cfg.RetryBaseSeconds = 1
			limit := 20
			cfg.ScanRetryLimit = &limit
			if err := st.Collect(page, cfg, time.Now()); err != nil {
				t.Fatal(err)
			}
			page.Items = nil
			calls, sent := 0, 0
			var saved relay.State
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Millisecond, 1,
				func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
					calls++
					if calls == 1 {
						return broker.ScanPage{}, e.New(e.CategoryNetwork, code, "transient")
					}
					return page, nil
				},
				func(context.Context, relay.Batch) error { sent++; return nil },
				func(s relay.State) error { saved = copyRelayState(t, s); return s.Validate() }, func(string, relay.State, string) {})
			if err != nil || calls != 2 || sent != 1 || saved.ScanFailures != 0 || saved.Acknowledged != 2 || saved.LastScanError == nil || saved.LastScanError.Code != code {
				t.Fatal(err, calls, sent, saved)
			}
		})
	}
}

func TestRelayRestartRetainsKnownPollingIntervalOnFailure(t *testing.T) {
	st, page, cfg := relayFixture()
	page.Coverage.Backend, page.Coverage.PollIntervalSeconds = "poll", 30
	page.Items, page.After, page.HighWatermark = nil, 0, 0
	if err := st.Collect(page, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	var saved relay.State
	err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			return broker.ScanPage{}, e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "transient")
		}, nil,
		func(s relay.State) error { saved = copyRelayState(t, s); return s.Validate() }, func(string, relay.State, string) {})
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" || saved.NextScan-saved.LastScanError.At < 30000 {
		t.Fatal(err, saved)
	}
}
