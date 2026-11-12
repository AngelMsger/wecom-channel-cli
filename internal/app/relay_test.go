package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func relayFixture() (relay.State, broker.ScanPage, relay.Config) {
	cp, p := streamFixture()
	for i := range p.Items {
		v := p.Items[i].Data.(callback.Event)
		v.ID, v.Change = string(rune('a'+i)), 1
		p.Items[i].Data = v
	}
	c := relay.DefaultConfig()
	c.BatchSize = 2
	c.URL = "https://example.invalid/events"
	return relay.State{Version: 1, Consumer: apiclient.Digest("consumer"), Identity: cp.Identity}, p, c
}

func copyRelayState(t *testing.T, s relay.State) relay.State {
	t.Helper()
	b, _ := json.Marshal(s)
	var out relay.State
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRelayIdleAndFilteredScansNeverDispatch(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		st, p, cfg := relayFixture()
		if filtered {
			cfg.Changes = []int{3}
		} else {
			p.Items, p.After, p.HighWatermark = nil, 0, 0
		}
		ctx, cancel := context.WithCancel(context.Background())
		var saved relay.State
		err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
			func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
			func(context.Context, relay.Batch) error { t.Fatal("idle/filter invoked receiver"); return nil },
			func(s relay.State) error { saved = copyRelayState(t, s); cancel(); return nil },
			func(string, relay.State, string) {})
		cancel()
		if err != nil || saved.Depth() != 0 || saved.After != p.After {
			t.Fatal(err, saved)
		}
		if filtered && saved.Filtered != 2 {
			t.Fatal("filter progress lost")
		}
	}
}

func TestRelayPersistencePrecedesDispatchAndAck(t *testing.T) {
	st, p, cfg := relayFixture()
	var saved relay.State
	saves, calls := 0, 0
	err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
		func(_ context.Context, b relay.Batch) error {
			calls++
			if saved.After != 2 || saved.Inflight == nil || saved.Inflight.Attempts != 1 || saved.Inflight.Batch.ID != b.ID {
				t.Fatal("send before durable commit")
			}
			return nil
		},
		func(s relay.State) error { saves++; saved = copyRelayState(t, s); return nil },
		func(string, relay.State, string) {})
	if err != nil || calls != 1 || saves != 4 || saved.Depth() != 0 || saved.Acknowledged != 2 {
		t.Fatal(err, calls, saves, saved)
	}
	for _, failAt := range []int{1, 2, 3} {
		saves = 0
		err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
			func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
			func(context.Context, relay.Batch) error { t.Fatal("dispatched after persistence failure"); return nil },
			func(relay.State) error {
				saves++
				if saves == failAt {
					return errors.New("disk failed")
				}
				return nil
			},
			func(string, relay.State, string) {})
		if err == nil {
			t.Fatal("ignored save failure")
		}
	}
}

func TestRelayRestartReusesBatchAndStopsOnUnsafeResume(t *testing.T) {
	st, p, cfg := relayFixture()
	cfg.RetryLimit = 1
	ctx, cancel := context.WithCancel(context.Background())
	var saved relay.State
	var first relay.Batch
	err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
		func(_ context.Context, b relay.Batch) error {
			first = b
			cancel()
			return e.New(e.CategoryNetwork, "RELAY_UNREACHABLE", "lost ack")
		},
		func(s relay.State) error { saved = copyRelayState(t, s); return nil },
		func(string, relay.State, string) {})
	if err != nil || saved.Depth() != 2 || saved.Acknowledged != 0 {
		t.Fatal(err, saved)
	}
	for _, code := range []string{"EVENT_GAP", "WATCH_BINDING_CHANGED", "WATCH_READ_SCOPE"} {
		err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
			func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
				return broker.ScanPage{}, e.New(e.CategoryConflict, code, "stop")
			},
			func(context.Context, relay.Batch) error { t.Fatal("dispatched after fatal resume"); return nil },
			func(relay.State) error { t.Fatal("saved after fatal resume"); return nil },
			func(string, relay.State, string) {})
		if e.Safe(err).Code != code {
			t.Fatal(err)
		}
	}
	// Simulate time passing while the process was stopped.
	saved.Inflight.NextAttempt = 0
	p.Items = nil
	called := 0
	err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
		func(_ context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
			if q.After != 2 || q.Binding != saved.Binding {
				t.Fatal("lost cursor")
			}
			return p, nil
		},
		func(_ context.Context, b relay.Batch) error {
			called++
			a, _ := json.Marshal(first)
			next, _ := json.Marshal(b)
			if string(a) != string(next) {
				t.Fatal("retry changed payload")
			}
			return nil
		},
		func(s relay.State) error { saved = copyRelayState(t, s); return nil },
		func(string, relay.State, string) {})
	if err != nil || called != 1 || saved.Acknowledged != 2 {
		t.Fatal(err, saved)
	}
}

func TestRelayRetryBudgetPersistsAndCancellationBoundsWait(t *testing.T) {
	st, p, cfg := relayFixture()
	var saved relay.State
	err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
		func(context.Context, relay.Batch) error {
			return e.New(e.CategoryNetwork, "RELAY_UNREACHABLE", "offline")
		},
		func(s relay.State) error { saved = copyRelayState(t, s); return nil },
		func(string, relay.State, string) {})
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" || saved.Inflight.Attempts != 1 {
		t.Fatal(err)
	}
	err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			t.Fatal("reset budget on restart")
			return p, nil
		}, nil, nil, func(string, relay.State, string) {})
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" {
		t.Fatal(err)
	}
	cfg.RetryLimit = 2
	saved.Inflight.NextAttempt = time.Now().Add(time.Hour).UnixMilli()
	p.Items = nil
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Minute, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
		func(context.Context, relay.Batch) error { t.Fatal("ignored retry deadline"); return nil },
		func(relay.State) error { return nil }, func(string, relay.State, string) {})
	if err != nil || time.Since(start) > time.Second {
		t.Fatal("uncancellable wait", err)
	}
}

func TestRelayStartupRetryBudgetSurvivesBeforeHandshake(t *testing.T) {
	st := relay.State{Version: 1, Consumer: apiclient.Digest("consumer"), FromNow: true}
	cfg := relay.DefaultConfig()
	var saved relay.State
	err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true, FromNow: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			return broker.ScanPage{}, e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "offline")
		},
		func(context.Context, relay.Batch) error { t.Fatal("send before handshake"); return nil },
		func(s relay.State) error { saved = copyRelayState(t, s); return saved.Validate() }, func(string, relay.State, string) {})
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" || saved.ScanFailures != 1 || !saved.FromNow || saved.Binding != "" {
		t.Fatal(err, saved)
	}
	err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true, FromNow: true}, saved, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
			t.Fatal("restart reset initial retry budget")
			return broker.ScanPage{}, nil
		}, nil, nil, func(string, relay.State, string) {})
	if e.Safe(err).Code != "RELAY_RETRIES_EXHAUSTED" {
		t.Fatal(err)
	}
}

func TestRelayFullQueueValidatesWithoutSkippingUncollectedEvent(t *testing.T) {
	st, p, cfg := relayFixture()
	cfg.QueueLimit = 2
	if err := st.Collect(p, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	st.Seal(cfg, time.Now())
	v := p.Items[0].Data.(callback.Event)
	v.Sequence, v.ID = 3, "third"
	p.Items = p.Items[:1]
	p.Items[0].Data = v
	p.After, p.HighWatermark = 3, 3
	var saved relay.State
	err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(_ context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
			if q.After != 2 || q.Limit != 1 {
				t.Fatal(q)
			}
			return p, nil
		},
		func(context.Context, relay.Batch) error { return nil },
		func(s relay.State) error { saved = copyRelayState(t, s); return nil }, func(string, relay.State, string) {})
	if err != nil || saved.After != 2 || saved.Acknowledged != 2 {
		t.Fatal("full queue skipped event", err, saved)
	}
}

func TestRelayAcceptedUnknownAndCrashNeverReplay(t *testing.T) {
	for _, outcome := range []string{"accepted", "unknown", "crash"} {
		t.Run(outcome, func(t *testing.T) {
			st, p, cfg := relayFixture()
			st.DeliveryMode = relay.HTTPAccepted
			cfg.DeliveryMode = relay.HTTPAccepted
			cfg.RetryLimit = 5
			var saved relay.State
			calls := 0
			err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
				func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
				func(context.Context, relay.Batch) error {
					calls++
					if saved.Inflight.Status != "sending" {
						t.Fatal("not persisted before POST")
					}
					if outcome == "unknown" {
						return relay.UnknownOutcome()
					}
					return nil
				},
				func(s relay.State) error {
					if outcome == "crash" && s.AcceptedBatches > 0 {
						return errors.New("disk failed after HTTP 200")
					}
					saved = copyRelayState(t, s)
					return nil
				}, func(string, relay.State, string) {})
			if calls != 1 {
				t.Fatal(calls)
			}
			if outcome == "accepted" {
				if err != nil || saved.AcceptedBatches != 1 || saved.AckedBatches != 0 || saved.Inflight != nil {
					t.Fatal(err, saved)
				}
				return
			}
			if err == nil || saved.Inflight == nil {
				t.Fatal(err)
			}
			err = pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, saved, time.Second, 1,
				func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
					t.Fatal("scan before reconciliation")
					return p, nil
				},
				func(context.Context, relay.Batch) error { t.Fatal("replayed possible wakeup"); return nil },
				func(s relay.State) error { saved = copyRelayState(t, s); return nil }, func(string, relay.State, string) {})
			if e.Safe(err).Code != "RELAY_OUTCOME_UNKNOWN" || saved.Inflight.Status != "unknown" {
				t.Fatal(err, saved)
			}
		})
	}
}

func TestRelayAcceptedPersistenceFailureAndPacing(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		st, p, cfg := relayFixture()
		st.DeliveryMode = relay.HTTPAccepted
		cfg.DeliveryMode = relay.HTTPAccepted
		saves := 0
		err := pumpRelay(context.Background(), cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
			func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
			func(context.Context, relay.Batch) error { t.Fatal("sent despite save failure"); return nil },
			func(relay.State) error {
				saves++
				if saves == failAt {
					return errors.New("disk full")
				}
				return nil
			}, func(string, relay.State, string) {})
		if err == nil {
			t.Fatal("ignored save failure")
		}
	}
	st, p, cfg := relayFixture()
	st.DeliveryMode = relay.HTTPAccepted
	cfg.DeliveryMode = relay.HTTPAccepted
	st.NextDelivery = time.Now().Add(time.Hour).UnixMilli()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := pumpRelay(ctx, cfg, broker.ScanRequest{WholeChannel: true}, st, time.Second, 1,
		func(context.Context, broker.ScanRequest) (broker.ScanPage, error) { return p, nil },
		func(context.Context, relay.Batch) error { t.Fatal("ignored persisted rate bound"); return nil },
		func(relay.State) error { return nil }, func(string, relay.State, string) {})
	if err != nil {
		t.Fatal(err)
	}
}
