package app

import (
	"context"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) relayCommand() *cobra.Command {
	scan := broker.ScanRequest{Limit: 100}
	var configPath, statePath string
	var duration, interval time.Duration
	var maxBatches int
	var dry, retryFailed bool
	cmd := &cobra.Command{Use: "relay", Short: "Collect observations durably and deliver acknowledged webhook batches without a model", Args: cobra.NoArgs,
		Long:    "Run this opt-in consumer as a separate trusted service, outside the model worker.\nIt reads the existing agent socket and a private operator-provisioned relay config.\nThe broker never receives a webhook URL or credential. Empty scans send nothing.\nBatches retain the untrusted envelope and contain metadata only. The receiver must\ndeduplicate Idempotency-Key and return {\"ack\":\"BATCH_ID\"} after processing or\ndurable handoff. Retries are explicitly bounded by config retry_limit (default 0).\nGaps, permission errors, binding changes and invalid acknowledgments stop delivery.\nUse a supervisor to renew bounded sessions; see docs/watching.md for deployment.",
		Example: "  wecom-channel-cli --use-context engineering event relay --whole-channel --relay-config /etc/wecom-relay/relay.json --state /var/lib/wecom-relay/state.json --duration 24h\n  wecom-channel-cli event relay-status --state /var/lib/wecom-relay/state.json",
	}
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if duration <= 0 || duration > 24*time.Hour || interval < time.Second || interval > time.Minute || maxBatches < 1 || maxBatches > 100000 || statePath == "" {
			return e.New(e.CategoryUsage, "RELAY_LIMITS", "Supply --state, duration up to 24h, interval 1s–1m, and max-batches 1–100000.")
		}
		var err error
		scan, err = broker.NormalizeScan(scan)
		if err != nil {
			return err
		}
		cfg, err := relay.LoadConfig(configPath)
		if err != nil {
			return err
		}
		if !dry {
			if err = s.guard("event relay"); err != nil {
				return err
			}
		}
		r := broker.Request{Operation: "event.scan", DryRun: dry}
		socket, err := s.resolve(cmd, &r)
		if err != nil {
			return err
		}
		if err = broker.ValidateSocket(socket); err != nil {
			return err
		}
		consumer := apiclient.Digest(struct {
			Socket, Scope string
			Config        relay.Config
		}{socket, watchScope(scan), cfg})
		var store *relay.Store
		if !dry {
			store, err = relay.Open(statePath)
			if err != nil {
				return err
			}
			defer store.Close()
		}
		st, exists, err := relay.Read(statePath)
		if err != nil {
			return err
		}
		if exists && (st.Consumer != consumer || cmd.Flags().Changed("after-sequence")) {
			return e.New(e.CategoryConflict, "RELAY_BINDING_CHANGED", "Use the original socket, scope and relay configuration with saved state, without --after-sequence. Drain or reconcile pending work before deliberately provisioning a new consumer.")
		}
		if !exists {
			st = relay.State{Version: 1, Consumer: consumer, After: scan.After, FromNow: !cmd.Flags().Changed("after-sequence")}
		}
		scan.FromNow = st.FromNow
		ctx, cancel := context.WithTimeout(cmd.Context(), duration)
		defer cancel()
		fetch := func(ctx context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
			r.Scan = &q
			child, stop := context.WithTimeout(ctx, s.timeout)
			defer stop()
			resp, err := broker.Call(child, socket, r)
			if err != nil {
				return broker.ScanPage{}, err
			}
			if resp.Scan == nil {
				return broker.ScanPage{}, e.New(e.CategoryParse, "WATCH_PROTOCOL", "The broker did not return a watch batch.")
			}
			return *resp.Scan, nil
		}
		if dry {
			scan.After, scan.Binding = st.After, st.Binding
			p, err := fetch(ctx, scan)
			if err != nil {
				return err
			}
			if err = validateRelayPage(p, st, scan); err != nil {
				return err
			}
			return s.emit(cmd, map[string]any{"dry_run": true, "identity": p.Identity, "coverage": p.Coverage, "queued_events": st.Depth(), "batch_size": cfg.BatchSize, "batch_window_seconds": cfg.BatchWindowSeconds, "retry_limit": cfg.RetryLimit, "retry_failed": retryFailed, "duration": duration.String(), "delivery": "webhook_with_explicit_ack", "content": "metadata_only"})
		}
		hook, err := relay.NewWebhook(cfg)
		if err != nil {
			return err
		}
		if !exists {
			if err = store.Save(st); err != nil {
				return err
			}
		}
		if retryFailed && exists {
			st.ScanFailures, st.NextScan = 0, 0
			if st.Inflight != nil {
				st.Inflight.Attempts, st.Inflight.NextAttempt = 0, 0
			}
			if err = store.Save(st); err != nil {
				return err
			}
		}
		skillDriftNotice(cmd)
		notice := func(reason string, st relay.State, code string) {
			v := relayStatus(st)
			v["state"], v["error_code"] = reason, code
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"relay": v}})
		}
		return pumpRelay(ctx, cfg, scan, st, interval, maxBatches, fetch, hook.Send, store.Save, notice)
	}
	f := cmd.Flags()
	f.BoolVar(&scan.WholeChannel, "whole-channel", false, "Observe the selected channel")
	f.StringArrayVar(&scan.Posts, "post", nil, "Observe this post and its replies; repeat for up to 32 IDs")
	f.BoolVar(&scan.AllowPartial, "allow-partial", false, "Explicitly accept allowlisted-human callback coverage")
	f.Int64Var(&scan.After, "after-sequence", 0, "Replay after this sequence only when creating new relay state")
	f.StringVar(&configPath, "relay-config", "", "Private operator-provisioned JSON with fixed destination, filters and retry bounds")
	f.StringVar(&statePath, "state", "", "Durable queue in an existing private service-owned directory (required)")
	f.DurationVar(&duration, "duration", 5*time.Minute, "Maximum session duration, up to 24h; queued work survives exit")
	f.DurationVar(&interval, "interval", 2*time.Second, "Minimum scan interval, 1s–1m; operator polling rates still apply")
	f.IntVar(&maxBatches, "max-batches", 1000, "Stop after this many acknowledged batches in this session")
	f.BoolVar(&retryFailed, "retry-failed", false, "Explicitly reset persisted retry budgets after repairing a failed receiver or scan")
	f.BoolVar(&dry, "dry-run", false, "Inspect binding and coverage without credentials, remote reads, locks or state writes")
	return cmd
}

func validateRelayPage(p broker.ScanPage, st relay.State, scan broker.ScanRequest) error {
	cp := watchCheckpoint{Binding: st.Binding, Identity: st.Identity, After: st.After}
	if cp.Binding == "" {
		cp.Binding, cp.Identity = p.Binding, p.Identity
		if scan.FromNow {
			cp.After = p.After
		}
	}
	return validateBatch(p, cp, scan.Limit)
}

func pumpRelay(ctx context.Context, cfg relay.Config, scan broker.ScanRequest, st relay.State, interval time.Duration, maxBatches int, fetch watchFetch, send func(context.Context, relay.Batch) error, save func(relay.State) error, notice func(string, relay.State, string)) error {
	initialAcks := st.AckedBatches
	started := false
	// Always validate the live ledger/policy before dispatching saved work.
	validated := false
	nextScan := time.Now()
	fail := func(err error) error { notice("failed", st, e.Safe(err).Code); return err }
	exhausted := func() error {
		return e.New(e.CategoryConflict, "RELAY_RETRIES_EXHAUSTED", "The persisted relay retry budget is exhausted. Repair the service, then explicitly use --retry-failed with the same state and configuration.")
	}
	for {
		if ctx.Err() != nil {
			notice("stopped", st, "")
			return nil
		}
		if st.ScanFailures > cfg.RetryLimit || (st.Inflight != nil && st.Inflight.Attempts > cfg.RetryLimit) {
			return fail(exhausted())
		}
		now := time.Now()
		scanDue := maxTime(nextScan, time.UnixMilli(st.NextScan))
		if (!validated || st.Depth() < cfg.QueueLimit) && !now.Before(scanDue) {
			scan.After, scan.Binding = st.After, st.Binding
			scan.Limit = max(1, min(100, cfg.QueueLimit-st.Depth()))
			p, err := fetch(ctx, scan)
			if err == nil {
				err = validateRelayPage(p, st, scan)
			}
			if err != nil {
				if ctx.Err() != nil {
					notice("stopped", st, "")
					return nil
				}
				if !relay.Retryable(err) {
					return fail(err)
				}
				st.ScanFailures++
				st.NextScan = time.Now().Add(max(interval, relay.RetryDelay(cfg, st.ScanFailures))).UnixMilli()
				if saveErr := save(st); saveErr != nil {
					return fail(saveErr)
				}
				if st.ScanFailures > cfg.RetryLimit {
					return fail(exhausted())
				}
				notice("scan_retry", st, e.Safe(err).Code)
			} else {
				// When the queue is full, validate without consuming the returned page.
				if st.Depth() < cfg.QueueLimit {
					if err = st.Collect(p, cfg, time.Now()); err != nil {
						return fail(err)
					}
					if err = save(st); err != nil {
						return fail(err)
					}
				} else {
					st.ScanFailures, st.NextScan = 0, 0
				}
				validated = true
				scan.FromNow = false
				if p.Coverage.Backend == "poll" {
					interval = max(interval, time.Duration(p.Coverage.PollIntervalSeconds)*time.Second)
				}
				nextScan = time.Now().Add(interval)
				if !started {
					notice("started", st, "")
					started = true
				}
			}
		}
		if ctx.Err() != nil {
			continue
		}
		if validated {
			if st.Seal(cfg, time.Now()) {
				if err := save(st); err != nil {
					return fail(err)
				}
			}
			if st.Inflight != nil && time.Now().UnixMilli() >= st.Inflight.NextAttempt {
				st.Inflight.Attempts++
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if err := save(st); err != nil {
					return fail(err)
				}
				err := send(ctx, st.Inflight.Batch)
				if err == nil {
					st.Ack(time.Now())
					if err = save(st); err != nil {
						return fail(err)
					}
					notice("acknowledged", st, "")
					if st.AckedBatches-initialAcks >= int64(maxBatches) {
						notice("max_batches", st, "")
						return nil
					}
					continue
				}
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if saveErr := save(st); saveErr != nil {
					return fail(saveErr)
				}
				if ctx.Err() != nil {
					notice("stopped", st, "")
					return nil
				}
				if !relay.Retryable(err) {
					return fail(err)
				}
				if st.Inflight.Attempts > cfg.RetryLimit {
					return fail(exhausted())
				}
				notice("delivery_retry", st, e.Safe(err).Code)
			}
		}
		wake := maxTime(nextScan, time.UnixMilli(st.NextScan))
		if validated && st.Depth() >= cfg.QueueLimit {
			wake = time.Now().Add(time.Minute)
		}
		if validated && st.Inflight != nil {
			wake = minTime(wake, time.UnixMilli(st.Inflight.NextAttempt))
		} else if validated && len(st.Pending) > 0 {
			wake = minTime(wake, time.UnixMilli(st.PendingSince).Add(time.Duration(cfg.BatchWindowSeconds)*time.Second))
		}
		timer := time.NewTimer(max(time.Until(wake), time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func relayStatus(st relay.State) map[string]any {
	v := map[string]any{"identity": st.Identity, "coverage": st.Coverage, "collected_sequence": st.After, "collected_events": st.Collected, "filtered_events": st.Filtered, "queued_events": st.Depth(), "acknowledged_events": st.Acknowledged, "acknowledged_batches": st.AckedBatches, "last_scan_at": st.LastScan, "last_ack_at": st.LastAck, "scan_failures": st.ScanFailures, "next_scan_at": st.NextScan}
	v["initialized"] = st.Binding != ""
	if st.Inflight != nil {
		v["inflight_id"], v["delivery_attempts"], v["next_attempt_at"] = st.Inflight.Batch.ID, st.Inflight.Attempts, st.Inflight.NextAttempt
	}
	return v
}

func (s *appState) relayStatusCommand() *cobra.Command {
	var path string
	cmd := &cobra.Command{Use: "relay-status", Short: "Read local queue counts and timestamps without a broker, webhook or model call", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		st, exists, err := relay.Read(path)
		if err != nil {
			return err
		}
		if !exists {
			return e.New(e.CategoryNotFound, "RELAY_STATE_MISSING", "No relay state exists yet. Start the configured relay to establish its baseline.")
		}
		return s.emit(cmd, relayStatus(st))
	}}
	cmd.Flags().StringVar(&path, "state", "", "Private relay state file (required)")
	return cmd
}
