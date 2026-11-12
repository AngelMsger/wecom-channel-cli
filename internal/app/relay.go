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

func (s *appState) relayCommands() *cobra.Command {
	g := &cobra.Command{Use: "relay", Short: "Deliver changes and track local delivery and processing state"}
	g.AddCommand(s.relayCommand(), s.relayStatusCommand(), s.relayResolveCommand(), s.relayClaimCommand(), s.relayConsumeCommand())
	return g
}

func (s *appState) relayCommand() *cobra.Command {
	scan := broker.ScanRequest{Limit: 100}
	var configPath, statePath string
	var duration, interval time.Duration
	var maxBatches int
	var dry, retryFailed, ephemeral bool
	cmd := &cobra.Command{Use: "run", Short: "Collect changes and deliver nonempty batches without a model", Args: cobra.NoArgs,
		Long:    "Run as a separate trusted service using a private fixed-destination config.\nEmpty scans send nothing. delivery_mode defaults to explicit_ack, requiring\n{\"ack\":\"BATCH_ID\"}. http_accepted accepts exactly HTTP 200 as host acceptance,\nnot processing completion; ambiguous outcomes stop until relay resolve.\nChoose durable JSON --state or lossy bounded-memory --ephemeral.\nRetries and request pacing are configured explicitly. Use a supervisor to renew\nbounded sessions; see docs/watching.md for Grok setup and worker claims.",
		Example: "  wecom-channel-cli --use-context engineering relay run --whole-channel --config /etc/wecom-relay/relay.json --state /var/lib/wecom-relay/state.json --duration 24h\n  wecom-channel-cli relay status --state /var/lib/wecom-relay/state.json",
	}
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if duration <= 0 || duration > 24*time.Hour || interval < time.Second || interval > time.Minute || maxBatches < 1 || maxBatches > 100000 || ((statePath != "") == ephemeral) || (ephemeral && retryFailed) {
			return e.New(e.CategoryUsage, "RELAY_LIMITS", "Choose --state or --ephemeral, duration up to 24h, interval 1s–1m, and max-batches 1–100000. Ephemeral mode cannot reset persisted retries.")
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
			if err = s.guard("relay run"); err != nil {
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
		if !dry && !ephemeral {
			store, err = relay.Open(statePath)
			if err != nil {
				return err
			}
			defer store.Close()
		}
		var st relay.State
		exists := false
		if !ephemeral {
			st, exists, err = relay.Read(statePath)
			if err != nil {
				return err
			}
		}
		if exists && (st.Consumer != consumer || cmd.Flags().Changed("after-sequence")) {
			return e.New(e.CategoryConflict, "RELAY_BINDING_CHANGED", "Use the original socket, scope and relay configuration with saved state, without --after-sequence. Drain or reconcile pending work before deliberately provisioning a new consumer.")
		}
		if !exists {
			st = relay.State{Version: 1, Consumer: consumer, After: scan.After, FromNow: !cmd.Flags().Changed("after-sequence")}
			if cfg.Mode() == relay.HTTPAccepted {
				st.DeliveryMode = relay.HTTPAccepted
			}
		}
		save := func(v relay.State) error { return v.Validate() }
		if store != nil {
			save = store.Save
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
			return s.emit(cmd, map[string]any{"dry_run": true, "identity": p.Identity, "coverage": p.Coverage, "queued_events": st.Depth(), "batch_size": cfg.BatchSize, "batch_window_seconds": cfg.BatchWindowSeconds, "retry_limit": cfg.RetryLimit, "retry_failed": retryFailed, "duration": duration.String(), "delivery_mode": cfg.Mode(), "durable": !ephemeral, "content": "metadata_only"})
		}
		hook, err := relay.NewWebhook(cfg)
		if err != nil {
			return err
		}
		if !exists {
			if err = save(st); err != nil {
				return err
			}
		}
		if retryFailed && exists {
			if st.Inflight != nil && (st.Inflight.Status == "unknown" || st.Inflight.Status == "sending") {
				return relay.UnknownOutcome()
			}
			st.ScanFailures, st.NextScan = 0, 0
			if st.Inflight != nil {
				st.Inflight.Attempts, st.Inflight.NextAttempt = 0, 0
				st.Inflight.Status, st.Inflight.HTTPStatus, st.Inflight.Failure = "", 0, ""
			}
			if err = save(st); err != nil {
				return err
			}
		}
		skillDriftNotice(cmd)
		notice := func(reason string, st relay.State, code string) {
			v := relayStatus(st)
			v["durable"] = !ephemeral
			v["state"], v["error_code"] = reason, code
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"relay": v}})
		}
		return pumpRelay(ctx, cfg, scan, st, interval, maxBatches, fetch, hook.Send, save, notice)
	}
	f := cmd.Flags()
	f.BoolVar(&scan.WholeChannel, "whole-channel", false, "Observe the selected channel")
	f.StringArrayVar(&scan.Posts, "post", nil, "Observe this post and its replies; repeat for up to 32 IDs")
	f.BoolVar(&scan.AllowPartial, "allow-partial", false, "Explicitly accept allowlisted-human callback coverage")
	f.Int64Var(&scan.After, "after-sequence", 0, "Replay after this sequence only when creating new relay state")
	f.StringVar(&configPath, "config", "", "Private operator-provisioned JSON with fixed destination, filters and retry bounds")
	f.StringVar(&statePath, "state", "", "Durable JSON queue in an existing private service-owned directory; mutually exclusive with --ephemeral")
	f.BoolVar(&ephemeral, "ephemeral", false, "Keep delivery state only in bounded memory; interruption loses pending work, restart starts quietly now")
	f.DurationVar(&duration, "duration", 5*time.Minute, "Maximum session duration, up to 24h; only --state preserves queued work on exit")
	f.DurationVar(&interval, "interval", 2*time.Second, "Minimum scan interval, 1s–1m; operator polling rates still apply")
	f.IntVar(&maxBatches, "max-batches", 1000, "Stop after this many acknowledged or HTTP-accepted batches in this session")
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
	initialAcks := st.AckedBatches + st.AcceptedBatches
	started := false
	// Always validate the live ledger/policy before dispatching saved work.
	validated := false
	nextScan := time.Now()
	fail := func(err error) error { notice("failed", st, e.Safe(err).Code); return err }
	exhausted := func() error {
		return e.New(e.CategoryConflict, "RELAY_RETRIES_EXHAUSTED", "The persisted relay retry budget is exhausted. Repair the service, then explicitly use --retry-failed with the same state and configuration.")
	}
	if cfg.Mode() == relay.HTTPAccepted && st.Inflight != nil {
		if st.Inflight.Status == "sending" {
			st.Inflight.Status = "unknown"
			if err := save(st); err != nil {
				return fail(err)
			}
		}
		if st.Inflight.Status == "unknown" {
			return fail(relay.UnknownOutcome())
		}
	}
	if st.Inflight != nil && st.Inflight.Failure != "" {
		return fail(e.New(e.CategoryConflict, st.Inflight.Failure, "The saved delivery stopped on a terminal error. Repair the receiver and explicitly reset retries before resuming."))
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
			if st.Inflight != nil && time.Now().UnixMilli() >= max(st.Inflight.NextAttempt, st.NextDelivery) {
				if cfg.Mode() == relay.HTTPAccepted && len(st.Receipts) >= 10000 {
					return fail(e.New(e.CategoryRateLimit, "RELAY_RECEIPTS_FULL", "Delivery receipt capacity is exhausted. Archive and reconcile the saved state before provisioning a new relay."))
				}
				st.Inflight.Attempts++
				if cfg.Mode() == relay.HTTPAccepted {
					st.Inflight.Status, st.Inflight.LastAttempt, st.Inflight.HTTPStatus = "sending", time.Now().UnixMilli(), 0
				}
				st.NextDelivery = time.Now().Add(time.Duration(cfg.MinDeliverySeconds) * time.Second).UnixMilli()
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if err := save(st); err != nil {
					return fail(err)
				}
				err := send(ctx, st.Inflight.Batch)
				if err == nil {
					reason := "acknowledged"
					if cfg.Mode() == relay.HTTPAccepted {
						if err = st.Accept(time.Now(), "http_200"); err != nil {
							return fail(err)
						}
						reason = "accepted"
					} else {
						st.Ack(time.Now())
					}
					if err = save(st); err != nil {
						return fail(err)
					}
					notice(reason, st, "")
					if st.AckedBatches+st.AcceptedBatches-initialAcks >= int64(maxBatches) {
						notice("max_batches", st, "")
						return nil
					}
					continue
				}
				if !relay.Retryable(err) {
					st.Inflight.Failure = e.Safe(err).Code
				}
				if cfg.Mode() == relay.HTTPAccepted {
					st.Inflight.Status = "unknown"
					if x := e.Safe(err); x.Code == "RELAY_REJECTED" || x.Code == "RELAY_REDIRECT" {
						st.Inflight.Status, st.Inflight.HTTPStatus = "rejected", x.HTTPStatus
					}
				}
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if saveErr := save(st); saveErr != nil {
					return fail(saveErr)
				}
				if st.Inflight.Status == "unknown" {
					return fail(relay.UnknownOutcome())
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
			wake = minTime(wake, time.UnixMilli(max(st.Inflight.NextAttempt, st.NextDelivery)))
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
	v["delivery_mode"] = relay.ExplicitAck
	if st.DeliveryMode != "" {
		v["delivery_mode"] = st.DeliveryMode
	}
	v["accepted_events"], v["accepted_batches"], v["last_accepted_at"], v["next_delivery_at"] = st.AcceptedEvents, st.AcceptedBatches, st.LastAccepted, st.NextDelivery
	v["processing"] = "not_reported_by_webhook"
	if len(st.Receipts) > 0 {
		r := st.Receipts[len(st.Receipts)-1]
		v["last_receipt"] = map[string]any{"batch_id": r.ID, "status": r.Status, "events": r.Events, "accepted_at": r.AcceptedAt}
	}
	if st.Inflight != nil {
		v["inflight_id"], v["delivery_attempts"], v["next_attempt_at"] = st.Inflight.Batch.ID, st.Inflight.Attempts, st.Inflight.NextAttempt
		v["delivery_status"], v["http_status"] = st.Inflight.Status, st.Inflight.HTTPStatus
		v["terminal_error"] = st.Inflight.Failure
	}
	return v
}

func (s *appState) relayStatusCommand() *cobra.Command {
	var path, consumerPath, id string
	cmd := &cobra.Command{Use: "status", Short: "Read local delivery counts and timestamps; this does not probe process liveness", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if (path == "") == (consumerPath == "") {
			return e.New(e.CategoryUsage, "RELAY_STATUS", "Choose --state for sender delivery or --consumer-state for worker processing.")
		}
		if consumerPath != "" {
			st, exists, err := relay.ReadConsumer(consumerPath)
			if err != nil {
				return err
			}
			if !exists {
				return e.New(e.CategoryNotFound, "RELAY_STATE_MISSING", "No worker claims have been recorded.")
			}
			v, err := consumerStatus(st, id)
			if err != nil {
				return err
			}
			return s.emit(cmd, v)
		}
		st, exists, err := relay.Read(path)
		if err != nil {
			return err
		}
		if !exists {
			return e.New(e.CategoryNotFound, "RELAY_STATE_MISSING", "No relay state exists yet. Start the configured relay to establish its baseline.")
		}
		v := relayStatus(st)
		if id != "" {
			found := false
			if st.Inflight != nil && st.Inflight.Batch.ID == id {
				v["batch"] = st.Inflight
				found = true
			}
			for _, r := range st.Receipts {
				if r.ID == id {
					v["receipt"] = r
					found = true
					break
				}
			}
			if !found {
				return e.New(e.CategoryNotFound, "RELAY_BATCH_MISSING", "No pending or retained accepted batch matches this ID.")
			}
		}
		return s.emit(cmd, v)
	}}
	cmd.Flags().StringVar(&path, "state", "", "Private sender-owned relay state")
	cmd.Flags().StringVar(&consumerPath, "consumer-state", "", "Private worker-owned processing state; mutually exclusive with --state")
	cmd.Flags().StringVar(&id, "batch-id", "", "Inspect a specific retained delivery or worker claim")
	return cmd
}
