package app

import (
	"context"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
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
	var dry, retryFailed, retryScan, ephemeral bool
	cmd := &cobra.Command{Use: "run", Short: "Collect changes and deliver nonempty batches without a model", Args: cobra.NoArgs,
		Long:    "Run as a separate trusted service using a private fixed-destination config.\nEmpty scans send nothing. delivery_mode defaults to explicit_ack, requiring\n{\"ack\":\"BATCH_ID\"}. http_accepted accepts exactly HTTP 200 as host acceptance,\nnot processing completion; ambiguous outcomes stop until relay resolve.\nChoose durable JSON --state or lossy bounded-memory --ephemeral.\nscan_retry_limit and delivery_retry_limit override the shared retry_limit.\nStatus retains sanitized failure causes across restart; retries require explicit policy.\nUse a supervisor to renew\nbounded sessions; see docs/watching.md for Grok setup and worker claims.",
		Example: "  wecom-channel-cli --use-context engineering relay run --whole-channel --config /etc/wecom-relay/relay.json --state /var/lib/wecom-relay/state.json --duration 24h\n  wecom-channel-cli relay status --state /var/lib/wecom-relay/state.json",
	}
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if duration < 0 || interval < time.Second || interval > time.Minute || maxBatches < 0 || maxBatches > 100000 || ((statePath != "") == ephemeral) || (ephemeral && (retryFailed || retryScan)) || (retryFailed && retryScan) {
			return e.New(e.CategoryUsage, "RELAY_LIMITS", "Choose --state or --ephemeral, non-negative duration (0 for continuous), interval 1s–1m, and max-batches 0–100000. Ephemeral mode cannot reset persisted retries.")
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
		scope := watchScope(scan)
		consumer := cfg.ConsumerDigest(socket, scope)
		legacyConsumer := cfg.LegacyConsumerDigest(socket, scope)
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
		consumerOK := st.Consumer == consumer || st.Consumer == legacyConsumer
		if exists && (!consumerOK || cmd.Flags().Changed("after-sequence")) {
			return e.New(e.CategoryConflict, "RELAY_BINDING_CHANGED", "Use the original socket, scope, destination and filters with saved state, without --after-sequence. Drain or reconcile pending work before deliberately provisioning a new consumer.")
		}
		// Migrate legacy full-config consumers to the tuning-independent digest,
		// preserving LegacyConsumer when historical batches or receipts depend on it.
		upgradedConsumer := false
		if exists && st.Consumer == legacyConsumer && st.Consumer != consumer && !dry {
			if st.Inflight != nil || len(st.Receipts) > 0 {
				st.LegacyConsumer = st.Consumer
			}
			st.Consumer = consumer
			upgradedConsumer = true
		}
		if retryScan && !exists {
			return e.New(e.CategoryNotFound, "RELAY_STATE_MISSING", "A scan repair requires the original sender state.")
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
		ctx := cmd.Context()
		var cancel context.CancelFunc
		if duration > 0 {
			ctx, cancel = context.WithTimeout(ctx, duration)
			defer cancel()
		}
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
			return s.emit(cmd, map[string]any{"dry_run": true, "identity": p.Identity, "coverage": p.Coverage, "queued_events": st.Depth(), "batch_size": cfg.BatchSize, "batch_window_seconds": cfg.BatchWindowSeconds, "retry_limit": cfg.RetryLimit, "scan_retry_limit": cfg.ScanRetries(), "delivery_retry_limit": cfg.DeliveryRetries(), "on_uncertain": cfg.UncertainPolicy(), "retry_failed": retryFailed, "duration": duration.String(), "delivery_mode": cfg.Mode(), "durable": !ephemeral, "content": "metadata_only"})
		}
		hook, err := relay.NewWebhook(cfg)
		if err != nil {
			return err
		}
		if !exists || (st.Binding == "" && st.StartedAt == 0) || upgradedConsumer {
			if st.StartedAt == 0 {
				st.StartedAt = time.Now().UnixMilli()
			}
			if err = save(st); err != nil {
				return err
			}
		}
		if (retryFailed || retryScan) && exists {
			if st.Inflight != nil && (st.Inflight.Status == "unknown" || st.Inflight.Status == "sending") {
				return relay.UnknownOutcome()
			}
			st.ResetScan()
			if retryFailed && st.Inflight != nil {
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
		scan.ResumePoll = !ephemeral
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
	f.DurationVar(&duration, "duration", 0, "Optional maximum session duration (0 runs continuously until interrupted)")
	f.DurationVar(&interval, "interval", 2*time.Second, "Minimum scan interval, 1s–1m; operator polling rates still apply")
	f.IntVar(&maxBatches, "max-batches", 0, "Optional maximum acknowledged/accepted batches in this session (0 for unlimited)")
	f.BoolVar(&retryScan, "retry-scan", false, "Resume a repaired scan with a fresh scan allowance and preserve delivery attempts")
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
	if p.LastSnapshot < 0 || (p.Progress != nil && (!scan.ResumePoll || !p.Progress.Valid() || len(p.Items) > 0 || p.After != scan.After || p.Coverage.Backend != "poll")) {
		return e.New(e.CategoryParse, "WATCH_PROTOCOL", "Invalid polling progress response.")
	}
	return validateBatch(p, cp, scan.Limit)
}

func pumpRelay(ctx context.Context, cfg relay.Config, scan broker.ScanRequest, st relay.State, interval time.Duration, maxBatches int, fetch watchFetch, send func(context.Context, relay.Batch) error, save func(relay.State) error, notice func(string, relay.State, string)) error {
	return pumpRelayWithWake(ctx, cfg, scan, st, interval, maxBatches, nil, fetch, send, save, notice)
}

func pumpRelayWithWake(ctx context.Context, cfg relay.Config, scan broker.ScanRequest, st relay.State, interval time.Duration, maxBatches int, wakeCh <-chan struct{}, fetch watchFetch, send func(context.Context, relay.Batch) error, save func(relay.State) error, notice func(string, relay.State, string)) error {
	st.QueueLimit = cfg.QueueLimit
	initialAcks := st.AckedBatches + st.AcceptedBatches
	started := false
	// Always validate the live ledger/policy before dispatching saved work.
	validated := false
	if st.Coverage.Backend == "poll" {
		interval = max(interval, time.Duration(st.Coverage.PollIntervalSeconds)*time.Second)
	}
	nextScan := time.Now()
	fail := func(err error) error { notice("failed", st, e.Safe(err).Code); return err }
	exhausted := func(phase string) error {
		record := st.LastScanError
		repair := "--retry-scan"
		if phase == "delivery" {
			record = st.LastDeliveryError
			repair = "--retry-failed"
		}
		return e.New(e.CategoryConflict, "RELAY_RETRIES_EXHAUSTED", "The persisted relay retry budget is exhausted. Repair the service, then explicitly use "+repair+" with the same state and configuration.").WithDetails(map[string]any{"phase": phase, "last_error": record})
	}
	handleUncertain := func(evidence string) (bool, error) {
		switch cfg.UncertainPolicy() {
		case relay.UncertainSkip:
			if err := st.RecordUncertain(time.Now(), evidence); err != nil {
				return false, fail(err)
			}
			if err := save(st); err != nil {
				return false, fail(err)
			}
			notice("uncertain_skipped", st, "RELAY_OUTCOME_UNKNOWN")
			return true, nil
		case relay.UncertainRetry:
			if st.Inflight.Attempts <= cfg.DeliveryRetries() {
				st.Inflight.Status = "rejected"
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if err := save(st); err != nil {
					return false, fail(err)
				}
				notice("delivery_retry", st, "RELAY_OUTCOME_UNKNOWN")
				return true, nil
			}
			if err := st.RecordUncertain(time.Now(), evidence); err != nil {
				return false, fail(err)
			}
			if err := save(st); err != nil {
				return false, fail(err)
			}
			notice("uncertain_skipped", st, "RELAY_OUTCOME_UNKNOWN")
			return true, nil
		default:
			return false, fail(relay.UnknownOutcome())
		}
	}
	if cfg.Mode() == relay.HTTPAccepted && st.Inflight != nil {
		if st.Inflight.Status == "sending" {
			st.Inflight.Status = "unknown"
			if err := save(st); err != nil {
				return fail(err)
			}
		}
		if st.Inflight.Status == "unknown" {
			if handled, err := handleUncertain("interrupted_sending"); !handled {
				return err
			}
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
		if !cfg.UnlimitedScanRetries() && st.ScanFailures > cfg.ScanRetries() {
			return fail(exhausted("scan"))
		}
		if st.Inflight != nil && st.Inflight.Attempts > cfg.DeliveryRetries() {
			return fail(exhausted("delivery"))
		}
		if st.ScanBlocked != "" {
			return fail(e.New(st.ScanBlocked, st.LastScanError.Code, "The saved scan is blocked. Repair the cause and use --retry-scan.").WithDetails(st.LastScanError))
		}
		if st.RetryStarted {
			// A crashed process may already have made its reserved read.
			st.RetryStarted = false
			if st.AuthRecovery && st.LastScanError != nil && st.LastScanError.Code == "AUTH_REJECTED" {
				st.ScanBlocked = e.CategoryAuth
			} else {
				st.ScanFailures++
				st.NextScan = time.Now().Add(relay.ScanRetryDelay(cfg, st.ScanFailures, interval, nil)).UnixMilli()
			}
			if err := save(st); err != nil {
				return fail(err)
			}
			continue
		}
		now := time.Now()
		scanDue := maxTime(nextScan, time.UnixMilli(st.NextScan))
		if (!validated || st.Depth() < cfg.QueueLimit) && !now.Before(scanDue) {
			scan.After, scan.Binding = st.After, st.Binding
			scan.Limit = max(1, min(100, cfg.QueueLimit-st.Depth()))
			if st.ScanFailures > 0 {
				st.RetryStarted = true
				if err := save(st); err != nil {
					return fail(err)
				}
			}
			p, err := fetch(ctx, scan)
			st.RetryStarted = false
			if err == nil {
				err = validateRelayPage(p, st, scan)
			}
			if err != nil {
				if ctx.Err() != nil {
					notice("stopped", st, "")
					return nil
				}
				now := time.Now()
				retry := st.RecordScanFailure(err, cfg, interval, now)
				if saveErr := save(st); saveErr != nil {
					return fail(saveErr)
				}
				if !retry {
					return fail(err)
				}
				if !cfg.UnlimitedScanRetries() && st.ScanFailures > cfg.ScanRetries() {
					return fail(exhausted("scan"))
				}
				notice("scan_retry", st, e.Safe(err).Code)
			} else if p.Progress != nil {
				if p.MigratedFrom != "" && st.Binding == p.MigratedFrom {
					st.Binding = p.Binding
				}
				st.Progress = p.Progress
				st.NextScan = max(time.Now().Add(time.Millisecond*100).UnixMilli(), p.Progress.NextAt)
				st.LastSnapshot = p.LastSnapshot
				if err = save(st); err != nil {
					return fail(err)
				}
			} else {
				if p.MigratedFrom != "" && st.Binding == p.MigratedFrom {
					st.Binding = p.Binding
				}
				st.ResetScan()
				st.LastSnapshot = p.LastSnapshot
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
					st.LastScan = time.Now().UnixMilli()
					if err = save(st); err != nil {
						return fail(err)
					}
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
					if maxBatches > 0 && st.AckedBatches+st.AcceptedBatches-initialAcks >= int64(maxBatches) {
						notice("max_batches", st, "")
						return nil
					}
					continue
				}
				st.LastDeliveryError = relay.NewErrorRecord(err, "delivery", st.Inflight.Attempts, cfg.DeliveryRetries(), time.Now())
				if !relay.Retryable(err) && e.Safe(err).Code != "RELAY_OUTCOME_UNKNOWN" {
					st.Inflight.Failure = e.Safe(err).Code
				}
				if cfg.Mode() == relay.HTTPAccepted {
					st.Inflight.Status = "unknown"
					if x := e.Safe(err); x.Code == "RELAY_REJECTED" || x.Code == "RELAY_REDIRECT" || x.Code == "RELAY_UNREACHABLE" {
						st.Inflight.Status, st.Inflight.HTTPStatus = "rejected", x.HTTPStatus
					}
				}
				st.Inflight.NextAttempt = time.Now().Add(relay.RetryDelay(cfg, st.Inflight.Attempts)).UnixMilli()
				if saveErr := save(st); saveErr != nil {
					return fail(saveErr)
				}
				if st.Inflight.Status == "unknown" {
					handled, handleErr := handleUncertain("post_send_timeout_or_eof")
					if !handled {
						return handleErr
					}
					continue
				}
				if ctx.Err() != nil {
					notice("stopped", st, "")
					return nil
				}
				if !relay.Retryable(err) {
					return fail(err)
				}
				if st.Inflight.Attempts > cfg.DeliveryRetries() {
					return fail(exhausted("delivery"))
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
		case <-wakeCh:
			timer.Stop()
			nextScan = time.Now()
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
	v["scan_state"] = st.ScanState()
	if st.StartedAt > 0 {
		v["started_at"] = st.StartedAt
	}
	if st.QueueLimit > 0 {
		v["queue_limit"] = st.QueueLimit
	}
	if st.LastSnapshot > 0 || (st.QueueLimit > 0 && st.Coverage.Backend == "poll") {
		v["last_snapshot_at"] = st.LastSnapshot
	}
	if st.Progress != nil {
		v["poll_progress"] = st.Progress
	}
	if st.FirstFailure > 0 {
		v["first_scan_failure_at"] = st.FirstFailure
	}
	if len(st.ScanHistory) > 0 {
		v["scan_history"] = st.ScanHistory
	}
	v["initialized"] = st.Binding != ""
	if st.LastScanError != nil {
		v["last_scan_error"] = st.LastScanError
	}
	if st.LastDeliveryError != nil {
		v["last_delivery_error"] = st.LastDeliveryError
	}
	// Preserve enqueue age across sealing/retry. Legacy batches fall back to the
	// broker's receipt time, an upper bound on their time in the sender queue.
	oldest := st.PendingSince
	if st.Inflight != nil {
		queuedAt := st.Inflight.QueuedAt
		if queuedAt == 0 {
			for _, thread := range st.Inflight.Batch.Threads {
				for _, record := range thread.Events {
					at := record.Data.Received * 1000
					if at > 0 && (queuedAt == 0 || at < queuedAt) {
						queuedAt = at
					}
				}
			}
		}
		if queuedAt > 0 && (oldest == 0 || queuedAt < oldest) {
			oldest = queuedAt
		}
	}
	v["oldest_queued_at"] = oldest
	v["delivery_mode"] = relay.ExplicitAck
	if st.DeliveryMode != "" {
		v["delivery_mode"] = st.DeliveryMode
	}
	v["accepted_events"], v["accepted_batches"], v["last_accepted_at"], v["next_delivery_at"] = st.AcceptedEvents, st.AcceptedBatches, st.LastAccepted, st.NextDelivery
	if st.UncertainBatches > 0 {
		v["uncertain_events"], v["uncertain_batches"] = st.UncertainEvents, st.UncertainBatches
	}
	v["processing"] = "not_reported_by_webhook"
	if len(st.Receipts) > 0 {
		r := st.Receipts[len(st.Receipts)-1]
		v["last_receipt"] = map[string]any{"batch_id": r.ID, "status": r.Status, "events": r.Events, "accepted_at": r.AcceptedAt}
	}
	if st.Inflight != nil {
		v["last_attempt_at"] = st.Inflight.LastAttempt
		v["inflight_id"], v["delivery_attempts"], v["next_attempt_at"] = st.Inflight.Batch.ID, st.Inflight.Attempts, st.Inflight.NextAttempt
		v["delivery_status"], v["http_status"] = st.Inflight.Status, st.Inflight.HTTPStatus
		v["terminal_error"] = st.Inflight.Failure
	}
	return v
}

func (s *appState) relayStatusCommand() *cobra.Command {
	var path, consumerPath, id string
	cmd := &cobra.Command{Use: "status", Short: "Read local delivery counts, timestamps and failure causes; this does not probe process liveness", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
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
