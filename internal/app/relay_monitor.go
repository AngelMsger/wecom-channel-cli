package app

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/monitor"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

type relayMonitorDeps struct {
	queryUnit    func(context.Context, string) (monitor.UnitStatus, error)
	monotonicNow func() (float64, error)
	now          func() time.Time
}

func defaultRelayMonitorDeps() relayMonitorDeps {
	return relayMonitorDeps{
		queryUnit:    monitor.QuerySystemdUnit,
		monotonicNow: monitor.MonotonicSeconds,
		now:          time.Now,
	}
}

func (s *appState) relayCheckCommand() *cobra.Command {
	defaults := monitor.DefaultOptions()
	var instance, unitName, statePath, alertStatePath, outboxDir string
	var single bool
	opts := defaults

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check one expected systemd relay listener and queue edge-triggered alert events",
		Args:  cobra.NoArgs,
		Long: "Read-only host probe for one expected systemd relay service. Evaluates unit\n" +
			"liveness and local sender state, always reporting the true current health\n" +
			"(exit 0 when healthy, exit 11 when the listener is unhealthy, other non-zero\n" +
			"exit codes only for probe errors). Freshness alerts default to 10 minutes;\n" +
			"hard failures (service missing, relay down, broker unavailable, blocked, full\n" +
			"or aged queues, unknown delivery) alert on first observation. With\n" +
			"--alert-state and --outbox, immutable events are queued only on threshold\n" +
			"crossing, escalation to a hard failure, and genuine recovery.",
		Example: "  wecom-channel-cli relay check --instance engineering\n" +
			"  wecom-channel-cli relay check --instance engineering --alert-state /var/lib/wecom-health-relay-engineering/probe-state.json --outbox /var/lib/wecom-health-relay-engineering/outbox",
	}

	f := cmd.Flags()
	f.StringVar(&instance, "instance", "", "Expected relay instance name (alphanumeric, _, -)")
	f.BoolVar(&single, "single", false, "Check the standalone wecom-channel-relay.service unit and /var/lib/wecom-relay/state.json")
	f.StringVar(&unitName, "unit", "", "Optional systemd unit name override")
	f.StringVar(&statePath, "state", "", "Optional private relay state.json path override")
	f.BoolVar(&opts.Paused, "paused", false, "Expect the relay service to be deliberately inactive")
	f.DurationVar(&opts.FreshnessThreshold, "freshness-threshold", defaults.FreshnessThreshold, "Freshness alert threshold for active listeners (scan_stale, and default for scan_outage and snapshot_stale)")
	f.DurationVar(&opts.Outage, "outage", 0, "Optional sustained scan-failure threshold override (0 inherits --freshness-threshold)")
	f.DurationVar(&opts.SnapshotAge, "snapshot-age", 0, "Optional polling snapshot age threshold override (0 inherits --freshness-threshold)")
	f.DurationVar(&opts.HardFailureThreshold, "hard-failure-threshold", 0, "Optional delay before alerting on hard failures (0 alerts on first observation)")
	f.DurationVar(&opts.Interval, "interval", defaults.Interval, "Minimum scan interval floor")
	f.DurationVar(&opts.ScanTimeout, "scan-timeout", defaults.ScanTimeout, "Single scan request deadline used for scan cycle floor")
	f.DurationVar(&opts.Grace, "grace", defaults.Grace, "Monitor scheduling grace used for scan cycle floor and delivery stall bound")
	f.DurationVar(&opts.Startup, "startup", defaults.Startup, "Startup and initial baseline deadline")
	f.DurationVar(&opts.QueueAge, "queue-age", defaults.QueueAge, "Maximum allowed age of queued events before alerting")
	f.IntVar(&opts.QueueLimit, "queue-limit", defaults.QueueLimit, "Fallback queue capacity when absent from relay state")
	f.StringVar(&alertStatePath, "alert-state", "", "Private probe-state.json file for edge-triggered incident deduplication and recovery tracking")
	f.StringVar(&outboxDir, "outbox", "", "Private FIFO directory for immutable edge notification events (requires --alert-state)")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if !monitor.ValidateInstanceName(instance) {
			return e.New(e.CategoryUsage, "BAD_INSTANCE", "Supply --instance with 1–64 alphanumeric, underscore, or hyphen characters.")
		}
		if err := opts.Validate(); err != nil {
			return err
		}
		if outboxDir != "" && alertStatePath == "" {
			return e.New(e.CategoryUsage, "BAD_PROBE_OPTIONS", "--outbox requires --alert-state for edge-triggered notification tracking.")
		}
		if unitName == "" {
			if single {
				unitName = "wecom-channel-relay.service"
			} else {
				unitName = "wecom-channel-relay@" + instance + ".service"
			}
		}
		if !monitor.ValidateUnitName(unitName) {
			return e.New(e.CategoryUsage, "BAD_UNIT", "Invalid systemd unit name.")
		}
		if statePath == "" {
			if single {
				statePath = "/var/lib/wecom-relay/state.json"
			} else {
				statePath = "/var/lib/wecom-relay-" + instance + "/state.json"
			}
		}

		deps := s.monitorDeps
		if deps.queryUnit == nil {
			deps = defaultRelayMonitorDeps()
		}

		unit, err := deps.queryUnit(cmd.Context(), unitName)
		if err != nil {
			return err
		}

		var statePtr *relay.State
		stateReadErr := false
		if !opts.Paused && unit.LoadState == "loaded" && unit.ActiveState == "active" && unit.SubState == "running" {
			st, exists, readErr := relay.Read(statePath)
			if readErr != nil {
				stateReadErr = true
			} else if exists {
				statePtr = &st
			}
		}

		monoSec, monoErr := deps.monotonicNow()
		if monoErr != nil {
			monoSec = 0
		}
		now := deps.now()
		assessment := monitor.Assess(unit, statePtr, stateReadErr, opts, now, monoSec)

		transition := monitor.TransitionNone
		var queuedEvent *monitor.NotificationEvent
		var activeIncident *monitor.Incident

		if alertStatePath != "" {
			store, prev, exists, err := monitor.OpenProbeState(alertStatePath)
			if err != nil {
				return err
			}
			defer store.Close()
			if exists && prev.Instance != instance {
				return e.New(e.CategoryConfig, "PROBE_STATE_MISMATCH", "Alert state file belongs to a different listener instance.")
			}
			next, ev, tr := monitor.EvaluateTransition(instance, unitName, assessment, prev, opts, now)
			if ev != nil && outboxDir != "" {
				if _, err := monitor.EnqueueOutbox(outboxDir, *ev); err != nil {
					return err
				}
			}
			if err := store.Save(next); err != nil {
				return err
			}
			transition = tr
			queuedEvent = ev
			activeIncident = next.Incident
		}

		report := map[string]any{
			"instance":      instance,
			"unit":          unitName,
			"ok":            assessment.OK,
			"reason":        assessment.Reason,
			"failure_class": string(assessment.FailureClass),
			"restarts":      assessment.Restarts,
			"transition":    transition,
			"notified":      queuedEvent != nil && outboxDir != "",
		}
		if assessment.FreshnessAgeSeconds != nil {
			report["freshness_age_seconds"] = *assessment.FreshnessAgeSeconds
		}
		if activeIncident != nil {
			report["incident"] = activeIncident
		}
		if queuedEvent != nil {
			report["event"] = queuedEvent
		}
		if !assessment.OK {
			return e.New(e.CategoryConflict, "LISTENER_UNHEALTHY", "Listener check failed: "+assessment.Reason).WithDetails(report)
		}
		return s.emit(cmd, report)
	}
	return cmd
}

func (s *appState) relayNotifyCommand() *cobra.Command {
	var outboxDir, lockPath, execPath string
	var execTimeout time.Duration
	var maxEvents int

	cmd := &cobra.Command{
		Use:   "notify [-- ARGS...]",
		Short: "Drain queued listener alert and recovery events in FIFO order",
		Args:  cobra.ArbitraryArgs,
		Long: "Deliver immutable edge notification events queued by `relay check --outbox`\n" +
			"in strict sequence order. Each event file is removed only after delivery\n" +
			"succeeds; on failure, draining stops immediately so no alert or recovery\n" +
			"is dropped or reordered. With --exec, each event's sanitized JSON is piped\n" +
			"to stdin of the fixed executable (arguments after -- are forwarded).",
		Example: "  wecom-channel-cli relay notify --outbox /var/lib/wecom-health-relay-engineering/outbox --lock /run/wecom-notify-relay-engineering/notify.lock --exec /usr/bin/logger -- --priority daemon.err --tag wecom-channel-alert",
	}

	f := cmd.Flags()
	f.StringVar(&outboxDir, "outbox", "", "Private FIFO directory written by relay check --outbox")
	f.StringVar(&lockPath, "lock", "", "Lock file path outside --outbox (defaults to <parent>/.notify.lock)")
	f.StringVar(&execPath, "exec", "", "Fixed absolute executable invoked once per queued event with JSON on stdin")
	f.DurationVar(&execTimeout, "exec-timeout", 30*time.Second, "Per-event execution deadline for --exec (1ms–2m)")
	f.IntVar(&maxEvents, "max-events", 100, "Maximum queued events to drain in one run (1–1000)")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if outboxDir == "" {
			return e.New(e.CategoryUsage, "BAD_OUTBOX", "Supply --outbox pointing to the private alert event directory.")
		}
		if execPath == "" && len(args) > 0 {
			return e.New(e.CategoryUsage, "BAD_ARGS", "Extra arguments require --exec.")
		}

		var deliver func(context.Context, monitor.NotificationEvent, []byte) error
		if execPath != "" {
			var err error
			deliver, err = monitor.ExecDeliverer(execPath, args, execTimeout)
			if err != nil {
				return err
			}
		} else {
			deliver = func(_ context.Context, _ monitor.NotificationEvent, raw []byte) error {
				if s.format == "ndjson" {
					_, err := fmt.Fprintln(cmd.OutOrStdout(), string(bytes.TrimSpace(raw)))
					return err
				}
				return nil
			}
		}

		res, err := monitor.DrainOutbox(cmd.Context(), outboxDir, lockPath, maxEvents, deliver)
		if err != nil {
			return e.Safe(err).WithDetails(res)
		}
		if execPath == "" && s.format == "ndjson" {
			return nil
		}
		return s.emit(cmd, res)
	}
	return cmd
}
