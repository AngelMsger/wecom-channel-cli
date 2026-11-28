package monitor

import (
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// FailureClass separates freshness-degradation alerts from immediate hard failures.
type FailureClass string

const (
	ClassNone      FailureClass = "none"
	ClassFreshness FailureClass = "freshness"
	ClassHard      FailureClass = "hard"
)

// Options configures listener health assessment and notification thresholds.
type Options struct {
	Paused               bool
	FreshnessThreshold   time.Duration
	Outage               time.Duration // 0 inherits FreshnessThreshold
	SnapshotAge          time.Duration // 0 inherits FreshnessThreshold
	HardFailureThreshold time.Duration // 0 alerts on first observation
	Interval             time.Duration
	ScanTimeout          time.Duration
	Grace                time.Duration
	Startup              time.Duration
	QueueAge             time.Duration
	QueueLimit           int
}

// DefaultOptions returns the product defaults: a 10-minute freshness alert
// threshold with immediate hard-failure alerting and preserved v0.5.0 queue,
// startup, and scan-cycle bounds.
func DefaultOptions() Options {
	return Options{
		FreshnessThreshold:   10 * time.Minute,
		Outage:               0,
		SnapshotAge:          0,
		HardFailureThreshold: 0,
		Interval:             30 * time.Second,
		ScanTimeout:          65 * time.Second,
		Grace:                60 * time.Second,
		Startup:              300 * time.Second,
		QueueAge:             300 * time.Second,
		QueueLimit:           1000,
	}
}

// Validate rejects non-positive required bounds or negative overrides.
func (o Options) Validate() error {
	if o.FreshnessThreshold <= 0 || o.Interval <= 0 || o.ScanTimeout <= 0 || o.Grace <= 0 || o.Startup <= 0 || o.QueueAge <= 0 || o.QueueLimit <= 0 {
		return e.New(e.CategoryUsage, "BAD_PROBE_OPTIONS", "Probe thresholds and limits must be positive.")
	}
	if o.Outage < 0 || o.SnapshotAge < 0 || o.HardFailureThreshold < 0 {
		return e.New(e.CategoryUsage, "BAD_PROBE_OPTIONS", "Probe threshold overrides must not be negative.")
	}
	return nil
}

// EffectiveOutage returns the explicit outage threshold or inherits FreshnessThreshold.
func (o Options) EffectiveOutage() time.Duration {
	if o.Outage > 0 {
		return o.Outage
	}
	return o.FreshnessThreshold
}

// EffectiveSnapshotAge returns the explicit polling snapshot threshold or inherits FreshnessThreshold.
func (o Options) EffectiveSnapshotAge() time.Duration {
	if o.SnapshotAge > 0 {
		return o.SnapshotAge
	}
	return o.FreshnessThreshold
}

// Assessment is the live, unfiltered result of a single listener probe.
type Assessment struct {
	OK                  bool         `json:"ok"`
	Reason              string       `json:"reason"`
	FailureClass        FailureClass `json:"failure_class"`
	Restarts            int          `json:"restarts"`
	FreshnessAgeSeconds *int64       `json:"freshness_age_seconds,omitempty"`
	LastScanAt          int64        `json:"-"`
	LastSnapshotAt      int64        `json:"-"`
	HasActiveFailure    bool         `json:"-"`
}

// Assess evaluates one expected listener from its systemd unit status and local
// relay state. It always reports the true current health; notification
// deduplication happens separately in EvaluateTransition.
func Assess(unit UnitStatus, state *relay.State, stateReadErr bool, opts Options, now time.Time, monotonicSec float64) Assessment {
	nowMs := now.UnixMilli()
	res := Assessment{Restarts: unit.NRestarts}
	if state != nil {
		res.LastScanAt = state.LastScan
		res.LastSnapshotAt = state.LastSnapshot
		res.HasActiveFailure = state.FirstFailure > 0 || state.ScanFailures > 0
		if state.LastScan > 0 {
			age := max(int64(0), (nowMs-state.LastScan)/1000)
			res.FreshnessAgeSeconds = &age
		}
	}
	ok := func(reason string) Assessment {
		res.OK = true
		res.Reason = reason
		res.FailureClass = ClassNone
		return res
	}
	fail := func(reason string, class FailureClass) Assessment {
		res.OK = false
		res.Reason = reason
		res.FailureClass = class
		return res
	}

	// An expected service that is uninstalled, masked, or unloaded is an
	// immediate hard failure rather than a generic probe error.
	if unit.LoadState != "loaded" {
		return fail("service_missing", ClassHard)
	}
	if opts.Paused {
		if unit.ActiveState == "inactive" {
			return ok("paused")
		}
		return fail("pause_mismatch", ClassHard)
	}
	if unit.ActiveState != "active" || unit.SubState != "running" {
		return fail("relay_down", ClassHard)
	}

	uptimeSec := -1.0
	if unit.ActiveEnterMonotonicMicros > 0 && monotonicSec > 0 {
		uptimeSec = monotonicSec - float64(unit.ActiveEnterMonotonicMicros)/1_000_000.0
	}
	if state == nil {
		if !stateReadErr && uptimeSec >= 0 && uptimeSec < opts.Startup.Seconds() {
			return ok("starting")
		}
		return fail("state_unavailable", ClassHard)
	}
	if state.ScanState() == "blocked" || (state.Inflight != nil && state.Inflight.Failure != "") {
		return fail("blocked", ClassHard)
	}
	if state.Inflight != nil && state.Inflight.Status == "unknown" {
		return fail("delivery_unknown", ClassHard)
	}
	if state.Inflight != nil && state.LastDeliveryError != nil && state.Inflight.Status == "rejected" && state.Inflight.Attempts > state.LastDeliveryError.RetryLimit {
		return fail("delivery_exhausted", ClassHard)
	}
	if state.Inflight != nil && state.Inflight.Status == "sending" && state.Inflight.LastAttempt > 0 && nowMs-state.Inflight.LastAttempt > (30*time.Second+opts.Grace).Milliseconds() {
		return fail("delivery_stalled", ClassHard)
	}

	depth := state.Depth()
	qLimit := state.QueueLimit
	if qLimit <= 0 {
		qLimit = opts.QueueLimit
	}
	if depth > 0 && depth >= qLimit {
		return fail("queue_full", ClassHard)
	}
	if depth > 0 {
		oldest := state.OldestQueuedAt()
		if oldest <= 0 {
			return fail("queue_age_unknown", ClassHard)
		}
		if nowMs-oldest > opts.QueueAge.Milliseconds() {
			return fail("queue_old", ClassHard)
		}
	}

	// Only treat BROKER_UNREACHABLE as active when the current scan failure
	// streak is open; historical LastScanError remains set after recovery.
	if state.FirstFailure > 0 && state.ScanFailures > 0 && state.LastScanError != nil && state.LastScanError.Code == "BROKER_UNREACHABLE" {
		return fail("broker_unavailable", ClassHard)
	}

	if state.Binding == "" {
		ageSec := uptimeSec
		if state.StartedAt > 0 {
			ageSec = float64(nowMs-state.StartedAt) / 1000.0
		}
		if ageSec >= 0 && ageSec < opts.Startup.Seconds() {
			return ok("starting")
		}
		return fail("baseline_overdue", ClassHard)
	}

	if state.FirstFailure > 0 && nowMs-state.FirstFailure > opts.EffectiveOutage().Milliseconds() {
		return fail("scan_outage", ClassFreshness)
	}

	pollInterval := time.Duration(state.Coverage.PollIntervalSeconds) * time.Second
	cycleBound := max(opts.Interval, pollInterval) + opts.ScanTimeout + opts.Grace
	freshnessBound := max(opts.FreshnessThreshold, cycleBound)
	// Do not allow a scheduled future retry (next_scan_at) to mask staleness
	// once last_scan_at has already exceeded the freshness bound.
	if state.LastScan <= 0 || nowMs-state.LastScan > freshnessBound.Milliseconds() {
		return fail("scan_stale", ClassFreshness)
	}

	if state.Coverage.Backend == "poll" && (state.LastSnapshot > 0 || state.QueueLimit > 0) {
		if state.LastSnapshot <= 0 || nowMs-state.LastSnapshot > opts.EffectiveSnapshotAge().Milliseconds() {
			return fail("snapshot_stale", ClassFreshness)
		}
	}

	if state.FirstFailure > 0 {
		return ok("retry_wait")
	}
	if state.ScanState() == "in_progress" {
		return ok("scanning")
	}
	return ok("healthy")
}
