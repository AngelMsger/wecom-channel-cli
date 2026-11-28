package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func activeUnit() UnitStatus {
	return UnitStatus{
		LoadState:                  "loaded",
		ActiveState:                "active",
		SubState:                   "running",
		ActiveEnterMonotonicMicros: 1_000_000, // 1s monotonic
		NRestarts:                  0,
	}
}

func healthyPollState(now time.Time) relay.State {
	ms := now.Add(-1 * time.Second).UnixMilli()
	return relay.State{
		Version:      1,
		Binding:      "binding-1",
		StartedAt:    now.Add(-1 * time.Hour).UnixMilli(),
		QueueLimit:   1000,
		Coverage:     broker.WatchCoverage{Backend: "poll", PollIntervalSeconds: 30},
		LastScan:     ms,
		LastSnapshot: ms,
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestParseSystemdShow(t *testing.T) {
	u, err := ParseSystemdShow([]byte("LoadState=loaded\nActiveState=active\nSubState=running\nActiveEnterTimestampMonotonic=1234567\nNRestarts=2\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.LoadState != "loaded" || u.ActiveState != "active" || u.SubState != "running" || u.ActiveEnterMonotonicMicros != 1234567 || u.NRestarts != 2 {
		t.Fatalf("unexpected unit: %+v", u)
	}

	missing, err := ParseSystemdShow([]byte("LoadState=not-found\nActiveState=inactive\nSubState=dead\nActiveEnterTimestampMonotonic=0\nNRestarts=0\n"))
	if err != nil {
		t.Fatalf("not-found should parse cleanly: %v", err)
	}
	if missing.LoadState != "not-found" {
		t.Fatalf("got LoadState=%q, want not-found", missing.LoadState)
	}

	if _, err := ParseSystemdShow([]byte("ActiveState=active\n")); err == nil {
		t.Fatal("expected error when LoadState is missing")
	}
	if _, err := ParseSystemdShow([]byte("LoadState=loaded\nNRestarts=bad\n")); err == nil {
		t.Fatal("expected error on invalid NRestarts")
	}
}

func TestAssessFreshnessDefaultsAndCustomSLO(t *testing.T) {
	now := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()

	// Freshness gap under 10m (even past the old 155s cycle bound) stays healthy/ok.
	for _, lag := range []time.Duration{1 * time.Second, 156 * time.Second, 5 * time.Minute, 9*time.Minute + 59*time.Second} {
		st := healthyPollState(now)
		st.LastScan = now.Add(-lag).UnixMilli()
		st.LastSnapshot = now.Add(-lag).UnixMilli()
		a := Assess(activeUnit(), &st, false, opts, now, 1000)
		if !a.OK || a.FailureClass != ClassNone {
			t.Fatalf("lag=%v: got ok=%v reason=%s class=%s, want ok=true", lag, a.OK, a.Reason, a.FailureClass)
		}
	}

	// Crossing default 10m threshold marks scan_stale (ClassFreshness).
	st := healthyPollState(now)
	st.LastScan = now.Add(-10*time.Minute - time.Second).UnixMilli()
	st.LastSnapshot = now.Add(-1 * time.Second).UnixMilli()
	a := Assess(activeUnit(), &st, false, opts, now, 1000)
	if a.OK || a.Reason != "scan_stale" || a.FailureClass != ClassFreshness {
		t.Fatalf("10m01s lag: got %+v, want scan_stale/freshness", a)
	}

	// Raising only --freshness-threshold to 30m lets scan_stale, snapshot_stale,
	// and scan_outage all inherit 30m without code changes.
	longSLO := DefaultOptions()
	longSLO.FreshnessThreshold = 30 * time.Minute
	st20 := healthyPollState(now)
	st20.LastScan = now.Add(-20 * time.Minute).UnixMilli()
	st20.LastSnapshot = now.Add(-20 * time.Minute).UnixMilli()
	st20.FirstFailure = now.Add(-20 * time.Minute).UnixMilli()
	st20.ScanFailures = 3
	st20.LastScanError = &relay.ErrorRecord{Code: "SCAN_TIMEOUT", RetryLimit: -1}
	if a := Assess(activeUnit(), &st20, false, longSLO, now, 1000); !a.OK || a.Reason != "retry_wait" {
		t.Fatalf("20m under 30m SLO: got %+v, want ok/retry_wait", a)
	}

	st31 := st20
	st31.FirstFailure = now.Add(-31 * time.Minute).UnixMilli()
	if a := Assess(activeUnit(), &st31, false, longSLO, now, 1000); a.OK || a.Reason != "scan_outage" || a.FailureClass != ClassFreshness {
		t.Fatalf("31m outage under 30m SLO: got %+v, want scan_outage/freshness", a)
	}

	// Explicit --snapshot-age override still applies independently.
	override := longSLO
	override.SnapshotAge = 5 * time.Minute
	stSnap := healthyPollState(now)
	stSnap.LastScan = now.Add(-1 * time.Minute).UnixMilli()
	stSnap.LastSnapshot = now.Add(-6 * time.Minute).UnixMilli()
	if a := Assess(activeUnit(), &stSnap, false, override, now, 1000); a.OK || a.Reason != "snapshot_stale" || a.FailureClass != ClassFreshness {
		t.Fatalf("explicit snapshot-age override: got %+v, want snapshot_stale/freshness", a)
	}

	// Quiet callback listener has no LastSnapshot requirement.
	cb := healthyPollState(now)
	cb.Coverage = broker.WatchCoverage{Backend: "callback"}
	cb.LastSnapshot = 0
	if a := Assess(activeUnit(), &cb, false, opts, now, 1000); !a.OK || a.Reason != "healthy" {
		t.Fatalf("callback listener: got %+v, want healthy", a)
	}
}

func TestAssessBackoffDoesNotMaskScanStale(t *testing.T) {
	// Reproduces Codex review simulation:
	// 1) last_scan_at already exceeded threshold -> scan_stale.
	// 2) One minute later, still no new successful scan, only a failed attempt
	//    scheduled next_scan_at in the future -> MUST remain scan_stale, never
	//    flip back to retry_wait.
	now := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()
	opts.FreshnessThreshold = 155 * time.Second

	st := healthyPollState(now)
	st.LastScan = now.Add(-200 * time.Second).UnixMilli()
	st.LastSnapshot = now.Add(-10 * time.Second).UnixMilli()
	first := Assess(activeUnit(), &st, false, opts, now, 1000)
	if first.OK || first.Reason != "scan_stale" {
		t.Fatalf("t=0: got %+v, want scan_stale", first)
	}

	later := now.Add(60 * time.Second)
	st.FirstFailure = later.UnixMilli()
	st.ScanFailures = 1
	st.NextScan = later.Add(5 * time.Minute).UnixMilli()
	st.LastScanError = &relay.ErrorRecord{Code: "SCAN_TIMEOUT", RetryLimit: -1}
	second := Assess(activeUnit(), &st, false, opts, later, 1060)
	if second.OK || second.Reason != "scan_stale" || second.FailureClass != ClassFreshness {
		t.Fatalf("t+60s during backoff: got %+v, want scan_stale/freshness (must not mask into retry_wait)", second)
	}
}

func TestAssessHardFailuresIndependentOfFreshnessThreshold(t *testing.T) {
	now := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()
	opts.FreshnessThreshold = 2 * time.Hour // very long freshness SLO

	// 1. Missing/uninstalled expected service -> service_missing (hard), not probe_failed.
	missingUnit := UnitStatus{LoadState: "not-found", ActiveState: "inactive", SubState: "dead"}
	if a := Assess(missingUnit, nil, false, opts, now, 1000); a.OK || a.Reason != "service_missing" || a.FailureClass != ClassHard {
		t.Fatalf("missing unit: got %+v, want service_missing/hard", a)
	}
	pausedOpts := opts
	pausedOpts.Paused = true
	if a := Assess(missingUnit, nil, false, pausedOpts, now, 1000); a.OK || a.Reason != "service_missing" || a.FailureClass != ClassHard {
		t.Fatalf("missing unit even with --paused: got %+v, want service_missing/hard", a)
	}

	// 2. Service down -> relay_down (hard) immediately, even with recent state.
	downUnit := UnitStatus{LoadState: "loaded", ActiveState: "inactive", SubState: "dead"}
	st := healthyPollState(now)
	if a := Assess(downUnit, &st, false, opts, now, 1000); a.OK || a.Reason != "relay_down" || a.FailureClass != ClassHard {
		t.Fatalf("down unit: got %+v, want relay_down/hard", a)
	}
	if a := Assess(downUnit, &st, false, pausedOpts, now, 1000); !a.OK || a.Reason != "paused" {
		t.Fatalf("paused inactive unit: got %+v, want paused", a)
	}
	if a := Assess(activeUnit(), &st, false, pausedOpts, now, 1000); a.OK || a.Reason != "pause_mismatch" || a.FailureClass != ClassHard {
		t.Fatalf("paused active unit: got %+v, want pause_mismatch/hard", a)
	}

	// 3. Blocked scan / terminal delivery error / delivery_unknown -> immediate hard.
	blocked := healthyPollState(now)
	blocked.ScanBlocked = e.CategoryAuth
	if a := Assess(activeUnit(), &blocked, false, opts, now, 1000); a.OK || a.Reason != "blocked" || a.FailureClass != ClassHard {
		t.Fatalf("blocked scan: got %+v, want blocked/hard", a)
	}

	termDel := healthyPollState(now)
	termDel.Inflight = &relay.Delivery{Failure: "RELAY_REJECTED"}
	if a := Assess(activeUnit(), &termDel, false, opts, now, 1000); a.OK || a.Reason != "blocked" || a.FailureClass != ClassHard {
		t.Fatalf("terminal delivery error: got %+v, want blocked/hard", a)
	}

	unk := healthyPollState(now)
	unk.Inflight = &relay.Delivery{Status: "unknown"}
	if a := Assess(activeUnit(), &unk, false, opts, now, 1000); a.OK || a.Reason != "delivery_unknown" || a.FailureClass != ClassHard {
		t.Fatalf("delivery unknown: got %+v, want delivery_unknown/hard", a)
	}

	// 4. Active BROKER_UNREACHABLE -> immediate broker_unavailable (hard), even under 2h freshness SLO.
	brokerDown := healthyPollState(now)
	brokerDown.FirstFailure = now.Add(-10 * time.Second).UnixMilli()
	brokerDown.ScanFailures = 1
	brokerDown.NextScan = now.Add(1 * time.Minute).UnixMilli()
	brokerDown.LastScanError = &relay.ErrorRecord{Code: "BROKER_UNREACHABLE", RetryLimit: -1}
	if a := Assess(activeUnit(), &brokerDown, false, opts, now, 1000); a.OK || a.Reason != "broker_unavailable" || a.FailureClass != ClassHard {
		t.Fatalf("active BROKER_UNREACHABLE: got %+v, want broker_unavailable/hard", a)
	}

	// Historical BROKER_UNREACHABLE after scan recovery MUST NOT false-alarm.
	brokerRecovered := healthyPollState(now)
	brokerRecovered.LastScanError = &relay.ErrorRecord{Code: "BROKER_UNREACHABLE", RetryLimit: -1}
	if a := Assess(activeUnit(), &brokerRecovered, false, opts, now, 1000); !a.OK || a.Reason != "healthy" {
		t.Fatalf("historical BROKER_UNREACHABLE after recovery: got %+v, want healthy", a)
	}

	// 5. Queue distinction: 1 fresh queued item does NOT alert; >300s queue_age alerts; full queue alerts immediately.
	freshQueue := healthyPollState(now)
	freshQueue.Pending = []relay.Record{{Data: callback.Event{ID: "ev-1"}}}
	freshQueue.PendingSince = now.Add(-10 * time.Second).UnixMilli()
	if a := Assess(activeUnit(), &freshQueue, false, opts, now, 1000); !a.OK || a.Reason != "healthy" {
		t.Fatalf("single fresh queued item: got %+v, want healthy", a)
	}
	oldQueue := freshQueue
	oldQueue.PendingSince = now.Add(-301 * time.Second).UnixMilli()
	if a := Assess(activeUnit(), &oldQueue, false, opts, now, 1000); a.OK || a.Reason != "queue_old" || a.FailureClass != ClassHard {
		t.Fatalf("aged queued item (>300s): got %+v, want queue_old/hard", a)
	}
	fullQueue := healthyPollState(now)
	fullQueue.QueueLimit = 2
	fullQueue.Pending = []relay.Record{{Data: callback.Event{ID: "1"}}, {Data: callback.Event{ID: "2"}}}
	fullQueue.PendingSince = now.Add(-1 * time.Second).UnixMilli()
	if a := Assess(activeUnit(), &fullQueue, false, opts, now, 1000); a.OK || a.Reason != "queue_full" || a.FailureClass != ClassHard {
		t.Fatalf("full queue: got %+v, want queue_full/hard", a)
	}

	// 6. Startup deadline stays 300s regardless of 2h freshness threshold.
	uninit := healthyPollState(now)
	uninit.Binding = ""
	uninit.StartedAt = now.Add(-200 * time.Second).UnixMilli()
	if a := Assess(activeUnit(), &uninit, false, opts, now, 1000); !a.OK || a.Reason != "starting" {
		t.Fatalf("within startup: got %+v, want starting", a)
	}
	uninit.StartedAt = now.Add(-305 * time.Second).UnixMilli()
	if a := Assess(activeUnit(), &uninit, false, opts, now, 1000); a.OK || a.Reason != "baseline_overdue" || a.FailureClass != ClassHard {
		t.Fatalf("past startup: got %+v, want baseline_overdue/hard", a)
	}
	if a := Assess(activeUnit(), nil, false, opts, now, 303); a.OK || a.Reason != "state_unavailable" || a.FailureClass != ClassHard {
		t.Fatalf("missing state past startup: got %+v, want state_unavailable/hard", a)
	}
	if a := Assess(activeUnit(), nil, true, opts, now, 10); a.OK || a.Reason != "state_unavailable" || a.FailureClass != ClassHard {
		t.Fatalf("corrupt state even during startup: got %+v, want state_unavailable/hard", a)
	}
}

func TestEdgeTransitionsAcceptanceScenarios(t *testing.T) {
	base := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()
	unitName := "wecom-channel-relay@engineering.service"

	// Scenario A: Short freshness gap (<10m) then recovery -> 0 notifications.
	var ps ProbeState
	st := healthyPollState(base)
	for _, offset := range []time.Duration{0, 3 * time.Minute, 8 * time.Minute} {
		now := base.Add(offset)
		a := Assess(activeUnit(), &st, false, opts, now, 1000+offset.Seconds())
		var ev *NotificationEvent
		var tr string
		ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, now)
		if !a.OK || ev != nil || tr != TransitionNone {
			t.Fatalf("short gap at %v: ok=%v tr=%s ev=%+v", offset, a.OK, tr, ev)
		}
	}
	// Recover at 9m.
	now := base.Add(9 * time.Minute)
	st.LastScan = now.UnixMilli()
	st.LastSnapshot = now.UnixMilli()
	a := Assess(activeUnit(), &st, false, opts, now, 1600)
	var ev *NotificationEvent
	var tr string
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, now)
	if !a.OK || ev != nil || tr != TransitionNone {
		t.Fatalf("recovery from unnotified short gap must emit nothing: tr=%s ev=%+v", tr, ev)
	}

	// Scenario B: Gap exceeds 10m -> 1 alert; continuing stale + backoff -> 0;
	// escalates to relay_down -> 1 escalated; continuing relay_down -> 0;
	// restart starting -> 0; quiet-channel fresh scan -> 1 recovered; next healthy -> 0.
	t0 := base.Add(20 * time.Minute)
	st = healthyPollState(t0) // LastScan & LastSnapshot at t0 - 1s
	tStale1 := t0.Add(11 * time.Minute)
	a = Assess(activeUnit(), &st, false, opts, tStale1, 2000)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ProbeState{}, opts, tStale1)
	if a.OK || tr != TransitionAlert || ev == nil || ev.Reason != "scan_stale" || ev.FailureClass != ClassFreshness {
		t.Fatalf("first crossing: ok=%v tr=%s ev=%+v", a.OK, tr, ev)
	}

	// Continuing stale 1 minute later -> STILL ok=false, 0 notifications.
	tStale2 := tStale1.Add(time.Minute)
	a = Assess(activeUnit(), &st, false, opts, tStale2, 2060)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tStale2)
	if a.OK || ev != nil || tr != TransitionNone {
		t.Fatalf("continuing stale must preserve ok=false and emit nothing: ok=%v tr=%s ev=%+v", a.OK, tr, ev)
	}

	// Backoff scheduled while still stale -> STILL ok=false, 0 notifications.
	tBackoff := tStale2.Add(time.Minute)
	st.FirstFailure = tBackoff.UnixMilli()
	st.ScanFailures = 1
	st.NextScan = tBackoff.Add(5 * time.Minute).UnixMilli()
	st.LastScanError = &relay.ErrorRecord{Code: "SCAN_TIMEOUT", RetryLimit: -1}
	a = Assess(activeUnit(), &st, false, opts, tBackoff, 2120)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tBackoff)
	if a.OK || a.Reason != "scan_stale" || ev != nil || tr != TransitionNone {
		t.Fatalf("backoff while stale must not recover or re-alert: ok=%v reason=%s tr=%s ev=%+v", a.OK, a.Reason, tr, ev)
	}

	// Deteriorates into relay_down -> exactly 1 escalation notification.
	tDown1 := tBackoff.Add(time.Minute)
	downUnit := UnitStatus{LoadState: "loaded", ActiveState: "inactive", SubState: "dead", NRestarts: 1}
	a = Assess(downUnit, nil, false, opts, tDown1, 2180)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tDown1)
	if a.OK || tr != TransitionEscalated || ev == nil || ev.Reason != "relay_down" || ev.PreviousReason != "scan_stale" || ev.FailureClass != ClassHard {
		t.Fatalf("escalation to relay_down: ok=%v tr=%s ev=%+v", a.OK, tr, ev)
	}

	// Continuing relay_down -> 0 further notifications (escalates only once!).
	tDown2 := tDown1.Add(time.Minute)
	a = Assess(downUnit, nil, false, opts, tDown2, 2240)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tDown2)
	if a.OK || ev != nil || tr != TransitionNone {
		t.Fatalf("continuing relay_down must emit nothing: ok=%v tr=%s ev=%+v", a.OK, tr, ev)
	}

	// Restarting (starting, state not ready yet) -> 0 recovery notifications.
	tStart := tDown2.Add(time.Minute)
	restartedUnit := UnitStatus{LoadState: "loaded", ActiveState: "active", SubState: "running", ActiveEnterMonotonicMicros: 2_295_000_000, NRestarts: 1}
	a = Assess(restartedUnit, nil, false, opts, tStart, 2300)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tStart)
	if !a.OK || a.Reason != "starting" || ev != nil || tr != TransitionNone || ps.Incident == nil {
		t.Fatalf("starting must not prematurely clear incident: a=%+v tr=%s ev=%+v inc=%+v", a, tr, ev, ps.Incident)
	}

	// Genuine recovery on a quiet channel (0 new events, LastScan & LastSnapshot advance, failures cleared).
	tRec := tStart.Add(time.Minute)
	st.ResetScan()
	st.LastScan = tRec.UnixMilli()
	st.LastSnapshot = tRec.UnixMilli()
	a = Assess(restartedUnit, &st, false, opts, tRec, 2360)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tRec)
	if !a.OK || tr != TransitionRecovered || ev == nil || ev.PreviousReason != "relay_down" || ev.DurationSeconds <= 0 || ps.Incident != nil {
		t.Fatalf("genuine recovery: a=%+v tr=%s ev=%+v inc=%+v", a, tr, ev, ps.Incident)
	}

	// Subsequent healthy probe -> 0 notifications.
	tAfter := tRec.Add(time.Minute)
	st.LastScan = tAfter.UnixMilli()
	st.LastSnapshot = tAfter.UnixMilli()
	a = Assess(restartedUnit, &st, false, opts, tAfter, 2420)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, tAfter)
	if !a.OK || ev != nil || tr != TransitionNone {
		t.Fatalf("post-recovery healthy probe must emit nothing: tr=%s ev=%+v", tr, ev)
	}
}

func TestSnapshotStaleRecoveryRequiresNewCommittedSnapshot(t *testing.T) {
	base := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()
	unitName := "wecom-channel-relay@engineering.service"

	// LastScan is recent (e.g. inbox drain), but LastSnapshot exceeded 10m -> snapshot_stale.
	st := healthyPollState(base)
	st.LastSnapshot = base.Add(-11 * time.Minute).UnixMilli()
	st.LastScan = base.Add(-10 * time.Second).UnixMilli()
	st.Progress = &broker.PollProgress{NextAt: base.Add(time.Minute).UnixMilli(), Requests: 4}

	a := Assess(activeUnit(), &st, false, opts, base, 1000)
	ps, ev, tr := EvaluateTransition("engineering", unitName, a, ProbeState{}, opts, base)
	if a.OK || a.Reason != "snapshot_stale" || tr != TransitionAlert || ev == nil {
		t.Fatalf("snapshot_stale open: a=%+v tr=%s ev=%+v", a, tr, ev)
	}

	// Widening threshold without committing a new snapshot MUST NOT emit recovered.
	wide := opts
	wide.FreshnessThreshold = 30 * time.Minute
	t1 := base.Add(time.Minute)
	st.LastScan = t1.UnixMilli()
	a = Assess(activeUnit(), &st, false, wide, t1, 1060)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, wide, t1)
	if ev != nil || tr != TransitionNone || ps.Incident == nil {
		t.Fatalf("must not recover snapshot_stale without newer LastSnapshot: a=%+v tr=%s ev=%+v", a, tr, ev)
	}

	// Committing a new full snapshot (even on a quiet channel with 0 new messages) recovers cleanly.
	t2 := t1.Add(time.Minute)
	st.Progress = nil
	st.LastScan = t2.UnixMilli()
	st.LastSnapshot = t2.UnixMilli()
	a = Assess(activeUnit(), &st, false, opts, t2, 1120)
	ps, ev, tr = EvaluateTransition("engineering", unitName, a, ps, opts, t2)
	if !a.OK || tr != TransitionRecovered || ev == nil || ev.PreviousReason != "snapshot_stale" {
		t.Fatalf("new committed snapshot must recover: a=%+v tr=%s ev=%+v", a, tr, ev)
	}
}

func TestHardFailureImmediateAndSeparateDelaySetting(t *testing.T) {
	base := time.Unix(1_764_120_000, 0)
	opts := DefaultOptions()
	opts.FreshnessThreshold = 2 * time.Hour
	unitName := "wecom-channel-relay@engineering.service"

	// Default HardFailureThreshold=0 alerts on first observation.
	down := UnitStatus{LoadState: "loaded", ActiveState: "inactive", SubState: "dead"}
	a := Assess(down, nil, false, opts, base, 1000)
	ps, ev, tr := EvaluateTransition("engineering", unitName, a, ProbeState{}, opts, base)
	if tr != TransitionAlert || ev == nil || ev.Reason != "relay_down" {
		t.Fatalf("immediate hard failure: tr=%s ev=%+v", tr, ev)
	}
	if ps.Incident == nil || ps.Incident.Class != ClassHard {
		t.Fatalf("expected active hard incident, got %+v", ps.Incident)
	}

	// Separate optional HardFailureThreshold=2m does not inherit 2h FreshnessThreshold.
	delayed := opts
	delayed.HardFailureThreshold = 2 * time.Minute
	ps2, ev2, tr2 := EvaluateTransition("engineering", unitName, a, ProbeState{}, delayed, base)
	if ev2 != nil || tr2 != TransitionNone || ps2.PendingHard == nil {
		t.Fatalf("first observation under 2m hard threshold should wait: tr=%s ev=%+v pending=%+v", tr2, ev2, ps2.PendingHard)
	}
	ps2, ev2, tr2 = EvaluateTransition("engineering", unitName, a, ps2, delayed, base.Add(2*time.Minute))
	if tr2 != TransitionAlert || ev2 == nil || ev2.Reason != "relay_down" {
		t.Fatalf("after 2m hard threshold should alert: tr=%s ev=%+v", tr2, ev2)
	}
}

func TestOutboxFIFOPreservesAlertAndRecoveryWhenNotifierStopped(t *testing.T) {
	root := canonicalTempDir(t)
	stateFile := filepath.Join(root, "probe-state.json")
	outboxDir := filepath.Join(root, "outbox")
	lockFile := filepath.Join(root, "notify.lock")

	now := time.Unix(1_764_120_000, 0)
	ev1 := NotificationEvent{Version: 1, Sequence: 1, ID: "eng-1", Instance: "engineering", Unit: "wecom-channel-relay@engineering.service", Transition: TransitionAlert, FailureClass: ClassFreshness, Reason: "scan_stale", OpenedAt: now.Unix(), OccurredAt: now.Unix()}
	ev2 := NotificationEvent{Version: 1, Sequence: 2, ID: "eng-2", Instance: "engineering", Unit: "wecom-channel-relay@engineering.service", Transition: TransitionRecovered, FailureClass: ClassNone, Reason: "healthy", PreviousReason: "scan_stale", OpenedAt: now.Unix(), OccurredAt: now.Add(5 * time.Minute).Unix(), DurationSeconds: 300}

	// Simulate notifier stopped: enqueue both alert and recovery before draining.
	if _, err := EnqueueOutbox(outboxDir, ev1); err != nil {
		t.Fatalf("enqueue ev1: %v", err)
	}
	if _, err := EnqueueOutbox(outboxDir, ev2); err != nil {
		t.Fatalf("enqueue ev2: %v", err)
	}
	// Duplicate sequence must refuse overwrite.
	if _, err := EnqueueOutbox(outboxDir, ev1); err == nil {
		t.Fatal("expected duplicate sequence overwrite refusal")
	}

	// Lock inside outbox must be rejected so DirectoryNotEmpty stays accurate.
	if _, err := DrainOutbox(context.Background(), outboxDir, filepath.Join(outboxDir, ".lock"), 10, func(context.Context, NotificationEvent, []byte) error { return nil }); err == nil {
		t.Fatal("expected BAD_LOCK_PATH when lock is inside outbox")
	}

	// First drain fails on ev2: ev1 is delivered and removed, ev2 stays queued.
	var delivered []string
	res, err := DrainOutbox(context.Background(), outboxDir, lockFile, 10, func(_ context.Context, ev NotificationEvent, _ []byte) error {
		if ev.Sequence == 2 {
			return errors.New("receiver unavailable")
		}
		delivered = append(delivered, ev.Transition)
		return nil
	})
	if err == nil || res.Delivered != 1 || res.Remaining != 1 {
		t.Fatalf("partial drain: res=%+v err=%v", res, err)
	}

	// Restart notifier: ev2 is delivered next and outbox becomes completely empty (even no dotfiles).
	res, err = DrainOutbox(context.Background(), outboxDir, lockFile, 10, func(_ context.Context, ev NotificationEvent, _ []byte) error {
		delivered = append(delivered, ev.Transition)
		return nil
	})
	if err != nil || res.Delivered != 1 || res.Remaining != 0 {
		t.Fatalf("second drain: res=%+v err=%v", res, err)
	}
	if len(delivered) != 2 || delivered[0] != TransitionAlert || delivered[1] != TransitionRecovered {
		t.Fatalf("unexpected delivery order: %v", delivered)
	}
	entries, err := os.ReadDir(outboxDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected completely empty outbox after drain, entries=%v err=%v", entries, err)
	}

	// ProbeState store round-trip and symlink refusal.
	store, _, exists, err := OpenProbeState(stateFile)
	if err != nil || exists {
		t.Fatalf("open fresh probe state: exists=%v err=%v", exists, err)
	}
	if err := store.Save(ProbeState{Version: 1, Instance: "engineering", NextSeq: 3, LastCheckedAt: now.Unix()}); err != nil {
		t.Fatalf("save probe state: %v", err)
	}
	store.Close()

	store2, loaded, exists, err := OpenProbeState(stateFile)
	if err != nil || !exists || loaded.NextSeq != 3 || loaded.Instance != "engineering" {
		t.Fatalf("reload probe state: exists=%v loaded=%+v err=%v", exists, loaded, err)
	}
	store2.Close()

	symOutbox := filepath.Join(root, "sym-outbox")
	if err := os.Symlink(outboxDir, symOutbox); err != nil {
		t.Fatal(err)
	}
	if _, err := EnqueueOutbox(symOutbox, ev1); err == nil {
		t.Fatal("expected symlink outbox to be rejected")
	}
}
