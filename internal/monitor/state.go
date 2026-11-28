package monitor

import (
	"fmt"
	"time"
)

const (
	TransitionNone      = "none"
	TransitionAlert     = "alert"
	TransitionEscalated = "escalated"
	TransitionRecovered = "recovered"
)

// Incident tracks one open, already-notified listener failure so repeated
// probes emit no duplicate notifications and recovery verifies the original cause.
type Incident struct {
	Active               bool         `json:"active"`
	Class                FailureClass `json:"class"`
	Reason               string       `json:"reason"`
	PreviousReason       string       `json:"previous_reason,omitempty"`
	OpenedAt             int64        `json:"opened_at"`
	NotifiedAt           int64        `json:"notified_at"`
	EscalatedAt          int64        `json:"escalated_at,omitempty"`
	LastSeenAt           int64        `json:"last_seen_at"`
	LastReason           string       `json:"last_reason"`
	OpenedLastScanAt     int64        `json:"opened_last_scan_at,omitempty"`
	OpenedLastSnapshotAt int64        `json:"opened_last_snapshot_at,omitempty"`
}

// PendingHard tracks a hard failure waiting for an optional non-zero
// HardFailureThreshold before opening an incident.
type PendingHard struct {
	Reason      string `json:"reason"`
	FirstSeenAt int64  `json:"first_seen_at"`
}

// ProbeState persists edge-triggered alert state for one listener instance.
type ProbeState struct {
	Version       int          `json:"version"`
	Instance      string       `json:"instance"`
	NextSeq       uint64       `json:"next_seq"`
	Incident      *Incident    `json:"incident,omitempty"`
	PendingHard   *PendingHard `json:"pending_hard,omitempty"`
	LastCheckedAt int64        `json:"last_checked_at"`
}

// NotificationEvent is the immutable, sanitized edge-transition record written
// to the outbox directory only when an alert, escalation, or recovery occurs.
type NotificationEvent struct {
	Version         int          `json:"version"`
	Sequence        uint64       `json:"sequence"`
	ID              string       `json:"id"`
	Instance        string       `json:"instance"`
	Unit            string       `json:"unit"`
	Transition      string       `json:"transition"`
	FailureClass    FailureClass `json:"failure_class"`
	Reason          string       `json:"reason"`
	PreviousReason  string       `json:"previous_reason,omitempty"`
	OpenedAt        int64        `json:"opened_at,omitempty"`
	OccurredAt      int64        `json:"occurred_at"`
	DurationSeconds int64        `json:"duration_seconds,omitempty"`
	Restarts        int          `json:"restarts"`
}

// Validate verifies a decoded NotificationEvent before delivery.
func (ev NotificationEvent) Validate() bool {
	if ev.Version != 1 || ev.Sequence == 0 || ev.ID == "" || !ValidateInstanceName(ev.Instance) || ev.Reason == "" || ev.OccurredAt <= 0 {
		return false
	}
	switch ev.Transition {
	case TransitionAlert, TransitionEscalated, TransitionRecovered:
	default:
		return false
	}
	switch ev.FailureClass {
	case ClassNone, ClassFreshness, ClassHard:
		return true
	default:
		return false
	}
}

// Validate verifies a decoded ProbeState.
func (s ProbeState) Validate() bool {
	if s.Version != 1 || !ValidateInstanceName(s.Instance) || s.NextSeq == 0 {
		return false
	}
	if s.Incident != nil {
		if s.Incident.Reason == "" || s.Incident.OpenedAt <= 0 {
			return false
		}
		switch s.Incident.Class {
		case ClassFreshness, ClassHard:
		default:
			return false
		}
	}
	if s.PendingHard != nil && (s.PendingHard.Reason == "" || s.PendingHard.FirstSeenAt <= 0) {
		return false
	}
	return true
}

func cloneProbeState(prev ProbeState) ProbeState {
	next := prev
	if prev.Incident != nil {
		cp := *prev.Incident
		next.Incident = &cp
	}
	if prev.PendingHard != nil {
		cp := *prev.PendingHard
		next.PendingHard = &cp
	}
	return next
}

func needsNewScanForRecovery(reason string) bool {
	switch reason {
	case "scan_stale", "scan_outage", "broker_unavailable":
		return true
	default:
		return false
	}
}

func needsNewSnapshotForRecovery(reason string) bool {
	return reason == "snapshot_stale"
}

func isGenuineRecovery(inc *Incident, a Assessment) bool {
	if !a.OK || a.HasActiveFailure {
		return false
	}
	if a.Reason != "healthy" && a.Reason != "scanning" {
		return false
	}
	if needsNewScanForRecovery(inc.Reason) || needsNewScanForRecovery(inc.PreviousReason) {
		if a.LastScanAt <= 0 || a.LastScanAt <= inc.OpenedLastScanAt {
			return false
		}
	}
	if needsNewSnapshotForRecovery(inc.Reason) || needsNewSnapshotForRecovery(inc.PreviousReason) || needsNewSnapshotForRecovery(inc.LastReason) {
		if a.LastSnapshotAt <= 0 || a.LastSnapshotAt <= inc.OpenedLastSnapshotAt {
			return false
		}
	}
	return true
}

// EvaluateTransition computes the next ProbeState and optional edge-triggered
// NotificationEvent from one live Assessment.
//
// Policy highlights:
//   - No notification while healthy or while freshness lag stays below threshold.
//   - Exactly one "alert" notification when a threshold is first crossed.
//   - Zero notifications on unchanged continuing incidents or retry backoff.
//   - One "escalated" notification if an active freshness incident deteriorates
//     into a hard failure (for example relay_down or blocked); continuing hard
//     failure emits nothing further.
//   - Exactly one "recovered" notification once the listener genuinely recovers
//     for its original failure cause (a new completed scan or full polling
//     snapshot, including on quiet channels with zero new messages).
func EvaluateTransition(instance, unit string, a Assessment, prev ProbeState, opts Options, now time.Time) (ProbeState, *NotificationEvent, string) {
	nowSec := now.Unix()
	next := cloneProbeState(prev)
	next.Version = 1
	next.Instance = instance
	next.LastCheckedAt = nowSec
	if next.NextSeq == 0 {
		next.NextSeq = 1
	}

	qualifiesHard := func(reason string) bool {
		if opts.HardFailureThreshold <= 0 {
			return true
		}
		if next.PendingHard == nil {
			next.PendingHard = &PendingHard{Reason: reason, FirstSeenAt: nowSec}
			return false
		}
		next.PendingHard.Reason = reason
		return nowSec-next.PendingHard.FirstSeenAt >= int64(opts.HardFailureThreshold.Seconds())
	}

	makeEvent := func(transition string, class FailureClass, reason, prevReason string, openedAt, duration int64) *NotificationEvent {
		seq := next.NextSeq
		next.NextSeq++
		return &NotificationEvent{
			Version:         1,
			Sequence:        seq,
			ID:              fmt.Sprintf("%s-%020d", instance, seq),
			Instance:        instance,
			Unit:            unit,
			Transition:      transition,
			FailureClass:    class,
			Reason:          reason,
			PreviousReason:  prevReason,
			OpenedAt:        openedAt,
			OccurredAt:      nowSec,
			DurationSeconds: duration,
			Restarts:        a.Restarts,
		}
	}

	// Branch 1: No currently open incident.
	if next.Incident == nil || !next.Incident.Active {
		switch a.FailureClass {
		case ClassNone:
			next.PendingHard = nil
			return next, nil, TransitionNone
		case ClassFreshness:
			next.PendingHard = nil
			next.Incident = &Incident{
				Active:               true,
				Class:                ClassFreshness,
				Reason:               a.Reason,
				OpenedAt:             nowSec,
				NotifiedAt:           nowSec,
				LastSeenAt:           nowSec,
				LastReason:           a.Reason,
				OpenedLastScanAt:     a.LastScanAt,
				OpenedLastSnapshotAt: a.LastSnapshotAt,
			}
			ev := makeEvent(TransitionAlert, ClassFreshness, a.Reason, "", nowSec, 0)
			return next, ev, TransitionAlert
		case ClassHard:
			if !qualifiesHard(a.Reason) {
				return next, nil, TransitionNone
			}
			next.PendingHard = nil
			next.Incident = &Incident{
				Active:               true,
				Class:                ClassHard,
				Reason:               a.Reason,
				OpenedAt:             nowSec,
				NotifiedAt:           nowSec,
				LastSeenAt:           nowSec,
				LastReason:           a.Reason,
				OpenedLastScanAt:     a.LastScanAt,
				OpenedLastSnapshotAt: a.LastSnapshotAt,
			}
			ev := makeEvent(TransitionAlert, ClassHard, a.Reason, "", nowSec, 0)
			return next, ev, TransitionAlert
		default:
			return next, nil, TransitionNone
		}
	}

	// Branch 2: Active incident exists.
	if a.Reason == "paused" {
		// Explicit operator pause closes local incident tracking silently.
		next.Incident = nil
		next.PendingHard = nil
		return next, nil, TransitionNone
	}

	if isGenuineRecovery(next.Incident, a) {
		prevReason := next.Incident.Reason
		openedAt := next.Incident.OpenedAt
		dur := max(int64(0), nowSec-openedAt)
		next.Incident = nil
		next.PendingHard = nil
		ev := makeEvent(TransitionRecovered, ClassNone, a.Reason, prevReason, openedAt, dur)
		return next, ev, TransitionRecovered
	}

	if next.Incident.Class == ClassFreshness && a.FailureClass == ClassHard {
		if qualifiesHard(a.Reason) {
			prevReason := next.Incident.Reason
			openedAt := next.Incident.OpenedAt
			dur := max(int64(0), nowSec-openedAt)
			next.PendingHard = nil
			next.Incident.Class = ClassHard
			next.Incident.PreviousReason = prevReason
			next.Incident.Reason = a.Reason
			next.Incident.LastReason = a.Reason
			next.Incident.EscalatedAt = nowSec
			next.Incident.LastSeenAt = nowSec
			ev := makeEvent(TransitionEscalated, ClassHard, a.Reason, prevReason, openedAt, dur)
			return next, ev, TransitionEscalated
		}
		next.Incident.LastSeenAt = nowSec
		next.Incident.LastReason = a.Reason
		return next, nil, TransitionNone
	}

	if a.FailureClass != ClassHard {
		next.PendingHard = nil
	}
	next.Incident.LastSeenAt = nowSec
	next.Incident.LastReason = a.Reason
	return next, nil, TransitionNone
}
