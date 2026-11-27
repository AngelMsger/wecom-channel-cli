package relay

import (
	"math/rand/v2"
	"regexp"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// ErrorRecord is bounded diagnostic history, not a retry authorization or latch.
// Never copy an error's message, hint, cause chain or arbitrary details here.
type ErrorRecord struct {
	Code       string                    `json:"code"`
	Cause      string                    `json:"cause"`
	Phase      string                    `json:"phase"`
	At         int64                     `json:"at"`
	Attempt    int                       `json:"attempt"`
	RetryLimit int                       `json:"retry_limit"`
	Retryable  bool                      `json:"retryable"`
	HTTPStatus int                       `json:"http_status,omitempty"`
	Metadata   apiclient.FailureMetadata `json:"metadata,omitempty"`
}

var diagnosticCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

func errorCause(code string) string {
	switch code {
	case "BROKER_UNREACHABLE":
		return "The broker did not return a response."
	case "REQUEST_UNCERTAIN":
		return "The upstream request did not return a reliable result."
	case "SCAN_TIMEOUT", "REQUEST_TIMEOUT":
		return "The scan request timed out before completing."
	case "POLL_RATE_LIMIT", "RATE_LIMITED":
		return "The broker request rate limit was reached."
	case "BUSY":
		return "The broker is temporarily at its concurrency limit."
	case "POLL_UNSTABLE":
		return "The resource changed during pagination; no partial snapshot was committed."
	case "HTTP_REJECTED", "WECOM_REJECTED":
		return "The upstream service rejected the request."
	case "RELAY_UNREACHABLE":
		return "The receiver did not return a reliable acknowledgment."
	case "RELAY_REJECTED", "RELAY_REDIRECT":
		return "The receiver rejected the batch."
	case "RELAY_ACK":
		return "The receiver did not return the required batch acknowledgment."
	case "RELAY_OUTCOME_UNKNOWN":
		return "The host may already have started a run; reconcile before replay."
	default:
		return "The operation failed; inspect the error code and HTTP status."
	}
}

func NewErrorRecord(err error, phase string, attempt, limit int, now time.Time) *ErrorRecord {
	x := e.Safe(err)
	code := x.Code
	if !diagnosticCode.MatchString(code) {
		code = "INTERNAL"
	}
	status := x.HTTPStatus
	if status < 100 || status > 599 {
		status = 0
	}
	retryable := Retryable(err)
	if phase == "scan" {
		retryable = ScanRetryable(err)
	}
	return &ErrorRecord{Code: code, Cause: errorCause(code), Phase: phase, At: now.UnixMilli(), Attempt: attempt, RetryLimit: limit, Retryable: retryable, HTTPStatus: status, Metadata: apiclient.ReadFailureMetadata(x.Details)}
}

// A read-only scan can restart a discarded snapshot, recover from timeouts, or
// wait for a busy broker. These do not authorize another webhook delivery or
// soften binding/gap errors.
func ScanRetryable(err error) bool {
	if Retryable(err) {
		return true
	}
	x := e.Safe(err)
	if x.Code == "AUTH_REJECTED" {
		m := apiclient.ReadFailureMetadata(x.Details)
		return m.Phase == apiclient.PhaseBusinessRequest && (m.UpstreamCode == 853004 || m.UpstreamCode == 42001 || m.UpstreamCode == 40014)
	}
	return x.Code == "RESPONSE_INTERRUPTED" || x.Code == "BUSY" || x.Code == "POLL_UNSTABLE" || x.Code == "SCAN_TIMEOUT" || x.Code == "REQUEST_TIMEOUT"
}

func (r *ErrorRecord) valid(phase string) bool {
	if r == nil {
		return true
	}
	maxAttempt := 21
	if phase == "scan" && r.RetryLimit < 0 {
		maxAttempt = 1_000_000
	}
	minLimit := 0
	if phase == "scan" {
		minLimit = -1
	}
	return r.Phase == phase && diagnosticCode.MatchString(r.Code) && r.Cause == errorCause(r.Code) && r.At > 0 && r.Attempt >= 1 && r.Attempt <= maxAttempt && r.RetryLimit >= minLimit && r.RetryLimit <= 20 && (r.HTTPStatus == 0 || r.HTTPStatus >= 100 && r.HTTPStatus <= 599) && r.Metadata.Valid()
}

// Local scan backoff uses equal jitter, capped at five minutes. Operator polling
// intervals and valid upstream Retry-After guidance are lower bounds, even when
// they exceed the local cap. Delivery retry scheduling is deliberately unchanged.
func ScanRetryDelay(c Config, failures int, interval time.Duration, err error) time.Duration {
	base := RetryDelay(c, failures)
	delay := base/2 + time.Duration(rand.Int64N(int64(base-base/2)+1))
	metadata := apiclient.ReadFailureMetadata(e.Safe(err).Details)
	return max(interval, delay, time.Duration(metadata.RetryAfterSeconds)*time.Second)
}

// ResetScan clears only active recovery state. Historical causes and delivery
// attempts remain visible after a successful scan or an operator repair.
func (s *State) ResetScan() {
	s.ScanFailures, s.NextScan, s.FirstFailure = 0, 0, 0
	s.ScanBlocked, s.AuthRecovery, s.RetryStarted = "", false, false
	s.Progress = nil
}

func (s *State) RecordScanFailure(err error, cfg Config, interval time.Duration, now time.Time) bool {
	record := NewErrorRecord(err, "scan", s.ScanFailures+1, cfg.ScanRetries(), now)
	retry := record.Retryable
	if record.Code == "AUTH_REJECTED" && retry {
		retry = !s.AuthRecovery
		s.AuthRecovery = true
	}
	record.Retryable = retry
	s.LastScanError = record
	s.ScanHistory = append(s.ScanHistory, *record)
	if len(s.ScanHistory) > 8 {
		s.ScanHistory = s.ScanHistory[len(s.ScanHistory)-8:]
	}
	if s.FirstFailure == 0 {
		s.FirstFailure = now.UnixMilli()
	}
	s.Progress = nil
	if retry {
		s.ScanFailures++
		s.NextScan = now.Add(ScanRetryDelay(cfg, s.ScanFailures, interval, err)).UnixMilli()
	} else {
		s.ScanBlocked = e.Safe(err).Category
		s.NextScan = 0
	}
	return retry
}
func (s State) ScanState() string {
	switch {
	case s.ScanBlocked != "" || (s.LastScanError != nil && s.LastScanError.RetryLimit >= 0 && s.ScanFailures > s.LastScanError.RetryLimit):
		return "blocked"
	case s.Progress != nil:
		return "in_progress"
	case s.ScanFailures > 0:
		return "retry_wait"
	case s.Binding == "":
		return "starting"
	default:
		return "current"
	}
}
