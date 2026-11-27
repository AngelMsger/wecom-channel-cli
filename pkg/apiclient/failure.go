package apiclient

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

// FailureMetadata is the only upstream error context passed to scan consumers.
// Correlation is hashed; no header, body, URL or resource identifier is retained.
type FailureMetadata struct {
	RequestsUsed      int          `json:"requests_used,omitempty"`
	RequestLimit      int          `json:"request_limit,omitempty"`
	RecordsSeen       int          `json:"records_seen,omitempty"`
	RecordLimit       int          `json:"record_limit,omitempty"`
	Phase             RequestPhase `json:"request_phase,omitempty"`
	UpstreamCode      int          `json:"upstream_errcode,omitempty"`
	RetryAfterSeconds int64        `json:"retry_after_seconds,omitempty"`
	RequestIDDigest   string       `json:"request_id_digest,omitempty"`
	NetworkReason     string       `json:"network_reason,omitempty"`
}

func (m FailureMetadata) Valid() bool {
	if m.RequestsUsed < 0 || m.RequestsUsed > 5000 || m.RequestLimit < 0 || m.RequestLimit > 5000 || m.RecordsSeen < 0 || m.RecordsSeen > 100001 || m.RecordLimit < 0 || m.RecordLimit > 100000 {
		return false
	}
	if m.Phase != "" && m.Phase != PhaseBusinessRequest && m.Phase != PhaseTokenBootstrap || m.UpstreamCode < 0 || m.UpstreamCode > 1<<31-1 {
		return false
	}
	if m.RetryAfterSeconds < 0 || m.RetryAfterSeconds > 1<<31-1 {
		return false
	}
	if !transport.ValidNetworkReason(m.NetworkReason) {
		return false
	}
	if m.RequestIDDigest == "" {
		return true
	}
	if len(m.RequestIDDigest) != sha256.Size*2 {
		return false
	}
	b, err := hex.DecodeString(m.RequestIDDigest)
	return err == nil && len(b) == sha256.Size && m.RequestIDDigest == strings.ToLower(m.RequestIDDigest)
}

// ReadFailureMetadata also accepts the JSON-decoded form from the broker socket.
// Unknown fields and invalid values never cross into persisted diagnostics.
func ReadFailureMetadata(details any) FailureMetadata {
	var m FailureMetadata
	switch v := details.(type) {
	case FailureMetadata:
		m = v
	case map[string]any:
		m.RetryAfterSeconds = int64(diagnosticNumber(v["retry_after_seconds"]))
		phase, _ := v["request_phase"].(string)
		m.Phase = RequestPhase(phase)
		m.UpstreamCode = diagnosticNumber(v["upstream_errcode"])
		m.RequestsUsed = diagnosticNumber(v["requests_used"])
		m.RequestLimit = diagnosticNumber(v["request_limit"])
		m.RecordsSeen = diagnosticNumber(v["records_seen"])
		m.RecordLimit = diagnosticNumber(v["record_limit"])
		m.RequestIDDigest, _ = v["request_id_digest"].(string)
		m.NetworkReason, _ = v["network_reason"].(string)
	}
	if m.RetryAfterSeconds < 0 || m.RetryAfterSeconds > 1<<31-1 {
		m.RetryAfterSeconds = 0
	}
	if !m.Valid() {
		return FailureMetadata{RetryAfterSeconds: m.RetryAfterSeconds}
	}
	return m
}

func failureMetadata(h http.Header, now time.Time) FailureMetadata {
	var m FailureMetadata
	value := strings.TrimSpace(h.Get("Retry-After"))
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds >= 0 {
		m.RetryAfterSeconds = seconds
	} else if at, err := http.ParseTime(value); err == nil && at.After(now) {
		// Round up so subsecond receipt time cannot shorten the server's wait.
		delay := at.Sub(now)
		m.RetryAfterSeconds = int64(delay / time.Second)
		if delay%time.Second != 0 {
			m.RetryAfterSeconds++
		}
	}
	if id := h.Get("X-Request-ID"); id != "" && len(id) <= 1024 {
		sum := sha256.Sum256([]byte(id))
		m.RequestIDDigest = hex.EncodeToString(sum[:])
	}
	return ReadFailureMetadata(m)
}

func diagnosticNumber(value any) int {
	switch n := value.(type) {
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 32)
		if err == nil && i >= 0 {
			return int(i)
		}
	case float64:
		if n >= 0 && n <= 1<<31-1 && n == float64(int(n)) {
			return int(n)
		}
	}
	return 0
}
