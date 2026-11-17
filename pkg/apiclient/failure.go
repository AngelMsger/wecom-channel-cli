package apiclient

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// FailureMetadata is the only upstream error context passed to scan consumers.
// Correlation is hashed; no header, body, URL or resource identifier is retained.
type FailureMetadata struct {
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
	RequestIDDigest   string `json:"request_id_digest,omitempty"`
}

func (m FailureMetadata) Valid() bool {
	if m.RetryAfterSeconds < 0 || m.RetryAfterSeconds > 1<<31-1 {
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
		switch n := v["retry_after_seconds"].(type) {
		case json.Number:
			if seconds, err := n.Int64(); err == nil {
				m.RetryAfterSeconds = seconds
			}
		case float64:
			if n >= 0 && n <= 1<<31-1 && n == float64(int64(n)) {
				m.RetryAfterSeconds = int64(n)
			}
		}
		m.RequestIDDigest, _ = v["request_id_digest"].(string)
	}
	if m.RetryAfterSeconds < 0 || m.RetryAfterSeconds > 1<<31-1 {
		m.RetryAfterSeconds = 0
	}
	if !m.Valid() {
		m.RequestIDDigest = ""
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
