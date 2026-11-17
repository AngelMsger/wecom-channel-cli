package relay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestIndependentRetryConfig(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		fields         string
		scan, delivery int
		valid          bool
	}{
		{``, 0, 0, true},
		{`,"retry_limit":3`, 3, 3, true},
		{`,"retry_limit":3,"scan_retry_limit":0`, 0, 3, true},
		{`,"retry_limit":3,"delivery_retry_limit":0`, 3, 0, true},
		{`,"scan_retry_limit":20,"delivery_retry_limit":0`, 20, 0, true},
		{`,"scan_retry_limit":-1`, 0, 0, false},
		{`,"delivery_retry_limit":21`, 0, 0, false},
	} {
		t.Run(test.fields, func(t *testing.T) {
			path := filepath.Join(dir, "config.json")
			if err := os.WriteFile(path, []byte(`{"version":1,"webhook_url":"https://example.invalid"`+test.fields+`}`), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := LoadConfig(path)
			if (err == nil) != test.valid {
				t.Fatal(err)
			}
			if test.valid && (c.ScanRetries() != test.scan || c.DeliveryRetries() != test.delivery) {
				t.Fatal(c)
			}
		})
	}
}

func TestScanRetryDelayHonorsBoundsAndThrottle(t *testing.T) {
	c := DefaultConfig()
	for _, failures := range []int{1, 2, 20} {
		for range 100 {
			err := e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "offline")
			delay := ScanRetryDelay(c, failures, time.Second, err)
			base := RetryDelay(c, failures)
			if delay < max(time.Second, base/2) || delay > base || delay > 5*time.Minute {
				t.Fatal(delay)
			}
		}
	}
	err := e.New(e.CategoryServer, "HTTP_REJECTED", "secret").WithHTTPStatus(429).WithDetails(apiclient.FailureMetadata{RetryAfterSeconds: 900})
	if delay := ScanRetryDelay(c, 1, 30*time.Second, err); delay != 15*time.Minute {
		t.Fatal(delay)
	}
	err.Details = nil
	if delay := ScanRetryDelay(c, 1, 30*time.Second, err); delay != 30*time.Second {
		t.Fatal(delay)
	}
}

func TestDiagnosticRecordRejectsRawContextAndCorruption(t *testing.T) {
	err := e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "secret-message https://secret.invalid").WithHint("secret-hint").WithDetails(map[string]any{"context": "secret-context", "request_id_digest": "secret-id", "retry_after_seconds": float64(60)})
	r := NewErrorRecord(err, "scan", 1, 0, time.Now())
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "secret") || !r.valid("scan") || r.Metadata.RetryAfterSeconds != 60 {
		t.Fatal(string(b))
	}
	for _, mutate := range []func(*ErrorRecord){
		func(r *ErrorRecord) { r.Code = strings.Repeat("A", 65) },
		func(r *ErrorRecord) { r.Cause = "secret" },
		func(r *ErrorRecord) { r.Phase = "delivery" },
		func(r *ErrorRecord) { r.Attempt = 22 },
		func(r *ErrorRecord) { r.RetryLimit = -1 },
		func(r *ErrorRecord) { r.Metadata.RequestIDDigest = "raw-id" },
	} {
		x := *r
		mutate(&x)
		st := State{Version: 1, Consumer: apiclient.Digest("consumer"), LastScanError: &x}
		if st.Validate() == nil {
			t.Fatal("accepted corrupt diagnostic", x)
		}
	}
}

func TestTransientScanFailuresDoNotRelaxDeliveryPolicy(t *testing.T) {
	for _, code := range []string{"BUSY", "POLL_UNSTABLE"} {
		err := e.New(e.CategoryConflict, code, "synthetic")
		if !ScanRetryable(err) || Retryable(err) || !NewErrorRecord(err, "scan", 1, 20, time.Now()).Retryable {
			t.Fatal("scan and delivery recovery were conflated", code)
		}
	}
	for _, code := range []string{"EVENT_GAP", "WATCH_BINDING_CHANGED", "POLL_CAPACITY", "POLL_BUDGET", "POLL_SCOPE_LIMIT", "AUTH_REJECTED", "WATCH_PROTOCOL"} {
		if ScanRetryable(e.New(e.CategoryRateLimit, code, "synthetic")) {
			t.Fatal("retried fatal scan", code)
		}
	}
}
