package apiclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	kit "github.com/angelmsger/agent-cli-kit/transport"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestFailureMetadataThroughUpstreamErrors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, test := range []struct {
		header, body string
		status       int
		seconds      int64
	}{
		{"900", `secret-response`, 429, 900},
		{now.Add(10 * time.Minute).Format(http.TimeFormat), `secret-response`, 503, 600},
		{"60", `{"errcode":45009,"errmsg":"secret-response"}`, 200, 60},
		{"-1", `secret-response`, 429, 0},
		{"garbage-secret", `secret-response`, 429, 0},
		{strings.Repeat("9", 1000), `secret-response`, 429, 0},
	} {
		c, _ := New("bot", "synthetic-secret")
		c.now = func() time.Time { return now }
		c.http = kit.New(kit.Options{Doer: doerFunc(func(*http.Request) (*http.Response, error) {
			r := response(test.body)
			r.StatusCode = test.status
			r.Header.Set("Retry-After", test.header)
			r.Header.Set("X-Request-ID", "secret-context-id")
			return r, nil
		})})
		_, err := c.call(context.Background(), PhaseBusinessRequest, "/synthetic", "synthetic-token", nil)
		if err == nil {
			t.Fatal("missing error")
		}
		m := ReadFailureMetadata(e.Safe(err).Details)
		sum := sha256.Sum256([]byte("secret-context-id"))
		if m.RetryAfterSeconds != test.seconds || m.RequestIDDigest != hex.EncodeToString(sum[:]) {
			t.Fatal(m, test)
		}
		b, _ := json.Marshal(e.Safe(err).Payload())
		if strings.Contains(string(b), "secret") || strings.Contains(string(b), "synthetic-token") {
			t.Fatal(string(b))
		}
		var decoded map[string]any
		b, _ = json.Marshal(m)
		if json.Unmarshal(b, &decoded) != nil || ReadFailureMetadata(decoded) != m {
			t.Fatal("lost socket metadata", decoded)
		}
	}
}

func TestInterruptedBodyAndConfirmedOversize(t *testing.T) {
	for _, tt := range []struct {
		reader io.Reader
		code   string
	}{
		{io.MultiReader(strings.NewReader("{"), iotest.ErrReader(io.ErrUnexpectedEOF)), "RESPONSE_INTERRUPTED"},
		{strings.NewReader(strings.Repeat("x", maxResponse+1)), "RESPONSE_LIMIT"},
	} {
		c, _ := New("bot", "secret")
		c.http = kit.New(kit.Options{Doer: doerFunc(func(*http.Request) (*http.Response, error) {
			r := response("")
			r.Body = io.NopCloser(tt.reader)
			return r, nil
		})})
		_, err := c.call(context.Background(), PhaseBusinessRequest, "/synthetic", "", nil)
		if e.Safe(err).Code != tt.code {
			t.Fatal(err)
		}
	}
}
