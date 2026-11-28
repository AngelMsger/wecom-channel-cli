package apiclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

const maxResponse = 2 << 20

// Options configures an HTTPClient's upstream transport behavior.
type Options struct {
	Transport transport.Config
	HTTP      *transport.Client
}

type HTTPClient struct {
	bot, secret string
	http        *transport.Client
	mu          sync.Mutex
	token       string
	expires     time.Time
	now         func() time.Time
}

func New(bot, secret string) (*HTTPClient, error) {
	return NewWithOptions(bot, secret, Options{Transport: transport.DefaultConfig()})
}

func NewWithOptions(bot, secret string, opts Options) (*HTTPClient, error) {
	if !Identifier(bot, 256) || !Identifier(secret, 1024) {
		return nil, e.New(e.CategoryAuth, "AUTH_MISSING", "The broker needs a valid bot ID and Secret.")
	}
	hc := opts.HTTP
	if hc == nil {
		var err error
		hc, err = transport.NewWithConfig(opts.Transport)
		if err != nil {
			return nil, err
		}
	}
	return &HTTPClient{bot: bot, secret: secret, http: hc, now: time.Now}, nil
}

func (c *HTTPClient) authorization(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && c.now().Before(c.expires) {
		return c.token, nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", PhaseTokenBootstrap.failed(e.New(e.CategoryInternal, "RANDOM_FAILED", "Secure randomness is unavailable."), nil)
	}
	n := hex.EncodeToString(nonce[:])
	stamp := c.now().Unix()
	sig := sha256.Sum256([]byte(c.secret + c.bot + strconv.FormatInt(stamp, 10) + n))
	data, err := c.callWithMode(transport.AsRead(ctx), PhaseTokenBootstrap, "/cgi-bin/aibot/get_token", "", map[string]any{"bot_id": c.bot, "time": stamp, "nonce": n, "signature": hex.EncodeToString(sig[:])}, true)
	if err != nil {
		return "", err
	}
	var v struct {
		Token string `json:"access_token"`
	}
	if safejson.Decode(data, &v, false) != nil || !Identifier(v.Token, 4096) {
		return "", PhaseTokenBootstrap.failed(e.New(e.CategoryParse, "BAD_TOKEN", "WeCom returned no valid access token."), new(int))
	}
	c.token = v.Token
	c.expires = c.now().Add(24*time.Hour - time.Minute)
	return c.token, nil
}

func (c *HTTPClient) call(ctx context.Context, phase RequestPhase, path, token string, body any) ([]byte, error) {
	return c.callWithMode(ctx, phase, path, token, body, false)
}

func (c *HTTPClient) callWithMode(ctx context.Context, phase RequestPhase, path, token string, body any, readOnly bool) ([]byte, error) {
	req, err := newRequest(ctx, path, token, body, readOnly)
	if err != nil {
		if phase == PhaseBusinessRequest {
			phase = PhasePrepare
		}
		return nil, phase.failed(err, nil)
	}
	data, upstreamCode, err := c.exchange(req)
	if err != nil {
		return nil, phase.failed(err, upstreamCode)
	}
	return data, nil
}

func newRequest(ctx context.Context, path, token string, body any, readOnly bool) (*http.Request, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, e.New(e.CategoryInternal, "ENCODE_FAILED", "Cannot encode request.")
	}
	u := Origin + path
	if token != "" {
		u += "?" + url.Values{"access_token": {token}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(b))
	if err != nil {
		return nil, e.New(e.CategoryInternal, "REQUEST_FAILED", "Cannot construct request.")
	}
	if !readOnly {
		// Prevent net/http from replaying a write POST on a reused idle connection.
		req.GetBody = nil
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func networkFailureMessage(reason string) string {
	switch reason {
	case "timeout":
		return "The WeCom request timed out."
	case "dns":
		return "Could not resolve the WeCom API host."
	case "dial":
		return "Could not connect to the WeCom API."
	case "tls":
		return "The WeCom TLS handshake failed."
	case "eof":
		return "The WeCom connection closed unexpectedly (EOF)."
	case "reset":
		return "The WeCom connection was reset by peer."
	case "proxy":
		return "The configured proxy connection failed."
	case "canceled":
		return "The WeCom request was canceled."
	default:
		return "The WeCom request did not return a reliable result."
	}
}

// exchange returns the WeCom code when it can decode the response envelope.
func (c *HTTPClient) exchange(req *http.Request) ([]byte, *int, error) {
	resp, err := c.http.Do(req.Context(), req)
	if err != nil {
		reason := transport.ClassifyNetworkError(err)
		x := e.New(e.CategoryNetwork, "REQUEST_UNCERTAIN", networkFailureMessage(reason)).WithDetails(FailureMetadata{NetworkReason: reason})
		x.Retryable = reason != "canceled"
		return nil, nil, x
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, e.New(e.CategoryServer, "HTTP_REJECTED", "WeCom returned a non-success HTTP status.").WithHTTPStatus(resp.StatusCode).WithDetails(failureMetadata(resp.Header, c.now()))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if len(data) > maxResponse {
		return nil, nil, e.New(e.CategoryParse, "RESPONSE_LIMIT", "The WeCom response exceeded 2 MiB.")
	}
	if err != nil {
		reason := transport.ClassifyNetworkError(err)
		x := e.New(e.CategoryNetwork, "RESPONSE_INTERRUPTED", "The WeCom response body was interrupted.").WithDetails(FailureMetadata{NetworkReason: reason})
		x.Retryable = reason != "canceled"
		return nil, nil, x
	}
	var result struct {
		Code *int `json:"errcode"`
	}
	if safejson.Decode(data, &result, false) != nil || result.Code == nil {
		return nil, nil, e.New(e.CategoryParse, "BAD_RESPONSE", "WeCom returned an invalid result envelope.")
	}
	if *result.Code != 0 {
		cat := e.CategoryServer
		code := "WECOM_REJECTED"
		hint := "Check API availability and channel permissions."
		switch *result.Code {
		case 45009:
			cat = e.CategoryRateLimit
			hint = "Wait before a new request; keep the cached access token."
		case 853000, 853004, 40014, 42001:
			cat = e.CategoryAuth
			code = "AUTH_REJECTED"
			hint = "Ask the operator to check credentials and the clock. An expired token is discarded; writes are never replayed."
		case 846601, 853006:
			cat = e.CategoryPermission
			hint = "Ask the operator to verify API mode and grayscale feature access."
		}
		x := e.New(cat, code, fmt.Sprintf("WeCom rejected the request (code %d).", *result.Code)).WithHint(hint).WithDetails(failureMetadata(resp.Header, c.now()))
		x.Retryable = false
		return nil, result.Code, x
	}
	return data, result.Code, nil
}

func (c *HTTPClient) evictToken(token string) {
	c.mu.Lock()
	if c.token == token {
		c.token = ""
	}
	c.mu.Unlock()
}

func isExpiredTokenError(err error) bool {
	var rf *RequestFailure
	if !errors.As(err, &rf) || rf.Phase != PhaseBusinessRequest || rf.UpstreamErrcode == nil {
		return false
	}
	switch *rf.UpstreamErrcode {
	case 853004, 42001, 40014:
		return true
	default:
		return false
	}
}

// readRequest performs an idempotent WeCom read POST with transport-level
// retries, one body-interruption retry, and one transparent expired-token refresh.
func (c *HTTPClient) readRequest(ctx context.Context, path string, body any) ([]byte, error) {
	readCtx := transport.AsRead(ctx)
	refreshed := false
	bodyRetried := false
	for {
		token, err := c.authorization(readCtx)
		if err != nil {
			return nil, err
		}
		data, err := c.callWithMode(readCtx, PhaseBusinessRequest, path, token, body, true)
		if err == nil {
			return data, nil
		}
		ce, ok := err.(*e.CLIError)
		if ok && ce.Code == "AUTH_REJECTED" {
			c.evictToken(token)
			if !refreshed && isExpiredTokenError(err) && ctx.Err() == nil {
				refreshed = true
				continue
			}
		}
		if ok && ce.Code == "RESPONSE_INTERRUPTED" && !bodyRetried && ctx.Err() == nil {
			bodyRetried = true
			continue
		}
		return nil, err
	}
}

// writeRequest sends a write POST at most once; it never retries automatically.
func (c *HTTPClient) writeRequest(ctx context.Context, path string, body any) ([]byte, error) {
	token, err := c.authorization(ctx)
	if err != nil {
		return nil, err
	}
	data, err := c.callWithMode(ctx, PhaseBusinessRequest, path, token, body, false)
	if ce, ok := err.(*e.CLIError); ok && ce.Code == "AUTH_REJECTED" {
		c.evictToken(token)
	}
	return data, err
}

func (c *HTTPClient) Send(ctx context.Context, r WriteRequest) (SendResult, error) {
	p, err := BuildWrite(r)
	if err != nil {
		return SendResult{}, PhasePrepare.failed(err, nil)
	}
	b, err := c.writeRequest(ctx, p.Path, p.Body)
	if err != nil {
		return SendResult{}, err
	}
	if r.Kind == "reaction" {
		// post_emoji acknowledges the operation with errcode=0, without a new ID.
		return SendResult{Kind: "reaction"}, nil
	}
	var v struct {
		Post  string `json:"post_id"`
		Reply string `json:"reply_id"`
	}
	if safejson.Decode(b, &v, false) != nil {
		return SendResult{}, PhaseBusinessRequest.failed(e.New(e.CategoryParse, "BAD_WRITE_RESULT", "The write result could not be decoded; reconcile before retrying."), new(int))
	}
	id := v.Post
	if r.Kind == "reply" {
		id = v.Reply
	}
	if !Identifier(id, 128) {
		return SendResult{}, PhaseBusinessRequest.failed(e.New(e.CategoryParse, "BAD_WRITE_RESULT", "The write returned no resource ID; reconcile before retrying."), new(int))
	}
	return SendResult{id, r.Kind}, nil
}

// scrub prevents accidental reflection of known authentication material.
func (c *HTTPClient) scrub(v string) string {
	c.mu.Lock()
	token := c.token
	c.mu.Unlock()
	for _, secret := range []string{c.secret, token} {
		if secret != "" {
			v = strings.ReplaceAll(v, secret, "[REDACTED]")
		}
	}
	return v
}
