package apiclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type HTTPClient struct {
	bot, secret string
	http        *transport.Client
	mu          sync.Mutex
	token       string
	expires     time.Time
	now         func() time.Time
}

func New(bot, secret string) (*HTTPClient, error) {
	if !Identifier(bot, 256) || !Identifier(secret, 1024) {
		return nil, e.New(e.CategoryAuth, "AUTH_MISSING", "The broker needs a valid bot ID and Secret.")
	}
	return &HTTPClient{bot: bot, secret: secret, http: transport.New(), now: time.Now}, nil
}

func (c *HTTPClient) authorization(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && c.now().Before(c.expires) {
		return c.token, nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", e.New(e.CategoryInternal, "RANDOM_FAILED", "Secure randomness is unavailable.")
	}
	n := hex.EncodeToString(nonce[:])
	stamp := c.now().Unix()
	sig := sha256.Sum256([]byte(c.secret + c.bot + strconv.FormatInt(stamp, 10) + n))
	data, err := c.call(ctx, "/cgi-bin/aibot/get_token", "", map[string]any{"bot_id": c.bot, "time": stamp, "nonce": n, "signature": hex.EncodeToString(sig[:])})
	if err != nil {
		return "", err
	}
	var v struct {
		Token string `json:"access_token"`
	}
	if safejson.Decode(data, &v, false) != nil || !Identifier(v.Token, 4096) {
		return "", e.New(e.CategoryParse, "BAD_TOKEN", "WeCom returned no valid access token.")
	}
	c.token = v.Token
	c.expires = c.now().Add(24*time.Hour - time.Minute)
	return c.token, nil
}

func (c *HTTPClient) call(ctx context.Context, path, token string, body any) ([]byte, error) {
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
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(ctx, req)
	if err != nil {
		return nil, e.New(e.CategoryNetwork, "REQUEST_UNCERTAIN", "The WeCom request did not return a reliable result.")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, e.New(e.CategoryServer, "HTTP_REJECTED", "WeCom returned a non-success HTTP status.").WithHTTPStatus(resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil || len(data) > maxResponse {
		return nil, e.New(e.CategoryParse, "RESPONSE_LIMIT", "The response was unreadable or exceeded 2 MiB.")
	}
	var result struct {
		Code *int `json:"errcode"`
	}
	if safejson.Decode(data, &result, false) != nil || result.Code == nil {
		return nil, e.New(e.CategoryParse, "BAD_RESPONSE", "WeCom returned an invalid result envelope.")
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
		x := e.New(cat, code, fmt.Sprintf("WeCom rejected the request (code %d).", *result.Code)).WithHint(hint)
		x.Retryable = false
		return nil, x
	}
	return data, nil
}

func (c *HTTPClient) request(ctx context.Context, path string, body any) ([]byte, error) {
	token, err := c.authorization(ctx)
	if err != nil {
		return nil, err
	}
	data, err := c.call(ctx, path, token, body)
	if ce, ok := err.(*e.CLIError); ok && ce.Code == "AUTH_REJECTED" {
		c.mu.Lock()
		if c.token == token {
			c.token = ""
		}
		c.mu.Unlock()
	}
	return data, err
}

func (c *HTTPClient) Send(ctx context.Context, r WriteRequest) (SendResult, error) {
	p, err := BuildWrite(r)
	if err != nil {
		return SendResult{}, err
	}
	b, err := c.request(ctx, p.Path, p.Body)
	if err != nil {
		return SendResult{}, err
	}
	var v struct {
		Post  string `json:"post_id"`
		Reply string `json:"reply_id"`
	}
	if safejson.Decode(b, &v, false) != nil {
		return SendResult{}, e.New(e.CategoryParse, "BAD_WRITE_RESULT", "The write result could not be decoded; reconcile before retrying.")
	}
	id := v.Post
	if r.Kind == "reply" {
		id = v.Reply
	}
	if !Identifier(id, 128) {
		return SendResult{}, e.New(e.CategoryParse, "BAD_WRITE_RESULT", "The write returned no resource ID; reconcile before retrying.")
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
