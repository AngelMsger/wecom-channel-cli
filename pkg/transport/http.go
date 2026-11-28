// Package transport uses the family HTTP client with channel-specific defaults.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
	"unicode"

	kit "github.com/angelmsger/agent-cli-kit/transport"
	"github.com/angelmsger/wecom-channel-cli/pkg/constants"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type Client = kit.Client
type Doer = kit.Doer
type Options = kit.Options
type Decorator = kit.Decorator

// AsRead marks a request's context as a safe, idempotent read (even when the
// upstream protocol uses HTTP POST) so transient network failures can retry.
// Never mark a write request with AsRead.
var AsRead = kit.AsRead

// Config tunes HTTP timeouts, connection reuse, read retries and an optional
// explicit operator-configured proxy. Ambient proxy environment variables are
// never inherited.
type Config struct {
	Timeout               time.Duration
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	KeepAlive             time.Duration
	IdleConnTimeout       time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	ReadMaxRetries        int
	RetryBaseDelay        time.Duration
	ProxyURL              string
}

// DefaultConfig returns cross-region resilient defaults tuned for long-haul
// links (such as North America to Mainland China WeCom endpoints).
func DefaultConfig() Config {
	return Config{
		Timeout:               45 * time.Second,
		DialTimeout:           15 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		KeepAlive:             30 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		ReadMaxRetries:        3,
		RetryBaseDelay:        time.Second,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Timeout <= 0 {
		c.Timeout = d.Timeout
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = min(d.DialTimeout, c.Timeout)
	}
	if c.TLSHandshakeTimeout <= 0 {
		c.TLSHandshakeTimeout = min(d.TLSHandshakeTimeout, c.Timeout)
	}
	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = min(d.ResponseHeaderTimeout, c.Timeout)
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = d.KeepAlive
	}
	if c.IdleConnTimeout <= 0 {
		c.IdleConnTimeout = d.IdleConnTimeout
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = d.MaxIdleConns
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = d.MaxIdleConnsPerHost
	}
	if c.ReadMaxRetries == 0 {
		c.ReadMaxRetries = d.ReadMaxRetries
	} else if c.ReadMaxRetries < 0 {
		c.ReadMaxRetries = 0
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = d.RetryBaseDelay
	}
	return c
}

// ValidateProxyURL accepts an empty string (direct) or an explicit http, https,
// socks5, or socks5h proxy URL without control characters or fragments.
func ValidateProxyURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 2048 || strings.IndexFunc(raw, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return nil, e.New(e.CategoryConfig, "BAD_PROXY", "Provide a valid http, https, socks5 or socks5h proxy URL.")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Fragment != "" || u.RawQuery != "" {
		return nil, e.New(e.CategoryConfig, "BAD_PROXY", "Provide a valid http, https, socks5 or socks5h proxy URL.")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks5", "socks5h":
		return u, nil
	default:
		return nil, e.New(e.CategoryConfig, "BAD_PROXY", "Proxy scheme must be http, https, socks5 or socks5h.")
	}
}

// resilientDoer flushes dead half-open idle connections after transport errors
// so subsequent read retries open a fresh TCP/TLS session.
type resilientDoer struct {
	hc *http.Client
	tr *http.Transport
}

func (d *resilientDoer) Do(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	resp, err := d.hc.Do(req)
	if err != nil && !errors.Is(err, context.Canceled) {
		d.tr.CloseIdleConnections()
	}
	return resp, err
}

// NewWithConfig builds a pinned HTTP client that never follows redirects,
// never inherits ambient proxy environment variables, and only retries requests
// explicitly marked with AsRead (never writes).
func NewWithConfig(cfg Config) (*Client, error) {
	proxyURL, err := ValidateProxyURL(cfg.ProxyURL)
	if err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	tr := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: cfg.KeepAlive,
		}).DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		ForceAttemptHTTP2:     true,
	}
	if proxyURL != nil {
		tr.Proxy = http.ProxyURL(proxyURL)
	}
	hc := &http.Client{
		Timeout:       cfg.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     tr,
	}
	return kit.New(kit.Options{
		Doer:           &resilientDoer{hc: hc, tr: tr},
		MaxRetries:     cfg.ReadMaxRetries,
		RetryBaseDelay: cfg.RetryBaseDelay,
		UserAgent:      constants.AppName + "/" + constants.Version,
	}), nil
}

// New never follows redirects, inherits proxies, logs token-bearing URLs, or
// retries writes. Reads explicitly marked with AsRead retry transient failures.
func New() *Client {
	c, _ := NewWithConfig(DefaultConfig())
	return c
}

// ValidNetworkReason reports whether reason is one of the curated transport
// failure classifications emitted in diagnostics.
func ValidNetworkReason(reason string) bool {
	switch reason {
	case "", "canceled", "timeout", "dns", "tls", "dial", "eof", "reset", "proxy", "network":
		return true
	default:
		return false
	}
}

// ClassifyNetworkError maps an arbitrary transport error to a secret-free,
// bounded reason token for operator diagnosis.
func ClassifyNetworkError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns"
	}
	var recordErr tls.RecordHeaderError
	var unknownAuth x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var certInvalid x509.CertificateInvalidError
	if errors.As(err, &recordErr) || errors.As(err, &unknownAuth) || errors.As(err, &hostErr) || errors.As(err, &certInvalid) {
		return "tls"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return "reset"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Op == "proxyconnect" {
			return "proxy"
		}
		if opErr.Timeout() {
			return "timeout"
		}
		if opErr.Op == "dial" {
			return "dial"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "proxyconnect"):
		return "proxy"
	case strings.Contains(msg, "tls:") || strings.Contains(msg, "x509:") || strings.Contains(msg, "handshake"):
		return "tls"
	case strings.Contains(msg, "connection reset") || strings.Contains(msg, "broken pipe"):
		return "reset"
	case strings.Contains(msg, "eof"):
		return "eof"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return "timeout"
	default:
		return "network"
	}
}

// PreConnectFailure reports whether err proves the HTTP request was never sent
// to the remote server (DNS, TCP dial, proxy connect, or TLS handshake error).
func PreConnectFailure(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Op == "dial" || opErr.Op == "proxyconnect" {
			return true
		}
	}
	var recordErr tls.RecordHeaderError
	var unknownAuth x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var certInvalid x509.CertificateInvalidError
	if errors.As(err, &recordErr) || errors.As(err, &unknownAuth) || errors.As(err, &hostErr) || errors.As(err, &certInvalid) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}
