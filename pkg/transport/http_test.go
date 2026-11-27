package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestNoRedirectOrEnvironmentProxy(t *testing.T) {
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"?access_token=SECRET", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	t.Setenv("HTTP_PROXY", target.URL)
	t.Setenv("HTTPS_PROXY", target.URL)
	t.Setenv("ALL_PROXY", target.URL)
	r, _ := http.NewRequest("POST", source.URL+"?access_token=SECRET", nil)
	resp, err := New().Do(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if hits != 0 || resp.StatusCode != 307 {
		t.Fatal("redirect or proxy followed")
	}
}

func TestExplicitProxyValidationAndRouting(t *testing.T) {
	for _, bad := range []string{"ftp://proxy.local:8080", "http://proxy.local/path?a=1", "http://proxy.local#frag", "http://bad host:8080", "http://"} {
		if _, err := ValidateProxyURL(bad); err == nil {
			t.Fatalf("accepted bad proxy %q", bad)
		}
	}
	for _, good := range []string{"", "http://127.0.0.1:8080", "https://proxy.example:8443", "socks5://127.0.0.1:1080", "socks5h://127.0.0.1:1080"} {
		if _, err := ValidateProxyURL(good); err != nil {
			t.Fatalf("rejected valid proxy %q: %v", good, err)
		}
	}
	proxied := false
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = true
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	cfg := DefaultConfig()
	cfg.ProxyURL = proxy.URL
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", "http://example.invalid/test", bytes.NewReader([]byte(`{}`)))
	resp, err := c.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !proxied || resp.StatusCode != http.StatusOK {
		t.Fatalf("explicit proxy not used: proxied=%v status=%d", proxied, resp.StatusCode)
	}
}

func TestAsReadRetriesTransientPostAndWriteNeverRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"ok":true}` {
			t.Errorf("lost body on retry: %q", body)
		}
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.RetryBaseDelay = 5 * time.Millisecond
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	req, _ := http.NewRequest("POST", srv.URL, bytes.NewReader([]byte(`{"ok":true}`)))
	resp, err := c.Do(AsRead(context.Background()), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || attempts.Load() != 3 {
		t.Fatalf("read POST did not retry cleanly: status=%d attempts=%d", resp.StatusCode, attempts.Load())
	}

	// Plain POST without AsRead must never retry even on 502.
	attempts.Store(0)
	req, _ = http.NewRequest("POST", srv.URL, bytes.NewReader([]byte(`{"ok":true}`)))
	req.GetBody = nil
	resp, err = c.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway || attempts.Load() != 1 {
		t.Fatalf("write POST retried unexpectedly: status=%d attempts=%d", resp.StatusCode, attempts.Load())
	}
}

func TestClassifyAndPreConnectNetworkErrors(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	dnsErr := &net.DNSError{Err: "no such host", Name: "qyapi.weixin.qq.com"}
	if ClassifyNetworkError(dialErr) != "dial" || !PreConnectFailure(dialErr) {
		t.Fatal("dial error misclassified")
	}
	if ClassifyNetworkError(dnsErr) != "dns" || !PreConnectFailure(dnsErr) {
		t.Fatal("dns error misclassified")
	}
	if ClassifyNetworkError(io.ErrUnexpectedEOF) != "eof" || PreConnectFailure(io.ErrUnexpectedEOF) {
		t.Fatal("eof misclassified")
	}
	if ClassifyNetworkError(context.DeadlineExceeded) != "timeout" || PreConnectFailure(context.DeadlineExceeded) {
		t.Fatal("timeout misclassified")
	}
	if ClassifyNetworkError(errors.New("tls: handshake failure")) != "tls" {
		t.Fatal("tls misclassified")
	}
}
