// Package transport uses the family HTTP client with channel-specific defaults.
package transport

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	kit "github.com/angelmsger/agent-cli-kit/transport"
	"github.com/angelmsger/wecom-channel-cli/pkg/constants"
)

type Client = kit.Client
type Doer = kit.Doer
type Options = kit.Options
type Decorator = kit.Decorator

// New never follows redirects, inherits proxies, logs token-bearing URLs, or
// retries POST. The API client pins the destination separately.
func New() *Client {
	hc := &http.Client{Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second,
			IdleConnTimeout: 60 * time.Second, MaxIdleConns: 4,
		},
	}
	return kit.New(kit.Options{Doer: hc, UserAgent: constants.AppName + "/" + constants.Version})
}
