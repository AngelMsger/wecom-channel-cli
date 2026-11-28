// Package relay implements consumer-owned durable delivery of channel observations.
// It has no broker credentials, approval capability, shell runner, or model client.
package relay

import (
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

type Config struct {
	Version            int      `json:"version"`
	URL                string   `json:"webhook_url"`
	TokenEnv           string   `json:"token_env,omitempty"`
	Kinds              []string `json:"kinds"`
	Changes            []int    `json:"changes"`
	ExcludeSelf        bool     `json:"exclude_self"` // Compatibility field; self exclusion is mandatory.
	ExcludeBots        []string `json:"exclude_bots,omitempty"`
	BatchWindowSeconds int      `json:"batch_window_seconds"`
	BatchSize          int      `json:"batch_size"`
	QueueLimit         int      `json:"queue_limit"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	RetryLimit         int      `json:"retry_limit"`
	RetryBaseSeconds   int      `json:"retry_base_seconds"`
	DeliveryMode       string   `json:"delivery_mode,omitempty"`
	MinDeliverySeconds int      `json:"min_delivery_interval_seconds,omitempty"`
	ScanRetryLimit     *int     `json:"scan_retry_limit,omitempty"`
	DeliveryRetryLimit *int     `json:"delivery_retry_limit,omitempty"`
	OnUncertain        string   `json:"on_uncertain,omitempty"`
	ProxyURL           string   `json:"proxy_url,omitempty"`
}

// Omitted phase limits inherit the legacy limit, including its zero default.
// A scan_retry_limit of -1 enables unlimited steady-state exponential backoff.
func (c Config) ScanRetries() int {
	if c.ScanRetryLimit != nil {
		return *c.ScanRetryLimit
	}
	return c.RetryLimit
}

func (c Config) UnlimitedScanRetries() bool {
	return c.ScanRetries() < 0
}

func (c Config) DeliveryRetries() int {
	if c.DeliveryRetryLimit != nil {
		return *c.DeliveryRetryLimit
	}
	return c.RetryLimit
}

const ExplicitAck = "explicit_ack"
const HTTPAccepted = "http_accepted"

const (
	UncertainHalt  = "halt"
	UncertainSkip  = "skip"
	UncertainRetry = "retry"
)

func (c Config) Mode() string {
	if c.DeliveryMode == "" {
		return ExplicitAck
	}
	return c.DeliveryMode
}

func (c Config) UncertainPolicy() string {
	if c.OnUncertain == "" {
		return UncertainHalt
	}
	return c.OnUncertain
}

// ConsumerDigest binds routing and filtering identity (socket, scope, destination,
// delivery mode and filters) without binding operational tuning parameters like
// timeouts, retry limits, batching windows or proxy URLs.
func (c Config) ConsumerDigest(socket, scope string) string {
	kinds := slices.Clone(c.Kinds)
	slices.Sort(kinds)
	changes := slices.Clone(c.Changes)
	slices.Sort(changes)
	bots := slices.Clone(c.ExcludeBots)
	slices.Sort(bots)
	return apiclient.Digest(struct {
		Socket, Scope, URL, Mode string
		Kinds                    []string
		Changes                  []int
		ExcludeBots              []string
	}{socket, scope, c.URL, c.Mode(), kinds, changes, bots})
}

// LegacyConsumerDigest reproduces the pre-0.6 full-config consumer digest so
// existing relay queues, sealed batch IDs, and receipts resume across upgrades.
func (c Config) LegacyConsumerDigest(socket, scope string) string {
	return apiclient.Digest(struct {
		Socket, Scope string
		Config        Config
	}{socket, scope, c})
}

func DefaultConfig() Config {
	return Config{Version: 1, Kinds: []string{"topic_event", "topic_reply_event"}, Changes: []int{1, 2, 3}, ExcludeSelf: true, BatchWindowSeconds: 5, BatchSize: 50, QueueLimit: 1000, TimeoutSeconds: 30, RetryBaseSeconds: 5}
}

func LoadConfig(path string) (Config, error) {
	c := DefaultConfig()
	abs, err := filepath.Abs(path)
	if err != nil || path == "" {
		return c, configError()
	}
	b, err := securefs.ReadPrivate(abs, 16384, true)
	if err != nil || safejson.Decode(b, &c, true) != nil {
		return c, configError()
	}
	return c, c.Validate()
}

func configError() error {
	return e.New(e.CategoryConfig, "RELAY_CONFIG", "Use a protected relay JSON configuration with a fixed HTTPS webhook (HTTP only for literal loopback), valid filters and bounded delivery limits.").WithNextSteps("wecom-channel-cli relay run --help", "Review deploy/relay.example.json and docs/watching.md.")
}

func (c Config) Validate() error {
	if (c.ScanRetries() < -1 || c.ScanRetries() > 20) || c.DeliveryRetries() < 0 || c.DeliveryRetries() > 20 {
		return configError()
	}
	if (c.Mode() != ExplicitAck && c.Mode() != HTTPAccepted) || c.MinDeliverySeconds < 0 || c.MinDeliverySeconds > 3600 || (c.Mode() == HTTPAccepted && c.TokenEnv == "") {
		return configError()
	}
	switch c.UncertainPolicy() {
	case UncertainHalt, UncertainSkip, UncertainRetry:
	default:
		return configError()
	}
	if _, err := transport.ValidateProxyURL(c.ProxyURL); err != nil {
		return configError()
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery || len(c.URL) > 2048 {
		return configError()
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback()) {
		return configError()
	}
	if c.Version != 1 || c.BatchWindowSeconds < 1 || c.BatchWindowSeconds > 300 || c.BatchSize < 1 || c.BatchSize > 100 || c.QueueLimit < c.BatchSize || c.QueueLimit > 10000 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 120 || c.RetryLimit < 0 || c.RetryLimit > 20 || c.RetryBaseSeconds < 1 || c.RetryBaseSeconds > 300 {
		return configError()
	}
	if c.TokenEnv != "" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(c.TokenEnv) {
		return configError()
	}
	if len(c.Kinds) < 1 || len(c.Kinds) > 2 || len(c.Changes) < 1 || len(c.Changes) > 3 || len(c.ExcludeBots) > 100 {
		return configError()
	}
	for _, k := range c.Kinds {
		if k != "topic_event" && k != "topic_reply_event" {
			return configError()
		}
	}
	for _, change := range c.Changes {
		if change < 1 || change > 3 {
			return configError()
		}
	}
	for _, id := range c.ExcludeBots {
		if !apiclient.Identifier(id, 256) {
			return configError()
		}
	}
	return nil
}

func (c Config) Accept(v callback.Event) bool {
	if v.IsSelf() || !slices.Contains(c.Kinds, v.Kind) || !slices.Contains(c.Changes, v.Change) {
		return false
	}
	// A snapshot's creator is not the actor of an edit. Unknown actors survive.
	return v.SenderKind != "bot" || !slices.Contains(c.ExcludeBots, v.Sender)
}
