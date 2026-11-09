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
)

type Config struct {
	Version            int      `json:"version"`
	URL                string   `json:"webhook_url"`
	TokenEnv           string   `json:"token_env,omitempty"`
	Kinds              []string `json:"kinds"`
	Changes            []int    `json:"changes"`
	ExcludeSelf        bool     `json:"exclude_self"`
	ExcludeBots        []string `json:"exclude_bots,omitempty"`
	BatchWindowSeconds int      `json:"batch_window_seconds"`
	BatchSize          int      `json:"batch_size"`
	QueueLimit         int      `json:"queue_limit"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	RetryLimit         int      `json:"retry_limit"`
	RetryBaseSeconds   int      `json:"retry_base_seconds"`
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
	return e.New(e.CategoryConfig, "RELAY_CONFIG", "Use a protected relay JSON configuration with a fixed HTTPS webhook (HTTP only for literal loopback), valid filters and bounded delivery limits.").WithNextSteps("wecom-channel-cli event relay --help", "Review deploy/relay.example.json and docs/watching.md.")
}

func (c Config) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery || len(c.URL) > 2048 {
		return configError()
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback()) {
		return configError()
	}
	if c.Version != 1 || c.BatchWindowSeconds < 1 || c.BatchWindowSeconds > 300 || c.BatchSize < 1 || c.BatchSize > 100 || c.QueueLimit < c.BatchSize || c.QueueLimit > 10000 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 30 || c.RetryLimit < 0 || c.RetryLimit > 20 || c.RetryBaseSeconds < 1 || c.RetryBaseSeconds > 300 {
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
	if !slices.Contains(c.Kinds, v.Kind) || !slices.Contains(c.Changes, v.Change) {
		return false
	}
	// A snapshot's creator is not the actor of an edit. Unknown actors survive.
	return v.SenderKind != "bot" || !((c.ExcludeSelf && v.Sender == v.Bot) || slices.Contains(c.ExcludeBots, v.Sender))
}
