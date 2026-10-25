// Package config loads operator policy, never agent-supplied configuration.
package config

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

type Channel struct {
	ObserveAll   bool     `json:"observe_all"`
	ID           string   `json:"id"`
	Senders      []string `json:"senders"`
	ReadAll      bool     `json:"read_all"`
	AllowPosts   bool     `json:"allow_posts"`
	AllowReplies bool     `json:"allow_replies"`
}

type Config struct {
	Name               string    `json:"name"`
	Version            int       `json:"version"`
	Bot                string    `json:"bot_id"`
	StateDir           string    `json:"state_dir"`
	AgentSocket        string    `json:"agent_socket"`
	AdminSocket        string    `json:"admin_socket"`
	CallbackListen     string    `json:"callback_listen"`
	ReadOnly           bool      `json:"read_only"`
	Channels           []Channel `json:"channels"`
	Events             []string  `json:"events"`
	MaxEvents          int       `json:"max_events"`
	MaxProposals       int       `json:"max_proposals"`
	RetentionHours     int       `json:"retention_hours"`
	ProposalTTLSeconds int       `json:"proposal_ttl_seconds"`
	RequestsPerMinute  int       `json:"requests_per_minute"`
}

func Default() Config {
	return Config{Name: "default", Version: 1, ReadOnly: true, Events: []string{"topic_at_bot"}, MaxEvents: 5000, MaxProposals: 1000, RetentionHours: 48, ProposalTTLSeconds: 3600, RequestsPerMinute: 60}
}

func Load(path string) (Config, error) {
	c := Default()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return c, e.New(e.CategoryConfig, "CONFIG_PATH", "Use a canonical absolute operator policy path.")
	}
	if err := securefs.Directory(filepath.Dir(path), false, true); err != nil {
		return c, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return c, e.New(e.CategoryConfig, "CONFIG_UNSAFE", "Policy must be an existing regular file, not writable by group or others.")
	}
	var stat unix.Stat_t
	if unix.Lstat(path, &stat) != nil || stat.Uid != 0 && int(stat.Uid) != os.Geteuid() {
		return c, e.New(e.CategoryConfig, "CONFIG_OWNER", "The policy must be owned by root or the broker identity.")
	}
	f, err := os.Open(path)
	if err != nil {
		return c, e.New(e.CategoryConfig, "CONFIG_READ", "Cannot read operator policy.")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 || safejson.Decode(b, &c, true) != nil {
		return c, e.New(e.CategoryConfig, "CONFIG_INVALID", "Policy is invalid, ambiguous, too large, or contains unknown fields.")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	bad := func() error {
		return e.New(e.CategoryConfig, "CONFIG_INVALID", "Invalid broker policy; see the example and security guide.")
	}
	if c.Version != 1 || !apiclient.Identifier(c.Bot, 256) || c.MaxEvents < 1 || c.MaxEvents > 100000 || c.MaxProposals < 1 || c.MaxProposals > 10000 || c.RetentionHours < 24 || c.RetentionHours > 168 || c.ProposalTTLSeconds < 60 || c.ProposalTTLSeconds > 3600 || c.RequestsPerMinute < 1 || c.RequestsPerMinute > 600 {
		return bad()
	}
	for _, p := range []string{c.StateDir, c.AgentSocket, c.AdminSocket} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || strings.ContainsAny(p, "\r\n") {
			return bad()
		}
	}
	if len(c.AgentSocket) > 100 || len(c.AdminSocket) > 100 || c.AgentSocket == c.AdminSocket || filepath.Dir(c.AgentSocket) == filepath.Dir(c.AdminSocket) {
		return bad()
	}
	if c.CallbackListen != "" {
		host, port, err := net.SplitHostPort(c.CallbackListen)
		if err != nil || port == "" {
			return bad()
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return e.New(e.CategoryConfig, "CALLBACK_BIND", "Bind callbacks to a loopback IP behind a TLS reverse proxy.")
		}
	}
	if !ValidContextName(c.Name) || len(c.Channels) > 1 {
		return bad()
	}
	seen := map[string]bool{}
	for _, ch := range c.Channels {
		if !apiclient.Identifier(ch.ID, 128) || ch.ID == "*" || seen[ch.ID] || len(ch.Senders) > 1000 {
			return bad()
		}
		seen[ch.ID] = true
		if ch.ObserveAll && !ch.ReadAll {
			return e.New(e.CategoryConfig, "OBSERVATION_SCOPE", "observe_all requires read_all in the same channel context.")
		}
		for _, s := range ch.Senders {
			if !apiclient.Identifier(s, 256) || s == "*" {
				return bad()
			}
		}
	}
	for _, ev := range c.Events {
		switch ev {
		case "topic_at_bot", "topic_event", "topic_reply_event", "topic_bot_join_leave", "topic_member_change", "topic_info_change", "feedback_event":
		default:
			return bad()
		}
	}
	return nil
}

func (c Config) Channel(id string) (Channel, bool) {
	for _, v := range c.Channels {
		if v.ID == id {
			return v, true
		}
	}
	return Channel{}, false
}

// AllowsEvent admits observation data separately from human mention triggers.
func (c Config) AllowsEvent(channel, sender, kind, senderKind string) bool {
	ch, ok := c.Channel(channel)
	if !ok {
		return false
	}
	enabled := false
	for _, v := range c.Events {
		if v == kind {
			enabled = true
		}
	}
	if !enabled {
		return false
	}
	if ch.ObserveAll && (kind == "topic_event" || kind == "topic_reply_event") {
		return senderKind == "user" || senderKind == "bot"
	}
	if senderKind != "user" {
		return false
	}
	for _, s := range ch.Senders {
		if s == sender {
			return true
		}
	}
	return false
}
