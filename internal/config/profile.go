package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const maxProfileBytes = 4 << 20

// Profile is the unified single-file configuration created by `connect` and
// shared by `listen`, `serve`, and `status`. Operational tuning (network,
// polling, webhook pacing/retries) is intentionally decoupled from checkpoint
// identity so tuning parameters never invalidate saved baselines or cursors.
type Profile struct {
	Version  int              `yaml:"version" json:"version"`
	Bot      string           `yaml:"bot_id" json:"bot_id"`
	DataDir  string           `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	Network  ProfileNetwork   `yaml:"network,omitempty" json:"network,omitempty"`
	Polling  ProfilePolling   `yaml:"polling,omitempty" json:"polling,omitempty"`
	Webhook  ProfileWebhook   `yaml:"webhook,omitempty" json:"webhook,omitempty"`
	Channels []ProfileChannel `yaml:"channels" json:"channels"`
}

type ProfileNetwork struct {
	TimeoutSeconds int    `yaml:"timeout_seconds,omitempty" json:"timeout_seconds,omitempty"`
	Proxy          string `yaml:"proxy,omitempty" json:"proxy,omitempty"`
	ReadRetries    int    `yaml:"read_retries,omitempty" json:"read_retries,omitempty"`
}

type ProfilePolling struct {
	IntervalSeconds   int `yaml:"interval_seconds,omitempty" json:"interval_seconds,omitempty"`
	MaxRequests       int `yaml:"max_requests,omitempty" json:"max_requests,omitempty"`
	MaxRecords        int `yaml:"max_records,omitempty" json:"max_records,omitempty"`
	RequestsPerMinute int `yaml:"requests_per_minute,omitempty" json:"requests_per_minute,omitempty"`
	Concurrency       int `yaml:"concurrency,omitempty" json:"concurrency,omitempty"`
}

type ProfileWebhook struct {
	URL                string   `yaml:"url,omitempty" json:"url,omitempty"`
	TokenEnv           string   `yaml:"token_env,omitempty" json:"token_env,omitempty"`
	DeliveryMode       string   `yaml:"delivery_mode,omitempty" json:"delivery_mode,omitempty"`
	OnUncertain        string   `yaml:"on_uncertain,omitempty" json:"on_uncertain,omitempty"`
	BatchWindowSeconds int      `yaml:"batch_window_seconds,omitempty" json:"batch_window_seconds,omitempty"`
	BatchSize          int      `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	QueueLimit         int      `yaml:"queue_limit,omitempty" json:"queue_limit,omitempty"`
	MinIntervalSeconds *int     `yaml:"min_delivery_interval_seconds,omitempty" json:"min_delivery_interval_seconds,omitempty"`
	TimeoutSeconds     int      `yaml:"timeout_seconds,omitempty" json:"timeout_seconds,omitempty"`
	RetryLimit         *int     `yaml:"retry_limit,omitempty" json:"retry_limit,omitempty"`
	RetryBaseSeconds   int      `yaml:"retry_base_seconds,omitempty" json:"retry_base_seconds,omitempty"`
	Kinds              []string `yaml:"kinds,omitempty" json:"kinds,omitempty"`
	Changes            []int    `yaml:"changes,omitempty" json:"changes,omitempty"`
	ExcludeBots        []string `yaml:"exclude_bots,omitempty" json:"exclude_bots,omitempty"`
	Proxy              string   `yaml:"proxy,omitempty" json:"proxy,omitempty"`
}

type ProfileChannel struct {
	ID       string `yaml:"id" json:"topic_id"`
	Name     string `yaml:"name" json:"topic_name"`
	Context  string `yaml:"context,omitempty" json:"context,omitempty"`
	Created  int64  `yaml:"created_at,omitempty" json:"create_time,omitempty"`
	Selected bool   `yaml:"selected" json:"selected"`
	ReadOnly *bool  `yaml:"read_only,omitempty" json:"read_only,omitempty"`
}

func ProfilePath(dir string) string {
	return filepath.Join(dir, "profile.yaml")
}

func DefaultProfile(bot string) Profile {
	return Profile{
		Version: 1,
		Bot:     bot,
		Network: ProfileNetwork{
			TimeoutSeconds: 45,
			ReadRetries:    3,
		},
		Polling: ProfilePolling{
			IntervalSeconds:   30,
			MaxRequests:       256,
			MaxRecords:        5000,
			RequestsPerMinute: 60,
			Concurrency:       4,
		},
		Channels: []ProfileChannel{},
	}
}

func (p Profile) WithDefaults() Profile {
	d := DefaultProfile(p.Bot)
	if p.Version == 0 {
		p.Version = 1
	}
	if p.Network.TimeoutSeconds == 0 {
		p.Network.TimeoutSeconds = d.Network.TimeoutSeconds
	}
	if p.Network.ReadRetries == 0 {
		p.Network.ReadRetries = d.Network.ReadRetries
	}
	if p.Polling.IntervalSeconds == 0 {
		p.Polling.IntervalSeconds = d.Polling.IntervalSeconds
	}
	if p.Polling.MaxRequests == 0 {
		p.Polling.MaxRequests = d.Polling.MaxRequests
	}
	if p.Polling.MaxRecords == 0 {
		p.Polling.MaxRecords = d.Polling.MaxRecords
	}
	if p.Polling.RequestsPerMinute == 0 {
		p.Polling.RequestsPerMinute = d.Polling.RequestsPerMinute
	}
	if p.Polling.Concurrency == 0 {
		p.Polling.Concurrency = d.Polling.Concurrency
	}
	if p.Channels == nil {
		p.Channels = []ProfileChannel{}
	}
	return p
}

func (p Profile) Validate() error {
	p = p.WithDefaults()
	if p.Version != 1 || !apiclient.Identifier(p.Bot, 256) {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "The saved profile requires version 1 and a valid bot_id.")
	}
	if p.Network.TimeoutSeconds < 5 || p.Network.TimeoutSeconds > 180 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "network.timeout_seconds must be between 5 and 180.")
	}
	if p.Network.ReadRetries < -1 || p.Network.ReadRetries > 5 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "network.read_retries must be between -1 and 5.")
	}
	if _, err := transport.ValidateProxyURL(p.Network.Proxy); err != nil {
		return err
	}
	if p.Polling.IntervalSeconds < 5 || p.Polling.IntervalSeconds > 3600 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "polling.interval_seconds must be between 5 and 3600.")
	}
	if p.Polling.MaxRequests < 1 || p.Polling.MaxRequests > 5000 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "polling.max_requests must be between 1 and 5000.")
	}
	if p.Polling.MaxRecords < 1 || p.Polling.MaxRecords > 100000 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "polling.max_records must be between 1 and 100000.")
	}
	if p.Polling.RequestsPerMinute < 1 || p.Polling.RequestsPerMinute > 600 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "polling.requests_per_minute must be between 1 and 600.")
	}
	if p.Polling.Concurrency < 1 || p.Polling.Concurrency > 8 {
		return e.New(e.CategoryConfig, "PROFILE_INVALID", "polling.concurrency must be between 1 and 8.")
	}
	if _, err := transport.ValidateProxyURL(p.Webhook.Proxy); err != nil {
		return err
	}
	seenID := map[string]bool{}
	seenCtx := map[string]bool{}
	for _, ch := range p.Channels {
		if !apiclient.Identifier(ch.ID, 128) || seenID[ch.ID] {
			return e.New(e.CategoryConfig, "PROFILE_INVALID", "The saved channel catalog contains duplicate or invalid channel IDs.")
		}
		seenID[ch.ID] = true
		if ch.Context != "" {
			if !ValidContextName(ch.Context) || seenCtx[ch.Context] {
				return e.New(e.CategoryConfig, "PROFILE_INVALID", "Channel context names must be unique lowercase identifiers.")
			}
			seenCtx[ch.Context] = true
		}
	}
	return nil
}

func (p Profile) TransportConfig() transport.Config {
	p = p.WithDefaults()
	t := transport.DefaultConfig()
	t.Timeout = time.Duration(p.Network.TimeoutSeconds) * time.Second
	t.DialTimeout = min(15*time.Second, t.Timeout)
	t.TLSHandshakeTimeout = min(15*time.Second, t.Timeout)
	t.ResponseHeaderTimeout = min(30*time.Second, t.Timeout)
	t.ReadMaxRetries = p.Network.ReadRetries
	t.ProxyURL = p.Network.Proxy
	return t
}

func (p Profile) EffectiveDataDir(configDir string) string {
	if strings.TrimSpace(p.DataDir) != "" {
		return p.DataDir
	}
	return filepath.Join(configDir, "data")
}

func (p Profile) SelectedChannels() []ProfileChannel {
	return slices.DeleteFunc(slices.Clone(p.Channels), func(ch ProfileChannel) bool { return !ch.Selected })
}

// ChannelContextName assigns a deterministic, valid lowercase context name to a
// channel, preferring an explicit ch.Context, then an ASCII slug of its name,
// and falling back to a stable bot/channel hash prefix.
func ChannelContextName(bot string, ch ProfileChannel, used map[string]bool) string {
	if ch.Context != "" && ValidContextName(ch.Context) && !used[ch.Context] {
		used[ch.Context] = true
		return ch.Context
	}
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(ch.Name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
		} else if (r == '-' || r == '_' || unicode.IsSpace(r)) && b.Len() > 0 && !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
		if b.Len() >= 40 {
			break
		}
	}
	base := strings.Trim(b.String(), "-_")
	if base == "" || !ValidContextName(base) {
		base = "ch-" + apiclient.Digest([]string{bot, ch.ID})[:12]
	}
	candidate := base
	if used[candidate] {
		candidate = base + "-" + apiclient.Digest([]string{bot, ch.ID})[:6]
		if len(candidate) > 63 {
			candidate = "ch-" + apiclient.Digest([]string{bot, ch.ID})[:12]
		}
	}
	used[candidate] = true
	return candidate
}

// ChannelStateSlug returns a collision-free private subdirectory name bound to
// (bot, channel ID) so channel renames never orphan state or checkpoints.
func ChannelStateSlug(bot, channelID string) string {
	return "ch-" + apiclient.Digest([]string{bot, channelID})[:16]
}

// ChannelSocketPaths derives distinct agent and admin Unix socket paths that
// stay within the 100-byte sockaddr_un limit on both Linux and macOS.
func ChannelSocketPaths(dataDir, slug string) (agentSock, adminSock string, err error) {
	base := filepath.Join(dataDir, "run", slug)
	agentSock = filepath.Join(base+"-a", "a.sock")
	adminSock = filepath.Join(base+"-o", "o.sock")
	if len(agentSock) <= 96 && len(adminSock) <= 96 {
		return agentSock, adminSock, nil
	}
	tmp := os.TempDir()
	if resolved, evalErr := filepath.EvalSymlinks(tmp); evalErr == nil {
		tmp = resolved
	}
	short := filepath.Join(tmp, "wcc-"+apiclient.Digest(dataDir)[:8], slug)
	agentSock = filepath.Join(short+"-a", "a.sock")
	adminSock = filepath.Join(short+"-o", "o.sock")
	if len(agentSock) > 100 || len(adminSock) > 100 {
		return "", "", e.New(e.CategoryConfig, "SOCKET_PATH_LIMIT", "Derived Unix socket path exceeds the 100-byte OS limit; pass a shorter --data-dir.")
	}
	return agentSock, adminSock, nil
}

func LoadProfile(dir string, strict bool) (Profile, bool, error) {
	var p Profile
	path := ProfilePath(dir)
	if strict {
		if err := securefs.Directory(dir, false, true); err != nil {
			return p, false, err
		}
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return p, false, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return p, false, e.New(e.CategoryConfig, "PROFILE_UNSAFE", "profile.yaml must be a regular file not writable by group or others.")
	}
	var st unix.Stat_t
	if unix.Lstat(path, &st) != nil || (int(st.Uid) != os.Geteuid() && st.Uid != 0) {
		return p, false, e.New(e.CategoryConfig, "PROFILE_UNSAFE", "profile.yaml must be owned by the current user or root.")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > maxProfileBytes {
		return p, false, e.New(e.CategoryConfig, "PROFILE_INVALID", "Could not read profile.yaml within its size limit.")
	}
	if err = yaml.Unmarshal(data, &p); err != nil {
		return p, false, e.New(e.CategoryConfig, "PROFILE_INVALID", "Could not parse profile.yaml.")
	}
	p = p.WithDefaults()
	if err = p.Validate(); err != nil {
		return p, false, err
	}
	return p, true, nil
}

func SaveProfile(dir string, p Profile, strict bool) error {
	p = p.WithDefaults()
	if err := p.Validate(); err != nil {
		return err
	}
	resolved, err := securefs.EnsurePrivateDir(dir, strict)
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(p)
	if err != nil || len(data) > maxProfileBytes {
		return e.New(e.CategoryConfig, "PROFILE_LIMIT", "Profile exceeds the 4 MiB limit.")
	}
	return securefs.WriteAtomic(ProfilePath(resolved), data)
}
