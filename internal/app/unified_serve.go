package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// ServiceStatus is the secret-free live snapshot written atomically by `serve`
// and read by `status`.
type ServiceStatus struct {
	Version         int             `json:"version"`
	PID             int             `json:"pid"`
	Bot             string          `json:"bot_id"`
	DataDir         string          `json:"data_dir"`
	StartedAt       int64           `json:"started_at"`
	UpdatedAt       int64           `json:"updated_at"`
	ProxyConfigured bool            `json:"proxy_configured"`
	WebhookEnabled  bool            `json:"webhook_enabled"`
	Channels        []ChannelStatus `json:"channels"`
}

type ChannelStatus struct {
	Name                string                `json:"name"`
	ChannelID           string                `json:"channel_id"`
	Context             string                `json:"context"`
	AgentSocket         string                `json:"agent_socket"`
	AdminSocket         string                `json:"admin_socket"`
	State               string                `json:"state"`
	LastSuccessAt       int64                 `json:"last_success_at,omitempty"`
	LastAttemptAt       int64                 `json:"last_attempt_at,omitempty"`
	ConsecutiveFailures int                   `json:"consecutive_failures,omitempty"`
	NextRetryAt         int64                 `json:"next_retry_at,omitempty"`
	EventsObserved      int64                 `json:"events_observed"`
	LastError           *ChannelErrorStatus   `json:"last_error,omitempty"`
	Webhook             *ChannelWebhookStatus `json:"webhook,omitempty"`
}

type ChannelErrorStatus struct {
	Code          string `json:"code"`
	NetworkReason string `json:"network_reason,omitempty"`
	At            int64  `json:"at"`
}

type ChannelWebhookStatus struct {
	State             string `json:"state,omitempty"`
	Mode              string `json:"delivery_mode"`
	OnUncertain       string `json:"on_uncertain"`
	DeliveryState     string `json:"delivery_state,omitempty"`
	QueuedEvents      int    `json:"queued_events"`
	DeliveredBatches  int64  `json:"delivered_batches"`
	UncertainBatches  int64  `json:"uncertain_batches,omitempty"`
	LastDeliveryAt    int64  `json:"last_delivery_at,omitempty"`
	LastDeliveryError string `json:"last_delivery_error,omitempty"`
}

type statusTracker struct {
	mu          sync.Mutex
	path        string
	status      ServiceStatus
	onNotice    func(map[string]any)
	writeFailed bool
}

func newStatusTracker(path string, init ServiceStatus, onNotice func(map[string]any)) *statusTracker {
	t := &statusTracker{path: path, status: init, onNotice: onNotice}
	t.flushLocked()
	return t
}

func (t *statusTracker) updateChannel(idx int, fn func(*ChannelStatus)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx >= 0 && idx < len(t.status.Channels) {
		fn(&t.status.Channels[idx])
	}
	t.status.UpdatedAt = time.Now().UnixMilli()
	t.flushLocked()
}

func (t *statusTracker) heartbeat() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.UpdatedAt = time.Now().UnixMilli()
	t.flushLocked()
}

func (t *statusTracker) flushLocked() {
	b, err := json.MarshalIndent(t.status, "", "  ")
	if err == nil {
		err = securefs.WriteAtomic(t.path, b)
	}
	if err != nil {
		if !t.writeFailed && t.onNotice != nil {
			t.writeFailed = true
			t.onNotice(map[string]any{"serve": map[string]any{"state": "status_write_failed", "code": e.Safe(err).Code}})
		}
		return
	}
	if t.writeFailed {
		t.writeFailed = false
		if t.onNotice != nil {
			t.onNotice(map[string]any{"serve": map[string]any{"state": "status_write_recovered"}})
		}
	}
}

func buildServeRelayConfig(p config.Profile) (relay.Config, error) {
	w := p.Webhook
	cfg := relay.DefaultConfig()
	cfg.URL = strings.TrimSpace(w.URL)
	cfg.TokenEnv = strings.TrimSpace(w.TokenEnv)
	cfg.DeliveryMode = strings.TrimSpace(w.DeliveryMode)
	if cfg.DeliveryMode == "" {
		cfg.DeliveryMode = relay.HTTPAccepted
	}
	cfg.OnUncertain = strings.TrimSpace(w.OnUncertain)
	if cfg.OnUncertain == "" {
		cfg.OnUncertain = relay.UncertainSkip
	}
	if w.BatchWindowSeconds > 0 {
		cfg.BatchWindowSeconds = w.BatchWindowSeconds
	} else {
		cfg.BatchWindowSeconds = 15
	}
	if w.BatchSize > 0 {
		cfg.BatchSize = w.BatchSize
	}
	if w.QueueLimit > 0 {
		cfg.QueueLimit = w.QueueLimit
	}
	if w.MinIntervalSeconds != nil {
		cfg.MinDeliverySeconds = *w.MinIntervalSeconds
	} else if cfg.Mode() == relay.HTTPAccepted {
		cfg.MinDeliverySeconds = 60
	}
	if w.TimeoutSeconds > 0 {
		cfg.TimeoutSeconds = w.TimeoutSeconds
	} else {
		cfg.TimeoutSeconds = 45
	}
	if w.RetryBaseSeconds > 0 {
		cfg.RetryBaseSeconds = w.RetryBaseSeconds
	}
	if len(w.Kinds) > 0 {
		cfg.Kinds = w.Kinds
	}
	if len(w.Changes) > 0 {
		cfg.Changes = w.Changes
	}
	if len(w.ExcludeBots) > 0 {
		cfg.ExcludeBots = w.ExcludeBots
	}
	if strings.TrimSpace(w.Proxy) != "" {
		cfg.ProxyURL = strings.TrimSpace(w.Proxy)
	} else {
		cfg.ProxyURL = p.Network.Proxy
	}
	unlimited := -1
	cfg.ScanRetryLimit = &unlimited
	deliveryRetries := 3
	if w.RetryLimit != nil {
		deliveryRetries = *w.RetryLimit
	}
	cfg.DeliveryRetryLimit = &deliveryRetries
	return cfg, cfg.Validate()
}

func syncServeClientContexts(configDir, bot string, entries []ChannelStatus) error {
	f, err := config.ReadClients(configDir)
	if err != nil {
		return err
	}
	byName := make(map[string]int, len(f.Contexts))
	for i, c := range f.Contexts {
		byName[c.Name] = i
	}
	for _, entry := range entries {
		cc := config.ClientContext{
			Name:    entry.Context,
			Server:  apiclient.Origin,
			Socket:  entry.AgentSocket,
			Bot:     bot,
			Channel: entry.ChannelID,
		}
		if idx, ok := byName[cc.Name]; ok {
			existing := f.Contexts[idx]
			if existing.Bot != bot || existing.Channel != entry.ChannelID {
				return e.New(e.CategoryConflict, "CONTEXT_CONFLICT", "Saved client context "+cc.Name+" already belongs to a different bot or channel. Assign a distinct context name in profile.yaml before running serve.")
			}
			f.Contexts[idx] = cc
		} else {
			byName[cc.Name] = len(f.Contexts)
			f.Contexts = append(f.Contexts, cc)
		}
	}
	if f.Current == "" && len(entries) > 0 {
		f.Current = entries[0].Context
	}
	return config.SaveClients(configDir, f)
}

func (s *appState) serveCommand(runtime profileRuntime) *cobra.Command {
	var selection channelSelection
	var webhookURL, webhookTokenEnv, deliveryMode, onUncertain, proxy, dataDir string
	var interval time.Duration
	var saveFlags, retryFailed, strictFS, dry bool

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the unified single-process channel daemon (polling, local CLI sockets, and optional webhook delivery)",
		Args:  cobra.NoArgs,
		Long: "Run one self-healing daemon from profile.yaml (created by `connect`).\n" +
			"In a single process it:\n" +
			"  1. Polls selected channels concurrently with read retries and unlimited steady-state backoff;\n" +
			"  2. Serves local agent/admin Unix sockets (cleaning stale sockets automatically) and syncs client contexts;\n" +
			"  3. Optionally batches and delivers non-empty observations to a configured webhook (e.g. Grok routines);\n" +
			"  4. Writes live health snapshots readable by `wecom-channel-cli status`.\n" +
			"Runs continuously until SIGINT/SIGTERM (no 24-hour forced exit).",
		Example: "  wecom-channel-cli serve\n" +
			"  wecom-channel-cli serve --channel Engineering --webhook-url https://... --webhook-token-env GROK_ROUTINE_SENDER_KEY --save\n" +
			"  wecom-channel-cli serve --retry-failed\n" +
			"  wecom-channel-cli status",
	}
	selection.bindFlags(cmd)
	cmd.Flags().StringVar(&webhookURL, "webhook-url", "", "Optional webhook URL (HTTPS or loopback HTTP) for unattended delivery")
	cmd.Flags().StringVar(&webhookTokenEnv, "webhook-token-env", "", "Environment variable name holding the webhook Bearer token (e.g. GROK_ROUTINE_SENDER_KEY)")
	cmd.Flags().StringVar(&deliveryMode, "delivery-mode", "", "Webhook contract: http_accepted (default, Grok) or explicit_ack")
	cmd.Flags().StringVar(&onUncertain, "on-uncertain", "", "Action on post-send uncertain webhook outcome: skip (default), retry, or halt")
	cmd.Flags().StringVar(&proxy, "proxy", "", "Optional explicit proxy URL (http/https/socks5/socks5h)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Optional private data directory override")
	cmd.Flags().DurationVar(&interval, "interval", 0, "Optional poll interval override (5s–1h)")
	cmd.Flags().BoolVar(&saveFlags, "save", false, "Persist flag overrides back into profile.yaml before starting")
	cmd.Flags().BoolVar(&retryFailed, "retry-failed", false, "Explicitly reset persisted webhook delivery failures after repairing the receiver or token")
	cmd.Flags().BoolVar(&strictFS, "strict-fs", false, "Enforce full ancestor directory checks up to /")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Preview resolved channels, sockets and webhook config without starting")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.profileOperatorDir(cmd)
		if err != nil {
			return err
		}
		if cmd.Flags().Changed("interval") && (interval < 5*time.Second || interval > time.Hour) {
			return e.New(e.CategoryUsage, "SERVE_OPTIONS", "--interval must be between 5s and 1h.")
		}
		if cmd.Flags().Changed("proxy") {
			if _, err := transport.ValidateProxyURL(proxy); err != nil {
				return err
			}
		}
		if !dry {
			if err = s.guard("serve"); err != nil {
				return err
			}
		}
		var profileLock *os.File
		if !dry && (saveFlags || selection.explicit()) {
			if dir, err = securefs.EnsurePrivateDir(dir, strictFS); err != nil {
				return err
			}
			if profileLock, err = lockProfile(dir); err != nil {
				return err
			}
		}
		releaseProfileLock := func() {
			if profileLock != nil {
				_ = profileLock.Close()
				profileLock = nil
			}
		}
		defer releaseProfileLock()

		p, exists, err := config.LoadProfile(dir, strictFS)
		if err != nil {
			return err
		}
		if !exists || p.Bot == "" {
			return profileMissingError()
		}
		if !dry && !selection.explicit() && len(p.SelectedChannels()) == 0 && len(p.Channels) > 0 && runtime.interactive(cmd.InOrStdin()) {
			selection, err = promptChannelSelection(cmd, p)
			if err != nil {
				return err
			}
			if selection.explicit() && profileLock == nil {
				if dir, err = securefs.EnsurePrivateDir(dir, strictFS); err != nil {
					return err
				}
				if profileLock, err = lockProfile(dir); err != nil {
					return err
				}
				p, exists, err = config.LoadProfile(dir, strictFS)
				if err != nil {
					return err
				}
				if !exists || p.Bot == "" {
					return profileMissingError()
				}
			}
		}
		baseProfile := p
		baseProfile.Channels = append([]config.ProfileChannel(nil), p.Channels...)
		if cmd.Flags().Changed("interval") {
			p.Polling.IntervalSeconds = int(interval / time.Second)
		}
		if cmd.Flags().Changed("proxy") {
			p.Network.Proxy = strings.TrimSpace(proxy)
		}
		if cmd.Flags().Changed("data-dir") {
			p.DataDir = strings.TrimSpace(dataDir)
		}
		if cmd.Flags().Changed("webhook-url") {
			p.Webhook.URL = strings.TrimSpace(webhookURL)
		}
		if cmd.Flags().Changed("webhook-token-env") {
			p.Webhook.TokenEnv = strings.TrimSpace(webhookTokenEnv)
		}
		if cmd.Flags().Changed("delivery-mode") {
			p.Webhook.DeliveryMode = strings.TrimSpace(deliveryMode)
		}
		if cmd.Flags().Changed("on-uncertain") {
			p.Webhook.OnUncertain = strings.TrimSpace(onUncertain)
		}
		if selection.explicit() {
			if err = selection.apply(&p); err != nil {
				return err
			}
		}
		channels := p.SelectedChannels()
		if len(channels) == 0 {
			return e.New(e.CategoryUsage, "NO_CHANNELS_SELECTED", "Select at least one channel with --channel NAME or --all.")
		}
		if len(channels) > 32 {
			return e.New(e.CategoryUsage, "SERVE_SCOPE_LIMIT", "Serve supports at most 32 channels per bot.")
		}
		var rcfg relay.Config
		webhookEnabled := strings.TrimSpace(p.Webhook.URL) != ""
		if webhookEnabled {
			rcfg, err = buildServeRelayConfig(p)
			if err != nil {
				return err
			}
		}
		savedClients, err := config.ReadClients(dir)
		if err != nil {
			return err
		}
		ctxNames := resolveChannelContexts(p.Bot, channels, savedClients.Contexts)
		effectiveData := p.EffectiveDataDir(dir)
		previewChannels := make([]ChannelStatus, 0, len(channels))
		for i, ch := range channels {
			ctxName := ctxNames[i]
			slug := config.ChannelStateSlug(p.Bot, ch.ID)
			agentSock, adminSock, sockErr := config.ChannelSocketPaths(effectiveData, slug)
			if sockErr != nil {
				return sockErr
			}
			cs := ChannelStatus{
				Name:        ch.Name,
				ChannelID:   ch.ID,
				Context:     ctxName,
				AgentSocket: agentSock,
				AdminSocket: adminSock,
				State:       "starting",
			}
			if webhookEnabled {
				cs.Webhook = &ChannelWebhookStatus{
					State:       "active",
					Mode:        rcfg.Mode(),
					OnUncertain: rcfg.UncertainPolicy(),
				}
			}
			previewChannels = append(previewChannels, cs)
		}
		if dry {
			return s.emit(cmd, map[string]any{
				"dry_run":          true,
				"bot_id":           p.Bot,
				"profile":          config.ProfilePath(dir),
				"data_dir":         effectiveData,
				"proxy_configured": p.Network.Proxy != "",
				"webhook_enabled":  webhookEnabled,
				"channels":         previewChannels,
			})
		}
		// Validate webhook (including token env) before persisting --save so
		// failed invocations never write half-configured destinations.
		var hook *relay.Webhook
		if webhookEnabled {
			hook, err = relay.NewWebhook(rcfg)
			if err != nil {
				return err
			}
		}
		if saveFlags {
			if err = config.SaveProfile(dir, p, strictFS); err != nil {
				return err
			}
		} else if selection.explicit() {
			if err = selection.apply(&baseProfile); err != nil {
				return err
			}
			if err = config.SaveProfile(dir, baseProfile, strictFS); err != nil {
				return err
			}
		}
		releaseProfileLock()
		secret, _, err := loadProfileSecret(cmd, p.Bot, runtime.credentials(dir), false)
		if err != nil {
			return err
		}
		api, err := runtime.client(p.Bot, secret, p.TransportConfig())
		if err != nil {
			return err
		}
		resolvedData, err := securefs.EnsurePrivateDir(effectiveData, strictFS)
		if err != nil {
			return err
		}
		lockPath := filepath.Join(resolvedData, "serve.lock")
		lock, err := securefs.LockPrivate(lockPath)
		if err != nil {
			if err == unix.EWOULDBLOCK {
				return e.New(e.CategoryConflict, "SERVICE_RUNNING", "Another wecom-channel-cli serve instance already owns this data directory.")
			}
			return e.New(e.CategoryConfig, "SERVICE_LOCK", "Could not acquire serve.lock in the data directory.")
		}
		defer lock.Close()
		now := time.Now()
		_ = os.Chtimes(lockPath, now, now)

		// Recompute socket paths against canonical resolvedData so config.yaml
		// and unixListener agree on exact paths.
		statuses := make([]ChannelStatus, 0, len(channels))
		brokers := make([]*broker.Broker, 0, len(channels))
		var stores []*state.Store
		defer func() {
			for _, st := range stores {
				_ = st.Close()
			}
		}()

		for i, ch := range channels {
			ctxName := ctxNames[i]
			slug := config.ChannelStateSlug(p.Bot, ch.ID)
			chDir := filepath.Join(resolvedData, "channels", slug)
			agentSock, adminSock, sockErr := config.ChannelSocketPaths(resolvedData, slug)
			if sockErr != nil {
				return sockErr
			}
			if strictFS {
				if _, dirErr := securefs.EnsurePrivateDir(filepath.Dir(agentSock), true); dirErr != nil {
					return dirErr
				}
				if _, dirErr := securefs.EnsurePrivateDir(filepath.Dir(adminSock), true); dirErr != nil {
					return dirErr
				}
			}
			var st *state.Store
			if strictFS {
				st, err = state.Open(chDir, p.Bot, ch.ID)
			} else {
				st, err = state.OpenRelaxed(chDir, p.Bot, ch.ID)
			}
			if err != nil {
				return err
			}
			stores = append(stores, st)
			bcfg := buildChannelBrokerConfig(p, ch, ctxName, chDir, agentSock, adminSock)
			brokers = append(brokers, broker.New(bcfg, st, api, nil))
			cs := ChannelStatus{
				Name:        ch.Name,
				ChannelID:   ch.ID,
				Context:     ctxName,
				AgentSocket: agentSock,
				AdminSocket: adminSock,
				State:       "starting",
			}
			if webhookEnabled {
				cs.Webhook = &ChannelWebhookStatus{
					State:       "active",
					Mode:        rcfg.Mode(),
					OnUncertain: rcfg.UncertainPolicy(),
				}
			}
			statuses = append(statuses, cs)
		}

		if err = syncServeClientContexts(dir, p.Bot, statuses); err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		var noticeMu sync.Mutex
		emitNotice := func(payload map[string]any) {
			noticeMu.Lock()
			defer noticeMu.Unlock()
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": payload})
		}

		nowMs := now.UnixMilli()
		tracker := newStatusTracker(filepath.Join(resolvedData, "status.json"), ServiceStatus{
			Version:         1,
			PID:             os.Getpid(),
			Bot:             p.Bot,
			DataDir:         resolvedData,
			StartedAt:       nowMs,
			UpdatedAt:       nowMs,
			ProxyConfigured: p.Network.Proxy != "",
			WebhookEnabled:  webhookEnabled,
			Channels:        statuses,
		}, emitNotice)

		serverErr := make(chan error, 1)
		socketsReady := make(chan struct{})
		go func() {
			serverErr <- broker.ServeContextsWithMode(ctx, brokers, "", strictFS, func() {
				close(socketsReady)
			})
		}()

		select {
		case <-socketsReady:
		case err = <-serverErr:
			return err
		case <-ctx.Done():
			<-serverErr
			return nil
		}

		emitNotice(map[string]any{
			"serve":            "ready",
			"bot_id":           p.Bot,
			"channels":         len(brokers),
			"webhook_enabled":  webhookEnabled,
			"proxy_configured": p.Network.Proxy != "",
			"status_file":      filepath.Join(resolvedData, "status.json"),
		})

		var wg sync.WaitGroup
		intervalDur := time.Duration(p.Polling.IntervalSeconds) * time.Second

		// Periodic heartbeat so status reflects active daemon liveness.
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					tracker.heartbeat()
				}
			}
		}()

		for idx, b := range brokers {
			ch := channels[idx]
			stInfo := statuses[idx]
			slug := config.ChannelStateSlug(p.Bot, ch.ID)
			chDir := filepath.Join(resolvedData, "channels", slug)
			var wakeCh chan struct{}

			// Prepare the durable webhook state before launching the poller so
			// initial FromNow baselines never race against newly polled events.
			var rstore *relay.Store
			var rst relay.State
			var deliveryPath string
			if webhookEnabled {
				wakeCh = make(chan struct{}, 1)
				deliveryPath = filepath.Join(chDir, "delivery.json")
				if strictFS {
					rstore, err = relay.Open(deliveryPath)
				} else {
					rstore, err = relay.OpenRelaxed(deliveryPath)
				}
				if err != nil {
					cancel()
					wg.Wait()
					<-serverErr
					return err
				}
				defer rstore.Close()
				var rexists bool
				if strictFS {
					rst, rexists, err = relay.Read(deliveryPath)
				} else {
					rst, rexists, err = relay.ReadRelaxed(deliveryPath)
				}
				if err != nil {
					cancel()
					wg.Wait()
					<-serverErr
					return err
				}
				scan := broker.ScanRequest{WholeChannel: true, Limit: 100}
				consumer := rcfg.ConsumerDigest(stInfo.Context, watchScope(scan))
				if rexists && rst.Consumer != consumer {
					cancel()
					wg.Wait()
					<-serverErr
					return e.New(e.CategoryConflict, "RELAY_BINDING_CHANGED", "Webhook URL, mode, or event filters changed for "+stInfo.Context+". Reconcile "+deliveryPath+" before switching destinations.")
				}
				if !rexists {
					rst = relay.State{Version: 1, Consumer: consumer, FromNow: true, StartedAt: nowMs}
					if rcfg.Mode() == relay.HTTPAccepted {
						rst.DeliveryMode = relay.HTTPAccepted
					}
					initScan := broker.ScanRequest{WholeChannel: true, Limit: 100, FromNow: true}
					resp, initErr := b.Handle(ctx, broker.Request{Operation: "event.scan", Scan: &initScan, DryRun: true}, false)
					if initErr != nil {
						cancel()
						wg.Wait()
						<-serverErr
						return initErr
					}
					if err = rst.Collect(*resp.Scan, rcfg, time.Now()); err != nil {
						cancel()
						wg.Wait()
						<-serverErr
						return err
					}
					if err = rstore.Save(rst); err != nil {
						cancel()
						wg.Wait()
						<-serverErr
						return err
					}
				} else if retryFailed && rst.Inflight != nil {
					if rcfg.Mode() == relay.HTTPAccepted && (rst.Inflight.Status == "unknown" || rst.Inflight.Status == "sending") {
						if rcfg.UncertainPolicy() == relay.UncertainHalt {
							cancel()
							wg.Wait()
							<-serverErr
							return relay.UnknownOutcome()
						}
						// Leave sending/unknown batches to pumpRelayWithWake's
						// uncertainty handler (on_uncertain: skip or retry).
					} else if rst.Inflight.Failure != "" || rst.Inflight.Attempts > rcfg.DeliveryRetries() {
						rst.Inflight.Attempts, rst.Inflight.NextAttempt = 0, 0
						rst.Inflight.Status, rst.Inflight.HTTPStatus, rst.Inflight.Failure = "", 0, ""
						rst.NextDelivery = 0
						if err = rstore.Save(rst); err != nil {
							cancel()
							wg.Wait()
							<-serverErr
							return err
						}
					}
				}
				tracker.updateChannel(idx, func(cs *ChannelStatus) {
					cs.Webhook = &ChannelWebhookStatus{
						State:            "active",
						Mode:             rcfg.Mode(),
						OnUncertain:      rcfg.UncertainPolicy(),
						DeliveryState:    deliveryPath,
						QueuedEvents:     rst.Depth(),
						DeliveredBatches: rst.AckedBatches + rst.AcceptedBatches,
						UncertainBatches: rst.UncertainBatches,
						LastDeliveryAt:   max(rst.LastAck, rst.LastAccepted),
					}
				})
			}

			// Background poller goroutine per channel: concurrent reply workers +
			// unlimited steady-state exponential backoff on transient failures.
			wg.Add(1)
			go func(idx int, b *broker.Broker, ch config.ProfileChannel, ctxName string, wakeCh chan struct{}) {
				defer wg.Done()
				failures := 0
				for {
					if ctx.Err() != nil {
						return
					}
					attemptAt := time.Now().UnixMilli()
					added, lastSnapshot, pollErr := b.PollNow(ctx)
					if ctx.Err() != nil {
						return
					}
					if pollErr != nil {
						meta := apiclient.ReadFailureMetadata(e.Safe(pollErr).Details)
						code := e.Safe(pollErr).Code
						if !isRecoverablePollError(pollErr) {
							tracker.updateChannel(idx, func(cs *ChannelStatus) {
								cs.State = "blocked"
								cs.LastAttemptAt = attemptAt
								cs.NextRetryAt = 0
								cs.LastError = &ChannelErrorStatus{Code: code, NetworkReason: meta.NetworkReason, At: attemptAt}
							})
							emitNotice(map[string]any{"serve": map[string]any{"state": "blocked", "context": ctxName, "channel_id": ch.ID, "code": code, "network_reason": meta.NetworkReason}})
							return
						}
						failures++
						delay := steadyPollBackoff(failures, intervalDur, pollErr)
						nextAt := time.Now().Add(delay).UnixMilli()
						tracker.updateChannel(idx, func(cs *ChannelStatus) {
							cs.State = "degraded"
							cs.LastAttemptAt = attemptAt
							cs.ConsecutiveFailures = failures
							cs.NextRetryAt = nextAt
							cs.LastError = &ChannelErrorStatus{Code: code, NetworkReason: meta.NetworkReason, At: attemptAt}
						})
						emitNotice(map[string]any{"serve": map[string]any{"state": "degraded", "context": ctxName, "channel_id": ch.ID, "code": code, "network_reason": meta.NetworkReason, "consecutive_failures": failures, "next_retry_at": nextAt}})
						if sleepContext(ctx, delay) != nil {
							return
						}
						continue
					}
					wasDegraded := failures > 0
					failures = 0
					tracker.updateChannel(idx, func(cs *ChannelStatus) {
						cs.State = "healthy"
						cs.LastAttemptAt = attemptAt
						cs.LastSuccessAt = lastSnapshot
						cs.ConsecutiveFailures = 0
						cs.NextRetryAt = 0
						cs.EventsObserved += int64(added)
					})
					if added > 0 && wakeCh != nil {
						select {
						case wakeCh <- struct{}{}:
						default:
						}
					}
					if wasDegraded {
						emitNotice(map[string]any{"serve": map[string]any{"state": "recovered", "context": ctxName, "channel_id": ch.ID, "last_snapshot_at": lastSnapshot}})
					}
					if sleepContext(ctx, intervalDur) != nil {
						return
					}
				}
			}(idx, b, ch, stInfo.Context, wakeCh)

			// Optional in-process durable webhook dispatcher consuming directly
			// from the in-process broker inbox (no Unix socket RPC overhead).
			if webhookEnabled {
				wg.Add(1)
				go func(idx int, b *broker.Broker, ctxName, channelID, dPath string, rst relay.State, rstore *relay.Store, wakeCh <-chan struct{}) {
					defer wg.Done()
					localScan := broker.ScanRequest{WholeChannel: true, Limit: 100, FromNow: rst.FromNow}
					fetchLocal := func(callCtx context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
						q.ResumePoll = false
						resp, callErr := b.Handle(callCtx, broker.Request{Operation: "event.scan", Scan: &q, DryRun: true}, false)
						if callErr != nil {
							return broker.ScanPage{}, callErr
						}
						return *resp.Scan, nil
					}
					saveAndTrack := func(next relay.State) error {
						if err := rstore.Save(next); err != nil {
							return err
						}
						tracker.updateChannel(idx, func(cs *ChannelStatus) {
							if cs.Webhook == nil {
								cs.Webhook = &ChannelWebhookStatus{}
							}
							cs.Webhook.State = "active"
							cs.Webhook.Mode = rcfg.Mode()
							cs.Webhook.OnUncertain = rcfg.UncertainPolicy()
							cs.Webhook.DeliveryState = dPath
							cs.Webhook.QueuedEvents = next.Depth()
							cs.Webhook.DeliveredBatches = next.AckedBatches + next.AcceptedBatches
							cs.Webhook.UncertainBatches = next.UncertainBatches
							lastOK := max(next.LastAck, next.LastAccepted)
							cs.Webhook.LastDeliveryAt = lastOK
							if next.LastDeliveryError != nil && next.LastDeliveryError.At >= lastOK {
								cs.Webhook.LastDeliveryError = next.LastDeliveryError.Code
							} else {
								cs.Webhook.LastDeliveryError = ""
							}
						})
						return nil
					}
					whNotice := func(reason string, next relay.State, code string) {
						emitNotice(map[string]any{"webhook": map[string]any{
							"state":             reason,
							"context":           ctxName,
							"channel_id":        channelID,
							"error_code":        code,
							"queued_events":     next.Depth(),
							"delivered_batches": next.AckedBatches + next.AcceptedBatches,
							"uncertain_batches": next.UncertainBatches,
						}})
					}
					if runErr := pumpRelayWithWake(ctx, rcfg, localScan, rst, time.Second, 0, wakeCh, fetchLocal, hook.Send, saveAndTrack, whNotice); runErr != nil && ctx.Err() == nil {
						code := e.Safe(runErr).Code
						tracker.updateChannel(idx, func(cs *ChannelStatus) {
							if cs.Webhook == nil {
								cs.Webhook = &ChannelWebhookStatus{}
							}
							cs.Webhook.State = "failed"
							cs.Webhook.DeliveryState = dPath
							cs.Webhook.LastDeliveryError = code
						})
						emitNotice(map[string]any{"webhook": map[string]any{"state": "failed", "context": ctxName, "channel_id": channelID, "error_code": code}})
					}
				}(idx, b, stInfo.Context, ch.ID, deliveryPath, rst, rstore, wakeCh)
			}
		}

		var serveErr error
		select {
		case serveErr = <-serverErr:
			cancel()
		case <-ctx.Done():
			serveErr = <-serverErr
		}
		wg.Wait()
		return serveErr
	}
	return cmd
}

func (s *appState) statusCommand() *cobra.Command {
	var dataDir string
	var check, strictFS bool
	var maxStale time.Duration

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Inspect live daemon liveness, per-channel polling health, and webhook delivery counters",
		Args:  cobra.NoArgs,
		Long: "Probes serve.lock for real process liveness and reads the atomic status.json\n" +
			"snapshot written by `wecom-channel-cli serve`. Use --check in host health\n" +
			"probes to exit non-zero when the daemon is stopped, blocked, or stale.",
		Example: "  wecom-channel-cli status\n  wecom-channel-cli status --check --max-stale 10m",
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Optional private data directory override")
	cmd.Flags().BoolVar(&check, "check", false, "Exit non-zero if the daemon is not running or any channel is blocked or stale")
	cmd.Flags().DurationVar(&maxStale, "max-stale", 10*time.Minute, "Staleness threshold used with --check")
	cmd.Flags().BoolVar(&strictFS, "strict-fs", false, "Enforce full ancestor directory checks up to /")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.profileDir(cmd)
		if err != nil {
			return err
		}
		if maxStale <= 0 {
			return e.New(e.CategoryUsage, "BAD_OPTIONS", "--max-stale must be greater than zero.")
		}
		resolvedData := dataDir
		if resolvedData == "" {
			if p, exists, err := config.LoadProfile(dir, strictFS); err == nil && exists {
				resolvedData = p.EffectiveDataDir(dir)
			} else {
				resolvedData = filepath.Join(dir, "data")
			}
		}
		if eval, err := filepath.EvalSymlinks(resolvedData); err == nil {
			resolvedData = eval
		}
		lockPath := filepath.Join(resolvedData, "serve.lock")
		running, _ := securefs.ProbeLock(lockPath)
		statusPath := filepath.Join(resolvedData, "status.json")
		raw, readErr := os.ReadFile(statusPath)
		var st ServiceStatus
		hasSnapshot := readErr == nil && json.Unmarshal(raw, &st) == nil && st.Version == 1

		now := time.Now()
		nowMs := now.UnixMilli()
		report := map[string]any{
			"running":  running,
			"data_dir": resolvedData,
		}
		healthy := running && hasSnapshot
		reason := "ok"
		if !running {
			reason = "stopped"
		} else if !hasSnapshot {
			if readErr == nil || !os.IsNotExist(readErr) {
				healthy = false
				reason = "status_unreadable"
			} else if lockInfo, statErr := os.Stat(lockPath); statErr != nil || now.Sub(lockInfo.ModTime()) > min(maxStale, time.Minute) {
				healthy = false
				reason = "status_overdue"
			} else {
				healthy = true
				reason = "starting"
			}
		}
		if hasSnapshot {
			report["pid"] = st.PID
			report["bot_id"] = st.Bot
			report["started_at"] = st.StartedAt
			report["updated_at"] = st.UpdatedAt
			if running && st.StartedAt > 0 {
				report["uptime_seconds"] = max(int64(0), (nowMs-st.StartedAt)/1000)
			}
			report["proxy_configured"] = st.ProxyConfigured
			report["webhook_enabled"] = st.WebhookEnabled
			report["channels"] = st.Channels
			if running {
				if st.UpdatedAt > 0 && nowMs-st.UpdatedAt > maxStale.Milliseconds() {
					healthy = false
					reason = "status_stale"
				}
				for _, ch := range st.Channels {
					if !healthy {
						break
					}
					if ch.State == "blocked" {
						healthy = false
						reason = "channel_blocked"
						break
					}
					if ch.Webhook != nil && ch.Webhook.State == "failed" {
						healthy = false
						reason = "webhook_failed"
						break
					}
					if ch.LastSuccessAt > 0 && nowMs-ch.LastSuccessAt > maxStale.Milliseconds() {
						healthy = false
						reason = "channel_stale"
						break
					}
					if ch.LastSuccessAt == 0 && st.StartedAt > 0 && nowMs-st.StartedAt > maxStale.Milliseconds() {
						healthy = false
						reason = "baseline_overdue"
						break
					}
				}
			}
		}
		report["healthy"] = healthy
		report["reason"] = reason
		if check && !healthy {
			return e.New(e.CategoryConflict, "SERVICE_UNHEALTHY", "Channel service check failed: "+reason).WithDetails(report)
		}
		return s.emit(cmd, report)
	}
	return cmd
}
