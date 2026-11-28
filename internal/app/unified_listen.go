package app

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
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
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
	"github.com/spf13/cobra"
)

type outputCursor struct {
	Version int    `json:"version"`
	Bot     string `json:"bot_id"`
	Channel string `json:"channel_id"`
	Ledger  string `json:"ledger_id"`
	After   int64  `json:"after_sequence"`
}

func readOutputCursor(path string, strict bool) (outputCursor, bool, error) {
	var c outputCursor
	data, err := securefs.ReadPrivateFile(path, 16384, false, strict)
	if os.IsNotExist(err) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	if safejson.Decode(data, &c, true) != nil || c.Version != 1 || c.After < 0 || !apiclient.Identifier(c.Bot, 256) || !apiclient.Identifier(c.Channel, 128) || len(c.Ledger) != 32 {
		return c, false, e.New(e.CategoryConfig, "WATCH_CHECKPOINT", "The saved channel output checkpoint is unreadable.")
	}
	return c, true, nil
}

func writeOutputCursor(path string, c outputCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return securefs.WriteAtomic(path, b)
}

// isRecoverablePollError classifies transient cross-region / upstream failures
// that should enter steady-state exponential backoff rather than stopping the
// channel listener permanently.
func isRecoverablePollError(err error) bool {
	if err == nil {
		return false
	}
	if relay.ScanRetryable(err) {
		return true
	}
	x := e.Safe(err)
	// Also recover from occasional garbled proxy/WAF non-JSON responses during
	// cross-border network blips without killing the whole listener permanently.
	return x.Code == "BAD_RESPONSE" && x.Category == e.CategoryParse
}

// steadyPollBackoff computes jittered exponential backoff capped at 5 minutes,
// always honoring both the configured poll interval and upstream Retry-After.
func steadyPollBackoff(failures int, interval time.Duration, err error) time.Duration {
	shift := min(max(failures-1, 0), 8)
	base := min(10*time.Second*time.Duration(1<<shift), 5*time.Minute)
	jittered := base/2 + time.Duration(rand.Int64N(int64(base-base/2)+1))
	meta := apiclient.ReadFailureMetadata(e.Safe(err).Details)
	return max(interval, jittered, time.Duration(meta.RetryAfterSeconds)*time.Second)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(max(time.Millisecond, d))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func profileMissingError() error {
	return e.New(e.CategoryConfig, "PROFILE_MISSING", "Connect a bot first.").WithNextSteps("wecom-channel-cli connect --bot-id BOT_ID")
}

func buildChannelBrokerConfig(p config.Profile, ch config.ProfileChannel, ctxName, stateDir, agentSock, adminSock string) config.Config {
	p = p.WithDefaults()
	cfg := config.Default()
	cfg.Name = ctxName
	cfg.Bot = p.Bot
	cfg.StateDir = stateDir
	cfg.AgentSocket = agentSock
	cfg.AdminSocket = adminSock
	cfg.ReadOnly = true
	if ch.ReadOnly != nil {
		cfg.ReadOnly = *ch.ReadOnly
	}
	cfg.WatchBackend = "poll"
	cfg.Events = []string{"topic_event", "topic_reply_event"}
	cfg.PollIntervalSeconds = p.Polling.IntervalSeconds
	cfg.PollMaxRequests = p.Polling.MaxRequests
	cfg.PollMaxRecords = p.Polling.MaxRecords
	cfg.RequestsPerMinute = p.Polling.RequestsPerMinute
	cfg.PollConcurrency = p.Polling.Concurrency
	cfg.UpstreamProxy = p.Network.Proxy
	cfg.UpstreamTimeoutSeconds = p.Network.TimeoutSeconds
	cfg.Channels = []config.Channel{{
		ID:             ch.ID,
		ReadAll:        true,
		ObserveAll:     true,
		AllowPosts:     !cfg.ReadOnly,
		AllowReplies:   !cfg.ReadOnly,
		AllowReactions: !cfg.ReadOnly,
		ReactionScope:  map[bool]string{true: "channel", false: "mentioned"}[!cfg.ReadOnly],
	}}
	return cfg
}

func (s *appState) listenCommand(runtime profileRuntime) *cobra.Command {
	var selection channelSelection
	var duration, interval time.Duration
	var proxy, dataDir string
	var strictFS, dry bool

	cmd := &cobra.Command{
		Use:   "listen",
		Short: "Stream observations from selected channels as NDJSON with automatic recovery",
		Args:  cobra.NoArgs,
		Long: "Listen to one or more channels discovered by connect and emit untrusted NDJSON\n" +
			"observations on stdout. Uses concurrent in-process polling with automatic read\n" +
			"retries and unlimited steady-state exponential backoff on transient cross-region\n" +
			"network errors (never permanently stopping after 20 failures). Runs continuously\n" +
			"until interrupted unless --duration is set. For unattended background serving\n" +
			"with local CLI sockets and optional Grok webhook delivery, use `serve`.",
		Example: "  wecom-channel-cli listen --channel Engineering\n  wecom-channel-cli listen --all\n  wecom-channel-cli listen --duration 10m",
	}
	selection.bindFlags(cmd)
	cmd.Flags().DurationVar(&duration, "duration", 0, "Optional session deadline (0 runs until interrupted)")
	cmd.Flags().DurationVar(&interval, "interval", 0, "Optional poll interval override (5s–1h)")
	cmd.Flags().StringVar(&proxy, "proxy", "", "Optional explicit proxy URL override (http/https/socks5/socks5h)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Optional private data directory override")
	cmd.Flags().BoolVar(&strictFS, "strict-fs", false, "Enforce full ancestor directory checks up to /")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Show the effective channel selection without saving or contacting WeCom")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.profileOperatorDir(cmd)
		if err != nil {
			return err
		}
		if duration < 0 || (!dry && !selection.none && cmd.Flag("format").Changed && s.format != "ndjson") {
			return e.New(e.CategoryUsage, "LISTEN_OPTIONS", "Use a non-negative duration and NDJSON for live observations.")
		}
		if cmd.Flags().Changed("interval") && (interval < 5*time.Second || interval > time.Hour) {
			return e.New(e.CategoryUsage, "LISTEN_OPTIONS", "--interval must be between 5s and 1h.")
		}
		if cmd.Flags().Changed("proxy") {
			if _, err := transport.ValidateProxyURL(proxy); err != nil {
				return err
			}
		}
		if !dry {
			if err = s.guard("listen"); err != nil {
				return err
			}
		}
		var profileLock *os.File
		if !dry && selection.explicit() {
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
			p.Network.Proxy = proxy
		}
		if dataDir != "" {
			p.DataDir = dataDir
		}
		if selection.explicit() {
			if err = selection.apply(&p); err != nil {
				return err
			}
		}
		channels := p.SelectedChannels()
		if len(channels) > 32 {
			return e.New(e.CategoryUsage, "LISTEN_SCOPE_LIMIT", "Select at most 32 channels in one session.")
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "bot_id": p.Bot, "channels": profileChoices(p), "selected_count": len(channels), "poll_interval_seconds": p.Polling.IntervalSeconds, "proxy_configured": p.Network.Proxy != ""})
		}
		if len(channels) == 0 && !selection.none {
			return e.New(e.CategoryUsage, "NO_CHANNELS_SELECTED", "Run connect to discover channels, then listen --channel NAME or listen --all.")
		}
		if selection.explicit() {
			if err = selection.apply(&baseProfile); err != nil {
				return err
			}
			if err = config.SaveProfile(dir, baseProfile, strictFS); err != nil {
				return err
			}
		}
		releaseProfileLock()
		if selection.none {
			return s.emit(cmd, map[string]int{"selected_count": 0})
		}
		secret, _, err := loadProfileSecret(cmd, p.Bot, runtime.credentials(dir), false)
		if err != nil {
			return err
		}
		api, err := runtime.client(p.Bot, secret, p.TransportConfig())
		if err != nil {
			return err
		}
		resolvedData, err := securefs.EnsurePrivateDir(p.EffectiveDataDir(dir), strictFS)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		if duration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, duration)
			defer cancel()
		}
		return runUnifiedListen(ctx, cmd, p, channels, resolvedData, api, strictFS)
	}
	return cmd
}

func runUnifiedListen(ctx context.Context, cmd *cobra.Command, p config.Profile, channels []config.ProfileChannel, dataDir string, api apiclient.Client, strictFS bool) error {
	var writeMu sync.Mutex
	emitRecord := func(record apiclient.Record) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return output.Emit(record, output.Options{Format: "ndjson", Writer: cmd.OutOrStdout()})
	}
	emitNotice := func(channelID, ctxName, stateName string, fields map[string]any) {
		if fields == nil {
			fields = map[string]any{}
		}
		fields["state"] = stateName
		fields["channel_id"] = channelID
		fields["context"] = ctxName
		writeMu.Lock()
		defer writeMu.Unlock()
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"listener": fields}})
	}

	usedCtx := map[string]bool{}
	results := make(chan error, len(channels))
	for _, ch := range channels {
		ctxName := config.ChannelContextName(p.Bot, ch, usedCtx)
		go func(ch config.ProfileChannel, ctxName string) {
			err := listenSingleChannel(ctx, p, ch, ctxName, dataDir, api, strictFS, emitRecord, func(st string, f map[string]any) {
				emitNotice(ch.ID, ctxName, st, f)
			})
			if err != nil && ctx.Err() == nil {
				emitNotice(ch.ID, ctxName, "blocked", map[string]any{"code": e.Safe(err).Code})
			}
			results <- err
		}(ch, ctxName)
	}
	var firstErr error
	for range channels {
		if err := <-results; err != nil && firstErr == nil && ctx.Err() == nil {
			firstErr = err
		}
	}
	return firstErr
}

func listenSingleChannel(ctx context.Context, p config.Profile, ch config.ProfileChannel, ctxName, dataDir string, api apiclient.Client, strictFS bool, emit func(apiclient.Record) error, notice func(string, map[string]any)) error {
	slug := config.ChannelStateSlug(p.Bot, ch.ID)
	chDir := filepath.Join(dataDir, "channels", slug)
	var st *state.Store
	var err error
	if strictFS {
		st, err = state.Open(chDir, p.Bot, ch.ID)
	} else {
		st, err = state.OpenRelaxed(chDir, p.Bot, ch.ID)
	}
	if err != nil {
		return err
	}
	defer st.Close()

	cfg := buildChannelBrokerConfig(p, ch, ctxName, chDir, "", "")
	b := broker.New(cfg, st, api, nil)
	cursorPath := filepath.Join(chDir, "output.json")
	ledgerID, head, _ := st.Inbox()
	cur, exists, err := readOutputCursor(cursorPath, strictFS)
	if err != nil {
		return err
	}
	if !exists {
		cur = outputCursor{Version: 1, Bot: p.Bot, Channel: ch.ID, Ledger: ledgerID, After: head}
		if err = writeOutputCursor(cursorPath, cur); err != nil {
			return err
		}
	} else if cur.Bot != p.Bot || cur.Channel != ch.ID || cur.Ledger != ledgerID {
		return e.New(e.CategoryConflict, "WATCH_BINDING_CHANGED", "The channel ledger and output checkpoint no longer match. Preserve both before starting a fresh listener.")
	}

	interval := time.Duration(p.Polling.IntervalSeconds) * time.Second
	emitted := 0
	failures := 0
	ready := false
	notice("starting", nil)

	drain := func() error {
		for {
			if ctx.Err() != nil {
				return nil
			}
			scan := broker.ScanRequest{WholeChannel: true, After: cur.After, Limit: 100}
			resp, scanErr := b.Handle(ctx, broker.Request{Operation: "event.scan", Scan: &scan, DryRun: true}, false)
			if scanErr != nil {
				return scanErr
			}
			p := resp.Scan
			previous := cur.After
			for _, rec := range p.Items {
				if err := emit(rec); err != nil {
					_ = writeOutputCursor(cursorPath, cur)
					return e.New(e.CategoryInternal, "WATCH_OUTPUT", "The output stream failed; the failed event was not checkpointed.")
				}
				if ev, evErr := watchEvent(rec); evErr == nil && ev.Sequence > cur.After {
					cur.After = ev.Sequence
				}
				emitted++
			}
			if p.After > cur.After {
				cur.After = p.After
			}
			if cur.After != previous {
				if err := writeOutputCursor(cursorPath, cur); err != nil {
					return err
				}
				notice("checkpoint", map[string]any{"emitted": emitted, "after_sequence": cur.After})
			}
			if !p.HasMore {
				return nil
			}
		}
	}

	for {
		if ctx.Err() != nil {
			notice("stopped", map[string]any{"emitted": emitted, "after_sequence": cur.After})
			return nil
		}
		_, lastSnapshot, pollErr := b.PollNow(ctx)
		if ctx.Err() != nil {
			notice("stopped", map[string]any{"emitted": emitted, "after_sequence": cur.After})
			return nil
		}
		if pollErr != nil {
			if !isRecoverablePollError(pollErr) {
				return pollErr
			}
			failures++
			delay := steadyPollBackoff(failures, interval, pollErr)
			nextAt := time.Now().Add(delay).UnixMilli()
			meta := apiclient.ReadFailureMetadata(e.Safe(pollErr).Details)
			fields := map[string]any{
				"code":                 e.Safe(pollErr).Code,
				"consecutive_failures": failures,
				"next_retry_at":        nextAt,
			}
			if meta.NetworkReason != "" {
				fields["network_reason"] = meta.NetworkReason
			}
			notice("degraded", fields)
			if err := sleepContext(ctx, delay); err != nil {
				notice("stopped", map[string]any{"emitted": emitted, "after_sequence": cur.After})
				return nil
			}
			continue
		}
		if failures > 0 {
			notice("recovered", map[string]any{"last_snapshot_at": lastSnapshot, "after_failures": failures})
			failures = 0
		}
		if !ready {
			ready = true
			notice("ready", map[string]any{"last_snapshot_at": lastSnapshot})
		}
		if err := drain(); err != nil {
			return err
		}
		if err := sleepContext(ctx, interval); err != nil {
			notice("stopped", map[string]any{"emitted": emitted, "after_sequence": cur.After})
			return nil
		}
	}
}
