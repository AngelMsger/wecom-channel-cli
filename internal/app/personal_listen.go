package app

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

type personalNotice func(string, map[string]any)

func listenPersonalChannels(ctx context.Context, cmd *cobra.Command, dir, bot string, channels []personalChannel, api apiclient.Client, timeout time.Duration) error {
	var writers sync.Mutex
	emit := func(record apiclient.Record) error {
		writers.Lock()
		defer writers.Unlock()
		return output.Emit(record, output.Options{Format: "ndjson", Writer: cmd.OutOrStdout()})
	}
	results := make(chan error, len(channels))
	for _, ch := range channels {
		go func() {
			notice := func(status string, fields map[string]any) {
				if fields == nil {
					fields = map[string]any{}
				}
				fields["state"], fields["channel_id"] = status, ch.ID
				writers.Lock()
				defer writers.Unlock()
				output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"listener": fields}})
			}
			err := listenPersonalChannel(ctx, dir, bot, ch.ID, api, timeout, emit, notice)
			if err != nil {
				notice("blocked", map[string]any{"code": e.Safe(err).Code})
			}
			results <- err
		}()
	}
	var firstErr error
	for range channels {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func listenPersonalChannel(ctx context.Context, dir, bot, channel string, api apiclient.Client, timeout time.Duration, emit func(apiclient.Record) error, notice personalNotice) error {
	name := "ch-" + apiclient.Digest([]string{bot, channel})[:24]
	cfg := config.Default()
	cfg.Bot, cfg.Name, cfg.StateDir = bot, name, filepath.Join(dir, name)
	cfg.WatchBackend = "poll"
	cfg.Events = []string{"topic_event", "topic_reply_event"}
	cfg.Channels = []config.Channel{{ID: channel, ReadAll: true, ObserveAll: true}}
	// Allow a larger whole-channel pass while retaining the broker's pacing,
	// record, pass lifetime and response-size limits.
	cfg.PollMaxRequests = 256
	if err := ensurePersonalDir(cfg.StateDir); err != nil {
		return err
	}
	st, err := state.Open(cfg.StateDir, bot, channel)
	if err != nil {
		return err
	}
	defer st.Close()
	b := broker.New(cfg, st, api, nil)
	file, err := openWatchCheckpoint(filepath.Join(cfg.StateDir, "output.json"), true)
	if err != nil {
		return err
	}
	defer file.close()
	scan := broker.ScanRequest{WholeChannel: true, ResumePoll: true, Limit: 100}
	preview, err := b.Handle(ctx, broker.Request{Operation: "event.scan", Scan: &scan, DryRun: true}, false)
	if err != nil {
		return err
	}
	cp, exists, err := file.read()
	if err != nil {
		return err
	}
	if !exists {
		cp = watchCheckpoint{Version: 1, Socket: name, Scope: watchScope(scan), Identity: preview.Scan.Identity, Binding: preview.Scan.Binding}
		if err = file.write(cp); err != nil {
			return err
		}
	} else if cp.Socket != name || cp.Scope != watchScope(scan) || cp.Binding != preview.Scan.Binding || cp.Identity != preview.Scan.Identity {
		return e.New(e.CategoryConflict, "WATCH_BINDING_CHANGED", "The personal channel ledger and output checkpoint no longer match. Preserve both for reconciliation.")
	}
	scan.After, scan.Binding = cp.After, cp.Binding
	interval := time.Duration(cfg.PollIntervalSeconds) * time.Second
	fetch := func(ctx context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
		return completePersonalScan(ctx, q, timeout, interval, func(child context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
			r, err := b.Handle(child, broker.Request{Operation: "event.scan", Scan: &q}, false)
			if err != nil {
				return broker.ScanPage{}, err
			}
			return *r.Scan, nil
		}, waitPersonal, notice)
	}
	notice("starting", nil)
	first, err := fetch(ctx, scan)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	notice("ready", map[string]any{"last_snapshot_at": first.LastSnapshot})
	return pumpWatch(ctx, scan, cp, first, interval, math.MaxInt, fetch, emit, file.write, func(status string, _ watchCheckpoint, count int) { notice(status, map[string]any{"emitted": count}) })
}

func waitPersonal(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(max(time.Millisecond, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// One logical scan can span rate windows and transient read failures. Keep
// the same binding and sequence until the broker commits a complete snapshot.
func completePersonalScan(ctx context.Context, q broker.ScanRequest, timeout, interval time.Duration, fetch watchFetch, wait func(context.Context, time.Duration) error, notice personalNotice) (broker.ScanPage, error) {
	const attempts = 20
	cfg := relay.DefaultConfig()
	cfg.RetryLimit = attempts
	var recovery relay.State
	for {
		if err := ctx.Err(); err != nil {
			return broker.ScanPage{}, err
		}
		child, cancel := context.WithTimeout(ctx, timeout)
		p, err := fetch(child, q)
		expired := child.Err() == context.DeadlineExceeded
		cancel()
		if ctx.Err() != nil {
			return broker.ScanPage{}, ctx.Err()
		}
		var delay time.Duration
		switch {
		case err != nil:
			if expired && (err == context.DeadlineExceeded || err == context.Canceled) {
				err = e.New(e.CategoryNetwork, "REQUEST_UNCERTAIN", "The read deadline expired.")
			}
			if !recovery.RecordScanFailure(err, cfg, interval, time.Now()) || recovery.ScanFailures > attempts {
				return broker.ScanPage{}, err
			}
			delay = time.Until(time.UnixMilli(recovery.NextScan))
			notice("retry_wait", map[string]any{"code": recovery.LastScanError.Code, "attempt": recovery.ScanFailures, "next_scan_at": recovery.NextScan})
		case p.Progress != nil:
			delay = time.Until(time.UnixMilli(p.Progress.NextAt))
		default:
			if recovery.ScanFailures > 0 {
				notice("recovered", map[string]any{"last_snapshot_at": p.LastSnapshot})
			}
			return p, nil
		}
		if err = wait(ctx, delay); err != nil {
			return broker.ScanPage{}, err
		}
	}
}
