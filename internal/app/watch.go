package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

type watchCheckpoint struct {
	Version  int                  `json:"version"`
	Socket   string               `json:"socket"`
	Binding  string               `json:"binding"`
	Identity broker.WatchIdentity `json:"identity"`
	Scope    string               `json:"scope"`
	After    int64                `json:"after_sequence"`
}

func watchScope(r broker.ScanRequest) string {
	return apiclient.Digest(struct {
		Whole   bool
		Posts   []string
		Partial bool
	}{r.WholeChannel, r.Posts, r.AllowPartial})
}

func (s *appState) watchCommand() *cobra.Command {
	scan := broker.ScanRequest{Limit: 100}
	var duration, interval time.Duration
	var maxEvents int
	var checkpoint string
	var dry bool
	cmd := &cobra.Command{Use: "watch", Short: "Watch a whole channel or selected post threads as bounded NDJSON", Args: cobra.NoArgs,
		Long:    "Observe post/reply changes through polling or authenticated callbacks, selected\nby operator policy. No callback is required for polling. Choose --whole-channel\nor repeated --post IDs. Polling starts with a quiet snapshot baseline and may\ncoalesce changes; it cannot infer deletion from absence. --after-sequence or a\nsaved --checkpoint replays retained observations. Duration bounds the session;\n--timeout bounds each scan, including its remote pages. Polling reads content\nfor comparison but emits only untrusted metadata. It never runs an agent or sends.",
		Example: "  wecom-channel-cli --use-context engineering event watch --whole-channel --duration 10m\n  wecom-channel-cli --use-context engineering event watch --post POST_ID --checkpoint .watch/thread.json --duration 10m",
	}
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if duration <= 0 || duration > 24*time.Hour || interval < time.Second || interval > time.Minute || maxEvents < 1 || maxEvents > 100000 {
			return e.New(e.CategoryUsage, "WATCH_LIMITS", "Use a duration up to 24h, interval 1s–1m, and max-events 1–100000.")
		}
		if !dry && cmd.Flag("format").Changed && s.format != "ndjson" {
			return e.New(e.CategoryUsage, "WATCH_FORMAT", "Live watches use NDJSON; omit --format or use --format ndjson.")
		}
		var err error
		scan, err = broker.NormalizeScan(scan)
		if err != nil {
			return err
		}
		skillDriftNotice(cmd)
		r := broker.Request{Operation: "event.scan", DryRun: dry}
		socket, err := s.resolve(cmd, &r)
		if err != nil {
			return err
		}
		if err = broker.ValidateSocket(socket); err != nil {
			return err
		}
		if checkpoint != "" && !dry {
			if err = s.guard(); err != nil {
				return err
			}
		}
		file, err := openWatchCheckpoint(checkpoint, !dry)
		if err != nil {
			return err
		}
		defer file.close()
		cp, exists, err := file.read()
		if err != nil {
			return err
		}
		if exists {
			if cmd.Flags().Changed("after-sequence") {
				return e.New(e.CategoryUsage, "WATCH_RESUME_FLAGS", "Use the saved checkpoint or --after-sequence, not both.")
			}
			if cp.Socket != socket || cp.Scope != watchScope(scan) {
				return e.New(e.CategoryConflict, "WATCH_BINDING_CHANGED", "The checkpoint's socket or thread filter differs. Use the original watch scope or a new checkpoint file.")
			}
			scan.After = cp.After
			scan.Binding = cp.Binding
		} else {
			scan.FromNow = !cmd.Flags().Changed("after-sequence")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), duration)
		defer cancel()
		fetch := func(ctx context.Context, q broker.ScanRequest) (broker.ScanPage, error) {
			r.Scan = &q
			child, stop := context.WithTimeout(ctx, s.timeout)
			defer stop()
			response, err := broker.Call(child, socket, r)
			if err != nil {
				return broker.ScanPage{}, err
			}
			if response.Scan == nil {
				return broker.ScanPage{}, e.New(e.CategoryParse, "WATCH_PROTOCOL", "The broker did not return a watch batch.")
			}
			return *response.Scan, nil
		}
		scan.Limit = min(100, maxEvents)
		first, err := fetch(ctx, scan)
		if err != nil {
			return err
		}
		if !exists {
			cp = watchCheckpoint{Version: 1, Socket: socket, Binding: first.Binding, Identity: first.Identity, Scope: watchScope(scan), After: scan.After}
			if scan.FromNow {
				cp.After = first.After
			}
		}
		if err = validateBatch(first, cp, scan.Limit); err != nil {
			return err
		}
		if first.Coverage.Backend == "poll" {
			interval = max(interval, time.Duration(first.Coverage.PollIntervalSeconds)*time.Second)
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "identity": first.Identity, "coverage": first.Coverage, "whole_channel": scan.WholeChannel, "posts": scan.Posts, "start_after_sequence": cp.After, "duration": duration.String(), "interval": interval.String(), "max_events": maxEvents, "checkpoint": checkpoint})
		}
		if err = file.write(cp); err != nil {
			return checkpointError()
		}
		notice := func(state string, cp watchCheckpoint, count int) {
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"watch": map[string]any{"state": state, "emitted": count, "resume": cp}}})
		}
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"watch": map[string]any{"state": "started", "identity": first.Identity, "coverage": first.Coverage, "duration": duration.String(), "max_events": maxEvents, "resume": cp}}})
		emit := func(record apiclient.Record) error {
			return output.Emit(record, output.Options{Format: "ndjson", Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
		}
		return pumpWatch(ctx, scan, cp, first, interval, maxEvents, fetch, emit, file.write, notice)
	}
	f := cmd.Flags()
	f.BoolVar(&scan.WholeChannel, "whole-channel", false, "Watch post/reply changes throughout this channel context")
	f.StringArrayVar(&scan.Posts, "post", nil, "Watch this post and its replies; repeat for up to 32 post IDs")
	f.Int64Var(&scan.After, "after-sequence", 0, "Replay after this sequence instead of starting at the current head")
	f.BoolVar(&scan.AllowPartial, "allow-partial", false, "Explicitly accept an inbox restricted to allowlisted human authors")
	f.DurationVar(&duration, "duration", 5*time.Minute, "Maximum total watch duration, up to 24h")
	f.DurationVar(&interval, "interval", 2*time.Second, "Minimum delay between scans, 1s–1m; broker polling policy may increase it")
	f.IntVar(&maxEvents, "max-events", 1000, "Stop after this many matching emitted events")
	f.StringVar(&checkpoint, "checkpoint", "", "Optional private resume file in an existing agent-owned directory")
	f.BoolVar(&dry, "dry-run", false, "Inspect scope and coverage without streaming or saving a checkpoint")
	return cmd
}

type watchFetch func(context.Context, broker.ScanRequest) (broker.ScanPage, error)

func pumpWatch(ctx context.Context, scan broker.ScanRequest, cp watchCheckpoint, batch broker.ScanPage, interval time.Duration, maxEvents int, fetch watchFetch, emit func(apiclient.Record) error, save func(watchCheckpoint) error, notice func(string, watchCheckpoint, int)) error {
	count := 0
	persist := func() error {
		if err := save(cp); err != nil {
			return e.New(e.CategoryConfig, "WATCH_CHECKPOINT_WRITE", "Output may already have been delivered, but the checkpoint could not be saved. Resume may replay events.").WithDetails(map[string]any{"resume": cp})
		}
		return nil
	}
	stop := func(reason string) error {
		if err := persist(); err != nil {
			return err
		}
		notice(reason, cp, count)
		return nil
	}
	for {
		if err := validateBatch(batch, cp, scan.Limit); err != nil {
			return err
		}
		previous := cp.After
		for _, record := range batch.Items {
			if ctx.Err() != nil {
				return stop("stopped")
			}
			if err := emit(record); err != nil {
				saveErr := persist()
				if saveErr != nil {
					return saveErr
				}
				return e.New(e.CategoryInternal, "WATCH_OUTPUT", "The output stream failed. The failed event was not checkpointed.").WithDetails(map[string]any{"resume": cp})
			}
			ev, _ := watchEvent(record)
			cp.After = ev.Sequence
			count++
			if count >= maxEvents {
				return stop("max_events")
			}
		}
		cp.After = batch.After
		if err := persist(); err != nil {
			return err
		}
		if cp.After != previous {
			notice("checkpoint", cp, count)
		}
		select {
		case <-ctx.Done():
			return stop("stopped")
		case <-time.After(interval):
		}
		scan.FromNow = false
		scan.After = cp.After
		scan.Binding = cp.Binding
		scan.Limit = min(100, maxEvents-count)
		var err error
		batch, err = fetch(ctx, scan)
		if err != nil {
			if ctx.Err() != nil {
				return stop("stopped")
			}
			notice("failed", cp, count)
			return err
		}
	}
}

func validateBatch(p broker.ScanPage, cp watchCheckpoint, limit int) error {
	bad := func() error {
		return e.New(e.CategoryParse, "WATCH_PROTOCOL", "The broker returned inconsistent watch identity, sequence, or trust metadata.")
	}
	if len(p.Binding) != 64 || p.Binding != cp.Binding || p.Identity != cp.Identity || len(p.Identity.Ledger) != 32 || !config.ValidContextName(p.Identity.Context) || !apiclient.Identifier(p.Identity.Bot, 256) || !apiclient.Identifier(p.Identity.Channel, 128) || p.After < cp.After || p.After > p.HighWatermark || p.HasMore != (p.After < p.HighWatermark) || len(p.Items) > limit {
		return bad()
	}
	prev := cp.After
	for _, r := range p.Items {
		v, err := watchEvent(r)
		if err != nil || v.Sequence <= prev || v.Sequence > p.After || v.Bot != cp.Identity.Bot || v.Channel != cp.Identity.Channel || !v.ObservationOnly || (v.Kind != "topic_event" && v.Kind != "topic_reply_event") || r.Trust != "untrusted" || r.Warning != apiclient.Warning || r.Source.Context != cp.Identity.Context || r.Source.Channel != cp.Identity.Channel {
			return bad()
		}
		prev = v.Sequence
	}
	return nil
}
func watchEvent(r apiclient.Record) (callback.Event, error) {
	b, err := json.Marshal(r.Data)
	if err != nil {
		return callback.Event{}, err
	}
	var v callback.Event
	err = json.Unmarshal(b, &v)
	return v, err
}
