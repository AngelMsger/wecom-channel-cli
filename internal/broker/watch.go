package broker

import (
	"context"
	"slices"
	"sort"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type ScanRequest struct {
	Preview      bool     `json:"-"`
	WholeChannel bool     `json:"whole_channel"`
	Posts        []string `json:"posts,omitempty"`
	AllowPartial bool     `json:"allow_partial,omitempty"`
	After        int64    `json:"after_sequence"`
	FromNow      bool     `json:"from_now,omitempty"`
	Binding      string   `json:"binding,omitempty"`
	Limit        int      `json:"limit"`
}
type WatchIdentity struct {
	Context string `json:"context"`
	Bot     string `json:"bot_id"`
	Channel string `json:"channel_id"`
	Ledger  string `json:"ledger_id"`
}
type WatchCoverage struct {
	Backend             string   `json:"backend"`
	PollIntervalSeconds int      `json:"poll_interval_seconds,omitempty"`
	PollMaxRequests     int      `json:"poll_max_requests,omitempty"`
	PollMaxRecords      int      `json:"poll_max_records,omitempty"`
	Actors              string   `json:"actors"`
	Kinds               []string `json:"event_kinds"`
	RetentionHours      int      `json:"retention_hours"`
	UpstreamVerified    bool     `json:"upstream_delivery_verified"`
	Note                string   `json:"note"`
}
type ScanPage struct {
	Items         []apiclient.Record `json:"items"`
	After         int64              `json:"after_sequence"`
	HighWatermark int64              `json:"high_watermark"`
	HasMore       bool               `json:"has_more"`
	Binding       string             `json:"binding"`
	Identity      WatchIdentity      `json:"identity"`
	Coverage      WatchCoverage      `json:"coverage"`
}

func NormalizeScan(r ScanRequest) (ScanRequest, error) {
	if r.WholeChannel == (len(r.Posts) > 0) || len(r.Posts) > 32 || r.After < 0 || r.Limit < 1 || r.Limit > 100 || (r.FromNow && r.After != 0) {
		return r, e.New(e.CategoryUsage, "WATCH_SCOPE", "Choose --whole-channel or 1–32 --post IDs, with a non-negative sequence and a bounded page.")
	}
	r.Posts = append([]string{}, r.Posts...)
	sort.Strings(r.Posts)
	r.Posts = slices.Compact(r.Posts)
	for _, id := range r.Posts {
		if !apiclient.Identifier(id, 128) {
			return r, e.New(e.CategoryUsage, "WATCH_POST", "Use post IDs discovered by post list, search result or an authenticated event.")
		}
	}
	return r, nil
}

func (b *Broker) scan(ctx context.Context, raw ScanRequest) (ScanPage, error) {
	r, err := NormalizeScan(raw)
	if err != nil {
		return ScanPage{}, err
	}
	if len(b.Config.Channels) != 1 {
		return ScanPage{}, denied()
	}
	ch := b.Config.Channels[0]
	backend := b.Config.EffectiveWatchBackend()
	if backend == "callback" && (b.Config.CallbackListen == "" || b.Verifier == nil) {
		return ScanPage{}, e.New(e.CategoryConfig, "WATCH_INGRESS_DISABLED", "Configure authenticated callback intake before watching live channel changes.")
	}
	if !ch.ReadAll {
		return ScanPage{}, e.New(e.CategoryPermission, "WATCH_READ_SCOPE", "Watching changes requires read_all in the operator's channel policy.")
	}
	if backend == "poll" && !ch.ObserveAll {
		return ScanPage{}, e.New(e.CategoryPermission, "POLL_OBSERVATION_SCOPE", "Polling requires read_all and observe_all; snapshots do not identify the actor who edited or recalled content. --allow-partial cannot bypass this requirement.")
	}
	for _, kind := range []string{"topic_event", "topic_reply_event"} {
		if !slices.Contains(b.Config.Events, kind) {
			return ScanPage{}, e.New(e.CategoryConfig, "WATCH_EVENTS_DISABLED", "Enable topic_event and topic_reply_event in operator policy before watching.")
		}
	}
	if !ch.ObserveAll && !r.AllowPartial {
		return ScanPage{}, e.New(e.CategoryPermission, "WATCH_COVERAGE", "This inbox admits only allowlisted humans. Enable observe_all in operator policy, or explicitly accept --allow-partial coverage.")
	}
	ledger, high, events := b.Store.Inbox()
	identity := WatchIdentity{b.Config.Name, b.Config.Bot, ch.ID, ledger}
	binding := apiclient.Digest(struct {
		Identity WatchIdentity
		Policy   string
		Whole    bool
		Posts    []string
		Partial  bool
	}{identity, apiclient.Digest(b.Config), r.WholeChannel, r.Posts, r.AllowPartial})
	if r.Binding != "" && r.Binding != binding {
		return ScanPage{}, e.New(e.CategoryConflict, "WATCH_BINDING_CHANGED", "The checkpoint belongs to another context, ledger, policy or thread filter. Review the change before starting a new watch.")
	}
	if r.After > high {
		return ScanPage{}, e.New(e.CategoryConflict, "SEQUENCE_AHEAD", "The checkpoint is ahead of this inbox.")
	}
	if backend == "poll" && !r.Preview && (r.FromNow || r.After == high) {
		if err := b.poll(ctx, r); err != nil {
			return ScanPage{}, err
		}
		_, high, events = b.Store.Inbox()
	}
	actors := "all_users_and_bots"
	if !ch.ObserveAll {
		actors = "allowlisted_humans"
	}
	coverage := WatchCoverage{Backend: backend, Actors: actors, Kinds: []string{"topic_event", "topic_reply_event"}, RetentionHours: b.Config.RetentionHours, Note: "Configured callback coverage, not proof of complete WeCom delivery. Events are observation-only; content requires an explicit read."}
	if backend == "poll" {
		coverage.PollIntervalSeconds, coverage.PollMaxRequests, coverage.PollMaxRecords = b.Config.PollIntervalSeconds, b.Config.PollMaxRequests, b.Config.PollMaxRecords
		coverage.Note = "Snapshot observations, not a complete event history: first snapshot is a baseline; edits may coalesce; absence is not deletion; recall requires a returned flag; change actors are unknown. Content is read for comparison but only fingerprints and event metadata persist. Polling runs only while a watch requests scans."
	}
	p := ScanPage{Items: []apiclient.Record{}, After: r.After, HighWatermark: high, Binding: binding, Identity: identity, Coverage: coverage}
	if r.FromNow {
		p.After = high
		return p, nil
	}
	if r.After > high {
		return p, e.New(e.CategoryConflict, "SEQUENCE_AHEAD", "The checkpoint is ahead of this inbox.")
	}
	expected := r.After + 1
	scanned := 0
	for _, v := range events {
		if v.Sequence <= r.After {
			continue
		}
		if v.Sequence != expected || !b.visibleEvent(v) {
			return p, watchGap()
		}
		expected++
		scanned++
		p.After = v.Sequence
		if (v.Kind == "topic_event" || v.Kind == "topic_reply_event") && (r.WholeChannel || slices.Contains(r.Posts, v.Post)) {
			v.ObservationOnly = true
			if v.Transport == "" {
				v.Transport = "callback"
			}
			record := apiclient.Untrusted(v, v.Channel, v.Post, v.ID)
			record.Source.Context = b.Config.Name
			p.Items = append(p.Items, record)
		}
		if scanned >= 100 || len(p.Items) >= r.Limit {
			break
		}
	}
	if p.After < high && scanned < 100 && len(p.Items) < r.Limit {
		return p, watchGap()
	}
	p.HasMore = p.After < high
	return p, nil
}
func watchGap() error {
	return e.New(e.CategoryConflict, "EVENT_GAP", "Watch history is missing, expired or no longer visible under current policy. The checkpoint has not advanced past the gap.")
}
