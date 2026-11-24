package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

// PollProgress contains only aggregate information safe to return over the agent socket.
type PollProgress struct {
	NextAt   int64 `json:"next_at"`
	Requests int   `json:"requests"`
	Records  int   `json:"records"`
	PageAt   int64 `json:"last_page_at,omitempty"`
}

func (p PollProgress) Valid() bool {
	return p.NextAt > 0 && p.Requests >= 0 && p.Requests <= 1000 && p.Records >= 0 && p.Records <= 100000 && p.PageAt >= 0
}

// There is one whole-channel scope per broker. Only the broker owns its cursors.
type pollPass struct {
	Policy, Scope, Ledger string
	Revision              uint64
	Started               int64
	Requests              int
	Window                rateWindow
	EligibleAt            int64
	PageAt                int64
	Cursor                string
	Seen                  map[string]bool
	Replies               bool
	Remaining             []string
	Observations          map[string]state.PollObservation
}

func (p *pollPass) valid(ledger string) bool {
	if p.Ledger != ledger || len(p.Policy) != 64 || len(p.Scope) != 64 || p.Started <= 0 || p.Requests < 1 || p.Requests > 1000 || p.Window.Start.IsZero() || p.Window.Count < 1 || p.Observations == nil || p.Seen == nil || (p.Replies && len(p.Remaining) == 0) || (!p.Replies && len(p.Remaining) != 0) {
		return false
	}
	if p.Cursor != "" && (!apiclient.Identifier(p.Cursor, 256) || !p.Seen[p.Cursor]) {
		return false
	}
	for _, post := range p.Remaining {
		if !apiclient.Identifier(post, 128) {
			return false
		}
	}
	return true
}

const maxPassBytes = 8 << 20

func (b *Broker) savePass(p *pollPass) error {
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > maxPassBytes {
		return e.New(e.CategoryRateLimit, "POLL_STAGE_CAPACITY", "The staged scan exceeds its 8 MiB storage limit.")
	}
	return b.Store.SavePollStage(raw)
}

func (b *Broker) discardPass(err error) error {
	if saveErr := b.Store.SavePollStage(nil); saveErr != nil {
		return saveErr
	}
	return err
}

func (p *pollPass) apply(b *Broker, q apiclient.Query, page apiclient.Page) error {
	if len(page.Items) > q.Limit || (page.HasMore && (!apiclient.Identifier(page.Next, 256) || p.Seen[page.Next])) {
		return pollResponse()
	}
	for _, record := range page.Items {
		if _, err := b.decodePoll(p.Observations, b.Now(), record, q.Post, p.Replies); err != nil {
			return err
		}
	}
	p.PageAt = b.Now().UnixMilli()
	if page.HasMore {
		p.Cursor = page.Next
		p.Seen[page.Next] = true
		return nil
	}
	p.Cursor = ""
	p.Seen = map[string]bool{}
	if p.Replies {
		p.Remaining = p.Remaining[1:]
	} else {
		for _, o := range p.Observations {
			if o.Event.Change != 3 {
				p.Remaining = append(p.Remaining, o.Event.Post)
			}
		}
		slices.Sort(p.Remaining)
		p.Replies = true
	}
	return nil
}

func (b *Broker) pollSlice(ctx context.Context, policy, scope string) (result *PollProgress, resultErr error) {
	now := b.Now()
	raw, revision, ledger := b.Store.StagedPoll()
	p := pollPass{Policy: policy, Scope: scope, Ledger: ledger, Revision: revision, Started: now.UnixMilli(), Seen: map[string]bool{}, Observations: map[string]state.PollObservation{}}
	// Include bounded utilization in terminal capacity diagnostics, including after restart.
	defer func() {
		if resultErr == nil {
			return
		}
		x := e.Safe(resultErr)
		switch x.Code {
		case "POLL_BUDGET", "POLL_CAPACITY", "POLL_STAGE_CAPACITY":
			resultErr = x.WithDetails(apiclient.FailureMetadata{RequestsUsed: p.Requests, RequestLimit: b.Config.PollMaxRequests, RecordsSeen: len(p.Observations), RecordLimit: b.Config.PollMaxRecords})
		}
	}()
	if len(raw) > 0 {
		p = pollPass{}
		if len(raw) > maxPassBytes || safejson.Decode(raw, &p, true) != nil || !p.valid(ledger) {
			return nil, e.New(e.CategoryConfig, "POLL_STAGE_INVALID", "The saved polling pass is unreadable. Inspect broker storage.")
		}
		if p.Policy != policy || p.Scope != scope || p.Revision != revision || now.Before(time.UnixMilli(p.Started)) || now.Sub(time.UnixMilli(p.Started)) >= time.Hour {
			return nil, b.discardPass(e.New(e.CategoryConflict, "POLL_UNSTABLE", "The saved pass expired or a concurrent scan changed the baseline."))
		}
	}
	progress := func(at time.Time) *PollProgress {
		return &PollProgress{NextAt: at.UnixMilli(), Requests: p.Requests, Records: len(p.Observations), PageAt: p.PageAt}
	}
	if now.UnixMilli() < p.EligibleAt {
		return progress(time.UnixMilli(p.EligibleAt)), nil
	}
	for i := 0; i < 8; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.Requests >= b.Config.PollMaxRequests {
			return nil, e.New(e.CategoryRateLimit, "POLL_BUDGET", fmt.Sprintf("Used %d of %d scan requests with pages remaining and %d observed records. Review coverage or capacity.", p.Requests, b.Config.PollMaxRequests, len(p.Observations)))
		}
		if deadline, ok := ctx.Deadline(); i > 0 && ok && time.Until(deadline) < 5*time.Second {
			return progress(b.Now().Add(time.Second)), nil
		}
		w, allowed := b.reserveRate("poll-upstream", p.Window)
		if !allowed {
			return progress(w.Start.Add(time.Minute)), nil
		}
		p.Window = w
		p.Requests++
		// A crash after reservation can waste one credit but cannot issue an uncharged read.
		if err := b.savePass(&p); err != nil {
			return nil, err
		}
		q := apiclient.Query{Channel: b.Config.Channels[0].ID, Operation: "post.list", Limit: 50, From: 1, To: time.UnixMilli(p.Started).Unix() + 1, Cursor: p.Cursor}
		if p.Replies {
			q.Operation, q.Post = "reply.list", p.Remaining[0]
		}
		page, err := b.API.Read(ctx, q)
		if err != nil {
			x := e.Safe(err)
			if x.Category == e.CategoryParse || (q.Cursor != "" && x.Code == "WECOM_REJECTED" && x.Category != e.CategoryRateLimit) {
				return nil, b.discardPass(err)
			}
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if err = p.apply(b, q, page); err != nil {
			return nil, b.discardPass(err)
		}
		if p.Replies && len(p.Remaining) == 0 {
			keys := slices.Sorted(maps.Keys(p.Observations))
			observations := make([]state.PollObservation, 0, len(keys))
			for _, k := range keys {
				observations = append(observations, p.Observations[k])
			}
			return nil, b.Store.CommitPoll(policy, scope, true, nil, observations, b.Now(), time.Duration(b.Config.RetentionHours)*time.Hour, b.Config.MaxEvents, b.Config.PollMaxRecords)
		}
		if i == 7 {
			p.EligibleAt = b.Now().Add(max(time.Second, 8*time.Minute/time.Duration(b.Config.RequestsPerMinute))).UnixMilli()
		}
		if err = b.savePass(&p); err != nil {
			return nil, err
		}
	}
	return progress(time.UnixMilli(p.EligibleAt)), nil
}
