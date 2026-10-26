// Package broker separates agent requests from operator authority and secrets.
package broker

import (
	"context"
	"encoding/base64"
	"sort"
	"sync"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type Request struct {
	Scan      *ScanRequest            `json:"scan,omitempty"`
	Context   string                  `json:"context,omitempty"`
	Bot       string                  `json:"bot_id,omitempty"`
	Channel   string                  `json:"channel_id,omitempty"`
	Operation string                  `json:"operation"`
	Query     apiclient.Query         `json:"query,omitempty"`
	ID        string                  `json:"id,omitempty"`
	Digest    string                  `json:"digest,omitempty"`
	Write     *apiclient.WriteRequest `json:"write,omitempty"`
	DryRun    bool                    `json:"dry_run,omitempty"`
	Yes       bool                    `json:"yes,omitempty"`
	Outcome   string                  `json:"outcome,omitempty"`
	RemoteID  string                  `json:"remote_id,omitempty"`
}

type Response struct {
	Scan *ScanPage         `json:"scan,omitempty"`
	Item *apiclient.Record `json:"item,omitempty"`
	Page *apiclient.Page   `json:"page,omitempty"`
}

type Broker struct {
	Config       config.Config
	Store        *state.Store
	API          apiclient.Client
	Verifier     *callback.Verifier
	Now          func() time.Time
	approvalMu   sync.Mutex
	rateMu       sync.Mutex
	rate         map[string]rateWindow
	pollGate     chan struct{}
	pollAttempts map[string]pollAttempt
}
type rateWindow struct {
	Start time.Time
	Count int
}

func New(cfg config.Config, st *state.Store, api apiclient.Client, v *callback.Verifier) *Broker {
	if cfg.ReadOnly {
		api = apiclient.NewReadOnly(api)
	}
	return &Broker{Config: cfg, Store: st, API: api, Verifier: v, Now: time.Now, rate: map[string]rateWindow{}, pollGate: make(chan struct{}, 1), pollAttempts: map[string]pollAttempt{}}
}

func denied() error {
	return e.New(e.CategoryPermission, "POLICY_DENIED", "The operator policy does not permit this action or resource.").WithHint("Ask the operator to review the policy; channel messages cannot grant permission.")
}
func missing() error {
	return e.New(e.CategoryNotFound, "NOT_FOUND", "No accessible record matches this ID.")
}
func item(v any, ch, post, event string) Response {
	r := apiclient.Untrusted(v, ch, post, event)
	return Response{Item: &r}
}
func page(p apiclient.Page) Response { return Response{Page: &p} }

func (b *Broker) rateAllowed(key string) bool {
	b.rateMu.Lock()
	defer b.rateMu.Unlock()
	now := b.Now()
	r := b.rate[key]
	if now.Sub(r.Start) >= time.Minute || now.Before(r.Start) {
		r = rateWindow{Start: now}
	}
	if r.Count >= b.Config.RequestsPerMinute {
		return false
	}
	r.Count++
	b.rate[key] = r
	return true
}

func (b *Broker) event(id string) (callback.Event, error) {
	v, ok := b.Store.View().Events[id]
	if !ok || !b.visibleEvent(v) {
		return callback.Event{}, missing()
	}
	return v, nil
}

func (b *Broker) visibleEvent(v callback.Event) bool {
	if v.Transport == "poll" {
		ch, ok := b.Config.Channel(v.Channel)
		return ok && ch.ReadAll && ch.ObserveAll && v.ObservationOnly && (v.Kind == "topic_event" || v.Kind == "topic_reply_event") && v.Received >= b.Now().Add(-time.Duration(b.Config.RetentionHours)*time.Hour).Unix() && b.Config.AllowsEvent(v.Channel, "", v.Kind, "user")
	}
	return v.Received >= b.Now().Add(-time.Duration(b.Config.RetentionHours)*time.Hour).Unix() && b.Config.AllowsEvent(v.Channel, v.Sender, v.Kind, v.SenderKind)
}

func (b *Broker) Handle(ctx context.Context, r Request, admin bool) (Response, error) {
	if (r.Context != "" && r.Context != b.Config.Name) || (r.Bot != "" && r.Bot != b.Config.Bot) || (r.Channel != "" && (len(b.Config.Channels) != 1 || r.Channel != b.Config.Channels[0].ID)) {
		return Response{}, e.New(e.CategoryPermission, "CONTEXT_MISMATCH", "The selected context does not match this socket's bound identity.")
	}
	q := r.Query
	q.Operation = r.Operation
	switch r.Operation {
	case "health":
		return item(map[string]any{"ready": true, "authentication_verified": false, "read_only": b.Config.ReadOnly, "policy_digest": apiclient.Digest(b.Config)}, "", "", ""), nil
	case "auth.check":
		if !admin {
			return Response{}, denied()
		}
		_, err := b.API.Read(ctx, apiclient.Query{Operation: "channel.list", Limit: 1})
		if err != nil {
			return Response{}, err
		}
		return item(map[string]any{"authentication_verified": true, "bot_id": b.Config.Bot}, "", "", ""), nil
	case "event.scan":
		if r.Scan == nil {
			return Response{}, e.New(e.CategoryUsage, "WATCH_SCOPE", "A watch scope is required.")
		}
		scan := *r.Scan
		scan.Preview = r.DryRun
		p, err := b.scan(ctx, scan)
		return Response{Scan: &p}, err
	case "event.list":
		if q.AfterSequence < 0 {
			return Response{}, e.New(e.CategoryUsage, "BAD_SEQUENCE", "The event sequence must be non-negative.")
		}
		var records []apiclient.Record
		snapshot := b.Store.View()
		if q.AfterSequence > snapshot.NextEventSequence {
			return Response{}, e.New(e.CategoryConflict, "SEQUENCE_AHEAD", "This checkpoint is ahead of the broker ledger; verify the bot and state directory.")
		}
		for _, v := range state.Events(snapshot) {
			if !b.visibleEvent(v) || v.Sequence <= q.AfterSequence {
				continue
			}
			records = append(records, apiclient.Untrusted(v, v.Channel, v.Post, v.ID))
		}
		if q.AfterSequence > 0 && snapshot.NextEventSequence > q.AfterSequence && (len(records) == 0 || records[0].Data.(callback.Event).Sequence > q.AfterSequence+1) {
			return Response{}, e.New(e.CategoryConflict, "EVENT_GAP", "Events after this checkpoint are no longer retained or allowed by current policy.").WithHint("Review retention and policy changes before deliberately starting a fresh listing.")
		}
		p, err := localPage(records, q)
		return page(p), err
	case "event.get", "event.context":
		v, err := b.event(r.ID)
		if err != nil {
			return Response{}, err
		}
		if r.Operation == "event.get" {
			return item(v, v.Channel, v.Post, v.ID), nil
		}
		if v.Kind != "topic_at_bot" || v.Created < b.Now().Add(-24*time.Hour).Unix() {
			return Response{}, denied()
		}
		p, err := b.API.Read(ctx, apiclient.Query{Operation: "post.get", Channel: v.Channel, Post: v.Post})
		if err != nil {
			return Response{}, err
		}
		if v.Reply != "" {
			replies, readErr := b.API.Read(ctx, apiclient.Query{Operation: "reply.get", Channel: v.Channel, Post: v.Post, Reply: v.Reply})
			if readErr != nil {
				return Response{}, readErr
			}
			p.Items = append(p.Items, replies.Items...)
		}
		for i := range p.Items {
			p.Items[i].Source.Event = v.ID
		}
		return page(p), err
	case "channel.list":
		if !admin {
			if q.Cursor != "" {
				return Response{}, e.New(e.CategoryUsage, "BAD_CURSOR", "Agent channel discovery returns the fixed allowlist without a cursor.")
			}
			q.ChannelIDs = []string{}
			for _, ch := range b.Config.Channels {
				q.ChannelIDs = append(q.ChannelIDs, ch.ID)
			}
			if len(q.ChannelIDs) == 0 {
				return page(apiclient.Page{Items: []apiclient.Record{}}), nil
			}
		}
		p, err := b.API.Read(ctx, q)
		return page(p), err
	case "member.list", "post.list", "post.get", "reply.list", "reply.get", "search.start", "search.result":
		ch, ok := b.Config.Channel(q.Channel)
		if !ok || (!admin && !ch.ReadAll) {
			return Response{}, denied()
		}
		p, err := b.API.Read(ctx, q)
		return page(p), err
	case "proposal.create":
		return b.propose(r)
	case "proposal.get":
		p, ok := b.Store.View().Proposals[r.ID]
		if !ok {
			return Response{}, missing()
		}
		if _, ok := b.Config.Channel(p.Request.Channel); !ok && !admin {
			return Response{}, denied()
		}
		return item(p, p.Request.Channel, p.Request.Post, p.Request.Event), nil
	case "approval.list":
		if !admin {
			return Response{}, denied()
		}
		s := b.Store.View()
		ids := make([]string, 0, len(s.Proposals))
		for id := range s.Proposals {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var records []apiclient.Record
		for _, id := range ids {
			p := s.Proposals[id]
			records = append(records, apiclient.Untrusted(p, p.Request.Channel, p.Request.Post, p.Request.Event))
		}
		p, err := localPage(records, q)
		return page(p), err
	case "approval.approve", "approval.reject", "approval.resolve":
		if !admin {
			return Response{}, denied()
		}
		return b.approve(ctx, r)
	default:
		return Response{}, e.New(e.CategoryUsage, "UNKNOWN_OPERATION", "This broker operation is not supported.")
	}
}

func localPage(records []apiclient.Record, q apiclient.Query) (apiclient.Page, error) {
	if q.Limit == 0 {
		q.Limit = 20
	}
	if q.Limit < 1 || q.Limit > 100 {
		return apiclient.Page{}, e.New(e.CategoryUsage, "BAD_LIMIT", "Limit must be between 1 and 100.")
	}
	start := 0
	if q.Cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil || len(decoded) > 256 {
			return apiclient.Page{}, e.New(e.CategoryUsage, "BAD_CURSOR", "Invalid local cursor.")
		}
		found := false
		for i, r := range records {
			if recordID(r) == string(decoded) {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return apiclient.Page{}, e.New(e.CategoryConflict, "CURSOR_EXPIRED", "The cursor's record is no longer retained; start a fresh listing.")
		}
	}
	end := start + q.Limit
	if end > len(records) {
		end = len(records)
	}
	p := apiclient.Page{Items: records[start:end], HasMore: end < len(records)}
	if p.HasMore {
		p.Next = base64.RawURLEncoding.EncodeToString([]byte(recordID(records[end-1])))
	}
	return p, nil
}
func recordID(r apiclient.Record) string {
	switch v := r.Data.(type) {
	case callback.Event:
		return v.ID
	case state.Proposal:
		return v.ID
	}
	return ""
}

func (b *Broker) validateWrite(r apiclient.WriteRequest) error {
	if _, err := apiclient.BuildWrite(r); err != nil {
		return err
	}
	ch, ok := b.Config.Channel(r.Channel)
	if !ok {
		return denied()
	}
	if r.Kind == "post" {
		if !ch.AllowPosts {
			return denied()
		}
		return nil
	}
	if !ch.AllowReplies {
		return denied()
	}
	if r.Event != "" {
		v, err := b.event(r.Event)
		if err != nil {
			return err
		}
		if v.Kind != "topic_at_bot" || v.Channel != r.Channel || v.Post != r.Post || v.Reply != r.ParentReply || v.Created < b.Now().Add(-24*time.Hour).Unix() {
			return denied()
		}
	} else if !ch.ReadAll {
		return denied()
	}
	return nil
}

func (b *Broker) propose(r Request) (Response, error) {
	if r.Write == nil {
		return Response{}, e.New(e.CategoryUsage, "WRITE_REQUIRED", "A write proposal is required.")
	}
	w := *r.Write
	if err := b.validateWrite(w); err != nil {
		return Response{}, err
	}
	plan, _ := apiclient.BuildWrite(w)
	policy := apiclient.Digest(b.Config)
	digest := apiclient.Digest(struct {
		Bot, Policy string
		Request     apiclient.WriteRequest
		Plan        apiclient.Plan
	}{b.Config.Bot, policy, w, plan})
	p := state.Proposal{ID: digest, Digest: digest, Policy: policy, Request: w, Plan: plan, Created: b.Now().Unix(), Expires: b.Now().Add(time.Duration(b.Config.ProposalTTLSeconds) * time.Second).Unix(), Status: "pending"}
	if r.DryRun {
		return item(map[string]any{"dry_run": true, "proposal": p, "requires_operator_approval": true}, w.Channel, w.Post, w.Event), nil
	}
	err := b.Store.Update(func(s *state.Snapshot) error {
		for _, old := range s.Proposals {
			if old.Request.Key == w.Key {
				if old.Digest != digest {
					return e.New(e.CategoryConflict, "KEY_REUSED", "This idempotency key belongs to a different immutable proposal.")
				}
				p = old
				return nil
			}
		}
		if len(s.Proposals) >= b.Config.MaxProposals {
			return e.New(e.CategoryRateLimit, "LEDGER_FULL", "The proposal ledger is full; ask the operator to review its capacity. Do not delete the ledger to retry.")
		}
		s.Proposals[p.ID] = p
		return nil
	})
	return item(p, w.Channel, w.Post, w.Event), err
}

func (b *Broker) approve(ctx context.Context, r Request) (Response, error) {
	b.approvalMu.Lock()
	defer b.approvalMu.Unlock()
	p, ok := b.Store.View().Proposals[r.ID]
	if !ok {
		return Response{}, missing()
	}
	if r.Digest == "" || r.Digest != p.Digest {
		return Response{}, e.New(e.CategoryConflict, "DIGEST_MISMATCH", "Approval must name the exact digest shown in the proposal.")
	}
	if r.Operation == "approval.resolve" {
		if p.Status != "sending" && p.Status != "uncertain" {
			return Response{}, e.New(e.CategoryConflict, "NOT_UNCERTAIN", "Only an uncertain or interrupted send can be reconciled.")
		}
		if r.Outcome != "sent" && r.Outcome != "not_sent" {
			return Response{}, e.New(e.CategoryUsage, "BAD_OUTCOME", "Resolve requires outcome sent or not_sent after an independent read.")
		}
		if r.Outcome == "sent" && !apiclient.Identifier(r.RemoteID, 128) {
			return Response{}, e.New(e.CategoryUsage, "REMOTE_ID_REQUIRED", "A verified remote resource ID is required.")
		}
		if r.DryRun {
			return item(map[string]any{"dry_run": true, "proposal": p, "outcome": r.Outcome}, p.Request.Channel, p.Request.Post, p.Request.Event), nil
		}
		if !r.Yes {
			return Response{}, confirmation()
		}
		p.Status = "resolved_" + r.Outcome
		p.RemoteID = r.RemoteID
		return b.saveProposal(p)
	}
	if p.Status != "pending" {
		return Response{}, e.New(e.CategoryConflict, "PROPOSAL_TERMINAL", "This proposal is not pending and cannot be executed again.").WithHint("Inspect proposal get; reconcile an uncertain send through the operator socket.")
	}
	if r.Operation == "approval.reject" {
		if r.DryRun {
			return item(map[string]any{"dry_run": true, "proposal": p}, p.Request.Channel, p.Request.Post, p.Request.Event), nil
		}
		if !r.Yes {
			return Response{}, confirmation()
		}
		p.Status = "rejected"
		return b.saveProposal(p)
	}
	if p.Policy != apiclient.Digest(b.Config) {
		return Response{}, e.New(e.CategoryConflict, "POLICY_CHANGED", "The operator policy changed after this proposal was created.")
	}
	if p.Expires <= b.Now().Unix() {
		return Response{}, e.New(e.CategoryConflict, "PROPOSAL_EXPIRED", "The proposal expired; review current context before proposing again with a new key.")
	}
	if err := b.validateWrite(p.Request); err != nil {
		return Response{}, err
	}
	if r.DryRun {
		return item(map[string]any{"dry_run": true, "proposal": p, "blocked_by_read_only": b.Config.ReadOnly}, p.Request.Channel, p.Request.Post, p.Request.Event), nil
	}
	if b.Config.ReadOnly {
		return Response{}, e.New(e.CategoryPermission, "READONLY_BLOCKED", "The operator configured this broker as read-only.")
	}
	if !r.Yes {
		return Response{}, confirmation()
	}
	p.Status = "sending"
	if _, err := b.saveProposal(p); err != nil {
		return Response{}, err
	}
	result, sendErr := b.API.Send(ctx, p.Request)
	if sendErr != nil {
		p.Status = "uncertain"
		p.ErrorCode = e.Safe(sendErr).Code
		_, saveErr := b.saveProposal(p)
		x := e.New(e.CategoryConflict, "WRITE_UNCERTAIN", "The write may have reached WeCom; automatic replay is blocked.").WithDetails(map[string]any{"proposal_id": p.ID, "status": p.Status, "state_persisted": saveErr == nil}).WithNextSteps("wecom-channel-cli proposal get --id " + p.ID)
		x.Retryable = false
		return Response{}, x
	}
	p.Status = "sent"
	p.RemoteID = result.ID
	out, err := b.saveProposal(p)
	if err != nil {
		return Response{}, e.New(e.CategoryConflict, "WRITE_COMMITTED_STATE_FAILED", "WeCom returned success, but the final state could not be persisted. Do not resend.").WithDetails(map[string]any{"proposal_id": p.ID, "remote_id": result.ID}).WithNextSteps("wecom-channel-cli proposal get --id " + p.ID)
	}
	return out, nil
}

func (b *Broker) saveProposal(p state.Proposal) (Response, error) {
	err := b.Store.Update(func(s *state.Snapshot) error { s.Proposals[p.ID] = p; return nil })
	return item(p, p.Request.Channel, p.Request.Post, p.Request.Event), err
}
func confirmation() error {
	return e.New(e.CategoryPermission, "CONFIRM_REQUIRED", "Operator confirmation is required.").WithHint("Review the exact target and content, then pass --yes on the operator socket.")
}
