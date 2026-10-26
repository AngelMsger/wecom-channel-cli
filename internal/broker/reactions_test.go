package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

type reactionAPI struct {
	*fakeAPI
	message apiclient.Message
}

func (a *reactionAPI) Send(ctx context.Context, r apiclient.WriteRequest) (apiclient.SendResult, error) {
	result, err := a.fakeAPI.Send(ctx, r)
	if r.Kind == "reaction" {
		result = apiclient.SendResult{Kind: "reaction"}
	}
	return result, err
}
func (a *reactionAPI) Read(_ context.Context, q apiclient.Query) (apiclient.Page, error) {
	a.reads = append(a.reads, q)
	return apiclient.Page{Items: []apiclient.Record{apiclient.Untrusted(a.message, q.Channel, q.Post, "")}}, nil
}
func reactionFixture(t *testing.T) (*Broker, *reactionAPI, apiclient.WriteRequest) {
	b, f := fixture(t)
	b.Config.Channels[0].ReadAll = true
	b.Config.Channels[0].AllowReactions = true
	b.Config.Channels[0].ReactionScope = "channel"
	b.Config.Channels[0].AllowReplies = false
	b.Config.Channels[0].AllowPosts = false
	api := &reactionAPI{fakeAPI: f}
	b.API = api
	return b, api, apiclient.WriteRequest{Kind: "reaction", Channel: "channel-a", Post: "post-a", EmojiType: 1, Action: "add", Key: "reaction-a"}
}
func createReaction(t *testing.T, b *Broker, w apiclient.WriteRequest) state.Proposal {
	t.Helper()
	out, err := b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	if err != nil {
		t.Fatal(err)
	}
	return out.Item.Data.(state.Proposal)
}

func TestReactionApprovalLifecycleAndDigest(t *testing.T) {
	b, api, w := reactionFixture(t)
	out, err := b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w, DryRun: true}, false)
	if err != nil || out.Item == nil || len(b.Store.View().Proposals) != 0 || api.sends != 0 || len(api.reads) != 0 {
		t.Fatal("dry-run effects", err)
	}
	p := createReaction(t, b, w)
	r := Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}
	_, err = b.Handle(context.Background(), r, false)
	assertCode(t, err, "POLICY_DENIED")
	bad := r
	bad.Digest = "different"
	_, err = b.Handle(context.Background(), bad, true)
	assertCode(t, err, "DIGEST_MISMATCH")
	b.Config.ReadOnly = true
	// Read-only changes invalidate the policy digest first; use a fresh fixture for that gate.
	_, err = b.Handle(context.Background(), r, true)
	assertCode(t, err, "POLICY_CHANGED")
	b.Config.ReadOnly = false
	r.DryRun = true
	_, err = b.Handle(context.Background(), r, true)
	if err != nil || api.sends != 0 {
		t.Fatal(err)
	}
	r.DryRun = false
	out, err = b.Handle(context.Background(), r, true)
	if err != nil || out.Item.Data.(state.Proposal).Status != "sent" || out.Item.Data.(state.Proposal).RemoteID != "" || api.sends != 1 {
		t.Fatal(out, err)
	}
	_, err = b.Handle(context.Background(), r, true)
	assertCode(t, err, "PROPOSAL_TERMINAL")
	w.Action = "remove"
	_, err = b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	assertCode(t, err, "KEY_REUSED")
	w.Key = "remove-1"
	w.Reply = "reply-a"
	p = createReaction(t, b, w)
	if p.Plan.Body["action"] != 0 || p.Plan.Body["reply_id"] != "reply-a" || p.Plan.Body["content"] != nil {
		t.Fatal(p.Plan)
	}
	_, err = b.Handle(context.Background(), Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}, true)
	if err != nil || api.sends != 2 {
		t.Fatal(err, api.sends)
	}
}

func TestReactionMentionScopeAndExpiryAtApproval(t *testing.T) {
	b, api, w := reactionFixture(t)
	b.Config.Channels[0].ReactionScope = ""
	_, err := b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	assertCode(t, err, "REACTION_MENTION_REQUIRED")
	addMention(t, b)
	w.Event = "event-a"
	_, err = b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	assertCode(t, err, "POLICY_DENIED")
	w.Reply = "reply-a"
	if err := b.Store.Update(func(s *state.Snapshot) error {
		v := s.Events["event-a"]
		v.Created = b.Now().Add(-24*time.Hour + time.Minute).Unix()
		s.Events[v.ID] = v
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p := createReaction(t, b, w)
	later := b.Now().Add(2 * time.Minute)
	b.Now = func() time.Time { return later }
	_, err = b.Handle(context.Background(), Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}, true)
	assertCode(t, err, "POLICY_DENIED")
	if api.sends != 0 {
		t.Fatal("expired mention sent")
	}
	if err := b.Store.AddEvent(callback.Event{ID: "poll", Bot: "bot", Channel: "channel-a", Post: "post-a", Kind: "topic_event", Transport: "poll", ObservationOnly: true, Received: b.Now().Unix(), Created: b.Now().Unix()}, b.Now(), 48*time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	b.Config.Channels[0].ObserveAll = true
	b.Config.Events = append(b.Config.Events, "topic_event")
	w.Event = "poll"
	w.Reply = ""
	_, err = b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	assertCode(t, err, "POLICY_DENIED")
	b.Config.Channels[0].AllowReactions = false
	w.Event = ""
	_, err = b.Handle(context.Background(), Request{Operation: "proposal.create", Write: &w}, false)
	assertCode(t, err, "POLICY_DENIED")
}

func TestReactionReadAndUncertainReconciliation(t *testing.T) {
	b, api, w := reactionFixture(t)
	api.message = apiclient.Message{PostID: "post-a", Content: "PRIVATE BODY", Reactions: []apiclient.Reaction{
		{EmojiType: 1, Users: []apiclient.User{{Type: 1, BotID: "bot"}, {Type: 0, UserID: "alice"}}},
		{EmojiType: 3, Users: []apiclient.User{{Type: 1, LegacyBotID: "another-bot"}}},
	}}
	out, err := b.Handle(context.Background(), Request{Operation: "reaction.list", Query: apiclient.Query{Channel: "channel-a", Post: "post-a"}}, false)
	if err != nil || len(out.Page.Items) != 2 || out.Page.Items[0].Trust != "untrusted" || out.Page.Items[0].Data.(map[string]any)["bot_reacted"] != true || out.Page.Items[1].Data.(map[string]any)["bot_reacted"] != false || out.Page.Items[0].Data.(map[string]any)["content"] != nil {
		t.Fatal(out, err)
	}
	b.Config.Channels[0].ReadAll = false
	_, err = b.Handle(context.Background(), Request{Operation: "reaction.list", Query: apiclient.Query{Channel: "channel-a", Post: "post-a"}}, false)
	assertCode(t, err, "POLICY_DENIED")
	addMention(t, b)
	api.message.PostID = ""
	api.message.ReplyID = "reply-a"
	_, err = b.Handle(context.Background(), Request{Operation: "reaction.list", ID: "event-a", Query: apiclient.Query{Channel: "channel-a", Post: "post-a", Reply: "reply-a"}}, false)
	if err != nil || api.reads[len(api.reads)-1].Operation != "reply.get" {
		t.Fatal(err)
	}
	b.Config.Channels[0].ReadAll = true
	api.err = errors.New("synthetic transport failure")
	p := createReaction(t, b, w)
	r := Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}
	_, err = b.Handle(context.Background(), r, true)
	assertCode(t, err, "WRITE_UNCERTAIN")
	_, err = b.Handle(context.Background(), r, true)
	assertCode(t, err, "PROPOSAL_TERMINAL")
	if api.sends != 1 {
		t.Fatal("uncertain reaction retried")
	}
	r.Operation = "approval.resolve"
	r.Outcome = "sent"
	r.RemoteID = "invented-reaction-id"
	_, err = b.Handle(context.Background(), r, true)
	assertCode(t, err, "REACTION_NO_REMOTE_ID")
	r.RemoteID = ""
	out, err = b.Handle(context.Background(), r, true)
	if err != nil || out.Item.Data.(state.Proposal).Status != "resolved_sent" || out.Item.Data.(state.Proposal).RemoteID != "" || api.sends != 1 {
		t.Fatal(out, err)
	}
}

func TestReadOnlyReactionProposalCannotExecute(t *testing.T) {
	b, api, w := reactionFixture(t)
	b.Config.ReadOnly = true
	p := createReaction(t, b, w)
	_, err := b.Handle(context.Background(), Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}, true)
	assertCode(t, err, "READONLY_BLOCKED")
	if api.sends != 0 {
		t.Fatal("read-only reaction sent")
	}
}

func TestReactionMembershipFailsOnAmbiguousIdentities(t *testing.T) {
	for _, users := range [][]apiclient.User{nil, {{Type: 1}}, {{Type: 1, BotID: "bot", LegacyBotID: "other"}}, {{Type: 0}}, {{Type: 2, BotID: "bot"}}} {
		b, api, _ := reactionFixture(t)
		api.message = apiclient.Message{PostID: "post-a", Reactions: []apiclient.Reaction{{EmojiType: 1, Users: users}}}
		_, err := b.Handle(context.Background(), Request{Operation: "reaction.list", Query: apiclient.Query{Channel: "channel-a", Post: "post-a"}}, false)
		assertCode(t, err, "BAD_REACTION_RESULT")
	}
}

func TestReactionUncertaintySurvivesBrokerRestart(t *testing.T) {
	b, api, w := reactionFixture(t)
	api.err = errors.New("synthetic lost acknowledgment")
	p := createReaction(t, b, w)
	r := Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}
	_, err := b.Handle(context.Background(), r, true)
	assertCode(t, err, "WRITE_UNCERTAIN")
	if err := b.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.Open(b.Config.StateDir, b.Config.Bot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	restarted := New(b.Config, reopened, api, b.Verifier)
	restarted.Now = b.Now
	_, err = restarted.Handle(context.Background(), r, true)
	assertCode(t, err, "PROPOSAL_TERMINAL")
	r.Operation = "approval.resolve"
	r.Outcome = "sent"
	out, err := restarted.Handle(context.Background(), r, true)
	if err != nil || out.Item.Data.(state.Proposal).Status != "resolved_sent" || api.sends != 1 {
		t.Fatal(out, err, api.sends)
	}
}
