package broker

import (
	"context"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

func watchFixture(t *testing.T) *Broker {
	b, _ := fixture(t)
	b.Config.CallbackListen = "127.0.0.1:8787"
	b.Config.Events = []string{"topic_event", "topic_reply_event", "topic_at_bot"}
	b.Config.Channels[0].ReadAll = true
	b.Config.Channels[0].ObserveAll = true
	return b
}
func observed(t *testing.T, b *Broker, id, post, kind string) {
	t.Helper()
	v := callback.Event{ID: id, Bot: b.Config.Bot, Channel: b.Config.Channels[0].ID, Sender: "outsider", SenderKind: "user", Kind: kind, Post: post, Created: b.Now().Unix(), Received: b.Now().Unix(), Change: 1}
	if err := b.Store.AddEvent(v, b.Now(), 48*time.Hour, 10000); err != nil {
		t.Fatal(err)
	}
}
func TestWatchScopesAndFilteredProgress(t *testing.T) {
	b := watchFixture(t)
	observed(t, b, "a", "post-a", "topic_event")
	observed(t, b, "b", "post-b", "topic_reply_event")
	observed(t, b, "c", "post-a", "topic_reply_event")
	r := ScanRequest{Posts: []string{"post-a"}, Limit: 100}
	p, err := b.scan(r)
	if err != nil || len(p.Items) != 2 || p.After != 3 {
		t.Fatal(p, err)
	}
	if p.Coverage.Actors != "all_users_and_bots" || !p.Items[0].Data.(callback.Event).ObservationOnly {
		t.Fatal("observation authority")
	}
	r.After = p.After
	r.Binding = p.Binding
	observed(t, b, "d", "post-b", "topic_event")
	p, err = b.scan(r)
	if err != nil || len(p.Items) != 0 || p.After != 4 {
		t.Fatal("nonmatching event did not advance scan", p, err)
	}
	r = ScanRequest{WholeChannel: true, Limit: 2}
	p, err = b.scan(r)
	if err != nil || len(p.Items) != 2 || p.After != 2 || !p.HasMore {
		t.Fatal(p, err)
	}
	r.After = p.After
	r.Binding = p.Binding
	p, err = b.scan(r)
	if err != nil || len(p.Items) != 2 || p.HasMore {
		t.Fatal(p, err)
	}
	tail, err := b.scan(ScanRequest{WholeChannel: true, FromNow: true, Limit: 100})
	if err != nil || tail.After != 4 || len(tail.Items) != 0 {
		t.Fatal(tail, err)
	}
}
func TestWatchBindingAndGaps(t *testing.T) {
	b := watchFixture(t)
	observed(t, b, "a", "p", "topic_event")
	observed(t, b, "b", "p", "topic_reply_event")
	observed(t, b, "c", "p", "topic_event")
	r := ScanRequest{WholeChannel: true, Limit: 100}
	first, err := b.scan(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Binding = first.Binding
	r.WholeChannel = false
	r.Posts = []string{"p"}
	_, err = b.scan(r)
	assertCode(t, err, "WATCH_BINDING_CHANGED")
	r = ScanRequest{WholeChannel: true, Limit: 100, Binding: first.Binding}
	b.Config.MaxEvents++
	_, err = b.scan(r)
	assertCode(t, err, "WATCH_BINDING_CHANGED")
	b.Config.MaxEvents--
	if err = b.Store.Update(func(s *state.Snapshot) error { delete(s.Events, "b"); return nil }); err != nil {
		t.Fatal(err)
	}
	_, err = b.scan(r)
	assertCode(t, err, "EVENT_GAP")
	b.Now = func() time.Time { return time.Unix(1792890000, 0).Add(50 * time.Hour) }
	r.After = 2
	_, err = b.scan(r)
	assertCode(t, err, "EVENT_GAP")
}
func TestWatchCoverageDoesNotAuthorizeRequests(t *testing.T) {
	b := watchFixture(t)
	if !b.Config.AllowsEvent("channel-a", "stranger", "topic_event", "user") || !b.Config.AllowsEvent("channel-a", "another-bot", "topic_reply_event", "bot") {
		t.Fatal("whole-channel actors missing")
	}
	if b.Config.AllowsEvent("channel-a", "stranger", "topic_at_bot", "user") || b.Config.AllowsEvent("channel-a", "another-bot", "topic_at_bot", "bot") {
		t.Fatal("observation granted request eligibility")
	}
	if !b.Config.AllowsEvent("channel-a", "alice", "topic_at_bot", "user") {
		t.Fatal("existing mention eligibility changed")
	}
	b.Config.Channels[0].ObserveAll = false
	r := ScanRequest{WholeChannel: true, Limit: 100}
	_, err := b.scan(r)
	assertCode(t, err, "WATCH_COVERAGE")
	r.AllowPartial = true
	p, err := b.scan(r)
	if err != nil || p.Coverage.Actors != "allowlisted_humans" {
		t.Fatal(p, err)
	}
	b.Config.Events = []string{"topic_at_bot"}
	_, err = b.scan(r)
	assertCode(t, err, "WATCH_EVENTS_DISABLED")
	b.Config.CallbackListen = ""
	_, err = b.scan(r)
	assertCode(t, err, "WATCH_INGRESS_DISABLED")
	_, err = b.Handle(context.Background(), Request{Operation: "event.scan", Context: "another", Scan: &r}, false)
	assertCode(t, err, "CONTEXT_MISMATCH")
}
func TestScanBudgetAdvancesWithoutMatches(t *testing.T) {
	b := watchFixture(t)
	if err := b.Store.Update(func(s *state.Snapshot) error {
		for i := int64(1); i <= 105; i++ {
			v := callback.Event{ID: string(rune(i + 100)), Sequence: i, Bot: b.Config.Bot, Channel: "channel-a", Post: "other", Sender: "alice", SenderKind: "user", Kind: "topic_event", Received: b.Now().Unix(), Created: b.Now().Unix()}
			s.Events[v.ID] = v
		}
		s.NextEventSequence = 105
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p, err := b.scan(ScanRequest{Posts: []string{"selected"}, Limit: 100})
	if err != nil || p.After != 100 || len(p.Items) != 0 || !p.HasMore {
		t.Fatal(p, err)
	}
}
