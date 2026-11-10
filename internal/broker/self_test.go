package broker

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

func TestCallbacksExcludeSameBotAcrossIndependentBrokers(t *testing.T) {
	for _, b := range []*Broker{watchFixture(t), watchFixture(t)} {
		// Neither broker needs a local send/proposal to recognize this bot's identity.
		h := b.CallbackHandler()
		for _, kind := range []string{"topic_event", "topic_reply_event"} {
			for change := 1; change <= 3; change++ {
				payload := fmt.Sprintf(`{"msgid":"%s-%d","aibotid":"bot","topic_id":"channel-a","create_time":%d,"from":{"aibotid":"bot"},"msgtype":"event","event":{"eventtype":"%s","%s":{"post_id":"p","reply_id":"r","type":%d}}}`, kind, change, b.Now().Unix(), kind, kind, change)
				for range 2 {
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, encryptedRequest(t, b, payload, b.Now().Unix()))
					if rr.Code != 200 || b.Store.View().NextEventSequence != 0 {
						t.Fatal("self callback was not quietly acknowledged", rr.Code, b.Store.View())
					}
				}
			}
		}
		for i, from := range []string{`{"aibotid":"other-bot"}`, `{"userid":"bot"}`} {
			payload := fmt.Sprintf(`{"msgid":"external-%d","aibotid":"bot","topic_id":"channel-a","create_time":%d,"from":%s,"msgtype":"event","event":{"eventtype":"topic_event","topic_event":{"post_id":"p","type":1}}}`, i, b.Now().Unix(), from)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, encryptedRequest(t, b, payload, b.Now().Unix()))
			if rr.Code != 200 || b.Store.View().NextEventSequence != int64(i+1) {
				t.Fatal("external actor was suppressed", rr.Code)
			}
		}
	}
}

func TestRetainedSelfEventsSkipWithoutHidingGaps(t *testing.T) {
	b := watchFixture(t)
	observed(t, b, "first", "p", "topic_event")
	for i := 2; i <= 104; i++ {
		v := callback.Event{ID: fmt.Sprint(i), Bot: "bot", Channel: "channel-a", Sender: "bot", SenderKind: "bot", Kind: "topic_event", Post: "p", Received: b.Now().Unix()}
		if err := b.Store.AddEvent(v, b.Now(), 48*time.Hour, 1000); err != nil {
			t.Fatal(err)
		}
	}
	observed(t, b, "last", "p", "topic_reply_event")
	r := ScanRequest{WholeChannel: true, After: 1, Limit: 1}
	p, err := b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 0 || p.After != 101 || !p.HasMore {
		t.Fatal("self-only page did not advance its bounded scan", p, err)
	}
	r.After, r.Binding = p.After, p.Binding
	p, err = b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 1 || p.After != 105 || p.HasMore {
		t.Fatal("external event lost behind self events", p, err)
	}
	out, err := b.Handle(context.Background(), Request{Operation: "event.list", Query: apiclient.Query{AfterSequence: 1}}, false)
	if err != nil || len(out.Page.Items) != 1 || out.Page.Items[0].Data.(callback.Event).ID != "last" {
		t.Fatal(out, err)
	}
	_, err = b.Handle(context.Background(), Request{Operation: "event.get", ID: "2"}, false)
	assertCode(t, err, "NOT_FOUND")
	if err := b.Store.Update(func(s *state.Snapshot) error { delete(s.Events, "3"); return nil }); err != nil {
		t.Fatal(err)
	}
	r.After = 1
	_, err = b.scan(context.Background(), r)
	assertCode(t, err, "EVENT_GAP")
	_, err = b.Handle(context.Background(), Request{Operation: "event.list", Query: apiclient.Query{AfterSequence: 1}}, false)
	assertCode(t, err, "EVENT_GAP")
}

func authored(post, reply string, creator, info *apiclient.User) apiclient.Record {
	r := polled(post, reply, "same text as a human message", false)
	m := r.Data.(apiclient.Message)
	m.Creator, m.CreatorInfo = creator, info
	r.Data = m
	return r
}

func TestPollingExcludesOwnResourcesButKeepsOtherReplies(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprint("selected=", selected), func(t *testing.T) {
			b, f := pollFixture(t)
			self := &apiclient.User{Type: 1, BotID: "bot"}
			legacy := &apiclient.User{Type: 1, LegacyBotID: "bot"}
			ownPost := authored("own", "", self, legacy)
			setPosts(f, ownPost)
			f.pages["post.get/own/"] = apiclient.Page{Items: []apiclient.Record{ownPost}}
			r := ScanRequest{WholeChannel: !selected, Limit: 100}
			if selected {
				r.Posts = []string{"own"}
			}
			if _, err := b.scan(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			// Every supported outer/inner creator shape identifies the same bot.
			f.pages["reply.list/own/"] = apiclient.Page{Items: []apiclient.Record{
				authored("own", "self-a", self, nil), authored("own", "self-b", legacy, nil),
				authored("own", "self-c", nil, self), authored("own", "self-d", nil, legacy),
				authored("own", "human", &apiclient.User{UserID: "bot"}, nil),
				authored("own", "other", nil, &apiclient.User{Type: 1, BotID: "other-bot"}),
			}}
			setPosts(f, ownPost, authored("new-own", "", nil, legacy))
			tick(b)
			p, err := b.scan(context.Background(), r)
			if err != nil || len(p.Items) != 2 || p.HighWatermark != 2 || p.Coverage.Actors != "unknown" {
				t.Fatal(p, err)
			}
			for _, row := range p.Items {
				v := row.Data.(callback.Event)
				if v.IsSelf() || (v.Reply != "human" && v.Reply != "other") || v.SenderKind != "unknown" || v.Sender != "" {
					t.Fatal("author became an actor or self event escaped", v)
				}
			}
			r.After = p.After
			// Own edits and recalls also remain silent, while fingerprints advance.
			for _, recall := range []bool{false, true} {
				page := f.pages["reply.list/own/"]
				for i := range 4 {
					m := page.Items[i].Data.(apiclient.Message)
					m.Content, m.Recalled = "self edit", recall
					page.Items[i].Data = m
				}
				f.pages["reply.list/own/"] = page
				tick(b)
				p, err = b.scan(context.Background(), r)
				if err != nil || len(p.Items) != 0 || p.After != 2 {
					t.Fatal("own edit/recall emitted", p, err)
				}
			}
			if err := b.Store.Close(); err != nil {
				t.Fatal(err)
			}
			st, err := state.Open(b.Config.StateDir, b.Config.Bot)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			restarted := New(b.Config, st, f, nil)
			restarted.Now = b.Now
			tick(restarted)
			p, err = restarted.scan(context.Background(), r)
			if err != nil || len(p.Items) != 0 || p.After != 2 {
				t.Fatal("restart replayed self content", p, err)
			}
		})
	}
}

func TestPollingAmbiguousCreatorCannotAdvanceSnapshot(t *testing.T) {
	self := &apiclient.User{Type: 1, BotID: "bot"}
	cases := []struct{ creator, info *apiclient.User }{
		{}, {&apiclient.User{}, nil}, {&apiclient.User{Type: 2, BotID: "bot"}, nil},
		{&apiclient.User{Type: 1, BotID: "bot", LegacyBotID: "other"}, nil},
		{&apiclient.User{Type: 0, UserID: "alice", BotID: "bot"}, nil},
		{&apiclient.User{Type: 1, BotID: "bot", UserID: "alice"}, nil},
		{self, &apiclient.User{Type: 1, BotID: "other"}},
		{self, &apiclient.User{UserID: "bot"}},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			b, f := pollFixture(t)
			r := ScanRequest{WholeChannel: true, Limit: 100}
			if _, err := b.scan(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			before := apiclient.Digest(b.Store.View())
			setPosts(f, polled("human", "", "valid", false), authored("ambiguous", "", tc.creator, tc.info))
			tick(b)
			_, err := b.scan(context.Background(), r)
			assertCode(t, err, "POLL_CREATOR_UNKNOWN")
			if apiclient.Digest(b.Store.View()) != before {
				t.Fatal("unverified author advanced snapshot")
			}
		})
	}
}

func TestLegacyPollingHistoryRequiresExplicitRecovery(t *testing.T) {
	b, _ := pollFixture(t)
	v := callback.Event{ID: "legacy", Bot: "bot", Channel: "channel-a", Kind: "topic_event", Post: "p", Transport: "poll", ObservationOnly: true, SenderKind: "unknown", Received: b.Now().Unix()}
	if err := b.Store.AddEvent(v, b.Now(), 48*time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	_, err := b.scan(context.Background(), ScanRequest{WholeChannel: true, Limit: 100})
	assertCode(t, err, "EVENT_GAP")
	_, err = b.Handle(context.Background(), Request{Operation: "event.get", ID: v.ID}, false)
	assertCode(t, err, "NOT_FOUND")
}

func TestOldSelfFilterBindingStopsBeforePolling(t *testing.T) {
	b, f := pollFixture(t)
	ledger, _, _ := b.Store.Inbox()
	legacy := apiclient.Digest(struct {
		Identity WatchIdentity
		Policy   string
		Whole    bool
		Posts    []string
		Partial  bool
	}{WatchIdentity{b.Config.Name, b.Config.Bot, "channel-a", ledger}, apiclient.Digest(b.Config), true, []string{}, false})
	_, err := b.scan(context.Background(), ScanRequest{WholeChannel: true, Limit: 100, Binding: legacy})
	assertCode(t, err, "WATCH_BINDING_CHANGED")
	if len(f.reads) != 0 {
		t.Fatal("old binding advanced polling before reconciliation")
	}
}
