package broker

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func pagedPosts(f *pollAPI, n int) {
	var first, second []apiclient.Record
	for i := 0; i < n; i++ {
		row := polled(fmt.Sprintf("post-%03d", i), "", "private body marker", false)
		if i < 50 {
			first = append(first, row)
		} else {
			second = append(second, row)
		}
	}
	f.pages["post.list//"] = apiclient.Page{Items: first, Next: "page2", HasMore: true}
	f.pages["post.list//page2"] = apiclient.Page{Items: second}
}

func TestResumableWholeChannelBudgetsAndRestart(t *testing.T) {
	for _, tc := range []struct {
		posts, cap int
		blocked    bool
	}{{58, 100, false}, {59, 100, false}, {99, 100, true}, {99, 101, false}} {
		t.Run(fmt.Sprintf("%d-requests-cap-%d", tc.posts+2, tc.cap), func(t *testing.T) {
			b, f := pollFixture(t)
			b.Config.RequestsPerMinute, b.Config.PollMaxRequests = 60, tc.cap
			pagedPosts(f, tc.posts)
			now := b.Now()
			clock := func() time.Time { return now }
			b.Now = clock
			r := ScanRequest{ResumePoll: true, WholeChannel: true, FromNow: true, Limit: 100}
			var result ScanPage
			var err error
			for step := 0; step < 40; step++ {
				result, err = b.scan(context.Background(), r)
				if err != nil || result.Progress == nil {
					break
				}
				if result.LastSnapshot != 0 || result.After != 0 || len(result.Items) != 0 || len(b.Store.View().Events) != 0 {
					t.Fatal("partial baseline")
				}
				raw, _, _ := b.Store.StagedPoll()
				if bytes.Contains(raw, []byte("private body marker")) {
					t.Fatal("stored content")
				}
				now = time.UnixMilli(result.Progress.NextAt)
				if step == 0 {
					if err := b.Store.Close(); err != nil {
						t.Fatal(err)
					}
					st, err := state.Open(b.Config.StateDir, b.Config.Bot)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { st.Close() })
					b = New(b.Config, st, f, nil)
					b.Now = clock
				}
			}
			if tc.blocked {
				if e.Safe(err).Code != "POLL_BUDGET" || len(f.reads) != tc.cap || b.Store.View().Polling != nil {
					t.Fatal(err, len(f.reads))
				}
				return
			}
			if err != nil || result.Progress != nil || result.LastSnapshot == 0 || len(result.Items) != 0 || len(f.reads) != tc.posts+2 {
				t.Fatal(result.Progress, err, len(f.reads))
			}
			seen := map[string]bool{}
			for _, q := range f.reads {
				if seen[pollKey(q)] {
					t.Fatal("repeated committed page", q)
				}
				seen[pollKey(q)] = true
			}
			raw, _, _ := b.Store.StagedPoll()
			if len(raw) > 0 {
				t.Fatal("final commit left stage")
			}
			now = now.Add(time.Minute)
			r.FromNow = false
			r.Binding = result.Binding
			f.pages["reply.list/post-000/"] = apiclient.Page{Items: []apiclient.Record{polled("post-000", "new-reply", "", false)}}
			for step := 0; step < 40; step++ {
				result, err = b.scan(context.Background(), r)
				if err != nil || result.Progress == nil {
					break
				}
				now = time.UnixMilli(result.Progress.NextAt)
			}
			if err != nil || len(result.Items) != 1 || len(b.Store.View().Events) != 1 {
				t.Fatal(err, len(result.Items))
			}
		})
	}
}

func TestPassYieldsToOverlappingScopeAndRejectsStaleCommit(t *testing.T) {
	b, f := pollFixture(t)
	pagedPosts(f, 59)
	r := ScanRequest{WholeChannel: true, ResumePoll: true, FromNow: true, Limit: 10}
	part, err := b.scan(context.Background(), r)
	if err != nil || part.Progress == nil {
		t.Fatal(part, err)
	}
	f.pages["post.get/post-000/"] = apiclient.Page{Items: []apiclient.Record{polled("post-000", "", "edited elsewhere", false)}}
	if _, err = b.scan(context.Background(), ScanRequest{Posts: []string{"post-000"}, Limit: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err = b.scan(context.Background(), r); e.Safe(err).Code != "POLL_UNSTABLE" {
		t.Fatal(err)
	}
	if len(b.Store.View().Events) != 0 {
		t.Fatal("stale data published")
	}
}

func TestPassInterruptionResumesOnlyUncommittedPage(t *testing.T) {
	b, f := pollFixture(t)
	pagedPosts(f, 59)
	f.fail = "reply.list/post-001/"
	r := ScanRequest{WholeChannel: true, ResumePoll: true, FromNow: true, Limit: 10}
	if _, err := b.scan(context.Background(), r); err == nil {
		t.Fatal("expected interruption")
	}
	raw, _, _ := b.Store.StagedPoll()
	if len(raw) == 0 || len(f.reads) != 4 {
		t.Fatal("missing reservation or wrong page", len(f.reads))
	}
	if p, err := b.scan(context.Background(), r); err != nil || p.Progress == nil || len(f.reads) != 4 {
		t.Fatal("cached failure spent another read", p, err)
	}
	// Simulate a failed process after the reservation and before saving the page.
	restarted := New(b.Config, b.Store, f, nil)
	restarted.Now = b.Now
	f.fail = ""
	if p, err := restarted.scan(context.Background(), r); err != nil || p.Progress == nil || p.Progress.Requests != 12 || pollKey(f.reads[4]) != pollKey(f.reads[3]) {
		t.Fatal(p.Progress, err, len(f.reads))
	}
	if len(b.Store.View().Events) != 0 {
		t.Fatal("published partial work")
	}
}

func TestPassRejectsRepeatedCursorAndExpiresUnusedStage(t *testing.T) {
	b, f := pollFixture(t)
	pagedPosts(f, 59)
	second := f.pages["post.list//page2"]
	second.Next = "page2"
	second.HasMore = true
	f.pages["post.list//page2"] = second
	r := ScanRequest{WholeChannel: true, ResumePoll: true, FromNow: true, Limit: 10}
	if _, err := b.scan(context.Background(), r); e.Safe(err).Code != "POLL_RESPONSE" {
		t.Fatal(err)
	}
	raw, _, _ := b.Store.StagedPoll()
	if len(raw) != 0 {
		t.Fatal("unsafe cursor persisted")
	}
	pagedPosts(f, 59)
	tick(b)
	if p, err := b.scan(context.Background(), r); err != nil || p.Progress == nil {
		t.Fatal(p, err)
	}
	reads := len(f.reads)
	now := b.Now().Add(2 * time.Hour)
	b.Now = func() time.Time { return now }
	if _, err := b.scan(context.Background(), r); e.Safe(err).Code != "POLL_UNSTABLE" || len(f.reads) != reads {
		t.Fatal(err)
	}
	if b.Store.View().Polling != nil {
		t.Fatal("expired stage published")
	}
}

func TestAllScopesKeepOnePollRateWindowAcrossRestart(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprintf("staged-%t", staged), func(t *testing.T) {
			b, f := pollFixture(t)
			b.Config.RequestsPerMinute = 8
			if staged {
				pagedPosts(f, 59)
			}
			whole := ScanRequest{WholeChannel: true, ResumePoll: true, Limit: 10}
			first, err := b.scan(context.Background(), whole)
			if err != nil || (first.Progress != nil) != staged {
				t.Fatal(first.Progress, err)
			}
			now := b.Now()
			dir, bot := b.Config.StateDir, b.Config.Bot
			if err := b.Store.Close(); err != nil {
				t.Fatal(err)
			}
			st, err := state.Open(dir, bot)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			b = New(b.Config, st, f, nil)
			b.Now = func() time.Time { return now }
			selected := ScanRequest{Posts: []string{"old"}, FromNow: true, Limit: 10}
			f.pages["post.get/old/"] = apiclient.Page{Items: []apiclient.Record{polled("old", "", "text", false)}}
			if !staged {
				// One request completed the empty baseline. Seven remain for all scopes.
				for range 3 {
					if _, err = b.scan(context.Background(), selected); err != nil {
						t.Fatal(err)
					}
					now = now.Add(6 * time.Second)
				}
			}
			if _, err := b.scan(context.Background(), selected); e.Safe(err).Code != "POLL_RATE_LIMIT" || len(f.reads) != 8 {
				t.Fatal("the selected watch exceeded or lost the original allowance", len(f.reads), err)
			}
			now = now.Add(time.Minute)
			if _, err := b.scan(context.Background(), selected); err != nil || len(f.reads) != 10 {
				t.Fatal("the new window did not reopen", len(f.reads), err)
			}
		})
	}
}
