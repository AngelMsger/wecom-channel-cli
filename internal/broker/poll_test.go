package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

type pollAPI struct {
	mu    sync.Mutex
	pages map[string]apiclient.Page
	reads []apiclient.Query
	fail  string
	block bool
}

func pollKey(q apiclient.Query) string { return q.Operation + "/" + q.Post + "/" + q.Cursor }
func (f *pollAPI) Read(ctx context.Context, q apiclient.Query) (apiclient.Page, error) {
	f.mu.Lock()
	f.reads = append(f.reads, q)
	p, fail, block := f.pages[pollKey(q)], f.fail == pollKey(q), f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return apiclient.Page{}, ctx.Err()
	}
	if fail {
		return apiclient.Page{}, errors.New("synthetic read failure")
	}
	return p, nil
}
func (*pollAPI) Send(context.Context, apiclient.WriteRequest) (apiclient.SendResult, error) {
	panic("polling attempted a remote write")
}
func polled(post, reply, content string, recalled bool) apiclient.Record {
	return apiclient.Untrusted(apiclient.Message{PostID: post, ReplyID: reply, Created: 1700000000, Modified: 1700000001, Content: content, Recalled: recalled, AttachmentsOmitted: true}, "channel-a", post, "")
}
func pollFixture(t *testing.T) (*Broker, *pollAPI) {
	b := watchFixture(t)
	b.Config.CallbackListen = ""
	b.Verifier = nil
	b.Config.PollIntervalSeconds = 5
	f := &pollAPI{pages: map[string]apiclient.Page{}}
	b.API = apiclient.NewReadOnly(f)
	return b, f
}
func tick(b *Broker) {
	now := b.Now().Add(6 * time.Second)
	b.Now = func() time.Time { return now }
}
func setPosts(f *pollAPI, rows ...apiclient.Record) {
	f.pages["post.list//"] = apiclient.Page{Items: rows}
}

func TestPollingBaselineChangesRecallAndRestart(t *testing.T) {
	b, f := pollFixture(t)
	setPosts(f, polled("old", "", "existing private body", false))
	r := ScanRequest{WholeChannel: true, FromNow: true, Limit: 100}
	baseline, err := b.scan(context.Background(), r)
	if err != nil || baseline.After != 0 || baseline.Coverage.Backend != "poll" {
		t.Fatal(baseline, err)
	}
	if len(f.reads) != 2 {
		t.Fatal("did not read replies to old post", f.reads)
	}
	for _, q := range f.reads {
		if q.From != 1 || q.To == 0 {
			t.Fatal("unbounded list", q)
		}
	}
	setPosts(f, polled("old", "", "edited body", false), polled("new", "", "new body", false))
	f.pages["reply.list/old/"] = apiclient.Page{Items: []apiclient.Record{polled("old", "reply", "reply on an old post", false)}}
	r.FromNow, r.Binding = false, baseline.Binding
	unchanged, err := b.scan(context.Background(), r)
	if err != nil || unchanged.After != 0 || len(f.reads) != 2 {
		t.Fatal("minimum interval bypassed", err)
	}
	tick(b)
	p, err := b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 3 {
		t.Fatal(p, err)
	}
	changes := map[string]int{}
	for _, record := range p.Items {
		v := record.Data.(callback.Event)
		changes[v.Post+"/"+v.Reply] = v.Change
		if v.Transport != "poll" || !v.ObservationOnly || v.SenderKind != "unknown" || v.Sender != "" || v.ResourceCreated != 1700000000 {
			t.Fatal(v)
		}
		if _, err := b.Handle(context.Background(), Request{Operation: "event.context", ID: v.ID}, false); err == nil {
			t.Fatal("polling minted mention authority")
		}
	}
	if changes["old/"] != 2 || changes["new/"] != 1 || changes["old/reply"] != 1 {
		t.Fatal(changes)
	}
	ledger, _ := os.ReadFile(filepath.Join(b.Config.StateDir, "ledger.json"))
	if strings.Contains(string(ledger), "edited body") || strings.Contains(string(ledger), "existing private body") {
		t.Fatal("message body persisted")
	}
	r.After = p.After
	setPosts(f, polled("old", "", "edited body", false)) // Disappearance is not deletion.
	tick(b)
	p, err = b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 0 {
		t.Fatal(p, err)
	}
	f.pages["reply.list/old/"] = apiclient.Page{Items: []apiclient.Record{polled("old", "reply", "", true)}}
	tick(b)
	p, err = b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 1 || p.Items[0].Data.(callback.Event).ChangeBasis != "recall_flag" {
		t.Fatal(p, err)
	}
	r.After = p.After
	if err := b.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.Open(b.Config.StateDir, b.Config.Bot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	restarted := New(b.Config, reopened, f, nil)
	restarted.Now = b.Now
	tick(restarted)
	p, err = restarted.scan(context.Background(), r)
	if err != nil || len(p.Items) != 0 {
		t.Fatal("restart replayed observations", p, err)
	}
}

func TestPollingFailureDoesNotAdvanceSnapshot(t *testing.T) {
	b, f := pollFixture(t)
	setPosts(f, polled("old", "", "before", false))
	r := ScanRequest{WholeChannel: true, Limit: 100}
	if _, err := b.scan(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	before := apiclient.Digest(b.Store.View())
	setPosts(f, polled("old", "", "after", false))
	f.fail = "reply.list/old/"
	tick(b)
	if _, err := b.scan(context.Background(), r); err == nil {
		t.Fatal("missing read error")
	}
	if apiclient.Digest(b.Store.View()) != before {
		t.Fatal("partial snapshot committed")
	}
	reads := len(f.reads)
	if _, err := b.scan(context.Background(), r); err == nil || len(f.reads) != reads {
		t.Fatal("failed scope retried without interval")
	}
	f.fail = ""
	tick(b)
	p, err := b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 1 {
		t.Fatal(p, err)
	}
}

func TestPollingSelectedScopeAndCrossScopeDedup(t *testing.T) {
	b, f := pollFixture(t)
	whole := ScanRequest{WholeChannel: true, Limit: 100}
	if _, err := b.scan(context.Background(), whole); err != nil {
		t.Fatal(err)
	}
	f.pages["post.get/a/"] = apiclient.Page{Items: []apiclient.Record{polled("a", "", "a body", false)}}
	selected := ScanRequest{Posts: []string{"a"}, Limit: 100}
	p, err := b.scan(context.Background(), selected)
	if err != nil || len(p.Items) != 1 {
		t.Fatal("new scope hid a change from the initialized whole-channel scope", p, err)
	}
	if len(f.reads) != 3 || f.reads[1].Operation != "post.get" || f.reads[2].Post != "a" {
		t.Fatal("selected poll enumerated unrelated posts", f.reads)
	}
	setPosts(f, polled("a", "", "a body", false))
	tick(b)
	p, err = b.scan(context.Background(), whole)
	if err != nil || len(p.Items) != 1 || p.HighWatermark != 1 {
		t.Fatal("cross-scope duplicate", p, err)
	}
}

func TestPollingDryRunBindingAndPermissionsBeforeNetwork(t *testing.T) {
	b, f := pollFixture(t)
	before := apiclient.Digest(b.Store.View())
	r := ScanRequest{WholeChannel: true, Limit: 100}
	out, err := b.Handle(context.Background(), Request{Operation: "event.scan", DryRun: true, Scan: &r}, false)
	if err != nil || out.Scan.Coverage.Backend != "poll" || len(f.reads) != 0 || before != apiclient.Digest(b.Store.View()) {
		t.Fatal("preview effects", err)
	}
	r.Binding = strings.Repeat("f", 64)
	_, err = b.scan(context.Background(), r)
	assertCode(t, err, "WATCH_BINDING_CHANGED")
	r.Binding, r.After = "", 5
	_, err = b.scan(context.Background(), r)
	assertCode(t, err, "SEQUENCE_AHEAD")
	r.After, r.AllowPartial = 0, true
	b.Config.Channels[0].ObserveAll = false
	_, err = b.scan(context.Background(), r)
	assertCode(t, err, "POLL_OBSERVATION_SCOPE")
	if len(f.reads) != 0 {
		t.Fatal("rejection contacted provider")
	}
}

func TestPollingPaginationBoundsAndCancellation(t *testing.T) {
	for _, scenario := range []string{"pages", "cycle", "records", "missing", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			b, f := pollFixture(t)
			setPosts(f, polled("a", "", "a", false))
			r := ScanRequest{WholeChannel: true, Limit: 100}
			ctx := context.Background()
			want := ""
			switch scenario {
			case "pages":
				b.Config.PollMaxRequests = 1
				want = "POLL_BUDGET"
			case "cycle":
				f.pages["post.list//"] = apiclient.Page{HasMore: true, Next: "a"}
				f.pages["post.list//a"] = apiclient.Page{HasMore: true, Next: "b"}
				f.pages["post.list//b"] = apiclient.Page{HasMore: true, Next: "a"}
				want = "POLL_RESPONSE"
			case "records":
				b.Config.PollMaxRecords = 1
				setPosts(f, polled("a", "", "a", false), polled("b", "", "b", false))
				want = "POLL_CAPACITY"
			case "missing":
				r.WholeChannel = false
				r.Posts = []string{"absent"}
				want = "POLL_POST_UNAVAILABLE"
			case "cancel":
				f.block = true
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			before := apiclient.Digest(b.Store.View())
			_, err := b.scan(ctx, r)
			if want != "" {
				assertCode(t, err, want)
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if apiclient.Digest(b.Store.View()) != before {
				t.Fatal("failed poll advanced baseline")
			}
		})
	}
}

func TestConcurrentPollingSharesOneSnapshot(t *testing.T) {
	b, f := pollFixture(t)
	setPosts(f, polled("a", "", "a", false))
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.scan(context.Background(), ScanRequest{WholeChannel: true, Limit: 100}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(f.reads) != 2 {
		t.Fatal("parallel watchers multiplied reads", len(f.reads))
	}
}

func TestPollingCompletePaginationAndAtomicInboxCapacity(t *testing.T) {
	b, f := pollFixture(t)
	f.pages["post.list//"] = apiclient.Page{Items: []apiclient.Record{polled("a", "", "old-a", false)}, HasMore: true, Next: "next"}
	f.pages["post.list//next"] = apiclient.Page{Items: []apiclient.Record{polled("b", "", "old-b", false)}}
	f.pages["reply.list/a/"] = apiclient.Page{HasMore: true, Next: "replies"}
	f.pages["reply.list/a/replies"] = apiclient.Page{Items: []apiclient.Record{polled("a", "r", "reply", false)}}
	b.Config.MaxEvents = 1
	r := ScanRequest{WholeChannel: true, Limit: 100}
	if p, err := b.scan(context.Background(), r); err != nil || p.After != 0 || len(f.reads) != 5 {
		t.Fatal(p, err, f.reads)
	}
	before := apiclient.Digest(b.Store.View())
	f.pages["post.list//"] = apiclient.Page{Items: []apiclient.Record{polled("a", "", "new-a", false)}, HasMore: true, Next: "next"}
	f.pages["post.list//next"] = apiclient.Page{Items: []apiclient.Record{polled("b", "", "new-b", false)}}
	tick(b)
	_, err := b.scan(context.Background(), r)
	assertCode(t, err, "INBOX_FULL")
	if apiclient.Digest(b.Store.View()) != before {
		t.Fatal("capacity failure committed a partial diff")
	}
}

func TestPollingDrainsHistoryWithoutRemoteAccess(t *testing.T) {
	b, f := pollFixture(t)
	r := ScanRequest{WholeChannel: true, Limit: 100}
	if _, err := b.scan(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	setPosts(f, polled("a", "", "new", false))
	tick(b)
	p, err := b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 1 {
		t.Fatal(p, err)
	}
	before := len(f.reads)
	f.fail = "post.list//"
	tick(b)
	p, err = b.scan(context.Background(), r)
	if err != nil || len(p.Items) != 1 || len(f.reads) != before {
		t.Fatal("offline retained-history replay attempted refresh", p, err)
	}
}
