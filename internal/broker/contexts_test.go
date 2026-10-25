package broker

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
)

func TestContextBoundaryAndCallbackRouting(t *testing.T) {
	a, _ := fixture(t)
	b, _ := fixture(t)
	a.Config.Name = "engineering"
	b.Config.Name = "security"
	b.Config.Channels[0].ID = "channel-b"
	p := proposal(t, a)
	_, err := b.Handle(context.Background(), Request{Operation: "proposal.get", ID: p.ID}, false)
	assertCode(t, err, "NOT_FOUND")
	for _, r := range []Request{{Operation: "health", Context: "engineering"}, {Operation: "health", Bot: "wrong"}, {Operation: "health", Channel: "channel-a"}, {Operation: "post.get", Query: apiclient.Query{Channel: "channel-a", Post: "p"}}} {
		_, err = b.Handle(context.Background(), r, false)
		if err == nil {
			t.Fatal("cross-context request accepted", r)
		}
	}
	payload := fmt.Sprintf(`{"msgid":"event-b","aibotid":"bot","create_time":%d,"topic_id":"channel-b","from":{"userid":"alice"},"msgtype":"event","event":{"eventtype":"topic_at_bot","topic_at_bot":{"post_id":"post-b"}}}`, a.Now().Unix())
	h := a.callbackRouter(map[string]*Broker{"channel-a": a, "channel-b": b})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, encryptedRequest(t, a, payload, a.Now().Unix()))
	if rr.Code != 200 || len(b.Store.View().Events) != 1 || len(a.Store.View().Events) != 1 {
		t.Fatal("callback crossed context boundary")
	}
	rr = httptest.NewRecorder()
	b.Handler(false).ServeHTTP(rr, httptest.NewRequest("POST", "/v1/request", strings.NewReader(`{"operation":"event.list","context":"security","bot_id":"bot","channel_id":"channel-b"}`)))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"context":"security"`) || strings.Contains(rr.Body.String(), "post-a") {
		t.Fatal(rr.Code, rr.Body.String())
	}
}
func TestStateCannotBeRebound(t *testing.T) {
	b, _ := fixture(t)
	p := proposal(t, b)
	if p.ID == "" {
		t.Fatal("missing proposal")
	}
	_ = b.Store.Close()
	if other, err := state.Open(b.Config.StateDir, "bot", "security\x00channel-b"); err == nil {
		other.Close()
		t.Fatal("unscoped ledger silently rebound")
	}
}
