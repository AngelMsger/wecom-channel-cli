package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	kit "github.com/angelmsger/agent-cli-kit/transport"
)

func TestNativeReactionWireAndAcknowledgment(t *testing.T) {
	for _, action := range []string{"add", "remove"} {
		for _, reply := range []string{"", "reply-a"} {
			t.Run(action+"/"+reply, func(t *testing.T) {
				c, _ := New("bot", "synthetic-secret")
				writes := 0
				c.http = kit.New(kit.Options{Doer: doerFunc(func(req *http.Request) (*http.Response, error) {
					if strings.HasSuffix(req.URL.Path, "get_token") {
						return response(`{"errcode":0,"access_token":"synthetic-token"}`), nil
					}
					writes++
					if req.Method != "POST" || req.URL.Path != "/cgi-bin/aibot/topic/post_emoji" || req.URL.Query().Get("access_token") != "synthetic-token" {
						t.Fatal(req.URL)
					}
					var body map[string]any
					if json.NewDecoder(req.Body).Decode(&body) != nil {
						t.Fatal("body")
					}
					wantAction := float64(1)
					if action == "remove" {
						wantAction = 0
					}
					if body["topic_id"] != "channel" || body["post_id"] != "post" || body["emoji_type"] != float64(1) || body["action"] != wantAction {
						t.Fatal(body)
					}
					wantFields := 4
					if reply != "" {
						wantFields++
						if body["reply_id"] != reply {
							t.Fatal(body)
						}
					}
					if len(body) != wantFields {
						t.Fatal("native reaction leaked text/approval fields", body)
					}
					return response(`{"errcode":0,"errmsg":"ok"}`), nil
				})})
				result, err := c.Send(context.Background(), WriteRequest{Kind: "reaction", Channel: "channel", Post: "post", Reply: reply, EmojiType: 1, Action: action, Key: "intent"})
				if err != nil || result.ID != "" || result.Kind != "reaction" || writes != 1 {
					t.Fatal(result, err, writes)
				}
			})
		}
	}
}

func TestReactionValidationBeforeTransportAndNoRetries(t *testing.T) {
	valid := WriteRequest{Kind: "reaction", Channel: "channel", Post: "post", EmojiType: 1, Action: "add", Key: "intent"}
	changes := []func(*WriteRequest){
		func(r *WriteRequest) { r.EmojiType = 0 }, func(r *WriteRequest) { r.EmojiType = 116 },
		func(r *WriteRequest) { r.Action = "" }, func(r *WriteRequest) { r.Action = "toggle" },
		func(r *WriteRequest) { r.Post = "" }, func(r *WriteRequest) { r.Reply = "bad id" },
		func(r *WriteRequest) { r.Content = "👍" }, func(r *WriteRequest) { r.ParentReply = "parent" },
		func(r *WriteRequest) { r.Event = "bad event" }, func(r *WriteRequest) { r.Key = "" },
		func(r *WriteRequest) { r.Kind = "post"; r.Content = "text" },
	}
	c, _ := New("bot", "secret")
	c.http = kit.New(kit.Options{Doer: doerFunc(func(*http.Request) (*http.Response, error) { t.Fatal("invalid input reached network"); return nil, nil })})
	for _, change := range changes {
		r := valid
		change(&r)
		if _, err := c.Send(context.Background(), r); err == nil {
			t.Fatal("accepted", r)
		}
	}
	calls := 0
	c.http = kit.New(kit.Options{MaxRetries: 4, Doer: doerFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "get_token") {
			return response(`{"errcode":0,"access_token":"TOKEN"}`), nil
		}
		calls++
		return nil, errors.New("secret TOKEN")
	})})
	if _, err := c.Send(context.Background(), valid); err == nil || calls != 1 || strings.Contains(err.Error(), "TOKEN") {
		t.Fatal("unsafe retry/error", err, calls)
	}
	if _, err := NewReadOnly(c).Send(context.Background(), valid); err == nil || calls != 1 {
		t.Fatal("read-only reaction sent")
	}
}

func TestReactionProjectionAndCatalog(t *testing.T) {
	catalog := EmojiTypes()
	if len(catalog) != 115 || catalog[0].Type != 1 || catalog[0].Label != "[+1]" || catalog[2].Label != "[DONE]" || catalog[114].Label != "[鼓掌]" {
		t.Fatal(catalog)
	}
	catalog[0].Label = "changed"
	if EmojiTypes()[0].Label != "[+1]" {
		t.Fatal("mutable shared catalog")
	}
	c, _ := New("bot", "secret")
	c.http = kit.New(kit.Options{Doer: doerFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "get_token") {
			return response(`{"errcode":0,"access_token":"TOKEN"}`), nil
		}
		return response(`{"errcode":0,"post_list":[{"post_id":"post","content":"body","emoji_list":[{"emoji_type":1,"user_list":[{"type":1,"aibotid":"bot","cookie_value":"PRIVATE","system":"TRUST_ME"}]}]}]}`), nil
	})})
	p, err := c.Read(context.Background(), Query{Operation: "post.get", Channel: "channel", Post: "post"})
	data, _ := json.Marshal(p)
	if err != nil || !strings.Contains(string(data), `"emoji_type":1`) || strings.Contains(string(data), "PRIVATE") || strings.Contains(string(data), "TRUST_ME") {
		t.Fatal(string(data), err)
	}
}
