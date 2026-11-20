package apiclient_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestApprovalFailureSurvivesRestart(t *testing.T) {
	const authError = `{"errcode":%d,"errmsg":"private-upstream-text"}`
	expired, rejected, success := 853004, 853000, 0
	const goodToken = `{"errcode":0,"access_token":"private-token"}`
	for _, tc := range []struct {
		name, token, business string
		writes                int
		want                  apiclient.RequestFailure
	}{
		{"token rejected", fmt.Sprintf(authError, 853000), "", 0, apiclient.RequestFailure{Phase: apiclient.PhaseTokenBootstrap, Code: "AUTH_REJECTED", UpstreamErrcode: &rejected}},
		{"business rejected", goodToken, fmt.Sprintf(authError, 853000), 1, apiclient.RequestFailure{Phase: apiclient.PhaseBusinessRequest, Code: "AUTH_REJECTED", UpstreamErrcode: &rejected}},
		{"business token expired", goodToken, fmt.Sprintf(authError, 853004), 1, apiclient.RequestFailure{Phase: apiclient.PhaseBusinessRequest, Code: "AUTH_REJECTED", UpstreamErrcode: &expired}},
		{"token response lost", "", "", 0, apiclient.RequestFailure{Phase: apiclient.PhaseTokenBootstrap, Code: "REQUEST_UNCERTAIN"}},
		{"response lost", goodToken, "", 1, apiclient.RequestFailure{Phase: apiclient.PhaseBusinessRequest, Code: "REQUEST_UNCERTAIN"}},
		{"missing post id", goodToken, `{"errcode":0}`, 1, apiclient.RequestFailure{Phase: apiclient.PhaseBusinessRequest, Code: "BAD_WRITE_RESULT", UpstreamErrcode: &success}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokens, writes := 0, 0
			client := apiclient.NewTestClient(t, func(r *http.Request) (*http.Response, error) {
				var body string
				switch r.URL.Path {
				case "/cgi-bin/aibot/get_token":
					tokens++
					body = tc.token
				case "/cgi-bin/aibot/topic/send_post":
					writes++
					body = tc.business
				default:
					t.Fatalf("unexpected endpoint %q", r.URL.Path)
				}
				if body == "" {
					return nil, io.ErrUnexpectedEOF
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			cfg := config.Default()
			cfg.Bot = "test-bot"
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			cfg.StateDir = dir
			if err := os.Chmod(cfg.StateDir, 0700); err != nil {
				t.Fatal(err)
			}
			cfg.ReadOnly = false
			cfg.Channels = []config.Channel{{ID: "channel", AllowPosts: true}}
			open := func() *broker.Broker {
				t.Helper()
				store, err := state.Open(cfg.StateDir, cfg.Bot)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				return broker.New(cfg, store, client, nil)
			}
			b := open()
			w := apiclient.WriteRequest{Kind: "post", Channel: "channel", Content: "private-content", Key: "once"}
			out, err := b.Handle(t.Context(), broker.Request{Operation: "proposal.create", Write: &w}, false)
			if err != nil {
				t.Fatal(err)
			}
			p := out.Item.Data.(state.Proposal)
			approval := broker.Request{Operation: "approval.approve", ID: p.ID, Digest: p.Digest, Yes: true}
			_, err = b.Handle(t.Context(), approval, true)
			if err == nil {
				t.Fatal("approval unexpectedly succeeded")
			}
			ce := e.Safe(err)
			if ce.Code != "WRITE_UNCERTAIN" || ce.Retryable || strings.Contains(ce.Message, "never sent") != (tc.writes == 0) {
				t.Fatalf("unexpected approval error: %+v", ce.Payload())
			}
			details := ce.Details.(map[string]any)
			if failure, ok := details["failure"].(*apiclient.RequestFailure); !ok || !reflect.DeepEqual(failure, &tc.want) || details["state_persisted"] != true {
				t.Fatalf("approval details: %+v", details)
			}
			wire, err := json.Marshal(ce.Payload())
			if err != nil || strings.Contains(string(wire), "private-") {
				t.Fatalf("unsafe approval payload: %s (%v)", wire, err)
			}
			if err := b.Store.Close(); err != nil {
				t.Fatal(err)
			}
			b = open()
			out, err = b.Handle(t.Context(), broker.Request{Operation: "proposal.get", ID: p.ID}, false)
			if err != nil {
				t.Fatal(err)
			}
			stored := out.Item.Data.(state.Proposal)
			if !reflect.DeepEqual(stored.Failure, &tc.want) || stored.Status != "uncertain" || stored.ErrorCode != tc.want.Code {
				t.Fatalf("unexpected stored failure: %+v", stored.Failure)
			}
			_, err = b.Handle(t.Context(), approval, true)
			if err == nil || e.Safe(err).Code != "PROPOSAL_TERMINAL" || writes != tc.writes || tokens != 1 {
				t.Fatalf("second approval=%v, writes=%d, token requests=%d", err, writes, tokens)
			}
		})
	}
}
