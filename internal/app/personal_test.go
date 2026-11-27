package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/angelmsger/agent-cli-kit/credstore"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type memoryCredential struct{ secret string }

func (m *memoryCredential) Load(string) (string, error) {
	if m.secret == "" {
		return "", credstore.ErrSecretNotFound
	}
	return m.secret, nil
}
func (m *memoryCredential) Save(_, secret string) (string, error) {
	m.secret = secret
	return "test", nil
}

type personalTestAPI struct {
	apiclient.Client // Any call outside Read fails the test.
	read             discoveryReader
}

func (f personalTestAPI) Read(ctx context.Context, q apiclient.Query) (apiclient.Page, error) {
	return f.read(ctx, q)
}

func TestPersonalConnectSelectRecoverAndResume(t *testing.T) {
	t.Setenv("WECOM_CHANNEL_SOCKET", "")
	t.Setenv("WECOM_CHANNEL_CONTEXT", "")
	t.Setenv("WECOM_CHANNEL_CLI_READ_ONLY", "")
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "fixture-secret")
	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(tmp, "fresh-user", "client")
	synctest.Test(t, func(t *testing.T) {
		var reads int
		partial, renamed := false, false
		creds := &memoryCredential{}
		topic := func(id, name string) apiclient.Record {
			return apiclient.Untrusted(apiclient.Topic{ID: id, Name: name}, id, "", "")
		}
		api := personalTestAPI{read: func(_ context.Context, q apiclient.Query) (apiclient.Page, error) {
			switch q.Operation {
			case "channel.list":
				if q.Cursor == "" {
					return apiclient.Page{Items: []apiclient.Record{topic("archive", "Archive"), topic("other", "Archive")}, HasMore: true, Next: "next"}, nil
				}
				if partial {
					return apiclient.Page{}, e.New(e.CategoryNetwork, "REQUEST_UNCERTAIN", "fixture outage")
				}
				name := "研发动态"
				if renamed {
					name = "研发周报"
				}
				return apiclient.Page{Items: []apiclient.Record{topic("engineering", name)}}, nil
			case "post.list":
				if q.Channel != "engineering" {
					return apiclient.Page{}, errors.New("scanned an unselected channel")
				}
				reads++
				if reads == 2 {
					return apiclient.Page{}, e.New(e.CategoryNetwork, "REQUEST_UNCERTAIN", "fixture outage")
				}
				m := apiclient.Message{PostID: "thread", Created: 100, Creator: &apiclient.User{UserID: "alice"}}
				return apiclient.Page{Items: []apiclient.Record{apiclient.Untrusted(m, q.Channel, "thread", "")}}, nil
			case "reply.list":
				if reads < 3 {
					return apiclient.Page{}, nil
				}
				m := apiclient.Message{PostID: "thread", ReplyID: "new-reply", Created: 200, Creator: &apiclient.User{UserID: "bob"}}
				return apiclient.Page{Items: []apiclient.Record{apiclient.Untrusted(m, q.Channel, "thread", "")}}, nil
			}
			return apiclient.Page{}, errors.New("unexpected API call")
		}}
		runtime := personalRuntime{credentials: func(string) personalCredentials { return creds }, client: func(bot, secret string) (apiclient.Client, error) {
			if bot != "bot" || secret != "fixture-secret" {
				return nil, errors.New("invalid credentials")
			}
			return api, nil
		}}
		run := func(args ...string) (string, string, error) {
			root := newRootCmd(runtime)
			var out, diag bytes.Buffer
			root.SetOut(&out)
			root.SetErr(&diag)
			root.SetIn(strings.NewReader(""))
			root.SetArgs(append([]string{"--config-dir", dir}, args...))
			err := root.ExecuteContext(context.Background())
			if strings.Contains(out.String()+diag.String(), "fixture-secret") {
				t.Fatal("credential in output")
			}
			return out.String(), diag.String(), err
		}
		if _, _, err := run("connect", "--bot-id", "bot", "--dry-run"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("preview created user configuration")
		}
		out, _, err := run("connect", "--bot-id", "bot")
		if err != nil || !strings.Contains(out, "研发动态") || !strings.Contains(out, "untrusted") || creds.secret == "" {
			t.Fatal(out, err)
		}
		t.Setenv("WECOM_CHANNEL_BOT_SECRET", "")
		if _, _, err := run("listen", "--channel", "Archive"); e.Safe(err).Code != "CHANNEL_NAME_AMBIGUOUS" {
			t.Fatal(err)
		}
		out, _, err = run("listen", "--pick", "1", "--dry-run")
		if err != nil || !strings.Contains(out, `"selected_count": 1`) {
			t.Fatal(out, err)
		}
		out, diag, err := run("listen", "--channel", "研发动态", "--duration", "95s")
		var event apiclient.Record
		if err != nil || json.Unmarshal([]byte(out), &event) != nil || event.Trust != "untrusted" || event.Source.Channel != "engineering" || !strings.Contains(out, "new-reply") || !strings.Contains(diag, "retry_wait") || !strings.Contains(diag, "recovered") {
			t.Fatal(out, diag, err)
		}
		path := personalPath(filepath.Join(dir, "personal"))
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		partial = true
		if _, _, err := run("connect"); e.Safe(err).Code != "REQUEST_UNCERTAIN" {
			t.Fatal(err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) || bytes.Contains(after, []byte("fixture-secret")) {
			t.Fatal("incomplete discovery or secret changed catalog", err)
		}
		partial = false
		renamed = true
		if _, _, err := run("connect"); err != nil {
			t.Fatal(err)
		}
		p, err := readPersonal(filepath.Dir(path))
		if err != nil || len(p.selected()) != 1 || p.selected()[0].Name != "研发周报" {
			t.Fatal(p, err)
		}
		out, _, err = run("listen", "--duration", "40s")
		if err != nil || out != "" {
			t.Fatal("duplicate event after restart", out, err)
		}
		if _, _, err := run("listen", "--none"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := run("listen"); e.Safe(err).Code != "NO_CHANNELS_SELECTED" {
			t.Fatal(err)
		}
	})
}

func TestPersonalScanStopsOnPermissionAndExhaustion(t *testing.T) {
	for _, tc := range []struct {
		code string
		want int
	}{{"POLICY_DENIED", 1}, {"REQUEST_UNCERTAIN", 21}} {
		t.Run(tc.code, func(t *testing.T) {
			calls := 0
			_, err := completePersonalScan(context.Background(), broker.ScanRequest{WholeChannel: true, ResumePoll: true, Limit: 100}, time.Second, time.Second, func(context.Context, broker.ScanRequest) (broker.ScanPage, error) {
				calls++
				return broker.ScanPage{}, e.New(e.CategoryNetwork, tc.code, "fixture")
			}, func(context.Context, time.Duration) error { return nil }, func(string, map[string]any) {})
			if calls != tc.want || e.Safe(err).Code != tc.code {
				t.Fatal(calls, err)
			}
		})
	}
}

func TestCLIProcessPersonalPreviewWithoutService(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(dir, "user-config")
	run := func(env []string, args ...string) ([]byte, []byte, error) {
		cmd := exec.Command(binary, append([]string{"--config-dir", configDir}, args...)...)
		cmd.Env = append([]string{"WECOM_CHANNEL_NO_SKILL_HINT=1"}, env...)
		var out, diag bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diag
		err := cmd.Run()
		return out.Bytes(), diag.Bytes(), err
	}
	if out, _, err := run(nil, "connect", "--bot-id", "example", "--dry-run"); err != nil || !bytes.Contains(out, []byte("channel.list")) {
		t.Fatal(string(out), err)
	}
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		t.Fatal("preview installed local state")
	}
	if out, diag, err := run([]string{"WECOM_CHANNEL_SOCKET=/example/managed.sock"}, "connect", "--bot-id", "example"); err == nil || len(out) != 0 || !bytes.Contains(diag, []byte("PERSONAL_CONNECTION_CONFLICT")) {
		t.Fatal(string(out), string(diag), err)
	}
	if out, diag, err := run([]string{"WECOM_CHANNEL_CLI_READ_ONLY=true"}, "connect", "--bot-id", "example"); err == nil || len(out) != 0 || !bytes.Contains(diag, []byte("READONLY_BLOCKED")) {
		t.Fatal(string(out), string(diag), err)
	}
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		t.Fatal("blocked setup installed local state")
	}
	p := personalProfile{Version: 1, Bot: "example", Channels: []personalChannel{{ID: "one", Name: "工程"}}}
	personal := filepath.Join(configDir, "personal")
	if err := ensurePersonalDir(personal); err != nil {
		t.Fatal(err)
	}
	raw, err := p.encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(personalPath(personal), raw, 0600); err != nil {
		t.Fatal(err)
	}
	out, _, err := run(nil, "listen", "--channel", "工程", "--dry-run")
	if err != nil || !json.Valid(out) || !bytes.Contains(out, []byte(`"selected_count": 1`)) {
		t.Fatal(string(out), err)
	}
	current, err := os.ReadFile(personalPath(personal))
	if err != nil || !bytes.Equal(raw, current) {
		t.Fatal("preview changed the saved selection", err)
	}
}

func TestPersonalLargeBaselineResumesAfterFailedReplyPage(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		var posts, replies, recoveries int
		failed := false
		api := personalTestAPI{read: func(_ context.Context, q apiclient.Query) (apiclient.Page, error) {
			switch q.Operation {
			case "post.list":
				posts++
				offset := 0
				page := apiclient.Page{Next: "next", HasMore: true}
				if q.Cursor == "next" {
					offset = 50
					page = apiclient.Page{}
				}
				for i := offset; i < offset+50; i++ {
					m := apiclient.Message{PostID: fmt.Sprintf("p-%03d", i), Created: 100, Creator: &apiclient.User{UserID: "user"}}
					page.Items = append(page.Items, apiclient.Untrusted(m, q.Channel, m.PostID, ""))
				}
				return page, nil
			case "reply.list":
				replies++
				if !failed && q.Post == "p-003" {
					failed = true
					return apiclient.Page{}, e.New(e.CategoryNetwork, "RESPONSE_INTERRUPTED", "fixture interruption")
				}
				return apiclient.Page{}, nil
			}
			return apiclient.Page{}, errors.New("unexpected API call")
		}}
		ready := false
		err := listenPersonalChannel(ctx, dir, "bot", "engineering", api, 2*time.Second, func(apiclient.Record) error {
			return errors.New("baseline emitted historical data")
		}, func(status string, _ map[string]any) {
			if status == "recovered" {
				recoveries++
			}
			if status == "ready" {
				ready = true
				cancel()
			}
		})
		if err != nil || !ready || recoveries != 1 || posts != 2 || replies != 101 {
			t.Fatalf("err=%v ready=%t recoveries=%d post reads=%d reply reads=%d", err, ready, recoveries, posts, replies)
		}
	})
}
