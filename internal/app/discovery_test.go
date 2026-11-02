package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type discoveryReader func(context.Context, apiclient.Query) (apiclient.Page, error)

func (f discoveryReader) Read(ctx context.Context, q apiclient.Query) (apiclient.Page, error) {
	return f(ctx, q)
}

func TestDiscoveryBeforeConfiguration(t *testing.T) {
	t.Setenv("WECOM_CHANNEL_BOT_ID", "env-bot")
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "synthetic-secret")
	t.Setenv("WECOM_CHANNEL_CONTEXT", "unconfigured")
	t.Setenv("WECOM_CHANNEL_CLI_READ_ONLY", "true")
	dir := t.TempDir()
	t.Setenv("WECOM_CHANNEL_CONFIG", dir)
	for _, format := range []string{"json", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			calls := 0
			s := &appState{format: format, timeout: time.Second}
			factory := func(bot, secret string) (apiclient.Reader, error) {
				if bot != "chosen-bot" || secret != "synthetic-secret" {
					t.Fatal("wrong credential selection")
				}
				return discoveryReader(func(ctx context.Context, q apiclient.Query) (apiclient.Page, error) {
					calls++
					if _, ok := ctx.Deadline(); !ok || q.Operation != "channel.list" || q.Channel != "" || q.ChannelIDs != nil || q.Limit != 1 {
						t.Fatalf("discovery is not an independent bounded read: %+v", q)
					}
					p := apiclient.Page{Items: []apiclient.Record{apiclient.Untrusted(apiclient.Topic{ID: "eng", Name: "Engineering"}, "eng", "", "")}}
					if q.Cursor == "" {
						p.Next, p.HasMore = "second", true
					} else if q.Cursor != "second" {
						t.Fatal("wrong continuation")
					}
					return p, nil
				}), nil
			}
			cursor := ""
			for i := 0; i < 2; i++ {
				cmd := s.discoveryCommand(factory)
				var out, stderr bytes.Buffer
				cmd.SilenceUsage, cmd.SilenceErrors = true, true
				cmd.SetOut(&out)
				cmd.SetErr(&stderr)
				cmd.SetArgs([]string{"--bot-id", "chosen-bot", "--limit", "1", "--cursor", cursor})
				if err := cmd.Execute(); err != nil {
					t.Fatal(err)
				}
				if !json.Valid(out.Bytes()) || strings.Contains(out.String()+stderr.String(), "synthetic-secret") {
					t.Fatal("invalid data channel or leaked credential")
				}
				var row apiclient.Record
				if format == "json" {
					var page struct {
						Items []apiclient.Record `json:"items"`
						Next  string             `json:"next"`
					}
					if err := json.Unmarshal(out.Bytes(), &page); err != nil || len(page.Items) != 1 {
						t.Fatal(out.String(), err)
					}
					row, cursor = page.Items[0], page.Next
				} else {
					if err := json.Unmarshal(out.Bytes(), &row); err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						var notice struct {
							Notice struct {
								Pagination struct {
									Next string `json:"next"`
								} `json:"pagination"`
							} `json:"_notice"`
						}
						if err := json.NewDecoder(&stderr).Decode(&notice); err != nil {
							t.Fatal(err)
						}
						cursor = notice.Notice.Pagination.Next
					}
				}
				if row.Trust != "untrusted" || row.Warning != apiclient.Warning || row.Source.Channel != "eng" || row.Data.(map[string]any)["topic_name"] != "Engineering" {
					t.Fatal("lost channel identity or trust envelope")
				}
				if i == 0 && cursor != "second" {
					t.Fatal("lost continuation")
				}
			}
			if calls != 2 {
				t.Fatal("unexpected discovery traversal")
			}
		})
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("discovery created configuration")
	}
}

func TestDiscoveryValidationAndDryRun(t *testing.T) {
	t.Setenv("WECOM_CHANNEL_BOT_ID", "env-bot")
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "")
	for _, args := range [][]string{{"--dry-run"}, {"--bot-id", ""}, {"--limit", "0"}, {"--limit", "51"}, {"--cursor", strings.Repeat("x", 257)}} {
		s := &appState{format: "json", timeout: time.Second}
		cmd := s.discoveryCommand(func(string, string) (apiclient.Reader, error) {
			t.Fatal("validation or dry-run acquired credentials")
			return nil, nil
		})
		var out bytes.Buffer
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs(args)
		err := cmd.Execute()
		if args[0] == "--dry-run" {
			if err != nil || !strings.Contains(out.String(), "env-bot") || !strings.Contains(out.String(), "get_topic_list") {
				t.Fatal(out.String(), err)
			}
		} else if err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
	code, out, stderr := run(t, "broker", "discover")
	if code != 4 || out != "" || !strings.Contains(stderr, "AUTH_MISSING") {
		t.Fatal(code, out, stderr)
	}
}

func TestDiscoveryEmptyErrorAndTimeout(t *testing.T) {
	t.Setenv("WECOM_CHANNEL_BOT_ID", "bot")
	for _, mode := range []string{"empty", "auth", "timeout"} {
		s := &appState{format: "json", timeout: 10 * time.Millisecond}
		wantErr := e.New(e.CategoryAuth, "AUTH_REJECTED", "Synthetic authentication failure.")
		cmd := s.discoveryCommand(func(string, string) (apiclient.Reader, error) {
			return discoveryReader(func(ctx context.Context, _ apiclient.Query) (apiclient.Page, error) {
				switch mode {
				case "auth":
					return apiclient.Page{}, wantErr
				case "timeout":
					<-ctx.Done()
					return apiclient.Page{}, ctx.Err()
				default:
					return apiclient.Page{Items: []apiclient.Record{}}, nil
				}
			}), nil
		})
		var out, stderr bytes.Buffer
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetOut(&out)
		cmd.SetErr(&stderr)
		cmd.SetArgs([]string{})
		err := cmd.Execute()
		if mode == "empty" {
			if err != nil || !strings.Contains(out.String(), `"items": []`) || !strings.Contains(stderr.String(), "joined") {
				t.Fatal(out.String(), stderr.String(), err)
			}
		} else if out.Len() != 0 || err == nil || mode == "auth" && err != wantErr || mode == "timeout" && err != context.DeadlineExceeded {
			t.Fatal(out.String(), err)
		}
	}
}

func TestDiscoverySetupRecovery(t *testing.T) {
	t.Setenv("WECOM_CHANNEL_ID", "")
	t.Setenv("WECOM_CHANNEL_CONTEXT", "")
	dir := t.TempDir()
	code, out, stderr := run(t, "--config-dir", dir, "config", "set-context", "engineering", "--socket", "/run/eng/agent.sock", "--bot-id", "bot")
	if code != 3 || out != "" || !strings.Contains(stderr, "CHANNEL_REQUIRED") || !strings.Contains(stderr, "broker discover") {
		t.Fatal(code, out, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "config.yaml")); !os.IsNotExist(err) {
		t.Fatal("failed setup saved a context")
	}
	code, out, stderr = run(t, "config", "example", "--bot-id", "bot", "--channel", "discovered")
	var cfg config.Config
	if code != 0 || json.Unmarshal([]byte(out), &cfg) != nil || cfg.Bot != "bot" || cfg.Channels[0].ID != "discovered" || !cfg.ReadOnly || cfg.Validate() != nil {
		t.Fatal(code, out, stderr)
	}
	code, out, stderr = run(t, "auth", "guide")
	var guide struct {
		Next []string `json:"next_steps"`
	}
	if code != 0 || json.Unmarshal([]byte(out), &guide) != nil || !strings.Contains(guide.Next[0], "broker discover") {
		t.Fatal(out, stderr)
	}
}

func TestCLIProcessDiscoveryBootstrap(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the built executable")
	}
	dir := t.TempDir()
	for _, dry := range []bool{true, false} {
		args := []string{"broker", "discover"}
		if dry {
			args = append(args, "--dry-run")
		}
		cmd := exec.Command(binary, args...)
		cmd.Env = []string{"WECOM_CHANNEL_BOT_ID=bot", "WECOM_CHANNEL_CONFIG=" + dir, "WECOM_CHANNEL_CONTEXT=not-created"}
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		err := cmd.Run()
		if dry {
			if err != nil || !json.Valid(out.Bytes()) || stderr.Len() != 0 {
				t.Fatal(out.String(), stderr.String(), err)
			}
		} else if err == nil || out.Len() != 0 || !strings.Contains(stderr.String(), "AUTH_MISSING") {
			t.Fatal(out.String(), stderr.String(), err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("bootstrap wrote local state")
	}
}
