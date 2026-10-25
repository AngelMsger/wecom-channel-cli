package app

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, err bytes.Buffer
	code := Execute(args, strings.NewReader(""), &out, &err)
	return code, out.String(), err.String()
}

func TestOfflineCLIContracts(t *testing.T) {
	for _, args := range [][]string{{"auth", "guide"}, {"version"}, {"config", "example"}, {"skill", "status", "--dir", t.TempDir()}} {
		code, out, err := run(t, args...)
		if code != 0 || !json.Valid([]byte(out)) || err != "" {
			t.Fatalf("%v: %d %s %s", args, code, out, err)
		}
	}
	for _, args := range [][]string{{"nonsense"}, {"post", "nonsense"}, {"event", "nonsense"}, {"post", "list", "unexpected"}, {"post", "list", "--format", "table"}, {"approval", "approve", "--admin"}, {"post", "propose", "--text", "x", "--stdin"}} {
		code, out, err := run(t, args...)
		if code == 0 || out != "" || !json.Valid([]byte(err)) {
			t.Fatalf("%v: %d %s %s", args, code, out, err)
		}
	}
}

func TestDryBrokerPreviewAndSkillLifecycle(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	cfg := ExampleConfig()
	cfg.StateDir = filepath.Join(dir, "does-not-exist")
	b, _ := json.Marshal(cfg)
	path := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	code, out, err := run(t, "broker", "serve", "--config", path, "--dry-run")
	if code != 0 || !strings.Contains(out, `"dry_run": true`) {
		t.Fatal(code, out, err)
	}
	if _, err := os.Stat(cfg.StateDir); !os.IsNotExist(err) {
		t.Fatal("dry-run created state")
	}
	code, _, err = run(t, "skill", "install", "--dir", dir, "--dry-run")
	if code != 0 {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wecom-channel")); !os.IsNotExist(err) {
		t.Fatal("dry-run installed Skill")
	}
	code, _, err = run(t, "skill", "install", "--dir", dir)
	if code != 0 {
		t.Fatal(err)
	}
	t.Setenv("WECOM_CHANNEL_SKILL_LOADED", "0.0.1")
	code, out, err = run(t, "skill", "status", "--dir", dir)
	if code != 0 || !strings.Contains(out, `"loaded_status": "outdated"`) || !strings.Contains(out, `"alignment": "current"`) {
		t.Fatal(code, out, err)
	}
	t.Setenv("WECOM_CHANNEL_CLI_READ_ONLY", "true")
	code, out, err = run(t, "post", "propose", "--channel", "c", "--key", "k", "--text", "x")
	if code != 5 || out != "" || !strings.Contains(err, "READONLY_BLOCKED") {
		t.Fatal(code, out, err)
	}
}

func TestWindowSemantics(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	f, to, err := window("1h", "", "", "", now)
	if err != nil || to-f != 3600 {
		t.Fatal(f, to, err)
	}
	for _, values := range [][4]string{{"1h", "2026-10-25T00:00:00Z", "", ""}, {"1h", "", "", "cursor"}, {"", "", "2026-10-25T10:00:00Z", ""}, {"", "2026-10-25T00:00:00Z", "", "cursor"}, {"", "2026-10-25", "", ""}} {
		if _, _, err := window(values[0], values[1], values[2], values[3], now); err == nil {
			t.Fatal("invalid window accepted", values)
		}
	}
}
