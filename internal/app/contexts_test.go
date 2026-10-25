package app

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextLifecycleOffline(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WECOM_CHANNEL_CONTEXT", "")
	t.Setenv("WECOM_CHANNEL_SOCKET", "")
	t.Setenv("WECOM_CHANNEL_BOT_ID", "")
	t.Setenv("WECOM_CHANNEL_ID", "")
	base := []string{"--config-dir", dir, "config", "set-context", "engineering", "--socket", "/run/eng/a.sock", "--bot-id", "bot", "--channel", "eng"}
	code, _, stderr := run(t, append(base, "--dry-run")...)
	if code != 0 {
		t.Fatal(stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "config.yaml")); !os.IsNotExist(err) {
		t.Fatal("dry-run wrote config")
	}
	code, _, stderr = run(t, base...)
	if code != 0 {
		t.Fatal(stderr)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "config.yaml"))
	stat, _ := os.Stat(filepath.Join(dir, "config.yaml"))
	code, _, stderr = run(t, base...)
	if code != 0 {
		t.Fatal(stderr)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "config.yaml"))
	newStat, _ := os.Stat(filepath.Join(dir, "config.yaml"))
	if !bytes.Equal(before, after) || !stat.ModTime().Equal(newStat.ModTime()) {
		t.Fatal("identical setup rewrote context")
	}
	conflict := append(append([]string{}, base...), "--channel", "different")
	code, _, stderr = run(t, conflict...)
	if code != 11 || !strings.Contains(stderr, "CONFIG_CONTEXT_CONFLICT") {
		t.Fatal(code, stderr)
	}
	code, _, stderr = run(t, "--config-dir", dir, "config", "set-context", "security", "--socket", "/run/sec/a.sock", "--bot-id", "bot", "--channel", "sec")
	if code != 0 {
		t.Fatal(stderr)
	}
	code, out, stderr := run(t, "--config-dir", dir, "config", "get-contexts")
	if code != 0 {
		t.Fatal(stderr)
	}
	var rows struct {
		Items []struct {
			Name    string `json:"name"`
			Current bool   `json:"current"`
		} `json:"items"`
	}
	_ = json.Unmarshal([]byte(out), &rows)
	if len(rows.Items) != 2 || !rows.Items[0].Current || rows.Items[1].Current {
		t.Fatal("wrong activation", out)
	}
	code, _, stderr = run(t, "--config-dir", dir, "config", "use-context", "security")
	if code != 0 {
		t.Fatal(stderr)
	}
	code, _, stderr = run(t, "--config-dir", dir, "config", "delete-context", "engineering")
	if code != 5 {
		t.Fatal(code, stderr)
	}
	code, _, stderr = run(t, "--config-dir", dir, "--use-context", "security", "post", "list", "--channel", "eng")
	if code != 5 || !strings.Contains(stderr, "CONTEXT_MISMATCH") {
		t.Fatal(code, stderr)
	}
	code, _, stderr = run(t, "--config-dir", dir, "--use-context", "missing", "post", "list")
	if code != 3 || !strings.Contains(stderr, "UNKNOWN_CONTEXT") {
		t.Fatal(code, stderr)
	}
}
