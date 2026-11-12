package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/inbox"
)

func TestInboxLocalCommands(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "inbox.sqlite")
	code, out, stderr := run(t, "inbox", "status", "--db", path)
	if code == 0 || out != "" || !strings.Contains(stderr, "INBOX_MISSING") {
		t.Fatal(code, out, stderr)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("status created database")
	}
	code, out, stderr = run(t, "inbox", "serve", "--db", path, "--dry-run")
	if code != 0 || !strings.Contains(out, `"dry_run": true`) {
		t.Fatal(code, out, stderr)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("serve dry-run created database")
	}
	for _, listen := range []string{"0.0.0.0:8789", "localhost:8789", "127.0.0.1:0", "[::]:8789"} {
		code, out, stderr = run(t, "inbox", "serve", "--db", path, "--listen", listen, "--dry-run")
		if code != 2 || out != "" {
			t.Fatal(code, out, stderr)
		}
	}
	db, err := inbox.Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	st, page, cfg := relayFixture()
	if err = st.Collect(page, cfg, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !st.Seal(cfg, time.Now()) {
		t.Fatal("not sealed")
	}
	if err = db.Accept(context.Background(), st.Inflight.Batch); err != nil {
		t.Fatal(err)
	}
	db.Close()
	code, out, stderr = run(t, "inbox", "list", "--db", path, "--status", "received", "--limit", "1")
	var result struct {
		Items []inbox.Item `json:"items"`
		Next  any          `json:"next"`
		More  bool         `json:"has_more"`
	}
	if code != 0 || json.Unmarshal([]byte(out), &result) != nil || len(result.Items) != 1 || result.More || result.Next != nil {
		t.Fatal(code, out, stderr)
	}
	code, out, stderr = run(t, "inbox", "get", "--db", path, "--id", result.Items[0].ID)
	if code != 0 || !strings.Contains(out, `"trust": "untrusted"`) || !strings.Contains(out, `"work"`) {
		t.Fatal(code, out, stderr)
	}
}
