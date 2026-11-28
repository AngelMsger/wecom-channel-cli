package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProfileLoadSaveAndValidation(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}

	// Absent profile returns exists=false with no error.
	if _, exists, err := LoadProfile(dir, false); err != nil || exists {
		t.Fatalf("expected absent profile, got exists=%v err=%v", exists, err)
	}

	p := DefaultProfile("bot-123")
	p.Network.Proxy = "socks5h://127.0.0.1:1080"
	p.Channels = []ProfileChannel{
		{ID: "ch-1", Name: "Engineering", Selected: true},
		{ID: "ch-2", Name: "研发频道", Selected: false},
	}
	if err := SaveProfile(dir, p, false); err != nil {
		t.Fatalf("SaveProfile failed: %v", err)
	}

	loaded, exists, err := LoadProfile(dir, false)
	if err != nil || !exists {
		t.Fatalf("LoadProfile failed: exists=%v err=%v", exists, err)
	}
	if loaded.Bot != "bot-123" || loaded.Network.Proxy != "socks5h://127.0.0.1:1080" || len(loaded.Channels) != 2 {
		t.Fatalf("unexpected loaded profile: %+v", loaded)
	}

	tcfg := loaded.TransportConfig()
	if tcfg.Timeout != 45*time.Second || tcfg.ReadMaxRetries != 3 || tcfg.ProxyURL != "socks5h://127.0.0.1:1080" {
		t.Fatalf("unexpected transport config: %+v", tcfg)
	}

	// Owner-edited 0644 profile.yaml is accepted (no secrets in profile.yaml),
	// while group/world-writable 0666 is rejected.
	path := ProfilePath(dir)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := LoadProfile(dir, false); err != nil || !exists {
		t.Fatalf("expected 0644 profile.yaml to load, got exists=%v err=%v", exists, err)
	}
	if err := os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadProfile(dir, false); err == nil {
		t.Fatal("expected world-writable profile.yaml to fail")
	}

	// Reject invalid settings.
	bad := DefaultProfile("bot-123")
	bad.Network.Proxy = "ftp://example.com"
	if err := bad.Validate(); err == nil {
		t.Fatal("expected ftp proxy to fail validation")
	}
	bad = DefaultProfile("bot-123")
	bad.Polling.Concurrency = 99
	if err := bad.Validate(); err == nil {
		t.Fatal("expected out-of-range concurrency to fail validation")
	}
	bad = DefaultProfile("bot-123")
	bad.Channels = []ProfileChannel{{ID: "dup", Name: "A"}, {ID: "dup", Name: "B"}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected duplicate channel IDs to fail validation")
	}
}

func TestChannelContextNameAndSocketPathLimit(t *testing.T) {
	used := map[string]bool{}
	c1 := ChannelContextName("bot-1", ProfileChannel{ID: "id-1", Name: "Engineering Ops"}, used)
	c2 := ChannelContextName("bot-1", ProfileChannel{ID: "id-2", Name: "Engineering Ops"}, used)
	c3 := ChannelContextName("bot-1", ProfileChannel{ID: "id-3", Name: "研发通知"}, used)
	c4 := ChannelContextName("bot-1", ProfileChannel{ID: "id-4", Name: "Ignored", Context: "custom-ctx"}, used)

	if c1 != "engineering-ops" {
		t.Fatalf("expected engineering-ops, got %q", c1)
	}
	if c2 == c1 || !strings.HasPrefix(c2, "engineering-ops-") || !ValidContextName(c2) {
		t.Fatalf("expected disambiguated valid context name, got %q", c2)
	}
	// Both the plain name and its first disambiguation can be user-owned.
	next := ChannelContextName("bot-1", ProfileChannel{ID: "id-2", Name: "Engineering Ops"}, used)
	if next == c1 || next == c2 || !ValidContextName(next) {
		t.Fatalf("generated an occupied or invalid context name %q", next)
	}
	if !strings.HasPrefix(c3, "ch-") || !ValidContextName(c3) {
		t.Fatalf("expected stable hash fallback for non-ASCII name, got %q", c3)
	}
	if c4 != "custom-ctx" {
		t.Fatalf("expected explicit custom-ctx, got %q", c4)
	}

	shortBase := "/tmp/wcc-short"
	slug := ChannelStateSlug("bot-1", "id-1")
	a, o, err := ChannelSocketPaths(shortBase, slug)
	if err != nil {
		t.Fatalf("short socket path failed: %v", err)
	}
	if len(a) > 100 || len(o) > 100 || !strings.HasPrefix(a, shortBase) {
		t.Fatalf("unexpected short socket paths: agent=%q admin=%q", a, o)
	}

	longBase := filepath.Join("/var/folders/very/long/macos/runner/workspace/directory/that/would/otherwise/exceed/sockaddr/limit", strings.Repeat("segment/", 5))
	a2, o2, err := ChannelSocketPaths(longBase, slug)
	if err != nil {
		t.Fatalf("long socket path fallback failed: %v", err)
	}
	if len(a2) > 100 || len(o2) > 100 || a2 == o2 {
		t.Fatalf("expected fallback socket paths <= 100 bytes and distinct, got agent=%q (%d) admin=%q (%d)", a2, len(a2), o2, len(o2))
	}
}
