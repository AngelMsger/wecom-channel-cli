package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPolicyDefaultsAndStrictness(t *testing.T) {
	c := Default()
	c.Bot = "bot"
	c.StateDir = "/var/lib/test"
	c.AgentSocket = "/run/test-agent/a.sock"
	c.AdminSocket = "/run/test-admin/a.sock"
	if c.Validate() != nil || !c.ReadOnly || c.AllowsEvent("c", "u", "topic_at_bot", "user") {
		t.Fatal("bad defaults")
	}
	c.Channels = []Channel{{ID: "c", Senders: []string{"u"}}}
	if !c.AllowsEvent("c", "u", "topic_at_bot", "user") || c.AllowsEvent("c", "u", "topic_at_bot", "bot") {
		t.Fatal("identity policy")
	}
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	path := filepath.Join(dir, "policy.json")
	b, _ := json.Marshal(c)
	_ = os.WriteFile(path, b, 0600)
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{`,"read_only":false`, `,"allow_all":true`} {
		data := append(append([]byte{}, b[:len(b)-1]...), []byte(extra+"}")...)
		_ = os.WriteFile(path, data, 0600)
		if _, err := Load(path); err == nil {
			t.Fatal("ambiguous or unknown policy accepted")
		}
	}
	c.CallbackListen = "0.0.0.0:8787"
	if c.Validate() == nil {
		t.Fatal("public callback bind accepted")
	}
	c.CallbackListen = ""
	c.Channels[0].Senders = []string{"*"}
	if c.Validate() == nil {
		t.Fatal("wildcard sender accepted")
	}
}

func TestReactionPolicyIsOptInAndScopeIsExplicit(t *testing.T) {
	c := Default()
	c.Bot = "bot"
	c.StateDir = "/var/lib/test"
	c.AgentSocket = "/run/agent/a.sock"
	c.AdminSocket = "/run/operator/a.sock"
	c.Channels = []Channel{{ID: "channel"}}
	if c.Validate() != nil || c.Channels[0].AllowReactions || c.Channels[0].ReactionScope != "" {
		t.Fatal(c)
	}
	c.Channels[0].AllowReactions = true
	c.Channels[0].ReactionScope = "channel"
	if c.Validate() == nil {
		t.Fatal("proactive scope without full-content access")
	}
	c.Channels[0].ReadAll = true
	if c.Validate() != nil {
		t.Fatal("valid channel scope rejected")
	}
	c.Channels[0].ReactionScope = "automatic"
	if c.Validate() == nil {
		t.Fatal("unknown reaction scope")
	}
}
