package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDeploymentIsolationValidation(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	path := filepath.Join(dir, "deployment.json")
	makeContext := func(name, channel string) map[string]any {
		return map[string]any{"name": name, "state_dir": "/var/lib/wcc/" + name, "agent_socket": "/run/" + name + "-agent/a.sock", "admin_socket": "/run/" + name + "-operator/a.sock", "channels": []Channel{{ID: channel, Senders: []string{"alice"}}}}
	}
	a, b := makeContext("engineering", "eng"), makeContext("security", "sec")
	write := func() {
		data, _ := json.Marshal(map[string]any{"version": 2, "bot_id": "bot", "callback_listen": "127.0.0.1:8787", "contexts": []any{a, b}})
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	d, err := LoadDeployment(path)
	if err != nil || len(d.Contexts) != 2 || !d.Contexts[0].ReadOnly || d.Contexts[1].Bot != "bot" {
		t.Fatal(d, err)
	}
	b["channels"] = []Channel{{ID: "eng"}}
	write()
	if _, err = LoadDeployment(path); err == nil {
		t.Fatal("duplicate channel accepted")
	}
	b = makeContext("security", "sec")
	b["agent_socket"] = a["agent_socket"]
	write()
	if _, err = LoadDeployment(path); err == nil {
		t.Fatal("shared agent socket accepted")
	}
	b = makeContext("security", "sec")
	b["bot_id"] = "other-bot"
	write()
	if _, err = LoadDeployment(path); err == nil {
		t.Fatal("mixed bot deployment accepted")
	}
}
