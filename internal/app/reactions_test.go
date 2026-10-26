package app

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReactionCatalogIsOfflineAndUsesListEnvelope(t *testing.T) {
	code, out, stderr := run(t, "--socket", "/does/not/exist.sock", "reaction", "types")
	var result struct {
		Items []struct {
			Type int `json:"emoji_type"`
		}
		Next    any
		HasMore bool `json:"has_more"`
	}
	if code != 0 || stderr != "" || json.Unmarshal([]byte(out), &result) != nil || len(result.Items) != 115 || result.Next != nil || result.HasMore || result.Items[0].Type != 1 {
		t.Fatal(code, out, stderr)
	}
	code, out, stderr = run(t, "reaction", "types", "--format", "ndjson")
	if code != 0 || len(strings.Split(strings.TrimSpace(out), "\n")) != 115 || stderr != "" {
		t.Fatal(code, out, stderr)
	}
	for _, args := range [][]string{{"reaction", "propose", "--text", "👍"}, {"reaction", "propose", "--parent-reply", "r"}, {"reaction", "list", "--cursor", "c"}} {
		code, out, _ = run(t, args...)
		if code == 0 || out != "" {
			t.Fatal("accepted incompatible flags", args)
		}
	}
}
