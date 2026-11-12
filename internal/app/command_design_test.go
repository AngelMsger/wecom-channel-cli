package app

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestCanonicalCommandResponsibilities(t *testing.T) {
	root := NewRootCmd()
	for group, want := range map[string][]string{
		"event":    {"context", "get", "list", "watch"},
		"relay":    {"claim", "consume", "resolve", "run", "status"},
		"policy":   {"example", "validate"},
		"config":   {"delete-context", "get-contexts", "path", "set-context", "use-context"},
		"proposal": {"approve", "get", "list", "reject", "resolve"},
		"auth":     {"check", "guide"},
		"search":   {"get", "start"},
	} {
		cmd, _, err := root.Find([]string{group})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, c := range cmd.Commands() {
			if !c.Hidden {
				got = append(got, c.Name())
			}
		}
		slices.Sort(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %v, want %v", group, got, want)
		}
	}
	// Every group rejects unknown children, including compatibility groups.
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if c.HasSubCommands() {
			path := strings.Fields(c.CommandPath())[1:]
			code, out, stderr := run(t, append(path, "not-a-command")...)
			if code != 2 || out != "" || !json.Valid([]byte(stderr)) {
				t.Fatalf("%s: %d %q %q", c.CommandPath(), code, out, stderr)
			}
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(root)
}

func TestPublishedCompatibilityKeepsDataAndExplainsMigration(t *testing.T) {
	code, canonical, stderr := run(t, "policy", "example", "--bot-id", "bot", "--channel", "channel")
	if code != 0 || stderr != "" {
		t.Fatal(code, stderr)
	}
	code, legacy, stderr := run(t, "config", "example", "--bot-id", "bot", "--channel", "channel")
	if code != 0 || legacy != canonical || !json.Valid([]byte(stderr)) || !strings.Contains(stderr, `"replacement":"wecom-channel-cli policy example"`) {
		t.Fatal(code, legacy, stderr)
	}
	for _, args := range [][]string{{"--help"}, {"config", "--help"}, {"auth", "--help"}, {"search", "--help"}} {
		code, out, stderr := run(t, args...)
		if code != 0 || stderr != "" {
			t.Fatal(code, stderr)
		}
		for _, legacy := range []string{"Compatibility alias", "Compatibility group"} {
			if strings.Contains(out, legacy) {
				t.Fatal("compatibility paths leaked into discovery", out)
			}
		}
	}
	for _, group := range []string{"", "config", "auth", "search"} {
		args := []string{"__complete"}
		if group != "" {
			args = append(args, group)
		}
		code, out, _ := run(t, append(args, "")...)
		if code != 0 || strings.Contains(out, "Compatibility alias") || strings.Contains(out, "Compatibility group") {
			t.Fatal("compatibility paths leaked into completion", group, code, out)
		}
	}
}

func TestReadFlagsMatchTheirResultCardinality(t *testing.T) {
	root := NewRootCmd()
	for _, command := range []string{"post get", "reply get", "event get", "event context"} {
		cmd, _, err := root.Find(strings.Fields(command))
		if err != nil || cmd.Flags().Lookup("cursor") != nil || cmd.Flags().Lookup("limit") != nil {
			t.Fatal(command, err)
		}
	}
	search, _, _ := root.Find([]string{"search", "get"})
	for _, flag := range []string{"job", "cursor", "limit"} {
		if search.Flags().Lookup(flag) == nil {
			t.Fatal("search get lost paged job retrieval", flag)
		}
	}
	for _, args := range [][]string{{"search", "get", "--job", "job", "--limit", "21"}, {"post", "list", "--limit", "0"}, {"reply", "list", "--limit", "51"}, {"event", "list", "--limit", "0"}, {"proposal", "list", "--limit", "101"}, {"approval", "list", "--limit", "0"}} {
		code, out, stderr := run(t, args...)
		if code != 2 || out != "" || !strings.Contains(stderr, "BAD_LIMIT") {
			t.Fatal(args, code, out, stderr)
		}
	}
}
