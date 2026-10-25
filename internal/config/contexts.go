package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	kit "github.com/angelmsger/agent-cli-kit/config"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// ClientContext contains public coordinates only. The socket grants access;
// these expected identities prevent accidentally targeting a different broker.
type ClientContext struct {
	Name    string `yaml:"name" json:"name" config:"name"`
	Server  string `yaml:"server" json:"server" config:"server"`
	Socket  string `yaml:"socket" json:"socket" config:"socket"`
	Bot     string `yaml:"bot_id" json:"bot_id" config:"bot_id"`
	Channel string `yaml:"channel_id" json:"channel_id" config:"channel_id"`
}
type ClientDefaults struct{}
type ClientFile struct {
	Current  string          `yaml:"current_context" json:"current_context"`
	Contexts []ClientContext `yaml:"contexts" json:"contexts"`
}

var contextName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func ValidContextName(name string) bool { return contextName.MatchString(name) }

func init() {
	kit.Register(&kit.Schema{
		Fields: []kit.Field{
			{Key: kit.KeyServer, Default: apiclient.Origin, Service: true},
			{Key: kit.KeyAuthScheme, Default: "broker"},
			{Key: "socket", Env: []string{"WECOM_CHANNEL_SOCKET"}, Service: true},
			{Key: "bot_id", Env: []string{"WECOM_CHANNEL_BOT_ID"}, Service: true},
			{Key: "channel_id", Env: []string{"WECOM_CHANNEL_ID"}, Service: true},
		},
		Paths:   kit.Paths{Dir: ".angelmsger/wecom-channel", File: "config.yaml"},
		Words:   kit.Words{App: "wecom-channel-cli", ContextFlag: "--use-context", ContextEnv: "WECOM_CHANNEL_CONTEXT", NoHintEnv: "WECOM_CHANNEL_NO_CONTEXT_HINT", ListContexts: "config get-contexts", UseContext: "config use-context", SetContext: "config set-context", AuthGuide: "auth guide", AuthLogin: "auth status"},
		Schemes: []string{"broker"},
		Validate: func(v kit.Values) error {
			if v[kit.KeyServer] != apiclient.Origin || !validSocket(v["socket"]) || !apiclient.Identifier(v["bot_id"], 256) || !apiclient.Identifier(v["channel_id"], 128) {
				return e.New(e.CategoryConfig, "BAD_CONTEXT", "A context requires a canonical socket path, bot ID and channel ID; the upstream host is fixed.")
			}
			return nil
		},
	})
}
func validSocket(s string) bool {
	return filepath.IsAbs(s) && filepath.Clean(s) == s && len(s) <= 100 && !strings.ContainsAny(s, "\r\n\x00")
}
func ClientDir(override string) (string, error) {
	if override != "" {
		return filepath.Abs(override)
	}
	if v := os.Getenv("WECOM_CHANNEL_CONFIG"); v != "" {
		return filepath.Abs(v)
	}
	return kit.ResolveDir()
}
func ReadClients(dir string) (ClientFile, error) {
	var f ClientFile
	if _, err := kit.ReadFile(dir, &f); err != nil {
		return f, e.New(e.CategoryConfig, "CONFIG_READ", "Cannot read the client context file.")
	}
	seen := map[string]bool{}
	for _, c := range f.Contexts {
		if !ValidContextName(c.Name) || seen[strings.ToLower(c.Name)] {
			return f, e.New(e.CategoryConfig, "BAD_CONTEXT", "Context names must be unique lowercase identifiers.")
		}
		seen[c.Name] = true
		if err := kit.ValidateService(kit.StoredContext(c, ClientDefaults{})); err != nil {
			return f, err
		}
	}
	return f, nil
}
func (f ClientFile) Kit() kit.File[ClientContext, ClientDefaults] {
	return kit.File[ClientContext, ClientDefaults]{CurrentContext: f.Current, Contexts: f.Contexts}
}
func FromKit(f kit.File[ClientContext, ClientDefaults]) ClientFile {
	return ClientFile{Current: f.CurrentContext, Contexts: f.Contexts}
}
func SaveClients(dir string, f ClientFile) error {
	if err := kit.WriteFile(dir, f); err != nil {
		return e.New(e.CategoryConfig, "CONFIG_WRITE", "Cannot atomically save client contexts.")
	}
	return nil
}
func ResolveClient(dir, name string, flags kit.Values, setup bool) (ClientFile, *kit.Resolved, error) {
	f, err := ReadClients(dir)
	if err != nil {
		return f, nil, err
	}
	// A project's .env is untrusted input, so it cannot silently select a socket.
	r, err := kit.Load(f.Kit(), kit.LoadOptions{Context: name, Flags: flags, Setup: setup, DotenvPath: os.DevNull})
	return f, r, err
}
