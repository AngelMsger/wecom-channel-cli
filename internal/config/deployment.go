package config

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"golang.org/x/sys/unix"
)

// Deployment shares one bot token and callback endpoint across isolated contexts.
type Deployment struct {
	Version        int      `json:"version"`
	Bot            string   `json:"bot_id"`
	CallbackListen string   `json:"callback_listen"`
	Contexts       []Config `json:"contexts"`
}

func LoadDeployment(path string) (Deployment, error) {
	var d Deployment
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return d, e.New(e.CategoryConfig, "CONFIG_PATH", "Use a canonical absolute operator policy path.")
	}
	if err := securefs.Directory(filepath.Dir(path), false, true); err != nil {
		return d, err
	}
	info, err := os.Lstat(path)
	var stat unix.Stat_t
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || unix.Lstat(path, &stat) != nil || (stat.Uid != 0 && int(stat.Uid) != os.Geteuid()) {
		return d, e.New(e.CategoryConfig, "CONFIG_UNSAFE", "Policy must be a protected regular file owned by root or the broker identity.")
	}
	f, err := os.Open(path)
	if err != nil {
		return d, e.New(e.CategoryConfig, "CONFIG_READ", "Cannot read operator policy.")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(data) > 65536 {
		return d, e.New(e.CategoryConfig, "CONFIG_INVALID", "Policy exceeds its size limit.")
	}
	var header struct {
		Version int `json:"version"`
	}
	if safejson.Decode(data, &header, false) != nil {
		return d, e.New(e.CategoryConfig, "CONFIG_INVALID", "Invalid policy document.")
	}
	if header.Version == 1 {
		c, err := Load(path)
		if err != nil {
			return d, err
		}
		return Deployment{Version: 1, Bot: c.Bot, CallbackListen: c.CallbackListen, Contexts: []Config{c}}, nil
	}
	var raw struct {
		Version        int               `json:"version"`
		Bot            string            `json:"bot_id"`
		CallbackListen string            `json:"callback_listen"`
		Contexts       []json.RawMessage `json:"contexts"`
	}
	if header.Version != 2 || safejson.Decode(data, &raw, true) != nil || len(raw.Contexts) == 0 || len(raw.Contexts) > 32 || !apiclient.Identifier(raw.Bot, 256) {
		return d, e.New(e.CategoryConfig, "CONFIG_INVALID", "A version 2 deployment requires one bot and 1–32 channel contexts.")
	}
	d = Deployment{Version: 2, Bot: raw.Bot, CallbackListen: raw.CallbackListen}
	seen := map[string]bool{}
	for _, data := range raw.Contexts {
		c := Default()
		c.Bot = raw.Bot
		c.CallbackListen = raw.CallbackListen
		if safejson.Decode(data, &c, true) != nil || c.Bot != raw.Bot || c.CallbackListen != raw.CallbackListen || len(c.Channels) != 1 {
			return d, e.New(e.CategoryConfig, "CONTEXT_SCOPE", "Every context must contain exactly one channel and share the deployment's bot/callback.")
		}
		if err := c.Validate(); err != nil {
			return d, err
		}
		for _, key := range []string{"name:" + c.Name, "channel:" + c.Channels[0].ID, "path:" + c.StateDir, "path:" + c.AgentSocket, "path:" + c.AdminSocket, "socket-dir:" + filepath.Dir(c.AgentSocket), "socket-dir:" + filepath.Dir(c.AdminSocket)} {
			if seen[key] {
				return d, e.New(e.CategoryConfig, "CONTEXT_COLLISION", "Context names, channels, state paths and socket directories must be distinct.")
			}
			seen[key] = true
		}
		d.Contexts = append(d.Contexts, c)
	}
	return d, nil
}
