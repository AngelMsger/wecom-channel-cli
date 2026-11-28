package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/angelmsger/agent-cli-kit/credstore"
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/agent-cli-kit/terminal"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

type profileCredentials interface {
	Load(string) (string, error)
	Save(string, string) (string, error)
	Delete(string) error
}

type profileRuntime struct {
	credentials func(string) profileCredentials
	client      func(bot, secret string, tcfg transport.Config) (apiclient.Client, error)
	isTerminal  func(any) bool
}

func (r profileRuntime) interactive(stream any) bool {
	if r.isTerminal != nil {
		return r.isTerminal(stream)
	}
	return terminal.Is(stream)
}

func newProfileRuntime() profileRuntime {
	return profileRuntime{
		credentials: func(dir string) profileCredentials {
			return credstore.NewStore(dir, "wecom-channel-cli."+apiclient.Digest(dir)[:16])
		},
		client: func(bot, secret string, tcfg transport.Config) (apiclient.Client, error) {
			return apiclient.NewWithOptions(bot, secret, apiclient.Options{Transport: tcfg})
		},
	}
}

func (s *appState) profileDir(cmd *cobra.Command) (string, error) {
	if cmd.Flag("socket").Changed || cmd.Flag("use-context").Changed {
		return "", e.New(e.CategoryUsage, "PROFILE_CONNECTION_CONFLICT", "connect, listen, serve, status and auth logout use the local profile directory, not --socket or --use-context.")
	}
	return s.clientDir()
}

// profileOperatorDir rejects managed worker environments (WECOM_CHANNEL_SOCKET
// or WECOM_CHANNEL_CONTEXT) so credential-holding commands only run in a
// trusted operator session.
func (s *appState) profileOperatorDir(cmd *cobra.Command) (string, error) {
	if cmd.Flag("socket").Changed || cmd.Flag("use-context").Changed || os.Getenv("WECOM_CHANNEL_SOCKET") != "" || os.Getenv("WECOM_CHANNEL_CONTEXT") != "" {
		return "", e.New(e.CategoryUsage, "PROFILE_CONNECTION_CONFLICT", "connect, listen, serve and auth logout require a trusted operator session without a managed socket or context.")
	}
	return s.clientDir()
}

func loadProfileSecret(cmd *cobra.Command, bot string, store profileCredentials, prompt bool) (string, bool, error) {
	if secret := os.Getenv("WECOM_CHANNEL_BOT_SECRET"); secret != "" {
		return secret, true, nil
	}
	secret, err := store.Load(bot)
	if err == nil {
		return secret, false, nil
	}
	if !errors.Is(err, credstore.ErrSecretNotFound) {
		return "", false, e.New(e.CategoryConfig, "CREDENTIAL_STORE", "Unlock or restore the credential store, or supply WECOM_CHANNEL_BOT_SECRET.")
	}
	if prompt && terminal.Is(cmd.InOrStdin()) {
		secret, err = (terminal.Prompter{In: cmd.InOrStdin(), Out: cmd.ErrOrStderr()}).Secret("Bot Secret")
		if err == nil && secret != "" {
			return secret, true, nil
		}
	}
	return "", false, e.New(e.CategoryAuth, "BOT_SECRET_REQUIRED", "Supply WECOM_CHANNEL_BOT_SECRET locally or run connect in an interactive terminal.")
}

// decodeTopic decodes a channel.list record whether its Data came from real
// HTTPClient.Read (map[string]any after JSON redaction) or an in-memory test mock.
func decodeTopic(raw any) (apiclient.Topic, bool) {
	switch v := raw.(type) {
	case apiclient.Topic:
		return v, true
	case *apiclient.Topic:
		if v == nil {
			return apiclient.Topic{}, false
		}
		return *v, true
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return apiclient.Topic{}, false
	}
	var topic apiclient.Topic
	if err := json.Unmarshal(b, &topic); err != nil {
		return apiclient.Topic{}, false
	}
	return topic, true
}

// resolveChannelContexts computes context names for channels against saved
// config.yaml entries. Explicit ch.Context values are preserved verbatim so
// syncServeClientContexts can reject collisions; missing names reuse a saved
// context only for the same (bot, channel) pair and otherwise avoid every
// existing saved context name.
func resolveChannelContexts(bot string, channels []config.ProfileChannel, saved []config.ClientContext) []string {
	used := make(map[string]bool, len(saved)+len(channels))
	ownedByChannel := make(map[string]string)
	for _, c := range saved {
		used[c.Name] = true
		if c.Bot == bot && c.Channel != "" && ownedByChannel[c.Channel] == "" {
			ownedByChannel[c.Channel] = c.Name
		}
	}
	assigned := make(map[string]bool, len(channels))
	for _, ch := range channels {
		if ch.Context != "" {
			used[ch.Context] = true
			assigned[ch.Context] = true
		}
	}
	out := make([]string, len(channels))
	for i, ch := range channels {
		if ch.Context != "" {
			out[i] = ch.Context
			continue
		}
		if name := ownedByChannel[ch.ID]; name != "" && !assigned[name] {
			out[i] = name
			assigned[name] = true
			continue
		}
		name := config.ChannelContextName(bot, ch, used)
		out[i] = name
		assigned[name] = true
	}
	return out
}

// discoverProfileChannels enumerates all joined channel pages before replacing
// the saved catalog, preserving selection, custom context names and read_only
// flags across channel renames by stable topic_id.
func discoverProfileChannels(ctx context.Context, api apiclient.Reader, old config.Profile, bot string, saved []config.ClientContext) (config.Profile, error) {
	p := old
	p.Version = 1
	p.Bot = bot
	p = p.WithDefaults()
	p.Channels = []config.ProfileChannel{}

	existing := make(map[string]config.ProfileChannel, len(old.Channels))
	for _, ch := range old.Channels {
		existing[ch.ID] = ch
	}

	seenIDs, cursors := make(map[string]bool), make(map[string]bool)
	q := apiclient.Query{Operation: "channel.list", Limit: 50}
	for {
		page, err := api.Read(ctx, q)
		if err != nil {
			return p, err
		}
		for _, record := range page.Items {
			topic, ok := decodeTopic(record.Data)
			if !ok || !apiclient.Identifier(topic.ID, 128) || seenIDs[topic.ID] {
				return p, e.New(e.CategoryParse, "CHANNEL_CATALOG_CHANGED", "Channel discovery returned inconsistent entries. Run connect again to refresh.")
			}
			seenIDs[topic.ID] = true
			prev := existing[topic.ID]
			p.Channels = append(p.Channels, config.ProfileChannel{
				ID:       topic.ID,
				Name:     topic.Name,
				Context:  prev.Context,
				Created:  topic.Created,
				Selected: prev.Selected,
				ReadOnly: prev.ReadOnly,
			})
		}
		if !page.HasMore {
			break
		}
		if !apiclient.Identifier(page.Next, 256) || cursors[page.Next] || len(cursors) >= 200 {
			return p, e.New(e.CategoryParse, "CHANNEL_DISCOVERY_INCOMPLETE", "Discovery did not complete within the page bound. The saved channel list is unchanged.")
		}
		cursors[page.Next] = true
		q.Cursor = page.Next
	}
	sort.Slice(p.Channels, func(i, j int) bool {
		a, b := p.Channels[i], p.Channels[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ID < b.ID
	})
	names := resolveChannelContexts(bot, p.Channels, saved)
	for i := range p.Channels {
		p.Channels[i].Context = names[i]
	}
	return p, nil
}

func profileChoices(p config.Profile) []apiclient.Record {
	used := map[string]bool{}
	rows := make([]apiclient.Record, 0, len(p.Channels))
	for i, ch := range p.Channels {
		ctxName := config.ChannelContextName(p.Bot, ch, used)
		rows = append(rows, apiclient.Untrusted(struct {
			config.ProfileChannel
			Choice          int    `json:"choice"`
			ResolvedContext string `json:"resolved_context"`
		}{ch, i + 1, ctxName}, ch.ID, "", ""))
	}
	return rows
}

type channelSelection struct {
	names     []string
	picks     []int
	ids       []string
	all, none bool
}

func (s channelSelection) explicit() bool {
	return s.all || s.none || len(s.names)+len(s.picks)+len(s.ids) > 0
}

func (s *channelSelection) bindFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringArrayVar(&s.names, "channel", nil, "Select a discovered channel by exact name or topic_id; repeat for multiple")
	f.IntSliceVar(&s.picks, "pick", nil, "Select numbered entries from the last connect list (for duplicate names)")
	f.BoolVar(&s.all, "all", false, "Select all discovered channels")
	f.BoolVar(&s.none, "none", false, "Clear the saved channel selection")
}

func (s channelSelection) apply(p *config.Profile) error {
	if (s.all && s.none) || ((s.all || s.none) && len(s.names)+len(s.picks)+len(s.ids) > 0) {
		return e.New(e.CategoryUsage, "CHANNEL_SELECTION", "Choose --channel/--pick, --all, or --none.")
	}
	chosen := make(map[int]bool)
	for _, pick := range s.picks {
		if pick < 1 || pick > len(p.Channels) {
			return e.New(e.CategoryUsage, "CHANNEL_CHOICE", "Choose a number from the channel list returned by connect.")
		}
		chosen[pick-1] = true
	}
	for _, id := range s.ids {
		found := -1
		for i, ch := range p.Channels {
			if ch.ID == id {
				found = i
				break
			}
		}
		if found < 0 {
			return e.New(e.CategoryNotFound, "CHANNEL_NAME_UNKNOWN", "The channel name or ID was not found. Refresh with connect and select a listed channel.")
		}
		chosen[found] = true
	}
	for _, want := range s.names {
		var nameMatches []int
		idMatch := -1
		for i, ch := range p.Channels {
			if ch.Name == want {
				nameMatches = append(nameMatches, i+1)
			}
			if ch.ID == want {
				idMatch = i
			}
		}
		switch {
		case len(nameMatches) == 1:
			chosen[nameMatches[0]-1] = true
		case len(nameMatches) > 1:
			return e.New(e.CategoryUsage, "CHANNEL_NAME_AMBIGUOUS", "Several channels have this name. Select a matching --pick number or pass its topic_id.").WithDetails(map[string]any{"choices": nameMatches})
		case idMatch >= 0:
			chosen[idMatch] = true
		default:
			return e.New(e.CategoryNotFound, "CHANNEL_NAME_UNKNOWN", "The channel name or ID was not found. Refresh with connect and select a listed channel.")
		}
	}
	for i := range p.Channels {
		p.Channels[i].Selected = s.all || chosen[i]
	}
	return nil
}

func promptChannelSelection(cmd *cobra.Command, p config.Profile) (channelSelection, error) {
	var s channelSelection
	for i, ch := range p.Channels {
		fmt.Fprintf(cmd.ErrOrStderr(), "%d  %q  (%s)\n", i+1, ch.Name, ch.ID)
	}
	answer, err := (terminal.Prompter{In: cmd.InOrStdin(), Out: cmd.ErrOrStderr()}).Line("Choose channel numbers separated by commas, all, or none", "")
	if err != nil {
		return s, err
	}
	switch strings.TrimSpace(answer) {
	case "all":
		s.all = true
	case "none":
		s.none = true
	default:
		for _, part := range strings.Split(answer, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 1 || n > len(p.Channels) {
				return s, e.New(e.CategoryUsage, "CHANNEL_CHOICE", "Enter channel numbers, all, or none.")
			}
			s.ids = append(s.ids, p.Channels[n-1].ID)
		}
	}
	return s, nil
}

func lockProfile(dir string) (*os.File, error) {
	f, err := securefs.LockPrivate(filepath.Join(dir, "profile.lock"))
	if err == unix.EWOULDBLOCK {
		return nil, e.New(e.CategoryConflict, "PROFILE_IN_USE", "Another connect operation is currently updating this profile.")
	}
	return f, err
}

func (s *appState) connectCommand(runtime profileRuntime) *cobra.Command {
	var bot, proxy string
	var upstreamTimeout time.Duration
	var strictFS, dry bool
	cmd := &cobra.Command{
		Use:   "connect",
		Short: "Connect a WeCom Bot, save credentials securely, and discover its channels",
		Args:  cobra.NoArgs,
		Long: "Connect or refresh a WeCom Bot using its Bot ID and Secret.\n" +
			"Discovers all joined channel pages, saves the verified Secret in the user\n" +
			"credential store (OS keychain or private file fallback), and writes a single\n" +
			"unified profile.yaml used by listen, serve, and status. Re-run connect anytime\n" +
			"to refresh renamed or newly joined channels without losing saved selections.",
		Example: "  wecom-channel-cli connect --bot-id BOT_ID\n  wecom-channel-cli connect --bot-id BOT_ID --proxy socks5://127.0.0.1:1080\n  wecom-channel-cli listen --channel Engineering\n  wecom-channel-cli serve --channel Engineering --webhook-url https://... --webhook-token-env GROK_ROUTINE_SENDER_KEY --save",
	}
	cmd.Flags().StringVar(&bot, "bot-id", "", "Bot ID (reused from profile or prompted in an interactive terminal)")
	cmd.Flags().StringVar(&proxy, "proxy", "", "Optional explicit proxy URL (http/https/socks5/socks5h) saved to profile.yaml")
	cmd.Flags().DurationVar(&upstreamTimeout, "upstream-timeout", 0, "Optional single-request timeout saved to profile.yaml (5s–180s, default 45s)")
	cmd.Flags().BoolVar(&strictFS, "strict-fs", false, "Enforce full ancestor directory checks up to /")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Preview discovery without reading credentials, making requests, or writing state")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.profileOperatorDir(cmd)
		if err != nil {
			return err
		}
		if cmd.Flags().Changed("bot-id") && !apiclient.Identifier(bot, 256) {
			return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a valid Bot ID.")
		}
		if cmd.Flags().Changed("proxy") {
			if _, err := transport.ValidateProxyURL(proxy); err != nil {
				return err
			}
		}
		if cmd.Flags().Changed("upstream-timeout") && (upstreamTimeout < 5*time.Second || upstreamTimeout > 180*time.Second) {
			return e.New(e.CategoryUsage, "BAD_TIMEOUT", "--upstream-timeout must be between 5s and 180s.")
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "operation": "channel.list", "profile": config.ProfilePath(dir)})
		}
		if err = s.guard("connect"); err != nil {
			return err
		}
		dir, err = securefs.EnsurePrivateDir(dir, strictFS)
		if err != nil {
			return err
		}
		lock, err := lockProfile(dir)
		if err != nil {
			return err
		}
		defer lock.Close()

		old, exists, err := config.LoadProfile(dir, strictFS)
		if err != nil {
			return err
		}
		if bot == "" {
			bot = old.Bot
		}
		if bot == "" {
			bot = os.Getenv("WECOM_CHANNEL_BOT_ID")
		}
		if bot == "" && terminal.Is(cmd.InOrStdin()) {
			bot, err = (terminal.Prompter{In: cmd.InOrStdin(), Out: cmd.ErrOrStderr()}).Line("Bot ID", "")
			if err != nil {
				return err
			}
		}
		if !apiclient.Identifier(bot, 256) {
			return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a Bot ID with --bot-id or run connect in a terminal.")
		}
		if exists && old.Bot != "" && bot != old.Bot {
			return e.New(e.CategoryConflict, "PROFILE_BOT_CHANGED", "Use a separate --config-dir for another bot to preserve existing checkpoints and state.")
		}
		if !exists {
			old = config.DefaultProfile(bot)
		}
		if cmd.Flags().Changed("proxy") {
			old.Network.Proxy = strings.TrimSpace(proxy)
		}
		if cmd.Flags().Changed("upstream-timeout") {
			old.Network.TimeoutSeconds = int(upstreamTimeout / time.Second)
		}

		store := runtime.credentials(dir)
		secret, persist, err := loadProfileSecret(cmd, bot, store, true)
		if err != nil {
			return err
		}
		api, err := runtime.client(bot, secret, old.TransportConfig())
		if err != nil {
			return err
		}
		var savedContexts []config.ClientContext
		if clients, readErr := config.ReadClients(dir); readErr == nil {
			savedContexts = clients.Contexts
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), max(s.timeout, 2*time.Minute))
		defer cancel()
		updated, err := discoverProfileChannels(ctx, api, old, bot, savedContexts)
		if err != nil {
			return err
		}
		if persist {
			if _, err = store.Save(bot, secret); err != nil {
				return e.New(e.CategoryConfig, "CREDENTIAL_SAVE_FAILED", "Could not finish saving the verified Bot Secret. Check the credential store and retry connect.")
			}
		}
		if err = config.SaveProfile(dir, updated, strictFS); err != nil {
			return err
		}
		next := []string{"wecom-channel-cli listen --channel NAME", "wecom-channel-cli serve --channel NAME --save"}
		if len(updated.Channels) == 0 {
			next = []string{"Verify this bot has joined a channel and has channel API access, then run connect again."}
		}
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"connect": "ok", "profile": config.ProfilePath(dir), "channels": len(updated.Channels), "next_steps": next}})
		return output.EmitList(profileChoices(updated), "", false, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
	}
	return cmd
}

func (s *appState) authLogoutCommand(runtime profileRuntime) *cobra.Command {
	var bot string
	var strictFS, dry bool
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Remove the saved Bot Secret from the user credential store",
		Args:  cobra.NoArgs,
		Long: "Remove a Bot Secret stored by `connect` from the OS keychain and local fallback store.\n" +
			"Preserves profile.yaml, client contexts, and channel ledgers. Idempotent when no secret is stored.",
		Example: "  wecom-channel-cli auth logout\n  wecom-channel-cli auth logout --bot-id BOT_ID --dry-run",
	}
	cmd.Flags().StringVar(&bot, "bot-id", "", "Bot ID to forget (defaults to bot_id in profile.yaml)")
	cmd.Flags().BoolVar(&strictFS, "strict-fs", false, "Enforce full ancestor directory checks up to /")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Preview credential removal without modifying the credential store")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.profileOperatorDir(cmd)
		if err != nil {
			return err
		}
		if cmd.Flags().Changed("bot-id") && !apiclient.Identifier(bot, 256) {
			return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a valid Bot ID.")
		}
		if bot == "" {
			p, exists, loadErr := config.LoadProfile(dir, strictFS)
			if loadErr != nil {
				return loadErr
			}
			if exists {
				bot = p.Bot
			}
		}
		if bot == "" {
			bot = os.Getenv("WECOM_CHANNEL_BOT_ID")
		}
		if !apiclient.Identifier(bot, 256) {
			return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Pass --bot-id BOT_ID or run inside a directory with profile.yaml.")
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "operation": "auth.logout", "bot_id": bot, "profile_preserved": true})
		}
		if err = s.guard("auth logout"); err != nil {
			return err
		}
		resolved := dir
		if eval, evalErr := filepath.EvalSymlinks(dir); evalErr == nil {
			resolved = eval
		}
		if err = runtime.credentials(resolved).Delete(bot); err != nil {
			return e.New(e.CategoryConfig, "CREDENTIAL_DELETE_FAILED", "Could not remove the saved Bot Secret from the credential store. Check keychain access and retry.")
		}
		return s.emit(cmd, map[string]any{"logged_out": true, "bot_id": bot, "profile_preserved": true})
	}
	return cmd
}
