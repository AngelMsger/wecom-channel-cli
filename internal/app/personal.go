package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/angelmsger/agent-cli-kit/credstore"
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/agent-cli-kit/terminal"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

type personalCredentials interface {
	Load(string) (string, error)
	Save(string, string) (string, error)
}

type personalRuntime struct {
	credentials func(string) personalCredentials
	client      func(string, string) (apiclient.Client, error)
}

func newPersonalRuntime() personalRuntime {
	return personalRuntime{
		credentials: func(dir string) personalCredentials {
			return credstore.NewStore(dir, "wecom-channel-cli.personal."+apiclient.Digest(dir)[:16])
		},
		client: func(bot, secret string) (apiclient.Client, error) { return apiclient.New(bot, secret) },
	}
}

func (s *appState) personalDir(cmd *cobra.Command) (string, error) {
	if cmd.Flag("socket").Changed || cmd.Flag("use-context").Changed || os.Getenv("WECOM_CHANNEL_SOCKET") != "" || os.Getenv("WECOM_CHANNEL_CONTEXT") != "" {
		return "", e.New(e.CategoryUsage, "PERSONAL_CONNECTION_CONFLICT", "Personal commands require a trusted user session without a managed socket or context.")
	}
	dir, err := s.clientDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "personal"), nil
}

// Validate existing ancestors before creating anything below them. A protected
// personal directory may live inside an ordinary user-owned config directory.
func ensurePersonalDir(dir string) error {
	parent := dir
	for {
		_, err := os.Lstat(parent)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		parent = filepath.Dir(parent)
	}
	if err := securefs.Directory(parent, false, true); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return securefs.Directory(dir, true, false)
}

func personalSecret(cmd *cobra.Command, bot string, store personalCredentials, prompt bool) (string, bool, error) {
	if secret := os.Getenv("WECOM_CHANNEL_BOT_SECRET"); secret != "" {
		return secret, true, nil
	}
	secret, err := store.Load(bot)
	if err == nil {
		return secret, false, nil
	}
	if !errors.Is(err, credstore.ErrSecretNotFound) {
		return "", false, e.New(e.CategoryConfig, "PERSONAL_CREDENTIAL_STORE", "Unlock or restore the personal credential store and try again.")
	}
	if prompt && terminal.Is(cmd.InOrStdin()) {
		secret, err = (terminal.Prompter{In: cmd.InOrStdin(), Out: cmd.ErrOrStderr()}).Secret("Bot Secret")
		if err == nil && secret != "" {
			return secret, true, nil
		}
	}
	return "", false, e.New(e.CategoryAuth, "BOT_SECRET_REQUIRED", "Supply WECOM_CHANNEL_BOT_SECRET locally or run connect in an interactive terminal.")
}

func (s *appState) connectCommand(runtime personalRuntime) *cobra.Command {
	var bot string
	var dry bool
	cmd := &cobra.Command{Use: "connect", Short: "Connect a personal Bot and discover its channels by name", Args: cobra.NoArgs,
		Long:    "Connect or refresh your personal Bot using only its ID and Secret. An interactive terminal prompts for missing credentials. Discovery follows every page before replacing the local catalog. Channel choices survive renames. A newly supplied Secret is saved in the user credential store after successful discovery.",
		Example: "  wecom-channel-cli connect --bot-id BOT_ID\n  wecom-channel-cli listen --channel Engineering",
	}
	cmd.Flags().StringVar(&bot, "bot-id", "", "Bot ID (prompted on first interactive use)")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Preview discovery without reading credentials, making requests or writing state")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.personalDir(cmd)
		if err != nil {
			return err
		}
		if cmd.Flags().Changed("bot-id") && !apiclient.Identifier(bot, 256) {
			return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a valid Bot ID.")
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "operation": "channel.list", "profile": personalPath(dir)})
		}
		if err = s.guard("connect"); err != nil {
			return err
		}
		if err = ensurePersonalDir(dir); err != nil {
			return err
		}
		lock, err := lockPersonalProfile(dir)
		if err != nil {
			return err
		}
		defer lock.Close()
		old, err := readPersonal(dir)
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
		if old.Bot != "" && bot != old.Bot {
			return e.New(e.CategoryConflict, "PERSONAL_BOT_CHANGED", "Use a separate --config-dir for another bot to preserve the original connection.")
		}
		store := runtime.credentials(dir)
		secret, persist, err := personalSecret(cmd, bot, store, true)
		if err != nil {
			return err
		}
		api, err := runtime.client(bot, secret)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), s.timeout)
		defer cancel()
		p, err := discoverPersonal(ctx, api, old, bot)
		if err != nil {
			return err
		}
		data, err := p.encode()
		if err != nil {
			return err
		}
		if persist {
			if _, err = store.Save(bot, secret); err != nil {
				return e.New(e.CategoryConfig, "PERSONAL_CREDENTIAL_SAVE", "Could not finish saving the verified Bot Secret. Check the user credential store and retry connect.")
			}
		}
		if err = securefs.WriteAtomic(personalPath(dir), data); err != nil {
			return err
		}
		next := "wecom-channel-cli listen --channel NAME"
		if len(p.Channels) == 0 {
			next = "Verify this bot has joined a channel and has channel API access, then run connect again."
		}
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"personal": "connected", "next_steps": []string{next}}})
		return output.EmitList(p.choices(), "", false, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
	}
	return cmd
}

func (s *appState) listenCommand(runtime personalRuntime) *cobra.Command {
	var selection personalSelection
	var dry bool
	var duration time.Duration
	cmd := &cobra.Command{Use: "listen", Short: "Listen to selected personal channels in the foreground", Args: cobra.NoArgs,
		Long:    "Use your saved channel names and user-owned checkpoints. Select channels with repeated --channel flags, --pick for duplicate names, or --all. Selection is remembered; --none clears it. With no saved selection, a terminal offers a numbered menu. Start again to change a running selection. Records are NDJSON. Recover transient read errors with bounded backoff; stop with Ctrl-C or use --duration.",
		Example: "  wecom-channel-cli listen --channel Engineering --channel Announcements\n  wecom-channel-cli listen",
	}
	selection.flags(cmd)
	cmd.Flags().DurationVar(&duration, "duration", 0, "Optional foreground session deadline (default until interrupted)")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Show the effective selection without saving or contacting WeCom")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		dir, err := s.personalDir(cmd)
		if err != nil {
			return err
		}
		if duration < 0 || !dry && !selection.none && cmd.Flag("format").Changed && s.format != "ndjson" {
			return e.New(e.CategoryUsage, "LISTEN_OPTIONS", "Use a nonnegative duration and NDJSON for live records.")
		}
		if !dry {
			if err = s.guard("listen"); err != nil {
				return err
			}
		}
		if _, err := os.Lstat(dir); os.IsNotExist(err) {
			return personalMissing()
		}
		if err = securefs.Directory(dir, true, false); err != nil {
			return err
		}
		if !dry {
			lock, err := lockPersonalProfile(dir)
			if err != nil {
				return err
			}
			defer lock.Close()
		}
		p, err := readPersonal(dir)
		if err != nil {
			return err
		}
		if p.Bot == "" {
			return personalMissing()
		}
		if !dry && !selection.explicit() && len(p.selected()) == 0 && len(p.Channels) > 0 && terminal.Is(cmd.InOrStdin()) {
			selection, err = promptSelection(cmd, p)
			if err != nil {
				return err
			}
		}
		if selection.explicit() {
			if err = selection.apply(&p); err != nil {
				return err
			}
		}
		channels := p.selected()
		if len(channels) > 32 {
			return e.New(e.CategoryUsage, "LISTEN_SCOPE_LIMIT", "Select at most 32 channels in one personal session.")
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "channels": p.choices(), "selected_count": len(channels)})
		}
		if len(channels) == 0 && !selection.none {
			return e.New(e.CategoryUsage, "NO_CHANNELS_SELECTED", "Run connect to list joined channels, then listen --channel NAME or listen --all.")
		}

		if selection.explicit() {
			data, err := p.encode()
			if err != nil {
				return err
			}
			if err = securefs.WriteAtomic(personalPath(dir), data); err != nil {
				return err
			}
		}
		if selection.none {
			return s.emit(cmd, map[string]int{"selected_count": 0})
		}
		secret, _, err := personalSecret(cmd, p.Bot, runtime.credentials(dir), false)
		if err != nil {
			return err
		}
		api, err := runtime.client(p.Bot, secret)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		if duration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, duration)
			defer cancel()
		}
		return listenPersonalChannels(ctx, cmd, dir, p.Bot, channels, api, s.timeout)
	}
	return cmd
}

func personalMissing() error {
	return e.New(e.CategoryConfig, "PERSONAL_CONNECTION_MISSING", "Connect your bot first.").WithNextSteps("wecom-channel-cli connect --bot-id BOT_ID")
}
