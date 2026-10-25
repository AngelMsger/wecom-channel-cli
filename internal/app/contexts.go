package app

import (
	"os"
	"path/filepath"

	kit "github.com/angelmsger/agent-cli-kit/config"
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) clientDir() (string, error) { return config.ClientDir(s.configDir) }
func (s *appState) resolve(cmd *cobra.Command, r *broker.Request) (string, error) {
	dir, err := s.clientDir()
	if err != nil {
		socket := s.socket
		if socket == "" {
			socket = os.Getenv("WECOM_CHANNEL_SOCKET")
		}
		if socket != "" && s.useContext == "" && os.Getenv("WECOM_CHANNEL_CONTEXT") == "" && s.configDir == "" && os.Getenv("WECOM_CHANNEL_CONFIG") == "" {
			return socket, nil
		}
		return "", e.New(e.CategoryConfig, "CONFIG_PATH", "Cannot locate client configuration; supply --config-dir or a socket without a named context.")
	}
	flags := kit.Values{}
	if s.socket != "" {
		flags["socket"] = s.socket
	}
	f, res, err := config.ResolveClient(dir, s.useContext, flags, false)
	if err != nil {
		return "", err
	}
	if res.ActiveContext == "" {
		if len(f.Contexts) > 1 {
			return "", e.New(e.CategoryConfig, "CONTEXT_REQUIRED", "Select a channel security context with --use-context.")
		}
		return res.Values["socket"], nil
	}
	if err = kit.ValidateService(res.Values); err != nil {
		return "", err
	}
	r.Context = res.ActiveContext
	r.Bot = res.Values["bot_id"]
	r.Channel = res.Values["channel_id"]
	if r.Query.Channel != "" && r.Query.Channel != r.Channel {
		return "", e.New(e.CategoryPermission, "CONTEXT_MISMATCH", "The requested channel differs from the selected security context.")
	}
	r.Query.Channel = r.Channel
	if r.Write != nil {
		if r.Write.Channel != "" && r.Write.Channel != r.Channel {
			return "", e.New(e.CategoryPermission, "CONTEXT_MISMATCH", "The proposal targets a different channel context.")
		}
		r.Write.Channel = r.Channel
	}
	if len(res.ContextNames) > 1 && res.ContextSource != kit.ContextSourceFlag && res.ContextSource != kit.ContextSourceEnv && os.Getenv("WECOM_CHANNEL_NO_CONTEXT_HINT") != "1" {
		output.EmitNotice(cmd.ErrOrStderr(), kit.ContextNotice(res.ActiveContext, res.ContextSource, res.ContextNames))
	}
	return res.Values["socket"], nil
}

func (s *appState) addContextCommands(group *cobra.Command) {
	group.AddCommand(&cobra.Command{Use: "path", Short: "Show the client configuration path", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		dir, err := s.clientDir()
		if err != nil {
			return err
		}
		return s.emit(cmd, map[string]string{"path": filepath.Join(dir, "config.yaml")})
	}})
	group.AddCommand(&cobra.Command{Use: "get-contexts", Aliases: []string{"contexts"}, Short: "List named channel security contexts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		dir, err := s.clientDir()
		if err != nil {
			return err
		}
		f, err := config.ReadClients(dir)
		if err != nil {
			return err
		}
		rows := []map[string]any{}
		for _, c := range f.Contexts {
			rows = append(rows, map[string]any{"name": c.Name, "socket": c.Socket, "bot_id": c.Bot, "channel_id": c.Channel, "current": c.Name == f.Current})
		}
		return output.EmitList(rows, "", false, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
	}})
	var socket, bot, channel string
	var overwrite, activate, dry bool
	set := &cobra.Command{Use: "set-context NAME", Short: "Prepare an offline channel context without credentials or changing broker policy", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		name := kit.NormalizeContextName(args[0])
		if !config.ValidContextName(name) {
			return e.New(e.CategoryUsage, "BAD_CONTEXT_NAME", "Use a lowercase context name with letters, digits, underscores or hyphens.")
		}
		dir, err := s.clientDir()
		if err != nil {
			return err
		}
		flags := kit.Values{}
		for k, v := range map[string]string{"socket": socket, "bot_id": bot, "channel_id": channel} {
			if v != "" {
				flags[k] = v
			}
		}
		f, res, err := config.ResolveClient(dir, name, flags, true)
		if err != nil {
			return err
		}
		plan, err := kit.PlanServiceContext(f.Kit(), name, res.Values, res.Sources, overwrite, activate)
		if err != nil {
			return err
		}
		if !dry && plan.Changed {
			if err = s.guard(); err != nil {
				return err
			}
			if err = config.SaveClients(dir, config.FromKit(plan.File)); err != nil {
				return err
			}
		}
		return s.emit(cmd, map[string]any{"dry_run": dry, "plan": plan})
	}}
	set.Flags().StringVar(&socket, "socket", "", "Agent socket for this channel context")
	set.Flags().StringVar(&bot, "bot-id", "", "Expected broker bot ID")
	set.Flags().StringVar(&channel, "channel", "", "Expected channel ID")
	set.Flags().BoolVar(&overwrite, "overwrite", false, "Replace conflicting public context coordinates")
	set.Flags().BoolVar(&activate, "activate", false, "Make this context current")
	set.Flags().BoolVar(&dry, "dry-run", false, "Preview the exact context merge without writing")
	group.AddCommand(set)
	for _, verb := range []string{"use-context", "delete-context"} {
		var preview, yes bool
		c := &cobra.Command{Use: verb + " NAME", Short: verb + " in the client configuration", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			dir, err := s.clientDir()
			if err != nil {
				return err
			}
			f, err := config.ReadClients(dir)
			if err != nil {
				return err
			}
			k := f.Kit()
			if verb == "use-context" {
				_, err = kit.UseContext(&k, args[0])
			} else {
				_, err = kit.DeleteContext(&k, args[0])
			}
			if err != nil {
				return err
			}
			if !preview {
				if err = s.guard(); err != nil {
					return err
				}
				if verb == "delete-context" && !yes {
					return e.New(e.CategoryPermission, "CONFIRM_REQUIRED", "Deleting a saved context requires --yes; this never deletes broker data.")
				}
				if err = config.SaveClients(dir, config.FromKit(k)); err != nil {
					return err
				}
			}
			return s.emit(cmd, map[string]any{"dry_run": preview, "context": args[0], "current_context": k.CurrentContext, "operation": verb})
		}}
		c.Flags().BoolVar(&preview, "dry-run", false, "Preview without modifying client configuration")
		if verb == "delete-context" {
			c.Flags().BoolVar(&yes, "yes", false, "Confirm deleting this saved connection only")
		}
		group.AddCommand(c)
	}
}
