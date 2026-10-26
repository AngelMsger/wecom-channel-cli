// Package app wires the agent-facing CLI and operator commands.
package app

import (
	"context"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/angelmsger/agent-cli-kit/cliflags"
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/constants"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

type appState struct {
	socket, format        string
	configDir, useContext string
	timeout               time.Duration
	allowWrites           bool
}

func NewRootCmd() *cobra.Command {
	s := &appState{}
	root := &cobra.Command{Use: constants.AppName, Short: "Read and watch WeCom channels; propose messages and native reactions through an isolated broker", SilenceUsage: true, SilenceErrors: true, Version: constants.Version}
	root.Long = root.Short + ". External content always carries an untrusted-data envelope.\nThe agent socket cannot approve writes. See `auth guide` and the companion Skill."
	root.PersistentFlags().StringVar(&s.socket, "socket", "", "Unix socket path (or WECOM_CHANNEL_SOCKET)")
	root.PersistentFlags().StringVar(&s.configDir, "config-dir", "", "Client context directory (or WECOM_CHANNEL_CONFIG)")
	root.PersistentFlags().StringVar(&s.useContext, "use-context", "", "Select a channel security context (or WECOM_CHANNEL_CONTEXT)")
	root.PersistentFlags().StringVar(&s.format, "format", "json", "Output format: json or ndjson; trust metadata is always retained")
	root.PersistentFlags().DurationVar(&s.timeout, "timeout", 65*time.Second, "Bound the entire local request")
	root.PersistentFlags().BoolVar(&s.allowWrites, "allow-writes", false, "Override only WECOM_CHANNEL_CLI_READ_ONLY; never broker policy or approval")
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		if s.format != "json" && s.format != "ndjson" {
			return e.New(e.CategoryUsage, "BAD_FORMAT", "Use json or ndjson; raw and projected output are intentionally unavailable.")
		}
		if s.timeout <= 0 || s.timeout > 2*time.Minute {
			return e.New(e.CategoryUsage, "BAD_TIMEOUT", "Timeout must be greater than zero and at most two minutes.")
		}
		return nil
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, _ error) error {
		return e.New(e.CategoryUsage, "BAD_FLAG", "Invalid or unsupported flag; inspect command help.")
	})
	root.AddCommand(s.readCommands()...)
	root.AddCommand(s.reactionCommands())
	root.AddCommand(s.eventCommands(), s.proposalCommands(), s.approvalCommands(), s.brokerCommands(), s.configCommands(), s.authCommands(), s.skillCommands())
	root.AddCommand(&cobra.Command{Use: "version", Short: "Print build metadata", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		return s.emit(cmd, map[string]any{"name": constants.AppName, "version": constants.Version, "commit": constants.Commit, "built_at": constants.BuildTime})
	}})
	root.AddCommand(&cobra.Command{Use: "doctor", Short: "Check broker connectivity and read-only policy", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return s.call(cmd, broker.Request{Operation: "health"}) }})
	var groups func(*cobra.Command)
	groups = func(c *cobra.Command) {
		for _, x := range c.Commands() {
			groups(x)
		}
		if c.HasSubCommands() && !c.Runnable() {
			c.RunE = func(cmd *cobra.Command, args []string) error {
				if len(args) == 0 {
					return cmd.Help()
				}
				return e.New(e.CategoryUsage, "UNKNOWN_COMMAND", "Unknown subcommand.").WithNextSteps(cmd.CommandPath() + " --help")
			}
		}
	}
	groups(root)
	return root
}

func Execute(args []string, in io.Reader, out, errout io.Writer) int {
	root := NewRootCmd()
	root.SetIn(in)
	root.SetOut(out)
	root.SetErr(errout)
	corrected, changes := cliflags.Normalize(args, cliflags.Collect(root))
	root.SetArgs(corrected)
	if len(changes) > 0 {
		output.EmitNotice(errout, map[string]any{"_notice": map[string]any{"corrections": changes}})
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		ce, ok := err.(*e.CLIError)
		if !ok {
			ce = e.New(e.CategoryUsage, "USAGE", "Invalid command or arguments; inspect --help.")
		}
		output.EmitError(ce, errout)
		return e.ExitCode(ce)
	}
	return 0
}

func (s *appState) emit(cmd *cobra.Command, v any) error {
	return output.Emit(v, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
}
func (s *appState) call(cmd *cobra.Command, r broker.Request) error {
	skillDriftNotice(cmd)
	if !r.DryRun && (r.Operation == "proposal.create" || strings.HasPrefix(r.Operation, "approval.") && r.Operation != "approval.list") {
		if err := s.guard(); err != nil {
			return err
		}
	}
	socket, err := s.resolve(cmd, &r)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), s.timeout)
	defer cancel()
	resp, err := broker.Call(ctx, socket, r)
	if err != nil {
		return err
	}
	opt := output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()}
	if resp.Page != nil {
		return output.EmitList(resp.Page.Items, resp.Page.Next, resp.Page.HasMore, opt)
	}
	return output.Emit(resp.Item, opt)
}
func (s *appState) guard() error {
	v := os.Getenv("WECOM_CHANNEL_CLI_READ_ONLY")
	if v == "" {
		return nil
	}
	ro, err := strconv.ParseBool(v)
	if err != nil {
		return e.New(e.CategoryConfig, "BAD_READ_ONLY", "WECOM_CHANNEL_CLI_READ_ONLY must be a boolean.")
	}
	if ro && !s.allowWrites {
		return e.New(e.CategoryPermission, "READONLY_BLOCKED", "This CLI session is read-only.").WithHint("Use --dry-run or an authorized --allow-writes; broker policy remains enforced.")
	}
	return nil
}
