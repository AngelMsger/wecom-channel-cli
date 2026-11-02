package app

import (
	"context"
	"os"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) channelCommands() *cobra.Command {
	g := &cobra.Command{Use: "channel", Short: "List channels during operator setup or through broker policy"}
	g.AddCommand(s.channelListCommand(func(bot, secret string) (apiclient.Reader, error) { return apiclient.New(bot, secret) }))
	return g
}

// Channel listing uses the broker by default. An explicit bot ID selects the
// operator's credential environment before a channel policy or sockets exist.
func (s *appState) channelListCommand(newClient func(string, string) (apiclient.Reader, error)) *cobra.Command {
	var bot, cursor string
	var limit int
	var dry bool
	c := &cobra.Command{
		Use: "list", Short: "List one page of channels; use --bot-id for initial operator setup",
		Long: "List channels through the selected context/socket. The agent socket returns only its allowed channel; the operator socket lists joined channels.\n" +
			"For initial setup, explicitly pass --bot-id and supply WECOM_CHANNEL_BOT_SECRET in a trusted operator environment.\n" +
			"That mode needs no channel ID, policy, sockets or client context, and ignores saved/environment connection settings.\n" +
			"Do not combine --bot-id with explicit --socket or --use-context. Credentials alone never select direct access or recover a failed broker call.\n" +
			"Select data.topic_id by data.topic_name, then configure policy and client contexts. No credentials or configuration are saved.",
		Example: "  wecom-channel-cli channel list --bot-id BOT_ID\n  wecom-channel-cli channel list --bot-id BOT_ID --cursor NEXT_CURSOR\n  wecom-channel-cli --use-context default channel list\n  wecom-channel-cli --socket /run/wecom-channel-admin/admin.sock channel list",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			direct := cmd.Flags().Changed("bot-id")
			if direct && (s.socket != "" || s.useContext != "" || cmd.Flags().Changed("socket") || cmd.Flags().Changed("use-context")) {
				return e.New(e.CategoryUsage, "CHANNEL_CONNECTION_CONFLICT", "Choose --bot-id for operator setup or a broker context/socket, not both.").WithNextSteps("wecom-channel-cli channel list --help")
			}
			if direct && !apiclient.Identifier(bot, 256) {
				return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a valid --bot-id for operator setup; no channel ID is needed.").WithNextSteps("wecom-channel-cli auth guide", "wecom-channel-cli channel list --help")
			}
			q := apiclient.Query{Operation: "channel.list", Cursor: cursor, Limit: limit}
			// Validate before reading credentials or acquiring an access token.
			if limit < 1 || limit > 50 {
				return e.New(e.CategoryUsage, "BAD_LIMIT", "Channel list --limit must be between 1 and 50.")
			}
			plan, _, err := apiclient.BuildRead(q)
			if err != nil {
				return err
			}
			if !direct {
				r := broker.Request{Operation: q.Operation, Query: q}
				if !dry {
					return s.call(cmd, r)
				}
				socket, err := s.resolve(cmd, &r)
				if err != nil {
					return err
				}
				if err = broker.ValidateSocket(socket); err != nil {
					return err
				}
				return s.emit(cmd, map[string]any{"dry_run": true, "connection": "broker", "socket": socket, "request": r})
			}
			if dry {
				return s.emit(cmd, map[string]any{"dry_run": true, "connection": "credentials", "bot_id": bot, "credential_environment": "WECOM_CHANNEL_BOT_SECRET", "request": plan})
			}
			api, err := newClient(bot, os.Getenv("WECOM_CHANNEL_BOT_SECRET"))
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), s.timeout)
			defer cancel()
			page, err := api.Read(ctx, q)
			if err != nil {
				return err
			}
			if err = output.EmitList(page.Items, page.Next, page.HasMore, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()}); err != nil {
				return err
			}
			message := "Select a channel by data.topic_name and use its data.topic_id in operator policy and config set-context --channel. Discovery does not grant read_all or write permission."
			if len(page.Items) == 0 && !page.HasMore {
				message = "No channels on this page. If no earlier page contained the target, verify this bot has joined that channel and has channel API access, then discover again. Do not invent a channel ID."
			}
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"discovery": message, "next_steps": []string{"wecom-channel-cli config example --help", "wecom-channel-cli broker serve --help", "wecom-channel-cli config set-context --help"}}})
			return nil
		},
	}
	c.Flags().StringVar(&bot, "bot-id", "", "Use operator credentials to list joined channels before broker setup; Secret comes from WECOM_CHANNEL_BOT_SECRET")
	c.Flags().StringVar(&cursor, "cursor", "", "Continuation cursor from the previous channel list page")
	c.Flags().IntVar(&limit, "limit", 20, "Maximum channels in this page (1–50)")
	c.Flags().BoolVar(&dry, "dry-run", false, "Preview the channel request without credentials, network or local writes")
	return c
}
