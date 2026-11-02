package app

import (
	"context"
	"os"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

// Discovery runs in the operator's credential environment before any channel
// policy or sockets exist. It exposes only the existing bounded channel read.
func (s *appState) discoveryCommand(newClient func(string, string) (apiclient.Reader, error)) *cobra.Command {
	var bot, cursor string
	var limit int
	var dry bool
	c := &cobra.Command{
		Use: "discover", Short: "Operator-only: discover joined channels from bot credentials before configuring a broker",
		Long: "Run in a trusted operator session with WECOM_CHANNEL_BOT_SECRET supplied by your secret manager.\n" +
			"Only a bot ID and Secret are needed; no channel ID, policy file, running broker or client context is required.\n" +
			"Returns one page of channel names and IDs. Select data.topic_id before configuring policy and client contexts.\n" +
			"Does not save credentials, create sockets or state, or grant agents access. Client context/socket flags do not select this bot.",
		Example: "  wecom-channel-cli broker discover --bot-id BOT_ID\n  wecom-channel-cli broker discover --bot-id BOT_ID --cursor NEXT_CURSOR\n  wecom-channel-cli broker discover --bot-id BOT_ID --dry-run",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id := bot
			if !cmd.Flags().Changed("bot-id") {
				id = os.Getenv("WECOM_CHANNEL_BOT_ID")
			}
			if !apiclient.Identifier(id, 256) {
				return e.New(e.CategoryUsage, "BOT_ID_REQUIRED", "Provide a valid bot ID with --bot-id or WECOM_CHANNEL_BOT_ID; no channel ID is needed.").WithNextSteps("wecom-channel-cli auth guide", "wecom-channel-cli broker discover --help")
			}
			q := apiclient.Query{Operation: "channel.list", Cursor: cursor, Limit: limit}
			// Validate before reading credentials or acquiring an access token.
			if limit < 1 || limit > 50 {
				return e.New(e.CategoryUsage, "BAD_LIMIT", "Channel discovery --limit must be between 1 and 50.")
			}
			plan, _, err := apiclient.BuildRead(q)
			if err != nil {
				return err
			}
			if dry {
				return s.emit(cmd, map[string]any{"dry_run": true, "bot_id": id, "credential_environment": "WECOM_CHANNEL_BOT_SECRET", "request": plan})
			}
			api, err := newClient(id, os.Getenv("WECOM_CHANNEL_BOT_SECRET"))
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
	c.Flags().StringVar(&bot, "bot-id", "", "Bot ID (or WECOM_CHANNEL_BOT_ID); independent of client contexts")
	c.Flags().StringVar(&cursor, "cursor", "", "Continuation cursor from the previous discovery page")
	c.Flags().IntVar(&limit, "limit", 20, "Maximum channels in this page (1–50)")
	c.Flags().BoolVar(&dry, "dry-run", false, "Preview the channel request without credentials, network or local writes")
	return c
}
