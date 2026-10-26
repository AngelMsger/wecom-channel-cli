package app

import (
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/spf13/cobra"
)

func (s *appState) reactionCommands() *cobra.Command {
	group := &cobra.Command{Use: "reaction", Short: "Inspect or propose native emoji reactions on posts and replies"}
	group.AddCommand(&cobra.Command{Use: "types", Short: "List the 115 documented native emoji types offline", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return output.EmitList(apiclient.EmojiTypes(), "", false, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
		}})
	r := broker.Request{Operation: "reaction.list"}
	list := &cobra.Command{Use: "list", Short: "Read native reactions and the bot's membership on one target", Args: cobra.NoArgs,
		Long: "Read one post or reply's emoji_list through the content API. A matching\n--event permits mention-scoped reads; otherwise the agent needs read_all.\nEach item includes bot_reacted for this broker's bot. No message body is returned.",
		RunE: func(cmd *cobra.Command, _ []string) error { return s.call(cmd, r) }}
	list.Flags().StringVar(&r.Query.Channel, "channel", "", "Channel ID from channel list or an authenticated event")
	list.Flags().StringVar(&r.Query.Post, "post", "", "Exact post ID from post list, search result or event get")
	list.Flags().StringVar(&r.Query.Reply, "reply", "", "Optional reply ID; omit to inspect the post itself")
	list.Flags().StringVar(&r.ID, "event", "", "Matching authenticated mention event for a mention-only read")
	group.AddCommand(list)
	w := apiclient.WriteRequest{Kind: "reaction"}
	var dry bool
	propose := &cobra.Command{Use: "propose", Short: "Propose adding or removing a native emoji; never sends immediately", Args: cobra.NoArgs,
		Long:    "Create an immutable native post_emoji proposal. Choose --action add/remove\nand a type from reaction types. The operator must enable allow_reactions and\napprove the exact target, action and emoji through the private socket. Mention\nscope requires an authenticated --event within 24h; channel scope requires the\noperator to verify proactive bot permissions. Poll observations grant no authority.",
		Example: "  wecom-channel-cli --use-context engineering reaction propose --post POST_ID --emoji-type 1 --action add --key reaction-1 --dry-run\n  wecom-channel-cli --use-context engineering reaction propose --post POST_ID --reply REPLY_ID --emoji-type 3 --action remove --event EVENT_ID --key reaction-2 --dry-run",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return s.call(cmd, broker.Request{Operation: "proposal.create", Write: &w, DryRun: dry})
		}}
	f := propose.Flags()
	f.StringVar(&w.Channel, "channel", "", "Exact channel ID")
	f.StringVar(&w.Post, "post", "", "Exact post ID to react to or containing the target reply")
	f.StringVar(&w.Reply, "reply", "", "Optional exact reply ID; omit to react to the post")
	f.IntVar(&w.EmojiType, "emoji-type", 0, "Native enum value from reaction types (not a Unicode emoji)")
	f.StringVar(&w.Action, "action", "", "Required: add or remove this bot's native reaction")
	f.StringVar(&w.Event, "event", "", "Matching authenticated mention event; required by mentioned scope")
	f.StringVar(&w.Key, "key", "", "Stable idempotency key; use a new key for a separately reviewed action")
	f.BoolVar(&dry, "dry-run", false, "Preview the exact native action without state changes or remote requests")
	group.AddCommand(propose)
	return group
}
