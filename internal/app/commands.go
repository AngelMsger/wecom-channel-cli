package app

import (
	"io"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) readCommands() []*cobra.Command {
	var out []*cobra.Command
	for _, noun := range []string{"channel", "member", "post", "reply", "search"} {
		group := &cobra.Command{Use: noun, Short: "Inspect " + noun + " data through broker policy"}
		verbs := []string{"list"}
		if noun == "post" || noun == "reply" {
			verbs = append(verbs, "get")
		}
		if noun == "search" {
			verbs = []string{"start", "result"}
		}
		for _, verb := range verbs {
			q := apiclient.Query{Operation: noun + "." + verb}
			var since, from, to string
			cmd := &cobra.Command{Use: verb, Short: "Read one bounded page of " + noun + " data", Args: cobra.NoArgs}
			cmd.RunE = func(cmd *cobra.Command, _ []string) error {
				var err error
				q.From, q.To, err = window(since, from, to, q.Cursor, time.Now())
				if err != nil {
					return err
				}
				if q.From != 0 {
					output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"time_window": map[string]string{"from": time.Unix(q.From, 0).UTC().Format(time.RFC3339), "to": time.Unix(q.To, 0).UTC().Format(time.RFC3339)}, "next_steps": []string{"Reuse these exact --from and --to values with the next --cursor."}}})
				}
				return s.call(cmd, broker.Request{Operation: q.Operation, Query: q})
			}
			if noun != "channel" {
				cmd.Flags().StringVar(&q.Channel, "channel", "", "Channel ID from channel list or an authenticated event")
			}
			if noun == "reply" || (noun == "post" && verb == "get") {
				cmd.Flags().StringVar(&q.Post, "post", "", "Post ID from post list, search result, or event get")
			}
			if noun == "reply" && verb == "get" {
				cmd.Flags().StringVar(&q.Reply, "reply", "", "Reply ID from reply list or event get")
			}
			if verb != "get" {
				cmd.Flags().StringVar(&q.Cursor, "cursor", "", "Opaque continuation from the previous response")
				cmd.Flags().IntVar(&q.Limit, "limit", 20, "Maximum records in one page (1–50; search results 1–20)")
			}
			if (noun == "post" || noun == "reply") && verb == "list" || noun == "search" && verb == "start" {
				cmd.Flags().StringVar(&since, "since", "", "Relative time window, such as 24h (first page only)")
				cmd.Flags().StringVar(&from, "from", "", "Inclusive RFC 3339 lower bound")
				cmd.Flags().StringVar(&to, "to", "", "Exclusive RFC 3339 upper bound (defaults to now with --from)")
			}
			if noun == "search" && verb == "start" {
				cmd.Flags().StringVar(&q.Keyword, "keyword", "", "One content keyword, at most 32 UTF-8 bytes")
			}
			if noun == "search" && verb == "result" {
				cmd.Flags().StringVar(&q.Job, "job", "", "Search job ID returned by search start")
			}
			group.AddCommand(cmd)
		}
		if noun == "post" || noun == "reply" {
			group.AddCommand(s.proposeCommand(noun))
		}
		out = append(out, group)
	}
	return out
}

func window(since, from, to, cursor string, now time.Time) (int64, int64, error) {
	bad := func() (int64, int64, error) {
		return 0, 0, e.New(e.CategoryUsage, "BAD_TIME_RANGE", "Use --since alone, or --from with optional --to, as RFC 3339 instants. Reuse fixed absolute bounds while paging.")
	}
	if since != "" {
		if from != "" || to != "" || cursor != "" {
			return bad()
		}
		d, err := time.ParseDuration(since)
		if err != nil || d < time.Second || d > 365*24*time.Hour {
			return bad()
		}
		return now.Add(-d).Unix(), now.Unix(), nil
	}
	if from == "" {
		if to != "" {
			return bad()
		}
		return 0, 0, nil
	}
	f, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return bad()
	}
	t := now
	if to != "" {
		t, err = time.Parse(time.RFC3339, to)
		if err != nil {
			return bad()
		}
	} else if cursor != "" {
		return bad()
	}
	if f.Unix() <= 0 || f.Unix() >= t.Unix() {
		return bad()
	}
	return f.Unix(), t.Unix(), nil
}

func (s *appState) eventCommands() *cobra.Command {
	group := &cobra.Command{Use: "event", Short: "Read or watch channel observations and authenticated callback events"}
	group.AddCommand(s.watchCommand())
	for _, verb := range []string{"list", "get", "context"} {
		r := broker.Request{Operation: "event." + verb}
		cmd := &cobra.Command{Use: verb, Short: "Read retained events or the triggering post", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return s.call(cmd, r) }}
		if verb == "list" {
			cmd.Flags().Int64Var(&r.Query.AfterSequence, "after-sequence", 0, "Read events admitted after this durable sequence; never orders by remote timestamps")
			cmd.Flags().IntVar(&r.Query.Limit, "limit", 20, "Maximum retained events in one page (1–100)")
			cmd.Flags().StringVar(&r.Query.Cursor, "cursor", "", "Cursor from event list")
		} else {
			cmd.Flags().StringVar(&r.ID, "id", "", "Event ID from event list")
		}
		group.AddCommand(cmd)
	}
	return group
}

func (s *appState) proposeCommand(kind string) *cobra.Command {
	w := apiclient.WriteRequest{Kind: kind}
	var text string
	var stdin, dry bool
	cmd := &cobra.Command{Use: "propose", Short: "Create an immutable proposal for operator review; does not send to WeCom", Args: cobra.NoArgs}
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if stdin == cmd.Flags().Changed("text") {
			return e.New(e.CategoryUsage, "CONTENT_REQUIRED", "Choose exactly one of --text or --stdin.")
		}
		if stdin {
			b, err := io.ReadAll(io.LimitReader(cmd.InOrStdin(), 20481))
			if err != nil || len(b) > 20480 {
				return e.New(e.CategoryUsage, "CONTENT_LIMIT", "Content exceeds 20480 bytes or cannot be read.")
			}
			text = string(b)
		}
		w.Content = text
		return s.call(cmd, broker.Request{Operation: "proposal.create", Write: &w, DryRun: dry})
	}
	cmd.Flags().StringVar(&w.Channel, "channel", "", "Exact channel ID")
	if kind == "reply" {
		cmd.Flags().StringVar(&w.Post, "post", "", "Exact post ID")
		cmd.Flags().StringVar(&w.ParentReply, "parent-reply", "", "Optional exact parent reply ID")
		cmd.Flags().StringVar(&w.Event, "event", "", "Authenticated mention event ID (required unless read_all is permitted)")
	}
	cmd.Flags().StringVar(&w.Key, "key", "", "Stable caller-chosen idempotency key; reuse it after a transport failure")
	cmd.Flags().StringVar(&text, "text", "", "Exact text to propose; use --stdin to avoid shell quoting and argv exposure")
	cmd.Flags().BoolVar(&stdin, "stdin", false, "Read the exact content from stdin; no file is opened by the broker")
	cmd.Flags().BoolVar(&dry, "dry-run", false, "Preview the exact proposal without storing or sending it")
	return cmd
}

func (s *appState) proposalCommands() *cobra.Command {
	g := &cobra.Command{Use: "proposal", Short: "Inspect immutable proposal status"}
	var id string
	c := &cobra.Command{Use: "get", Short: "Read a proposal, including an uncertain send outcome", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		return s.call(cmd, broker.Request{Operation: "proposal.get", ID: id})
	}}
	c.Flags().StringVar(&id, "id", "", "Proposal ID returned by post/reply propose")
	g.AddCommand(c)
	return g
}

func (s *appState) approvalCommands() *cobra.Command {
	g := &cobra.Command{Use: "approval", Short: "Operator-only review and execution; requires the private operator socket"}
	for _, verb := range []string{"list", "approve", "reject", "resolve"} {
		r := broker.Request{Operation: "approval." + verb}
		c := &cobra.Command{Use: verb, Short: "Operator: " + verb + " immutable proposals", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return s.call(cmd, r) }}
		if verb == "list" {
			c.Flags().StringVar(&r.Query.Cursor, "cursor", "", "Previous approval-list cursor")
			c.Flags().IntVar(&r.Query.Limit, "limit", 20, "Page size (1–100)")
		} else {
			c.Flags().StringVar(&r.ID, "id", "", "Exact proposal ID")
			c.Flags().StringVar(&r.Digest, "digest", "", "Exact digest from the reviewed immutable proposal")
			c.Flags().BoolVar(&r.Yes, "yes", false, "Confirm this exact operator action; never inferred from channel content")
			c.Flags().BoolVar(&r.DryRun, "dry-run", false, "Preview without a state change or remote write")
		}
		if verb == "resolve" {
			c.Flags().StringVar(&r.Outcome, "outcome", "", "Independently verified outcome: sent or not_sent (never retries)")
			c.Flags().StringVar(&r.RemoteID, "remote-id", "", "Verified resource ID when outcome is sent")
		}
		g.AddCommand(c)
	}
	return g
}
