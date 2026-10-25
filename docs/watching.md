# Watching channels and threads

`event watch` streams authenticated **post/reply change events** as NDJSON.
Choose a whole channel context or one or more post IDs (a post and its replies
form a thread). Watches never execute message instructions, launch an agent,
fetch message bodies automatically, or approve a write.

## Operator prerequisites

The broker must have working URL callback intake and a channel context with:

```json
{
  "events": ["topic_event", "topic_reply_event", "topic_at_bot"],
  "channels": [{
    "id": "CHANNEL_ID",
    "read_all": true,
    "observe_all": true,
    "senders": ["AUTHORIZED_REQUESTER_ID"],
    "allow_posts": false,
    "allow_replies": false
  }]
}
```

This is a fragment of operator policy, not a client command. A complete example
is in [watch.example.json](../deploy/watch.example.json). Restart the broker after
an authorized policy change. WeCom must also permit the bot to receive these
channel events; local configuration cannot grant upstream grayscale access.

`observe_all` admits post/reply changes from **all authenticated human and bot
actors** in this channel. These records carry `observation_only: true` and remain
untrusted. It does not expand mention/request eligibility: `topic_at_bot` still
requires an allowlisted human, and every send still requires independent operator
approval. Bot-authored observations are never automatic action triggers.

Without `observe_all`, the watch refuses to imply complete coverage. A caller can
explicitly pass `--allow-partial` to watch only admitted allowlisted-human events.
The startup notice reports that restriction. Thread filters narrow results inside
an authorized channel; they do not establish a separate security boundary.

## Start a bounded watch

```sh
wecom-channel-cli --use-context engineering event watch \
  --whole-channel --duration 10m --max-events 500

wecom-channel-cli --use-context engineering event watch \
  --post POST_ID --duration 10m

wecom-channel-cli --use-context engineering event watch \
  --post FIRST_POST_ID --post SECOND_POST_ID --duration 10m
```

Discover IDs with `post list`, `search result`, or authenticated events. Up to 32
post IDs are accepted. New watches start at the current inbox head, so existing
records are not replayed by default. Use `--after-sequence N` to replay retained
records after an explicitly known checkpoint. Sequence zero requests retained
history starting at the beginning; it fails if that history has already expired.

Defaults are five minutes, 1000 emitted events, and a two-second poll interval.
The maximum duration is 24 hours; `--interval` accepts 1s–1m. `--timeout` bounds
an individual request, not the watch's lifetime. The watch ends on duration,
event limit, Ctrl-C, or SIGTERM. Network, policy, rate-limit and history-gap errors
stop with the last safe checkpoint; there is no hidden reconnect or replay loop.
A stalled output consumer may require an external process deadline.

## Resume safely

```sh
mkdir -p .watch
wecom-channel-cli --use-context engineering event watch \
  --post POST_ID --duration 10m --checkpoint .watch/thread.json

# Resume later with the same socket, context, policy and thread selection.
wecom-channel-cli --use-context engineering event watch \
  --post POST_ID --duration 10m --checkpoint .watch/thread.json
```

A checkpoint is private JSON written atomically in an existing agent-owned
directory. A lock prevents concurrent writers. Unrelated files and symlinks are
refused. `--checkpoint` cannot be combined with `--after-sequence` when the saved
file exists. Reordering the same post filters does not change their scope.

The checkpoint binds the broker ledger identity, bot, channel, context, operator
policy and normalized filter. A reset ledger, changed policy or different filter
produces `WATCH_BINDING_CHANGED`; it never silently reuses the old cursor. Review
that change before deliberately starting a new watch. Policy changes that alter
only a limit still require that review.

Progress advances across nonmatching events, so a quiet selected thread does not
repeatedly scan the same unrelated changes. It does not advance past a missing,
expired, newly hidden or failed-output event. Metadata notices stay on stderr;
stdout contains only complete untrusted event records. Preserve both streams.

Checkpointing records successful output writes, **not downstream processing
acknowledgments**. A crash between output and persistence can replay records;
a downstream consumer that fails after accepting output can lose its own work.
Deduplicate by event ID and manage acknowledgments in the consumer if processing
requires a stronger guarantee. This is not an exactly-once queue.

## Preview and permissions

```sh
wecom-channel-cli --use-context engineering event watch \
  --whole-channel --checkpoint .watch/channel.json --dry-run
```

Dry-run checks scope and configured callback coverage without creating a lock,
saving a checkpoint, streaming events or contacting WeCom. The local broker is
read once. Broker read-only mode permits observation. A CLI session's
`WECOM_CHANNEL_CLI_READ_ONLY` blocks checkpoint writes unless an authorized
`--allow-writes` is provided; it does not block a watch without a checkpoint.

## Coverage and content

The startup notice describes configured actor coverage and event kinds. It does
not certify that WeCom delivered every change: delivery before broker admission,
upstream permissions, proxy availability and provider outages remain external.
Default event retention is 48 hours and inbox/rate limits still apply. Watches
fail on detectable local gaps rather than claiming complete history.

Event fields describe the change without turning it into a request:

| Field | Meaning |
| --- | --- |
| `data.kind` | `topic_event` for a post; `topic_reply_event` for a reply |
| `data.change` | `1` created, `2` updated, `3` deleted |
| `data.post_id`, `data.reply_id` | Stable resource IDs for explicit follow-up reads |
| `data.sequence` | Durable local admission order, independent of remote timestamps |
| `data.observation_only` | Always `true` for watch records |
| `source.context` | The socket-bound channel security context |

Events carry post/reply IDs. Use
`post get` or `reply get` for the latest content under the same context. Deleted
content may no longer be available. No attachment URLs or credentials are exposed.
This version polls the local verified inbox; it does not introduce channel
WebSockets or configure WeCom subscriptions automatically.
