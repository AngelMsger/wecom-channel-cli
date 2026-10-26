# Watching channels and threads

`event watch` streams **post/reply observations** as NDJSON through one interface.
Choose a whole channel context or one or more post IDs (a post and its replies
form a thread). Without a callback listener, the default `auto` backend polls the
channel HTTP APIs: no public address, tunnel or callback keys are needed. Existing
callback deployments retain their event intake. Watches never execute message
instructions, launch an agent or approve a write.

## Operator prerequisites

For polling, supply the bot Secret to the broker and use a channel context with:

```json
{
  "watch_backend": "auto",
  "poll_interval_seconds": 30,
  "poll_max_requests": 100,
  "poll_max_records": 5000,
  "events": ["topic_event", "topic_reply_event", "topic_at_bot"],
  "channels": [{
    "id": "CHANNEL_ID",
    "read_all": true,
    "observe_all": true,
    "senders": [],
    "allow_posts": false,
    "allow_replies": false
  }]
}
```

This is a fragment of operator policy, not a client command. A complete example
is in [watch.example.json](../deploy/watch.example.json); its `callback_listen`
is empty. Restart after an authorized policy change. WeCom must permit the bot to
read all content in this channel; local policy cannot grant upstream access.

`watch_backend` is operator-controlled: `auto` chooses polling without a callback
listener and callbacks when one is configured. `poll` forces polling; `callback`
requires working authenticated callback intake. There is no silent fallback after
an error, and a backend/policy change invalidates existing checkpoints. When
polling is selected, incoming post/reply callbacks are acknowledged without adding
duplicate observations; configured mention callbacks remain independent.

Polling requires both `read_all` and `observe_all`. Snapshot APIs identify a
resource's creator, not necessarily the person who edited or recalled it. Poll
observations therefore use `sender_kind: unknown`; they cannot be filtered or
authorized as if the creator performed the change. `--allow-partial` does not
bypass this requirement.

In callback mode, `observe_all` admits post/reply changes from **all authenticated
human and bot actors** in this channel. All observation records carry
`observation_only: true` and remain
untrusted. It does not expand mention/request eligibility: `topic_at_bot` still
requires an allowlisted human, and every send still requires independent operator
approval. Bot-authored observations are never automatic action triggers.

In callback mode, without `observe_all`, the watch refuses to imply complete coverage. A caller can
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

Defaults are five minutes, 1000 emitted events, and a two-second local scan interval.
For remote polling, the broker's minimum refresh interval (30 seconds by default)
takes precedence when longer.
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
| `data.change` | Normalized change code; see the transport-specific meanings below |
| `data.post_id`, `data.reply_id` | Stable resource IDs for explicit follow-up reads |
| `data.sequence` | Durable local admission order, independent of remote timestamps |
| `data.observation_only` | Always `true` for watch records |
| `source.context` | The socket-bound channel security context |

Events carry post/reply IDs. Use
`post get` or `reply get` for the latest content under the same context. Deleted
content may no longer be available. No attachment URLs or credentials are exposed.
This version consumes the local durable inbox; it does not introduce channel
WebSockets or configure WeCom subscriptions automatically. With the polling
backend, scans first refresh a bounded remote snapshot when due, then consume
the same durable inbox.

## Polling behavior and limits

The first successful snapshot of a new scope establishes a quiet baseline.
Later snapshots compare curated content, modification times and recall flags.
Only fingerprints, resource IDs and derived event metadata persist; polled message
bodies are neither stored in the polling index nor emitted by the watch. The
broker still reads those bodies to detect changes. Media URLs and credentials
remain excluded by the HTTP client's existing projection.

Whole-channel polling enumerates posts and then each non-recalled post's replies.
Selected-thread polling reads only the selected posts and their replies. Creation
time is not a modification cursor: each cycle revisits the visible snapshot,
using a fixed creation-time window from Unix second 1 to that cycle's start plus
one second. This includes replies and edits on old posts. All pages must complete
within the scan timeout and configured request/record budgets; exhaustion or a
failed page stops the watch without committing a partial baseline. Large channels
may require selected threads, a longer `--timeout`, or an operator-reviewed budget.

The broker defaults to a 30-second minimum interval per scope, configurable from
5 to 3600 seconds. `--interval` cannot shorten it. Concurrent watches of the same
scope share one refresh; overlapping scopes share resource fingerprints and
deduplicate changes. The per-context `requests_per_minute` also bounds remote
reads across scopes. Failures stop the consumer; retrying the same scope before
the minimum interval returns its cached failure rather than another remote attempt.

At most 32 distinct polling scopes and `poll_max_records` indexed resources are
retained per context/policy. The defaults permit 100 data requests per snapshot
and 5000 indexed records, with operator limits up to 1000 and 100000 respectively.
Capacity exhaustion fails explicitly; the index never silently evicts fingerprints.
An operator policy change establishes a new polling baseline without erasing the
event or write ledger. Saved consumer checkpoints must be reviewed separately.

Polling runs only when an active watch asks for a scan. No background poller keeps
running after the watch ends. Restarting resumes fingerprints and retained events,
but changes between snapshots or while nobody watches can coalesce or disappear.
Draining an existing inbox does not depend on remote availability.

The event envelope and IDs stay consistent across backends. Inspect these fields
when a consumer needs to distinguish the evidence:

| Field | Callback | Polling |
| --- | --- | --- |
| `data.transport` | `callback` | `poll` |
| `data.change` | Provider event: 1 created, 2 updated, 3 deleted | 1 first observed after baseline, 2 changed snapshot, 3 returned recall flag |
| `data.change_basis` | Omitted | `first_seen`, `snapshot_changed`, or `recall_flag` |
| `data.created_at` | Provider event time | Local observation time |
| `data.resource_created_at`, `data.resource_modified_at` | Omitted | Resource timestamps from the HTTP response |
| `data.sender_kind` | Authenticated event actor | `unknown`; never substitute the resource creator |

Absence from a response is not proof of deletion. Only an explicit returned
`is_recalled` flag yields change 3. Multiple intermediate edits, briefly visible
posts/replies, reactions and membership changes cannot be reconstructed by this
poller. Polling never synthesizes `topic_at_bot` or grants mention-only reply
authority. `coverage.backend` and its note explain these limits at startup;
`upstream_delivery_verified` remains false for both transports.
