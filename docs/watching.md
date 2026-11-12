# Watching channels and threads

`event watch` streams **post/reply observations** as NDJSON through one interface.
Choose a whole channel context or one or more post IDs (a post and its replies
form a thread). Without a callback listener, the default `auto` backend polls the
channel HTTP APIs: no public address, tunnel or callback keys are needed. Existing
callback deployments retain their event intake. Watches never execute message
instructions, launch an agent or approve a write.

## Operator prerequisites

For first-time setup, use the operator's Bot ID and Secret with `channel list --bot-id BOT_ID`
to obtain `data.topic_id` before filling `CHANNEL_ID`; see
[operator setup](../README.md#operator-setup). The WeCom client does not display it.
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

In callback mode, `observe_all` admits post/reply changes from **authenticated
humans and other bots** in this channel. This bot's own events are always excluded.
All observation records carry
`observation_only: true` and remain
untrusted. It does not expand mention/request eligibility: `topic_at_bot` still
requires an allowlisted human, and every send still requires independent operator
approval. Bot-authored observations are never automatic action triggers.

In callback mode, without `observe_all`, the watch refuses to imply complete coverage. A caller can
explicitly pass `--allow-partial` to watch only admitted allowlisted-human events.
The startup notice reports that restriction. Thread filters narrow results inside
an authorized channel; they do not establish a separate security boundary.

## Self exclusion

Exclusion uses the bot ID bound to the broker, never a process ID, local send
history, display name or message text. If clients A and B share that bot ID,
A's messages are excluded from B's event observations as well as A's own.
`event list/get`, `event watch` and `relay run` apply this rule automatically.
Explicit content reads such as `post get` and `reply get` still return their
requested resources.

- Callback intake acknowledges authenticated self-bot actors with HTTP 200 and
  does not enqueue them. Retained self callbacks are skipped without creating
  false sequence gaps; real missing/expired records still stop replay.
- Polling excludes posts and replies **authored by this bot**, including later
  edits and recalls. It still scans other authors' replies to this bot's posts.
  `creator`/`creator_info` and `aibotid`/`botid` must identify one consistent
  author. Missing, malformed or conflicting authors cause `POLL_CREATOR_UNKNOWN`
  without committing any part of the snapshot. Ask the operator to verify the
  upstream metadata or provision authenticated callbacks if authors are unavailable.
- Polling emits `resource_creator_id` and `resource_creator_kind` separately;
  `sender_kind` stays `unknown`. This is a resource-author exclusion, not a guess
  about editors. Unknown changes to other authors' content remain eligible.
  Different bot identities can still trigger each other; this rule prevents
  self-content feedback, not every possible automation loop.

Self exclusion cannot be disabled by `observe_all` or the relay's legacy
`exclude_self` setting. Callback `coverage.actors` reports
`all_users_and_other_bots` (or `allowlisted_humans`); polling reports `unknown`
and explains the resource filter in its coverage note.

**Upgrade:** the exclusion revision changes watch/relay bindings. Existing
checkpoints and saved queues stop with `WATCH_BINDING_CHANGED` before dispatch;
preserve them and reconcile pending work with the receiver before deliberately
provisioning a new checkpoint/consumer at the current head. Do not delete state
or retry with a changed binding automatically. Old polling observations without
creator evidence are unavailable and cause `EVENT_GAP` on replay. New watches
start at the current head as usual. Upgrade the broker and consuming CLI together.

## Start a bounded watch

```sh
wecom-channel-cli --use-context engineering event watch \
  --whole-channel --duration 10m --max-events 500

wecom-channel-cli --use-context engineering event watch \
  --post POST_ID --duration 10m

wecom-channel-cli --use-context engineering event watch \
  --post FIRST_POST_ID --post SECOND_POST_ID --duration 10m
```

Discover IDs with `post list`, `search get`, or authenticated events. Up to 32
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
requires a stronger guarantee. This is not an exactly-once queue. For an integrated
durable consumer, use the opt-in relay described below.

## Relay events without model polling

`event watch` and the broker do not invoke a model. Scheduling a Bot to run them
every few minutes still pays for a model turn even when stdout is empty. Repeated
model-driven `reply list` calls also put unrelated history back into context.
Run `relay run` as ordinary supervised software instead: it collects from the
same `event.scan` interface, maintains a bounded queue and calls a fixed receiver only for
nonempty eligible batches. Empty scans and health checks make zero receiver or
model calls. There is no model SDK or shell-command adapter in the relay.

The existing `event watch` contract is unchanged. Relay configuration and state
belong to a separate trusted service account, **not the model worker**. The relay
needs only its channel's agent socket and a receiver credential; never give it
the WeCom Secret, broker state, or operator socket. No destination or credential
is sent through the broker API.

### Provision and preview

Copy [relay.example.json](../deploy/relay.example.json) to a protected service
directory, set its fixed receiver URL, and install it with mode 0600, owned by
root or the relay account. Every ancestor must be root/service-owned and not
writable by others. HTTPS is required except for a literal loopback IP. URLs
cannot contain userinfo, query strings or fragments. Put the optional bearer
token in the environment variable named by `token_env`; an empty `token_env`
omits authentication only in `explicit_ack` mode. `http_accepted` requires a token. Neither credentials nor destination URLs appear in status.
The HTTP client follows no redirects, uses no ambient proxy and does not log
headers, bodies, or URL-bearing errors.

Create a private 0700 relay-owned state directory, then preview with the same
context and scope that the service will use:

```sh
wecom-channel-cli --use-context engineering relay run \
  --whole-channel --config /etc/wecom-relay/relay.json \
  --state /var/lib/wecom-relay/state.json --duration 24h --dry-run

wecom-channel-cli --use-context engineering relay run \
  --whole-channel --config /etc/wecom-relay/relay.json \
  --state /var/lib/wecom-relay/state.json --duration 24h

wecom-channel-cli relay status --state /var/lib/wecom-relay/state.json
```

Dry-run validates the same config, scope, saved binding and broker coverage but
does not read the token environment, call WeCom/the receiver, lock or write files.
Normal relay execution is gated by `WECOM_CHANNEL_CLI_READ_ONLY`; broker read-only
mode still permits observation. A new relay starts with a quiet baseline unless
`--after-sequence` explicitly requests retained history. Existing watch checkpoints
cannot serve as relay state: output success cannot reconstruct processing success.
For migration, stop the old consumer and deliberately select a known safe sequence;
downstream deduplication must cover any overlapping events.

[wecom-channel-relay.service](../deploy/wecom-channel-relay.service) is a Linux
supervision example. Provision `wecom-relay` separately from the worker, give it
the agent socket group, and match the unit's socket path to the actual broker.
Use a root-owned 0600 environment file and relay-readable 0600 config. The unit
renews bounded sessions and restarts signal crashes, but stops on structured CLI
errors. Wire service failure and queue-age monitoring to an ordinary operator
monitor, not a periodic model prompt. The unit is a deployment reference, not a
claim of a tested production installation or Grok-host compatibility.

`relay status` reads only the atomic local snapshot, even while the service runs.
It exposes queued/collected/filtered/acknowledged counts, inflight ID and attempts,
coverage, and last-scan/last-ack timestamps (Unix milliseconds). `initialized`
is false until the first successful broker handshake; startup retry budgets are
already durable in that state. Status cannot prove
process liveness; compare timestamps with the configured scan interval and use
the supervisor for exit/liveness checks. Normal relay stdout is empty; structured
startup, retry, acknowledgment, stop and failure notices go to stderr.

### Filters and batching

| Configuration | Default / range | Meaning |
| --- | --- | --- |
| `delivery_mode` | `explicit_ack` / `http_accepted` | Exact batch ACK or the documented Grok HTTP-200 acceptance contract |
| `min_delivery_interval_seconds` | 0 / 0–3600 | Minimum time between POST attempts per relay, persisted with durable state |
| `kinds` | Both post/reply observation kinds | Admit only the named event kinds |
| `changes` | `[1,2,3]` | Admit create/update/recall-or-delete observations |
| `exclude_self` | `true` | Compatibility field; self exclusion is mandatory even if set to `false` |
| `exclude_bots` | `[]`, up to 100 exact bot IDs | Exclude known bot actors; discover IDs from callback events |
| `batch_window_seconds` | 5 / 1–300 | Maximum collection window from the oldest pending event |
| `batch_size` | 50 / 1–100 | Maximum events per webhook request; full batches can dispatch early |
| `queue_limit` | 1000 / batch size–10000 | Pending plus inflight event bound |
| `timeout_seconds` | 30 / 1–30 | Webhook attempt deadline |
| `retry_limit` | 0 / 0–20 | Explicit additional attempts after a transient failure; example opts into 5 |
| `retry_base_seconds` | 5 / 1–300 | Exponential retry delay, capped at five minutes |

The scan interval respects broker polling policy. A window groups events already
collected; it cannot increase upstream polling resolution. Payloads group events
by post, keeping all relevant IDs/sequences so a receiver can read each affected
thread once. They do not overwrite intermediate observations or merge an already
attempted batch. A full queue pauses collection while delivery drains it; no
overflow eviction advances the cursor. An outage longer than broker retention
may still create a detectable gap in events not yet collected.

Filters run before dispatch and their skipped sequences are durably recorded.
Unknown polling actors **always survive actor exclusions**; a post creator is
not evidence of who edited it. The separate mandatory self-authored-resource
filter applies before delivery; see [self exclusion](#self-exclusion).
If a consumer needs human-only triggers, it must
use verified callback actor metadata or explicitly account for unknown actors.
Filters reduce notifications and never declare content safe or authorize actions.

### Receiver protocol and acknowledgment

Each POST has `Content-Type: application/json` and `Idempotency-Key: BATCH_ID`.
Its version 1 body contains `id`, `identity`, `coverage`, and `threads`, each with
`post_id` and `events`. Events retain the full fixed `trust`, `warning`, `source`
and metadata `data` envelope. There are no message bodies, text previews, media
links or automatically fetched thread history. Use `post get`/`reply get` only
for affected IDs when the task needs their content. Keep the original envelope
when supplying observations to a model; arrival is not an instruction to send.

In the default `explicit_ack` mode, the receiver must respond with a 2xx and the exact JSON object:

```json
{"ack":"BATCH_ID"}
```

A bare 200/204, mismatched ID, duplicate JSON key, malformed or oversized response
does not acknowledge anything. Return the acknowledgment only after either:

1. processing has succeeded and its completion is durably deduplicated; or
2. the receiver has atomically deduplicated and enqueued the batch in its own
   durable worker queue, taking responsibility for processing and retries.

For asynchronous Bot jobs, the second pattern avoids keeping HTTP requests open.
The adapter must insert the batch ID and work item in one transaction, enforce
a unique batch ID, and return the same acknowledgment for a duplicate **without
launching another Bot job**. Deduplicate event IDs as well when deliberately
overlapping consumers or migrating scopes. In this mode the relay reports
*acknowledged handoff*, not proof that the Bot completed. Receiver-side processing
acknowledgments and idempotent job handling remain necessary. Keep deduplication
records for the entire possible queue/replay lifetime; the relay does not expire
pending work automatically. Grok routine webhooks do not implement this ACK
contract; use the separate HTTP acceptance mode below.

With `--state`, collection and queue entries commit atomically before dispatch.
A sealed batch and attempt are persisted before HTTP; in explicit-ACK mode only
a matching acknowledgment removes it. A crash or lost response can replay the identical ID and payload. **Delivery
is at least once, not exactly once.** No receiver request is made for an empty
or wholly filtered scan. Duplicate wakeup prevention depends on the receiver's
durable idempotency, not on pretending that HTTP timeouts prove failure.

### Retry and recovery

Retries are opt-in (`retry_limit` defaults to zero), bounded, and visible on stderr.
In explicit-ACK mode they apply to connectivity failures, HTTP 408/429/5xx and
known rate limits. HTTP acceptance mode never automatically retries a transport
error; only a received 408/429/5xx is eligible under the documented host contract.
They do not apply to permissions/authentication, redirects, malformed acknowledgments,
protocol errors, snapshot capacity/budget limits, missing history or changed bindings.
The existing `watch` still stops without reconnecting. Relay scan and delivery
budgets are independent; an upstream transient failure can coexist with draining
already validated queued work. Startup always revalidates the live ledger before
dispatching persisted work, so a broker outage can delay that validation.

Attempt counts and next-attempt times survive process restart, including before
the first successful broker handshake. After exhaustion,
repair the receiver/broker and explicitly restart the same invocation with
`--retry-failed`; this resets retry budgets while preserving queued IDs/payloads.
It cannot clear `unknown` or interrupted `sending` in HTTP acceptance mode.
Do not put that flag in a supervisor restart loop. Scope/socket/policy/ledger,
destination, filters and delivery configuration are bound to the state. Changing
them cannot silently redirect, refilter or reinterpret pending work. Restore the
original configuration to drain it, or reconcile it before deliberately creating
a separate consumer. Never delete state or reuse `--after-sequence` as automatic
gap recovery.

### Direct Grok routine webhook

The official [routine documentation](https://cursor.com/help/grok-bot/routines.md)
was reviewed on 2026-11-12; see [research notes](research.md#grok-bot-routine-webhook-review--2026-11-12).
The routine accepts Bearer-authenticated JSON and starts a run on HTTP **200**.
It does not promise custom ACKs, `Idempotency-Key` deduplication or completion
callbacks. Other HTTP responses mean no run started under that documented
contract. This mode is not a generic “all 2xx are successful” adapter.

Use [grok.example.json](../deploy/grok.example.json), with
`delivery_mode: "http_accepted"`, for direct delivery. No ACK-only receiver or
SQLite service is needed. The original version 1 payload stays intact:
`id` is the batch ID, `identity.context` selects the channel, and
`threads[].events[].data` contains the post/reply IDs. Grok accepts arbitrary JSON;
there is no need to invent a second `batch_id`/`events` wire format. The header
`Idempotency-Key` still equals `id`, but is only a correlation key at this host.

The states have distinct meanings:

| State | Evidence | Meaning |
| --- | --- | --- |
| Queued / sending | Local atomic sender file, when `--state` is used | Metadata has been saved; not proof of host acceptance |
| Accepted | HTTP 200, or operator-verified host evidence | A host run was accepted/started; not proof of processing |
| Claimed | Atomic worker claim | One local worker obtained the new event IDs |
| Processed / failed | Matching worker claim token and explicit result | Worker-reported outcome; not independently verified by the webhook |
| Unknown | Missing response, interrupted request, or restart after `sending` | A host run may already exist; automatic replay is blocked |

Acceptance retains the original metadata batch in the sender receipt for
reconciliation. The sender never infers processing success from a 200, a missing
worker claim, or elapsed time. No completion command sends a channel reply.

#### Provision two independent channels

Keep the existing `daily-ai` and `knowledge-lab` broker contexts and permissions.
Provision one private config per context and give both the same routine URL/key
if desired. Configure secrets through the host's protected service environment,
not chat, command arguments, source files, or logs. No secret value belongs in
the example config. Give each relay its own state path:

```sh
wecom-channel-cli --use-context daily-ai relay run --whole-channel \
  --config /etc/wecom-relay/daily-ai.json \
  --state /var/lib/wecom-relay-daily-ai/state.json --duration 24h --dry-run
wecom-channel-cli --use-context knowledge-lab relay run --whole-channel \
  --config /etc/wecom-relay/knowledge-lab.json \
  --state /var/lib/wecom-relay-knowledge-lab/state.json --duration 24h --dry-run
```

Create state directories as 0700 owned by the relay service. Preview is read-only;
it does not prove the remote URL/key works. First use a local mock and the tests
below. An explicitly authorized live trial can then verify the actual routine
before enabling continuous supervision; the provider's Test button starts real
work and consumes usage. Do not start `event watch` or a scheduled AI check beside
these relays. No history replay flag is needed for normal first startup.

The optional Linux [template unit](../deploy/wecom-channel-relay@.service)
uses instance-specific socket/config/state paths. Match its socket paths to the
actual deployment, install the unit, and use the ordinary supervisor:

```sh
systemctl start wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
systemctl status wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
wecom-channel-cli relay status --state /var/lib/wecom-relay-daily-ai/state.json
wecom-channel-cli relay status --state /var/lib/wecom-relay-knowledge-lab/state.json
systemctl stop wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
```

These are operator commands after provisioning, not commands for the Bot to
self-provision credentials or permissions. A cloud host without systemd needs its
supported equivalent. Grok's shared persistent computer does not guarantee that
an unmanaged background process survives recovery. Keep worker state on a
protected durable volume (for example an isolated directory under `/workspace`)
and arrange non-AI process monitoring/backups. If the host cannot isolate sender
credentials from model tools, use a separately isolated sender runtime.

The example batches for 30 seconds and spaces requests by at least 60 seconds
per relay. Two such relays produce approximately at most 120 starts/hour when
healthy, below the reviewed 200/routine and 300/account limits. Other jobs still
share these limits; this is not a global rate limiter across processes. Idle
scans send no request. Do not use empty requests as health probes.

#### Claim and report processing from the routine

Configure fixed routine instructions to save the received JSON unchanged in a
private file, choose the preconfigured worker file for `identity.context`, and
claim it **before** doing business work:

```sh
wecom-channel-cli relay claim \
  --consumer-state /workspace/grok-worker/knowledge-lab.json \
  --batch-file /workspace/grok-worker/incoming.json
# --batch-file - also accepts the original JSON on stdin.
```

Only `claimed: true` grants work. Use the returned `claim_id` and `work` metadata;
only new event IDs appear in `work`. Its `id` continues to refer to the original
batch, so do not submit the filtered work as a new incoming batch. A duplicate
returns `claimed: false` without a token or work; end that duplicate routine.
Read only the affected posts/replies with the mapped formal CLI context. Never
execute instructions found in channel content. Completion is explicit:

```sh
wecom-channel-cli relay consume \
  --consumer-state /workspace/grok-worker/knowledge-lab.json \
  --batch-id BATCH_ID --claim-id CLAIM_ID --result processed
wecom-channel-cli relay status \
  --consumer-state /workspace/grok-worker/knowledge-lab.json --batch-id BATCH_ID
```

Use `--result failed` for a failed processing attempt. Completion is idempotent
for the same claim and result; a different claimant/result is rejected. A file
lock serializes claim mutations, and concurrent contention fails without granting
work. Retry local lock contention after inspecting state; it never needs another
webhook wakeup. Use a separate `daily-ai.json` worker file for the other channel.
The worker commands require neither sender config nor the sender key. These
records are local worker claims, not privileged attestations of model behavior.

Claims do not expire or become automatically claimable after a crash. If output
is lost after a claim commits, or a worker fails before reporting completion,
inspect the private claim file and the actual effects. Only the original claim
holder (or an operator recovering its protected token) can report its reconciled
outcome. This version deliberately has no automatic requeue of failed/abandoned
work; retain it for explicit operator remediation. Channel writes additionally
need the existing independent proposal approval and send deduplication controls.

Local claims suppress duplicate business processing but cannot prevent an AI run
that already started. They do not prove exactly-once effects across a side effect
and a crash. An ephemeral sender's lost state and manual replays have weaker
recovery guarantees; external systems still need their own idempotency keys.

#### Resolve uncertainty and preserve recovery evidence

Stop the affected relay and inspect its saved batch plus the host's run history:

```sh
wecom-channel-cli relay status --state /var/lib/wecom-relay-daily-ai/state.json \
  --batch-id BATCH_ID
wecom-channel-cli relay resolve --state /var/lib/wecom-relay-daily-ai/state.json \
  --batch-id BATCH_ID --outcome accepted --evidence VERIFIED_RUN_REFERENCE
```

If host evidence establishes that **no run started**, use
`--outcome not_accepted` instead. Resume the original invocation after resolving;
only if its known-failure retry budget is exhausted, explicitly add
`--retry-failed` once. Never resolve based only on a timeout, lack of a local
claim, or lack of a channel reply. If the outcome cannot be established, leave
it `unknown` for operator review. This conservative stop avoids automatic extra
paid wakeups; it does not promise uninterrupted delivery through uncertainty.

Sender state is bounded to a 16 MiB snapshot, 10000 queue events, and at most
10000 accepted receipts; worker state is bounded to 16 MiB, 10000 batches and
20000 unique event IDs. Capacity failures stop without evicting work or moving a
cursor past unsaved data. Archive and reconcile completed histories before those
limits, with the service stopped. Retain old worker ledgers for the entire replay
window; rotate only after proving that old batches can no longer arrive. Do not
silently delete state to make room. If retained broker history expires during an
outage, the CLI reports a gap; it cannot recreate missing upstream changes.

#### Optional delivery persistence

`--state FILE` uses the existing atomic JSON store, not SQLite. Use it when
restart recovery matters. `--ephemeral` is mutually exclusive and keeps only a
bounded in-memory delivery queue and receipts; it creates no sender state/lock
file. Its startup starts at the current head. Interruption discards pending work,
retry budgets and uncertain outcomes; changes during downtime are intentionally
not replayed. It still uses the broker's existing bounded persistent observation
ledger/fingerprint index, and does not turn off broker persistence.

This separation keeps watch responsible for producing changes and relay
responsible for dispatching them. A post-ID Set alone would miss edits and new
replies to old posts; the existing fingerprint comparison is retained. A database
backend can be considered when deployment scale needs it, without making it a
prerequisite for a Bot webhook or adding a database service now.

### Acceptance evidence

Local synthetic tests cover idle silence, baseline establishment, both delivery
contracts, exact-200 acceptance without reading a response body, immutable retries,
persist-before-send failures, lost responses and interrupted sending, restart
blocking, explicit resolution, local claim deduplication/completion, private paths,
context binding and request pacing. `make e2e` runs actual CLI processes against
mock broker/webhook services. Existing polling tests cover old-thread replies,
atomic snapshots and failed-page recovery. These tests validate the CLI behavior;
no real Grok routine or channel posting was used, and no AI billing was measured.

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
| `data.resource_creator_id`, `data.resource_creator_kind` | Omitted | Validated resource author, separate from the unknown change actor |

Absence from a response is not proof of deletion. Only an explicit returned
`is_recalled` flag yields change 3. Multiple intermediate edits, briefly visible
posts/replies, reactions and membership changes cannot be reconstructed by this
poller. Polling never synthesizes `topic_at_bot` or grants mention-only reply
authority. `coverage.backend` and its note explain these limits at startup;
`upstream_delivery_verified` remains false for both transports.
