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
same `event.scan` interface, persists a queue and delivers nonempty eligible
batches. Without `--config`, the destination is the built-in SQLite inbox at
`STATE.inbox.sqlite`; `--inbox FILE` selects another local path. No HTTP service,
external database, token or model runtime is required for local receipt.
An explicit `--config` retains the existing webhook destination or selects an
absolute `inbox_path`. Empty scans and health checks make zero receiver or
model calls. There is no model SDK or shell-command adapter in the relay.

The existing `event watch` contract is unchanged. Relay configuration and state
belong to a separate trusted service account, **not the model worker**. The relay
needs its channel's agent socket and, only for authenticated webhook delivery,
a receiver credential. Never give it the WeCom Secret, broker state, or operator socket. No destination or credential
is sent through the broker API.

### Local receipt for two channels

Use a persistent local disk or persistent cloud volume, with correct file locking
and fsync semantics. An ephemeral container filesystem is not durable across
host replacement; SQLite does not provide replication or host-loss recovery.
Run these commands as the trusted relay service identity, outside the model
worker. Existing `daily-ai` and `knowledge-lab` client contexts must already point
to their separate authorized broker sockets. No broker permission changes are
needed. Create private directories under a service-owned persistent location:

```sh
install -d -m 0700 /var/lib/wecom-relay/daily-ai
install -d -m 0700 /var/lib/wecom-relay/knowledge-lab

wecom-channel-cli --use-context daily-ai relay run --whole-channel \
  --state /var/lib/wecom-relay/daily-ai/state.json --duration 24h --dry-run
wecom-channel-cli --use-context knowledge-lab relay run --whole-channel \
  --state /var/lib/wecom-relay/knowledge-lab/state.json --duration 24h --dry-run
```

After the previews and local tests pass, run each command without `--dry-run`
under its own supervisor. Only `relay run` is needed; do not also run `event watch`
or scheduled AI inspections. Each new state starts at the broker's quiet baseline;
do not add `--after-sequence 0` unless deliberately replaying retained history.
Collection progress and the inbox are independent durable stages. A crash between
the SQLite commit and relay acknowledgment replays the batch safely.

The [local service template](../deploy/wecom-channel-relay@.service) runs one
instance per client context. Provision its service account, socket access, binary
and client config directory first, then install the unit. The commands below apply
only after that setup and verification:

```sh
systemctl start wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
systemctl status wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
systemctl stop wecom-channel-relay@daily-ai wecom-channel-relay@knowledge-lab
```

For other hosts, use their ordinary process supervisor with the same commands.
SIGTERM/Ctrl-C stops a foreground relay; queued work survives. Renew normal
bounded sessions without an AI turn. A failed service requires repair, not an
automatic `--retry-failed` loop. Inspect both stages independently:

```sh
wecom-channel-cli relay status --state /var/lib/wecom-relay/daily-ai/state.json
wecom-channel-cli inbox status --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite
wecom-channel-cli inbox list --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite \
  --status received --limit 50
wecom-channel-cli inbox get --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite --id BATCH_ID
```

Use the corresponding `knowledge-lab` paths for the other channel. Lists return
`items/next/has_more`; reuse `next` as `--cursor` with the same status filter. Each
database binds to one context/bot/channel/ledger on first receipt; another identity
is rejected, never merged. A batch ID reused with a different payload or an event
ID reused with conflicting metadata also fails closed. The same batch gets the
same acknowledgment without another insert. Overlapping batches insert only new
events into `work`; `original` retains the delivery for audit. An all-duplicate
batch has status `duplicate` and cannot be claimed. Process `work`, not `original`.

To customize batching, filters or scan retry limits, provision a private config
using [inbox.example.json](../deploy/inbox.example.json) and pass `--config FILE`
instead of `--inbox`. Select these settings before creating relay state; later
changes still require the existing binding/migration checks. The default without
a config has no additional transient retries; the example opts into five.

### Receipt and host processing are separate

| Inbox status | Meaning | Automatic replay |
| --- | --- | --- |
| `received` | Metadata and deduplication keys committed; ready for a trusted host adapter | Can be claimed once |
| `dispatching` | Attempt committed before host I/O; outcome may be unknown | Never reclaimed by timeout or restart |
| `submitted` | Adapter recorded a host acceptance receipt | Never interpreted as completion |
| `completed` | Adapter/operator recorded separate completion evidence | Terminal |
| `uncertain` | Host outcome needs reconciliation | Never retried automatically |
| `duplicate` | All events already belong to earlier work | No new work |

`inbox status.received_batches` counts durable receipts, including overlapping
batches; `received_events` counts unique events. `work_states` distinguishes all
stages. `relay status.acknowledged_*` remains a handoff count, not a processing
count. Neither status command probes liveness or calls a model.

This release implements receipt and state tracking, **not a verified Grok
forwarder**. Do not point the relay directly at a Bot wakeup webhook unless it
implements the exact durable acknowledgment contract. A trusted adapter may
inspect the inbox using ordinary software; it must invoke the host only when
claiming new work. No empty batch or health check should start an AI turn.

The adapter claims a specific ID before any host request:

```sh
wecom-channel-cli inbox claim --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite --id BATCH_ID
# Retain attempt_id and process only work. Contact the verified host adapter here.
wecom-channel-cli inbox resolve --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite \
  --id BATCH_ID --attempt ATTEMPT_ID --status submitted --evidence HOST_JOB_ID
# Only after a separate host completion observation:
wecom-channel-cli inbox resolve --db /var/lib/wecom-relay/daily-ai/state.json.inbox.sqlite \
  --id BATCH_ID --attempt ATTEMPT_ID --status completed --evidence COMPLETION_REFERENCE
```

Claim and resolve support `--dry-run` and the CLI read-only guard. They never
send a network request. Resolve records the trusted adapter/operator's assertion;
the CLI does not independently verify a Grok job. Evidence must be a non-secret
receipt/reference, never credentials or a signed URL. The exact attempt ID blocks
stale adapters from changing a newer attempt. HTTP 200 from Grok means at most
`submitted` when its documented contract establishes acceptance. A timeout,
interrupted request or lost claim output leaves `dispatching`/`uncertain`: inspect
`inbox get`, query the host by its idempotency/job key, and reconcile before acting.
Only verified non-submission may be resolved with `--status not_submitted`, which
returns the work to `received`. Absence of evidence or elapsed time is not proof
of non-submission. A recorded `submitted` batch cannot be reset this way.

### Local receiver for existing webhook deployments

For a relay already configured for HTTP, the same storage is available through
an optional loopback receiver. Use one database and receiver port per channel.
Supply the matching bearer token to the receiver and relay through protected
service environments; do not put its value in argv, config JSON or logs:

```sh
wecom-channel-cli inbox serve --db /var/lib/wecom-relay/daily-ai/inbox.sqlite \
  --listen 127.0.0.1:8789 --token-env WECOM_CHANNEL_INBOX_TOKEN --dry-run
wecom-channel-cli inbox serve --db /var/lib/wecom-relay/daily-ai/inbox.sqlite \
  --listen 127.0.0.1:8789 --token-env WECOM_CHANNEL_INBOX_TOKEN
```

Set the protected relay config's `webhook_url` to
`http://127.0.0.1:8789/batches` and `token_env` to `WECOM_CHANNEL_INBOX_TOKEN`.
The listener requires authentication, accepts only bounded version 1 JSON batches,
checks `Idempotency-Key == id`, and returns `{"ack":"ACTUAL_BATCH_ID"}` only after
the transaction commits. Storage failure returns a non-2xx; it never confirms
success. SIGTERM drains active requests and closes the database. Restart the
receiver with the same database, then recover the relay's retry budget if needed.
There is no remote status, claim or arbitrary command endpoint.

### Inbox recovery and external storage

The inbox permits at most 10,000 unfinished unique events. Capacity/disk failures
stop receipt without dropping work; the relay retains its sealed batch and its
existing bounded queue. Completed receipts and deduplication keys are never
automatically expired or pruned. Monitor disk space, oldest pending work and
`dispatching`/`uncertain` counts with ordinary operator tooling, without model
prompts. Deduplication history consumes storage even after work completes.

After a crash, restart the receiver/local relay normally with the same database
to allow SQLite hot-journal recovery. Read-only inspection may fail until recovery
finishes. If the relay stopped after an unconfirmed attempt, inspect the receipt,
repair the problem, then explicitly run the original invocation with
`--retry-failed`. Replaying into the same inbox is safe; replaying the Bot request
without host reconciliation is not. A missing inbox or receipt count older than
acknowledged relay progress fails startup. Never delete state, the database or
journal files as recovery. Stop writers before taking a consistent backup of the
relay state and inbox together; restore both with their original identities and
configuration. A partial/older backup cannot prove that no events were lost.

`internal/inbox.Store` defines atomic receipt and processing transitions; SQLite
is the shipped implementation, using the family's pure-Go driver. An external
database can implement the same semantics in a separate receiver behind the
existing webhook protocol. No external database or plugin installation is required,
and no arbitrary storage module or shell hook is loaded by the CLI. Existing
webhook configs and their binding digests remain compatible. Switching a saved
relay to a new destination still requires the existing deliberate migration:
drain/reconcile it first and preserve receiver deduplication across any overlap.

### Provision an external webhook and preview

Copy [relay.example.json](../deploy/relay.example.json) to a protected service
directory, set its fixed receiver URL, and install it with mode 0600, owned by
root or the relay account. Every ancestor must be root/service-owned and not
writable by others. HTTPS is required except for a literal loopback IP. URLs
cannot contain userinfo, query strings or fragments. Put the optional bearer
token in the environment variable named by `token_env`; an empty `token_env`
omits authentication. Neither credentials nor destination URLs appear in status.
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

The receiver must respond with a 2xx and the exact JSON object:

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
pending work automatically. This endpoint contract must be verified against the
actual Grok host; an arbitrary webhook that starts a Bot is not sufficient.

Collection and queue entries commit atomically before dispatch. A sealed batch
and attempt are persisted before HTTP; only a matching acknowledgment removes
it. A crash or lost response can replay the identical ID and payload. **Delivery
is at least once, not exactly once.** No receiver request is made for an empty
or wholly filtered scan. Duplicate wakeup prevention depends on the receiver's
durable idempotency, not on pretending that HTTP timeouts prove failure.

### Retry and recovery

Retries are opt-in (`retry_limit` defaults to zero), bounded, and visible on stderr.
They apply to known connectivity failures, HTTP 408/429/5xx and known rate limits.
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
Do not put that flag in a supervisor restart loop. Scope/socket/policy/ledger,
destination, filters and delivery configuration are bound to the state. Changing
them cannot silently redirect, refilter or reinterpret pending work. Restore the
original configuration to drain it, or reconcile it before deliberately creating
a separate consumer. Never delete state or reuse `--after-sequence` as automatic
gap recovery.

### Acceptance evidence

Local synthetic tests cover empty/filtered scans making zero requests, grouping
same-thread observations, failed persistence preventing dispatch, explicit ack
validation, immutable retry payloads across process restart, bounded retry/cancel,
binding/gap failures and private-file/lock protection. `make e2e` runs the actual
CLI against mock broker and webhook services. Existing polling tests continue to
cover old-thread replies and snapshot completeness. These tests establish CLI
behavior; they do not measure model billing or verify a live Grok integration.
Inbox tests additionally cover concurrent duplicate receipt/claim, overlapping
events, identity isolation, transaction rollback, full storage, a lost HTTP ack,
hot-journal recovery after process exit, and actual CLI receiver/relay restart.

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
