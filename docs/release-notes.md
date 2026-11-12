# WeCom Channel CLI 0.4.0

Deliver channel changes directly to Grok routine webhooks without periodic AI
polling or an ACK-only receiver service.

- Add opt-in `delivery_mode: "http_accepted"`. Exactly HTTP 200 records host
  acceptance, separately from processing completion. Preserve the existing
  `explicit_ack` mode for receivers with durable idempotent handoff.
- Persist sending before HTTP and retain accepted metadata batches for recovery.
  Missing responses and interrupted sending stop as `unknown`; `relay resolve`
  records verified host evidence before any retry. Disable implicit HTTP POST
  replay even when an `Idempotency-Key` header is present.
- Add worker-owned `relay claim/consume` and state queries. Atomic batch/event
  deduplication grants work once per retained worker ledger, with explicit
  processed/failed results. Worker files do not need sender credentials.
- Keep durable queues in the existing atomic JSON store, with optional bounded
  `--ephemeral` memory delivery when interruption loss is acceptable. SQLite is
  not required. Idle scans make no webhook/model requests.
- Provide per-context Grok configuration, request pacing, supervision and recovery
  guidance. Ship companion Skill 0.1.10 and Go toolchain 1.27.4.

## Upgrade

```sh
npm install -g @angelmsger/wecom-channel-cli@0.4.0
wecom-channel-cli version
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Reload the agent's Skill after installation. Existing 0.3.0 explicit-ACK relay
configurations retain their default behavior and saved consumer digest. Keep the
same config/socket/scope when resuming their queues. Switching a saved relay to
`http_accepted` changes its bound configuration: stop and reconcile old work,
then provision a new consumer with its own state path and quiet current baseline.
Do not delete pending state or use history replay as automatic migration.
Broker policies, permissions and observation storage are unchanged. The self-bot
exclusion migration still applies when upgrading from versions older than 0.3.0;
see [self exclusion](watching.md#self-exclusion).

Use separate sender and worker state per channel. `--ephemeral` loses pending
work and uncertainty state on interruption; broker persistence remains enabled.
Worker claims never expire into automatic replay. Capacity limits stop without
evicting events; retain deduplication history for the possible replay lifetime.
See [Grok setup and recovery](watching.md#direct-grok-routine-webhook).

The official Grok contract was reviewed on 2026-11-12. HTTP acceptance does not
prove processing finished, and local claims cannot prevent duplicated AI starts
that already occurred at the host. Network ambiguity requires reconciliation;
external side effects still need their own idempotency and independent approval.

Validation uses synthetic services and actual CLI processes, including empty-scan
silence, quiet restarts, lost-response handling, claims and completion. No live
Grok routine or channel write is used. Verify the actual host integration before
enabling continuous production monitoring.
