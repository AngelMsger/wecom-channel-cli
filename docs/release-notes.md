# WeCom Channel CLI 0.3.0

Prevent feedback loops when multiple clients use the same WeCom bot identity.

- Always exclude authenticated self-bot callbacks and polled posts/replies authored
  by this bot, including messages sent through another client. Other authors'
  replies to its posts remain eligible. Direct content reads remain available.
- Validate both creator shapes and bot-ID aliases during polling. Missing,
  malformed or conflicting creators stop the whole snapshot without advancing
  fingerprints. Resource authors are separate from unknown change actors.
- Add `relay run/status` for bounded, durable webhook delivery with immutable
  batches, explicit acknowledgments, persisted retry budgets and no idle model
  calls. Receivers must deduplicate before launching work; delivery is at least once.
- Consolidate commands under `policy`, `proposal`, `search get` and `doctor`.
  Published aliases remain compatible and emit migration notices on stderr.
- Ship companion Skill 0.1.9 and agent-cli-kit 0.8.1 with updated read-only errors.

## Upgrade

```sh
npm install -g @angelmsger/wecom-channel-cli@0.3.0
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Upgrade the broker and consuming CLI together, then reload the agent's Skill.
Self exclusion is mandatory, including with legacy relay `exclude_self: false`.
Existing watch checkpoints and saved relay queues stop with `WATCH_BINDING_CHANGED`
before delivery because their filter binding changed. Preserve them and reconcile
pending work with the receiver before deliberately provisioning a new checkpoint
or consumer at the current head. Old polling observations lacking author evidence
cannot be replayed. See [self exclusion and migration](watching.md#self-exclusion).

Broker policies and client contexts remain valid. Missing polling creator metadata
now reports `POLL_CREATOR_UNKNOWN`; the operator must verify upstream metadata or
provision authenticated callbacks if it is unavailable. Polling still cannot
attribute edits to other authors' content, reconstruct every change, or prevent
loops between different bot identities.

Validation uses synthetic callbacks, local source/receiver services and real CLI
processes. Live tenant behavior and production worker isolation require verification
in the operator's deployment; no live WeCom messages are sent during these checks.
