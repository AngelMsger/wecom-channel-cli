# WeCom Channel CLI 0.4.2

Version 0.4.2 fixes the scan-diagnostic gap in Issue #13 and improves unattended
relay resilience without increasing webhook delivery retries. Companion Skill
0.1.12 documents the recovery and monitoring contract.

## Changes

- Save sanitized scan failures before testing the retry budget. Even a zero-budget
  stop retains the original error code/cause, phase, timestamp, attempt and budget
  in exhaustion details, notices and `relay status`, including after restart.
  Retain corresponding delivery error history without weakening terminal latches.
- Add optional `scan_retry_limit` and `delivery_retry_limit`. Omitted/null fields
  inherit `retry_limit`; explicit zero disables retries for that phase. The Grok
  example now allows twenty scan retries and zero delivery retries.
- Use capped exponential scan backoff with jitter, respect valid upstream
  Retry-After guidance and keep the wait deadline across restart. Preserve hashed
  request correlation when supplied. Retry temporary broker saturation and safely
  discarded unstable snapshots; retain known polling intervals on restart.
- Preserve queue age through batch sealing/retries and document ordinary process
  supervision, exit/scan-staleness/queue-age alerts, safe shell exit capture and
  authorized end-to-end acceptance checks.

The original reported incident's scan cause remains unknown: this release cannot
recover diagnostics older binaries never recorded. A synthetic HTTP 200 alone is
not evidence of a working scan → acceptance → worker-claim path.

## Upgrade and recovery

```sh
npm install -g @angelmsger/wecom-channel-cli@0.4.2
wecom-channel-cli version
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Reload the agent's Skill after installation. Upgrade the broker as well as the
relay CLI to obtain upstream throttling metadata. Existing configurations and
saved state remain readable with their original settings; omitted phase limits
preserve the consumer digest. The old Grok example's shared `retry_limit: 0` still
stops on the first retryable scan failure. The new example is a provisioning
choice, not an automatic edit to existing deployments.

Stop and reconcile saved work before changing a bound configuration or provisioning
a new consumer. Do not delete state, strip diagnostic fields for a downgrade or
replay history as automatic migration. Older binaries may reject newer snapshots.
After repairing an exhausted budget, an operator may explicitly use `--retry-failed`
once with the same configuration. Never put that flag in a supervisor restart loop.

Unknown HTTP outcomes and interrupted `sending` still stop before scanning or
replaying; use `relay resolve` only with verified host evidence. Terminal delivery
errors remain latched until explicit repair/reset. Worker claims never expire
into automatic business-work replay. HTTP acceptance is not processing completion,
and local deduplication cannot prevent a paid host run that already started.

See [Grok deployment and monitoring](watching.md#supervision-and-activation-checklist)
and [recovery](watching.md#resolve-uncertainty-and-preserve-recovery-evidence).
Validation uses synthetic services and actual CLI processes; no live Grok routine,
channel write or production supervision deployment is part of these checks.
