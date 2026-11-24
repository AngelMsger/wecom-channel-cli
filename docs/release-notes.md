# WeCom Channel CLI 0.5.0

A durable listener can now resume a whole-channel scan from the last saved
page after a network interruption or broker restart. A separate host check
reports when an expected listener stops, even while the broker stays healthy.

## What changes for operators

- A quiet first pass establishes the baseline. Later passes still check older
  posts for new replies. The broker publishes changes only after the entire
  pass completes. The request cap applies to the entire pass across restarts.
- Transient read failures use the configured scan retry allowance. A confirmed
  expired token permits one recovery within that allowance. Repeated rejection,
  a response over 2 MiB or exhausted capacity leaves a durable reason in
  `relay status`. After repairing a scan, `--retry-scan` preserves delivery attempts.
- The Linux listener timer checks service state, scan freshness and queue age.
  Configure its alert destination and verify receipt there. Paused listeners
  have an explicit monitoring mode. [Listener monitoring](watching.md#monitor-each-expected-listener)

## Upgrade

```sh
npm install -g @angelmsger/wecom-channel-cli@0.5.0
wecom-channel-cli version
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Reload companion Skill 0.1.15. Back up the broker ledger and sender state
together. Install and exercise the per-listener probe, then upgrade the broker
before starting the upgraded relay. Keep intentionally paused instances paused.
Review capacity for large channels. A channel needing at least 102 requests per
pass still exceeds the default cap of 100. If rolling back, reconcile queued and
inflight work before restoring compatible state. [Deployment guide](security.md) ·
[Polling and recovery](watching.md) · [Issue #18](https://github.com/AngelMsger/wecom-channel-cli/issues/18) ·
[Issue #19](https://github.com/AngelMsger/wecom-channel-cli/issues/19)
