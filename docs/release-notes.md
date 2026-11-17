# WeCom Channel CLI 0.4.3

Version 0.4.3 improves broker recovery and host supervision. Companion Skill
0.1.13 distinguishes local availability, upstream authentication and uncertain
writes without inventing a checksum-match gate for an authorized restart.

## Changes

- UNSAFE_PATH now identifies the failing directory or ancestor and a stable
  reason, retaining all ownership, permission and symlink checks. Diagnostics
  contain no file contents or raw OS errors.
- Bound broker restarts to five starts per 300 seconds with a 10-second delay.
  Deterministic configuration/authentication/permission failures stop for repair.
  Unexpected clean exits and panic exits use the bounded recovery budget.
- Add independent systemd timers for local readiness and upstream authentication.
  Each check has at most three attempts and hard process deadlines, and reports
  only health metadata. The probes receive no Secret environment and never restart
  services, reset state or replay a write.
- Connect broker, relay and probe failures to a local journal/syslog alert unit.
  Document installation, external-notification integration, maintenance and host
  acceptance tests. Local alerts do not guarantee external delivery.

## Upgrade and deployment

```sh
npm install -g @angelmsger/wecom-channel-cli@0.4.3
wecom-channel-cli version
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Reload the agent's Skill after installation. If installed content differs, inspect
it before explicitly replacing it. Existing 0.4.2 policy, proposal and relay state
remain compatible; no ledger migration or credential change is required. Preserve
all state, terminal delivery latches and unknown outcomes across upgrades.

The deployment helper and systemd units are available in the tagged source under
`deploy/`; the native/npm CLI upgrade does not install or activate them. Follow
[the operator runbook](security.md#supervision-and-health-monitoring), provision
its dedicated health account, and review fixed socket paths before activation.
An existing running service needs a deliberate restart to apply changed settings.
For multiple contexts provision a probe pair per context. Hosts without systemd
need equivalent host-native supervision; model turns are not service monitors.

Readiness probes do not measure queue age or guarantee delivery. Preserve the
0.4.2 scan diagnostics/retry budgets and monitor `relay status` separately; see
[watching and recovery](watching.md). Repair unsafe deployment paths without
changing shared parent permissions blindly. Verify local health, then upstream
authentication. Reconcile uncertain proposals/deliveries before any further send.

Validation uses synthetic credentials, local services and actual CLI processes,
plus bounded health-helper fault tests and systemd static validation. A live
Grokbot service restart, host reboot and external alert delivery require the
operator's acceptance drill; this release does not attest those deployment facts.
