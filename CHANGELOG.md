# Changelog

## [Unreleased]

### Changed

- Use agent-cli-kit v0.6.0. The CLI session's `READONLY_BLOCKED` (from
  `WECOM_CHANNEL_CLI_READ_ONLY`) now has the family's shared wording: the
  message names the blocked operation (`operation "proposal.create" blocked:
  read-only mode is enabled`), the hint says read-only mode is the user's
  setting, and the next steps are `--dry-run` and, only for a write the user
  authorized, `--allow-writes`. Code, category and exit code are unchanged.
  The broker's own read-only policy is the operator's and keeps its error,
  which `--allow-writes` cannot lift. `pkg/errors` re-exports `ReadOnlyBlocked`,
  `ReadOnlyMode` and `CodeReadOnlyBlocked`.
- Use agent-cli-kit v0.7.0. The `READONLY_BLOCKED` hint names only
  `WECOM_CHANNEL_CLI_READ_ONLY` as the switch the user turned on; with
  v0.6.0 it also named `defaults.read_only`, a setting this CLI does not have.
  The kit's change to its default HTTP client's redirects does not apply:
  this CLI passes its own client, which already followed no redirect.
- Use agent-cli-kit v0.8.1. Its HTTP client changes (a read with a body
  answered by the hop that kept its method, a write answered by its first
  response with any client, a response that always names its request) do not
  change this CLI: its own client follows no redirect, so every request is
  answered by the response it got, as before.

## [0.2.0] — 2026-11-03

### Added

- Extend the existing `channel list` with explicit `--bot-id` operator setup,
  listing joined channel names/IDs from the Secret environment before any channel
  policy, sockets or contexts exist. The default remains broker-based listing;
  there is no second discovery command or automatic credential fallback. Both
  modes support offline dry-run; direct reads retain bounded pagination.
- `config example --bot-id ... --channel ...` fills templates from discovery;
  missing-channel setup errors, auth guidance, docs and companion Skill 0.1.6
  now lead through credentials, discovery, target selection and configuration.
  The first context example matches the generated policy's default name and socket.

- `pkg/apiclient` exports `Reader` and `Writer`; `Client` is now their union,
  with the same method set and signatures as before.

### Changed

- Update `golang.org/x/sys` to v0.44.0, removing GO-2026-5024 from the module
  dependency graph. The vulnerable Windows package is not used by this connector.

- The broker's read-only API wrapper keeps the inner client only as a `Reader`
  and blocks each `Writer` method itself, so a write added without a blocking
  override fails to compile instead of reaching WeCom from a read-only broker.
  No CLI behavior, output or error changes.

- Align release automation with the sibling CLIs: push a version tag to run unit
  tests, cross-compile, checksum binaries, create or refresh the GitHub Release,
  and publish npm through OIDC without a separate environment approval.
- Derive binary version/commit metadata from Git and set npm's version from the
  tag. Replace the custom Python publication pipeline with the family workflow
  and build script; keep Linux/macOS as this connector's supported targets.
- Document the existing approval-free environment reference required by the
  validated npm identity and test publication/rerun paths without remote writes.

## [0.1.0] — 2026-10-27

Published from `3a65bf9` through GitHub Actions with npm provenance.

### Native emoji reactions

- Add reaction types, list and propose commands for native post/reply reactions,
  including explicit add/remove actions through the existing approval ledger.
- Separate opt-in reaction permission and exact-mention/proactive channel scope;
  recheck mention identity and its 24-hour window immediately before execution.
- Accept provider acknowledgments without inventing reaction IDs; reconcile
  uncertain outcomes with target membership reads and no automatic retry.
- Expose curated emoji_list and bot_reacted metadata, retain the official 115-type
  catalog, and exclude membership-only changes from polling fingerprints.
- Add reaction policy examples, generated docs, process/wire/security tests and
  companion Skill 0.1.4. Live native reaction delivery remains unverified.

### Release verification

- Record successful live polling, filtering and checkpoint-resume verification
  with a dedicated test bot, without public ingress or outgoing messages.
- Add a reproducible Linux container check for distinct worker/service identities,
  cross-context denial, private state/credentials, privilege and network limits.
- Reject incomplete/stale release manifests, dirty-source or wrong-platform
  binaries, mismatched checksums and npm archives that differ from reviewed files.
- Keep release retries immutable: verify remote tag/asset hashes, upload missing
  files into a draft and publish the exact verified npm archive without clobbering.

### Polling without public ingress

- Use HTTP polling for event watch when no callback listener is configured;
  retain the command, NDJSON trust envelope, channel context and checkpoints.
- Compare complete bounded snapshots, establish quiet baselines, share resource
  fingerprints across watch scopes and commit derived events atomically.
- Report polling provenance and unknown change actors; never infer deletion from
  absence or manufacture mentions. Honor recall flags returned by WeCom.
- Enforce operator-controlled interval, request/record budgets and scope capacity;
  fail incomplete reads without advancing state and keep dry-run offline.
- Update setup examples, website, deployment guides and companion Skill to 0.1.3.

### Watch support and family presentation

- Add bounded event watch for whole-channel and selected-thread scopes, durable
  checkpoint binding, filtered progress, explicit partial coverage and dry-run.
- Separate all-actor observation policy from human mention/request eligibility.
- Add the README overview image and promote the family slogan, "Built for agents.
  Designed for humans." Normalize the project wordmark and navigation type scale.
- Update the companion Skill to 0.1.2 for watch routing and recovery.

### Design revision

- Replace the dark lime candidate with an original blue-and-white visual identity,
  using WeCom only as a color reference. Rebuild the layout, product illustration,
  iconography, interactive explorer and generated documentation styling.

### Contexts and release preparation

- Add kit-backed named client contexts and one-channel server boundaries, with
  expected identity checks, separate ledgers and a shared verified callback router.
- Add the Blue Current GitHub Pages candidate, generated narrative documentation
  and searchable CLI reference, responsive layouts and reduced-motion support.
- Update the documentation renderer to Goldmark 1.7.17 after vulnerability
  scanning found GO-2026-5320; add the scanner to CI and release checks.
- Prepare four platform artifacts, mandatory installer checksum verification,
  npm packaging, CI/Pages/release workflows and offline release preflight.

### Added

- Introduce the WeCom channel broker and credential-free agent CLI.
- Add fixed-endpoint token acquisition, channel/member discovery, post/reply
  reads and scoped two-step search with curated untrusted-data envelopes.
- Verify encrypted URL callbacks and retain allowlisted human events with
  durable deduplication and local admission sequences.
- Add immutable text proposals, exact-digest operator approval, read-only
  policy, expiry, idempotency keys and persistent uncertain-write recovery.
- Bound input/output, pagination, rate, concurrency, time and state; refuse
  redirects, inherited proxies, arbitrary URLs, raw output and media access.
- Ship a companion Skill, generated CLI reference, official API research,
  threat model, Linux service reference and offline protocol/process tests.

### Limitations

- Live authentication, discovery, polling, filtering and resume passed in one
  test tenant; distinct-user isolation passed in a synthetic Linux container.
  Live text/reaction writes, optional callbacks and the actual production worker
  environment still require operator verification.
- Media transfer, content edits/deletes, forwarded chat expansion, Windows and
  channel long connections are not implemented. Deployment must sandbox the agent.

[Unreleased]: https://github.com/AngelMsger/wecom-channel-cli/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/AngelMsger/wecom-channel-cli/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/AngelMsger/wecom-channel-cli/releases/tag/v0.1.0
