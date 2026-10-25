# Changelog

## [Unreleased]

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

## [0.1.0] — Release candidate

Prepared for owner review; no tag or publication has occurred.

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

- Live grayscale tenant access, callback deployment and full worker isolation
  require operator verification. No package release is published.
- Media, emoji mutations, edits/deletes, forwarded chat expansion and channel
  long connections are not implemented. The agent runtime itself is not sandboxed
  by this CLI; deployment must provide that boundary.
