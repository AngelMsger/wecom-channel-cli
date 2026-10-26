# WeCom Channel CLI 0.1.0

Initial release candidate for the grayscale WeCom channel APIs.

- Named channel security contexts, with offline family-style configuration and
  independent socket, policy, inbox and approval boundaries.
- Shared token cache per bot deployment and optional verified callback ingress.
- Channel/member discovery, post/reply reads, scoped search and durable events.
- Bounded NDJSON watches for a whole channel or selected post threads, with
  explicit observation coverage, gap detection and context-bound checkpoints.
- Default HTTP polling without a public address: quiet baselines, atomic snapshot
  comparison, deduplicated changes and bounded request/record budgets. Callbacks
  retain the same watch command and output envelope.
- Immutable text proposals and independent exact-digest operator approval.
- Strict HTTP destinations, authenticated callbacks, replay protection, bounded
  output and secret-free untrusted-data envelopes.
- Linux/macOS amd64/arm64 binaries, mandatory SHA-256 verification in the npm
  installer, a companion Skill, and a static documentation site.

This connector does not sandbox arbitrary agent tools. Separate OS identities
and a restricted worker environment are required. Authentication and channel
discovery, whole-channel/thread polling, filtering and resume passed in one test
tenant. A synthetic Linux container verified distinct-user socket/file access
controls. Live sending, optional callback delivery and the actual production
deployment still require operator verification. Polling may coalesce intermediate changes and never infers deletion
from absence. Media transfer, content mutation beyond text posts/replies,
Windows and channel long connections are outside this release.
