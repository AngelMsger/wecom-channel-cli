# WeCom Channel CLI 0.1.0

Initial release candidate for the grayscale WeCom channel APIs.

- Named channel security contexts, with offline family-style configuration and
  independent socket, policy, inbox and approval boundaries.
- One verified callback ingress and shared token cache per bot deployment.
- Channel/member discovery, post/reply reads, scoped search and durable events.
- Bounded NDJSON watches for a whole channel or selected post threads, with
  explicit observation coverage, gap detection and context-bound checkpoints.
- Immutable text proposals and independent exact-digest operator approval.
- Strict HTTP destinations, authenticated callbacks, replay protection, bounded
  output and secret-free untrusted-data envelopes.
- Linux/macOS amd64/arm64 binaries, mandatory SHA-256 verification in the npm
  installer, a companion Skill, and a static documentation site.

This connector does not sandbox arbitrary agent tools. Separate OS identities
and a restricted worker environment are required. Live tenant access, callback
configuration and the actual deployed isolation boundary require operator
verification. Media transfer, content mutation beyond text posts/replies,
Windows and channel long connections are outside this release.
