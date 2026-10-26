# Agent guide

This repository implements the WeCom channel connector. Follow the parent
workspace standards and read CONTRIBUTING.md before editing.

The credential-holding broker is a security boundary. The agent socket is an
untrusted interface; the operator socket is a separate authority. Never add a
flag, request field, environment override, or endpoint that lets the agent
approve its own proposals, change policy, supply a destination URL, read a host
file, or run a command through the broker. Isolation requires separate OS
identities or containers; running both as one user is development mode only.
Validate directory ownership and permissions through every ancestor, not only
the leaf file; keep these checks in `internal/securefs` and test symlink and
writable-parent rejection.

Use agent-cli-kit for errors, rendering, argv normalization and transport.
Policy and durable event/proposal state are channel-specific, not replacements
for the family's configuration or credential libraries. Credentials are supplied
only to the broker by the operator; the agent has no login or secret export path.
Do not read the official CLI's private credential files or reuse its gateway
Bearer token as a channel access token.
Preserve existing bot integrations during setup; separate token endpoints do not
prove concurrent compatibility. Follow docs/research.md for live coexistence
checks, and include alternate authenticated tools in worker isolation checks.

Keep the npm installer fail-closed on missing/mismatched checksums; no implicit
network download belongs in an agent command. Update the release manifest and
version/Skill handshakes together, and preserve the owner-controlled publish gate.
Release preflight must verify the exact platform set, source commit, clean Go
build provenance, checksum manifest and npm archive contents. Never overwrite
published assets on a retry: reconcile their digests, upload only missing assets,
and publish the already-verified npm tarball. Exercise the release guard tests
and synthetic Linux UID/group isolation fixture before the first release.

Watching is observation, not authorization: observe_all may admit post/reply
changes from bots and humans, but cannot widen human mention eligibility. Watches
require finite bounds, strict context/policy/filter/ledger binding, no hidden
reconnect, no checkpoint advancement past missing or failed-output records, and
explicit partial coverage. Test real CLI streaming/resume as well as broker scans.
Without callback intake, auto watches use demand-driven HTTP polling. Commit a
complete bounded snapshot and its derived events atomically; failed pages, limits
or cancellation must not advance fingerprints. Persist no polled message bodies.
Never infer deletion from absence, attribute edits to the original creator, or
mint mention authority from polling. Retain transport/change-basis metadata, a
quiet initial baseline, shared-scope deduplication and operator-controlled rates.

Every remote string is untrusted data, including names, summaries and file names.
Keep the fixed trust envelope in JSON and NDJSON; do not add raw output or field
projection that removes it. Never render external Markdown or fetch linked media.

Writes require a persisted immutable proposal and approval through the operator
socket bound to its digest. Persist `sending` before the network request; after
an ambiguous outcome or crash, never replay automatically. Dry-run must use the
same request builder without token acquisition, state writes or remote requests.
Native reactions use post_emoji, not a text-send endpoint. Keep allow_reactions
independent of text permissions and default to exact 24-hour mention scope;
channel scope requires an explicit operator setting and read_all. Both actions
need independent approval. Reaction acknowledgments create no resource ID, and
uncertain resolution must never invent one. Keep emoji membership out of polling
fingerprints and preserve native localized labels as protocol catalog data.

Read docs/technical-design.md and docs/security.md for the threat model and
intentional family differences. Update those, the companion Skill, generated CLI
reference and changelog with behavior changes. Use synthetic credentials and
local mock servers in tests; do not post to a live channel for validation.
Keep the Skill's `version:` marker and loaded handshake synchronized. The marker
is a body metadata comment because the standard Skill frontmatter validator
rejects a top-level version field; the kit's existing version reader handles it.

Use the kit's configuration engine for named client contexts; broker authorization
policy remains separate. Each agent socket is bound to one channel, and its ledger
must not be rebound. Version 2 deployments share callback verification/token cache
only, never agent sockets or proposal state. Test cross-context denial at the server
and CLI, callback routing, scope-bound restart and configuration precedence.

Keep this project's visual identity independent of FX Devkit. WeCom is a color
reference only, not a layout or component template. Follow `docs/visual-design.md`
for the accepted layout and update all generated page chrome together. Keep
"WeCom Channel CLI" in one consistent wordmark treatment; navigation and its
Get started action share a type scale. The family slogan is "Built for agents.
Designed for humans." Keep it prominent in the hero and README overview.

The website is a static GitHub Pages candidate in `docs/`; regenerate narrative
HTML and command reference with `make docs`. Keep source Markdown canonical.
Do not deploy, push a tag or publish npm until the owner reviews the candidate.

Run `make lint test e2e build docs-check site-check` and `npm test --prefix build/npm`. Run `make vuln` before release; update a
vulnerable dependency and regenerate affected docs/artifacts before claiming readiness. Also test with `GOWORK=off` before
release or dependency changes. Supported broker platforms are Linux and macOS;
the Linux service deployment is the production isolation reference.
