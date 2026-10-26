# Contributing

Read AGENTS.md, docs/technical-design.md and docs/security.md first. Engineering
prose is English. Changes remain scoped to this repository unless explicitly
authorized as family-wide.

The layers are `cmd -> internal -> pkg`: `internal/app` owns CLI wiring,
`internal/broker` owns policy enforcement and the two local sockets,
`internal/state` owns durable replay/proposal state, `internal/config` owns the
strict operator policy document and thin kit-backed client-context adapter, and `pkg/apiclient` and `pkg/callback` own the
documented WeCom protocols. `pkg/errors` and `pkg/transport` re-export the kit.
`internal/securefs` validates ownership and permissions through the complete
policy/state/socket directory ancestry.

Run `make lint test e2e build docs-check`. Tests must cover negative security
paths, malformed callbacks, socket separation, restart recovery, uncertain
writes and secret-free output, in addition to ordinary successful requests.
`make docs` regenerates the Markdown/HTML CLI reference and narrative HTML.
The buildless landing page, styles and interactions live in `docs/index.html` and
`docs/assets/`. `make site-check` validates local links and offline assets.
`make package release-check` prepares binary checksums and the npm tarball without
publishing. See docs/releasing.md for the final owner-controlled release gate.
Run `make release-tests` after release-script changes, and `make cross isolation-check`
for the synthetic Linux service/worker permission checks (Docker required).

No release is published by the build. Keep changes under `[Unreleased]` until
the owner chooses a release; do not create tags automatically.
