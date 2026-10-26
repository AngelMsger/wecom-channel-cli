# First release handoff

The proposed first CLI release is **0.1.0**, embedding companion Skill **0.1.3**
(the Skill was revised for polling watches), with GitHub coordinates
`AngelMsger/wecom-channel-cli` and npm package `@angelmsger/wecom-channel-cli`.
Metadata and workflows are prepared locally. No remote repository, tag, GitHub
Release, npm version or Pages deployment is created by the preparation commands.
The website is a design candidate awaiting review.

## Local release gate

```sh
make lint test e2e docs-check site-check
npm test --prefix build/npm
go test -race ./...
make vuln
GOWORK=off make test
make package
make release-check
```

`make package` cross-compiles Linux/macOS on amd64/arm64, injects version/commit/
build time, writes `dist/checksums.txt` and `release-manifest.json`, then packs
an npm tarball without running installation scripts. `release-check` verifies
metadata and artifact hashes. It does not contact a registry or publish.

The npm installer follows the family's binary-wrapper architecture. It deliberately
requires one valid matching checksum, bounds HTTPS downloads and writes atomically.
The runtime launcher does not download code. This strengthens the older sibling
installer pattern that can skip an unavailable checksum; those repositories are
outside this change's write scope.

## Decisions and external prerequisites before the first push/tag

1. Review the **Blue Current** site candidate and source changes. Confirm the
   proposed repository/package names and that the owner will publish under them.
2. Verify an eligible WeCom bot in a designated test channel: channel access,
   polling baseline/change detection, selected-thread filtering and resume;
   also verify the authenticated URL challenge for optional callbacks. Test
   one explicitly approved reply, replay rejection,
   and actual worker denial of operator sockets/state/credentials. Offline tests
   do not prove tenant rollout or OS deployment. Prefer a dedicated test bot when
   the existing bot serves another integration; follow the
   [coexistence checks](research.md#coexistence-with-an-existing-official-cli).
   The worker must also lack access to other authenticated WeCom tools or sessions.
3. Create or select the approved GitHub repository, set its canonical `origin`,
   and push reviewed commits. No remote is configured in this local repository yet.
4. Enable Pages with source **GitHub Actions**. The workflow uploads only `docs/`;
   it checks generated material and local links before deploying. All site assets
   are local; no external font/CDN/runtime service is required.
5. Configure the npm package's trusted publisher for the repository and
   `release.yml`, with the `release` environment and permission for direct `npm publish`. Verify npm account/
   organization rights and the registry's first-publication bootstrap process.
   Do not assume OIDC creates a new package or account authorization automatically.
   New trusted-publisher configurations must complete their first publication
   within two days; configure this close to the approved release. See the
   [official npm guidance](https://docs.npmjs.com/trusted-publishers/).
6. Configure the protected GitHub **release** environment with owner review, and
   review the final changelog and release notes. Create the release tag only after
   that review. The workflow is tag-driven and never tags source itself.

```sh
python3 scripts/release-check.py --publish
```

This stricter gate requires committed, clean source, the canonical origin, and
an exact owner-approved `v0.1.0` tag on HEAD. It is expected to fail during local
candidate review. It does not substitute for npm/Pages permissions or live checks.

The vulnerability scanner uses the Go 1.26+ toolchain (automatic toolchain
selection is supported). CI and release builds use the current stable Go release;
the source minimum remains Go 1.25.

## Publishing behavior

The release workflow reruns tests, builds and verifies assets, creates or refreshes
the GitHub Release, then publishes npm with provenance and trusted publishing.
An existing npm version is not overwritten. If npm publication fails after GitHub
succeeds, retain that release and repair the publishing configuration before
rerunning; do not create a replacement version merely to retry.

Check GitHub asset checksums, npm metadata/provenance and a clean-machine launch
on each intended native platform. Cross-builds establish compilation, not native
permission or signal behavior. Pages deploys on reviewed pushes to `main` or a
manual workflow dispatch.

## Upgrade contract

Upgrade the CLI, run `skill install` (use `--overwrite` only after reviewing
changed installed content), then reload the agent context. `skill status` compares
loaded, installed and embedded versions. The agent-side CLI does not run automatic
release network checks, preserving offline socket-only operation. This is an
intentional domain difference from the network-facing sibling clients.
