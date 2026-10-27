# Release guide

The first CLI release is **0.1.0**, embedding companion Skill **0.1.4**
(the Skill was revised for native reactions), with GitHub coordinates
`AngelMsger/wecom-channel-cli` and npm package `@angelmsger/wecom-channel-cli`.
It was published on 2026-10-27 from `3a65bf9` on
[npm](https://www.npmjs.com/package/@angelmsger/wecom-channel-cli/v/0.1.0) and
[GitHub Releases](https://github.com/AngelMsger/wecom-channel-cli/releases/tag/v0.1.0).
The [documentation site](https://angelmsger.github.io/wecom-channel-cli/) is live.
Preparation commands do not publish; subsequent releases remain owner-controlled.

## Local release gate

```sh
make lint test e2e docs-check site-check
npm test --prefix build/npm
make release-tests
go test -race ./...
make vuln
GOWORK=off make test
make package
make isolation-check
make release-check
```

`make package` cross-compiles Linux/macOS on amd64/arm64, injects version/commit/
build metadata, packs npm without installation scripts, then hashes all five
assets into `dist/checksums.txt` and `release-manifest.json`. The build timestamp
defaults to the source commit timestamp so rebuilding with the same toolchains
does not introduce a new clock value. Toolchain changes can still change bytes.
`release-check` verifies the exact platform/asset set, HEAD commit, clean Go build
provenance, native version/commit, checksums, sizes and npm contents against the
reviewed source. It does not contact a registry or publish. Rebuild after a source
commit; stale or dirty-source binaries are rejected.

`make release-tests` exercises malformed, missing, stale, tampered and unsafe
release material plus retry decisions. `make isolation-check` runs the Linux
binary in a disposable Docker container with synthetic credentials, distinct
service/worker UIDs, separate channel groups, a read-only root and no network.
Only the executable and fixture are mounted, read-only; no host credentials or
Docker socket are exposed. The fixture supervisor has only the capabilities
needed to set up identities and terminate children; worker capabilities are zero.
This verifies the reference permission boundary, not every production host.

The npm installer follows the family's binary-wrapper architecture. It deliberately
requires one valid matching checksum, bounds HTTPS downloads and writes atomically.
The runtime launcher does not download code. This strengthens the older sibling
installer pattern that can skip an unavailable checksum; those repositories are
outside this change's write scope.

The older sibling release workflows also use wall-clock build timestamps and
asset replacement on retries. This project now rejects replacement and uses a
stable source timestamp; that release-hardening difference is intentional here.
No sibling repositories were modified.

## Verification recorded on 2026-10-26

| Check | Evidence and status |
| --- | --- |
| Live authentication and channel discovery | Passed with a dedicated test bot; one channel |
| Live polling baseline, whole channel and selected thread | Passed; four whole-channel and three selected-thread observations |
| Live checkpoint resume, unrelated-post filtering and trust envelope | Passed; no public listener and no messages sent by the test |
| Linux reference isolation | Passed 19 synthetic checks, including own/cross-context sockets, operator access, files, environment, escalation and network denial |
| Actual production deployment | Operator-specific; not attested by the local/container checks |
| Live operator-approved reply | Not run; requires a reviewed destination and exact content |
| Live native emoji add/remove | Not run; wire, permission, membership, restart and CLI-process checks passed against synthetic APIs |
| Live URL callback delivery | Not run; optional transport, not required for polling deployment |
| Publication infrastructure | Canonical repository/origin configured; Pages uses Actions with HTTPS and main-only deployments; the release environment requires AngelMsger review for v* tags. Version 0.1.0 was published on 2026-10-27 |

### Publication verification on 2026-10-27

All seven GitHub assets retain their original verified digests. The npm tarball
matches the CI archive (SHA-256
`581de5c206b935e6f511c0b226f663b35f5b14c4ef5dec4f5e7ba67c0e194439`).
`npm audit signatures` verified its registry signature and attestation. The SLSA
statement names the reviewed commit, `refs/tags/v0.1.0`, `release.yml` and
[release attempt 3](https://github.com/AngelMsger/wecom-channel-cli/actions/runs/40361741630/attempts/3).
A fresh macOS arm64 npm installation downloaded the checksum-matching binary,
reported version `0.1.0` / commit `3a65bf9`, and returned all 115 reaction types.
This establishes the installed CLI path, not live WeCom writes or every native platform.

That workflow attempt reports failure because its immediate post-publish read
did not yet expose the successful publication. Independent registry, signature,
provenance and installation checks reconciled the result without publishing again.
The bounded confirmation fix is on main for future releases; the original tag and
release assets remain unchanged.

Main requires the GitHub Actions checks `test (ubuntu-latest)`,
`test (macos-latest)` and `isolation`, with an up-to-date branch and resolved review
conversations. These requirements also apply to administrators; force pushes and
deletion are disabled. Use a feature branch and pull request for subsequent work.

## Publication prerequisites

1. Review the source changes. Confirm the
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
3. Push reviewed commits to the approved canonical repository,
   `AngelMsger/wecom-channel-cli`. The local origin uses HTTPS authentication
   delegated to the existing GitHub CLI login; no credential is stored in the repository.
4. Pages is configured with source **GitHub Actions**, HTTPS enforcement and the
   `github-pages` environment restricted to `main`. The workflow uploads only `docs/`;
   it checks generated material and local links before deploying. All site assets
   are local; no external font/CDN/runtime service is required.
5. Configure the npm package's trusted publisher for the repository and
   `release.yml`, with the `release` environment and permission for direct `npm publish`. Verify npm account/
   organization rights and the registry's first-publication bootstrap process.
   Do not assume OIDC creates a new package or account authorization automatically.
   New trusted-publisher configurations must complete their first publication
   within two days; configure this close to the approved release. See the
   [official npm guidance](https://docs.npmjs.com/trusted-publishers/).
   When staging is used to initialize a new package, retain a separate temporary
   staged version until the real version is published and verified. Do not stage
   the intended release version merely to reserve the package: staged versions
   occupy their version slot. During the first release, rejecting the only stage
   before publication removed the package's trusted-publisher configuration.
   Reconcile the real version and trust before rejecting the temporary stage;
   never approve a bootstrap-only package as a product release.
6. The protected GitHub **release** environment requires `AngelMsger` review and
   permits only `v*` tags. Self-review remains available for the sole maintainer;
   the workflow never approves its own deployment. Review the final changelog and
   release notes. Create the release tag only after
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

The release workflow reruns tests, builds/verifies assets and runs the container
isolation fixture. `scripts/publish-release.py --apply` then verifies that the
remote tag resolves to the reviewed commit. It creates a draft release, uploads
only missing assets, verifies all remote SHA-256 digests, and publishes the draft.
Existing mismatched, duplicate or unverified assets stop the workflow; it never
uses `--clobber`. A network/auth error is not treated as an absent release.
Drafts are resolved with `gh release view` to a database ID and verified through
the release-ID API. The REST published-release-by-tag endpoint does not return
drafts and must not be used to check uploaded draft assets or recover a retry.

The same verified npm tarball is published with provenance and trusted publishing.
An existing npm version is accepted only when its registry SHA-512 integrity
matches that archive. After npm reports success, confirmation makes at most seven
fresh reads with 125 seconds of total backoff and a 20-second npm fetch timeout
per read. Only missing versions are retried; conflicts, malformed responses and
authentication/network failures stop immediately. Publication itself is never
retried by this confirmation loop. Safe npm error codes are preserved without
printing credential-bearing command output.
If npm publication fails after GitHub succeeds, retain the release and reconcile
that exact npm version before retrying. Do not overwrite assets or bump the version
merely to recover. Reuse the original toolchains when rebuilding for a retry.

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
