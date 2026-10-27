# Releasing

WeCom Channel CLI follows the sibling CLI release process. Pushing a `v*` tag
runs the release workflow automatically; there is no separate approval click.
GitHub Releases holds the native binaries and checksums, and npm distributes the
installer wrapper with OIDC provenance.

## Publishing setup

The public repository is `AngelMsger/wecom-channel-cli`, the npm package is
`@angelmsger/wecom-channel-cli`, and the workflow filename is `release.yml`.
The workflow grants `contents: write` and `id-token: write`, uses Node 24's
bundled npm, and does not inject a registry token or configure `registry-url`
on `actions/setup-node`.

The existing validated npm trusted publisher includes Environment `release`.
The workflow retains that environment name solely to match the OIDC identity.
Its GitHub configuration has **no required reviewers and no wait timer**; its
`v*` tag restriction does not require human interaction. This single configuration
reference is the compatibility difference from the siblings, whose npm Environment
field is blank. Replacing a validated publisher is unnecessary for automatic
publication and would require a successful first publish within two days of the
new binding. See [npm's trusted-publisher guidance](https://docs.npmjs.com/trusted-publishers/).

Do not add deployment reviewers as a second release gate. The maintainer's tag
push is the authorization to publish. Main's existing required Ubuntu/macOS and
Linux isolation checks remain contributor safeguards; use pull requests for
source changes and do not bypass branch protection.

## Cutting a release

Before tagging:

1. Run the checks below, including the pinned dependency build with `GOWORK=off`.
2. Move the changelog's `[Unreleased]` changes into the version/date section and
   create a new empty `[Unreleased]` section. Update `build/npm/package.json`
   to the intended version.
3. Update the companion Skill version and loaded handshake if its content changed.
   Refresh generated docs and relevant README/site version references.
4. Commit the reviewed changes and merge the pull request after required checks.
5. From that clean commit, create and push the intended semantic-version tag.
   Do not create a tag merely to exercise a test.

For example, after preparing version `0.1.1`:

```sh
git tag v0.1.1
git push origin v0.1.1
```

The workflow uses the same sequence as WeCom Calendar and the other sibling CLIs:

1. Check out the complete history and install the Go version from `go.mod`.
2. Run `go test ./...`.
3. Run `make cross`, which invokes `scripts/build.sh` and injects the Git version,
   commit and build time into each native binary.
4. Write SHA-256 checksums for the binaries in `dist/`.
5. Create the GitHub Release with generated notes, or refresh its assets on rerun.
6. Install Node 24, set the npm version from the tag without `v`, and run
   `npm publish --access public` unless that exact version already exists.

No custom Python publisher, release manifest, staged-package approval, or
immediate post-publish confirmation loop participates in this workflow. npm
trusted publishing authenticates automatically; routine releases do not need
another npm login or two-factor challenge.

The `go.mod` toolchain directive retains the compiler verified for the first
release when adopting the family's `go-version-file` setup. Update that toolchain
alongside vulnerability checks; the source language minimum remains Go 1.25.

## Validation

```sh
GOWORK=off make lint test e2e build docs-check site-check
npm test --prefix build/npm
make release-tests
GOWORK=off go test -race ./...
make vuln
make cross isolation-check
```

`make release-tests` executes the actual workflow shell steps with local `gh`,
`npm` and compiler substitutes. It checks Git tag version injection, fresh and
existing release paths, npm duplicate skipping, failure propagation, checksums
and the supported platform set without contacting publishing services.

CI retains the connector's protocol, CLI-process, race, vulnerability, generated
site and distinct-user Linux isolation checks. These are domain validation, not
additional release approvals. The installer continues to reject missing or
mismatched checksums and never downloads code implicitly during an agent command.

## Retry and artifact contract

The family workflow supports reruns after partial failure. It refreshes GitHub
assets using `gh release upload --clobber` and skips an npm version that is already
on the registry. This means a rerun may replace native assets; inspect the cause
and intended source before rerunning. Do not move an already published tag.
An npm name/version cannot be overwritten. Inspect registry state before deciding
whether a failed npm step needs another attempt.

The installer depends on these stable names:

```text
wecom-channel-cli-darwin-amd64
wecom-channel-cli-darwin-arm64
wecom-channel-cli-linux-amd64
wecom-channel-cli-linux-arm64
checksums.txt
```

Download pattern:
`https://github.com/AngelMsger/wecom-channel-cli/releases/download/v<version>/<asset>`.
The broker requires Unix sockets, so Windows is not an advertised target. The
initial `v0.1.0` release additionally contains its historical npm archive and
manifest; those seven existing assets and that tag are unchanged. Later tags use
the family artifact set above.

## Family comparison

The comparison was made against all seven maintained CLI workflows. This task
changes only WeCom Channel CLI; it does not modify sibling checkouts or the kit.

| Sibling | Applicable release pattern | Alignment |
| --- | --- | --- |
| Bitbucket CLI | Tag, unit tests, cross-build, checksums, GitHub Release, npm OIDC | Same pipeline |
| Confluence CLI | Same family template | Same pipeline |
| Jenkins CLI | Same family template | Same pipeline |
| Jira CLI | Same family template | Same pipeline |
| OpenObserve CLI | Same family template | Same pipeline |
| Prometheus CLI | Same pipeline, plus duplicate-publication error recognition | Common template retained; that extra recovery branch is not shared by the other six |
| WeCom Calendar CLI | Same family template and build script | Direct implementation reference; only supported platform targets differ |
| Agent CLI Kit | Owner-tagged Go module; no npm release workflow | No change needed |

The environment identity described above does not add a step or an approval.
The stronger checksum installer and the connector's Unix-only platform boundary
remain product-specific requirements.

## First-release evidence

Version **0.1.0**, embedding companion Skill **0.1.4**, was published on 2026-10-27
from `3a65bf9` on [npm](https://www.npmjs.com/package/@angelmsger/wecom-channel-cli/v/0.1.0)
and [GitHub Releases](https://github.com/AngelMsger/wecom-channel-cli/releases/tag/v0.1.0).
The [documentation site](https://angelmsger.github.io/wecom-channel-cli/) is live.

The npm archive matches its original CI asset, SHA-256
`581de5c206b935e6f511c0b226f663b35f5b14c4ef5dec4f5e7ba67c0e194439`.
`npm audit signatures` verified the registry signature and attestation. Provenance
names the reviewed commit, `refs/tags/v0.1.0`, `release.yml` and
[release attempt 3](https://github.com/AngelMsger/wecom-channel-cli/actions/runs/40361741630/attempts/3).
A clean macOS arm64 npm installation matched the native checksum, reported
`0.1.0` / `3a65bf9` and returned all 115 reaction types.

That historical workflow reports failure because its immediate post-publish read
had not yet exposed the successful publication. Independent registry, signature,
provenance and installation checks reconciled it without republishing. The custom
confirmation path has since been replaced by the family release workflow.

Live authentication, channel discovery, whole-channel and selected-thread polling,
filtering and checkpoint resume passed in one test tenant. All 19 synthetic Linux
isolation checks passed. Live text/reaction writes, optional URL callbacks and the
actual production deployment remain unverified. Publication does not attest those
operator-specific conditions; see [security and deployment](security.md).

## Upgrade and documentation

After upgrading the CLI, install the matching companion Skill and reload the
agent context. `skill status` detects loaded/installed/embedded version drift.
The credential-free agent CLI performs no automatic release network lookup.

Pages deploys `docs/` on main through GitHub Actions. Regenerate narrative HTML
and command reference with `make docs`, and keep Markdown canonical.
