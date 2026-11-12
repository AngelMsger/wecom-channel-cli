# Command responsibilities and migration

The public grammar is `wecom-channel-cli <resource> <action>`. A command's
resource, state owner, output and side effects determine its place in the tree.
Operator authority comes from the private socket and policy, never from a noun,
flag, alias or the fact that a command appears in help.

## Canonical tree

```text
wecom-channel-cli
  channel   list
  member    list
  post      list | get | propose
  reply     list | get | propose
  reaction  types | list | propose
  search    start | get
  event     list | get | context | watch
  relay     run | status
  proposal  list | get | approve | reject | resolve
  policy    example | validate
  broker    serve
  config    path | get-contexts | set-context | use-context | delete-context
  auth      guide | check
  skill     show | path | status | install
  doctor
  version
```

This inventory omits Cobra's built-in help/completion. The
[generated reference](cli/README.md) is authoritative for flags and examples.

| Surface | Primitive and result | Authority / state owner |
| --- | --- | --- |
| `channel list`, `member list` | Discover channels/members in one bounded page | Broker policy; explicit `channel list --bot-id` is credential-based operator bootstrap |
| `post/reply list` | Retrieve a bounded page of current content | Broker read scope |
| `post/reply get` | Retrieve current content for an exact known ID | Broker read scope |
| `reaction types` | Discover the offline native emoji enum | No network or credentials |
| `reaction list` | Inspect current reaction membership on one target | General or exact-mention read scope |
| `post/reply/reaction propose` | Build and persist one immutable intent; no remote action | Agent socket can propose; action-specific fields stay with the resource |
| `proposal get` | Inspect the intent and current execution/reconciliation state | Agent or operator socket within allowed scope |
| `proposal list/approve/reject/resolve` | Enumerate retained proposals, execute an approved pending intent, reject, or record a verified uncertain outcome | Private operator socket; resolve never resends |
| `search start` | Initiate a server-side search and return its job identity | Observation through broker; not a host/agent job |
| `search get --job` | Read one bounded page of that search's results | Same broker context as the search |
| `event list/get` | Read retained observation/mention metadata once | Broker ledger; these commands do not listen |
| `event context` | Read the post and exact reply authorized by a fresh human mention | Mention-specific permission, unlike general content browsing |
| `event watch` | Stream bounded observations to NDJSON | Consumer checkpoint records successful output, not processing |
| `relay run` | Collect, queue, filter, batch and deliver to one configured host adapter | Separate trusted service; durable queue and explicit acknowledgment |
| `relay status` | Read local queue counts/timestamps | No network; not a process-liveness probe or Bot-completion claim |
| `policy example/validate` | Generate or validate broker authorization policy offline | Operator-owned configuration; neither command installs it |
| `broker serve` | Run the credential-holding service from that policy | Isolated broker identity |
| `config ...` | Manage local connection coordinates and selected context | Client configuration; never changes broker permissions |
| `auth guide` | Explain credential ownership and bootstrap | Offline |
| `auth check` | Perform a bounded upstream authentication check | Private operator socket; remote I/O |
| `doctor` | Check local broker readiness and read-only policy | Does not verify upstream authentication |
| `skill ...`, `version` | Inspect/install agent guidance and report build identity | Local software metadata; installation is an explicit local write |

## Choosing an observation primitive

Use `event list/get` for a one-time inspection of retained events, `event watch`
for a bounded foreground stream, and `relay run` for unattended acknowledged
delivery. A relay is a stateful service with its own queue and lifecycle, so its
status belongs under `relay`, not under `event`. There is one broker scan path
behind watch and relay; neither adds a second upstream observation engine.

`inbox` owns downstream receipt and host-processing state. `serve` adapts the
receipt contract to existing HTTP relays; default local delivery calls the same
store directly. `status/list/get` inspect it, `claim` reserves work before host
I/O, and `resolve` records the reconciled outcome. No command invokes a Bot.
The relay's collected/acknowledged cursor is not a replacement for these states,
and no inbox operation changes broker permissions or channel content.

`event context` deliberately stays separate from `post/reply get`: it expresses
the narrower permission granted by a fresh authenticated human mention. It is
not an alternative to `config` contexts and does not fetch arbitrary history.
Likewise, `propose` creates an intent while `approve` executes it; combining them
into a generic send command would conceal the independent operator boundary.

The three resource-specific proposal commands remain because text, replies and
native reactions have different target and validation rules. Their resulting
resource has one lifecycle under `proposal`. There is no separately stored
approval resource to list. Native `reaction types` and the family's conventional
`config *-context` verbs are intentional vocabulary, not reasons to add synonyms.

## Audit findings and fixes

| Previous ambiguity | Resolution |
| --- | --- |
| `event relay` and compound `event relay-status` mixed event reads with a service lifecycle | `relay run/status`; `run --config` names the service config |
| `config` mixed client connection state and operator authorization policy | `config` owns client contexts; `policy example/validate` owns offline policy tooling |
| `proposal get` versus `approval list` split a single stored resource | All proposal lifecycle actions are discoverable under `proposal`; private-socket gates remain |
| `auth status` returned exactly the same readiness response as `doctor` | One canonical readiness command, `doctor`; `auth check` explicitly tests upstream credentials |
| `search result` used a result noun as a retrieval action | `search get --job`, with pagination retained |
| Generic help described exact reads, job initiation and mention context as the same page read | Operation-specific summaries, precise discovery paths and accurate per-command limits |
| Explicit `--limit 0` silently became the API default | Usage error before broker access; content pages accept 1–50, search result pages 1–20, retained events/proposals 1–100 |

The rewrite changes CLI routing, documentation and local input validation, not
the broker protocol. Existing `approval.*` and `search.result` operation names
remain wire compatibility details. Both CLI routes use the same command factories
and therefore the same request builders, socket checks, dry runs and write gates.

## Published compatibility

| Published path | Canonical path |
| --- | --- |
| `config example`, `config validate` | `policy example`, `policy validate` |
| `approval list/approve/reject/resolve` | `proposal list/approve/reject/resolve` |
| `auth status` | `doctor` |
| `search result` | `search get` |

Published paths remain callable with their existing data and authorization
semantics. They emit a structured stderr notice with `deprecated_command` and
`replacement`, and are hidden from ordinary help, completion and generated
reference. Consumers should already separate stderr notices from stdout data.
There is no scheduled removal in this change; removal requires a documented
compatibility release. New examples and Skills use only canonical paths.

The earlier relay paths existed only in this unreleased change. They are replaced
directly: use `relay run --config FILE --state FILE` and `relay status --state FILE`.
Its queue and webhook schema remain version 1, so a locally tested queue can
resume through the new command without discarding state.

## Family applicability

The seven maintained siblings and the kit were compared. All use the shared
resource/action, config-context, error and output conventions. Jenkins log follow
and OpenObserve tail are streams; Confluence watch changes a remote notification
subscription; Calendar sync maintains a mirror; Bitbucket activity, Jira worklogs
and Prometheus queries are bounded reads. These are different primitives and are
not renamed to pretend they implement durable host delivery. The fx adapters do
not call the channel-specific paths changed here. Shared host-integration design
and migration guidance live in the workspace's
[event-driven agent integration guide](https://github.com/AngelMsger/agent-clis-workspace/blob/main/docs/event-driven-agent-integration.md).
