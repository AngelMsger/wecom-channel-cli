# Security boundary and deployment

The enforceable boundary is **an isolated broker plus a sandboxed agent**.
A warning or system prompt alone cannot prevent a model with a shell from
deleting files or reading credentials. This program does not intercept another
agent's shell, filesystem, browser, network or MCP tools. Those capabilities
must be restricted by the agent host, OS or container independently.

## Trust and authority

Treat every channel participant, including an allowlisted coworker, as a
potentially malicious content author. Also treat quoted replies, channel names,
filenames, search snippets, feedback and linked pages as untrusted. A valid
WeCom signature authenticates transport origin and event metadata; it does not
turn a message into an operator instruction. A mention is an event, not consent
to run commands or disclose data.

| Authority | Allowed capabilities | Must not possess |
| --- | --- | --- |
| WeCom callback caller | Submit signed, encrypted events to `/callback` | Local control sockets, arbitrary host operations |
| Agent worker | Read allowed records, submit immutable text proposals | Bot Secret, token, callback keys, operator socket, policy-write access, broker state, host secrets |
| Broker service | Fixed WeCom HTTP API, verified callback intake, private ledger, two local interfaces | General shell execution, arbitrary URL fetching, attachment/path interpretation |
| Trusted operator | Provision policy/credentials; review and approve exact proposals; reconcile writes | No authority may be inferred from content viewed inside an agent session |

The supported isolation unit is one channel context. A bot deployment may share
a token cache and verified callback ingress across contexts; each context has
separate sockets, policy and durable state. Use distinct socket groups and worker
identities for mutually distrusting workers, as described in [contexts](contexts.md).
The broker process is trusted across its contexts; isolate deployments by bot
and service identity when that is not acceptable. This is not a multi-replica service.

## Enforced controls

| Threat | Implemented control | Limit |
| --- | --- | --- |
| Prompt injection through text | Fixed trust/provenance envelope; data-only parsing; no command runner or tool dispatcher | The consuming model can still misunderstand text; its sandbox must contain the damage |
| Coworker impersonating an operator | Exact channel/sender IDs, authenticated callback, separate operator socket | Membership or display names do not grant host authority; approved senders still supply untrusted content |
| Forged/replayed callback | Constant-time signature comparison before decryption; strict padding/length/receive-ID checks; five-minute clock window; durable bot/msgid deduplication | Operators must protect callback keys and maintain accurate time |
| Bot loops and unsolicited triggers | Human mentions remain allowlisted; optional observe_all admits post/reply observations from all actors with observation_only=true | Observations never authorize an action or automatically launch an agent |
| Self-approval | Separate listener-bound authority; private operator socket; no request field or flag grants it | Same-user development runs do not isolate the sockets from that user |
| Changed draft or destination | Immutable request and SHA-256 digest; exact event-to-channel/post/reply binding; short expiry; policy digest recheck | A digest is not authentication; only operator socket access grants approval |
| Duplicate sends | Persist `sending` before I/O; terminal states reject replay; same-key submissions are idempotent | No documented remote idempotency key; operator-driven duplicates remain possible |
| Credential exfiltration | Secret environment only in broker; fixed TLS host/paths; redirects and proxy environment disabled; raw network errors suppressed; no download cookies/URLs returned | A compromised broker OS account or host root remains trusted |
| Filesystem/SSRF abuse | No remote file-path operations, shell commands, URL following, media fetch/upload, or arbitrary API method | Markdown links remain untrusted text, not approved network destinations |
| Resource exhaustion | Request/response sizes, JSON depth, page sizes, timeouts, concurrency, rate budgets, retention and ledger capacity | Volumetric attacks need reverse-proxy/network controls; authenticated users can consume permitted capacity |
| Terminal/UI spoofing | JSON escaping; no table/raw output or removable trust fields; outbound control/bidi overrides rejected | Review untrusted inbound text without rendering Markdown, links or terminal escapes |

These are structural controls, not a heuristic injection detector. No regex or
classifier is used to declare a message safe. Do not automatically give a model
additional tools because content passed a filter.

## Linux deployment reference

Use dedicated service and worker accounts. The broker account must not have
interactive login privileges or access to unrelated host secrets. The worker
must not be the operator's login account and must have no sudo, Docker socket,
container-control, SSH-agent, OS-keychain or unrestricted host filesystem access.

Recommended ownership:

| Path | Owner/group | Mode | Contents |
| --- | --- | --- | --- |
| `/etc/wecom-channel/` | root/root | 0755 | Directory without secrets visible to workers |
| `/etc/wecom-channel/broker.json` | root/root | 0644 | Non-secret policy; workers cannot edit it |
| `/etc/wecom-channel/secrets.env` | root/root | 0600 | Broker-only secret environment, loaded by systemd |
| `/var/lib/wecom-channel/` | wecom-channel/wecom-channel-agents | 0700 | Private ledger and lock |
| `/run/wecom-channel-agent/` | wecom-channel/wecom-channel-agents | 0750 | Agent socket, 0660 |
| `/run/wecom-channel-admin/` | wecom-channel/wecom-channel-agents | 0700 | Operator socket, 0600 |

Add only the dedicated worker account to `wecom-channel-agents`. The broker's
primary group is that group so its agent socket is accessible. The operator
socket is protected by both its private parent and its 0600 mode. The trusted
human can invoke the CLI as the broker service identity through an explicitly
controlled administrative mechanism; the worker must not have that mechanism.

The [systemd unit](../deploy/wecom-channel-broker.service) is a concrete reference.
Install the reviewed binary at `/usr/local/bin/wecom-channel-cli`, provision
accounts and files as above, validate policy, then start the unit deliberately.
The unit is not automatically installed by this project and has not been tested
on a live production host. Canonical, absolute paths are required: symlinked
state/socket directories and group/world-writable parents are refused.
Every ancestor must be owned by root or the service identity; checking only the
leaf file's permissions would allow a different owner to replace its parent.

The service starts with a read-only filesystem apart from its state and socket
directories, no Linux capabilities, no privilege escalation, no home access and
only Unix/IPv4/IPv6 sockets. Its environment comes from the protected file;
do not expose it through debugging, process-inspection permissions or crash dumps.
Use encrypted storage if the at-rest channel content requires it.

### Contain the entire agent worker

Separating only the CLI process does not constrain a host agent. Put the model's
tool executor and working directory in a separate unprivileged sandbox/VM:

- mount only a disposable working directory and the agent socket directory;
- expose no broker state, operator socket, policy-writable mount, home directory,
  cloud credentials, SSH keys, keychain or privileged daemon socket;
- expose no authenticated official `wecom-cli` session or equivalent WeCom
  connector to the channel worker: alternate credentials, browser sessions and
  tools could bypass this broker's channel policy and proposal review;
- deny outbound network by default; if remote inference is required, use a
  separately controlled gateway with a narrowly scoped model credential and
  explicit endpoint policy, not general network access or host credentials;
- disable privilege escalation and container escape capabilities; restrict
  available tools to the task; keep approval controls outside model reach;
- apply CPU, memory, process and filesystem quotas; use short job deadlines;
- treat any model-produced result as untrusted when the supervisor receives it.

This project does not provision or attest that sandbox. Verify it before
enabling unattended work. On macOS the same code runs, but production separation
needs a dedicated account or VM; two terminals owned by one user do not create
a security boundary. Windows named-pipe support is not implemented.

### Expose only the callback endpoint

The broker binds callbacks to a literal loopback IP. Put an HTTPS reverse proxy
in front of `/callback`, with a small body limit (256 KiB), short request timeout
and rate limits. Do not proxy `/v1/request`, and never publish either Unix socket.
Disable callback query/body logging: signed callback URLs and encrypted payloads
are sensitive. The GET challenge response must be forwarded byte-for-byte.

Review WeCom's existing bot connection configuration before making any change.
URL mode and long-connection mode are mutually exclusive, and group-channel
compatibility can produce duplicates. The connector neither switches modes nor
disables that compatibility path.

## Observation and watches

Channel-wide observation is an explicit operator policy: `observe_all` requires
`read_all` and does not widen the human mention allowlist. The broker can admit
bot-authored post/reply changes as data without treating them as requests.
`event watch` supports a whole-channel scope or selected post IDs, a total
deadline and count bound, and private resumable checkpoints. It fails on detected
history gaps and scope/ledger/policy changes. Filters reduce delivery, not authority.
See [watching](watching.md) for configured coverage and output-acknowledgment limits.

## Review and recovery

Review proposals in a trusted operator interface with no untrusted-content tool
execution. Confirm the channel, post, parent reply, full text, content length and
digest. Human-authored replies require per-item approval. Reject draft text that
discloses private information or exceeds the requested action even if its syntax
is valid. Approval cannot retroactively protect information already sent by a
privileged agent through another tool.

`--yes` is confirmation only after access to the operator socket; it is not an
authentication token. There is no approve-all or auto-approve mode. Pending
proposals expire after one hour by default (configurable down to 60 seconds).
Policy changes invalidate existing proposals. Rejection is terminal.

For an interrupted or uncertain send, retain the ledger, inspect the remote
thread through a read or WeCom's UI, and record the verified result with
`approval resolve`. Do not approve a new key as a workaround. A success followed
by a failed local persistence step reports the known remote ID in the structured
error. The CLI has no deletion/pruning command for the proposal ledger: its
configured capacity is a fail-closed operational limit, not permission to erase
idempotency history.

The ledger is private operational/audit evidence, not a tamper-proof log against
the operator or host root. Back it up with the same access controls. Default
event retention is 48 hours; physical expiration is applied on later accepted
intake, while reads hide expired events immediately. Proposals and their keys
remain until a future explicit archival design preserves replay protection.

## Before live activation

Verify the eligible bot's API mode and permissions with the operator socket.
Confirm the actual worker cannot open the operator socket or state/secret files,
or reach another authenticated WeCom tool or session. Preserve existing
integrations using the [coexistence guidance](research.md#coexistence-with-an-existing-official-cli).
Exercise the encrypted URL challenge, and test one explicitly authorized reply
in a designated test channel. Confirm that a replay cannot send it twice and
that the reverse proxy records neither callback bodies nor query strings.

Offline tests establish protocol and application invariants; they cannot certify
WeCom tenant rollout, the external TLS proxy or the sandbox used by your agent.
