# Channel security contexts

Each context binds one bot/channel identity to its own agent socket, operator
socket, policy, event inbox, proposal ledger and checkpoints. The client uses
named contexts through the same `agent-cli-kit/config` engine as the other CLIs.
Context names select connections; Unix permissions and server policy enforce access.

## Client configuration

The default file is `~/.angelmsger/wecom-channel/config.yaml`. Override its
directory with `--config-dir` or `WECOM_CHANNEL_CONFIG`. No credential is stored
in this file. Setup is offline and does not alter the broker's policy.

```sh
wecom-channel-cli config set-context engineering \
  --socket /run/wecom-channel/engineering-agent/agent.sock \
  --bot-id BOT_ID --channel ENGINEERING_CHANNEL --dry-run

wecom-channel-cli config set-context engineering \
  --socket /run/wecom-channel/engineering-agent/agent.sock \
  --bot-id BOT_ID --channel ENGINEERING_CHANNEL

wecom-channel-cli config set-context security \
  --socket /run/wecom-channel/security-agent/agent.sock \
  --bot-id BOT_ID --channel SECURITY_CHANNEL

wecom-channel-cli config get-contexts
wecom-channel-cli config use-context engineering
wecom-channel-cli --use-context engineering post list
wecom-channel-cli --use-context security event list
```

The first context becomes current. Adding another leaves the current selection
unchanged unless `--activate` is explicit. Identical setup is a no-op; conflicting
coordinates require `--overwrite`. `config use-context` and `delete-context`
support dry-run; deletion requires `--yes` and removes only the saved client
connection. The final remaining context cannot be deleted through that command.

Selection: `--use-context` > `WECOM_CHANNEL_CONTEXT` > saved current context >
only context > a context named `default`. With multiple contexts and no selection,
the CLI fails. Implicit selection among multiple contexts emits the family
stderr notice; `WECOM_CHANNEL_NO_CONTEXT_HINT=1` suppresses only that notice.

Coordinates resolve flag > environment > context > default. Environment names
are `WECOM_CHANNEL_SOCKET`, `WECOM_CHANNEL_BOT_ID`, and `WECOM_CHANNEL_ID`.
Project `.env` files are deliberately ignored, so repository content cannot
silently redirect the client. The upstream WeCom host is fixed.

A selected context supplies the channel when `--channel` is omitted. An explicit
conflicting channel fails before I/O. Each request also carries expected context,
bot and channel identities; the selected socket independently verifies them.
Using an accessible socket from another context does not make that mismatch valid.

## One bot, multiple channel boundaries

Use [the version 2 deployment example](../deploy/contexts.example.json).
One broker process holds the bot credentials and cached access token. A single
verified callback endpoint routes by the authenticated channel ID. Each context
has exactly one channel and distinct socket directories and durable state.

```json
{
  "version": 2,
  "bot_id": "BOT_ID",
  "callback_listen": "127.0.0.1:8787",
  "contexts": [
    {
      "name": "engineering",
      "state_dir": "/var/lib/wecom-channel/engineering",
      "agent_socket": "/run/wecom-channel/engineering-agent/agent.sock",
      "admin_socket": "/run/wecom-channel/engineering-admin/admin.sock",
      "read_only": true,
      "channels": [{
        "id": "ENGINEERING_CHANNEL", "senders": ["AUTHORIZED_USER_ID"],
        "read_all": true, "allow_posts": false, "allow_replies": true
      }]
    }
  ]
}
```

Add another object with another name/channel and distinct paths. Up to 32 contexts
are accepted. Other policy limits inherit the safe defaults from `config example`.
Bot identity and callback address cannot differ between contexts in one deployment;
use another deployment for another bot. Duplicate channels, names or socket/state
paths fail validation. State is bound to bot, context and channel; moving a ledger
to a different context fails rather than reinterpreting its approvals.

The single-context version 1 format remains supported, with default context name
`default`; it now rejects policies granting more than one channel to the same
socket. Its existing ledger format remains readable. To migrate an old multi-channel
prototype, preserve its ledger and reconcile pending/uncertain writes before
provisioning separate contexts. Do not discard idempotency history to enable a send.

## OS permissions are part of the boundary

For mutually untrusted workers, use distinct Unix groups and worker identities
per channel. Agent socket directories should be broker-owned, **setgid 2750**,
with their respective context group. Newly created sockets inherit that group
and are chmod 0660. Operator directories remain broker-owned 0700 and sockets
0600. On systems where group inheritance differs, verify actual ownership before
admitting a worker; the CLI does not claim to attest the OS deployment.

A worker must not belong to every context group unless cross-channel access is
intentional. Read-only context configuration alone is not an access control list.
The shared broker process remains trusted across all its contexts. If that trust
is unacceptable, use separate bot deployments, service identities and sandboxes.

Use the existing single-context systemd service as a reference, provisioning
multi-context runtime directories with your OS tooling. Do not let a broad
`RuntimeDirectoryMode` erase per-context group separation. See [security](security.md)
for the complete worker and filesystem requirements.

## Watching within a context

`event watch --whole-channel` or repeated `--post` IDs stays bound to the selected
channel. Enable read_all, post/reply event kinds, and observe_all for all-actor
coverage. Observation does not expand request eligibility. Checkpoints cannot
be shared across contexts, policies, filters or reset ledgers. See [watching](watching.md).

## Agent and operator workflows

The agent proposes a draft within its selected context. The trusted operator
selects the same context and overrides only the socket to its private counterpart:

```sh
wecom-channel-cli --use-context engineering reply propose \
  --post POST_ID --event EVENT_ID --key REQUEST_KEY --stdin < draft.txt

wecom-channel-cli --use-context engineering \
  --socket /run/wecom-channel/engineering-admin/admin.sock approval list
```

Context metadata is always returned in `source.context` on broker records. A
proposal from another context is absent from this ledger; an identical idempotency
key in another channel is independent. Changing context never grants approval or
bypasses read-only policy. Keep incremental event checkpoints with the bot and
context identity, not just the numeric sequence.
