# Native emoji reactions

Reactions use WeCom's `topic/post_emoji` endpoint. They add or remove the bot's
native emoji membership on an existing post or reply; they do not publish emoji
text or create a new post/comment. They use the same proposal, operator approval,
read-only and uncertain-write protections as text writes.

## Discover types and inspect a target

```sh
# Offline catalog: the 115 documented numeric types and official display labels.
wecom-channel-cli reaction types

# Current native reactions on a post, or on one reply beneath it.
wecom-channel-cli --use-context engineering reaction list --post POST_ID
wecom-channel-cli --use-context engineering reaction list --post POST_ID --reply REPLY_ID
```

The catalog is bounded local schema data in the standard list envelope. Labels
retain WeCom's official spelling, including localized labels. For example, type
1 is `[+1]`, 3 is `[DONE]`, 6 is `[OK]`, 69 is `[收到]`, and 115 is `[鼓掌]`.
Use the numeric type; arbitrary Unicode emoji and guessed identifiers are not
accepted as native reaction types.

`reaction list` reads the target through the post/reply content API and returns
only curated reaction groups. Each untrusted item contains `emoji_type`,
`user_list`, and `bot_reacted` for the bot bound to this context. An absent target
is an error, not an empty reaction list. Ambiguous or missing member identities
fail instead of claiming the bot did not react. The command does not return message
bodies, download URLs, cookies or raw upstream fields.

Discover targets with `post list`, `reply list`, `search result` or a verified
event. In mention-only mode, pass the matching `--event EVENT_ID` when reading
reactions; otherwise agent reads require `read_all`. No actor-selection flag can
impersonate another member or remove their reactions.

## Operator policy and permission paths

Reactions are disabled by default. The operator must explicitly set
`allow_reactions: true` for the channel. Permission is independent of
`allow_posts` and `allow_replies`; permitting one does not enable the others.

| `reaction_scope` | Required local authority | Required WeCom access |
| --- | --- | --- |
| `mentioned` (also the default when omitted) | Matching authenticated `topic_at_bot` event for the exact post/reply, no older than 24 hours | Access to react to the mentioned content |
| `channel` | Explicit operator policy plus `read_all: true` | Bot permission to read all channel content and proactively send |

For a polling deployment with proactive bot access, the channel policy can be:

```json
{
  "id": "CHANNEL_ID",
  "senders": [],
  "read_all": true,
  "observe_all": true,
  "allow_posts": false,
  "allow_replies": false,
  "allow_reactions": true,
  "reaction_scope": "channel"
}
```

A full read-only starter is in [reactions.example.json](../deploy/reactions.example.json).
It needs no callback listener. Proposing remains available with broker read-only
policy, but actual approval execution requires the operator to deliberately set
`read_only: false` after reviewing the deployment. WeCom enforces its own bot
permissions; local policy is a restriction, not a remote permission grant.

If `--event` is supplied, it must match even under channel scope. A mention on a
reply authorizes that exact reply, not another reply or its parent post. Eligibility
is checked both when creating the proposal and immediately before execution.
Polling never manufactures mention events, and `read_all` alone never enables
proactive reactions. Removing a reaction follows the same permission rules;
an expired mention does not grant indefinite removal access.

## Propose, review, apply

```sh
# Add a native +1 on the post under an operator-configured channel scope.
wecom-channel-cli --use-context engineering reaction propose \
  --post POST_ID --emoji-type 1 --action add --key acknowledge-post-1 --dry-run

wecom-channel-cli --use-context engineering reaction propose \
  --post POST_ID --emoji-type 1 --action add --key acknowledge-post-1

# Remove a native DONE reaction from the exact mentioned reply.
wecom-channel-cli --use-context engineering reaction propose \
  --post POST_ID --reply REPLY_ID --emoji-type 3 --action remove \
  --event EVENT_ID --key remove-done-1 --dry-run
```

Every proposal binds the channel, post, optional reply, native type, add/remove
action, event when supplied, policy and idempotency key. There is no default action
or toggle operation. Reusing a key with a different target, type or action fails.
Dry-run uses the real builder with no state changes or upstream request.

The trusted operator inspects the proposal and approves its exact digest through
the private socket, outside the agent's tool authority:

```sh
wecom-channel-cli --use-context engineering \
  --socket /run/wecom-channel/engineering-admin/admin.sock proposal get --id PROPOSAL_ID
wecom-channel-cli --use-context engineering \
  --socket /run/wecom-channel/engineering-admin/admin.sock approval approve \
  --id PROPOSAL_ID --digest REVIEWED_DIGEST --yes --dry-run
wecom-channel-cli --use-context engineering \
  --socket /run/wecom-channel/engineering-admin/admin.sock approval approve \
  --id PROPOSAL_ID --digest REVIEWED_DIGEST --yes
```

Review the exact reaction with the human just as for a textual reply: a small
gesture still expresses an action under the bot's identity. Agent sockets cannot
approve it, and `--allow-writes` cannot bypass server policy or operator approval.

## Success and uncertain outcomes

The provider acknowledges `post_emoji` with `errcode: 0` and no new resource ID.
The proposal becomes `sent`, meaning the action was acknowledged; `remote_id`
remains absent. Its immutable request contains the target/type/action receipt.
Do not invent a reaction ID or interpret this as a newly created message.

The broker persists `sending` before the request. A timeout or ambiguous response
becomes `uncertain`, and a crash may retain `sending`. Neither can be replayed.
Use `reaction list` on the exact post/reply (or the WeCom UI) to inspect
`bot_reacted` for that emoji type; an absent group means no membership was returned
for that type on the existing target. After independently reconciling the desired
state, the operator can resolve the proposal with its ID/digest and `--outcome
sent` or `not_sent`. **Omit `--remote-id` for reactions**; it is rejected because
the API creates no independent resource. Resolution changes the local ledger only.

Adding and removing are distinct reviewed intentions with separate keys. An
opposite action is not an automatic rollback for an uncertain operation.
Watching posts/replies does not emit a dedicated reaction-change stream; reaction
membership alone is excluded from polling fingerprints. Refresh `reaction list`
explicitly when current membership matters.

## Verification and sources

Tests cover native wire bodies, add/remove targets, acknowledgment without IDs,
permission and expiry checks, read-only/digest/replay gates, uncertain resolution,
reaction projection and real CLI-process flows against synthetic APIs. Live native
reactions have not been exercised against the test tenant yet.

The official [mention response API](https://developer.work.weixin.qq.com/document/path/101915)
documents the 24-hour mention path. The [proactive API](https://developer.work.weixin.qq.com/document/path/101924)
also documents `post_emoji`, allowing a separate operator-approved channel scope.
The [content API](https://developer.work.weixin.qq.com/document/path/101922) supplies
`emoji_list` and the type catalog. These are native channel operations, separate
from posting emoji text.
