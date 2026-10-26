# WeCom channel API research

Checked on 2026-10-25. The implementation is based on official developer
documentation and the official CLI source, not guessed endpoints. Private
credentials and real channel content were not inspected.

## Authentication and reuse decision

The official CLI source at commit
[`55785fa0a5c922279533a5b5a836b44b56f65ce7`](https://github.com/WecomTeam/wecom-cli/tree/55785fa0a5c922279533a5b5a836b44b56f65ce7)
has these relevant boundaries:

| Component | Evidence | Implication |
| --- | --- | --- |
| Authentication bootstrap | [auth/bootstrap.rs](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/crates/wecom-cli/src/auth/bootstrap.rs) | Uses `/cgi-bin/aibot/cli/get_cli_config`, returns a gateway Bearer token; signing is SHA-256 of Secret + bot ID + decimal seconds + nonce |
| Credential ownership | [auth/types.rs](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/crates/wecom-cli/src/auth/types.rs), [auth/store.rs](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/crates/wecom-cli/src/auth/store.rs) | Bot credentials and token belong to the official CLI's private encrypted store |
| Binary vs libraries | [wecom-cli/Cargo.toml](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/crates/wecom-cli/Cargo.toml) | Auth is in the binary crate; `wecom` and `wecom-transport` are separate Rust libraries |
| Gateway envelopes | [transport/catalog.rs](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/crates/wecom-cli/src/transport/catalog.rs) | Gateway discovery/envelope behavior is not the channel HTTP schema |
| License | [MIT license](https://github.com/WecomTeam/wecom-cli/blob/55785fa0a5c922279533a5b5a836b44b56f65ce7/LICENSE) | Reuse is legally possible with attribution, but does not solve the protocol or privilege boundary |

The [channel token API](https://developer.work.weixin.qq.com/document/path/101910)
uses `/cgi-bin/aibot/get_token`, the same signing formula, and a one-day
`access_token` passed in query parameters. There is no documented guarantee
that the gateway token is interchangeable. The broker obtains and caches the
channel token in memory, renewing before expiry; a rejected token is discarded
without replaying the failed operation. The next separately initiated request
can acquire a fresh one.

Decision: use the workspace's maintained Go `agent-cli-kit` for generic
transport, output, errors and argv handling. Implement the small documented
channel protocol in Go. Do not vendor private Rust auth modules, parse encrypted
official credential files, scrape an exported Secret, or shell out to the
official CLI on behalf of a channel message. Revisit supported credential
delegation if Tencent publishes a stable API for it.

### Coexistence with an existing official CLI

Rechecked the official CLI's authentication source and
[command reference](https://github.com/WecomTeam/wecom-cli/blob/main/docs/cli-reference.md)
on 2026-10-26. Its supported login methods are QR binding and manual bot ID/Secret
entry. `auth show` displays authorization status and bot ID; it is not a supported
Secret export or channel-API credential delegation interface. A remembered login
method is not required to preserve the existing installation.

Keep the following checks separate:

| Check | What it establishes | What it does not establish |
| --- | --- | --- |
| Existing official CLI login | The CLI has its own authorization configuration | Channel API access, callback configuration, or shared-token compatibility |
| Broker `auth check` and bounded channel discovery | Channel API access for the supplied bot at that time | Callback delivery, complete watch coverage, or production isolation |
| Authenticated callback challenge and observed test event | The configured callback route works for that bot/event | Isolation of the worker or delivery of every event |

The separate `get_cli_config` and `get_token` endpoints do not document a guarantee
that credential issuance or refresh for the same bot cannot affect another
consumer. Do not claim coexistence is verified from endpoint names alone.
For first live verification, use a dedicated API-enabled test bot in a designated
test channel with the required grayscale access. This avoids changing the bot
already used by the official CLI. Keep the test broker read-only initially.

For an intentional shared-bot deployment, inventory its existing integrations
and connection configuration first, obtain the existing Secret through the
operator's supported management path, and verify both consumers before and after
channel access. If the Secret is unavailable, do not decrypt the CLI store, reset
the bot, or rerun `auth init` as a recovery shortcut. Use a separate test bot or
wait for a supported delegation mechanism. Coordinate any callback setup with
the existing receiver; this connector does not automatically install callback
fan-out or take over an active connection. A read-only HTTP check does not test
`event watch`, which consumes verified callbacks rather than polling remote posts.

## Connection mode and access

The [overview](https://developer.work.weixin.qq.com/document/path/101906)
describes channel rollout from version 5.0.12 and still marks channel long
connections as in development. It distinguishes three bot permission levels:
read all and proactively send; read all and reply only when mentioned; and
read/reply only when mentioned. Do not confuse `TopicInfo.permission`
(public/private channel) with the bot's capabilities. The CLI enforces its own
narrower operator policy; WeCom remains the final authority on API access.

[URL callbacks](https://developer.work.weixin.qq.com/document/path/101908)
require a publicly reachable address. Connection modes are mutually exclusive;
changing them invalidates the existing mode. The connector never changes this
configuration itself. Existing group-channel compatibility can cause duplicate
responses; the operator must coordinate it using the documented
[stop-sync operation](https://developer.work.weixin.qq.com/document/path/101948),
which this version deliberately does not expose.

## Endpoint coverage

All implemented requests are fixed HTTPS POSTs on `qyapi.weixin.qq.com`, under
`/cgi-bin/aibot/`. There is no arbitrary URL, path, method or request-body command.

| Official documentation | Endpoint suffix | Support |
| --- | --- | --- |
| [Token](https://developer.work.weixin.qq.com/document/path/101910) | `get_token` | Broker only; memory cache |
| [Channel information](https://developer.work.weixin.qq.com/document/path/101925) | `topic/get_topic_list`, `topic/get_topic_member_list` | Discovery, one bounded page; agent channel list is restricted to configured IDs |
| [Mention content](https://developer.work.weixin.qq.com/document/path/101913) / [all content](https://developer.work.weixin.qq.com/document/path/101922) | `topic/get_post_list`, `topic/get_reply_list` | Exact reads and bounded listing; attachment credentials removed |
| [Search](https://developer.work.weixin.qq.com/document/path/101923) | `topic/search`, `topic/get_search_result` | Two-step search scoped to one allowed channel; job ID is discoverable from matching channels |
| [Mention replies](https://developer.work.weixin.qq.com/document/path/101915) / [proactive content](https://developer.work.weixin.qq.com/document/path/101924) | `topic/send_post`, `topic/send_reply` | Reviewed text/Markdown only, up to 20480 UTF-8 bytes |
| [Events](https://developer.work.weixin.qq.com/document/path/101918) | Callback JSON | Seven documented kinds recognized; mentions remain human-allowlisted, while explicit observe_all permits post/reply observations from all actors |
| [Crypto](https://developer.work.weixin.qq.com/document/path/101909) / [algorithm](https://developer.work.weixin.qq.com/document/path/101778) | GET verification and encrypted POST | SHA-1 sorted signature, AES-256-CBC, PKCS#7 block size 32, empty receive ID for self-built bots |
| [Temporary media](https://developer.work.weixin.qq.com/document/path/101917) | Upload lifecycle | Deferred: filesystem access and content disclosure require a separate capability design |
| [Forwarded chat records](https://developer.work.weixin.qq.com/document/path/101959) | Chat-record reads | Deferred; no automatic expansion |
| [Emoji](https://developer.work.weixin.qq.com/document/path/101915) | `topic/post_emoji` | Deferred |

The maximum exposed page size is deliberately smaller than the upstream maximum
where useful: 50 for remote reads, 20 for search results, and 100 for local inbox
or proposal pages. Agent channel discovery is bound to exactly one channel per context.

## Documentation ambiguities handled conservatively

- Mention-only content is available within 24 hours. The broker checks the
  triggering event age before context reads and reply approvals.
- Examples use both `creator` and `creator_info`, and both `aibotid` and `botid`.
  The curated model retains these variants without treating a display name as
  identity or authority.
- Channel time filtering does not clearly specify inclusive/exclusive edges.
  Post/reply listings apply `[from,to)` locally as well as sending the bounds;
  this cannot recover records omitted upstream. Search summaries have no times,
  so their time restriction is server-native and cannot be independently checked
  without fetching the complete messages. Do not claim exact history coverage
  from search results.
- The official AES-key example is not canonical Base64. Ordinary key decoding
  is required for compatibility; ciphertext is strictly decoded. An independent
  known-answer test uses the official encrypted sample.
- No remote idempotency key is documented for sending posts/replies. Local
  at-most-once attempt tracking prevents automatic replay in one retained
  broker ledger; it does not establish remote exactly-once delivery.
