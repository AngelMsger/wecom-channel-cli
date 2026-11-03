# WeCom Channel CLI 0.2.0

Set up channel access with Bot ID and Secret before selecting a channel ID.

- Use the existing `channel list --bot-id BOT_ID` with the operator's
  `WECOM_CHANNEL_BOT_SECRET` to discover joined channel names and IDs without
  first creating a policy, broker or client context. Reads support bounded
  pagination and offline dry-run.
- Continue using `channel list` through the configured context/socket for normal
  operation. Credentials in the environment never select direct access or
  recover a failed broker call. Explicit bot/socket/context conflicts fail.
- Fill the policy template with `config example --bot-id ... --channel ...`.
  Auth guidance, missing-ID recovery, README and companion Skill 0.1.6 now follow
  credentials → channel selection → policy → broker → client context.
- Match the first client context to the template's `default` name and socket.
- Make read-only enforcement structural: `pkg/apiclient.Client` combines the
  exported `Reader` and `Writer` interfaces, and missing write-blocking overrides
  fail compilation without changing the existing method signatures.
- Publish Linux/macOS amd64/arm64 binaries, checksums and the npm wrapper through
  the shared tag-triggered GitHub Actions workflow with npm OIDC provenance.

## Upgrade

```sh
npm install -g @angelmsger/wecom-channel-cli@0.2.0
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Reload the agent context after refreshing its Skill. Existing broker policies,
client contexts and agent socket restrictions remain valid. No migration or
credential re-entry is required for existing broker use.

The new credential-based setup path is tested with synthetic services. Earlier
live authentication, channel discovery and polling checks cover one test tenant;
live text/reaction writes, optional callbacks and actual production worker
isolation remain operator-specific verification. The connector does not sandbox
arbitrary agent tools or grant permissions from channel membership.
