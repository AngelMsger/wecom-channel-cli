# WeCom Channel CLI

An independent channel connector for agents. Named channel contexts, a protected
credential broker, polling without public ingress, optional authenticated
callbacks, native emoji reactions, and exact-intent operator approvals.

Install or upgrade:

```sh
npm install -g @angelmsger/wecom-channel-cli@0.4.2
wecom-channel-cli auth guide
wecom-channel-cli skill install
```

For initial operator setup, supply `WECOM_CHANNEL_BOT_SECRET` locally and run
`wecom-channel-cli channel list --bot-id BOT_ID` before configuring a channel.
Choose `data.topic_id`, generate policy with `policy example --bot-id BOT_ID
--channel CHANNEL_ID`, then provision the broker and matching client context.
Normal `channel list` uses the configured broker socket.

Reload the agent context after refreshing the Skill. Operator provisioning and
OS isolation are required before unattended use. Installation downloads the
matching Linux/macOS binary and requires a valid SHA-256 checksum manifest.
The launcher never downloads a missing binary during an agent command; if scripts
were disabled, run `npm rebuild @angelmsger/wecom-channel-cli` in a trusted setup
session. Windows is not supported in this release.

[Documentation](https://angelmsger.github.io/wecom-channel-cli/)
· [Source](https://github.com/AngelMsger/wecom-channel-cli)
