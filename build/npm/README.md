# WeCom Channel CLI

An independent channel connector for agents. Named channel contexts, a protected
credential broker, polling without public ingress, optional authenticated
callbacks, native emoji reactions, and exact-intent operator approvals.

After the first release:

```sh
npm install -g @angelmsger/wecom-channel-cli
wecom-channel-cli auth guide
wecom-channel-cli skill install
```

Reload the agent context after refreshing the Skill. Operator provisioning and
OS isolation are required before unattended use. Installation downloads the
matching Linux/macOS binary and requires a valid SHA-256 checksum manifest.
The launcher never downloads a missing binary during an agent command; if scripts
were disabled, run `npm rebuild @angelmsger/wecom-channel-cli` in a trusted setup
session. Windows is not supported in this release.

[Documentation](https://angelmsger.github.io/wecom-channel-cli/)
· [Source](https://github.com/AngelMsger/wecom-channel-cli)
