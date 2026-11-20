# WeCom Channel CLI 0.4.4

A failed proposal approval now tells the operator how far the write got.
The approval error and `proposal get` after restart show the same evidence.
[Issue #16](https://github.com/AngelMsger/wecom-channel-cli/issues/16)

## Reading a failed approval

- `prepare` or `token_bootstrap` means the business request was never sent.
- `business_request` means the client handed the request to the transport.
  If the reply is lost or WeCom rejects the request, the remote outcome still
  needs operator verification.
- `failure.code` gives the normalized error. When the client decoded a WeCom
  response, `failure.upstream_errcode` gives its numeric code. This preserves
  the distinction between authentication errors such as 853000 and 853004.

Existing scripts can still use `WRITE_UNCERTAIN` and the proposal's `error_code`.
When an older record or an interrupted send lacks phase evidence, the outcome
remains unknown. Keep the original proposal and use the existing operator
reconciliation flow. [Proposal lifecycle](technical-design.md#write-state)

## Upgrade

```sh
npm install -g @angelmsger/wecom-channel-cli@0.4.4
wecom-channel-cli version
wecom-channel-cli skill install
wecom-channel-cli skill status
```

Restart the broker with the upgraded binary to record the new diagnostics.
Keep the existing ledger and reload companion Skill 0.1.14 in the agent context.
