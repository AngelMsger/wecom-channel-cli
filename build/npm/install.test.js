'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { assetName, verifyChecksum, httpGet } = require('./install.js');
test('only shipped platform targets resolve', () => {
  for (const platform of ['linux','darwin']) for(const arch of ['x64','arm64']) assert.match(assetName(platform,arch),/^wecom-channel-cli-(linux|darwin)-(amd64|arm64)$/);
  assert.throws(()=>assetName('win32','x64')); assert.throws(()=>assetName('linux','ia32'));
});
test('checksums are mandatory, unique and exact',()=>{
  const data=Buffer.from('synthetic executable');const hash=crypto.createHash('sha256').update(data).digest('hex');
  verifyChecksum(data,`${hash}  asset\n`,'asset');
  for(const manifest of ['', `${'0'.repeat(64)}  asset`,`${hash}  asset\n${hash}  asset`, `${hash}  other`]) assert.throws(()=>verifyChecksum(data,manifest,'asset'));
});
test('unsafe download destinations are rejected before network I/O',async()=>{
  for(const url of ['http://github.com/release','https://evil.invalid/release','https://user:secret@github.com/release','file:///etc/passwd']) await assert.rejects(httpGet(url));
});
