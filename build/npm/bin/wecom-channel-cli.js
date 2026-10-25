#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const { spawnSync } = require('node:child_process');
const { binPath } = require('../install.js');
const { file } = binPath();
if (!fs.existsSync(file)) {
  process.stderr.write('wecom-channel-cli: binary missing; run npm rebuild @angelmsger/wecom-channel-cli in a trusted setup session. No automatic download occurs in an agent command.\n');
  process.exit(1);
}
const result = spawnSync(file, process.argv.slice(2), { stdio: 'inherit' });
if (result.error) { process.stderr.write('wecom-channel-cli: cannot start the installed binary.\n'); process.exit(1); }
if (result.signal) { process.kill(process.pid, result.signal); }
else process.exit(result.status === null ? 1 : result.status);
