'use strict';
// Based on the family's MIT-licensed installer (Copyright 2026 AngelMsger),
// with mandatory checksums and bounded downloads for this connector.
// install.js downloads the prebuilt wecom-channel-cli binary that matches the
// host platform from the matching GitHub Release. It runs as the npm
// `postinstall` script. The runtime launcher never downloads implicitly.

const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');

const pkg = require('./package.json');

const REPO = 'AngelMsger/wecom-channel-cli';

const goosByPlatform = { darwin: 'darwin', linux: 'linux' };
const goarchByArch = { x64: 'amd64', arm64: 'arm64' };

// assetName returns the release asset file name for the current platform.
function assetName(platform = process.platform, arch = process.arch) {
  const goos = goosByPlatform[platform];
  const goarch = goarchByArch[arch];
  if (!goos || !goarch) {
    throw new Error(
      `unsupported platform ${platform}/${arch}; ` +
        `build from source instead (see https://github.com/${REPO})`
    );
  }
  return `wecom-channel-cli-${goos}-${goarch}` + (goos === 'windows' ? '.exe' : '');
}

// binPath returns the directory and file path for the installed binary.
function binPath(platform = process.platform) {
  const dir = path.join(__dirname, 'binary');
  const exe = platform === 'win32' ? 'wecom-channel-cli.exe' : 'wecom-channel-cli';
  return { dir, file: path.join(dir, exe) };
}

// releaseBaseURL is the GitHub Release download prefix for this package version.
function releaseBaseURL() {
  return `https://github.com/${REPO}/releases/download/v${pkg.version}`;
}

// Fetch only HTTPS release assets, with finite time, size and redirect budgets.
function httpGet(raw, redirects = 0) {
  const url = new URL(raw);
  if (url.protocol !== 'https:' || (url.port && url.port !== '443') || url.username || url.password ||
      !(url.hostname === 'github.com' || url.hostname.endsWith('.githubusercontent.com')) || redirects > 5) {
    return Promise.reject(new Error('release download destination rejected'));
  }
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'wecom-channel-cli-installer' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume(); resolve(httpGet(new URL(res.headers.location, url).href, redirects + 1)); return;
      }
      if (res.statusCode !== 200) { res.resume(); reject(new Error(`release download returned HTTP ${res.statusCode}`)); return; }
      let size = 0; const chunks = [];
      res.on('data', chunk => { size += chunk.length; if (size > 128 * 1024 * 1024) { req.destroy(new Error('release asset exceeds size limit')); return; } chunks.push(chunk); });
      res.on('end', () => resolve(Buffer.concat(chunks))); res.on('error', reject);
    });
    const deadline = setTimeout(() => req.destroy(new Error('release download timed out')), 30000);
    req.on('close', () => clearTimeout(deadline));
    req.on('error', reject);
  });
}
function verifyChecksum(data, manifest, asset) {
  const matches = manifest.split('\n').map(line => line.trim().split(/\s+/)).filter(parts => parts[1] === asset);
  if (matches.length !== 1 || !/^[a-fA-F0-9]{64}$/.test(matches[0][0])) throw new Error('missing or ambiguous release checksum');
  const actual = crypto.createHash('sha256').update(data).digest('hex');
  if (actual !== matches[0][0].toLowerCase()) throw new Error(`checksum mismatch for ${asset}`);
}

// install downloads, verifies and writes the binary. It is idempotent.
async function install() {
  if (!pkg.version || pkg.version === '0.0.0') {
    throw new Error('package version is unset; install from a published release');
  }
  const asset = assetName();
  const { dir, file } = binPath();

  const data = await httpGet(`${releaseBaseURL()}/${asset}`);

  const manifest = (await httpGet(`${releaseBaseURL()}/checksums.txt`)).toString('utf8');
  verifyChecksum(data, manifest, asset);
  fs.mkdirSync(dir, { recursive: true });
  const temporary = path.join(dir, `.download-${process.pid}-${crypto.randomBytes(8).toString('hex')}`);
  try { fs.writeFileSync(temporary, data, { mode: 0o755, flag: 'wx' }); fs.renameSync(temporary, file); }
  finally { fs.rmSync(temporary, { force: true }); }
  return file;
}

function welcomeText() {
  return 'After upgrading: wecom-channel-cli skill install, then reload the agent context.\nSetup: wecom-channel-cli auth guide; wecom-channel-cli config get-contexts\n';
}
module.exports = { install, binPath, assetName, verifyChecksum, httpGet, welcomeText, REPO };
if (require.main === module) {
  install().then(() => process.stderr.write(welcomeText())).catch(err => {
    process.stderr.write(`wecom-channel-cli: ${err.message}\n`); process.exitCode = 1;
  });
}
