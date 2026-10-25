#!/usr/bin/env python3
"""Validate local release material. --publish also checks Git/tag cleanliness."""
import hashlib
import json
import pathlib
import re
import subprocess
import sys
root = pathlib.Path(__file__).resolve().parents[1]
pkg = json.loads((root / 'build/npm/package.json').read_text())
version = pkg['version']
errors = []
checks = {}
for label, path, pattern in [
    ('CLI', 'pkg/constants/constants.go', r'var Version = "([^"]+)"'),
    ('Makefile', 'Makefile', r'VERSION \?= ([^\n]+)')]:
    match = re.search(pattern, (root / path).read_text(), re.M)
    checks[label] = bool(match and match[1] == version)
    if not checks[label]: errors.append(f'{label} version differs from {version}')
skill = (root / 'skills/wecom-channel/SKILL.md').read_text()
marker = re.search(r'^version: (.+)$', skill, re.M)
handshake = re.search(r'WECOM_CHANNEL_SKILL_LOADED=([^`\s]+)', skill)
checks['Skill'] = bool(marker and re.fullmatch(r'\d+\.\d+\.\d+', marker[1]))
checks['handshake'] = bool(marker and handshake and marker[1] == handshake[1])
if not checks['Skill'] or not checks['handshake']: errors.append('Skill version and loaded handshake must agree')
if f'## [{version}]' not in (root / 'CHANGELOG.md').read_text(): errors.append('Changelog has no candidate version entry')
for path in ('docs/index.html', 'docs/cli/index.html', 'docs/contexts.html', '.github/workflows/pages.yml', '.github/workflows/release.yml'):
    if not (root / path).is_file(): errors.append(f'Missing {path}')
manifest_path = root / 'dist/release-manifest.json'
if not manifest_path.exists(): errors.append('Run make package first')
else:
    manifest = json.loads(manifest_path.read_text())
    if manifest['version'] != version: errors.append('Artifact version is stale')
    for asset in manifest['assets']:
        path = root / 'dist' / asset['name']
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != asset['sha256']: errors.append(f'Bad artifact: {asset["name"]}')
if '--publish' in sys.argv:
    def git(*args): return subprocess.run(['git', *args], cwd=root, capture_output=True, text=True)
    status = git('status','--porcelain')
    if status.stdout.strip(): errors.append('Commit the reviewed candidate before publishing')
    remote = git('remote','get-url','origin')
    if remote.returncode or not re.search(r'github.com[:/]AngelMsger/wecom-channel-cli(?:\.git)?$',remote.stdout.strip(),re.I): errors.append('Configure the approved canonical origin')
    tag = git('describe','--tags','--exact-match','HEAD')
    if tag.returncode or tag.stdout.strip() != 'v'+version: errors.append('HEAD must carry the owner-approved release tag')
print(json.dumps({'version': version, 'ok': not errors, 'skill_version': marker[1] if marker else None, 'mode': 'publish' if '--publish' in sys.argv else 'local-candidate', 'checks': checks, 'errors': errors}, indent=2))
raise SystemExit(bool(errors))
