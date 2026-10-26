#!/usr/bin/env python3
"""Validate local release material. --publish also checks Git/tag cleanliness."""
import json
import pathlib
import platform
import re
import subprocess
import sys
from release_material import verify_assets
root = pathlib.Path(__file__).resolve().parents[1]
pkg = json.loads((root / 'build/npm/package.json').read_text())
version = pkg['version']
errors = []
checks = {}
checks['package_metadata'] = pkg.get('name') == '@angelmsger/wecom-channel-cli' and pkg.get('repository', {}).get('url') == 'git+https://github.com/AngelMsger/wecom-channel-cli.git'
if not checks['package_metadata']: errors.append('Package identity differs from the approved candidate coordinates')
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
def git(*args): return subprocess.run(['git', *args], cwd=root, capture_output=True, text=True)
head = git('rev-parse', 'HEAD').stdout.strip()
asset_errors = verify_assets(root, version, head)
errors.extend(asset_errors)
checks['artifacts'] = not asset_errors
if not asset_errors:
    system = platform.system().lower()
    arch = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())
    try:
        path = root / 'dist' / f'wecom-channel-cli-{system}-{arch}'
        result = subprocess.run([str(path), 'version'], env={}, capture_output=True, text=True, timeout=10, check=True)
        native = json.loads(result.stdout)
        checks['native_version'] = native.get('version') == version and native.get('commit') == git('rev-parse', '--short', 'HEAD').stdout.strip()
        if not checks['native_version']: errors.append('Native executable version/commit differs from the candidate')
    except (OSError, ValueError, subprocess.SubprocessError): errors.append('Cannot verify the native executable version')
if '--publish' in sys.argv:
    status = git('status','--porcelain')
    if status.stdout.strip(): errors.append('Commit the reviewed candidate before publishing')
    remote = git('remote','get-url','origin')
    if remote.returncode or not re.search(r'github.com[:/]AngelMsger/wecom-channel-cli(?:\.git)?$',remote.stdout.strip(),re.I): errors.append('Configure the approved canonical origin')
    tag = git('describe','--tags','--exact-match','HEAD')
    if tag.returncode or tag.stdout.strip() != 'v'+version: errors.append('HEAD must carry the owner-approved release tag')
print(json.dumps({'version': version, 'ok': not errors, 'skill_version': marker[1] if marker else None, 'mode': 'publish' if '--publish' in sys.argv else 'local-candidate', 'checks': checks, 'errors': errors}, indent=2))
raise SystemExit(bool(errors))
