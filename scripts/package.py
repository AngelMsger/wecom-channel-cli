#!/usr/bin/env python3
"""Create checksums and a local release manifest; never publish or tag."""
import hashlib
import json
import pathlib
import subprocess

root = pathlib.Path(__file__).resolve().parents[1]
package = json.loads((root / 'build/npm/package.json').read_text())
assets = [root / 'dist' / f'wecom-channel-cli-{os_name}-{arch}' for os_name in ('linux', 'darwin') for arch in ('amd64', 'arm64')]
rows = []
for asset in assets:
    if not asset.is_file():
        raise SystemExit(f'Missing release artifact: {asset.name}')
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    rows.append({'name': asset.name, 'sha256': digest, 'bytes': asset.stat().st_size})
(root / 'dist/checksums.txt').write_text(''.join(f'{r["sha256"]}  {r["name"]}\n' for r in rows))
commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True)
manifest = {'version': package['version'], 'repository': 'AngelMsger/wecom-channel-cli', 'commit': commit.stdout.strip() if commit.returncode == 0 else None, 'published': False, 'assets': rows}
(root / 'dist/release-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'version': package['version'], 'assets': len(rows), 'published': False}))
