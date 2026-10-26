#!/usr/bin/env python3
"""Create checksums and a local release manifest; never publish or tag."""
import hashlib
import json
import pathlib
import subprocess
from release_material import REPOSITORY, asset_names

root = pathlib.Path(__file__).resolve().parents[1]
package = json.loads((root / 'build/npm/package.json').read_text())
assets = [root / 'dist' / name for name in asset_names(package['version'])]
rows = []
for asset in assets:
    if asset.is_symlink() or not asset.is_file():
        raise SystemExit(f'Missing release artifact: {asset.name}')
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    rows.append({'name': asset.name, 'sha256': digest, 'bytes': asset.stat().st_size})
(root / 'dist/checksums.txt').write_text(''.join(f'{r["sha256"]}  {r["name"]}\n' for r in rows))
commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True)
manifest = {'version': package['version'], 'repository': REPOSITORY, 'commit': commit.stdout.strip() if commit.returncode == 0 else None, 'published': False, 'assets': rows}
(root / 'dist/release-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'version': package['version'], 'assets': len(rows), 'published': False}))
