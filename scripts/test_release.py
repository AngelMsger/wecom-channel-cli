"""Exercise the real release steps with local command substitutes, never services."""
import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / '.github/workflows/release.yml').read_text()


def run_step(name):
    """Extract a named literal shell step without a YAML dependency in CI."""
    steps = re.split(r'^      - ', WORKFLOW, flags=re.M)
    block = next(s for s in steps if s.startswith('name: ' + name + '\n'))
    if '        run: |\n' in block:
        return textwrap.dedent(block.split('        run: |\n', 1)[1])
    return re.search(r'^        run: (.+)$', block, re.M)[1]


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'tools'
        self.bin.mkdir()
        self.log = self.root / 'calls.jsonl'
        self.env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                    'TAG': 'v9.8.7', 'RELEASE_TEST_LOG': str(self.log)}
        self.stub = '''#!/usr/bin/env python3
import json, os
from pathlib import Path
import sys
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['RELEASE_TEST_LOG'], 'a') as f:
    f.write(json.dumps({'name': name, 'args': args, 'os': os.getenv('GOOS'), 'arch': os.getenv('GOARCH'), 'cgo': os.getenv('CGO_ENABLED')}) + '\\n')
if name == 'gh':
    if args[:2] == ['release', 'view']:
        sys.exit(0 if os.getenv('RELEASE_TEST_EXISTS') == '1' else 1)
    sys.exit(int(os.getenv('RELEASE_TEST_GH_EXIT', '0')))
if name == 'node':
    print('v24.0.0')
if name == 'npm':
    if args == ['--version']:
        print('11.20.0')
    elif args and args[0] == 'view':
        sys.exit(0 if os.getenv('RELEASE_TEST_PUBLISHED') == '1' else 1)
    elif args and args[0] == 'publish':
        sys.exit(int(os.getenv('RELEASE_TEST_NPM_EXIT', '0')))
if name == 'go':
    out = Path(args[args.index('-o') + 1])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text('synthetic binary')
'''
        for name in ('gh', 'npm', 'node', 'go'):
            p = self.bin / name
            p.write_text(self.stub)
            p.chmod(0o755)
        (self.root / 'dist').mkdir()
        (self.root / 'dist/checksums.txt').write_text('synthetic checksum')

    def calls(self, name=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [r['args'] for r in rows if r['name'] == name] if name else rows

    def shell(self, step):
        return subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', run_step(step)],
                              cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10)

    def test_tag_trigger_and_oidc_runtime_follow_family(self):
        self.assertRegex(WORKFLOW, r"(?s)on:\s+push:\s+tags:\s+- 'v\*'")
        self.assertNotRegex(WORKFLOW, r'(?m)^\s*workflow_dispatch:')
        self.assertIn('environment: release', WORKFLOW)
        self.assertIn('id-token: write', WORKFLOW)
        self.assertIn('go-version-file: go.mod', WORKFLOW)
        self.assertIn('node-version: 24', WORKFLOW)
        self.assertNotRegex(WORKFLOW, r'(?m)^\s+registry-url:')
        self.assertIn('working-directory: build/npm', WORKFLOW)
        self.assertNotIn('scripts/publish-release.py', WORKFLOW)

    def test_new_release_creates_assets_and_generated_notes(self):
        self.assertEqual(self.shell('create or update GitHub Release').returncode, 0)
        calls = self.calls('gh')
        self.assertEqual(calls[0], ['release', 'view', 'v9.8.7'])
        self.assertEqual(calls[1], ['release', 'create', 'v9.8.7', 'dist/checksums.txt',
                                   '--title', 'v9.8.7', '--generate-notes'])

    def test_existing_release_refreshes_assets_without_creation(self):
        self.env['RELEASE_TEST_EXISTS'] = '1'
        self.assertEqual(self.shell('create or update GitHub Release').returncode, 0)
        self.assertEqual(self.calls('gh')[1], ['release', 'upload', 'v9.8.7',
                                               'dist/checksums.txt', '--clobber'])

    def test_release_upload_failure_stops_the_step(self):
        self.env.update(RELEASE_TEST_EXISTS='1', RELEASE_TEST_GH_EXIT='1')
        self.assertNotEqual(self.shell('create or update GitHub Release').returncode, 0)

    def test_new_npm_version_comes_from_tag_and_publishes_once(self):
        self.assertEqual(self.shell('publish to npm').returncode, 0)
        calls = self.calls('npm')
        self.assertIn(['version', '9.8.7', '--no-git-tag-version', '--allow-same-version'], calls)
        self.assertIn(['view', '@angelmsger/wecom-channel-cli@9.8.7', 'version'], calls)
        self.assertEqual(calls.count(['publish', '--access', 'public']), 1)
        # No immediate post-publish lookup that can mistake propagation delay for failure.
        self.assertEqual(sum(args[0] == 'view' for args in calls), 1)

    def test_existing_npm_version_skips_publish(self):
        self.env['RELEASE_TEST_PUBLISHED'] = '1'
        self.assertEqual(self.shell('publish to npm').returncode, 0)
        self.assertFalse(any(args[0] == 'publish' for args in self.calls('npm')))

    def test_npm_failure_is_not_hidden(self):
        self.env['RELEASE_TEST_NPM_EXIT'] = '1'
        self.assertNotEqual(self.shell('publish to npm').returncode, 0)
        self.assertEqual(self.calls('npm').count(['publish', '--access', 'public']), 1)

    def test_checksums_cover_release_binaries(self):
        data = {'wecom-channel-cli-linux-amd64': b'linux', 'wecom-channel-cli-darwin-arm64': b'mac'}
        for name, content in data.items():
            (self.root / 'dist' / name).write_bytes(content)
        subprocess.run(['bash', '-e', '-c', run_step('checksums')],
                       cwd=self.root / 'dist', check=True, timeout=10)
        actual = (self.root / 'dist/checksums.txt').read_text()
        for name, content in data.items():
            self.assertIn(hashlib.sha256(content).hexdigest() + '  ' + name, actual)
        self.assertEqual(len(actual.splitlines()), 2)

    def test_tag_version_reaches_make_and_every_supported_binary(self):
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()
        git('init', '-q')
        git('config', 'user.name', 'Release Test')
        git('config', 'user.email', 'release-test@example.invalid')
        git('-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-qm', 'fixture')
        git('-c', 'tag.gpgsign=false', 'tag', 'v9.8.7')
        probe = self.root / 'probe.mk'
        probe.write_text('print-version:\n\t@echo $(VERSION)\n')
        version = subprocess.check_output(['make', '-s', '-f', str(ROOT / 'Makefile'),
            '-f', str(probe), 'print-version'], cwd=self.root, text=True).strip()
        self.assertEqual(version, 'v9.8.7')
        (self.root / 'scripts').mkdir()
        shutil.copy2(ROOT / 'scripts/build.sh', self.root / 'scripts/build.sh')
        subprocess.run(['bash', 'scripts/build.sh'], cwd=self.root, env=self.env,
                       capture_output=True, text=True, check=True, timeout=10)
        calls = [row for row in self.calls() if row['name'] == 'go']
        self.assertEqual({(row['os'], row['arch']) for row in calls},
                         {('darwin', 'amd64'), ('darwin', 'arm64'), ('linux', 'amd64'), ('linux', 'arm64')})
        for row in calls:
            self.assertEqual(row['cgo'], '0')
            flags = row['args'][row['args'].index('-ldflags') + 1]
            self.assertIn('.Version=v9.8.7', flags)
            self.assertIn('.Commit=' + git('rev-parse', '--short', 'HEAD'), flags)


if __name__ == '__main__':
    unittest.main()
