#!/usr/bin/env python3
"""Owner-gated publication: reconcile immutable assets before any retry."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess

from release_material import REPOSITORY, asset_names, digest, npm_name

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = "https://registry.npmjs.org"


def run(argv):
    return subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=180)


def require(argv):
    result = run(argv)
    if result.returncode:
        raise RuntimeError(f"Command failed: {argv[0]} {argv[1]}; reconcile the existing release/version before retrying")
    return result.stdout


def github(path, optional=False):
    result = run(["gh", "api", path])
    if result.returncode:
        if optional and "(HTTP 404)" in result.stderr:
            return None
        raise RuntimeError("GitHub state could not be read; publication stopped")
    return json.loads(result.stdout)


def release_by_tag(tag):
    # REST's releases/tags route resolves published releases, not drafts.
    result = run(["gh", "release", "view", tag, "--repo", REPOSITORY,
                  "--json", "databaseId,tagName"])
    if result.returncode:
        if result.stderr.strip() == "release not found":
            return None
        raise RuntimeError("GitHub release identity could not be read; publication stopped")
    identity = json.loads(result.stdout)
    release_id = identity.get("databaseId")
    if type(release_id) is not int or release_id <= 0 or identity.get("tagName") != tag:
        raise RuntimeError("GitHub returned a mismatched release identity")
    release = github(f"repos/{REPOSITORY}/releases/{release_id}")
    if release.get("id") != release_id or release.get("tag_name") != tag:
        raise RuntimeError("GitHub returned a mismatched release record")
    return release


def upload_plan(local, remote):
    seen = set()
    for asset in remote:
        name = asset.get("name")
        if name in seen or name not in local:
            raise RuntimeError("Existing release contains duplicate or unexpected assets; reconcile it manually")
        seen.add(name)
        if asset.get("state") != "uploaded" or asset.get("digest") != "sha256:" + local[name]:
            raise RuntimeError(f"Existing release asset differs or has no verified digest: {name}; it will not be overwritten")
    return [name for name in local if name not in seen]


def npm_state(package, integrity):
    result = run(["npm", "view", package, "dist.integrity", "--json", "--registry", REGISTRY])
    try:
        value = json.loads(result.stdout)
    except ValueError:
        raise RuntimeError("npm registry state is unknown; publication stopped") from None
    if result.returncode:
        if isinstance(value, dict) and value.get("error", {}).get("code") == "E404":
            return "missing"
        raise RuntimeError("npm lookup failed; publication stopped")
    if value != integrity:
        raise RuntimeError("The npm version already exists with different content; it will not be replaced")
    return "matching"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Publish only after owner-controlled tag/environment approval")
    args = parser.parse_args()
    require(["python3", "scripts/release-check.py", "--publish"])
    pkg = json.loads((ROOT / "build/npm/package.json").read_text())
    version, tag = pkg["version"], "v" + pkg["version"]
    if os.environ.get("TAG", tag) != tag:
        raise RuntimeError("Workflow tag differs from package version")
    commit = require(["git", "rev-parse", "HEAD"]).strip()
    object_ = github(f"repos/{REPOSITORY}/git/ref/tags/{tag}")["object"]
    for _ in range(5):
        if object_["type"] != "tag":
            break
        object_ = github(f"repos/{REPOSITORY}/git/tags/{object_['sha']}")["object"]
    if object_.get("type") != "commit" or object_.get("sha") != commit:
        raise RuntimeError("Remote release tag does not resolve to the reviewed source commit")
    names = (*asset_names(version), "checksums.txt", "release-manifest.json")
    local = {name: digest(ROOT / "dist" / name) for name in names}
    release = release_by_tag(tag)
    missing = upload_plan(local, release["assets"] if release else [])
    archive = ROOT / "dist" / npm_name(version)
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(archive.read_bytes()).digest()).decode()
    package = pkg["name"] + "@" + version
    registry = npm_state(package, integrity)
    print(json.dumps({"tag": tag, "commit": commit, "missing_assets": missing,
                      "npm": registry, "apply": args.apply}, indent=2))
    if not args.apply:
        return 0
    if release is None:
        require(["gh", "release", "create", tag, "--repo", REPOSITORY, "--draft", "--verify-tag",
                 "--title", tag, "--notes-file", "docs/release-notes.md"])
        release = release_by_tag(tag)
        if release is None:
            raise RuntimeError("The new draft could not be reconciled; inspect the release before retrying")
    for name in missing:
        require(["gh", "release", "upload", tag, str(ROOT / "dist" / name), "--repo", REPOSITORY])
    release = github(f"repos/{REPOSITORY}/releases/{release['id']}")
    if upload_plan(local, release["assets"]):
        raise RuntimeError("Release upload remains incomplete; draft retained for reconciliation")
    if release.get("draft"):
        require(["gh", "release", "edit", tag, "--repo", REPOSITORY, "--draft=false", "--latest"])
    if registry == "missing":
        require(["npm", "publish", str(archive), "--access", "public", "--provenance", "--registry", REGISTRY])
        # A failed follow-up preserves the known version; a retry must reconcile it.
        if npm_state(package, integrity) != "matching":
            raise RuntimeError(f"Publish returned success but {package} is not confirmed; reconcile it before retrying")
    print(json.dumps({"tag": tag, "npm_package": package, "verified": True}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error))
