"""Local release asset contract shared by packaging, validation and publication."""
import hashlib
import json
import re
from pathlib import Path
import subprocess
import tarfile

REPOSITORY = "AngelMsger/wecom-channel-cli"
BINARY_NAMES = tuple(f"wecom-channel-cli-{system}-{arch}" for system in ("linux", "darwin") for arch in ("amd64", "arm64"))
NPM_FILES = ("LICENSE", "README.md", "package.json", "install.js", "bin/wecom-channel-cli.js")


def npm_name(version):
    return f"angelmsger-wecom-channel-cli-{version}.tgz"


def asset_names(version):
    return (*BINARY_NAMES, npm_name(version))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect_binary(path):
    result = subprocess.run(["go", "version", "-m", str(path)], capture_output=True, text=True, check=True)
    fields = {}
    for line in result.stdout.splitlines():
        if line.startswith("\tbuild\t") and "=" in line:
            key, value = line[len("\tbuild\t"):].split("=", 1)
            fields[key] = value
    return fields


def verify_assets(root, version, commit, inspect=inspect_binary):
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
        return ["Invalid release version"]
    errors = []
    dist = root / "dist"
    if dist.is_symlink():
        return ["Release directory must not be a symlink"]
    try:
        manifest_path = dist / "release-manifest.json"
        if manifest_path.is_symlink():
            return ["Release manifest must not be a symlink"]
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("version") != version or manifest.get("repository") != REPOSITORY or manifest.get("commit") != commit or manifest.get("published") is not False:
            errors.append("Release manifest does not match this candidate version/repository/commit")
        assets = manifest["assets"]
        expected = asset_names(version)
        if not isinstance(assets, list) or len(assets) != len(expected) or [a.get("name") for a in assets] != list(expected):
            return errors + ["Manifest must contain exactly the four supported binaries and current npm tarball, in canonical order"]
        checksum_lines = []
        for asset in assets:
            path = dist / asset["name"]
            if path.is_symlink() or not path.is_file():
                errors.append(f"Missing or unsafe artifact: {path.name}")
                continue
            actual = digest(path)
            if asset.get("sha256") != actual or type(asset.get("bytes")) is not int or asset["bytes"] != path.stat().st_size:
                errors.append(f"Artifact hash or size mismatch: {path.name}")
            checksum_lines.append(f"{actual}  {path.name}\n")
            if path.name in BINARY_NAMES:
                info = inspect(path)
                system, arch = path.name.removeprefix("wecom-channel-cli-").split("-")
                if info.get("GOOS") != system or info.get("GOARCH") != arch or info.get("vcs.revision") != commit or info.get("vcs.modified") != "false":
                    errors.append(f"Binary platform/source provenance mismatch: {path.name}")
        checksums = dist / "checksums.txt"
        if checksums.is_symlink() or checksums.read_text() != "".join(checksum_lines):
            errors.append("checksums.txt does not exactly match the packaged assets")
        tarball = dist / npm_name(version)
        if tarball.is_symlink():
            return errors + ["npm tarball must not be a symlink"]
        with tarfile.open(tarball, "r:gz") as archive:
            members = archive.getmembers()
            names = [m.name for m in members]
            if len(names) != len(NPM_FILES) or set(names) != {"package/" + name for name in NPM_FILES} or any(not m.isfile() or m.size > 1024 * 1024 for m in members):
                errors.append("npm tarball contains missing, extra, oversized or unsafe entries")
            else:
                for name in NPM_FILES:
                    if archive.extractfile("package/" + name).read() != (root / "build/npm" / name).read_bytes():
                        errors.append(f"npm package differs from reviewed source: {name}")
    except (OSError, ValueError, KeyError, TypeError, AttributeError, tarfile.TarError, subprocess.SubprocessError):
        errors.append("Release material is missing, malformed or cannot be inspected; run make package")
    return errors
