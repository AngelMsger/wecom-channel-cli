#!/usr/bin/env python3
"""Run synthetic distinct-UID checks without exposing the host or enabling networking."""
import json
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "python:3.13-slim@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962"


def main():
    result = subprocess.run(["docker", "version", "--format", "{{json .Server}}"], capture_output=True, text=True, check=True)
    server = json.loads(result.stdout)
    arch = server.get("Arch")
    if server.get("Os") != "linux" or arch not in ("arm64", "amd64"):
        raise SystemExit("Use a Linux Docker daemon on arm64 or amd64.")
    binary = ROOT / "dist" / ("wecom-channel-cli-linux-" + arch)
    if not binary.is_file():
        raise SystemExit("Run make cross before the isolation check.")
    name = "wecom-channel-isolation-" + uuid.uuid4().hex[:12]
    command = ["docker", "run", "--rm", "--name", name, "--network", "none", "--read-only",
               "--cap-drop", "ALL", "--cap-add", "SETUID", "--cap-add", "SETGID",
               "--cap-add", "CHOWN", "--cap-add", "FOWNER", "--cap-add", "FSETID",
               "--cap-add", "KILL", "--security-opt", "no-new-privileges",
               "--memory", "256m", "--pids-limit", "128", "--cpus", "1",
               "--tmpfs", "/case:rw,nosuid,nodev,size=32m,mode=0755",
               "--mount", f"type=bind,src={binary},dst=/usr/local/bin/wecom-channel-cli,readonly",
               "--mount", f"type=bind,src={ROOT / 'scripts/isolation-fixture.py'},dst=/fixture.py,readonly",
               IMAGE, "python3", "-B", "/fixture.py"]
    try:
        return subprocess.run(command, timeout=90).returncode
    finally:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=15)


if __name__ == "__main__":
    raise SystemExit(main())
