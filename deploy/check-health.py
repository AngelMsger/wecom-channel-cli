#!/usr/bin/python3
"""Bounded host-side probes; no credentials, remote writes, or restart authority."""
import argparse
import json
import os
import subprocess
import time

CLI = "/usr/local/bin/wecom-channel-cli"
ATTEMPTS = 3
# (CLI timeout, hard process timeout, delay between failures)
BOUNDS = {"local": (5, 10, 5), "upstream": (30, 35, 10)}


def probe(layer, socket, *, binary=CLI, run=subprocess.run, sleep=time.sleep):
    request_timeout, hard_timeout, delay = BOUNDS[layer]
    command = [binary, "--socket", socket, "--timeout", f"{request_timeout}s"]
    command += ["doctor"] if layer == "local" else ["auth", "check"]
    # Do not inherit credentials, client context selection, or runtime injection
    # variables. The service already holds credentials; auth uses its admin socket.
    environment = {"PATH": "/usr/bin:/bin", "LANG": "C"}
    for attempt in range(1, ATTEMPTS + 1):
        try:
            result = run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, timeout=hard_timeout,
                         env=environment, check=False)
            code = result.returncode
            reason = "ok" if code == 0 else "cli_failure"
        except subprocess.TimeoutExpired:
            code, reason = None, "probe_timeout"
        except OSError:
            code, reason = None, "probe_execution_failed"
        if reason == "ok":
            return {"layer": layer, "status": "healthy", "attempts": attempt,
                    "reason": reason, "exit_code": code}
        if attempt < ATTEMPTS:
            sleep(delay)
    return {"layer": layer, "status": "unhealthy", "attempts": ATTEMPTS,
            "reason": reason, "exit_code": code}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("layer", choices=BOUNDS)
    parser.add_argument("socket")
    args = parser.parse_args()
    if not os.path.isabs(args.socket):
        parser.error("socket must be absolute")
    result = probe(args.layer, args.socket)
    # systemd strips this prefix and records the corresponding journal priority.
    priority = 6 if result["status"] == "healthy" else 3
    print(f"<{priority}>" + json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
