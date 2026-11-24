#!/usr/bin/python3
"""Read-only host probe for one explicitly provisioned listener."""
import argparse
import json
import re
import subprocess
import time


def assess(unit, state, args, now):
    active = unit.get("ActiveState")
    if args.paused:
        return "paused" if active == "inactive" else "pause_mismatch"
    if active != "active" or unit.get("SubState") != "running":
        return "relay_down"
    # systemd's activation time uses the host monotonic clock.
    uptime = time.monotonic() - int(unit["ActiveEnterTimestampMonotonic"]) / 1_000_000
    if not state:
        return "starting" if 0 <= uptime < args.startup else "state_unavailable"
    if state.get("scan_state") == "blocked" or state.get("terminal_error"):
        return "blocked"
    if state.get("delivery_status") == "unknown":
        return "delivery_unknown"
    delivery = state.get("last_delivery_error")
    if (delivery and state.get("delivery_status") == "rejected"
            and state.get("delivery_attempts", 0) > delivery["retry_limit"]):
        return "delivery_exhausted"
    attempt = state.get("last_attempt_at", 0) / 1000
    if state.get("delivery_status") == "sending" and attempt and now - attempt > 30 + args.grace:
        return "delivery_stalled"
    depth = state["queued_events"]
    if depth and depth >= state.get("queue_limit", args.queue_limit):
        return "queue_full"
    oldest = state["oldest_queued_at"] / 1000
    if depth and (oldest <= 0 or now - oldest > args.queue_age):
        return "queue_age_unknown" if oldest <= 0 else "queue_old"
    retry = state.get("next_scan_at", 0) / 1000
    failure = state.get("first_scan_failure_at", 0) / 1000
    if failure and now - failure > args.outage:
        return "scan_outage"
    if not state["initialized"]:
        start = state.get("started_at", 0) / 1000
        age = now - start if start else uptime
        return "starting" if 0 <= age < args.startup else "baseline_overdue"
    coverage = state["coverage"]
    interval = max(args.interval, coverage.get("poll_interval_seconds", 0))
    grace = args.timeout + args.grace
    scan = state["last_scan_at"] / 1000
    if not scan or now > max(scan + interval, retry) + grace:
        return "scan_stale"
    if coverage.get("backend") == "poll" and "last_snapshot_at" in state:
        completed = state["last_snapshot_at"] / 1000
        if not completed or now - completed > args.snapshot_age:
            return "snapshot_stale"
    if failure:
        return "retry_wait"
    return "scanning" if state.get("scan_state") == "in_progress" else "healthy"


def read(command):
    result = subprocess.run(command, timeout=5, check=True, capture_output=True,
                            stdin=subprocess.DEVNULL, env={"PATH": "/usr/bin:/bin", "LANG": "C"})
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance")
    parser.add_argument("--paused", action="store_true")
    for name, default in [("interval", 30), ("timeout", 65), ("grace", 60), ("startup", 300),
                          ("queue-age", 300), ("queue-limit", 1000), ("snapshot-age", 300),
                          ("outage", 600)]:
        parser.add_argument("--" + name, type=int, default=default)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", args.instance) or any(
            value <= 0 for value in vars(args).values() if type(value) is int):
        parser.error("invalid listener name or probe bound")
    reason, restarts = "probe_failed", None
    try:
        unit = dict(line.split("=", 1) for line in read([
            "/usr/bin/systemctl", "show", "wecom-channel-relay@" + args.instance + ".service",
            "--property=LoadState,ActiveState,SubState,ActiveEnterTimestampMonotonic,NRestarts"
        ]).decode().splitlines())
        if unit["LoadState"] != "loaded":
            raise ValueError()
        restarts = int(unit["NRestarts"])
        state = None
        if not args.paused and unit["ActiveState"] == "active":
            try:
                state = json.loads(read(["/usr/local/bin/wecom-channel-cli", "relay", "status",
                                         "--state", "/var/lib/wecom-relay-" + args.instance + "/state.json"]))
            except subprocess.CalledProcessError:
                pass
        reason = assess(unit, state, args, time.time())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        pass
    ok = reason in ("healthy", "retry_wait", "starting", "paused", "scanning")
    print(("<6>" if ok else "<3>") + json.dumps(
        {"instance": args.instance, "reason": reason, "restarts": restarts, "ok": ok}))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
