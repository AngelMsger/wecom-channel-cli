#!/usr/bin/env python3
"""Synthetic Linux UID/group isolation checks; run only in the disposable container."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

ROOT = Path("/case")
BINARY = "/usr/local/bin/wecom-channel-cli"
ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin"}


def identity(uid, gid):
    def apply():
        os.setgroups([gid])
        os.setgid(gid)
        os.setuid(uid)
    return apply


def mkdir(path, uid, gid, mode):
    path.mkdir()
    os.chown(path, uid, gid)
    path.chmod(mode)


def cli(uid, gid, channel, *args, admin=False):
    socket_path = ROOT / channel / ("admin" if admin else "agent") / "a.sock"
    return subprocess.run([BINARY, "--config-dir", f"/case/worker-{uid}",
                           "--socket", str(socket_path), *args],
                          env=ENV, preexec_fn=identity(uid, gid), capture_output=True,
                          text=True, timeout=10)


def main():
    if sys.platform != "linux" or os.geteuid() != 0 or not Path("/.dockerenv").exists():
        raise SystemExit("Run through run-isolation-check.py in its disposable container.")
    mkdir(ROOT / "etc", 0, 0, 0o755)
    secret = ROOT / "etc/secrets.env"
    secret.write_text("WECOM_CHANNEL_BOT_SECRET=synthetic-isolation-secret\n")
    secret.chmod(0o600)
    mkdir(ROOT / "operator-home", 10001, 20001, 0o700)
    # Provision before removing supervisor traversal access; values are synthetic.
    for uid, gid in ((10001, 20001), (10002, 20001), (10003, 20002)):
        mkdir(ROOT / f"worker-{uid}", uid, gid, 0o700)
    contexts = []
    for name, group in (("a", 20001), ("b", 20002)):
        mkdir(ROOT / name, 0, 0, 0o755)
        for child, mode in (("agent", 0o2750), ("admin", 0o700), ("state", 0o700)):
            mkdir(ROOT / name / child, 10001, group, mode)
        contexts.append({"name": name, "state_dir": f"/case/{name}/state",
                         "agent_socket": f"/case/{name}/agent/a.sock",
                         "admin_socket": f"/case/{name}/admin/a.sock",
                         "read_only": True,
                         "channels": [{"id": "channel-" + name, "senders": []}]})
    policy = ROOT / "etc/policy.json"
    policy.write_text(json.dumps({"version": 2, "bot_id": "synthetic-bot", "callback_listen": "", "contexts": contexts}))
    policy.chmod(0o644)
    broker = subprocess.Popen([BINARY, "broker", "serve", "--config", str(policy)],
                              env={**ENV, "WECOM_CHANNEL_BOT_SECRET": "synthetic-isolation-secret"},
                              preexec_fn=identity(10001, 20001), stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 10
        while True:
            ready = cli(10002, 20001, "a", "doctor")
            if ready.returncode == 0:
                break
            if broker.poll() is not None or time.monotonic() >= deadline:
                raise AssertionError("broker_not_ready")
            time.sleep(0.1)
        results = {"worker_a_agent_socket": True,
                   "worker_b_agent_socket": cli(10003, 20002, "b", "doctor").returncode == 0,
                   "operator_admin_socket": cli(10001, 20001, "a", "approval", "list", admin=True).returncode == 0}
        forbidden = cli(10002, 20001, "a", "approval", "list")
        results["agent_cannot_approve"] = forbidden.returncode != 0 and "POLICY_DENIED" in forbidden.stderr
        # This child has no host mounts, credentials, root UID or effective capabilities.
        probe = r'''
import errno, json, os, pathlib, socket, sys
result = {}
def denied(name, action):
    try:
        action()
        result[name] = False
    except OSError as error:
        result[name] = error.errno in (errno.EACCES, errno.EPERM, errno.EROFS, errno.ENETUNREACH, errno.EHOSTUNREACH)
def connect(path):
    with socket.socket(socket.AF_UNIX) as client:
        client.connect(path)
denied('operator_socket_denied', lambda: connect('/case/a/admin/a.sock'))
denied('other_context_denied', lambda: connect('/case/b/agent/a.sock'))
denied('broker_state_denied', lambda: pathlib.Path('/case/a/state/ledger.json').read_bytes())
denied('secret_file_denied', lambda: pathlib.Path('/case/etc/secrets.env').read_bytes())
denied('operator_home_denied', lambda: list(pathlib.Path('/case/operator-home').iterdir()))
denied('policy_write_denied', lambda: pathlib.Path('/case/etc/policy.json').write_text('changed'))
denied('broker_environment_denied', lambda: pathlib.Path('/proc/' + sys.argv[1] + '/environ').read_bytes())
denied('root_escalation_denied', lambda: os.setuid(0))
denied('root_filesystem_write_denied', lambda: pathlib.Path('/usr/local/probe').write_text('test'))
def network():
    with socket.socket() as client:
        client.settimeout(1)
        client.connect(('192.0.2.1', 443))
denied('external_network_denied', network)
result['secret_not_in_worker_environment'] = 'WECOM_CHANNEL_BOT_SECRET' not in os.environ
status = pathlib.Path('/proc/self/status').read_text()
result['worker_has_no_capabilities'] = 'CapEff:\t0000000000000000' in status
result['no_new_privileges'] = 'NoNewPrivs:\t1' in status
result['no_docker_socket'] = not pathlib.Path('/var/run/docker.sock').exists()
scratch = pathlib.Path('/case/worker-10002/scratch')
scratch.write_text('allowed'); result['worker_scratch_writable'] = scratch.read_text() == 'allowed'
print(json.dumps(result))
'''
        checked = subprocess.run([sys.executable, "-c", probe, str(broker.pid)], env=ENV,
                                 preexec_fn=identity(10002, 20001), capture_output=True,
                                 text=True, timeout=10, check=True)
        results.update(json.loads(checked.stdout))
        result = {"mode": "synthetic-linux-container", "checks": results, "passed": all(results.values()),
                  "live_wecom_used": False, "production_deployment_attested": False}
        print(json.dumps(result, indent=2))
        return 0 if result["passed"] else 1
    finally:
        broker.terminate()
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
            broker.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
