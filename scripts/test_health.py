"""Exercise bounded deployment probes without contacting WeCom or alert receivers."""
import configparser
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("health", ROOT / "deploy/check-health.py")
health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health)


def unit(name):
    config = configparser.ConfigParser(interpolation=None)
    config.read(ROOT / "deploy" / name)
    return config


class HealthTest(unittest.TestCase):
    def run_sequence(self, layer, codes):
        calls, delays = [], []
        def run(command, **kwargs):
            calls.append((command, kwargs))
            result = codes.pop(0)
            if isinstance(result, Exception):
                raise result
            return subprocess.CompletedProcess(command, result)
        result = health.probe(layer, "/run/test.sock", run=run, sleep=delays.append)
        return result, calls, delays

    def test_transient_failure_recovers_without_third_probe(self):
        result, calls, delays = self.run_sequence("local", [8, 0])
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(delays, [5])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0][-1:], ["doctor"])

    def test_persistent_failure_has_finite_budget_and_no_restart(self):
        for layer in health.BOUNDS:
            with self.subTest(layer=layer):
                result, calls, delays = self.run_sequence(layer, [4, 4, 4])
                self.assertEqual(result, {"layer": layer, "status": "unhealthy",
                    "attempts": 3, "reason": "cli_failure", "exit_code": 4})
                self.assertEqual(len(delays), 2)
                for command, opts in calls:
                    self.assertEqual(command[0], health.CLI)
                    self.assertNotIn("restart", command)
                    self.assertNotIn("WECOM_CHANNEL_BOT_SECRET", opts["env"])
                    self.assertEqual(opts["stdout"], subprocess.DEVNULL)
                    self.assertEqual(opts["stderr"], subprocess.DEVNULL)
                    self.assertEqual(opts["stdin"], subprocess.DEVNULL)
                    self.assertLessEqual(opts["timeout"], 35)
                    self.assertEqual(command[-2:] if layer == "upstream" else command[-1:],
                                     ["auth", "check"] if layer == "upstream" else ["doctor"])

    def test_timeout_and_missing_executable_are_redacted(self):
        for exc, reason in [(subprocess.TimeoutExpired("secret", 1), "probe_timeout"),
                            (OSError("secret"), "probe_execution_failed")]:
            result, calls, delays = self.run_sequence("local", [exc, exc, exc])
            self.assertEqual(result["reason"], reason)
            self.assertNotIn("secret", str(result))
            self.assertEqual(len(calls), 3)

    def test_real_child_exit_timeout_and_secret_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "cli"
            binary.write_text('''#!/usr/bin/python3
import os, sys, time
assert "WECOM_CHANNEL_BOT_SECRET" not in os.environ
assert "WECOM_CHANNEL_CONTEXT" not in os.environ
assert sys.argv[1:3] == ["--socket", "/run/test.sock"]
assert sys.argv[-2:] == ["auth", "check"]
sys.exit(4)
''')
            binary.chmod(0o700)
            with patch.dict(os.environ, {"WECOM_CHANNEL_BOT_SECRET": "synthetic-secret",
                                        "WECOM_CHANNEL_CONTEXT": "wrong"}):
                result = health.probe("upstream", "/run/test.sock", binary=str(binary),
                                      sleep=lambda _: None)
            self.assertEqual(result["exit_code"], 4)
            binary.write_text('#!/usr/bin/python3\nimport time\ntime.sleep(10)\n')
            with patch.dict(health.BOUNDS, {"local": (1, 0.05, 0)}):
                result = health.probe("local", "/run/test.sock", binary=str(binary))
            self.assertEqual(result["reason"], "probe_timeout")

    def test_main_emits_only_sanitized_health_status(self):
        for status, code, priority in [("healthy", 0, "<6>"), ("unhealthy", 1, "<3>")]:
            report = {"layer": "local", "status": status, "attempts": 3,
                      "reason": "ok" if code == 0 else "probe_timeout", "exit_code": None}
            output = io.StringIO()
            with patch("sys.argv", ["check-health.py", "local", "/run/test.sock"]), \
                 patch.object(health, "probe", return_value=report), \
                 contextlib.redirect_stdout(output):
                self.assertEqual(health.main(), code)
            self.assertTrue(output.getvalue().startswith(priority))
            self.assertEqual(json.loads(output.getvalue()[3:]), report)

    def test_unit_authority_limits_and_failure_routing(self):
        broker = unit("wecom-channel-broker.service")
        self.assertEqual(broker["Service"]["Restart"], "always")
        self.assertIn("3", broker["Service"]["RestartPreventExitStatus"].split())
        self.assertNotIn("2", broker["Service"]["RestartPreventExitStatus"].split())
        for name in ("wecom-channel-broker.service", "wecom-channel-relay.service",
                     "wecom-channel-relay@.service"):
            config = unit(name)
            self.assertEqual(config["Unit"]["StartLimitBurst"], "5")
            self.assertEqual(config["Unit"]["StartLimitIntervalSec"], "300")
            self.assertEqual(config["Unit"]["OnFailure"], "wecom-channel-alert@%n.service")
        for layer in health.BOUNDS:
            config = unit(f"wecom-channel-health-{layer}.service")
            service = config["Service"]
            self.assertNotIn("EnvironmentFile", service)
            self.assertEqual(service["Restart"], "no")
            self.assertEqual(service["RestrictAddressFamilies"], "AF_UNIX")
            self.assertNotIn("Requires", config["Unit"])
            self.assertNotIn("ConditionPathExists", config["Unit"])
            self.assertEqual(service["User"], "wecom-health" if layer == "local" else "wecom-channel")
            _, hard_timeout, delay = health.BOUNDS[layer]
            self.assertGreater(int(service["TimeoutStartSec"]), 3 * hard_timeout + 2 * delay)
            timer = unit(f"wecom-channel-health-{layer}.timer")
            self.assertEqual(timer["Timer"]["Unit"], f"wecom-channel-health-{layer}.service")


if __name__ == "__main__":
    unittest.main()
