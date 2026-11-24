"""Exercise the listener timer independently of the broker and notification transport."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("listener", Path(__file__).resolve().parents[1] / "deploy/check-listener.py")
listener = importlib.util.module_from_spec(spec)
spec.loader.exec_module(listener)


class ListenerTest(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(paused=False, interval=30, timeout=65, grace=60, queue_age=300,
                                    queue_limit=1000, outage=600, startup=300, snapshot_age=300)
        self.unit = dict(ActiveState="active", SubState="running", ActiveEnterTimestampMonotonic="1000000")
        self.state = dict(initialized=True, coverage={"backend": "poll"}, queued_events=0,
                          oldest_queued_at=0, last_scan_at=999000, last_snapshot_at=999000)

    def reason(self, **fields):
        return listener.assess(self.unit, self.state | fields, self.args, 1000)

    def test_live_progress_and_quiet_channel(self):
        self.assertEqual(self.reason(), "healthy")
        self.assertEqual(self.reason(last_scan_at=844000), "scan_stale")
        self.assertEqual(self.reason(scan_state="in_progress", last_snapshot_at=699000), "snapshot_stale")
        self.assertEqual(self.reason(scan_state="in_progress"), "scanning")
        self.assertEqual(self.reason(last_snapshot_at=0, coverage={"backend": "callback"}), "healthy")

    def test_delivery_and_recovery_limits(self):
        self.assertEqual(self.reason(queued_events=1, oldest_queued_at=699000), "queue_old")
        self.assertEqual(self.reason(queued_events=1000), "queue_full")
        self.assertEqual(self.reason(delivery_status="unknown"), "delivery_unknown")
        self.assertEqual(self.reason(scan_state="blocked"), "blocked")
        self.assertEqual(self.reason(first_scan_failure_at=950000, next_scan_at=1200000), "retry_wait")
        self.assertEqual(self.reason(first_scan_failure_at=399000, next_scan_at=1200000), "scan_outage")

    def test_down_even_with_recent_state_and_explicit_pause(self):
        self.unit["ActiveState"] = "inactive"
        self.assertEqual(self.reason(), "relay_down")
        self.args.paused = True
        self.assertEqual(self.reason(), "paused")
        self.unit["ActiveState"] = "active"
        self.assertEqual(self.reason(), "pause_mismatch")

    @patch("time.monotonic", return_value=303)
    def test_startup_deadline(self, _clock):
        self.assertEqual(self.reason(initialized=False), "baseline_overdue")
        self.assertEqual(listener.assess(self.unit, None, self.args, 1000), "state_unavailable")

    def test_down_result_reaches_monitor_even_without_local_sender_state(self):
        import contextlib
        import io
        import json
        output = io.StringIO()
        down = b"LoadState=loaded\nActiveState=inactive\nSubState=dead\nNRestarts=0\n"
        with patch("sys.argv", ["check-listener.py", "engineering"]), \
                patch.object(listener, "read", return_value=down) as run, \
                contextlib.redirect_stdout(output):
            self.assertEqual(listener.main(), 1)
        self.assertEqual(run.call_count, 1)
        record = json.loads(output.getvalue().removeprefix("<3>"))
        self.assertEqual(record, {"ok": False, "instance": "engineering", "reason": "relay_down", "restarts": 0})
