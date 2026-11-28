package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/monitor"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func runRootWithMonitor(deps relayMonitorDeps, args ...string) (int, string, string) {
	root := newRootCmdWithMonitor(newProfileRuntime(), deps)
	var out, errBuf bytes.Buffer
	root.SetIn(strings.NewReader(""))
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs(args)
	if err := root.ExecuteContext(context.Background()); err != nil {
		ce, ok := err.(*e.CLIError)
		if !ok {
			ce = e.New(e.CategoryUsage, "USAGE", "Invalid command or arguments; inspect --help.")
		}
		b, _ := json.Marshal(map[string]any{"error": ce})
		errBuf.Write(b)
		return e.ExitCode(ce), out.String(), errBuf.String()
	}
	return 0, out.String(), errBuf.String()
}

func writeRelayStateFixture(t *testing.T, path string, st relay.State) {
	t.Helper()
	if err := st.Validate(); err != nil {
		t.Fatalf("invalid relay state fixture: %v", err)
	}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func makePrivateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestRelayCheckAndNotifyEndToEndEdgeLifecycle(t *testing.T) {
	root := makePrivateTempDir(t)
	relayDir := filepath.Join(root, "relay")
	probeDir := filepath.Join(root, "probe")
	for _, d := range []string{relayDir, probeDir} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	relayStatePath := filepath.Join(relayDir, "state.json")
	alertStatePath := filepath.Join(probeDir, "probe-state.json")
	outboxDir := filepath.Join(probeDir, "outbox")
	lockPath := filepath.Join(probeDir, "notify.lock")

	// Prepare a real executable notifier stub that records each delivered event
	// JSON line and verifies WeCom bot secrets are stripped while alert env survives.
	logPath := filepath.Join(root, "notified.jsonl")
	scriptPath := filepath.Join(root, "notifier.sh")
	script := "#!/bin/sh\nset -eu\n" +
		"if [ -n \"${WECOM_CHANNEL_BOT_SECRET:-}\" ]; then exit 99; fi\n" +
		"if [ \"${ALERT_CUSTOM_TOKEN:-}\" != \"alert-ok\" ]; then exit 98; fi\n" +
		"cat >> " + logPath + "\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WECOM_CHANNEL_BOT_SECRET", "must-be-stripped")
	t.Setenv("ALERT_CUSTOM_TOKEN", "alert-ok")

	base := time.Unix(1_764_120_000, 0)
	curNow := base
	curMono := 1000.0
	curUnit := monitor.UnitStatus{
		LoadState:                  "loaded",
		ActiveState:                "active",
		SubState:                   "running",
		ActiveEnterMonotonicMicros: 1_000_000,
	}
	var unitErr error
	deps := relayMonitorDeps{
		queryUnit: func(_ context.Context, _ string) (monitor.UnitStatus, error) {
			if unitErr != nil {
				return monitor.UnitStatus{}, unitErr
			}
			return curUnit, nil
		},
		monotonicNow: func() (float64, error) { return curMono, nil },
		now:          func() time.Time { return curNow },
	}

	st := relay.State{
		Version:  1,
		Consumer: strings.Repeat("a", 64),
		Binding:  strings.Repeat("b", 64),
		Identity: broker.WatchIdentity{
			Context: "engineering",
			Bot:     "bot-1",
			Channel: "channel-1",
			Ledger:  strings.Repeat("c", 32),
		},
		StartedAt:    base.Add(-1 * time.Hour).UnixMilli(),
		QueueLimit:   1000,
		Coverage:     broker.WatchCoverage{Backend: "poll", PollIntervalSeconds: 30},
		LastScan:     base.UnixMilli(),
		LastSnapshot: base.UnixMilli(),
	}
	writeRelayStateFixture(t, relayStatePath, st)

	checkArgs := []string{
		"relay", "check",
		"--instance", "engineering",
		"--state", relayStatePath,
		"--alert-state", alertStatePath,
		"--outbox", outboxDir,
	}

	// 1. Short 4-minute freshness gap (< 10m default) -> exit 0, 0 queued events.
	curNow = base.Add(4 * time.Minute)
	code, out, stderr := runRootWithMonitor(deps, checkArgs...)
	if code != 0 || stderr != "" || !strings.Contains(out, `"ok": true`) || !strings.Contains(out, `"transition": "none"`) {
		t.Fatalf("4m gap: code=%d out=%s err=%s", code, out, stderr)
	}
	if _, err := os.Stat(outboxDir); !os.IsNotExist(err) {
		t.Fatalf("short gap should not create/enqueue outbox, err=%v", err)
	}

	// 2. Gap crosses 10m (11m) while notifier is stopped -> exit 11 (LISTENER_UNHEALTHY),
	//    1 alert queued in outbox.
	curNow = base.Add(11 * time.Minute)
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 11 || out != "" || !strings.Contains(stderr, `"LISTENER_UNHEALTHY"`) || !strings.Contains(stderr, `"transition":"alert"`) {
		t.Fatalf("11m crossing: code=%d out=%s err=%s", code, out, stderr)
	}

	// 3. Continuing stale + retry backoff at 12m -> STILL exit 11 & ok=false,
	//    but transition="none" and still only 1 queued event.
	curNow = base.Add(12 * time.Minute)
	st.FirstFailure = curNow.UnixMilli()
	st.ScanFailures = 1
	st.NextScan = curNow.Add(5 * time.Minute).UnixMilli()
	st.LastScanError = &relay.ErrorRecord{Code: "SCAN_TIMEOUT", Cause: "The scan request timed out before completing.", Phase: "scan", At: curNow.UnixMilli(), Attempt: 1, RetryLimit: -1, Retryable: true}
	writeRelayStateFixture(t, relayStatePath, st)
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 11 || out != "" || !strings.Contains(stderr, `"reason":"scan_stale"`) || !strings.Contains(stderr, `"transition":"none"`) || !strings.Contains(stderr, `"notified":false`) {
		t.Fatalf("12m backoff continuing: code=%d out=%s err=%s", code, out, stderr)
	}

	// 4. Deteriorates into relay_down at 13m -> exit 11, 1 escalation queued.
	curNow = base.Add(13 * time.Minute)
	curUnit.ActiveState = "inactive"
	curUnit.SubState = "dead"
	curUnit.NRestarts = 1
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 11 || !strings.Contains(stderr, `"transition":"escalated"`) || !strings.Contains(stderr, `"reason":"relay_down"`) {
		t.Fatalf("13m escalation: code=%d out=%s err=%s", code, out, stderr)
	}

	// 5. Still down at 14m -> exit 11, transition="none" (escalates only once!).
	curNow = base.Add(14 * time.Minute)
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 11 || !strings.Contains(stderr, `"transition":"none"`) {
		t.Fatalf("14m continuing down: code=%d out=%s err=%s", code, out, stderr)
	}

	// 6. Genuine recovery at 15m (still before notifier has run!) -> exit 0,
	//    1 recovered event queued alongside alert & escalated (no overwrite!).
	curNow = base.Add(15 * time.Minute)
	curUnit.ActiveState = "active"
	curUnit.SubState = "running"
	st.ResetScan()
	st.LastScan = curNow.UnixMilli()
	st.LastSnapshot = curNow.UnixMilli()
	writeRelayStateFixture(t, relayStatePath, st)
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 0 || stderr != "" || !strings.Contains(out, `"transition": "recovered"`) {
		t.Fatalf("15m recovery: code=%d out=%s err=%s", code, out, stderr)
	}

	// Verify all 3 immutable events accumulated in outbox while notifier was down.
	entries, err := os.ReadDir(outboxDir)
	if err != nil || len(entries) != 3 {
		t.Fatalf("expected 3 queued outbox events, got %d err=%v", len(entries), err)
	}

	// 7. Start notifier now -> drains alert, escalated, recovered in strict FIFO order.
	code, out, stderr = runRootWithMonitor(deps, "relay", "notify", "--outbox", outboxDir, "--lock", lockPath, "--exec", scriptPath)
	if code != 0 || stderr != "" || !strings.Contains(out, `"delivered": 3`) || !strings.Contains(out, `"remaining": 0`) {
		t.Fatalf("relay notify: code=%d out=%s err=%s", code, out, stderr)
	}
	rawLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read notifier log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(rawLog)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 delivered events, got %d:\n%s", len(lines), string(rawLog))
	}
	wantTransitions := []string{"alert", "escalated", "recovered"}
	for i, line := range lines {
		var ev monitor.NotificationEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("line %d invalid json: %v", i, err)
		}
		if ev.Transition != wantTransitions[i] || ev.Sequence != uint64(i+1) {
			t.Fatalf("event %d = %+v, want transition=%s seq=%d", i, ev, wantTransitions[i], i+1)
		}
	}
	entries, err = os.ReadDir(outboxDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected empty outbox after notify, entries=%v err=%v", entries, err)
	}

	// 8. Probe execution failure (systemctl error) exits 1 (NOT 11!) so OnFailure fires.
	unitErr = e.New(e.CategoryInternal, "PROBE_FAILED", "Failed to query systemd unit state.")
	code, out, stderr = runRootWithMonitor(deps, checkArgs...)
	if code != 1 || out != "" || !strings.Contains(stderr, `"PROBE_FAILED"`) {
		t.Fatalf("probe execution error must exit 1: code=%d out=%s err=%s", code, out, stderr)
	}
	unitErr = nil

	// 9. Cross-instance alert-state misuse exits 3 (config), not 11.
	code, _, stderr = runRootWithMonitor(deps, "relay", "check", "--instance", "other", "--state", relayStatePath, "--alert-state", alertStatePath)
	if code != 3 || !strings.Contains(stderr, `"PROBE_STATE_MISMATCH"`) {
		t.Fatalf("instance mismatch must exit 3: code=%d err=%s", code, stderr)
	}

	// 10. --outbox without --alert-state is a usage error (exit 2).
	code, _, stderr = runRootWithMonitor(deps, "relay", "check", "--instance", "engineering", "--state", relayStatePath, "--outbox", outboxDir)
	if code != 2 || !strings.Contains(stderr, `"BAD_PROBE_OPTIONS"`) {
		t.Fatalf("outbox without alert-state must exit 2: code=%d err=%s", code, stderr)
	}
}

func TestRelayCheckServiceMissingAndBrokerUnavailable(t *testing.T) {
	root := makePrivateTempDir(t)
	relayDir := filepath.Join(root, "relay")
	probeDir := filepath.Join(root, "probe")
	for _, d := range []string{relayDir, probeDir} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	relayStatePath := filepath.Join(relayDir, "state.json")
	alertStatePath := filepath.Join(probeDir, "probe-state.json")
	outboxDir := filepath.Join(probeDir, "outbox")

	base := time.Unix(1_764_120_000, 0)
	curNow := base
	curUnit := monitor.UnitStatus{LoadState: "not-found", ActiveState: "inactive", SubState: "dead"}
	deps := relayMonitorDeps{
		queryUnit:    func(_ context.Context, _ string) (monitor.UnitStatus, error) { return curUnit, nil },
		monotonicNow: func() (float64, error) { return 1000, nil },
		now:          func() time.Time { return curNow },
	}
	args := []string{
		"relay", "check",
		"--instance", "engineering",
		"--state", relayStatePath,
		"--freshness-threshold", "1h",
		"--alert-state", alertStatePath,
		"--outbox", outboxDir,
	}

	// Uninstalled unit -> immediate service_missing (exit 11, hard alert), even with 1h freshness threshold.
	code, _, stderr := runRootWithMonitor(deps, args...)
	if code != 11 || !strings.Contains(stderr, `"reason":"service_missing"`) || !strings.Contains(stderr, `"transition":"alert"`) {
		t.Fatalf("service_missing: code=%d err=%s", code, stderr)
	}

	// Unit installed & running, but active BROKER_UNREACHABLE -> still unhealthy, no false recovery.
	curNow = base.Add(time.Minute)
	curUnit = monitor.UnitStatus{LoadState: "loaded", ActiveState: "active", SubState: "running", ActiveEnterMonotonicMicros: 1_000_000}
	st := relay.State{
		Version:       1,
		Consumer:      strings.Repeat("a", 64),
		Binding:       strings.Repeat("b", 64),
		Identity:      broker.WatchIdentity{Context: "engineering", Bot: "bot-1", Channel: "channel-1", Ledger: strings.Repeat("c", 32)},
		StartedAt:     base.Add(-1 * time.Hour).UnixMilli(),
		QueueLimit:    1000,
		Coverage:      broker.WatchCoverage{Backend: "poll", PollIntervalSeconds: 30},
		LastScan:      base.UnixMilli(),
		LastSnapshot:  base.UnixMilli(),
		FirstFailure:  curNow.UnixMilli(),
		ScanFailures:  1,
		NextScan:      curNow.Add(time.Minute).UnixMilli(),
		LastScanError: &relay.ErrorRecord{Code: "BROKER_UNREACHABLE", Cause: "The broker did not return a response.", Phase: "scan", At: curNow.UnixMilli(), Attempt: 1, RetryLimit: -1, Retryable: true},
	}
	writeRelayStateFixture(t, relayStatePath, st)
	code, _, stderr = runRootWithMonitor(deps, args...)
	if code != 11 || !strings.Contains(stderr, `"reason":"broker_unavailable"`) || !strings.Contains(stderr, `"transition":"none"`) {
		t.Fatalf("broker_unavailable while incident active: code=%d err=%s", code, stderr)
	}

	// Broker returns and a new scan succeeds (LastScanError stays as history!) -> genuine recovery.
	curNow = base.Add(2 * time.Minute)
	st.ResetScan()
	st.LastScan = curNow.UnixMilli()
	st.LastSnapshot = curNow.UnixMilli()
	writeRelayStateFixture(t, relayStatePath, st)
	code, out, stderr := runRootWithMonitor(deps, args...)
	if code != 0 || stderr != "" || !strings.Contains(out, `"reason": "healthy"`) || !strings.Contains(out, `"transition": "recovered"`) {
		t.Fatalf("recovery with historical BROKER_UNREACHABLE: code=%d out=%s err=%s", code, out, stderr)
	}

	// Drain outbox via NDJSON stdout mode and verify exact 2 events (alert then recovered).
	code, out, stderr = runRootWithMonitor(deps, "--format", "ndjson", "relay", "notify", "--outbox", outboxDir)
	if code != 0 || stderr != "" {
		t.Fatalf("ndjson notify drain: code=%d out=%s err=%s", code, out, stderr)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"reason":"service_missing"`) || !strings.Contains(lines[1], `"transition":"recovered"`) {
		t.Fatalf("unexpected ndjson events:\n%s", out)
	}
}

func TestRelayMonitorSystemdUnitContracts(t *testing.T) {
	readUnit := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", "deploy", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(b)
	}

	health := readUnit("wecom-channel-health-relay@.service")
	for _, want := range []string{
		"OnFailure=wecom-channel-alert@%n.service",
		"ExecStart=/usr/local/bin/wecom-channel-cli relay check --instance %i --alert-state /var/lib/wecom-health-relay-%i/probe-state.json --outbox /var/lib/wecom-health-relay-%i/outbox",
		"SuccessExitStatus=11",
		"StateDirectory=wecom-health-relay-%i",
		"StateDirectoryMode=0700",
		"ReadOnlyPaths=-/var/lib/wecom-relay-%i",
		"ProtectSystem=strict",
		"RestrictAddressFamilies=AF_UNIX",
	} {
		if !strings.Contains(health, want) {
			t.Errorf("wecom-channel-health-relay@.service missing %q", want)
		}
	}
	for _, forbid := range []string{"python3", "check-listener.py", "EnvironmentFile"} {
		if strings.Contains(health, forbid) {
			t.Errorf("wecom-channel-health-relay@.service must not contain %q", forbid)
		}
	}

	pathUnit := readUnit("wecom-channel-notify-relay@.path")
	for _, want := range []string{
		"DirectoryNotEmpty=/var/lib/wecom-health-relay-%i/outbox",
		"Unit=wecom-channel-notify-relay@%i.service",
	} {
		if !strings.Contains(pathUnit, want) {
			t.Errorf("wecom-channel-notify-relay@.path missing %q", want)
		}
	}

	notify := readUnit("wecom-channel-notify-relay@.service")
	for _, want := range []string{
		"OnFailure=wecom-channel-alert@%n.service",
		"ExecStart=/usr/local/bin/wecom-channel-cli relay notify --outbox /var/lib/wecom-health-relay-%i/outbox --lock /run/wecom-notify-relay-%i/notify.lock --exec /usr/bin/logger -- --priority daemon.err --tag wecom-channel-alert",
		"RuntimeDirectory=wecom-notify-relay-%i",
		"ReadWritePaths=-/var/lib/wecom-health-relay-%i/outbox",
		"InaccessiblePaths=-/var/lib/wecom-relay-%i",
		"ProtectSystem=strict",
		"RestrictAddressFamilies=AF_UNIX",
	} {
		if !strings.Contains(notify, want) {
			t.Errorf("wecom-channel-notify-relay@.service missing %q", want)
		}
	}
	if strings.Contains(notify, "EnvironmentFile=") {
		t.Error("default notify unit must not ship with EnvironmentFile=")
	}
}

func TestCLIProcessRelayNotify(t *testing.T) {
	binary := os.Getenv("WECOM_CHANNEL_TEST_BINARY")
	if binary == "" {
		t.Skip("make e2e supplies the executable")
	}
	root := makePrivateTempDir(t)
	outboxDir := filepath.Join(root, "outbox")
	lockPath := filepath.Join(root, "notify.lock")
	logPath := filepath.Join(root, "delivered.jsonl")
	scriptPath := filepath.Join(root, "record.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nset -eu\ncat >> "+logPath+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if _, err := monitor.EnqueueOutbox(outboxDir, monitor.NotificationEvent{
		Version: 1, Sequence: 1, ID: "eng-1", Instance: "engineering",
		Unit: "wecom-channel-relay@engineering.service", Transition: monitor.TransitionAlert,
		FailureClass: monitor.ClassFreshness, Reason: "scan_stale", OpenedAt: now, OccurredAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.EnqueueOutbox(outboxDir, monitor.NotificationEvent{
		Version: 1, Sequence: 2, ID: "eng-2", Instance: "engineering",
		Unit: "wecom-channel-relay@engineering.service", Transition: monitor.TransitionRecovered,
		FailureClass: monitor.ClassNone, Reason: "healthy", PreviousReason: "scan_stale",
		OpenedAt: now, OccurredAt: now + 60, DurationSeconds: 60,
	}); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(binary, "relay", "notify", "--outbox", outboxDir, "--lock", lockPath, "--exec", scriptPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("binary relay notify failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), `"delivered": 2`) {
		t.Fatalf("unexpected output: %s", string(out))
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"transition":"alert"`) || !strings.Contains(lines[1], `"transition":"recovered"`) {
		t.Fatalf("unexpected delivered lines:\n%s", string(raw))
	}
}
