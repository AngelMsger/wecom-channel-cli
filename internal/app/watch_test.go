package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
)

func streamFixture() (watchCheckpoint, broker.ScanPage) {
	cp := watchCheckpoint{Version: 1, Socket: "/run/context/a.sock", Binding: apiclient.Digest("binding"), Scope: apiclient.Digest("scope"), Identity: broker.WatchIdentity{Context: "ctx", Bot: "bot", Channel: "channel", Ledger: strings.Repeat("a", 32)}}
	p := broker.ScanPage{Binding: cp.Binding, Identity: cp.Identity, After: 2, HighWatermark: 2}
	for _, seq := range []int64{1, 2} {
		r := apiclient.Untrusted(callback.Event{Sequence: seq, Bot: "bot", Channel: "channel", Post: "post", Kind: "topic_event", ObservationOnly: true}, "channel", "post", "event")
		r.Source.Context = "ctx"
		p.Items = append(p.Items, r)
	}
	return cp, p
}
func TestWatchDoesNotCheckpointFailedOutput(t *testing.T) {
	cp, p := streamFixture()
	emitted := 0
	var saved watchCheckpoint
	err := pumpWatch(context.Background(), broker.ScanRequest{Limit: 100}, cp, p, 0, 10, nil, func(apiclient.Record) error {
		emitted++
		if emitted == 2 {
			return errors.New("broken pipe")
		}
		return nil
	}, func(v watchCheckpoint) error { saved = v; return nil }, func(string, watchCheckpoint, int) {})
	if err == nil || saved.After != 1 {
		t.Fatal("failed output advanced checkpoint", saved, err)
	}
}
func TestWatchMaxEventsAndCancellation(t *testing.T) {
	cp, p := streamFixture()
	emitted := 0
	var saved watchCheckpoint
	reason := ""
	err := pumpWatch(context.Background(), broker.ScanRequest{Limit: 100}, cp, p, 0, 1, nil, func(apiclient.Record) error { emitted++; return nil }, func(v watchCheckpoint) error { saved = v; return nil }, func(s string, _ watchCheckpoint, _ int) { reason = s })
	if err != nil || emitted != 1 || saved.After != 1 || reason != "max_events" {
		t.Fatal(emitted, saved, reason, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = pumpWatch(ctx, broker.ScanRequest{Limit: 100}, cp, p, time.Hour, 10, nil, func(apiclient.Record) error { t.Fatal("emitted after cancellation"); return nil }, func(v watchCheckpoint) error { saved = v; return nil }, func(s string, _ watchCheckpoint, _ int) { reason = s })
	if err != nil || saved.After != 0 || reason != "stopped" {
		t.Fatal(saved, reason, err)
	}
}
func TestCheckpointOwnershipAndIdempotence(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	path := filepath.Join(dir, "watch.json")
	f, err := openWatchCheckpoint(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer f.close()
	if other, err := openWatchCheckpoint(path, true); err == nil {
		other.close()
		t.Fatal("concurrent checkpoint writer")
	}
	cp, _ := streamFixture()
	if err = f.write(cp); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("checkpoint permissions")
	}
	if err = f.write(cp); err != nil {
		t.Fatal(err)
	}
	next, _ := os.Stat(path)
	if !info.ModTime().Equal(next.ModTime()) {
		t.Fatal("idle checkpoint rewritten")
	}
	if got, exists, err := f.read(); err != nil || !exists || got != cp {
		t.Fatal(got, err)
	}
	otherPath := filepath.Join(dir, "human.json")
	_ = os.WriteFile(otherPath, []byte(`{"personal":"keep"}`), 0600)
	other, err := openWatchCheckpoint(otherPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if other.write(cp) == nil {
		t.Fatal("overwrote unrelated file")
	}
}
func TestWatchCLIValidation(t *testing.T) {
	for _, args := range [][]string{{"event", "watch"}, {"event", "watch", "--whole-channel", "--post", "p"}, {"event", "watch", "--post", "p", "--duration", "0s"}, {"event", "watch", "--whole-channel", "--interval", "0s"}, {"event", "watch", "--whole-channel", "--format", "json"}} {
		code, out, _ := run(t, args...)
		if code != 2 || out != "" {
			t.Fatal(args, code, out)
		}
	}
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	path := filepath.Join(dir, "resume.json")
	code, _, _ := run(t, "--config-dir", dir, "--socket", "relative.sock", "event", "watch", "--whole-channel", "--checkpoint", path)
	if code != 3 {
		t.Fatal("invalid socket accepted")
	}
	if _, err := os.Stat(path + ".lock"); !os.IsNotExist(err) {
		t.Fatal("invalid socket created checkpoint lock")
	}
}
