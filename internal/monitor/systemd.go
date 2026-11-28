// Package monitor implements read-only relay listener health assessment,
// edge-triggered incident transitions, and FIFO alert outbox storage.
package monitor

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

const systemctlPath = "/usr/bin/systemctl"

var validUnitName = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.@-]{0,127}$`)
var validInstanceName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// UnitStatus holds the sanitized systemd properties needed to assess one
// expected relay service unit.
type UnitStatus struct {
	LoadState                  string
	ActiveState                string
	SubState                   string
	ActiveEnterMonotonicMicros int64
	NRestarts                  int
}

// ValidateInstanceName verifies a listener instance identifier before it is
// interpolated into unit or filesystem paths.
func ValidateInstanceName(name string) bool {
	return validInstanceName.MatchString(name)
}

// ValidateUnitName verifies a systemd unit name before invoking systemctl.
func ValidateUnitName(name string) bool {
	return validUnitName.MatchString(name) && !strings.Contains(name, "..")
}

// ParseSystemdShow parses key=value output from `systemctl show`.
func ParseSystemdShow(raw []byte) (UnitStatus, error) {
	fields := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || k == "" {
			return UnitStatus{}, e.New(e.CategoryParse, "PROBE_FAILED", "Could not parse systemd unit properties.")
		}
		fields[k] = v
	}
	loadState := fields["LoadState"]
	if loadState == "" {
		return UnitStatus{}, e.New(e.CategoryParse, "PROBE_FAILED", "Systemd unit output is missing LoadState.")
	}
	var enterMicros int64
	if s := fields["ActiveEnterTimestampMonotonic"]; s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			return UnitStatus{}, e.New(e.CategoryParse, "PROBE_FAILED", "Systemd unit activation timestamp is invalid.")
		}
		enterMicros = n
	}
	var restarts int
	if s := fields["NRestarts"]; s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return UnitStatus{}, e.New(e.CategoryParse, "PROBE_FAILED", "Systemd unit restart count is invalid.")
		}
		restarts = n
	}
	return UnitStatus{
		LoadState:                  loadState,
		ActiveState:                fields["ActiveState"],
		SubState:                   fields["SubState"],
		ActiveEnterMonotonicMicros: enterMicros,
		NRestarts:                  restarts,
	}, nil
}

// QuerySystemdUnit inspects one systemd unit with a bounded timeout and
// stripped environment.
func QuerySystemdUnit(ctx context.Context, unit string) (UnitStatus, error) {
	if !ValidateUnitName(unit) {
		return UnitStatus{}, e.New(e.CategoryUsage, "BAD_UNIT", "Invalid systemd unit name.")
	}
	childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(childCtx, systemctlPath, "show", unit,
		"--property=LoadState,ActiveState,SubState,ActiveEnterTimestampMonotonic,NRestarts")
	cmd.Stdin = nil
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C"}
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return UnitStatus{}, e.New(e.CategoryInternal, "PROBE_FAILED", "Failed to query systemd unit state.")
	}
	return ParseSystemdShow(out.Bytes())
}
