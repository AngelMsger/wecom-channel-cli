//go:build linux

package monitor

import "golang.org/x/sys/unix"

// MonotonicSeconds returns the host CLOCK_MONOTONIC timestamp in seconds,
// matching systemd's ActiveEnterTimestampMonotonic clock.
func MonotonicSeconds() (float64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return 0, err
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9, nil
}
