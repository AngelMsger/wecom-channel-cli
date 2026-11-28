//go:build !linux

package monitor

// MonotonicSeconds returns zero on non-Linux hosts where systemd is unavailable.
func MonotonicSeconds() (float64, error) {
	return 0, nil
}
