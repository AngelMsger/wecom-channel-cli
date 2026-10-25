package callback

import (
	"strings"
	"testing"
	"time"
)

// This known-answer vector is published in WeCom document 101778; unlike a
// round-trip test, it checks interoperability with an independent producer.
func TestOfficialVector(t *testing.T) {
	v, err := New("QDG6eK", "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C", "wx5823bf96d3bd56c7")
	if err != nil {
		t.Fatal(err)
	}
	encrypted := "RypEvHKD8QQKFhvQ6QleEB4J58tiPdvo+rtK1I9qca6aM/wvqnLSV5zEPeusUiX5L5X/0lWfrf0QADHHhGd3QczcdCUpj911L3vg3W/sYYvuJTs3TUUkSUXxaccAS0qhxchrRYt66wiSpGLYL42aM6A8dTT+6k4aSknmPj48kzJs8qLjvd4Xgpue06DOdnLxAUHzM6+kDZ+HMZfJYuR+LtwGc2hgf5gsijff0ekUNXZiqATP7PF5mZxZ3Izoun1s4zG4LUMnvw2r+KqCKIw+3IQH03v+BCA9nMELNqbSf6tiWSrXJB3LAVGUcallcrw8V2t9EL4EhzJWrQUax5wLVMNS0+rUPA3k22Ncx4XXZS9o0MBH27Bo6BpNelZpS+/uh9KsNlY6bHCmJU9p8g7m3fVKn28H3KDYA5Pl/T8Z1ptDAVe0lXdQ2YoyyH2uyPIGHBZZIs2pDBS8R07+qN+E7Q=="
	sig := "477715d11cdb4164915debcba66cb864d751f3e6"
	now := time.Unix(1409659813, 0)
	plain, err := v.Open(sig, "1409659813", "1372623149", encrypted, now)
	if err != nil || !strings.Contains(string(plain), "hello") {
		t.Fatalf("vector failed: %v", err)
	}
	for _, tc := range []struct {
		sig, stamp, nonce, body string
		now                     time.Time
	}{
		{"0000000000000000000000000000000000000000", "1409659813", "1372623149", encrypted, now},
		{sig, "1409659813", "different", encrypted, now},
		{sig, "1409659813", "1372623149", encrypted, now.Add(301 * time.Second)},
		{sig, "1409659813", "1372623149", encrypted, now.Add(-301 * time.Second)},
		{sig, "01409659813", "1372623149", encrypted, now},
		{sig, "1409659813", "1372623149", encrypted[:len(encrypted)-4], now},
	} {
		if _, err := v.Open(tc.sig, tc.stamp, tc.nonce, tc.body, tc.now); err != ErrInvalid {
			t.Fatal("accepted invalid callback")
		}
	}
	v.receiver = ""
	if _, err := v.Open(sig, "1409659813", "1372623149", encrypted, now); err != ErrInvalid {
		t.Fatal("accepted wrong receiver")
	}
}

func FuzzOpen(f *testing.F) {
	f.Add("abc", "123", "nonce", "abc")
	v, _ := New("token", "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C", "")
	f.Fuzz(func(t *testing.T, sig, stamp, nonce, body string) {
		_, _ = v.Open(sig, stamp, nonce, body, time.Unix(123, 0))
	})
}
