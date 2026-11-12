package inbox

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

// Handler adapts the same durable receipt transaction used by the local sink to
// the existing relay protocol. It has no host dispatch or health wakeup path.
func Handler(store Store, token string) (http.Handler, error) {
	if token == "" || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
		return nil, e.New(e.CategoryConfig, "INBOX_TOKEN", "Supply the inbox bearer token through its protected service environment.")
	}
	active := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fail := func(status int, code string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
		}
		if len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			fail(http.StatusUnauthorized, "INBOX_AUTH")
			return
		}
		select {
		case active <- struct{}{}:
			defer func() { <-active }()
		default:
			fail(http.StatusTooManyRequests, "INBOX_BUSY")
			return
		}
		if r.URL.Path != "/batches" || r.URL.RawQuery != "" {
			fail(http.StatusNotFound, "INBOX_ROUTE")
			return
		}
		if r.Method != http.MethodPost {
			fail(http.StatusMethodNotAllowed, "INBOX_METHOD")
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			fail(http.StatusUnsupportedMediaType, "INBOX_JSON")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBatchBytes))
		var b relay.Batch
		if err != nil {
			fail(http.StatusRequestEntityTooLarge, "INBOX_SIZE")
			return
		}
		if safejson.Decode(raw, &b, true) != nil || b.Validate() != nil || len(r.Header.Values("Idempotency-Key")) != 1 || r.Header.Get("Idempotency-Key") != b.ID {
			fail(http.StatusBadRequest, "INBOX_BATCH")
			return
		}
		if err = store.Accept(r.Context(), b); err != nil {
			status := http.StatusServiceUnavailable
			if e.Safe(err).Category == e.CategoryConflict {
				status = http.StatusConflict
			}
			fail(status, e.Safe(err).Code)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"ack": b.ID})
	}), nil
}
