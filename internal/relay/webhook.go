package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"github.com/angelmsger/wecom-channel-cli/pkg/transport"
)

type Webhook struct {
	config Config
	token  string
	client *transport.Client
}

func NewWebhook(c Config) (*Webhook, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	token := ""
	if c.TokenEnv != "" {
		token = os.Getenv(c.TokenEnv)
		if token == "" || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
			return nil, e.New(e.CategoryConfig, "RELAY_TOKEN", "Supply the relay's configured bearer token through its protected service environment.")
		}
	}
	return &Webhook{config: c, token: token, client: transport.New()}, nil
}

// Send performs exactly one attempt. The durable runner owns retry scheduling;
// the shared transport never retries this POST or follows a redirect.
func (h *Webhook) Send(ctx context.Context, batch Batch) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return stateError()
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(h.config.TimeoutSeconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.config.URL, bytes.NewReader(body))
	if err != nil {
		return configError()
	}
	// net/http treats Idempotency-Key as permission to replay a POST on a
	// reused connection. This host does not promise deduplication. Disable body
	// rewind so only the persisted relay state machine can authorize a replay.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", batch.ID)
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.client.Do(ctx, req)
	if err != nil {
		if h.config.Mode() == HTTPAccepted {
			return UnknownOutcome()
		}
		return e.New(e.CategoryNetwork, "RELAY_UNREACHABLE", "The receiver did not return a reliable acknowledgment; the immutable batch remains queued.")
	}
	defer resp.Body.Close()
	if h.config.Mode() == HTTPAccepted && resp.StatusCode == http.StatusOK {
		// The documented host contract establishes acceptance from the status
		// alone. Never wait for or interpret a body as processing completion.
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || h.config.Mode() == HTTPAccepted {
		x := e.New(e.CategoryServer, "RELAY_REJECTED", "The receiver rejected the batch; it remains queued.").WithHTTPStatus(resp.StatusCode)
		x.Retryable = Retryable(x)
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			x.Code = "RELAY_REDIRECT"
			x.Retryable = false
			return x.WithHint("Redirects are refused. Have the operator verify the configured webhook destination; no redirect target is followed or exposed.")
		}
		return x
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	var ack struct {
		Ack string `json:"ack"`
	}
	if err != nil {
		return e.New(e.CategoryNetwork, "RELAY_UNREACHABLE", "The acknowledgment was interrupted; the immutable batch remains queued.")
	}
	if len(b) > 4096 || safejson.Decode(b, &ack, true) != nil || ack.Ack != batch.ID {
		return e.New(e.CategoryParse, "RELAY_ACK", "The receiver must return a JSON ack matching the batch ID after successful processing or durable handoff. The batch remains queued.")
	}
	return nil
}

func UnknownOutcome() error {
	return e.New(e.CategoryConflict, "RELAY_OUTCOME_UNKNOWN", "The host may already have started a run. Automatic replay is stopped; inspect host history and use relay resolve with verified evidence before retrying.")
}

// Retryable is deliberately narrower than the generic error categories: queue
// capacity, permissions, bad protocols, gaps, and policy changes require repair.
func Retryable(err error) bool {
	x, ok := err.(*e.CLIError)
	if !ok {
		return false
	}
	switch x.Code {
	case "BROKER_UNREACHABLE", "REQUEST_UNCERTAIN", "RELAY_UNREACHABLE", "POLL_RATE_LIMIT", "RATE_LIMITED":
		return true
	case "HTTP_REJECTED", "RELAY_REJECTED":
		return x.HTTPStatus == 408 || x.HTTPStatus == 429 || x.HTTPStatus >= 500 && x.HTTPStatus <= 599
	case "WECOM_REJECTED":
		return x.Category == e.CategoryRateLimit
	}
	return false
}

func RetryDelay(c Config, failures int) time.Duration {
	return min(time.Duration(c.RetryBaseSeconds)*time.Second*time.Duration(1<<min(max(failures-1, 0), 10)), 5*time.Minute)
}
