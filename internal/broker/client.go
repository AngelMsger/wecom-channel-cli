package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

func urlQuery(r *http.Request) (url.Values, error) { return url.ParseQuery(r.URL.RawQuery) }

// Call never sends credentials or performs arbitrary network requests.
func Call(ctx context.Context, socket string, req Request) (Response, error) {
	if err := ValidateSocket(socket); err != nil {
		return Response{}, err
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
	}}
	defer tr.CloseIdleConnections()
	c := &http.Client{Transport: tr, Timeout: 65 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	body, err := json.Marshal(req)
	if err != nil {
		return Response{}, e.New(e.CategoryUsage, "BAD_REQUEST", "Cannot encode broker request.")
	}
	r, err := http.NewRequestWithContext(ctx, "POST", "http://broker/v1/request", bytes.NewReader(body))
	if err != nil {
		return Response{}, e.New(e.CategoryInternal, "BAD_REQUEST", "Cannot construct broker request.")
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(r)
	if err != nil {
		return Response{}, e.New(e.CategoryNetwork, "BROKER_UNREACHABLE", "The broker did not return a response. For a proposal, reuse the same key; for approval, inspect proposal get before any further action.")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
	if err != nil || len(data) > 4<<20 {
		return Response{}, e.New(e.CategoryParse, "BROKER_RESPONSE", "The broker response was incomplete or too large.")
	}
	if resp.StatusCode != 200 {
		var p e.Payload
		if safejson.Decode(data, &p, true) != nil || p.Error.Code == "" {
			return Response{}, e.New(e.CategoryServer, "BROKER_REJECTED", "The broker rejected the request.")
		}
		x := e.New(p.Error.Category, p.Error.Code, p.Error.Message).WithHint(p.Error.Hint).WithNextSteps(p.Error.NextSteps...).WithDetails(p.Error.Details)
		x.Retryable = p.Error.Retryable
		return Response{}, x
	}
	var out Response
	if safejson.Decode(data, &out, true) != nil {
		return Response{}, e.New(e.CategoryParse, "BROKER_RESPONSE", "The broker response was invalid.")
	}
	count := 0
	if out.Item != nil {
		count++
	}
	if out.Page != nil {
		count++
	}
	if out.Scan != nil {
		count++
	}
	if count != 1 || (req.Operation == "event.scan") != (out.Scan != nil) {
		return Response{}, e.New(e.CategoryParse, "BROKER_RESPONSE", "The broker response was invalid.")
	}
	return out, nil
}

func ValidateSocket(socket string) error {
	if !filepath.IsAbs(socket) || len(socket) > 100 {
		return e.New(e.CategoryConfig, "SOCKET_REQUIRED", "Supply an absolute broker socket path using --socket or WECOM_CHANNEL_SOCKET.")
	}
	return nil
}
