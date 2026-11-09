package broker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func TestCallPreservesHTTPFailureClassification(t *testing.T) {
	dir, err := os.MkdirTemp("/private/tmp", "wce-")
	if err != nil {
		dir, err = os.MkdirTemp("/tmp", "wce-")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, _ = filepath.EvalSymlinks(dir)
	socket := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(e.New(e.CategoryServer, "HTTP_REJECTED", "upstream rejected").WithHTTPStatus(503).Payload())
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { _ = server.Close(); <-done }()
	_, err = Call(context.Background(), socket, Request{Operation: "event.scan", Scan: &ScanRequest{WholeChannel: true, Limit: 1}})
	if err == nil || e.Safe(err).HTTPStatus != 503 || e.Safe(err).Code != "HTTP_REJECTED" {
		t.Fatal("lost retry classification at socket boundary", err)
	}
}
