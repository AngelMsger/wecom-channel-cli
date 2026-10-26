package broker

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

// Handler binds authority to the listener, never a caller-supplied field.
func (b *Broker) Handler(admin bool) http.Handler {
	sem := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/request" || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		key := "agent"
		if admin {
			key = "operator"
		}
		if !b.rateAllowed(key) {
			writeError(w, e.New(e.CategoryRateLimit, "RATE_LIMITED", "The local request budget is exhausted."))
			return
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			writeError(w, e.New(e.CategoryRateLimit, "BUSY", "The broker is at its concurrency limit."))
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		var req Request
		if err != nil || safejson.Decode(data, &req, true) != nil {
			writeError(w, e.New(e.CategoryUsage, "BAD_REQUEST", "Invalid, oversized, ambiguous or unsupported request fields."))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		out, err := b.Handle(ctx, req, admin)
		if err != nil {
			writeError(w, e.Safe(err))
			return
		}
		if out.Item != nil {
			out.Item.Source.Context = b.Config.Name
		}
		if out.Page != nil {
			for i := range out.Page.Items {
				out.Page.Items[i].Source.Context = b.Config.Name
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(out)
	})
}

func writeError(w http.ResponseWriter, err *e.CLIError) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	code := http.StatusBadRequest
	switch err.Category {
	case e.CategoryPermission:
		code = 403
	case e.CategoryNotFound:
		code = 404
	case e.CategoryConflict:
		code = 409
	case e.CategoryRateLimit:
		code = 429
	case e.CategoryInternal, e.CategoryConfig:
		code = 500
	case e.CategoryNetwork, e.CategoryServer:
		code = 502
	case e.CategoryAuth:
		code = 401
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(err.Payload())
}

func (b *Broker) CallbackHandler() http.Handler { return b.callbackRouter(nil) }

func (b *Broker) callbackRouter(routes map[string]*Broker) http.Handler {
	sem := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != "/callback" || b.Verifier == nil || len(r.URL.RawQuery) > 8192 {
			http.NotFound(w, r)
			return
		}
		if !b.rateAllowed("callback") {
			w.WriteHeader(429)
			return
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			w.WriteHeader(429)
			return
		}
		q, err := netQuery(r)
		if err != nil {
			w.WriteHeader(403)
			return
		}
		encrypted := q["echostr"]
		switch r.Method {
		case "GET":
			if encrypted == "" {
				w.WriteHeader(403)
				return
			}
		case "POST":
			if encrypted != "" {
				w.WriteHeader(403)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
			defer r.Body.Close()
			body, err := io.ReadAll(r.Body)
			var v struct {
				Encrypt string `json:"encrypt"`
			}
			if err != nil || safejson.Decode(body, &v, true) != nil {
				w.WriteHeader(403)
				return
			}
			encrypted = v.Encrypt
		default:
			w.WriteHeader(405)
			return
		}
		plain, err := b.Verifier.Open(q["msg_signature"], q["timestamp"], q["nonce"], encrypted, b.Now())
		if err != nil {
			w.WriteHeader(403)
			return
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(plain)
			return
		}
		v, err := callback.Parse(plain, b.Config.Bot, b.Now())
		if err != nil {
			w.WriteHeader(403)
			return
		}
		target := b
		if routes != nil {
			target = routes[v.Channel]
			if target == nil {
				w.WriteHeader(200)
				return
			}
		}
		if !target.Config.AllowsEvent(v.Channel, v.Sender, v.Kind, v.SenderKind) {
			w.WriteHeader(200)
			return
		}
		if target.Config.EffectiveWatchBackend() == "poll" && v.ObservationOnly {
			w.WriteHeader(200)
			return // One observation backend prevents duplicate callback/poll changes.
		}
		if err = target.Store.AddEvent(v, target.Now(), time.Duration(target.Config.RetentionHours)*time.Hour, target.Config.MaxEvents); err != nil {
			w.WriteHeader(503)
			return
		}
		// The callback only persists facts. Content retrieval and any reply are
		// separate bounded operations, so a webhook never runs an agent or tool.
		w.WriteHeader(200)
	})
}

func netQuery(r *http.Request) (map[string]string, error) {
	values, err := urlQuery(r)
	if err != nil {
		return nil, callback.ErrInvalid
	}
	q := map[string]string{}
	for k, v := range values {
		if len(v) != 1 {
			return nil, callback.ErrInvalid
		}
		switch k {
		case "msg_signature", "timestamp", "nonce", "echostr":
			q[k] = v[0]
		default:
			return nil, callback.ErrInvalid
		}
	}
	for _, k := range []string{"msg_signature", "timestamp", "nonce"} {
		if q[k] == "" {
			return nil, callback.ErrInvalid
		}
	}
	return q, nil
}

func unixListener(path string, private bool) (net.Listener, error) {
	if err := state.CheckDir(filepath.Dir(path), private); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return nil, e.New(e.CategoryConflict, "SOCKET_EXISTS", "A socket path already exists; inspect the previous broker before removing a stale socket.")
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, e.New(e.CategoryConfig, "SOCKET_BIND", "Cannot bind the configured Unix socket.")
	}
	mode := os.FileMode(0660)
	if private {
		mode = 0600
	}
	if err = os.Chmod(path, mode); err != nil {
		l.Close()
		return nil, e.New(e.CategoryConfig, "SOCKET_PERMISSIONS", "Cannot secure the Unix socket.")
	}
	return l, nil
}

func server(h http.Handler) *http.Server {
	return &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8192, ErrorLog: log.New(io.Discard, "", 0)}
}

// Serve requires pre-provisioned socket directories. The process never creates
// accounts, changes groups, opens a public port, or modifies bot connection mode.
func (b *Broker) Serve(ctx context.Context, ready func()) error {
	return ServeContexts(ctx, []*Broker{b}, b.Config.CallbackListen, ready)
}

// ServeContexts binds every context before reporting readiness and shares one
// authenticated callback router. No agent request chooses another context.
func ServeContexts(ctx context.Context, brokers []*Broker, callbackListen string, ready func()) error {
	if len(brokers) == 0 {
		return e.New(e.CategoryConfig, "NO_CONTEXT", "No broker contexts configured.")
	}
	var listeners []net.Listener
	var servers []*http.Server
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	routes := map[string]*Broker{}
	for _, b := range brokers {
		agent, err := unixListener(b.Config.AgentSocket, false)
		if err != nil {
			return err
		}
		listeners = append(listeners, agent)
		servers = append(servers, server(b.Handler(false)))
		admin, err := unixListener(b.Config.AdminSocket, true)
		if err != nil {
			return err
		}
		listeners = append(listeners, admin)
		servers = append(servers, server(b.Handler(true)))
		for _, ch := range b.Config.Channels {
			routes[ch.ID] = b
		}
	}
	if callbackListen != "" {
		l, err := net.Listen("tcp", callbackListen)
		if err != nil {
			return e.New(e.CategoryConfig, "CALLBACK_BIND", "Cannot bind the callback listener.")
		}
		listeners = append(listeners, l)
		servers = append(servers, server(brokers[0].callbackRouter(routes)))
	}
	var err error
	errs := make(chan error, len(servers))
	for i, s := range servers {
		go func(s *http.Server, l net.Listener) { errs <- s.Serve(l) }(s, listeners[i])
	}
	if ready != nil {
		ready()
	}
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	for _, s := range servers {
		if s.Shutdown(shutdown) != nil {
			_ = s.Close()
		}
	}
	if err != nil && err != http.ErrServerClosed {
		return e.New(e.CategoryNetwork, "BROKER_STOPPED", "A broker listener stopped unexpectedly.")
	}
	return nil
}
