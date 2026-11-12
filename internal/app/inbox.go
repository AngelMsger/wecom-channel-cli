package app

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/inbox"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) inboxCommands() *cobra.Command {
	g := &cobra.Command{Use: "inbox", Short: "Inspect durable receipt and explicitly track host processing; never invokes a Bot"}
	for _, verb := range []string{"status", "list", "get", "claim", "resolve"} {
		var path, id, status, cursor, attempt, evidence string
		var limit int
		var dry bool
		c := &cobra.Command{Use: verb, Short: map[string]string{
			"status":  "Read receipt counts and distinct host-processing states",
			"list":    "List received batches and processing states with bounded pagination",
			"get":     "Read a batch, its deduplicated work and host-processing evidence",
			"claim":   "Persist a dispatch attempt before a trusted adapter contacts its host",
			"resolve": "Record a verified host outcome; uncertain attempts are never retried automatically",
		}[verb], Args: cobra.NoArgs}
		c.RunE = func(cmd *cobra.Command, _ []string) error {
			write := verb == "claim" || verb == "resolve"
			if path == "" || ((verb == "get" || write) && id == "") {
				return e.New(e.CategoryUsage, "INBOX_FLAGS", "Supply --db and, for get/claim/resolve, --id from inbox list.")
			}
			var after int64
			if verb == "list" {
				if cursor != "" {
					var err error
					after, err = strconv.ParseInt(cursor, 10, 64)
					if err != nil || after < 0 {
						return e.New(e.CategoryUsage, "INBOX_CURSOR", "Reuse next as --cursor with the same database and status filter.")
					}
				}
				if limit < 1 || limit > 100 {
					return e.New(e.CategoryUsage, "BAD_LIMIT", "Use --limit 1–100.")
				}
			}
			if write && !dry {
				if err := s.guard("inbox " + verb); err != nil {
					return err
				}
			}
			db, err := inbox.Open(path, !write || dry)
			if err != nil {
				return inboxOpenError(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), s.timeout)
			defer cancel()
			switch verb {
			case "status":
				v, err := db.Status(ctx)
				if err != nil {
					return err
				}
				return s.emit(cmd, v)
			case "list":
				items, more, err := db.List(ctx, status, after, limit)
				if err != nil {
					return err
				}
				var next string
				if more {
					next = strconv.FormatInt(items[len(items)-1].Sequence, 10)
				}
				return output.EmitList(items, next, more, output.Options{Format: s.format, Writer: cmd.OutOrStdout(), NoticeWriter: cmd.ErrOrStderr()})
			case "get":
				v, err := db.Get(ctx, id)
				if err != nil {
					return err
				}
				return s.emit(cmd, v)
			default:
				var v inbox.Item
				if verb == "claim" {
					v, err = db.Claim(ctx, id, dry)
				} else {
					v, err = db.Resolve(ctx, id, attempt, status, evidence, dry)
				}
				if err != nil {
					return err
				}
				if dry {
					return s.emit(cmd, map[string]any{"dry_run": true, "planned": v})
				}
				return s.emit(cmd, v)
			}
		}
		c.Flags().StringVar(&path, "db", "", "Private SQLite inbox (default relay destination is STATE.inbox.sqlite)")
		if verb == "list" {
			c.Flags().StringVar(&status, "status", "", "Filter received, dispatching, submitted, completed, uncertain or duplicate")
			c.Flags().StringVar(&cursor, "cursor", "", "Continuation from the same inbox list query")
			c.Flags().IntVar(&limit, "limit", 50, "Maximum batches, 1–100")
		}
		if verb == "get" || verb == "claim" || verb == "resolve" {
			c.Flags().StringVar(&id, "id", "", "Batch ID from inbox list")
		}
		if verb == "claim" || verb == "resolve" {
			c.Flags().BoolVar(&dry, "dry-run", false, "Preview the transition without writing or contacting a host")
		}
		if verb == "resolve" {
			c.Flags().StringVar(&attempt, "attempt", "", "Exact attempt_id from claim/get; prevents stale adapters changing newer attempts")
			c.Flags().StringVar(&status, "status", "", "Verified submitted/completed, uncertain, or verified not_submitted to permit a new claim")
			c.Flags().StringVar(&evidence, "evidence", "", "Non-secret host receipt or reconciliation reference; never a token or URL")
		}
		g.AddCommand(c)
	}
	g.AddCommand(s.inboxServeCommand())
	return g
}

func inboxOpenError(err error) error {
	if os.IsNotExist(err) {
		return e.New(e.CategoryNotFound, "INBOX_MISSING", "No inbox exists at this path. Start relay run or inbox serve first; status never creates storage.")
	}
	return err
}

func (s *appState) inboxServeCommand() *cobra.Command {
	var path, listen, tokenEnv string
	var dry bool
	c := &cobra.Command{Use: "serve", Short: "Serve the relay acknowledgment protocol on loopback with a durable SQLite inbox", Args: cobra.NoArgs}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		host, port, err := net.SplitHostPort(listen)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || !net.ParseIP(host).IsLoopback() || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(tokenEnv) || path == "" {
			return e.New(e.CategoryUsage, "INBOX_LISTEN", "Supply --db, a literal loopback --listen IP:port and the name of a protected bearer-token environment variable.")
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return e.New(e.CategoryUsage, "INBOX_PATH", "Supply a local inbox database path.")
		}
		if err = securefs.Directory(filepath.Dir(abs), true, false); err != nil {
			return err
		}
		if dry {
			if _, err = os.Lstat(abs); err == nil {
				db, err := inbox.Open(abs, true)
				if err != nil {
					return err
				}
				db.Close()
			} else if !os.IsNotExist(err) {
				return inboxOpenError(err)
			}
			return s.emit(cmd, map[string]any{"dry_run": true, "delivery": "sqlite_inbox", "listen": listen, "path": "/batches"})
		}
		if err = s.guard("inbox serve"); err != nil {
			return err
		}
		// Validate the secret before creating state; it is never printed or logged.
		token := os.Getenv(tokenEnv)
		if _, err = inbox.Handler(nil, token); err != nil {
			return err
		}
		db, err := inbox.Open(abs, false)
		if err != nil {
			return err
		}
		defer db.Close()
		handler, err := inbox.Handler(db, token)
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			return e.New(e.CategoryNetwork, "INBOX_LISTEN", "Cannot bind the configured loopback receiver. Check its supervisor and port.")
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384, ErrorLog: log.New(io.Discard, "", 0)}
		done := make(chan error, 1)
		go func() { done <- server.Serve(ln) }()
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"inbox": "listening", "listen": listen}})
		select {
		case err = <-done:
		case <-cmd.Context().Done():
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if server.Shutdown(ctx) != nil {
				_ = server.Close()
			}
			err = <-done
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return e.New(e.CategoryNetwork, "INBOX_SERVER", "The inbox listener stopped unexpectedly; restart with the same database.")
		}
		return nil
	}
	c.Flags().StringVar(&path, "db", "", "SQLite file in an existing private service-owned directory")
	c.Flags().StringVar(&listen, "listen", "127.0.0.1:8789", "Literal loopback address; POST /batches accepts relay deliveries")
	c.Flags().StringVar(&tokenEnv, "token-env", "WECOM_CHANNEL_INBOX_TOKEN", "Protected environment variable holding the required receiver bearer token")
	c.Flags().BoolVar(&dry, "dry-run", false, "Validate paths and listener configuration without token access, writes or binding")
	return c
}
