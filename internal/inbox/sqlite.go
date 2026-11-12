package inbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	_ "modernc.org/sqlite"
)

const applicationID = 1464027470
const maxBatchBytes = 1 << 20
const maxPendingEvents = 10000

const schema = `
CREATE TABLE identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL);
CREATE TABLE batches (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
 digest TEXT NOT NULL, original BLOB NOT NULL, work BLOB NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('received','dispatching','submitted','completed','uncertain','duplicate')),
 received_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 new_events INTEGER NOT NULL, attempt_id TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT ''
);
CREATE TABLE events (id TEXT PRIMARY KEY, digest TEXT NOT NULL, batch_id TEXT NOT NULL REFERENCES batches(id));
CREATE INDEX batches_status_sequence ON batches(status, sequence);
CREATE TABLE transitions (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL REFERENCES batches(id),
 attempt_id TEXT NOT NULL, status TEXT NOT NULL, evidence TEXT NOT NULL, recorded_at INTEGER NOT NULL
);
PRAGMA application_id=1464027470;
PRAGMA user_version=1;
`

type SQLite struct{ db *sql.DB }

var _ Store = (*SQLite)(nil)

// Open uses a private local filesystem. Read-only opens neither create a file
// nor migrate it. DELETE + EXTRA syncs the rollback journal and its directory;
// every acknowledgment follows Commit, including duplicate deliveries.
func Open(path string, readOnly bool) (*SQLite, error) {
	abs, err := filepath.Abs(path)
	if err != nil || path == "" {
		return nil, storageError()
	}
	if err = securefs.Database(abs, !readOnly); err != nil {
		return nil, err
	}
	q := url.Values{"_pragma": {"busy_timeout(5000)", "foreign_keys(ON)", "trusted_schema(OFF)"}}
	if readOnly {
		q.Set("mode", "ro")
		q.Add("_pragma", "query_only(ON)")
	} else {
		q.Set("mode", "rw")
		q.Set("_txlock", "immediate")
		q.Add("_pragma", "synchronous(EXTRA)")
	}
	u := url.URL{Scheme: "file", Path: abs, RawQuery: q.Encode()}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, storageError()
	}
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db}
	if err = s.initialize(readOnly); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) initialize(readOnly bool) error {
	var appID, version int
	if s.db.QueryRow("PRAGMA application_id").Scan(&appID) != nil || s.db.QueryRow("PRAGMA user_version").Scan(&version) != nil {
		return storageError()
	}
	if appID == 0 && version == 0 && !readOnly {
		tx, err := s.db.Begin()
		if err != nil {
			return storageError()
		}
		defer tx.Rollback()
		// Another process may have initialized while we waited for the write lock.
		if tx.QueryRow("PRAGMA application_id").Scan(&appID) != nil || tx.QueryRow("PRAGMA user_version").Scan(&version) != nil {
			return storageError()
		}
		if appID == 0 && version == 0 {
			var tables int
			if tx.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&tables) != nil || tables != 0 {
				return storageError()
			}
			if _, err = tx.Exec(schema); err != nil {
				return storageError()
			}
			appID, version = applicationID, 1
		}
		if err = tx.Commit(); err != nil {
			return storageError()
		}
	}
	if appID != applicationID || version != 1 {
		return storageError()
	}
	var journal string
	if s.db.QueryRow("PRAGMA journal_mode").Scan(&journal) != nil || journal != "delete" {
		return storageError()
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Accept(ctx context.Context, b relay.Batch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(b)
	if err != nil || len(raw) > maxBatchBytes {
		return storageError()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storageError()
	}
	defer tx.Rollback()
	identity, _ := json.Marshal(b.Identity)
	var savedIdentity string
	err = tx.QueryRowContext(ctx, "SELECT value FROM identity WHERE singleton=1").Scan(&savedIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO identity VALUES (1, ?)", string(identity)); err != nil {
			return storageError()
		}
	} else if err != nil {
		return storageError()
	} else if savedIdentity != string(identity) {
		return e.New(e.CategoryConflict, "INBOX_BINDING_CHANGED", "This inbox belongs to another context, bot, channel or ledger. Use its original identity; provision a separate inbox for another channel.")
	}
	var digest string
	err = tx.QueryRowContext(ctx, "SELECT digest FROM batches WHERE id=?", b.ID).Scan(&digest)
	if err == nil {
		if digest != apiclient.Digest(b) {
			return conflict()
		}
		if tx.Commit() != nil {
			return storageError()
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storageError()
	}
	work := b
	work.Threads = nil
	type eventKey struct{ id, digest string }
	var fresh []eventKey
	for _, thread := range b.Threads {
		out := relay.Thread{Post: thread.Post}
		for _, r := range thread.Events {
			key := eventKey{r.Data.ID, apiclient.Digest(r)}
			err = tx.QueryRowContext(ctx, "SELECT digest FROM events WHERE id=?", key.id).Scan(&digest)
			if err == nil {
				if digest != key.digest {
					return conflict()
				}
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return storageError()
			}
			fresh = append(fresh, key)
			out.Events = append(out.Events, r)
		}
		if len(out.Events) > 0 {
			work.Threads = append(work.Threads, out)
		}
	}
	workRaw, _ := json.Marshal(work)
	if len(fresh) > 0 {
		var pending int
		if tx.QueryRowContext(ctx, "SELECT coalesce(sum(new_events),0) FROM batches WHERE status IN ('received','dispatching','submitted','uncertain')").Scan(&pending) != nil {
			return storageError()
		}
		if pending+len(fresh) > maxPendingEvents {
			return e.New(e.CategoryRateLimit, "INBOX_FULL", "The inbox has 10000 unfinished events. Drain or reconcile existing work before retrying; no receipt or event keys were committed.")
		}
	}
	status := Received
	if len(fresh) == 0 {
		status = Duplicate
	}
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO batches(id,digest,original,work,status,received_at,updated_at,new_events) VALUES(?,?,?,?,?,?,?,?)`, b.ID, apiclient.Digest(b), raw, workRaw, status, now, now, len(fresh))
	if err != nil {
		return storageError()
	}
	for _, key := range fresh {
		if _, err = tx.ExecContext(ctx, "INSERT INTO events VALUES(?,?,?)", key.id, key.digest, b.ID); err != nil {
			return storageError()
		}
	}
	if tx.Commit() != nil {
		return storageError()
	}
	return nil
}

func (s *SQLite) Status(ctx context.Context) (Status, error) {
	v := Status{States: map[string]int64{Received: 0, Dispatching: 0, Submitted: 0, Completed: 0, Uncertain: 0, Duplicate: 0}}
	// Read the summary in one snapshot while another process receives work.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return v, storageError()
	}
	defer tx.Rollback()
	var identity []byte
	err = tx.QueryRowContext(ctx, "SELECT value FROM identity WHERE singleton=1").Scan(&identity)
	if err == nil {
		v.Identity = &broker.WatchIdentity{}
		if safejson.Decode(identity, v.Identity, true) != nil {
			return v, storageError()
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return v, storageError()
	}
	rows, err := tx.QueryContext(ctx, "SELECT status,count(*),sum(new_events),min(received_at) FROM batches GROUP BY status")
	if err != nil {
		return v, storageError()
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count, events, oldest int64
		if rows.Scan(&status, &count, &events, &oldest) != nil {
			return v, storageError()
		}
		v.States[status] = count
		v.ReceivedBatches += count
		v.ReceivedEvents += events
		if status == Received {
			v.OldestReceivedAt = oldest
		}
	}
	if rows.Err() != nil {
		return v, storageError()
	}
	return v, nil
}

const itemColumns = "sequence,id,status,received_at,updated_at,new_events,attempt_id,evidence"

type scanner interface{ Scan(...any) error }

func scanItem(row scanner, detail bool) (Item, error) {
	var v Item
	args := []any{&v.Sequence, &v.ID, &v.Status, &v.ReceivedAt, &v.UpdatedAt, &v.NewEvents, &v.Attempt, &v.Evidence}
	var original, work []byte
	if detail {
		args = append(args, &original, &work)
	}
	if err := row.Scan(args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return v, e.New(e.CategoryNotFound, "INBOX_MISSING", "No such inbox batch. Discover IDs with inbox list.")
		}
		return v, storageError()
	}
	if detail {
		v.Original, v.Batch = &relay.Batch{}, &relay.Batch{}
		if safejson.Decode(original, v.Original, true) != nil || safejson.Decode(work, v.Batch, true) != nil {
			return v, storageError()
		}
	}
	return v, nil
}

func (s *SQLite) Get(ctx context.Context, id string) (Item, error) {
	return scanItem(s.db.QueryRowContext(ctx, "SELECT "+itemColumns+",original,work FROM batches WHERE id=?", id), true)
}

func (s *SQLite) List(ctx context.Context, status string, after int64, limit int) ([]Item, bool, error) {
	if (status != "" && !validStatus(status)) || after < 0 || limit < 1 || limit > 100 {
		return nil, false, e.New(e.CategoryUsage, "INBOX_FILTER", "Use a known status, the returned --cursor and --limit 1–100.")
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+itemColumns+" FROM batches WHERE sequence>? AND (?='' OR status=?) ORDER BY sequence LIMIT ?", after, status, status, limit+1)
	if err != nil {
		return nil, false, storageError()
	}
	defer rows.Close()
	items := []Item{}
	for rows.Next() {
		v, err := scanItem(rows, false)
		if err != nil {
			return nil, false, err
		}
		items = append(items, v)
	}
	if rows.Err() != nil {
		return nil, false, storageError()
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

func (s *SQLite) Claim(ctx context.Context, id string, dry bool) (Item, error) {
	return s.transition(ctx, id, "", "claim", "", dry)
}

func (s *SQLite) Resolve(ctx context.Context, id, attempt, status, evidence string, dry bool) (Item, error) {
	if !apiclient.Identifier(evidence, 256) || attempt == "" || (status != Submitted && status != Completed && status != Uncertain && status != "not_submitted") {
		return Item{}, e.New(e.CategoryUsage, "INBOX_RESOLUTION", "Supply --attempt, a non-secret --evidence reference and --status submitted, completed, uncertain or not_submitted after verifying the host outcome.")
	}
	return s.transition(ctx, id, attempt, status, evidence, dry)
}

func (s *SQLite) transition(ctx context.Context, id, attempt, target, evidence string, dry bool) (Item, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Item{}, storageError()
	}
	defer tx.Rollback()
	v, err := scanItem(tx.QueryRowContext(ctx, "SELECT "+itemColumns+",original,work FROM batches WHERE id=?", id), true)
	if err != nil {
		return v, err
	}
	if target == "claim" {
		if v.Status != Received {
			return v, conflict()
		}
		v.Status = Dispatching
		if !dry {
			var token [16]byte
			if _, err = rand.Read(token[:]); err != nil {
				return v, storageError()
			}
			v.Attempt = hex.EncodeToString(token[:])
		}
		v.Evidence = ""
	} else {
		if v.Attempt != attempt {
			return v, conflict()
		}
		if target == v.Status && evidence == v.Evidence {
			return v, nil
		}
		if v.Status != Dispatching && v.Status != Uncertain && !(v.Status == Submitted && target == Completed) {
			return v, conflict()
		}
		v.Status, v.Evidence = target, evidence
		if target == "not_submitted" {
			v.Status = Received
		}
	}
	if dry {
		return v, nil
	}
	v.UpdatedAt = time.Now().UnixMilli()
	if _, err = tx.ExecContext(ctx, "UPDATE batches SET status=?,attempt_id=?,evidence=?,updated_at=? WHERE id=?", v.Status, v.Attempt, v.Evidence, v.UpdatedAt, id); err != nil {
		return v, storageError()
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO transitions(batch_id,attempt_id,status,evidence,recorded_at) VALUES(?,?,?,?,?)", id, v.Attempt, v.Status, v.Evidence, v.UpdatedAt); err != nil {
		return v, storageError()
	}
	if tx.Commit() != nil {
		return v, storageError()
	}
	return v, nil
}
