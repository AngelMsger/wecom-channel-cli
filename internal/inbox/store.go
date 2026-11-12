// Package inbox owns durable receipt and host-processing state, independently
// of the relay collection checkpoint. It never invokes a model or channel write.
package inbox

import (
	"context"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/relay"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

const (
	Received    = "received"
	Dispatching = "dispatching"
	Submitted   = "submitted"
	Completed   = "completed"
	Uncertain   = "uncertain"
	Duplicate   = "duplicate"
)

// Store is the receiver contract. Accept must atomically persist the batch,
// event deduplication keys and work before returning. Implementations must retain
// those keys across restart and must never reclaim ambiguous host submissions.
// External services can implement the same contract behind the relay webhook.
type Store interface {
	Accept(context.Context, relay.Batch) error
	Status(context.Context) (Status, error)
	List(context.Context, string, int64, int) ([]Item, bool, error)
	Get(context.Context, string) (Item, error)
	Claim(context.Context, string, bool) (Item, error)
	Resolve(context.Context, string, string, string, string, bool) (Item, error)
	Close() error
}

type Item struct {
	Sequence   int64  `json:"sequence"`
	ID         string `json:"id"`
	Status     string `json:"status"`
	ReceivedAt int64  `json:"received_at"`
	UpdatedAt  int64  `json:"updated_at"`
	NewEvents  int    `json:"new_events"`
	Attempt    string `json:"attempt_id,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
	// Batch contains only newly admitted events. Original retains the exact
	// decoded delivery for audit; a host must process Batch, never Original.
	Batch    *relay.Batch `json:"work,omitempty"`
	Original *relay.Batch `json:"original,omitempty"`
}

type Status struct {
	Identity         *broker.WatchIdentity `json:"identity"`
	ReceivedBatches  int64                 `json:"received_batches"`
	ReceivedEvents   int64                 `json:"received_events"`
	States           map[string]int64      `json:"work_states"`
	OldestReceivedAt int64                 `json:"oldest_received_at"`
}

func storageError() error {
	return e.New(e.CategoryConfig, "INBOX_STORAGE", "Inbox storage could not be read or committed. Preserve the database and relay state, repair storage, then retry the original batch.")
}

func conflict() error {
	return e.New(e.CategoryConflict, "INBOX_CONFLICT", "The batch identity, payload or processing state conflicts with durable inbox records. Inspect inbox get and reconcile with the host before retrying.")
}

func validStatus(s string) bool {
	return s == Received || s == Dispatching || s == Submitted || s == Completed || s == Uncertain || s == Duplicate
}
