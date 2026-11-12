package relay

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

// ConsumerState belongs to the worker, not to the credential-bearing sender.
// Claims never expire automatically: a lost worker may have performed a side effect.
type ConsumerState struct {
	Version  int                  `json:"consumer_version"`
	Identity broker.WatchIdentity `json:"identity"`
	Batches  map[string]Claim     `json:"batches"`
	Events   map[string]string    `json:"events"`
}

type Claim struct {
	Digest      string `json:"digest"`
	Status      string `json:"status"`
	Token       string `json:"claim_id,omitempty"`
	Events      int    `json:"new_events"`
	ClaimedAt   int64  `json:"claimed_at"`
	CompletedAt int64  `json:"completed_at,omitempty"`
}

type ClaimResult struct {
	Claimed bool   `json:"claimed"`
	BatchID string `json:"batch_id"`
	Status  string `json:"status"`
	Token   string `json:"claim_id,omitempty"`
	Work    *Batch `json:"work,omitempty"`
}

func consumerError() error {
	return e.New(e.CategoryConflict, "RELAY_CONSUMER_STATE", "Consumer state or batch conflicts with prior claims. Preserve the file and reconcile the previous worker; never reset claims to force a replay.")
}

func (s ConsumerState) Validate() error {
	if s.Version != 1 || len(s.Batches) > 10000 || len(s.Events) > 20000 || s.Batches == nil || s.Events == nil {
		return consumerError()
	}
	if len(s.Batches) == 0 {
		if s.Identity != (broker.WatchIdentity{}) || len(s.Events) != 0 {
			return consumerError()
		}
		return nil
	}
	if s.Identity.Context == "" || s.Identity.Bot == "" || s.Identity.Channel == "" || len(s.Identity.Ledger) != 32 {
		return consumerError()
	}
	total := 0
	for id, c := range s.Batches {
		if len(id) != 64 || len(c.Digest) != 64 || c.ClaimedAt <= 0 || c.Events < 0 || c.Events > 100 {
			return consumerError()
		}
		switch c.Status {
		case "claimed":
			if c.CompletedAt != 0 {
				return consumerError()
			}
		case "processed", "failed":
			if c.CompletedAt < c.ClaimedAt {
				return consumerError()
			}
		case "duplicate":
			if c.Events != 0 || c.Token != "" {
				return consumerError()
			}
		default:
			return consumerError()
		}
		if c.Status != "duplicate" && (c.Events == 0 || len(c.Token) != 32) {
			return consumerError()
		}
		total += c.Events
	}
	if total != len(s.Events) {
		return consumerError()
	}
	for id, d := range s.Events {
		if !apiclient.Identifier(id, 256) || len(d) != 64 {
			return consumerError()
		}
	}
	return nil
}

func ReadConsumer(path string) (ConsumerState, bool, error) {
	s := ConsumerState{Version: 1, Batches: map[string]Claim{}, Events: map[string]string{}}
	abs, err := filepath.Abs(path)
	if err != nil || path == "" {
		return s, false, consumerError()
	}
	if err = securefs.Directory(filepath.Dir(abs), true, false); err != nil {
		return s, false, err
	}
	b, err := securefs.ReadPrivate(abs, maxStateBytes, false)
	if os.IsNotExist(err) {
		return s, false, nil
	}
	if err != nil || safejson.Decode(b, &s, true) != nil || s.Validate() != nil {
		return s, false, consumerError()
	}
	return s, true, nil
}

func (s *Store) SaveConsumer(v ConsumerState) error {
	if err := v.Validate(); err != nil {
		return err
	}
	return s.save(v)
}

// ClaimBatch must be called under the file lock; publish its result only after SaveConsumer.
func (s *ConsumerState) ClaimBatch(b Batch, now time.Time) (ClaimResult, error) {
	result := ClaimResult{BatchID: b.ID}
	if err := b.Validate(); err != nil {
		return result, err
	}
	if err := s.Validate(); err != nil {
		return result, err
	}
	if len(s.Batches) > 0 && s.Identity != b.Identity {
		return result, consumerError()
	}
	digest := apiclient.Digest(b)
	if c, ok := s.Batches[b.ID]; ok {
		if c.Digest != digest {
			return result, consumerError()
		}
		result.Status = c.Status
		return result, nil
	}
	if len(s.Batches) >= 10000 {
		return result, consumerCapacity()
	}
	work := b
	work.Threads = nil
	additions := map[string]string{}
	for _, t := range b.Threads {
		out := Thread{Post: t.Post}
		for _, r := range t.Events {
			// The event ID is the deduplication key within this bound ledger;
			// a repeated key must retain the exact immutable event metadata.
			d := apiclient.Digest(r.Data)
			if previous, ok := s.Events[r.Data.ID]; ok {
				if previous != d {
					return result, consumerError()
				}
				continue
			}
			additions[r.Data.ID] = d
			out.Events = append(out.Events, r)
		}
		if len(out.Events) > 0 {
			work.Threads = append(work.Threads, out)
		}
	}
	if len(s.Events)+len(additions) > 20000 {
		return result, consumerCapacity()
	}
	c := Claim{Digest: digest, Status: "duplicate", Events: len(additions), ClaimedAt: now.UnixMilli()}
	if c.Events > 0 {
		token := make([]byte, 16)
		if _, err := rand.Read(token); err != nil {
			return result, consumerError()
		}
		c.Token = hex.EncodeToString(token)
		c.Status = "claimed"
		result.Claimed = true
		result.Token = c.Token
		result.Work = &work
	}
	result.Status = c.Status
	s.Identity = b.Identity
	s.Batches[b.ID] = c
	for id, d := range additions {
		s.Events[id] = d
	}
	return result, nil
}

func (s *ConsumerState) Complete(id, token, result string, now time.Time) error {
	if result != "processed" && result != "failed" {
		return consumerError()
	}
	c, ok := s.Batches[id]
	if !ok || token == "" || c.Token != token || (c.Status != "claimed" && c.Status != result) {
		return consumerError()
	}
	if c.Status == result {
		return nil
	}
	c.Status = result
	c.CompletedAt = now.UnixMilli()
	s.Batches[id] = c
	return s.Validate()
}

func consumerCapacity() error {
	return e.New(e.CategoryRateLimit, "RELAY_CONSUMER_FULL", "Worker deduplication history reached capacity. Preserve and reconcile the ledger before controlled archival; no new work was claimed.")
}
