package state

import (
	"slices"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// Only fingerprints and observation metadata persist, never polled message bodies.
type PollResource struct {
	Digest string `json:"digest"`
}
type PollState struct {
	Policy    string                  `json:"policy"`
	Scopes    map[string]PollScope    `json:"scopes"`
	Resources map[string]PollResource `json:"resources"`
}
type PollScope struct {
	LastSuccess int64    `json:"last_success"`
	Whole       bool     `json:"whole_channel"`
	Posts       []string `json:"posts"`
}
type PollObservation struct {
	Key    string
	Digest string
	Event  callback.Event
}

func (s *Store) PollStatus(policy, scope string) (int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Polling == nil || s.snapshot.Polling.Policy != policy {
		return 0, 0
	}
	return s.snapshot.Polling.Scopes[scope].LastSuccess, len(s.snapshot.Polling.Scopes)
}

// CommitPoll atomically commits a complete snapshot and every derived event.
// Absence is never interpreted as deletion; only a returned recall flag is evidence.
func (s *Store) CommitPoll(policy, scope string, whole bool, posts []string, observations []PollObservation, now time.Time, retention time.Duration, maxEvents, maxRecords int) error {
	return s.Update(func(st *Snapshot) error {
		p := st.Polling
		if p == nil || p.Policy != policy {
			p = &PollState{Policy: policy, Scopes: map[string]PollScope{}, Resources: map[string]PollResource{}}
			st.Polling = p
		}
		_, initialized := p.Scopes[scope]
		if !initialized && len(p.Scopes) >= 32 {
			return e.New(e.CategoryRateLimit, "POLL_SCOPE_LIMIT", "This context already tracks 32 polling scopes. Reuse an existing scope or ask the operator to provision another context.")
		}
		for id, ev := range st.Events {
			if ev.Received < now.Add(-retention).Unix() {
				delete(st.Events, id)
			}
		}
		for _, observation := range observations {
			old, exists := p.Resources[observation.Key]
			if exists && old.Digest == observation.Digest {
				continue
			}
			if !exists && len(p.Resources) >= maxRecords {
				return e.New(e.CategoryRateLimit, "POLL_CAPACITY", "The persistent polling index reached poll_max_records; narrow the scope or ask the operator to review capacity. No partial snapshot was committed.")
			}
			p.Resources[observation.Key] = PollResource{Digest: observation.Digest}
			covered := initialized
			for _, other := range p.Scopes {
				covered = covered || other.Whole || slices.Contains(other.Posts, observation.Event.Post)
			}
			if !exists && !covered {
				continue // First successful snapshot establishes a quiet baseline.
			}
			if len(st.Events) >= maxEvents {
				return e.New(e.CategoryRateLimit, "INBOX_FULL", "The event inbox is full; the polling snapshot was not advanced.")
			}
			v := observation.Event
			if v.Change != 3 {
				v.Change, v.ChangeBasis = 1, "first_seen"
				if exists {
					v.Change, v.ChangeBasis = 2, "snapshot_changed"
				}
			}
			st.NextEventSequence++
			v.Sequence = st.NextEventSequence
			v.ID = apiclient.Digest([]any{"poll", st.ID, v.Sequence, observation.Key, observation.Digest})
			st.Events[v.ID] = v
		}
		p.Scopes[scope] = PollScope{LastSuccess: now.Unix(), Whole: whole, Posts: slices.Clone(posts)}
		return nil
	})
}
