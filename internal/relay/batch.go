package relay

import (
	"encoding/hex"

	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// Validate checks the version 1 receiver boundary without trusting a caller's
// supplied batch ID as authentication or removing the fixed trust envelope.
func (b Batch) Validate() error {
	bad := func() error {
		return e.New(e.CategoryParse, "RELAY_BATCH", "Supply a nonempty version 1 relay batch with matching identity and intact metadata-only trust envelopes.")
	}
	id, err := hex.DecodeString(b.ID)
	ledger, ledgerErr := hex.DecodeString(b.Identity.Ledger)
	if err != nil || len(id) != 32 || ledgerErr != nil || len(ledger) != 16 || b.Version != 1 || !config.ValidContextName(b.Identity.Context) || !apiclient.Identifier(b.Identity.Bot, 256) || !apiclient.Identifier(b.Identity.Channel, 128) || len(b.Threads) == 0 || len(b.Threads) > 100 {
		return bad()
	}
	ids, sequences, posts := map[string]bool{}, map[int64]bool{}, map[string]bool{}
	n := 0
	for _, t := range b.Threads {
		if !apiclient.Identifier(t.Post, 128) || posts[t.Post] || len(t.Events) == 0 {
			return bad()
		}
		posts[t.Post] = true
		for _, r := range t.Events {
			v := r.Data
			n++
			if n > 100 || v.Sequence < 1 || sequences[v.Sequence] || ids[v.ID] || !apiclient.Identifier(v.ID, 256) || v.Bot != b.Identity.Bot || v.Channel != b.Identity.Channel || v.Post != t.Post || !v.ObservationOnly || v.Feedback != "" || v.IsSelf() || (v.Kind != "topic_event" && v.Kind != "topic_reply_event") || v.Change < 1 || v.Change > 3 || r.Trust != "untrusted" || r.Warning != apiclient.Warning || r.Source.Context != b.Identity.Context || r.Source.Channel != b.Identity.Channel || r.Source.Service != "wecom" {
				return bad()
			}
			ids[v.ID], sequences[v.Sequence] = true, true
		}
	}
	return nil
}
