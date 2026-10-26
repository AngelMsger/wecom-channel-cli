package broker

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

type pollAttempt struct {
	at  time.Time
	err error
}

// poll is demand-driven: only an active non-preview watch may perform remote reads.
// All readers of a scope share its persisted baseline and minimum poll interval.
func (b *Broker) poll(ctx context.Context, r ScanRequest) error {
	select {
	case b.pollGate <- struct{}{}:
		defer func() { <-b.pollGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	policy := apiclient.Digest(b.Config)
	scope := apiclient.Digest(struct {
		Whole bool
		Posts []string
	}{r.WholeChannel, r.Posts})
	now := b.Now()
	interval := time.Duration(b.Config.PollIntervalSeconds) * time.Second
	last, count := b.Store.PollStatus(policy, scope)
	if last != 0 && now.Sub(time.Unix(last, 0)) >= 0 && now.Sub(time.Unix(last, 0)) < interval {
		return nil
	}
	if last == 0 && count >= 32 {
		return e.New(e.CategoryRateLimit, "POLL_SCOPE_LIMIT", "This context already tracks 32 polling scopes; reuse a scope or ask the operator to provision another context.")
	}
	if attempt, ok := b.pollAttempts[scope]; ok && now.Sub(attempt.at) >= 0 && now.Sub(attempt.at) < interval {
		return attempt.err
	}
	if _, ok := b.pollAttempts[scope]; !ok && len(b.pollAttempts) >= 32 {
		return e.New(e.CategoryRateLimit, "POLL_SCOPE_LIMIT", "Too many polling scopes were requested from this broker; reuse an existing scope.")
	}
	observations, err := b.pollSnapshot(ctx, r, now)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = b.Store.CommitPoll(policy, scope, r.WholeChannel, r.Posts, observations, b.Now(), time.Duration(b.Config.RetentionHours)*time.Hour, b.Config.MaxEvents, b.Config.PollMaxRecords)
	}
	b.pollAttempts[scope] = pollAttempt{b.Now(), err}
	return err
}

func (b *Broker) pollSnapshot(ctx context.Context, r ScanRequest, now time.Time) ([]state.PollObservation, error) {
	channel := b.Config.Channels[0].ID
	requests := 0
	observations := map[string]state.PollObservation{}
	readPage := func(q apiclient.Query) (apiclient.Page, error) {
		if err := ctx.Err(); err != nil {
			return apiclient.Page{}, err
		}
		if requests >= b.Config.PollMaxRequests {
			return apiclient.Page{}, e.New(e.CategoryRateLimit, "POLL_BUDGET", "The polling snapshot exceeds poll_max_requests. Select fewer --post IDs or ask the operator to review the budget; no partial snapshot was committed.")
		}
		if !b.rateAllowed("poll-upstream") {
			return apiclient.Page{}, e.New(e.CategoryRateLimit, "POLL_RATE_LIMIT", "Polling reached the context's requests_per_minute budget. Increase the interval, narrow the scope, or ask the operator to review the rate limit.")
		}
		requests++
		return b.API.Read(ctx, q)
	}
	decode := func(record apiclient.Record, post string, reply bool) (apiclient.Message, error) {
		var m apiclient.Message
		data, err := json.Marshal(record.Data)
		if err != nil || json.Unmarshal(data, &m) != nil || record.Trust != "untrusted" || record.Source.Channel != channel || m.Created <= 0 {
			return m, pollResponse()
		}
		kind := "topic_event"
		if reply {
			kind = "topic_reply_event"
			if !apiclient.Identifier(m.ReplyID, 128) || (m.PostID != "" && m.PostID != post) || record.Source.Post != post {
				return m, pollResponse()
			}
			m.PostID = post
		} else if !apiclient.Identifier(m.PostID, 128) || (post != "" && m.PostID != post) || record.Source.Post != m.PostID {
			return m, pollResponse()
		}
		key := apiclient.Digest([]string{channel, m.PostID, m.ReplyID, kind})
		v := callback.Event{Transport: "poll", ObservationOnly: true, Bot: b.Config.Bot, Channel: channel,
			Post: m.PostID, Reply: m.ReplyID, Kind: kind, SenderKind: "unknown",
			Created: now.Unix(), Received: now.Unix(), ResourceCreated: m.Created, ResourceModified: m.Modified}
		if m.Recalled {
			v.Change, v.ChangeBasis = 3, "recall_flag"
		}
		fingerprint := m
		fingerprint.Reactions = nil // Reaction membership is not a post/reply change feed.
		observation := state.PollObservation{Key: key, Digest: apiclient.Digest(fingerprint), Event: v}
		if old, exists := observations[key]; exists && old.Digest != observation.Digest {
			return m, e.New(e.CategoryConflict, "POLL_UNSTABLE", "A resource changed while pagination repeated it. Retry a complete snapshot; no partial snapshot was committed.")
		}
		observations[key] = observation
		if len(observations) > b.Config.PollMaxRecords {
			return m, e.New(e.CategoryRateLimit, "POLL_CAPACITY", "The polling snapshot exceeds poll_max_records; narrow the scope or ask the operator to review capacity.")
		}
		return m, nil
	}
	list := func(operation, post string) ([]apiclient.Message, error) {
		// Creation-time pagination is not a change feed. Revisit the complete
		// bounded snapshot so edits and replies on old posts remain observable.
		q := apiclient.Query{Operation: operation, Channel: channel, Post: post, Limit: 50, From: 1, To: now.Unix() + 1}
		seen := map[string]bool{"": true}
		var messages []apiclient.Message
		for {
			page, err := readPage(q)
			if err != nil {
				return nil, err
			}
			if len(page.Items) > q.Limit || (page.HasMore && (!apiclient.Identifier(page.Next, 256) || seen[page.Next])) {
				return nil, pollResponse()
			}
			for _, record := range page.Items {
				m, err := decode(record, post, operation == "reply.list")
				if err != nil {
					return nil, err
				}
				messages = append(messages, m)
			}
			if !page.HasMore {
				return messages, nil
			}
			seen[page.Next] = true
			q.Cursor = page.Next
		}
	}
	var posts []apiclient.Message
	if r.WholeChannel {
		var err error
		posts, err = list("post.list", "")
		if err != nil {
			return nil, err
		}
	} else {
		for _, id := range r.Posts {
			page, err := readPage(apiclient.Query{Operation: "post.get", Channel: channel, Post: id})
			if err != nil {
				return nil, err
			}
			if page.HasMore || len(page.Items) != 1 {
				return nil, e.New(e.CategoryNotFound, "POLL_POST_UNAVAILABLE", "A selected post is absent or inaccessible. This is not proof of deletion; no snapshot was advanced.")
			}
			m, err := decode(page.Items[0], id, false)
			if err != nil {
				return nil, err
			}
			posts = append(posts, m)
		}
	}
	seenPosts := map[string]bool{}
	for _, post := range posts {
		if post.Recalled || seenPosts[post.PostID] {
			continue
		}
		seenPosts[post.PostID] = true
		if _, err := list("reply.list", post.PostID); err != nil {
			return nil, err
		}
	}
	keys := make([]string, 0, len(observations))
	for key := range observations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]state.PollObservation, 0, len(keys))
	for _, key := range keys {
		out = append(out, observations[key])
	}
	return out, nil
}

func pollResponse() error {
	return e.New(e.CategoryParse, "POLL_RESPONSE", "Polling returned inconsistent resource identity, creation time or pagination. No partial snapshot was committed.")
}
