package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

func (b *Broker) validateMention(id, channel, post, reply string) error {
	v, err := b.event(id)
	if err != nil {
		return err
	}
	if v.Kind != "topic_at_bot" || v.ObservationOnly || (v.Transport != "" && v.Transport != "callback") ||
		v.Channel != channel || v.Post != post || v.Reply != reply || v.Created < b.Now().Add(-24*time.Hour).Unix() {
		return denied()
	}
	return nil
}

func (b *Broker) listReactions(ctx context.Context, r Request, admin bool) (Response, error) {
	q := r.Query
	if !apiclient.Identifier(q.Channel, 128) || !apiclient.Identifier(q.Post, 128) ||
		(q.Reply != "" && !apiclient.Identifier(q.Reply, 128)) || q.Cursor != "" || q.From != 0 || q.To != 0 {
		return Response{}, e.New(e.CategoryUsage, "BAD_REACTION_TARGET", "Choose one channel/post and an optional reply; reaction list does not accept history windows or cursors.")
	}
	ch, ok := b.Config.Channel(q.Channel)
	if !ok {
		return Response{}, denied()
	}
	if r.ID != "" {
		if err := b.validateMention(r.ID, q.Channel, q.Post, q.Reply); err != nil {
			return Response{}, err
		}
	} else if !admin && !ch.ReadAll {
		return Response{}, denied()
	}
	operation := "post.get"
	if q.Reply != "" {
		operation = "reply.get"
	}
	result, err := b.API.Read(ctx, apiclient.Query{Operation: operation, Channel: q.Channel, Post: q.Post, Reply: q.Reply})
	if err != nil {
		return Response{}, err
	}
	if len(result.Items) == 0 {
		return Response{}, missing()
	}
	bad := func() (Response, error) {
		return Response{}, e.New(e.CategoryParse, "BAD_REACTION_RESULT", "The reaction read returned ambiguous or mismatched target metadata.")
	}
	if len(result.Items) != 1 || result.HasMore || result.Items[0].Source.Channel != q.Channel || result.Items[0].Source.Post != q.Post {
		return bad()
	}
	encoded, err := json.Marshal(result.Items[0].Data)
	var message apiclient.Message
	if err != nil || json.Unmarshal(encoded, &message) != nil || len(message.Reactions) > 256 {
		return bad()
	}
	if q.Reply == "" && message.PostID != q.Post || q.Reply != "" && (message.ReplyID != q.Reply || message.PostID != "" && message.PostID != q.Post) {
		return bad()
	}
	items := []apiclient.Record{}
	seen := map[int]bool{}
	for _, reaction := range message.Reactions {
		if reaction.EmojiType <= 0 || seen[reaction.EmojiType] || reaction.Users == nil {
			return bad()
		}
		seen[reaction.EmojiType] = true
		own := false
		for _, member := range reaction.Users {
			if member.Type == 0 {
				if !apiclient.Identifier(member.UserID, 256) {
					return bad()
				}
				continue
			}
			if member.Type != 1 {
				return bad()
			}
			if member.BotID != "" && member.LegacyBotID != "" && member.BotID != member.LegacyBotID {
				return bad()
			}
			id := member.BotID
			if id == "" {
				id = member.LegacyBotID
			}
			if !apiclient.Identifier(id, 256) {
				return bad()
			}
			own = own || member.BotID == b.Config.Bot || member.LegacyBotID == b.Config.Bot
		}
		record := apiclient.Untrusted(map[string]any{"emoji_type": reaction.EmojiType, "user_list": reaction.Users, "bot_reacted": own}, q.Channel, q.Post, r.ID)
		record.Source.Context = b.Config.Name
		record.Source.Reply = q.Reply
		items = append(items, record)
	}
	return page(apiclient.Page{Items: items}), nil
}
