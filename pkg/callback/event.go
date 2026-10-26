package callback

import (
	"encoding/json"
	"time"

	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

type Event struct {
	Transport        string `json:"transport,omitempty"`
	ChangeBasis      string `json:"change_basis,omitempty"`
	ResourceCreated  int64  `json:"resource_created_at,omitempty"`
	ResourceModified int64  `json:"resource_modified_at,omitempty"`
	ObservationOnly  bool   `json:"observation_only"`
	ID               string `json:"id"`
	Sequence         int64  `json:"sequence"`
	MessageID        string `json:"-"`
	Bot              string `json:"bot_id"`
	Channel          string `json:"channel_id"`
	Sender           string `json:"sender_id"`
	SenderKind       string `json:"sender_kind"`
	Kind             string `json:"kind"`
	Post             string `json:"post_id,omitempty"`
	Reply            string `json:"reply_id,omitempty"`
	Created          int64  `json:"created_at"`
	Received         int64  `json:"received_at"`
	Change           int    `json:"change,omitempty"`
	Feedback         string `json:"feedback,omitempty"`
}

func Parse(data []byte, bot string, now time.Time) (Event, error) {
	var raw struct {
		ID      string `json:"msgid"`
		Created int64  `json:"create_time"`
		Bot     string `json:"aibotid"`
		Channel string `json:"topic_id"`
		From    struct {
			User string `json:"userid"`
			Bot  string `json:"aibotid"`
		} `json:"from"`
		Type  string                     `json:"msgtype"`
		Event map[string]json.RawMessage `json:"event"`
	}
	if safejson.Decode(data, &raw, false) != nil || !apiclient.Identifier(raw.ID, 256) || raw.Bot != bot || raw.Type != "event" || !apiclient.Identifier(raw.Channel, 128) || raw.Created <= 0 || raw.Created > now.Unix()+300 || raw.Created < now.Unix()-86400 {
		return Event{}, ErrInvalid
	}
	var kind string
	if json.Unmarshal(raw.Event["eventtype"], &kind) != nil {
		return Event{}, ErrInvalid
	}
	v := Event{Transport: "callback", ID: apiclient.Digest(bot + "\x00" + raw.ID), MessageID: raw.ID, Bot: bot, Channel: raw.Channel, Created: raw.Created, Received: now.Unix(), Kind: kind}
	if raw.From.User != "" && raw.From.Bot == "" {
		v.Sender = raw.From.User
		v.SenderKind = "user"
	} else if raw.From.Bot != "" && raw.From.User == "" {
		v.Sender = raw.From.Bot
		v.SenderKind = "bot"
	} else {
		return Event{}, ErrInvalid
	}
	if !apiclient.Identifier(v.Sender, 256) {
		return Event{}, ErrInvalid
	}
	var fields struct {
		Post    string `json:"post_id"`
		Reply   string `json:"reply_id"`
		Type    int    `json:"type"`
		Change  int    `json:"change_type"`
		Info    int    `json:"info_type"`
		Content string `json:"content"`
	}
	if json.Unmarshal(raw.Event[kind], &fields) != nil {
		return Event{}, ErrInvalid
	}
	v.Post, v.Reply = fields.Post, fields.Reply
	v.ObservationOnly = kind == "topic_event" || kind == "topic_reply_event"
	switch kind {
	case "topic_at_bot":
		if v.SenderKind != "user" || !apiclient.Identifier(v.Post, 128) {
			return Event{}, ErrInvalid
		}
	case "topic_event", "topic_reply_event", "feedback_event":
		if !apiclient.Identifier(v.Post, 128) || fields.Type < 1 || fields.Type > 3 {
			return Event{}, ErrInvalid
		}
		v.Change = fields.Type
		if kind == "topic_reply_event" && !apiclient.Identifier(v.Reply, 128) {
			return Event{}, ErrInvalid
		}
		if kind == "feedback_event" {
			if len(fields.Content) > 8192 {
				return Event{}, ErrInvalid
			}
			v.Feedback = fields.Content
		}
	case "topic_bot_join_leave", "topic_member_change":
		if fields.Change < 1 || fields.Change > 2 {
			return Event{}, ErrInvalid
		}
		v.Change = fields.Change
	case "topic_info_change":
		if fields.Info < 1 || fields.Info > 4 {
			return Event{}, ErrInvalid
		}
		v.Change = fields.Info
	default:
		return Event{}, ErrInvalid
	}
	if v.Reply != "" && !apiclient.Identifier(v.Reply, 128) {
		return Event{}, ErrInvalid
	}
	return v, nil
}
