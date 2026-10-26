package apiclient

import (
	"context"
	"encoding/json"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
)

type User struct {
	Type        int    `json:"type"`
	UserID      string `json:"userid,omitempty"`
	BotID       string `json:"aibotid,omitempty"`
	LegacyBotID string `json:"botid,omitempty"`
}

type Reaction struct {
	EmojiType int    `json:"emoji_type"`
	Users     []User `json:"user_list"`
}

type File struct {
	Placeholder string `json:"placeholder"`
	Name        string `json:"file_name"`
}

type Message struct {
	PostID             string     `json:"post_id,omitempty"`
	ReplyID            string     `json:"reply_id,omitempty"`
	ParentReplyID      string     `json:"parent_reply_id,omitempty"`
	Creator            *User      `json:"creator,omitempty"`
	CreatorInfo        *User      `json:"creator_info,omitempty"`
	Created            int64      `json:"create_time"`
	Modified           int64      `json:"modify_time,omitempty"`
	Recalled           bool       `json:"is_recalled"`
	IsModified         bool       `json:"is_modified"`
	Content            string     `json:"content"`
	Files              []File     `json:"inline_file_list,omitempty"`
	ImageCount         int        `json:"image_count"`
	AttachmentsOmitted bool       `json:"attachment_access_omitted"`
	Reactions          []Reaction `json:"emoji_list,omitempty"`
}

type Topic struct {
	ID          string `json:"topic_id"`
	Name        string `json:"topic_name"`
	Description string `json:"topic_desc"`
	Permission  int    `json:"permission"`
	Creator     *User  `json:"creator,omitempty"`
	Created     int64  `json:"create_time"`
}

func BuildRead(q Query) (Plan, string, error) {
	bad := func() (Plan, string, error) {
		return Plan{}, "", e.New(e.CategoryUsage, "BAD_QUERY", "The query has an invalid identifier, cursor, limit, keyword or time window.")
	}
	if q.Limit == 0 {
		q.Limit = 20
	}
	if len(q.Cursor) > 256 || q.Limit < 1 || q.Limit > 50 || q.From < 0 || q.To < 0 || (q.To > 0 && (q.From == 0 || q.To <= q.From)) {
		return bad()
	}
	b := map[string]any{"cursor": q.Cursor, "limit": q.Limit}
	field, path := "", ""
	if q.Operation != "channel.list" && !Identifier(q.Channel, 128) {
		return bad()
	}
	switch q.Operation {
	case "channel.list":
		path = "get_topic_list"
		field = "topic_list"
		b["type"] = 1
		if q.ChannelIDs != nil {
			if len(q.ChannelIDs) > 100 {
				return bad()
			}
			b = map[string]any{"type": 0, "topic_id_list": q.ChannelIDs}
		}
	case "member.list":
		path = "get_topic_member_list"
		field = "member_list"
		b["topic_id"] = q.Channel
	case "post.list", "post.get", "reply.list", "reply.get":
		kind := "post"
		if q.Operation == "reply.list" || q.Operation == "reply.get" {
			kind = "reply"
			if !Identifier(q.Post, 128) {
				return bad()
			}
			b["post_id"] = q.Post
		}
		field = kind + "_list"
		path = "get_" + field
		b["topic_id"] = q.Channel
		b["type"] = 1
		b["sort_type"] = 0
		if q.From > 0 {
			b["begin_time"] = q.From
		}
		if q.To > 0 {
			b["end_time"] = q.To
		}
		if q.Operation == "post.get" || q.Operation == "reply.get" {
			id := q.Post
			if kind == "reply" {
				id = q.Reply
			}
			if !Identifier(id, 128) || q.Cursor != "" || q.From != 0 || q.To != 0 {
				return bad()
			}
			b = map[string]any{"topic_id": q.Channel, "type": 0, kind + "_id_list": []string{id}}
			if kind == "reply" {
				b["post_id"] = q.Post
			}
		}
	case "search.start":
		if q.Keyword == "" || len(q.Keyword) > 32 {
			return bad()
		}
		path = "search"
		field = "topic_list"
		b["keyword_list"] = []string{q.Keyword}
		b["topic_id_list"] = []string{q.Channel}
		if q.From > 0 {
			b["begin_time"] = q.From
		}
		if q.To > 0 {
			b["end_time"] = q.To
		}
	case "search.result":
		if !Identifier(q.Job, 256) || q.Limit > 20 {
			return bad()
		}
		path = "get_search_result"
		field = "result_list"
		b["jobid"] = q.Job
		b["topic_id"] = q.Channel
	default:
		return Plan{}, "", e.New(e.CategoryUsage, "UNKNOWN_OPERATION", "This read operation is not supported.")
	}
	return Plan{"POST", "/cgi-bin/aibot/topic/" + path, b}, field, nil
}

func (c *HTTPClient) Read(ctx context.Context, q Query) (Page, error) {
	p, field, err := BuildRead(q)
	if err != nil {
		return Page{}, err
	}
	b, err := c.request(ctx, p.Path, p.Body)
	if err != nil {
		return Page{}, err
	}
	var result struct {
		Topics  []json.RawMessage `json:"topic_list"`
		Members []json.RawMessage `json:"member_list"`
		Posts   []json.RawMessage `json:"post_list"`
		Replies []json.RawMessage `json:"reply_list"`
		Results []json.RawMessage `json:"result_list"`
		More    *bool             `json:"has_more"`
		Next    string            `json:"next_cursor"`
		Job     string            `json:"jobid"`
	}
	if safejson.Decode(b, &result, false) != nil {
		return Page{}, badResult()
	}
	paged := p.Body["type"] != 0
	if paged && result.More == nil {
		return Page{}, badResult()
	}
	more := result.More != nil && *result.More
	if !paged && more {
		return Page{}, badResult()
	}
	if more && (!Identifier(result.Next, 256) || result.Next == q.Cursor) {
		return Page{}, badResult()
	}
	rows := map[string][]json.RawMessage{"topic_list": result.Topics, "member_list": result.Members, "post_list": result.Posts, "reply_list": result.Replies, "result_list": result.Results}[field]
	if rows == nil || len(rows) > 200 {
		return Page{}, badResult()
	}
	page := Page{Items: []Record{}, Next: result.Next, HasMore: more}
	if !more {
		page.Next = ""
	}
	for _, raw := range rows {
		var data any
		channel, post := q.Channel, q.Post
		switch field {
		case "topic_list":
			var v Topic
			if json.Unmarshal(raw, &v) != nil || !Identifier(v.ID, 128) {
				return Page{}, badResult()
			}
			channel = v.ID
			if q.Operation == "search.start" {
				if v.ID != q.Channel || !Identifier(result.Job, 256) {
					return Page{}, badResult()
				}
				data = map[string]any{"topic_id": v.ID, "job_id": result.Job}
			} else {
				if q.ChannelIDs != nil && !contains(q.ChannelIDs, v.ID) {
					return Page{}, badResult()
				}
				data = v
			}
		case "member_list":
			var v User
			if json.Unmarshal(raw, &v) != nil {
				return Page{}, badResult()
			}
			data = v
		case "post_list", "reply_list":
			var v Message
			var media struct {
				Images []json.RawMessage `json:"image_list"`
			}
			if json.Unmarshal(raw, &v) != nil || json.Unmarshal(raw, &media) != nil {
				return Page{}, badResult()
			}
			if field == "post_list" {
				if !Identifier(v.PostID, 128) || (q.Operation == "post.get" && v.PostID != q.Post) {
					return Page{}, badResult()
				}
				post = v.PostID
			} else if !Identifier(v.ReplyID, 128) || (q.Operation == "reply.get" && v.ReplyID != q.Reply) {
				return Page{}, badResult()
			}
			if (q.From > 0 && v.Created < q.From) || (q.To > 0 && v.Created >= q.To) {
				continue
			}
			v.ImageCount = len(media.Images)
			v.AttachmentsOmitted = true
			data = v
		case "result_list":
			var v struct {
				Post    string `json:"post_id"`
				Brief   string `json:"brief_content"`
				Replies []struct {
					ID    string `json:"reply_id"`
					Brief string `json:"brief_content"`
				} `json:"reply_list"`
			}
			if json.Unmarshal(raw, &v) != nil || !Identifier(v.Post, 128) {
				return Page{}, badResult()
			}
			post = v.Post
			data = v
		}
		// JSON round-tripping the curated model cannot introduce upstream keys.
		encoded, _ := json.Marshal(data)
		var generic any
		if json.Unmarshal(encoded, &generic) != nil {
			return Page{}, badResult()
		}
		page.Items = append(page.Items, Untrusted(c.redact(generic), channel, post, ""))
	}
	return page, nil
}

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
func badResult() *e.CLIError {
	return e.New(e.CategoryParse, "BAD_RESPONSE", "WeCom returned invalid identity or pagination data.")
}
func (c *HTTPClient) redact(v any) any {
	switch x := v.(type) {
	case string:
		return c.scrub(x)
	case []any:
		for i := range x {
			x[i] = c.redact(x[i])
		}
	case map[string]any:
		for k, v := range x {
			x[k] = c.redact(v)
		}
	}
	return v
}
