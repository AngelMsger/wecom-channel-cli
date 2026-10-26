package apiclient

import e "github.com/angelmsger/wecom-channel-cli/pkg/errors"

// EmojiType is a native WeCom type, not a Unicode character to send as content.
type EmojiType struct {
	Type  int    `json:"emoji_type"`
	Label string `json:"label"`
}

// Preserve the official localized labels verbatim as protocol catalog data.
// Source: developer.work.weixin.qq.com/document/path/101922, checked 2026-10-26.
var emojiLabels = [...]string{
	"[+1]",
	"[666]",
	"[DONE]",
	"[Emm]",
	"[GET]",
	"[OK]",
	"[YES]",
	"[亲亲]",
	"[便便]",
	"[偷笑]",
	"[傲慢]",
	"[再见]",
	"[凋谢]",
	"[加油]",
	"[勾引]",
	"[发呆]",
	"[发怒]",
	"[发抖]",
	"[可怜]",
	"[右哼哼]",
	"[叹气]",
	"[吃瓜]",
	"[合十]",
	"[吐]",
	"[呲牙]",
	"[咒骂]",
	"[咖啡]",
	"[哇]",
	"[啤酒]",
	"[嘘]",
	"[嘴唇]",
	"[嘿哈]",
	"[囧]",
	"[困]",
	"[坏笑]",
	"[大哭]",
	"[天啊]",
	"[太阳]",
	"[失望]",
	"[奸笑]",
	"[好]",
	"[好的]",
	"[委屈]",
	"[害羞]",
	"[尴尬]",
	"[庆祝]",
	"[弱]",
	"[强]",
	"[得意]",
	"[微笑]",
	"[心碎]",
	"[快哭了]",
	"[恐惧]",
	"[悠闲]",
	"[惊恐]",
	"[惊讶]",
	"[愉快]",
	"[憨笑]",
	"[打脸]",
	"[抓狂]",
	"[抠鼻]",
	"[抱拳]",
	"[拥抱]",
	"[拳头]",
	"[捂脸]",
	"[握手]",
	"[撇嘴]",
	"[擦汗]",
	"[收到]",
	"[敲打]",
	"[无语]",
	"[旺柴]",
	"[晕]",
	"[月亮]",
	"[机智]",
	"[汗]",
	"[流泪]",
	"[炸弹]",
	"[烟花]",
	"[爆竹]",
	"[爱心]",
	"[猪头]",
	"[玫瑰]",
	"[生病]",
	"[疑问]",
	"[發]",
	"[白眼]",
	"[皱眉]",
	"[睡]",
	"[破涕为笑]",
	"[礼物]",
	"[社会社会]",
	"[福]",
	"[笑脸]",
	"[红包]",
	"[翻白眼]",
	"[耶]",
	"[胜利]",
	"[脸红]",
	"[色]",
	"[苦涩]",
	"[菜刀]",
	"[蛋糕]",
	"[衰]",
	"[裂开]",
	"[让我看看]",
	"[调皮]",
	"[跳跳]",
	"[转圈]",
	"[鄙视]",
	"[闭嘴]",
	"[阴险]",
	"[难过]",
	"[骷髅]",
	"[鼓掌]",
}

func EmojiTypes() []EmojiType {
	out := make([]EmojiType, len(emojiLabels))
	for i, label := range emojiLabels {
		out[i] = EmojiType{Type: i + 1, Label: label}
	}
	return out
}

func ValidEmojiType(value int) bool { return value >= 1 && value <= len(emojiLabels) }

func buildReaction(r WriteRequest) (Plan, error) {
	if !Identifier(r.Channel, 128) || !Identifier(r.Key, 128) || !Identifier(r.Post, 128) ||
		(r.Reply != "" && !Identifier(r.Reply, 128)) || (r.Event != "" && !Identifier(r.Event, 256)) ||
		r.Content != "" || r.ParentReply != "" {
		return Plan{}, e.New(e.CategoryUsage, "BAD_REACTION_TARGET", "A reaction requires a channel, post and idempotency key, with an optional reply and mention event. Text and parent-reply fields are not accepted.")
	}
	if !ValidEmojiType(r.EmojiType) {
		return Plan{}, e.New(e.CategoryUsage, "BAD_EMOJI_TYPE", "Choose a native emoji_type from reaction types.").WithNextSteps("wecom-channel-cli reaction types")
	}
	action := 1
	switch r.Action {
	case "add":
	case "remove":
		action = 0
	default:
		return Plan{}, e.New(e.CategoryUsage, "BAD_REACTION_ACTION", "Choose --action add or --action remove explicitly.")
	}
	body := map[string]any{"topic_id": r.Channel, "post_id": r.Post, "emoji_type": r.EmojiType, "action": action}
	if r.Reply != "" {
		body["reply_id"] = r.Reply
	}
	return Plan{Method: "POST", Path: "/cgi-bin/aibot/topic/post_emoji", Body: body}, nil
}
