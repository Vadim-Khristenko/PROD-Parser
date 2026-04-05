package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MessageRecord is canonical normalized record stored in JSONL.
type MessageRecord struct {
	InternalID uint64 `json:"internal_id"`

	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`
	MessageID int    `json:"message_id"`

	Date       time.Time `json:"date"`
	FromUserID int64     `json:"from_user_id"`
	// Optional user metadata snapshot, useful for profile exports.
	FromUsername    string   `json:"from_username,omitempty"`
	FromFirstName   string   `json:"from_first_name,omitempty"`
	FromLastName    string   `json:"from_last_name,omitempty"`
	FromDisplayName string   `json:"from_display_name,omitempty"`
	FromBio         string   `json:"from_bio,omitempty"`
	FromAvatarRef   string   `json:"from_avatar_ref,omitempty"`
	FromAvatarFile  string   `json:"from_avatar_file,omitempty"`
	Text            string   `json:"text,omitempty"`
	URLs            []string `json:"urls,omitempty"`

	ReplyToMsgID *int `json:"reply_to_msg_id,omitempty"`
	ReplyToTopID *int `json:"reply_to_top_id,omitempty"`
	ForumTopic   bool `json:"forum_topic"`

	MentionsUserIDs []int64 `json:"mentions_user_ids,omitempty"`
	MediaType       string  `json:"media_type,omitempty"`
	MediaID         string  `json:"media_id,omitempty"`
	MediaCanonical  string  `json:"media_canonical,omitempty"`
	MediaFileHash   string  `json:"media_file_hash,omitempty"`
	MediaFileName   string  `json:"media_file_name,omitempty"`
	MediaLocalPath  string  `json:"media_local_path,omitempty"`
	MediaFileSize   int64   `json:"media_file_size,omitempty"`
	HasVoice        bool    `json:"has_voice,omitempty"`

	// DerivedTopicID is custom detected topic id, independent from Telegram topic flags.
	DerivedTopicID string `json:"derived_topic_id,omitempty"`

	// ToxicityScore is heuristic score in [0..1].
	ToxicityScore float64 `json:"toxicity_score,omitempty"`

	// Raw is optional raw MTProto payload snapshot.
	Raw json.RawMessage `json:"raw,omitempty"`
}

func (m MessageRecord) Key() string {
	return fmt.Sprintf("%s:%d:%d", m.AccountID, m.ChatID, m.MessageID)
}

func (m MessageRecord) ChatKey() string {
	return fmt.Sprintf("%s:%d", m.AccountID, m.ChatID)
}

func (m MessageRecord) SearchText() string {
	if len(m.MentionsUserIDs) == 0 && len(m.URLs) == 0 {
		return m.Text
	}
	var b strings.Builder
	b.WriteString(m.Text)
	for _, uid := range m.MentionsUserIDs {
		b.WriteString(" @")
		b.WriteString(fmt.Sprintf("u%d", uid))
	}
	for _, u := range m.URLs {
		if strings.TrimSpace(u) == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(u)
	}
	return b.String()
}

// StoredPointer points to one JSONL entry and allows O(1) random read.
type StoredPointer struct {
	InternalID uint64 `json:"internal_id"`
	AccountID  string `json:"account_id"`
	ChatID     int64  `json:"chat_id"`
	MessageID  int    `json:"message_id"`

	FilePath string `json:"file_path"`
	Offset   int64  `json:"offset"`
	Length   int32  `json:"length"`
}
