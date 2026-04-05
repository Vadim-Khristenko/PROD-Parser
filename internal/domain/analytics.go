package domain

import "time"

type ChatStats struct {
	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`

	MessagesTotal        int64   `json:"messages_total"`
	TextMessages         int64   `json:"text_messages"`
	EmptyMessages        int64   `json:"empty_messages"`
	AvgMessageLength     float64 `json:"avg_message_length"`
	AvgWordsPerMessage   float64 `json:"avg_words_per_message"`
	MeaningfulWordsTotal int64   `json:"meaningful_words_total"`

	MessagesByHour    [24]int64 `json:"messages_by_hour"`
	MessagesByWeekday [7]int64  `json:"messages_by_weekday"`
	MessagesByMonth   [12]int64 `json:"messages_by_month"`
	// Index 1..366, 0 is unused.
	MessagesByYearDay [367]int64       `json:"messages_by_year_day"`
	MessagesByDate    map[string]int64 `json:"messages_by_date"`

	EmojiCount int64 `json:"emoji_count"`
	MediaCount int64 `json:"media_count"`
	VoiceCount int64 `json:"voice_count"`
	URLsShared int64 `json:"urls_shared"`

	QuestionMessages    int64   `json:"question_messages"`
	ExclamationMessages int64   `json:"exclamation_messages"`
	NightMessages       int64   `json:"night_messages"`
	WeekendMessages     int64   `json:"weekend_messages"`
	NightSharePct       float64 `json:"night_share_pct"`
	WeekendSharePct     float64 `json:"weekend_share_pct"`

	ToxicMessages int64   `json:"toxic_messages"`
	AvgToxicity   float64 `json:"avg_toxicity"`

	TopWords   []WordScore         `json:"top_words"`
	SmartWords []WeightedWordScore `json:"smart_words"`

	UpdatedAt time.Time `json:"updated_at"`
}

type UserStats struct {
	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`

	MessagesTotal                int64   `json:"messages_total"`
	TextMessages                 int64   `json:"text_messages"`
	EmptyMessages                int64   `json:"empty_messages"`
	AvgMessageLength             float64 `json:"avg_message_length"`
	AvgWordsPerMessage           float64 `json:"avg_words_per_message"`
	MeaningfulWordsTotal         int64   `json:"meaningful_words_total"`
	AvgMeaningfulWordsPerMessage float64 `json:"avg_meaningful_words_per_message"`
	MeaningfulWordRate           float64 `json:"meaningful_word_rate"`
	MessageSharePct              float64 `json:"message_share_pct"`
	ActiveDays                   int64   `json:"active_days"`
	AvgMessagesPerActiveDay      float64 `json:"avg_messages_per_active_day"`
	EngagementRate               float64 `json:"engagement_rate"`
	ActivityScore                float64 `json:"activity_score"`

	MessagesByHour    [24]int64 `json:"messages_by_hour"`
	MessagesByWeekday [7]int64  `json:"messages_by_weekday"`
	MessagesByMonth   [12]int64 `json:"messages_by_month"`
	// Index 1..366, 0 is unused.
	MessagesByYearDay [367]int64       `json:"messages_by_year_day"`
	MessagesByDate    map[string]int64 `json:"messages_by_date"`

	ReplyOut   int64 `json:"reply_out"`
	ReplyIn    int64 `json:"reply_in"`
	MentionOut int64 `json:"mention_out"`
	MentionIn  int64 `json:"mention_in"`

	EmojiCount int64 `json:"emoji_count"`
	MediaCount int64 `json:"media_count"`
	VoiceCount int64 `json:"voice_count"`
	URLsShared int64 `json:"urls_shared"`

	QuestionMessages    int64   `json:"question_messages"`
	ExclamationMessages int64   `json:"exclamation_messages"`
	NightMessages       int64   `json:"night_messages"`
	WeekendMessages     int64   `json:"weekend_messages"`
	NightSharePct       float64 `json:"night_share_pct"`
	WeekendSharePct     float64 `json:"weekend_share_pct"`

	ToxicMessages int64   `json:"toxic_messages"`
	AvgToxicity   float64 `json:"avg_toxicity"`

	TopWords   []WordScore         `json:"top_words"`
	SmartWords []WeightedWordScore `json:"smart_words"`

	UpdatedAt time.Time `json:"updated_at"`
}

type WordScore struct {
	Word  string `json:"word"`
	Count int64  `json:"count"`
}

type WeightedWordScore struct {
	Word  string  `json:"word"`
	Count int64   `json:"count"`
	Score float64 `json:"score"`
}

type RelationEdge struct {
	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`

	FromUserID int64 `json:"from_user_id"`
	ToUserID   int64 `json:"to_user_id"`

	Weight            float64 `json:"weight"`
	Replies           int64   `json:"replies"`
	Mentions          int64   `json:"mentions"`
	CoTopicCount      int64   `json:"co_topic_count"`
	TemporalAdjacency int64   `json:"temporal_adjacency"`
	ContextOverlap    int64   `json:"context_overlap"`
}

type Topic struct {
	TopicID    string  `json:"topic_id"`
	AccountID  string  `json:"account_id"`
	ChatID     int64   `json:"chat_id"`
	MessageIDs []int   `json:"message_ids"`
	UserIDs    []int64 `json:"user_ids"`

	StartAt    time.Time `json:"start_at"`
	EndAt      time.Time `json:"end_at"`
	Keywords   []string  `json:"keywords"`
	Summary    string    `json:"summary,omitempty"`
	Confidence float64   `json:"confidence"`
}

type Persona struct {
	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`

	Role           string           `json:"role"`
	Tone           string           `json:"tone"`
	Traits         []string         `json:"traits"`
	Interests      []string         `json:"interests"`
	Triggers       []string         `json:"triggers"`
	TypicalPhrases []string         `json:"typical_phrases"`
	Relations      map[int64]string `json:"relations"`
	Summary        string           `json:"summary"`
	Confidence     float64          `json:"confidence"`

	UpdatedAt time.Time `json:"updated_at"`
}

type TopicContextPacket struct {
	Topic    Topic           `json:"topic"`
	Messages []MessageRecord `json:"messages"`
}

type PersonaContextPacket struct {
	UserID     int64           `json:"user_id"`
	ChatID     int64           `json:"chat_id"`
	Stats      UserStats       `json:"stats"`
	RecentMsgs []MessageRecord `json:"recent_messages"`
	Edges      []RelationEdge  `json:"edges"`
}
