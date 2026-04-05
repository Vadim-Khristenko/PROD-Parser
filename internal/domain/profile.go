package domain

import "time"

// UserProfile stores the latest known profile snapshot for user in chat context.
type UserProfile struct {
	AccountID string `json:"account_id"`
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`

	Username    string `json:"username,omitempty"`
	FirstName   string `json:"first_name,omitempty"`
	LastName    string `json:"last_name,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Bio         string `json:"bio,omitempty"`
	AvatarRef   string `json:"avatar_ref,omitempty"`
	AvatarFile  string `json:"avatar_file,omitempty"`

	UpdatedAt time.Time `json:"updated_at"`
}

// ParticipantSnapshot is an API/static-site-friendly per-user export payload.
type ParticipantSnapshot struct {
	GeneratedAt time.Time `json:"generated_at"`
	AccountID   string    `json:"account_id"`
	ChatID      int64     `json:"chat_id"`
	UserID      int64     `json:"user_id"`

	Profile *UserProfile `json:"profile,omitempty"`
	Stats   UserStats    `json:"stats"`

	OutgoingRelations []RelationEdge          `json:"outgoing_relations"`
	IncomingRelations []RelationEdge          `json:"incoming_relations"`
	Topics            []Topic                 `json:"topics"`
	Persona           *Persona                `json:"persona,omitempty"`
	LLMNote           string                  `json:"llm_note,omitempty"`
	RecentMessages    []MessageRecord         `json:"recent_messages"`
	Content           ParticipantContentStats `json:"content"`
}

type CountByUser struct {
	UserID int64 `json:"user_id"`
	Count  int64 `json:"count"`
}

type CountByString struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type FileStat struct {
	CanonicalID string   `json:"canonical_id"`
	FileHash    string   `json:"file_hash,omitempty"`
	IDs         []string `json:"ids,omitempty"`
	Count       int64    `json:"count"`
}

type FileStatsScope struct {
	GIFs      []FileStat `json:"gifs,omitempty"`
	Stickers  []FileStat `json:"stickers,omitempty"`
	Documents []FileStat `json:"documents,omitempty"`
}

type FileStats struct {
	Chat    FileStatsScope            `json:"chat"`
	ByUser  map[int64]FileStatsScope  `json:"by_user,omitempty"`
	ByTopic map[string]FileStatsScope `json:"by_topic,omitempty"`
}

type MentionStats struct {
	Chat    []CountByUser            `json:"chat"`
	ByUser  map[int64][]CountByUser  `json:"by_user,omitempty"`
	ByTopic map[string][]CountByUser `json:"by_topic,omitempty"`
}

type URLStats struct {
	Chat    []CountByString            `json:"chat"`
	ByUser  map[int64][]CountByString  `json:"by_user,omitempty"`
	ByTopic map[string][]CountByString `json:"by_topic,omitempty"`
}

type ContentStats struct {
	Files    FileStats    `json:"files"`
	Mentions MentionStats `json:"mentions"`
	URLs     URLStats     `json:"urls"`
}

type ParticipantContentStats struct {
	Files    FileStatsScope  `json:"files"`
	Mentions []CountByUser   `json:"mentions"`
	URLs     []CountByString `json:"urls"`
}

type MindmapNode struct {
	ID     string  `json:"id"`
	Label  string  `json:"label"`
	Type   string  `json:"type"`
	Group  string  `json:"group,omitempty"`
	Weight float64 `json:"weight,omitempty"`
}

type MindmapEdge struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Label  string  `json:"label,omitempty"`
	Weight float64 `json:"weight,omitempty"`
}

type Mindmap struct {
	Nodes []MindmapNode `json:"nodes"`
	Edges []MindmapEdge `json:"edges"`
}

type RelationInsight struct {
	FromUserID int64   `json:"from_user_id"`
	ToUserID   int64   `json:"to_user_id"`
	Label      string  `json:"label"`
	Reason     string  `json:"reason,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type LLMInsight struct {
	Enabled   bool      `json:"enabled"`
	Provider  string    `json:"provider,omitempty"`
	Models    []string  `json:"models,omitempty"`
	Summary   string    `json:"summary,omitempty"`
	Generated time.Time `json:"generated_at"`

	TopicPatches     map[string]string `json:"topic_patches,omitempty"`
	PersonaNotes     map[int64]string  `json:"persona_notes,omitempty"`
	RelationInsights []RelationInsight `json:"relation_insights,omitempty"`
	Error            string            `json:"error,omitempty"`
}

// ChatInsights is typed aggregate view of one chat used for exports/API.
type ChatInsights struct {
	GeneratedAt time.Time `json:"generated_at"`
	AccountID   string    `json:"account_id"`
	ChatID      int64     `json:"chat_id"`
	Summary     string    `json:"summary,omitempty"`

	ChatStats ChatStats `json:"chat_stats"`

	Users    []UserStats    `json:"users"`
	Profiles []UserProfile  `json:"profiles"`
	Edges    []RelationEdge `json:"edges"`
	Topics   []Topic        `json:"topics"`
	Personas []Persona      `json:"personas"`

	TopicContexts   []TopicContextPacket   `json:"topic_contexts"`
	PersonaContexts []PersonaContextPacket `json:"persona_contexts"`
	ContentStats    ContentStats           `json:"content"`
	Mindmap         Mindmap                `json:"mindmap"`
	LLM             *LLMInsight            `json:"llm,omitempty"`
}
