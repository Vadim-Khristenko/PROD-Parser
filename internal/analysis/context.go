package analysis

import (
	"sort"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

// BuildTopicContextPackets emits model-ready packets for topic summarization.
func BuildTopicContextPackets(topics []domain.Topic, byMessageID map[int]domain.MessageRecord) []domain.TopicContextPacket {
	out := make([]domain.TopicContextPacket, 0, len(topics))
	for _, t := range topics {
		msgs := make([]domain.MessageRecord, 0, len(t.MessageIDs))
		for _, id := range t.MessageIDs {
			if m, ok := byMessageID[id]; ok {
				msgs = append(msgs, m)
			}
		}
		sort.Slice(msgs, func(i, j int) bool {
			if msgs[i].Date.Equal(msgs[j].Date) {
				return msgs[i].MessageID < msgs[j].MessageID
			}
			return msgs[i].Date.Before(msgs[j].Date)
		})
		out = append(out, domain.TopicContextPacket{Topic: t, Messages: msgs})
	}
	return out
}

// BuildPersonaContextPackets emits packets for LLM persona generation.
func BuildPersonaContextPackets(
	chatID int64,
	users []domain.UserStats,
	recentByUser map[int64][]domain.MessageRecord,
	edgesByUser map[int64][]domain.RelationEdge,
) []domain.PersonaContextPacket {
	out := make([]domain.PersonaContextPacket, 0, len(users))
	for _, u := range users {
		out = append(out, domain.PersonaContextPacket{
			UserID:     u.UserID,
			ChatID:     chatID,
			Stats:      u,
			RecentMsgs: recentByUser[u.UserID],
			Edges:      edgesByUser[u.UserID],
		})
	}
	return out
}
