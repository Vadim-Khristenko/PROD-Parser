package analysis

import (
	"sort"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/tokenize"
)

type edgeKey struct {
	accountID string
	chatID    int64
	from      int64
	to        int64
}

type RelationsEngine struct {
	msgAuthor map[string]int64
	msgTokens map[string]map[string]struct{}
	chatPrev  map[string]prevMessage
	edges     map[edgeKey]*domain.RelationEdge
}

type prevMessage struct {
	userID int64
	at     time.Time
	tokens map[string]struct{}
}

func NewRelationsEngine() *RelationsEngine {
	return &RelationsEngine{
		msgAuthor: make(map[string]int64, 1<<16),
		msgTokens: make(map[string]map[string]struct{}, 1<<16),
		chatPrev:  make(map[string]prevMessage, 2048),
		edges:     make(map[edgeKey]*domain.RelationEdge, 1<<16),
	}
}

func (r *RelationsEngine) AddMessage(m domain.MessageRecord) {
	tokens := tokenize.Tokens(m.Text)
	tokenSet := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		tokenSet[t] = struct{}{}
	}

	msgKeyCurrent := m.Key()
	r.msgAuthor[msgKeyCurrent] = m.FromUserID
	r.msgTokens[msgKeyCurrent] = tokenSet

	chatKey := m.ChatKey()
	if prev, ok := r.chatPrev[chatKey]; ok {
		if prev.userID != 0 && prev.userID != m.FromUserID {
			delta := m.Date.Sub(prev.at)
			if delta < 0 {
				delta = -delta
			}
			if delta <= 15*time.Minute {
				ov := overlapCount(tokenSet, prev.tokens)
				r.addEdge(m.AccountID, m.ChatID, m.FromUserID, prev.userID, 0.3+0.05*float64(minInt(ov, 4)), 0, 0, 0, 1, int64(ov))
			}
		}
	}

	if m.ReplyToMsgID != nil {
		targetKey := msgKey(m.AccountID, m.ChatID, *m.ReplyToMsgID)
		if to, ok := r.msgAuthor[targetKey]; ok && to != m.FromUserID {
			ctxOv := overlapCount(tokenSet, r.msgTokens[targetKey])
			r.addEdge(m.AccountID, m.ChatID, m.FromUserID, to, 1.0+0.07*float64(minInt(ctxOv, 4)), 1, 0, 0, 0, int64(ctxOv))
		}
	}
	for _, to := range m.MentionsUserIDs {
		if to == m.FromUserID {
			continue
		}
		r.addEdge(m.AccountID, m.ChatID, m.FromUserID, to, 0.7, 0, 1, 0, 0, 0)
	}

	r.chatPrev[chatKey] = prevMessage{userID: m.FromUserID, at: m.Date, tokens: tokenSet}
}

func (r *RelationsEngine) AddCoTopic(accountID string, chatID int64, users []int64) {
	if len(users) < 2 {
		return
	}
	for i := 0; i < len(users); i++ {
		for j := i + 1; j < len(users); j++ {
			u1, u2 := users[i], users[j]
			if u1 == u2 {
				continue
			}
			r.addEdge(accountID, chatID, u1, u2, 0.4, 0, 0, 1, 0, 0)
			r.addEdge(accountID, chatID, u2, u1, 0.4, 0, 0, 1, 0, 0)
		}
	}
}

func (r *RelationsEngine) TopEdges(accountID string, chatID int64, limit int) []domain.RelationEdge {
	if limit <= 0 {
		limit = 100
	}
	out := make([]domain.RelationEdge, 0, 256)
	for k, e := range r.edges {
		if k.accountID == accountID && k.chatID == chatID {
			out = append(out, *e)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight == out[j].Weight {
			if out[i].FromUserID == out[j].FromUserID {
				return out[i].ToUserID < out[j].ToUserID
			}
			return out[i].FromUserID < out[j].FromUserID
		}
		return out[i].Weight > out[j].Weight
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (r *RelationsEngine) addEdge(
	accountID string,
	chatID int64,
	from, to int64,
	w float64,
	replies, mentions, cotopic int64,
	temporalAdjacency, contextOverlap int64,
) {
	k := edgeKey{accountID: accountID, chatID: chatID, from: from, to: to}
	e := r.edges[k]
	if e == nil {
		e = &domain.RelationEdge{AccountID: accountID, ChatID: chatID, FromUserID: from, ToUserID: to}
		r.edges[k] = e
	}
	e.Weight += w
	e.Replies += replies
	e.Mentions += mentions
	e.CoTopicCount += cotopic
	e.TemporalAdjacency += temporalAdjacency
	e.ContextOverlap += contextOverlap
}

func msgKey(accountID string, chatID int64, messageID int) string {
	return accountID + ":" + formatInt64(chatID) + ":" + formatInt(messageID)
}

func overlapCount(a map[string]struct{}, b map[string]struct{}) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	count := 0
	for k := range a {
		if _, ok := b[k]; ok {
			count++
		}
	}
	return count
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
