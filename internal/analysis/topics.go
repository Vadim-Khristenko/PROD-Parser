package analysis

import (
	"sort"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/tokenize"
)

type TopicMode string

const (
	TopicModeHeuristic   TopicMode = "heuristic"
	TopicModeEmbedding   TopicMode = "embedding"
	TopicModeLLMFallback TopicMode = "llm-fallback"
)

type activeTopic struct {
	ID string

	AccountID string
	ChatID    int64

	MessageIDs []int
	UserSet    map[int64]struct{}
	KeywordCnt map[string]int

	LastAt  time.Time
	StartAt time.Time
}

type TopicsEngine struct {
	window           time.Duration
	minSimilarity    float64
	mode             TopicMode
	embeddingModel   string
	llmFallbackModel string

	chatSeq map[string]int64

	activeByChat map[string]map[string]*activeTopic
	msgToTopic   map[string]string
	finalized    map[string][]domain.Topic
}

func NewTopicsEngine(window time.Duration, minSimilarity float64) *TopicsEngine {
	if window <= 0 {
		window = 6 * time.Hour
	}
	if minSimilarity <= 0 {
		minSimilarity = 0.35
	}
	return &TopicsEngine{
		window:        window,
		minSimilarity: minSimilarity,
		mode:          TopicModeHeuristic,
		chatSeq:       make(map[string]int64, 128),
		activeByChat:  make(map[string]map[string]*activeTopic, 128),
		msgToTopic:    make(map[string]string, 1<<16),
		finalized:     make(map[string][]domain.Topic, 128),
	}
}

func (e *TopicsEngine) SetMode(mode TopicMode) {
	switch mode {
	case TopicModeEmbedding, TopicModeLLMFallback, TopicModeHeuristic:
		e.mode = mode
	default:
		e.mode = TopicModeHeuristic
	}
}

func (e *TopicsEngine) SetEmbeddingModel(model string) {
	e.embeddingModel = model
}

func (e *TopicsEngine) SetLLMFallbackModel(model string) {
	e.llmFallbackModel = model
}

func (e *TopicsEngine) Mode() TopicMode {
	return e.mode
}

func (e *TopicsEngine) EmbeddingModel() string {
	return e.embeddingModel
}

func (e *TopicsEngine) LLMFallbackModel() string {
	return e.llmFallbackModel
}

func (e *TopicsEngine) Assign(m domain.MessageRecord) string {
	chatKey := m.ChatKey()
	if _, ok := e.activeByChat[chatKey]; !ok {
		e.activeByChat[chatKey] = map[string]*activeTopic{}
	}

	if m.ReplyToTopID != nil {
		id := topicFromTopID(m.AccountID, m.ChatID, *m.ReplyToTopID)
		return e.upsertTopic(chatKey, id, m)
	}
	if m.ReplyToMsgID != nil {
		parentKey := m.AccountID + ":" + formatInt64(m.ChatID) + ":" + formatInt(*m.ReplyToMsgID)
		if tid, ok := e.msgToTopic[parentKey]; ok {
			return e.upsertTopic(chatKey, tid, m)
		}
	}

	cand := e.bestActive(chatKey, m)
	if cand != nil {
		return e.upsertTopic(chatKey, cand.ID, m)
	}

	e.chatSeq[chatKey]++
	id := m.AccountID + ":" + formatInt64(m.ChatID) + ":t:" + formatInt64(e.chatSeq[chatKey])
	return e.upsertTopic(chatKey, id, m)
}

func (e *TopicsEngine) FinalizeExpired(now time.Time) []domain.Topic {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var out []domain.Topic
	for chatKey, active := range e.activeByChat {
		for id, t := range active {
			if now.Sub(t.LastAt) <= e.window {
				continue
			}
			topic := toDomainTopic(t)
			out = append(out, topic)
			e.finalized[chatKey] = append(e.finalized[chatKey], topic)
			delete(active, id)
		}
	}
	return out
}

func (e *TopicsEngine) FinalizedTopics(accountID string, chatID int64) []domain.Topic {
	chatKey := accountID + ":" + formatInt64(chatID)
	in := e.finalized[chatKey]
	out := make([]domain.Topic, len(in))
	copy(out, in)
	return out
}

func (e *TopicsEngine) upsertTopic(chatKey, id string, m domain.MessageRecord) string {
	a := e.activeByChat[chatKey][id]
	if a == nil {
		a = &activeTopic{
			ID:         id,
			AccountID:  m.AccountID,
			ChatID:     m.ChatID,
			UserSet:    map[int64]struct{}{},
			KeywordCnt: map[string]int{},
			StartAt:    m.Date,
			LastAt:     m.Date,
		}
		e.activeByChat[chatKey][id] = a
	}
	a.MessageIDs = append(a.MessageIDs, m.MessageID)
	a.UserSet[m.FromUserID] = struct{}{}
	for _, tok := range tokenize.Tokens(m.Text) {
		a.KeywordCnt[tok]++
	}
	if m.Date.After(a.LastAt) {
		a.LastAt = m.Date
	}
	if m.Date.Before(a.StartAt) || a.StartAt.IsZero() {
		a.StartAt = m.Date
	}
	msgKey := m.AccountID + ":" + formatInt64(m.ChatID) + ":" + formatInt(m.MessageID)
	e.msgToTopic[msgKey] = id
	return id
}

func (e *TopicsEngine) bestActive(chatKey string, m domain.MessageRecord) *activeTopic {
	active := e.activeByChat[chatKey]
	if len(active) == 0 {
		return nil
	}
	toks := tokenize.Tokens(m.Text)
	if len(toks) == 0 {
		return nil
	}
	msgSet := map[string]struct{}{}
	for _, t := range toks {
		msgSet[t] = struct{}{}
	}
	var best *activeTopic
	bestScore := 0.0
	for _, t := range active {
		if !m.Date.IsZero() && !t.LastAt.IsZero() && m.Date.Sub(t.LastAt) > e.window {
			continue
		}
		inter := 0
		union := len(msgSet)
		for kw := range t.KeywordCnt {
			if _, ok := msgSet[kw]; ok {
				inter++
			} else {
				union++
			}
		}
		if union == 0 {
			continue
		}
		score := float64(inter) / float64(union)
		if _, ok := t.UserSet[m.FromUserID]; ok {
			score += 0.08
		}
		if score > bestScore {
			bestScore = score
			best = t
		}
	}
	minSimilarity := e.minSimilarity
	switch e.mode {
	case TopicModeEmbedding:
		// In embedding mode we would normally rely on vector similarity.
		// Until vectors are plugged in, use slightly more permissive lexical threshold.
		minSimilarity *= 0.85
	case TopicModeLLMFallback:
		// LLM fallback can disambiguate borderline clusters later, so we can be a bit permissive.
		minSimilarity *= 0.9
	}

	if bestScore < minSimilarity {
		return nil
	}
	return best
}

func toDomainTopic(a *activeTopic) domain.Topic {
	users := make([]int64, 0, len(a.UserSet))
	for uid := range a.UserSet {
		users = append(users, uid)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })
	kws := topKeywords(a.KeywordCnt, 12)
	return domain.Topic{
		TopicID:    a.ID,
		AccountID:  a.AccountID,
		ChatID:     a.ChatID,
		MessageIDs: append([]int(nil), a.MessageIDs...),
		UserIDs:    users,
		StartAt:    a.StartAt,
		EndAt:      a.LastAt,
		Keywords:   kws,
		Confidence: topicConfidence(a),
	}
}

func topKeywords(freq map[string]int, n int) []string {
	type kv struct {
		k string
		v int
	}
	all := make([]kv, 0, len(freq))
	for k, v := range freq {
		all = append(all, kv{k: k, v: v})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v == all[j].v {
			return all[i].k < all[j].k
		}
		return all[i].v > all[j].v
	})
	if len(all) > n {
		all = all[:n]
	}
	out := make([]string, len(all))
	for i, p := range all {
		out[i] = p.k
	}
	return out
}

func topicConfidence(a *activeTopic) float64 {
	sz := len(a.MessageIDs)
	if sz == 0 {
		return 0
	}
	if sz > 30 {
		sz = 30
	}
	users := len(a.UserSet)
	if users > 10 {
		users = 10
	}
	return 0.5*float64(sz)/30 + 0.5*float64(users)/10
}

func topicFromTopID(accountID string, chatID int64, topID int) string {
	return accountID + ":" + formatInt64(chatID) + ":top:" + formatInt(topID)
}
