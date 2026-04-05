package analysis

import (
	"sort"
	"strings"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/tokenize"
)

type StatsEngine struct {
	chatStats map[string]*domain.ChatStats
	userStats map[string]*domain.UserStats

	chatWordFreq map[string]map[string]int64
	userWordFreq map[string]map[string]int64

	chatAllWordCount map[string]int64
	userAllWordCount map[string]int64
	userActiveDates  map[string]map[string]struct{}
}

func NewStatsEngine() *StatsEngine {
	return &StatsEngine{
		chatStats:        make(map[string]*domain.ChatStats, 256),
		userStats:        make(map[string]*domain.UserStats, 2048),
		chatWordFreq:     make(map[string]map[string]int64, 256),
		userWordFreq:     make(map[string]map[string]int64, 2048),
		chatAllWordCount: make(map[string]int64, 256),
		userAllWordCount: make(map[string]int64, 2048),
		userActiveDates:  make(map[string]map[string]struct{}, 2048),
	}
}

func (s *StatsEngine) Add(m domain.MessageRecord) {
	chatKey := m.ChatKey()
	userKey := chatKey + ":" + formatInt64(m.FromUserID)

	cs := s.chatStats[chatKey]
	if cs == nil {
		cs = &domain.ChatStats{AccountID: m.AccountID, ChatID: m.ChatID}
		s.chatStats[chatKey] = cs
	}
	us := s.userStats[userKey]
	if us == nil {
		us = &domain.UserStats{AccountID: m.AccountID, ChatID: m.ChatID, UserID: m.FromUserID}
		s.userStats[userKey] = us
	}

	tokens := tokenize.Tokens(m.Text)
	meaningfulWords := int64(len(tokens))
	totalWords := int64(len(strings.Fields(tokenize.Normalize(m.Text))))

	updateRollupChat(cs, m, meaningfulWords)
	updateRollupUser(us, m, meaningfulWords)

	s.chatAllWordCount[chatKey] += totalWords
	s.userAllWordCount[userKey] += totalWords
	if cs.MessagesTotal > 0 {
		cs.AvgWordsPerMessage = float64(s.chatAllWordCount[chatKey]) / float64(cs.MessagesTotal)
	}
	if us.MessagesTotal > 0 {
		us.AvgWordsPerMessage = float64(s.userAllWordCount[userKey]) / float64(us.MessagesTotal)
	}

	if _, ok := s.chatWordFreq[chatKey]; !ok {
		s.chatWordFreq[chatKey] = map[string]int64{}
	}
	if _, ok := s.userWordFreq[userKey]; !ok {
		s.userWordFreq[userKey] = map[string]int64{}
	}
	for _, tok := range tokens {
		s.chatWordFreq[chatKey][tok]++
		s.userWordFreq[userKey][tok]++
	}

	if _, ok := s.userActiveDates[userKey]; !ok {
		s.userActiveDates[userKey] = make(map[string]struct{}, 64)
	}
	activeAt := m.Date
	if activeAt.IsZero() {
		activeAt = time.Now().UTC()
	}
	dayKey := activeAt.Format("2006-01-02")
	s.userActiveDates[userKey][dayKey] = struct{}{}
	us.ActiveDays = int64(len(s.userActiveDates[userKey]))
	if us.ActiveDays > 0 {
		us.AvgMessagesPerActiveDay = float64(us.MessagesTotal) / float64(us.ActiveDays)
	}
	if us.MessagesTotal > 0 {
		us.AvgMeaningfulWordsPerMessage = float64(us.MeaningfulWordsTotal) / float64(us.MessagesTotal)
	}
	if all := s.userAllWordCount[userKey]; all > 0 {
		us.MeaningfulWordRate = float64(us.MeaningfulWordsTotal) / float64(all)
	}

	cs.TopWords = topWords(s.chatWordFreq[chatKey], 25)
	us.TopWords = topWords(s.userWordFreq[userKey], 15)
}

func (s *StatsEngine) Chat(accountID string, chatID int64) (domain.ChatStats, bool) {
	k := accountID + ":" + formatInt64(chatID)
	v, ok := s.chatStats[k]
	if !ok {
		return domain.ChatStats{}, false
	}
	return *v, true
}

func (s *StatsEngine) Users(accountID string, chatID int64) []domain.UserStats {
	prefix := accountID + ":" + formatInt64(chatID) + ":"
	out := make([]domain.UserStats, 0, 128)
	for k, st := range s.userStats {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, *st)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].MessagesTotal > out[j].MessagesTotal
	})
	return out
}

func updateRollupChat(cs *domain.ChatStats, m domain.MessageRecord, meaningfulWords int64) {
	cs.MessagesTotal++
	text := strings.TrimSpace(m.Text)
	if text == "" {
		cs.EmptyMessages++
	} else {
		cs.TextMessages++
	}
	msgLen := float64(len([]rune(text)))
	cs.AvgMessageLength += (msgLen - cs.AvgMessageLength) / float64(cs.MessagesTotal)
	cs.MeaningfulWordsTotal += meaningfulWords

	t := m.Date
	if t.IsZero() {
		t = time.Now().UTC()
	}
	cs.MessagesByHour[t.Hour()]++
	cs.MessagesByWeekday[int(t.Weekday())]++
	cs.MessagesByMonth[int(t.Month())-1]++
	if day := t.YearDay(); day > 0 && day < len(cs.MessagesByYearDay) {
		cs.MessagesByYearDay[day]++
	}
	if isNightHour(t.Hour()) {
		cs.NightMessages++
	}
	if isWeekend(t.Weekday()) {
		cs.WeekendMessages++
	}
	if cs.MessagesByDate == nil {
		cs.MessagesByDate = make(map[string]int64, 365)
	}
	cs.MessagesByDate[t.Format("2006-01-02")]++

	emojis := int64(tokenize.CountEmoji(m.Text))
	cs.EmojiCount += emojis
	if m.MediaType != "" {
		cs.MediaCount++
	}
	if m.HasVoice {
		cs.VoiceCount++
	}
	cs.URLsShared += int64(countDistinctNonEmptyStrings(m.URLs))
	if strings.Contains(text, "?") {
		cs.QuestionMessages++
	}
	if strings.Contains(text, "!") {
		cs.ExclamationMessages++
	}
	if m.ToxicityScore >= 0.5 {
		cs.ToxicMessages++
	}
	cs.AvgToxicity += (m.ToxicityScore - cs.AvgToxicity) / float64(cs.MessagesTotal)
	if cs.MessagesTotal > 0 {
		cs.NightSharePct = 100 * float64(cs.NightMessages) / float64(cs.MessagesTotal)
		cs.WeekendSharePct = 100 * float64(cs.WeekendMessages) / float64(cs.MessagesTotal)
	}
	cs.UpdatedAt = time.Now().UTC()
}

func updateRollupUser(us *domain.UserStats, m domain.MessageRecord, meaningfulWords int64) {
	us.MessagesTotal++
	text := strings.TrimSpace(m.Text)
	if text == "" {
		us.EmptyMessages++
	} else {
		us.TextMessages++
	}
	msgLen := float64(len([]rune(text)))
	us.AvgMessageLength += (msgLen - us.AvgMessageLength) / float64(us.MessagesTotal)
	us.MeaningfulWordsTotal += meaningfulWords

	t := m.Date
	if t.IsZero() {
		t = time.Now().UTC()
	}
	us.MessagesByHour[t.Hour()]++
	us.MessagesByWeekday[int(t.Weekday())]++
	us.MessagesByMonth[int(t.Month())-1]++
	if day := t.YearDay(); day > 0 && day < len(us.MessagesByYearDay) {
		us.MessagesByYearDay[day]++
	}
	if isNightHour(t.Hour()) {
		us.NightMessages++
	}
	if isWeekend(t.Weekday()) {
		us.WeekendMessages++
	}
	if us.MessagesByDate == nil {
		us.MessagesByDate = make(map[string]int64, 365)
	}
	us.MessagesByDate[t.Format("2006-01-02")]++

	emojis := int64(tokenize.CountEmoji(m.Text))
	us.EmojiCount += emojis
	if m.MediaType != "" {
		us.MediaCount++
	}
	if m.HasVoice {
		us.VoiceCount++
	}
	us.URLsShared += int64(countDistinctNonEmptyStrings(m.URLs))
	if strings.Contains(text, "?") {
		us.QuestionMessages++
	}
	if strings.Contains(text, "!") {
		us.ExclamationMessages++
	}
	if m.ReplyToMsgID != nil {
		us.ReplyOut++
	}
	if len(m.MentionsUserIDs) > 0 {
		us.MentionOut += int64(len(m.MentionsUserIDs))
	}
	if m.ToxicityScore >= 0.5 {
		us.ToxicMessages++
	}
	us.AvgToxicity += (m.ToxicityScore - us.AvgToxicity) / float64(us.MessagesTotal)
	if us.MessagesTotal > 0 {
		us.NightSharePct = 100 * float64(us.NightMessages) / float64(us.MessagesTotal)
		us.WeekendSharePct = 100 * float64(us.WeekendMessages) / float64(us.MessagesTotal)
	}
	us.UpdatedAt = time.Now().UTC()
}

func topWords(freq map[string]int64, n int) []domain.WordScore {
	if len(freq) == 0 || n <= 0 {
		return nil
	}
	all := make([]domain.WordScore, 0, len(freq))
	for w, c := range freq {
		all = append(all, domain.WordScore{Word: w, Count: c})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Count == all[j].Count {
			return all[i].Word < all[j].Word
		}
		return all[i].Count > all[j].Count
	})
	if len(all) > n {
		all = all[:n]
	}
	return all
}

func isNightHour(hour int) bool {
	return hour >= 0 && hour < 6
}

func isWeekend(weekday time.Weekday) bool {
	return weekday == time.Saturday || weekday == time.Sunday
}

func countDistinctNonEmptyStrings(values []string) int {
	if len(values) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.TrimSpace(strings.ToLower(value))
		if normalized == "" {
			continue
		}
		seen[normalized] = struct{}{}
	}
	return len(seen)
}
