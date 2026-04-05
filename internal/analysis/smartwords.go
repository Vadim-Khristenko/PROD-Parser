package analysis

import (
	"math"
	"sort"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/tokenize"
)

func BuildSmartWordRanks(messages []domain.MessageRecord, topN int) ([]domain.WeightedWordScore, map[int64][]domain.WeightedWordScore) {
	if topN <= 0 {
		topN = 20
	}
	chatTF := make(map[string]int64, 1024)
	userTF := make(map[int64]map[string]int64, 1024)
	docFreq := make(map[string]int64, 1024)

	totalDocs := int64(0)
	for _, m := range messages {
		tokens := tokenize.Tokens(m.Text)
		if len(tokens) == 0 {
			continue
		}
		totalDocs++
		if _, ok := userTF[m.FromUserID]; !ok {
			userTF[m.FromUserID] = make(map[string]int64, 128)
		}
		seen := make(map[string]struct{}, len(tokens))
		for _, tok := range tokens {
			chatTF[tok]++
			userTF[m.FromUserID][tok]++
			if _, ok := seen[tok]; ok {
				continue
			}
			seen[tok] = struct{}{}
			docFreq[tok]++
		}
	}

	chatSmart := rankSmart(chatTF, docFreq, totalDocs, topN)
	byUser := make(map[int64][]domain.WeightedWordScore, len(userTF))
	for uid, tf := range userTF {
		byUser[uid] = rankSmart(tf, docFreq, totalDocs, topN)
	}
	return chatSmart, byUser
}

func rankSmart(tf map[string]int64, df map[string]int64, totalDocs int64, topN int) []domain.WeightedWordScore {
	if len(tf) == 0 {
		return nil
	}
	if totalDocs <= 0 {
		totalDocs = 1
	}
	out := make([]domain.WeightedWordScore, 0, len(tf))
	for word, count := range tf {
		d := df[word]
		idf := math.Log(1 + float64(totalDocs)/float64(1+d))
		score := float64(count) * idf
		out = append(out, domain.WeightedWordScore{
			Word:  word,
			Count: count,
			Score: score,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			if out[i].Count == out[j].Count {
				return out[i].Word < out[j].Word
			}
			return out[i].Count > out[j].Count
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > topN {
		out = out[:topN]
	}
	return out
}
