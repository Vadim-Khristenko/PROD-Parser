package analysis

import (
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

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
	chatContext := make(map[string]int64, 1024)
	userContext := make(map[int64]map[string]int64, 1024)

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
		if _, ok := userContext[m.FromUserID]; !ok {
			userContext[m.FromUserID] = make(map[string]int64, 128)
		}
		seen := make(map[string]struct{}, len(tokens))
		uniqueTokens := make([]string, 0, len(tokens))
		for _, tok := range tokens {
			chatTF[tok]++
			userTF[m.FromUserID][tok]++
			if _, ok := seen[tok]; ok {
				continue
			}
			seen[tok] = struct{}{}
			uniqueTokens = append(uniqueTokens, tok)
			docFreq[tok]++
		}
		if len(uniqueTokens) > 1 {
			neighbors := int64(len(uniqueTokens) - 1)
			for _, tok := range uniqueTokens {
				chatContext[tok] += neighbors
				userContext[m.FromUserID][tok] += neighbors
			}
		}
	}

	chatSmart := rankSmart(chatTF, docFreq, chatContext, totalDocs, topN)
	byUser := make(map[int64][]domain.WeightedWordScore, len(userTF))
	for uid, tf := range userTF {
		byUser[uid] = rankSmart(tf, docFreq, userContext[uid], totalDocs, topN)
	}
	return chatSmart, byUser
}

func rankSmart(tf map[string]int64, df map[string]int64, context map[string]int64, totalDocs int64, topN int) []domain.WeightedWordScore {
	if len(tf) == 0 {
		return nil
	}
	if totalDocs <= 0 {
		totalDocs = 1
	}
	maxTF := int64(1)
	maxContext := int64(1)
	for word, count := range tf {
		if count > maxTF {
			maxTF = count
		}
		if context[word] > maxContext {
			maxContext = context[word]
		}
	}

	idfNormDen := math.Log1p(float64(totalDocs))
	if idfNormDen <= 0 {
		idfNormDen = 1
	}

	out := make([]domain.WeightedWordScore, 0, len(tf))
	for word, count := range tf {
		d := df[word]
		if d <= 0 {
			d = 1
		}

		tfNorm := clamp01(math.Log1p(float64(count)) / math.Log1p(float64(maxTF)))
		specificity := clamp01(math.Log1p(float64(totalDocs)/float64(d)) / idfNormDen)
		focus := clamp01(math.Log1p(float64(count)/float64(d)) / math.Log1p(4))
		contextRichness := clamp01(math.Log1p(float64(context[word])) / math.Log1p(float64(maxContext)))
		docRatio := float64(d) / float64(totalDocs)
		rarity := clamp01(1 - docRatio)
		coverageBalance := clamp01(1 - math.Abs(docRatio-0.12)/0.25)
		lexical := lexicalQuality(word)

		// Composite 0..100 smartness: specificity + concentration + contextual richness + lexical quality.
		base := 0.22*specificity + 0.18*tfNorm + 0.16*focus + 0.16*contextRichness + 0.08*coverageBalance + 0.12*lexical + 0.08*rarity
		score01 := clamp01(base * (0.2 + 0.8*rarity))
		score := score01 * 100
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

func lexicalQuality(word string) float64 {
	word = stringsTrimSpaceLower(word)
	if word == "" {
		return 0
	}

	runeLen := utf8.RuneCountInString(word)
	if runeLen <= 1 {
		return 0
	}

	letters := 0
	digits := 0
	vowels := 0
	unique := make(map[rune]struct{}, runeLen)
	for _, r := range word {
		if unicode.IsLetter(r) {
			letters++
			if isVowelRune(r) {
				vowels++
			}
		}
		if unicode.IsDigit(r) {
			digits++
		}
		unique[r] = struct{}{}
	}

	alphaRatio := clamp01(float64(letters) / float64(runeLen))
	digitRatio := clamp01(float64(digits) / float64(runeLen))
	uniqueRatio := clamp01(float64(len(unique)) / float64(runeLen))
	lengthBalance := clamp01(1 - math.Abs(float64(runeLen)-8)/10)

	vowelScore := 0.35
	if letters > 0 {
		vr := float64(vowels) / float64(letters)
		switch {
		case vr >= 0.20 && vr <= 0.75:
			vowelScore = 1
		case vr >= 0.10 && vr <= 0.85:
			vowelScore = 0.65
		}
	}

	quality := 0.30*lengthBalance + 0.28*alphaRatio + 0.18*uniqueRatio + 0.14*vowelScore + 0.10*(1-digitRatio)
	if runeLen <= 2 {
		quality *= 0.75
	}
	return clamp01(quality)
}

func isVowelRune(r rune) bool {
	switch unicode.ToLower(r) {
	case 'a', 'e', 'i', 'o', 'u', 'y', 'а', 'е', 'ё', 'и', 'о', 'у', 'ы', 'э', 'ю', 'я':
		return true
	default:
		return false
	}
}

func stringsTrimSpaceLower(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
