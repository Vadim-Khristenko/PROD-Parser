package analysis

import "strings"

var toxicLexicon = []string{
	"идиот", "туп", "дебил", "мраз", "ненавиж", "убью", "заткнись", "пошел",
	"idiot", "stupid", "moron", "hate", "kill", "shut up", "trash", "loser",
}

// ToxicityScore calculates rough lexical toxicity in [0..1].
func ToxicityScore(text string) float64 {
	norm := strings.ToLower(strings.TrimSpace(text))
	if norm == "" {
		return 0
	}
	hits := 0
	for _, marker := range toxicLexicon {
		if strings.Contains(norm, marker) {
			hits++
		}
	}
	if hits == 0 {
		return 0
	}
	if hits >= 4 {
		return 1
	}
	return float64(hits) / 4
}
