package tokenize

import (
	"strings"
	"unicode"
)

var stopWords = map[string]struct{}{
	"и": {}, "в": {}, "во": {}, "на": {}, "с": {}, "со": {}, "по": {}, "за": {}, "к": {}, "ко": {}, "о": {}, "об": {},
	"the": {}, "a": {}, "an": {}, "in": {}, "on": {}, "at": {}, "to": {}, "for": {}, "of": {}, "and": {}, "or": {},
}

func Normalize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
		case r == '_', r == '#':
			b.WriteRune(r)
		default:
			b.WriteRune(' ')
		}
	}
	return strings.TrimSpace(b.String())
}

func Tokens(s string) []string {
	norm := Normalize(s)
	if norm == "" {
		return nil
	}
	parts := strings.Fields(norm)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if len([]rune(p)) < 2 {
			continue
		}
		if _, ok := stopWords[p]; ok {
			continue
		}
		out = append(out, p)
	}
	return out
}

func CountEmoji(s string) int {
	count := 0
	for _, r := range s {
		if isEmojiRune(r) {
			count++
		}
	}
	return count
}

func isEmojiRune(r rune) bool {
	return (r >= 0x1F300 && r <= 0x1FAFF) || (r >= 0x2600 && r <= 0x27BF)
}
