package analysis

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

var urlRegexp = regexp.MustCompile(`(?i)\b((?:(?:https?|ftps?):\/\/(?:[^\s<>"'@\/]+@)?(?:(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}|\[[\da-fA-F:.]+\]|(?:\d{1,3}\.){3}\d{1,3})(?::\d{1,5})?|[a-z][\w+.-]*:\/\/(?:[^\s<>"'@\/]+@)?(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*|\[[\da-fA-F:.]+\]|(?:\d{1,3}\.){3}\d{1,3})(?::\d{1,5})?|(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}(?::\d{1,5})?)(?:\/[^\s<>"']*)?(?:\?[^\s<>"']*)?(?:#[^\s<>"']*)?)`)

func ExtractURLs(text string) []string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	matches := urlRegexp.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(matches))
	out := make([]string, 0, len(matches))
	for _, raw := range matches {
		norm := normalizeURL(raw)
		if norm == "" {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	sort.Strings(out)
	return out
}

func normalizeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, ".,;:!?)]}")
	raw = strings.TrimLeft(raw, "([{")
	if raw == "" {
		return ""
	}
	candidate := raw
	if strings.HasPrefix(strings.ToLower(candidate), "www.") {
		candidate = "https://" + candidate
	}
	u, err := url.Parse(candidate)
	if err != nil {
		return ""
	}
	if u.Host == "" {
		return ""
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	return u.String()
}

type fileAgg struct {
	CanonicalID string
	FileHash    string
	Count       int64
	IDs         map[string]struct{}
}

type scopeAgg struct {
	GIFs      map[string]*fileAgg
	Stickers  map[string]*fileAgg
	Documents map[string]*fileAgg
	Mentions  map[int64]int64
	URLs      map[string]int64
}

func newScopeAgg() *scopeAgg {
	return &scopeAgg{
		GIFs:      map[string]*fileAgg{},
		Stickers:  map[string]*fileAgg{},
		Documents: map[string]*fileAgg{},
		Mentions:  map[int64]int64{},
		URLs:      map[string]int64{},
	}
}

func BuildContentStats(messages []domain.MessageRecord) domain.ContentStats {
	chat := newScopeAgg()
	byUser := map[int64]*scopeAgg{}
	byTopic := map[string]*scopeAgg{}

	for _, m := range messages {
		uScope := byUser[m.FromUserID]
		if uScope == nil {
			uScope = newScopeAgg()
			byUser[m.FromUserID] = uScope
		}
		var tScope *scopeAgg
		topicID := strings.TrimSpace(m.DerivedTopicID)
		if topicID != "" {
			tScope = byTopic[topicID]
			if tScope == nil {
				tScope = newScopeAgg()
				byTopic[topicID] = tScope
			}
		}

		addFileToScopes(chat, uScope, tScope, m)
		addMentionsToScopes(chat, uScope, tScope, m)
		addURLsToScopes(chat, uScope, tScope, m)
	}

	return domain.ContentStats{
		Files: domain.FileStats{
			Chat:    scopeFiles(chat),
			ByUser:  mapUserFileScopes(byUser),
			ByTopic: mapTopicFileScopes(byTopic),
		},
		Mentions: domain.MentionStats{
			Chat:    sortedMentions(chat.Mentions),
			ByUser:  mapUserMentions(byUser),
			ByTopic: mapTopicMentions(byTopic),
		},
		URLs: domain.URLStats{
			Chat:    sortedURLs(chat.URLs),
			ByUser:  mapUserURLs(byUser),
			ByTopic: mapTopicURLs(byTopic),
		},
	}
}

func addFileToScopes(chat, user, topic *scopeAgg, m domain.MessageRecord) {
	kind := strings.TrimSpace(strings.ToLower(m.MediaType))
	if kind != "gif" && kind != "sticker" && kind != "document" {
		return
	}
	canonical := strings.TrimSpace(m.MediaCanonical)
	if canonical == "" {
		canonical = strings.TrimSpace(m.MediaID)
	}
	if canonical == "" && strings.TrimSpace(m.MediaFileHash) != "" {
		canonical = "hash:" + strings.TrimSpace(m.MediaFileHash)
	}
	if canonical == "" {
		canonical = fmt.Sprintf("msg:%d", m.MessageID)
	}
	fileID := strings.TrimSpace(m.MediaID)
	if fileID == "" {
		fileID = canonical
	}
	updateFileScope(chat, kind, canonical, m.MediaFileHash, fileID)
	updateFileScope(user, kind, canonical, m.MediaFileHash, fileID)
	if topic != nil {
		updateFileScope(topic, kind, canonical, m.MediaFileHash, fileID)
	}
}

func updateFileScope(scope *scopeAgg, kind, canonicalID, fileHash, fileID string) {
	if scope == nil {
		return
	}
	target := scope.Documents
	switch kind {
	case "gif":
		target = scope.GIFs
	case "sticker":
		target = scope.Stickers
	case "document":
		target = scope.Documents
	default:
		return
	}
	item := target[canonicalID]
	if item == nil {
		item = &fileAgg{
			CanonicalID: canonicalID,
			FileHash:    strings.TrimSpace(fileHash),
			IDs:         map[string]struct{}{},
		}
		target[canonicalID] = item
	}
	item.Count++
	if fileID != "" {
		item.IDs[fileID] = struct{}{}
	}
}

func addMentionsToScopes(chat, user, topic *scopeAgg, m domain.MessageRecord) {
	if len(m.MentionsUserIDs) == 0 {
		return
	}
	seen := map[int64]struct{}{}
	for _, uid := range m.MentionsUserIDs {
		if uid <= 0 {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		chat.Mentions[uid]++
		user.Mentions[uid]++
		if topic != nil {
			topic.Mentions[uid]++
		}
	}
}

func addURLsToScopes(chat, user, topic *scopeAgg, m domain.MessageRecord) {
	urls := m.URLs
	if len(urls) == 0 {
		urls = ExtractURLs(m.Text)
	}
	if len(urls) == 0 {
		return
	}
	seen := map[string]struct{}{}
	for _, u := range urls {
		norm := normalizeURL(u)
		if norm == "" {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		chat.URLs[norm]++
		user.URLs[norm]++
		if topic != nil {
			topic.URLs[norm]++
		}
	}
}

func scopeFiles(scope *scopeAgg) domain.FileStatsScope {
	if scope == nil {
		return domain.FileStatsScope{}
	}
	return domain.FileStatsScope{
		GIFs:      sortedFiles(scope.GIFs),
		Stickers:  sortedFiles(scope.Stickers),
		Documents: sortedFiles(scope.Documents),
	}
}

func sortedFiles(m map[string]*fileAgg) []domain.FileStat {
	if len(m) == 0 {
		return nil
	}
	out := make([]domain.FileStat, 0, len(m))
	for _, v := range m {
		ids := make([]string, 0, len(v.IDs))
		for id := range v.IDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		out = append(out, domain.FileStat{
			CanonicalID: v.CanonicalID,
			FileHash:    v.FileHash,
			IDs:         ids,
			Count:       v.Count,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].CanonicalID < out[j].CanonicalID
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func sortedMentions(m map[int64]int64) []domain.CountByUser {
	if len(m) == 0 {
		return nil
	}
	out := make([]domain.CountByUser, 0, len(m))
	for uid, count := range m {
		out = append(out, domain.CountByUser{UserID: uid, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].UserID < out[j].UserID
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func sortedURLs(m map[string]int64) []domain.CountByString {
	if len(m) == 0 {
		return nil
	}
	out := make([]domain.CountByString, 0, len(m))
	for value, count := range m {
		out = append(out, domain.CountByString{Value: value, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Value < out[j].Value
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func mapUserFileScopes(m map[int64]*scopeAgg) map[int64]domain.FileStatsScope {
	if len(m) == 0 {
		return nil
	}
	out := make(map[int64]domain.FileStatsScope, len(m))
	for uid, scope := range m {
		out[uid] = scopeFiles(scope)
	}
	return out
}

func mapTopicFileScopes(m map[string]*scopeAgg) map[string]domain.FileStatsScope {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]domain.FileStatsScope, len(m))
	for topicID, scope := range m {
		out[topicID] = scopeFiles(scope)
	}
	return out
}

func mapUserMentions(m map[int64]*scopeAgg) map[int64][]domain.CountByUser {
	if len(m) == 0 {
		return nil
	}
	out := make(map[int64][]domain.CountByUser, len(m))
	for uid, scope := range m {
		out[uid] = sortedMentions(scope.Mentions)
	}
	return out
}

func mapTopicMentions(m map[string]*scopeAgg) map[string][]domain.CountByUser {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]domain.CountByUser, len(m))
	for topicID, scope := range m {
		out[topicID] = sortedMentions(scope.Mentions)
	}
	return out
}

func mapUserURLs(m map[int64]*scopeAgg) map[int64][]domain.CountByString {
	if len(m) == 0 {
		return nil
	}
	out := make(map[int64][]domain.CountByString, len(m))
	for uid, scope := range m {
		out[uid] = sortedURLs(scope.URLs)
	}
	return out
}

func mapTopicURLs(m map[string]*scopeAgg) map[string][]domain.CountByString {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]domain.CountByString, len(m))
	for topicID, scope := range m {
		out[topicID] = sortedURLs(scope.URLs)
	}
	return out
}
