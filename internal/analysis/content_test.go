package analysis

import (
	"testing"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestExtractURLs(t *testing.T) {
	in := "See https://Example.com/a?b=1 and also www.test.io/path."
	urls := ExtractURLs(in)
	if len(urls) != 2 {
		t.Fatalf("urls len = %d, want 2 (%v)", len(urls), urls)
	}
}

func TestBuildContentStats(t *testing.T) {
	base := time.Date(2026, time.April, 5, 10, 0, 0, 0, time.UTC)
	msgs := []domain.MessageRecord{
		{
			AccountID:       "acc1",
			ChatID:          100,
			MessageID:       1,
			Date:            base,
			FromUserID:      10,
			DerivedTopicID:  "t1",
			MediaType:       "gif",
			MediaID:         "m1",
			MediaCanonical:  "canon-a",
			MediaFileHash:   "hash-a",
			MentionsUserIDs: []int64{20, 20},
			URLs:            []string{"https://example.com/x"},
		},
		{
			AccountID:       "acc1",
			ChatID:          100,
			MessageID:       2,
			Date:            base.Add(time.Minute),
			FromUserID:      11,
			DerivedTopicID:  "t1",
			MediaType:       "gif",
			MediaID:         "m2",
			MediaCanonical:  "canon-a",
			MediaFileHash:   "hash-a",
			MentionsUserIDs: []int64{20},
			Text:            "link https://example.com/x",
		},
		{
			AccountID:      "acc1",
			ChatID:         100,
			MessageID:      3,
			Date:           base.Add(2 * time.Minute),
			FromUserID:     10,
			DerivedTopicID: "t2",
			MediaType:      "document",
			MediaID:        "doc1",
			MediaFileHash:  "hash-doc",
			URLs:           []string{"https://docs.example.com/a"},
		},
	}

	stats := BuildContentStats(msgs)
	if len(stats.Files.Chat.GIFs) == 0 || stats.Files.Chat.GIFs[0].Count != 2 {
		t.Fatalf("unexpected gif stats: %+v", stats.Files.Chat.GIFs)
	}
	if len(stats.Mentions.Chat) == 0 || stats.Mentions.Chat[0].UserID != 20 || stats.Mentions.Chat[0].Count != 2 {
		t.Fatalf("unexpected mention stats: %+v", stats.Mentions.Chat)
	}
	if len(stats.URLs.Chat) == 0 || stats.URLs.Chat[0].Count < 2 {
		t.Fatalf("unexpected url stats: %+v", stats.URLs.Chat)
	}
	if len(stats.Files.ByTopic["t1"].GIFs) == 0 {
		t.Fatalf("expected topic t1 gif stats")
	}
}
