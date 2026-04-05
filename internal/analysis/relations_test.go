package analysis

import (
	"testing"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func intPtr(v int) *int { return &v }

func TestRelationsEngineSignals(t *testing.T) {
	eng := NewRelationsEngine()
	base := time.Date(2026, time.April, 5, 10, 0, 0, 0, time.UTC)

	eng.AddMessage(domain.MessageRecord{
		AccountID:  "acc1",
		ChatID:     100,
		MessageID:  1,
		Date:       base,
		FromUserID: 10,
		Text:       "release fix now",
	})
	eng.AddMessage(domain.MessageRecord{
		AccountID:       "acc1",
		ChatID:          100,
		MessageID:       2,
		Date:            base.Add(2 * time.Minute),
		FromUserID:      20,
		Text:            "release fix accepted",
		ReplyToMsgID:    intPtr(1),
		MentionsUserIDs: []int64{10},
	})

	edges := eng.TopEdges("acc1", 100, 100)
	if len(edges) == 0 {
		t.Fatal("expected edges")
	}
	var e *domain.RelationEdge
	for i := range edges {
		if edges[i].FromUserID == 20 && edges[i].ToUserID == 10 {
			e = &edges[i]
			break
		}
	}
	if e == nil {
		t.Fatal("edge 20->10 not found")
	}
	if e.Replies != 1 {
		t.Fatalf("Replies=%d, want 1", e.Replies)
	}
	if e.Mentions != 1 {
		t.Fatalf("Mentions=%d, want 1", e.Mentions)
	}
	if e.TemporalAdjacency < 1 {
		t.Fatalf("TemporalAdjacency=%d, want >=1", e.TemporalAdjacency)
	}
	if e.ContextOverlap < 1 {
		t.Fatalf("ContextOverlap=%d, want >=1", e.ContextOverlap)
	}
}
