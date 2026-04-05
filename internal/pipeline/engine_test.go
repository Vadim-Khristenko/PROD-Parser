package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func ptr(v int) *int { return &v }

func TestEngineParticipantsAndRange(t *testing.T) {
	root := t.TempDir()
	eng, err := NewEngine(root, zap.NewNop())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer eng.Close()

	base := time.Date(2026, time.April, 5, 10, 0, 0, 0, time.UTC)
	batch := []domain.MessageRecord{
		{
			AccountID:       "acc1",
			ChatID:          100,
			MessageID:       1,
			Date:            base,
			FromUserID:      11,
			FromUsername:    "alice",
			FromDisplayName: "Alice",
			Text:            "release plan",
		},
		{
			AccountID:       "acc1",
			ChatID:          100,
			MessageID:       2,
			Date:            base.Add(5 * time.Minute),
			FromUserID:      22,
			FromUsername:    "bob",
			FromDisplayName: "Bob",
			Text:            "agree release",
			ReplyToMsgID:    ptr(1),
			MentionsUserIDs: []int64{11},
		},
	}
	if err := eng.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("process batch: %v", err)
	}

	outDir := filepath.Join(root, "users")
	files, err := eng.SaveParticipantsSnapshots("acc1", 100, outDir, 50)
	if err != nil {
		t.Fatalf("save participants: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("participant files=%d, want 2", len(files))
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("expected file %s: %v", f, err)
		}
	}

	insights, err := eng.BuildChatInsights("acc1", 100)
	if err != nil {
		t.Fatalf("build insights: %v", err)
	}
	if len(insights.Users) != 2 {
		t.Fatalf("users=%d, want 2", len(insights.Users))
	}
	if insights.Users[0].MessageSharePct <= 0 || insights.Users[0].ActivityScore <= 0 {
		t.Fatalf("activity metrics not populated: %+v", insights.Users[0])
	}
	if len(insights.Users[0].SmartWords) == 0 {
		t.Fatalf("smart words are empty: %+v", insights.Users[0])
	}

	msgs, err := eng.MessagesInRange("acc1", 100, base.Add(1*time.Minute), base.Add(10*time.Minute), 10)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(msgs) != 1 || msgs[0].MessageID != 2 {
		t.Fatalf("unexpected range messages: %+v", msgs)
	}
}
