package analysis

import (
	"testing"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestBuildSmartWordRanks(t *testing.T) {
	msgs := []domain.MessageRecord{
		{AccountID: "acc", ChatID: 1, MessageID: 1, Date: time.Now().UTC(), FromUserID: 10, Text: "release bug fix"},
		{AccountID: "acc", ChatID: 1, MessageID: 2, Date: time.Now().UTC(), FromUserID: 10, Text: "release release fix"},
		{AccountID: "acc", ChatID: 1, MessageID: 3, Date: time.Now().UTC(), FromUserID: 20, Text: "analytics dashboard report"},
	}
	chat, byUser := BuildSmartWordRanks(msgs, 5)
	if len(chat) == 0 {
		t.Fatal("expected chat smart words")
	}
	if len(byUser[10]) == 0 {
		t.Fatal("expected user smart words")
	}
	if byUser[10][0].Score <= 0 {
		t.Fatalf("expected positive score, got %f", byUser[10][0].Score)
	}
	if byUser[10][0].Score > 100 {
		t.Fatalf("expected bounded smart score <=100, got %f", byUser[10][0].Score)
	}
}

func TestBuildSmartWordRanksPenalizesNoisyCommonToken(t *testing.T) {
	base := time.Now().UTC()
	msgs := []domain.MessageRecord{
		{AccountID: "acc", ChatID: 1, MessageID: 1, Date: base, FromUserID: 10, Text: "ok ok ok deploy kubernetes rollout"},
		{AccountID: "acc", ChatID: 1, MessageID: 2, Date: base.Add(time.Minute), FromUserID: 20, Text: "ok ok release kubernetes cluster"},
		{AccountID: "acc", ChatID: 1, MessageID: 3, Date: base.Add(2 * time.Minute), FromUserID: 30, Text: "ok ok deploy postgres migration"},
	}

	chat, _ := BuildSmartWordRanks(msgs, 5)
	if len(chat) == 0 {
		t.Fatal("expected chat smart words")
	}

	topWords := make([]string, 0, len(chat))
	for _, item := range chat {
		topWords = append(topWords, item.Word)
	}

	if topWords[0] == "ok" {
		t.Fatalf("expected noisy token to be demoted, got top words: %v", topWords)
	}
}
