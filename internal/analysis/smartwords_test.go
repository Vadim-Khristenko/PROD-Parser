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
}
