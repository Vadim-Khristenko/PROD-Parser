package analysis

import (
	"testing"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestStatsEngineTemporalBreakdowns(t *testing.T) {
	eng := NewStatsEngine()
	d := time.Date(2026, time.April, 5, 14, 30, 0, 0, time.UTC)
	eng.Add(domain.MessageRecord{
		AccountID:     "acc1",
		ChatID:        100,
		MessageID:     1,
		Date:          d,
		FromUserID:    77,
		Text:          "release release bug",
		ToxicityScore: 0.6,
	})

	chat, ok := eng.Chat("acc1", 100)
	if !ok {
		t.Fatal("chat stats not found")
	}
	if chat.MessagesByHour[14] != 1 {
		t.Fatalf("MessagesByHour[14]=%d, want 1", chat.MessagesByHour[14])
	}
	if chat.MessagesByWeekday[int(d.Weekday())] != 1 {
		t.Fatalf("MessagesByWeekday[%d]=%d, want 1", d.Weekday(), chat.MessagesByWeekday[int(d.Weekday())])
	}
	if chat.MessagesByMonth[int(d.Month())-1] != 1 {
		t.Fatalf("MessagesByMonth[%d]=%d, want 1", d.Month()-1, chat.MessagesByMonth[int(d.Month())-1])
	}
	if chat.MessagesByYearDay[d.YearDay()] != 1 {
		t.Fatalf("MessagesByYearDay[%d]=%d, want 1", d.YearDay(), chat.MessagesByYearDay[d.YearDay()])
	}
	if chat.MessagesByDate["2026-04-05"] != 1 {
		t.Fatalf("MessagesByDate[2026-04-05]=%d, want 1", chat.MessagesByDate["2026-04-05"])
	}
	if chat.ToxicMessages != 1 {
		t.Fatalf("ToxicMessages=%d, want 1", chat.ToxicMessages)
	}
	if chat.MeaningfulWordsTotal == 0 {
		t.Fatalf("MeaningfulWordsTotal=%d, want >0", chat.MeaningfulWordsTotal)
	}
	if len(chat.TopWords) == 0 || chat.TopWords[0].Word != "release" {
		t.Fatalf("unexpected top words: %+v", chat.TopWords)
	}
}
