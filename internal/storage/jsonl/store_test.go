package jsonl

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestStoreRotatesByMessageCount(t *testing.T) {
	root := t.TempDir()
	store := NewStore(root)
	store.SetMaxMessagesPerFile(2)
	defer func() {
		_ = store.Close()
	}()

	base := time.Date(2026, time.April, 5, 12, 0, 0, 0, time.UTC)
	batch := make([]domain.MessageRecord, 0, 5)
	for i := 1; i <= 5; i++ {
		batch = append(batch, domain.MessageRecord{
			InternalID: uint64(i),
			AccountID:  "acc1",
			ChatID:     42,
			MessageID:  i,
			Date:       base.Add(time.Duration(i) * time.Second),
			FromUserID: 100,
			Text:       "message",
		})
	}

	ptrs, err := store.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if len(ptrs) != 5 {
		t.Fatalf("pointers len=%d, want 5", len(ptrs))
	}

	paths := map[string]struct{}{}
	for _, p := range ptrs {
		paths[p.FilePath] = struct{}{}
	}
	if len(paths) != 3 {
		t.Fatalf("segment files=%d, want 3", len(paths))
	}

	dir := filepath.Join(root, "jsonl", "acc1", "42")
	for _, name := range []string{"messages_000001.jsonl", "messages_000002.jsonl", "messages_000003.jsonl"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected segment %s: %v", name, err)
		}
	}

	msgs, err := store.ReadChat("acc1", 42)
	if err != nil {
		t.Fatalf("read chat: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("messages len=%d, want 5", len(msgs))
	}
	for i := 0; i < len(msgs); i++ {
		if got, want := msgs[i].MessageID, i+1; got != want {
			t.Fatalf("message[%d].id=%d, want %d", i, got, want)
		}
	}
}

func TestStoreMaxMessageID(t *testing.T) {
	root := t.TempDir()
	store := NewStore(root)
	store.SetMaxMessagesPerFile(2)
	defer func() {
		_ = store.Close()
	}()

	base := time.Date(2026, time.April, 5, 12, 0, 0, 0, time.UTC)
	batch := []domain.MessageRecord{
		{InternalID: 1, AccountID: "acc1", ChatID: 77, MessageID: 10, Date: base, FromUserID: 1, Text: "a"},
		{InternalID: 2, AccountID: "acc1", ChatID: 77, MessageID: 11, Date: base.Add(time.Second), FromUserID: 1, Text: "b"},
		{InternalID: 3, AccountID: "acc1", ChatID: 77, MessageID: 25, Date: base.Add(2 * time.Second), FromUserID: 1, Text: "c"},
	}
	if _, err := store.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	maxID, err := store.MaxMessageID("acc1", 77)
	if err != nil {
		t.Fatalf("max message id: %v", err)
	}
	if maxID != 25 {
		t.Fatalf("max message id=%d, want 25", maxID)
	}
}
