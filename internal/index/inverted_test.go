package index

import (
	"path/filepath"
	"testing"
)

func TestInvertedIndexSearchAndPersistence(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add(1, "release bug bug")
	idx.Add(2, "release candidate")
	idx.Add(3, "other context")

	hits := idx.Search("release bug", 10)
	if len(hits) < 2 {
		t.Fatalf("expected at least 2 hits, got %d", len(hits))
	}
	if hits[0].InternalID != 1 {
		t.Fatalf("top hit internal_id=%d, want 1", hits[0].InternalID)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "inv.json.gz")
	if err := idx.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded := NewInvertedIndex()
	if err := loaded.Load(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	hits2 := loaded.Search("release bug", 10)
	if len(hits2) == 0 || hits2[0].InternalID != 1 {
		t.Fatalf("loaded index returned unexpected hits: %+v", hits2)
	}
}
