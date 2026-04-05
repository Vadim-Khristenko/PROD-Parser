package index

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/tokenize"
)

type tokenPosting map[uint64]uint16

// InvertedIndex keeps token -> message internal IDs map for very fast in-process search.
type InvertedIndex struct {
	mu       sync.RWMutex
	postings map[string]tokenPosting
}

func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{postings: make(map[string]tokenPosting, 1<<14)}
}

func (i *InvertedIndex) Add(id uint64, text string) {
	tokens := tokenize.Tokens(text)
	if len(tokens) == 0 {
		return
	}
	freq := make(map[string]uint16, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for t, tf := range freq {
		p, ok := i.postings[t]
		if !ok {
			p = make(tokenPosting)
			i.postings[t] = p
		}
		p[id] = tf
	}
}

func (i *InvertedIndex) Search(query string, limit int) []domain.SearchHit {
	tokens := tokenize.Tokens(query)
	if len(tokens) == 0 {
		return nil
	}
	if limit <= 0 {
		limit = 20
	}

	i.mu.RLock()
	scores := map[uint64]float64{}
	for _, t := range tokens {
		for id, tf := range i.postings[t] {
			scores[id] += float64(tf)
		}
	}
	i.mu.RUnlock()

	if len(scores) == 0 {
		return nil
	}
	hits := make([]domain.SearchHit, 0, len(scores))
	for id, score := range scores {
		hits = append(hits, domain.SearchHit{InternalID: id, Score: score})
	}
	sort.Slice(hits, func(a, b int) bool {
		if hits[a].Score == hits[b].Score {
			return hits[a].InternalID > hits[b].InternalID
		}
		return hits[a].Score > hits[b].Score
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

func (i *InvertedIndex) Save(path string) error {
	i.mu.RLock()
	data := make(map[string]tokenPosting, len(i.postings))
	for k, v := range i.postings {
		inner := make(tokenPosting, len(v))
		for id, tf := range v {
			inner[id] = tf
		}
		data[k] = inner
	}
	i.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	defer zw.Close()
	enc := json.NewEncoder(zw)
	return enc.Encode(data)
}

func (i *InvertedIndex) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()

	data := map[string]tokenPosting{}
	dec := json.NewDecoder(zr)
	if err := dec.Decode(&data); err != nil {
		return err
	}
	i.mu.Lock()
	i.postings = data
	i.mu.Unlock()
	return nil
}
