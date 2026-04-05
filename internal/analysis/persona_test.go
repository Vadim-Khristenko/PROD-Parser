package analysis

import (
	"testing"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestWordsToInterestsClassifier(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "release", Count: 12},
		{Word: "deploy", Count: 9},
		{Word: "bug", Count: 7},
		{Word: "service", Count: 6},
	})
	if len(interests) == 0 {
		t.Fatal("expected interests")
	}
	found := false
	for _, interest := range interests {
		if interest == "software_engineering" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected software_engineering in interests: %+v", interests)
	}
}

func TestWordsToInterestsFallback(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "ne", Count: 10},
		{Word: "chto", Count: 8},
		{Word: "signal", Count: 7},
	})
	if len(interests) == 0 {
		t.Fatal("expected fallback interests")
	}
}
