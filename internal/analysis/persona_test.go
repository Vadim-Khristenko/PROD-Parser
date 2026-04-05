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

func TestWordsToInterestsNoSetMatchReturnsEmpty(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "ne", Count: 10},
		{Word: "chto", Count: 8},
		{Word: "signal", Count: 7},
	})
	if len(interests) != 0 {
		t.Fatalf("expected no interests when nothing matches interest sets, got: %+v", interests)
	}
}

func TestWordsToInterestsRecognizesAutomationSet(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "автоматизация", Count: 9},
		{Word: "workflow", Count: 6},
		{Word: "zapier", Count: 3},
	})

	found := false
	for _, interest := range interests {
		if interest == "automation_productivity" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected automation_productivity in interests: %+v", interests)
	}
}

func TestWordsToInterestsRecognizesStemmedCareerTokens(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "собеседование", Count: 8},
		{Word: "вакансии", Count: 7},
		{Word: "резюме", Count: 5},
	})

	found := false
	for _, interest := range interests {
		if interest == "career_hiring" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected career_hiring in interests: %+v", interests)
	}
}

func TestWordsToInterestsRecognizesAnimeFandomTokens(t *testing.T) {
	interests := wordsToInterests([]domain.WordScore{
		{Word: "фембой", Count: 6},
		{Word: "сенпай", Count: 4},
		{Word: "косплей", Count: 3},
	})

	found := false
	for _, interest := range interests {
		if interest == "anime_fandom" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected anime_fandom in interests: %+v", interests)
	}
}
