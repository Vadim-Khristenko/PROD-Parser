package tokenize

import "testing"

func TestTokens(t *testing.T) {
	in := "Привет, это test_message #release и bugfix!"
	tokens := Tokens(in)
	if len(tokens) == 0 {
		t.Fatal("expected tokens")
	}
	wantHas := map[string]bool{
		"привет":       true,
		"test_message": true,
		"#release":     true,
		"bugfix":       true,
	}
	for _, tok := range tokens {
		delete(wantHas, tok)
	}
	if len(wantHas) != 0 {
		t.Fatalf("missing tokens: %+v", wantHas)
	}
}

func TestCountEmoji(t *testing.T) {
	in := "ok 😀👍 text"
	if got := CountEmoji(in); got != 2 {
		t.Fatalf("CountEmoji=%d, want 2", got)
	}
}
