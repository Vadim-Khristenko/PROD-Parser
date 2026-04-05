package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExtractJSONObject(t *testing.T) {
	raw := "```json\n{\"chat_summary\":\"ok\"}\n```"
	b, err := extractJSONObject(raw)
	if err != nil {
		t.Fatalf("extract json: %v", err)
	}
	if !json.Valid(b) {
		t.Fatalf("invalid json: %s", string(b))
	}
}

func TestPickModel(t *testing.T) {
	c := &Client{cfg: Config{
		RoutingMode:    "per-task",
		Model:          "base-model",
		PersonaModel:   "persona-model",
		RelationModel:  "relation-model",
		DigDeeperModel: "deep-model",
	}}

	if got := c.pickModel(taskRelation, false); got != "relation-model" {
		t.Fatalf("pick relation model = %s", got)
	}
	if got := c.pickModel(taskPersona, true); got != "deep-model" {
		t.Fatalf("pick deep persona model = %s", got)
	}
}

func TestNewClientLoadsModelInfoFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "models.json")
	content := `{"models":[{"id":"gpt-4o-mini","max_input_tokens":8192,"max_output_tokens":1024}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write model file: %v", err)
	}

	c, err := NewClient(Config{
		Enabled:       true,
		BaseURL:       "https://example.com/v1",
		Model:         "gpt-4o-mini",
		ModelInfoFile: path,
		Timeout:       2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	cap, ok := c.capacityForModel("gpt-4o-mini")
	if !ok {
		t.Fatal("expected model capacity")
	}
	if cap.InputTokens != 8192 || cap.OutputTokens != 1024 {
		t.Fatalf("unexpected capacity: %+v", cap)
	}
}

func TestCompleteJSONUsesModelInfoEndpointAndCompactsPayload(t *testing.T) {
	var captured chatRequest
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-4o-mini","max_input_tokens":320,"max_output_tokens":64}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"chat_summary\":\"ok\"}"}}]}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	c, err := NewClient(Config{
		Enabled:             true,
		BaseURL:             ts.URL + "/v1",
		Model:               "gpt-4o-mini",
		ModelInfoURL:        "/models/info",
		DefaultInputTokens:  10000,
		DefaultOutputTokens: 1000,
		SafetyMarginTokens:  32,
		MinOutputTokens:     16,
		Timeout:             3 * time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	users := make([]map[string]any, 0, 200)
	for i := 0; i < 200; i++ {
		users = append(users, map[string]any{
			"user_id": i,
			"text":    strings.Repeat("signal ", 30),
		})
	}

	raw, err := c.completeJSON(context.Background(), "gpt-4o-mini", "system prompt", map[string]any{"users": users})
	if err != nil {
		t.Fatalf("complete json: %v", err)
	}
	if !json.Valid(raw) {
		t.Fatalf("invalid response json: %s", string(raw))
	}
	if captured.MaxTokens <= 0 || captured.MaxTokens > 64 {
		t.Fatalf("unexpected max tokens: %d", captured.MaxTokens)
	}
	if len(captured.Messages) != 2 {
		t.Fatalf("unexpected messages len: %d", len(captured.Messages))
	}
	if !strings.Contains(captured.Messages[1].Content, "_input_truncated") {
		t.Fatalf("expected compacted payload marker, got: %s", captured.Messages[1].Content)
	}
}
