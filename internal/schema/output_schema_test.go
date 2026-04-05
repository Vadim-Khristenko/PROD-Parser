package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteOutputSchemas(t *testing.T) {
	t.Parallel()

	outDir := t.TempDir()
	paths, err := WriteOutputSchemas(outDir, true)
	if err != nil {
		t.Fatalf("WriteOutputSchemas() error = %v", err)
	}
	if len(paths) < 5 {
		t.Fatalf("expected at least 5 schema files, got %d", len(paths))
	}

	bundlePath := filepath.Join(outDir, "output_format.schema.bundle.json")
	bundleRaw, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}

	var parsed Bundle
	if err := json.Unmarshal(bundleRaw, &parsed); err != nil {
		t.Fatalf("unmarshal bundle: %v", err)
	}
	if len(parsed.Schemas) == 0 {
		t.Fatal("expected schemas in bundle")
	}
	if _, ok := parsed.Schemas["chat_insights"]; !ok {
		t.Fatal("missing chat_insights schema")
	}
	if _, ok := parsed.Schemas["participant_snapshot"]; !ok {
		t.Fatal("missing participant_snapshot schema")
	}
}
