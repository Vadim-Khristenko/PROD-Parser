package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestLoggerWritesJSONFile(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "prod-parser.log")
	logger, cleanup, err := New(Options{
		Level:          "info",
		ConsoleEnabled: false,
		FileEnabled:    true,
		FilePath:       logPath,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("test-entry", zap.String("scope", "unit"))
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "\"msg\":\"test-entry\"") {
		t.Fatalf("expected message in log, got: %s", content)
	}
	if !strings.Contains(content, "\"scope\":\"unit\"") {
		t.Fatalf("expected field in log, got: %s", content)
	}
}
