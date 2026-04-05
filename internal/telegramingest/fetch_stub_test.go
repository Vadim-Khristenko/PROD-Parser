//go:build !telegram

package telegramingest

import (
	"context"
	"strings"
	"testing"
)

func TestRunStubDisabled(t *testing.T) {
	_, err := Run(context.Background(), Options{}, Credentials{}, nil)
	if err == nil {
		t.Fatal("expected disabled error")
	}
	if !strings.Contains(err.Error(), "build") {
		t.Fatalf("unexpected error: %v", err)
	}
}
