package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestParseChatDirName(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		wantID int64
		wantOK bool
	}{
		{name: "valid", raw: "2080029993_users", wantID: 2080029993, wantOK: true},
		{name: "missing suffix", raw: "2080029993", wantID: 0, wantOK: false},
		{name: "zero", raw: "0_users", wantID: 0, wantOK: false},
		{name: "invalid", raw: "abc_users", wantID: 0, wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseChatDirName(tc.raw)
			if got != tc.wantID || ok != tc.wantOK {
				t.Fatalf("parseChatDirName(%q) = (%d, %v), want (%d, %v)", tc.raw, got, ok, tc.wantID, tc.wantOK)
			}
		})
	}
}

func TestIsSafeAccountID(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		wantOK bool
	}{
		{name: "simple", raw: "acc1", wantOK: true},
		{name: "with symbols", raw: "prod-alpha_1.2", wantOK: true},
		{name: "empty", raw: "", wantOK: false},
		{name: "path traversal", raw: "../acc1", wantOK: false},
		{name: "slash", raw: "acc/1", wantOK: false},
		{name: "space", raw: "acc 1", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isSafeAccountID(tc.raw)
			if got != tc.wantOK {
				t.Fatalf("isSafeAccountID(%q) = %v, want %v", tc.raw, got, tc.wantOK)
			}
		})
	}
}

func TestRootServesIndexWithoutRedirect(t *testing.T) {
	h, err := NewHandler(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want %d", rr.Code, http.StatusOK)
	}
	if location := rr.Header().Get("Location"); strings.TrimSpace(location) != "" {
		t.Fatalf("unexpected redirect to %q", location)
	}
	if !strings.Contains(strings.ToLower(rr.Body.String()), "<!doctype html>") {
		t.Fatalf("unexpected body, index html marker missing")
	}
}
