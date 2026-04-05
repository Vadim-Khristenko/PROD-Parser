package telegramingest

import (
	"testing"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

func TestNewCommandAuthorizer(t *testing.T) {
	a, err := newCommandAuthorizer("owner", 100, nil, nil)
	if err != nil {
		t.Fatalf("owner authorizer: %v", err)
	}
	if !a.IsAllowed(100) || a.IsAllowed(200) {
		t.Fatalf("owner policy mismatch")
	}

	a, err = newCommandAuthorizer("admins", 10, []int64{20, 30}, nil)
	if err != nil {
		t.Fatalf("admins authorizer: %v", err)
	}
	if !a.IsAllowed(10) || !a.IsAllowed(20) || a.IsAllowed(99) {
		t.Fatalf("admins policy mismatch")
	}

	a, err = newCommandAuthorizer("users", 0, nil, nil)
	if err != nil {
		t.Fatalf("users authorizer: %v", err)
	}
	if !a.IsAllowed(1) || !a.IsAllowed(999) {
		t.Fatalf("users policy mismatch")
	}

	a, err = newCommandAuthorizer("ids", 0, nil, []int64{7, 8})
	if err != nil {
		t.Fatalf("ids authorizer: %v", err)
	}
	if !a.IsAllowed(7) || a.IsAllowed(9) {
		t.Fatalf("ids policy mismatch")
	}
}

func TestParsePollCommand(t *testing.T) {
	cmd, ok := parsePollCommand("/stop@mybot now", "/")
	if !ok {
		t.Fatal("expected command")
	}
	if cmd.Name != "stop" {
		t.Fatalf("cmd name = %s, want stop", cmd.Name)
	}
	if len(cmd.Args) != 1 || cmd.Args[0] != "now" {
		t.Fatalf("cmd args mismatch: %+v", cmd.Args)
	}

	if _, ok := parsePollCommand("hello", "/"); ok {
		t.Fatal("unexpected command for plain text")
	}
}

func TestApplyPollCommand(t *testing.T) {
	state := &pollControlState{}

	handled, status := applyPollCommand(state, pollCommand{Name: "pause"})
	if !handled || status == "" || !state.Paused {
		t.Fatalf("pause failed: handled=%v status=%q paused=%v", handled, status, state.Paused)
	}

	handled, status = applyPollCommand(state, pollCommand{Name: "resume"})
	if !handled || status == "" || state.Paused {
		t.Fatalf("resume failed: handled=%v status=%q paused=%v", handled, status, state.Paused)
	}

	handled, status = applyPollCommand(state, pollCommand{Name: "stop"})
	if !handled || status == "" || !state.Stop {
		t.Fatalf("stop failed: handled=%v status=%q stop=%v", handled, status, state.Stop)
	}
}

func TestParseUserIDs(t *testing.T) {
	ids, err := parseUserIDs("1, 2,2, 3")
	if err != nil {
		t.Fatalf("parse ids: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids len = %d, want 3", len(ids))
	}
}

func TestApplyCommandsOnBatch(t *testing.T) {
	authorizer, err := newCommandAuthorizer("ids", 0, nil, []int64{42})
	if err != nil {
		t.Fatalf("authorizer: %v", err)
	}
	state := &pollControlState{}
	batch := []domain.MessageRecord{
		{MessageID: 1, FromUserID: 11, Text: "/stop"},
		{MessageID: 2, FromUserID: 42, Text: "/pause"},
		{MessageID: 3, FromUserID: 42, Text: "regular message"},
		{MessageID: 4, FromUserID: 42, Text: "/resume"},
		{MessageID: 5, FromUserID: 42, Text: "/stop"},
	}

	toIngest, handled := applyCommandsOnBatch(batch, "/", authorizer, state)
	if len(handled) != 3 {
		t.Fatalf("handled len = %d, want 3", len(handled))
	}
	if !state.Stop {
		t.Fatalf("expected stop=true")
	}
	if state.Paused {
		t.Fatalf("expected paused=false after resume")
	}
	if len(toIngest) == 0 {
		t.Fatalf("expected messages to ingest")
	}
}
