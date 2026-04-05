package telegramingest

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type commandAccessMode string

const (
	commandAccessOwner  commandAccessMode = "owner"
	commandAccessAdmins commandAccessMode = "admins"
	commandAccessUsers  commandAccessMode = "users"
	commandAccessIDs    commandAccessMode = "ids"
)

type commandAuthorizer struct {
	mode     commandAccessMode
	allowAll bool
	allowed  map[int64]struct{}
}

func newCommandAuthorizer(policy string, ownerID int64, adminIDs []int64, explicitIDs []int64) (commandAuthorizer, error) {
	mode, err := normalizeCommandPolicy(policy)
	if err != nil {
		return commandAuthorizer{}, err
	}
	a := commandAuthorizer{
		mode:    mode,
		allowed: map[int64]struct{}{},
	}
	switch mode {
	case commandAccessUsers:
		a.allowAll = true
	case commandAccessOwner:
		if ownerID <= 0 {
			return commandAuthorizer{}, errors.New("owner policy requires resolved owner user id")
		}
		a.allowed[ownerID] = struct{}{}
	case commandAccessAdmins:
		if ownerID > 0 {
			a.allowed[ownerID] = struct{}{}
		}
		for _, id := range adminIDs {
			if id > 0 {
				a.allowed[id] = struct{}{}
			}
		}
		if len(a.allowed) == 0 {
			return commandAuthorizer{}, errors.New("admins policy resolved empty allow-list")
		}
	case commandAccessIDs:
		for _, id := range explicitIDs {
			if id > 0 {
				a.allowed[id] = struct{}{}
			}
		}
		if len(a.allowed) == 0 {
			return commandAuthorizer{}, errors.New("ids policy requires non-empty user id list")
		}
	default:
		return commandAuthorizer{}, fmt.Errorf("unsupported command policy: %s", policy)
	}
	if !a.allowAll && ownerID > 0 {
		a.allowed[ownerID] = struct{}{}
	}
	return a, nil
}

func normalizeCommandPolicy(policy string) (commandAccessMode, error) {
	s := strings.TrimSpace(strings.ToLower(policy))
	switch s {
	case "", "owner":
		return commandAccessOwner, nil
	case "admins", "admin":
		return commandAccessAdmins, nil
	case "users", "all", "everyone":
		return commandAccessUsers, nil
	case "ids", "list":
		return commandAccessIDs, nil
	default:
		return "", fmt.Errorf("unknown command policy: %s", policy)
	}
}

func (a commandAuthorizer) IsAllowed(userID int64) bool {
	if userID <= 0 {
		return false
	}
	if a.allowAll {
		return true
	}
	_, ok := a.allowed[userID]
	return ok
}

type pollCommand struct {
	Name string
	Args []string
	Raw  string
}

type pollControlState struct {
	Paused bool
	Stop   bool
}

func parsePollCommand(text, prefix string) (pollCommand, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return pollCommand{}, false
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "/"
	}
	if !strings.HasPrefix(text, prefix) {
		return pollCommand{}, false
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return pollCommand{}, false
	}
	name := strings.TrimPrefix(parts[0], prefix)
	if at := strings.IndexByte(name, '@'); at >= 0 {
		name = name[:at]
	}
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return pollCommand{}, false
	}
	cmd := pollCommand{Name: name, Raw: text}
	if len(parts) > 1 {
		cmd.Args = parts[1:]
	}
	return cmd, true
}

func applyPollCommand(state *pollControlState, cmd pollCommand) (handled bool, status string) {
	if state == nil {
		state = &pollControlState{}
	}
	switch cmd.Name {
	case "stop", "shutdown", "exit":
		state.Stop = true
		return true, "stop requested"
	case "pause":
		state.Paused = true
		return true, "polling paused"
	case "resume", "start":
		state.Paused = false
		return true, "polling resumed"
	case "status":
		if state.Paused {
			return true, "status: paused"
		}
		return true, "status: running"
	case "help":
		return true, "commands: /status /pause /resume /stop"
	default:
		return false, ""
	}
}

func applyCommandsOnBatch(
	messages []domain.MessageRecord,
	prefix string,
	authorizer commandAuthorizer,
	state *pollControlState,
) (toIngest []domain.MessageRecord, handled []pollCommand) {
	if state == nil {
		state = &pollControlState{}
	}
	toIngest = make([]domain.MessageRecord, 0, len(messages))
	handled = make([]pollCommand, 0, 4)
	for _, msg := range messages {
		if cmd, ok := parsePollCommand(msg.Text, prefix); ok && authorizer.IsAllowed(msg.FromUserID) {
			if ok, _ := applyPollCommand(state, cmd); ok {
				handled = append(handled, cmd)
			}
		}
		if !state.Paused {
			toIngest = append(toIngest, msg)
		}
	}
	return toIngest, handled
}

func parseUserIDs(raw string) ([]int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
		if id <= 0 {
			return nil, fmt.Errorf("id must be positive: %d", id)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}
