package index

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type registryState struct {
	NextID    uint64                          `json:"next_id"`
	Pointers  map[uint64]domain.StoredPointer `json:"pointers"`
	ByChatMsg map[string]uint64               `json:"by_chat_msg"`
}

// Registry tracks custom internal IDs and dedup keys for Telegram messages.
type Registry struct {
	mu sync.RWMutex

	nextID    uint64
	pointers  map[uint64]domain.StoredPointer
	byChatMsg map[string]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		nextID:    1,
		pointers:  make(map[uint64]domain.StoredPointer, 1024),
		byChatMsg: make(map[string]uint64, 1024),
	}
}

func (r *Registry) EnsureID(accountID string, chatID int64, messageID int) (uint64, bool) {
	key := dedupKey(accountID, chatID, messageID)

	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.byChatMsg[key]; ok {
		return id, false
	}
	id := r.nextID
	r.nextID++
	r.byChatMsg[key] = id
	return id, true
}

func (r *Registry) PutPointer(ptr domain.StoredPointer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pointers[ptr.InternalID] = ptr
	r.byChatMsg[dedupKey(ptr.AccountID, ptr.ChatID, ptr.MessageID)] = ptr.InternalID
	if ptr.InternalID >= r.nextID {
		r.nextID = ptr.InternalID + 1
	}
}

func (r *Registry) Pointer(id uint64) (domain.StoredPointer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ptr, ok := r.pointers[id]
	return ptr, ok
}

func (r *Registry) Save(path string) error {
	r.mu.RLock()
	state := registryState{
		NextID:    r.nextID,
		Pointers:  make(map[uint64]domain.StoredPointer, len(r.pointers)),
		ByChatMsg: make(map[string]uint64, len(r.byChatMsg)),
	}
	for k, v := range r.pointers {
		state.Pointers[k] = v
	}
	for k, v := range r.byChatMsg {
		state.ByChatMsg[k] = v
	}
	r.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (r *Registry) Load(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var state registryState
	if err := json.Unmarshal(b, &state); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID = state.NextID
	r.pointers = state.Pointers
	r.byChatMsg = state.ByChatMsg
	if r.pointers == nil {
		r.pointers = make(map[uint64]domain.StoredPointer)
	}
	if r.byChatMsg == nil {
		r.byChatMsg = make(map[string]uint64)
	}
	if r.nextID == 0 {
		r.nextID = 1
	}
	return nil
}

func dedupKey(accountID string, chatID int64, messageID int) string {
	return accountID + ":" + itoa64(chatID) + ":" + itoa(messageID)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [24]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
