package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.uber.org/zap"
)

//go:embed static/*
var staticFiles embed.FS

type Server struct {
	stateDir string
	log      *zap.Logger
	static   http.Handler
	indexHTML []byte
}

type chatSummary struct {
	AccountID    string `json:"account_id"`
	ChatID       int64  `json:"chat_id"`
	UsersCount   int    `json:"users_count"`
	LatestUpdate string `json:"latest_update,omitempty"`
}

type userSummary struct {
	UserID          int64   `json:"user_id"`
	Username        string  `json:"username,omitempty"`
	DisplayName     string  `json:"display_name"`
	MessagesTotal   int     `json:"messages_total"`
	MessageSharePct float64 `json:"message_share_pct"`
	ActivityScore   float64 `json:"activity_score"`
	ActiveDays      int     `json:"active_days"`
	UpdatedAt       string  `json:"updated_at,omitempty"`
}

type snapshotHeader struct {
	Profile struct {
		UserID      int64  `json:"user_id"`
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		FirstName   string `json:"first_name"`
		LastName    string `json:"last_name"`
		UpdatedAt   string `json:"updated_at"`
	} `json:"profile"`
	Stats struct {
		MessagesTotal   int     `json:"messages_total"`
		MessageSharePct float64 `json:"message_share_pct"`
		ActivityScore   float64 `json:"activity_score"`
		ActiveDays      int     `json:"active_days"`
		UpdatedAt       string  `json:"updated_at"`
	} `json:"stats"`
	GeneratedAt string `json:"generated_at"`
}

func NewHandler(stateDir string, log *zap.Logger) (http.Handler, error) {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return nil, fmt.Errorf("state directory is required")
	}

	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return nil, fmt.Errorf("load dashboard assets: %w", err)
	}

	indexHTML, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return nil, fmt.Errorf("load dashboard index: %w", err)
	}

	srv := &Server{
		stateDir: stateDir,
		log:      log.With(zap.String("component", "dashboard")),
		static:   http.FileServer(http.FS(sub)),
		indexHTML: indexHTML,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/chats", srv.handleChats)
	mux.HandleFunc("/api/users", srv.handleUsers)
	mux.HandleFunc("/api/user", srv.handleUser)
	mux.HandleFunc("/", srv.handleStatic)
	return mux, nil
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	if r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(s.indexHTML)
		return
	}
	s.static.ServeHTTP(w, r)
}

func (s *Server) handleChats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	chats, err := s.listChats()
	if err != nil {
		s.log.Error("list chats failed", zap.Error(err))
		s.writeError(w, http.StatusInternalServerError, "failed to list chats")
		return
	}
	s.writeJSON(w, http.StatusOK, chats)
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	accountID, chatID, err := parseSourceQuery(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	users, err := s.listUsers(accountID, chatID)
	if err != nil {
		s.log.Error("list users failed", zap.Error(err), zap.String("account_id", accountID), zap.Int64("chat_id", chatID))
		s.writeError(w, http.StatusInternalServerError, "failed to list users")
		return
	}
	s.writeJSON(w, http.StatusOK, users)
}

func (s *Server) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	accountID, chatID, err := parseSourceQuery(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	userID, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("user_id")), 10, 64)
	if err != nil || userID <= 0 {
		s.writeError(w, http.StatusBadRequest, "invalid user_id")
		return
	}

	path := userSnapshotPath(s.stateDir, accountID, chatID, userID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			s.writeError(w, http.StatusNotFound, "user snapshot not found")
			return
		}
		s.log.Error("open user snapshot failed", zap.Error(err), zap.String("path", path))
		s.writeError(w, http.StatusInternalServerError, "failed to read user snapshot")
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := io.Copy(w, f); err != nil {
		s.log.Error("stream user snapshot failed", zap.Error(err), zap.String("path", path))
	}
}

func (s *Server) listChats() ([]chatSummary, error) {
	exportsRoot := filepath.Join(s.stateDir, "exports")
	accounts, err := os.ReadDir(exportsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return []chatSummary{}, nil
		}
		return nil, err
	}

	result := make([]chatSummary, 0, len(accounts))
	for _, accountEntry := range accounts {
		if !accountEntry.IsDir() {
			continue
		}
		accountID := accountEntry.Name()
		if !isSafeAccountID(accountID) {
			continue
		}
		chatDirs, err := os.ReadDir(filepath.Join(exportsRoot, accountID))
		if err != nil {
			return nil, err
		}
		for _, chatDir := range chatDirs {
			if !chatDir.IsDir() {
				continue
			}
			chatID, ok := parseChatDirName(chatDir.Name())
			if !ok {
				continue
			}

			usersDir := filepath.Join(exportsRoot, accountID, chatDir.Name())
			files, err := os.ReadDir(usersDir)
			if err != nil {
				return nil, err
			}

			usersCount := 0
			latest := time.Time{}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if !strings.HasPrefix(file.Name(), "user_") || !strings.HasSuffix(file.Name(), ".json") {
					continue
				}
				usersCount++
				info, err := file.Info()
				if err == nil && info.ModTime().After(latest) {
					latest = info.ModTime()
				}
			}
			if usersCount == 0 {
				continue
			}

			entry := chatSummary{
				AccountID:  accountID,
				ChatID:     chatID,
				UsersCount: usersCount,
			}
			if !latest.IsZero() {
				entry.LatestUpdate = latest.UTC().Format(time.RFC3339)
			}
			result = append(result, entry)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].AccountID == result[j].AccountID {
			return result[i].ChatID > result[j].ChatID
		}
		return result[i].AccountID < result[j].AccountID
	})

	return result, nil
}

func (s *Server) listUsers(accountID string, chatID int64) ([]userSummary, error) {
	usersDir := filepath.Join(s.stateDir, "exports", accountID, fmt.Sprintf("%d_users", chatID))
	entries, err := os.ReadDir(usersDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []userSummary{}, nil
		}
		return nil, err
	}

	users := make([]userSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "user_") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(usersDir, entry.Name())
		summary, err := readUserSummary(path)
		if err != nil {
			s.log.Warn("skip invalid snapshot", zap.String("path", path), zap.Error(err))
			continue
		}
		users = append(users, summary)
	}

	sort.Slice(users, func(i, j int) bool {
		if users[i].MessagesTotal == users[j].MessagesTotal {
			if users[i].DisplayName == users[j].DisplayName {
				return users[i].UserID < users[j].UserID
			}
			return users[i].DisplayName < users[j].DisplayName
		}
		return users[i].MessagesTotal > users[j].MessagesTotal
	})

	return users, nil
}

func readUserSummary(path string) (userSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return userSummary{}, err
	}
	defer f.Close()

	var header snapshotHeader
	if err := json.NewDecoder(f).Decode(&header); err != nil {
		return userSummary{}, err
	}
	if header.Profile.UserID <= 0 {
		return userSummary{}, fmt.Errorf("missing profile.user_id")
	}

	displayName := strings.TrimSpace(header.Profile.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(strings.TrimSpace(header.Profile.FirstName + " " + header.Profile.LastName))
	}
	if displayName == "" {
		displayName = strings.TrimSpace(header.Profile.Username)
	}
	if displayName == "" {
		displayName = fmt.Sprintf("user_%d", header.Profile.UserID)
	}

	updatedAt := strings.TrimSpace(header.Stats.UpdatedAt)
	if updatedAt == "" {
		updatedAt = strings.TrimSpace(header.Profile.UpdatedAt)
	}
	if updatedAt == "" {
		updatedAt = strings.TrimSpace(header.GeneratedAt)
	}

	return userSummary{
		UserID:          header.Profile.UserID,
		Username:        strings.TrimSpace(header.Profile.Username),
		DisplayName:     displayName,
		MessagesTotal:   header.Stats.MessagesTotal,
		MessageSharePct: header.Stats.MessageSharePct,
		ActivityScore:   header.Stats.ActivityScore,
		ActiveDays:      header.Stats.ActiveDays,
		UpdatedAt:       updatedAt,
	}, nil
}

func parseSourceQuery(r *http.Request) (string, int64, error) {
	accountID := strings.TrimSpace(r.URL.Query().Get("account"))
	if !isSafeAccountID(accountID) {
		return "", 0, fmt.Errorf("invalid account")
	}

	chatID, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("chat")), 10, 64)
	if err != nil || chatID == 0 {
		return "", 0, fmt.Errorf("invalid chat")
	}
	return accountID, chatID, nil
}

func userSnapshotPath(stateDir, accountID string, chatID, userID int64) string {
	return filepath.Join(stateDir, "exports", accountID, fmt.Sprintf("%d_users", chatID), fmt.Sprintf("user_%d.json", userID))
}

func parseChatDirName(name string) (int64, bool) {
	if !strings.HasSuffix(name, "_users") {
		return 0, false
	}
	chatPart := strings.TrimSuffix(name, "_users")
	if chatPart == "" {
		return 0, false
	}
	chatID, err := strconv.ParseInt(chatPart, 10, 64)
	if err != nil || chatID == 0 {
		return 0, false
	}
	return chatID, true
}

func isSafeAccountID(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	for _, r := range v {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.':
			continue
		default:
			return false
		}
	}
	return true
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]any{
		"error": strings.TrimSpace(message),
	})
}
