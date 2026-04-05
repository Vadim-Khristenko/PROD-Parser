package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/analysis"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/index"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/llm"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/storage/jsonl"
)

type Engine struct {
	mu sync.Mutex

	log  *zap.Logger
	root string

	store    *jsonl.Store
	registry *index.Registry
	inv      *index.InvertedIndex

	stats     *analysis.StatsEngine
	relations *analysis.RelationsEngine
	topics    *analysis.TopicsEngine

	profiles          map[string]domain.UserProfile
	fileHashCanonical map[string]string

	llmClient    *llm.Client
	llmDigDeeper bool
}

type MessageFilter struct {
	From    time.Time
	To      time.Time
	Weekday int
	YearDay int
	Limit   int
}

type LLMOptions struct {
	Enabled bool

	BaseURL string
	APIKey  string

	RoutingMode string

	Model         string
	PersonaModel  string
	TopicModel    string
	RelationModel string
	MindmapModel  string

	DigDeeperModel string
	DigDeeper      bool

	ModelInfoURL  string
	ModelInfoFile string

	DefaultInputTokens  int
	DefaultOutputTokens int
	SafetyMarginTokens  int
	MinOutputTokens     int
	BatchMessages       int

	Timeout     time.Duration
	RetryMax    int
	Temperature float64
}

type SnapshotProfile string

const (
	SnapshotProfileFast     SnapshotProfile = "fast"
	SnapshotProfileBalanced SnapshotProfile = "balanced"
	SnapshotProfileFull     SnapshotProfile = "full"
)

type SnapshotOptions struct {
	Profile SnapshotProfile

	IncludeSmartWords     bool
	IncludeContentStats   bool
	IncludeTopicContexts  bool
	IncludePersonaContext bool
	IncludeLLM            bool
	PrettyOutput          bool
	MaxEdges              int
}

func DefaultSnapshotOptions() SnapshotOptions {
	return SnapshotOptionsFromProfile(SnapshotProfileFull)
}

func SnapshotOptionsFromProfile(profile SnapshotProfile) SnapshotOptions {
	switch profile {
	case SnapshotProfileFast:
		return SnapshotOptions{
			Profile:               SnapshotProfileFast,
			IncludeSmartWords:     false,
			IncludeContentStats:   false,
			IncludeTopicContexts:  false,
			IncludePersonaContext: false,
			IncludeLLM:            false,
			PrettyOutput:          false,
			MaxEdges:              300,
		}
	case SnapshotProfileBalanced:
		return SnapshotOptions{
			Profile:               SnapshotProfileBalanced,
			IncludeSmartWords:     true,
			IncludeContentStats:   true,
			IncludeTopicContexts:  false,
			IncludePersonaContext: false,
			IncludeLLM:            false,
			PrettyOutput:          false,
			MaxEdges:              700,
		}
	default:
		return SnapshotOptions{
			Profile:               SnapshotProfileFull,
			IncludeSmartWords:     true,
			IncludeContentStats:   true,
			IncludeTopicContexts:  true,
			IncludePersonaContext: true,
			IncludeLLM:            true,
			PrettyOutput:          true,
			MaxEdges:              1000,
		}
	}
}

func ParseSnapshotProfile(raw string) SnapshotProfile {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case string(SnapshotProfileFast):
		return SnapshotProfileFast
	case string(SnapshotProfileBalanced):
		return SnapshotProfileBalanced
	default:
		return SnapshotProfileFull
	}
}

func normalizeSnapshotOptions(opts SnapshotOptions) SnapshotOptions {
	if opts.Profile == "" {
		opts.Profile = SnapshotProfileFull
	}
	if opts.MaxEdges <= 0 {
		opts.MaxEdges = 1000
	}
	return opts
}

func NewEngine(root string, log *zap.Logger) (*Engine, error) {
	if root == "" {
		return nil, errors.New("state root path is empty")
	}
	if log == nil {
		log = zap.NewNop()
	}
	log = log.With(zap.String("component", "pipeline"))

	e := &Engine{
		log:               log,
		root:              root,
		store:             jsonl.NewStore(root),
		registry:          index.NewRegistry(),
		inv:               index.NewInvertedIndex(),
		stats:             analysis.NewStatsEngine(),
		relations:         analysis.NewRelationsEngine(),
		topics:            analysis.NewTopicsEngine(6*time.Hour, 0.35),
		profiles:          make(map[string]domain.UserProfile, 2048),
		fileHashCanonical: make(map[string]string, 4096),
	}
	if err := e.registry.Load(filepath.Join(root, "state", "registry.json")); err != nil {
		return nil, fmt.Errorf("load registry: %w", err)
	}
	if err := e.inv.Load(filepath.Join(root, "state", "inverted_index.json.gz")); err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}
	if err := e.loadProfiles(filepath.Join(root, "state", "profiles.json")); err != nil {
		return nil, fmt.Errorf("load profiles: %w", err)
	}
	if err := e.loadFileHashCanonical(filepath.Join(root, "state", "file_hash_index.json")); err != nil {
		return nil, fmt.Errorf("load file hash index: %w", err)
	}

	e.log.Info("engine ready")
	return e, nil
}

func (e *Engine) ConfigureTopicDetection(mode analysis.TopicMode, embeddingModel, llmFallbackModel string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.topics.SetMode(mode)
	if embeddingModel != "" {
		e.topics.SetEmbeddingModel(embeddingModel)
	}
	if llmFallbackModel != "" {
		e.topics.SetLLMFallbackModel(llmFallbackModel)
	}
	e.log.Info("topic detection configured",
		zap.String("mode", string(e.topics.Mode())),
		zap.String("embedding_model", e.topics.EmbeddingModel()),
		zap.String("llm_fallback_model", e.topics.LLMFallbackModel()),
	)
}

func (e *Engine) ConfigureStorage(maxMessagesPerFile int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store.SetMaxMessagesPerFile(maxMessagesPerFile)
	e.log.Info("storage configured", zap.Int("max_messages_per_file", maxMessagesPerFile))
}

func (e *Engine) ConfigureLLM(opts LLMOptions) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !opts.Enabled {
		e.llmClient = nil
		e.llmDigDeeper = false
		e.log.Info("llm disabled")
		return nil
	}

	client, err := llm.NewClient(llm.Config{
		Enabled:        opts.Enabled,
		BaseURL:        opts.BaseURL,
		APIKey:         opts.APIKey,
		RoutingMode:    opts.RoutingMode,
		Model:          opts.Model,
		PersonaModel:   opts.PersonaModel,
		TopicModel:     opts.TopicModel,
		RelationModel:  opts.RelationModel,
		MindmapModel:   opts.MindmapModel,
		DigDeeperModel: opts.DigDeeperModel,
		ModelInfoURL:   opts.ModelInfoURL,
		ModelInfoFile:  opts.ModelInfoFile,

		DefaultInputTokens:  opts.DefaultInputTokens,
		DefaultOutputTokens: opts.DefaultOutputTokens,
		SafetyMarginTokens:  opts.SafetyMarginTokens,
		MinOutputTokens:     opts.MinOutputTokens,
		BatchMessages:       opts.BatchMessages,

		Timeout:     opts.Timeout,
		MaxRetries:  opts.RetryMax,
		Temperature: opts.Temperature,
	})
	if err != nil {
		return err
	}
	e.llmClient = client
	e.llmDigDeeper = opts.DigDeeper
	e.log.Info("llm configured",
		zap.String("routing", client.Config().RoutingMode),
		zap.String("model", client.Config().Model),
		zap.Bool("dig_deeper", opts.DigDeeper),
	)
	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var errs []error
	if err := e.registry.Save(filepath.Join(e.root, "state", "registry.json")); err != nil {
		errs = append(errs, err)
	}
	if err := e.inv.Save(filepath.Join(e.root, "state", "inverted_index.json.gz")); err != nil {
		errs = append(errs, err)
	}
	if err := e.saveProfiles(filepath.Join(e.root, "state", "profiles.json")); err != nil {
		errs = append(errs, err)
	}
	if err := e.saveFileHashCanonical(filepath.Join(e.root, "state", "file_hash_index.json")); err != nil {
		errs = append(errs, err)
	}
	if err := e.store.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (e *Engine) ProcessBatch(ctx context.Context, in []domain.MessageRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(in) == 0 {
		return nil
	}

	started := time.Now()
	chatSet := make(map[string]struct{}, 32)
	toWrite := make([]domain.MessageRecord, 0, len(in))
	for _, m := range in {
		if m.AccountID == "" {
			continue
		}
		if m.Date.IsZero() {
			m.Date = time.Now().UTC()
		}
		e.preprocessMessage(&m)
		e.upsertProfileFromMessage(m)
		id, fresh := e.registry.EnsureID(m.AccountID, m.ChatID, m.MessageID)
		if !fresh {
			continue
		}
		m.InternalID = id
		m.ToxicityScore = analysis.ToxicityScore(m.Text)
		m.DerivedTopicID = e.topics.Assign(m)

		e.relations.AddMessage(m)
		e.stats.Add(m)
		e.inv.Add(m.InternalID, m.SearchText())
		toWrite = append(toWrite, m)
		chatSet[m.ChatKey()] = struct{}{}
	}
	if len(toWrite) == 0 {
		e.log.Debug("processed batch contains no fresh messages", zap.Int("input", len(in)))
		return nil
	}

	ptrs, err := e.store.AppendBatch(ctx, toWrite)
	if err != nil {
		return err
	}
	for _, p := range ptrs {
		e.registry.PutPointer(p)
	}

	expired := e.topics.FinalizeExpired(time.Now().UTC())
	for _, t := range expired {
		e.relations.AddCoTopic(t.AccountID, t.ChatID, t.UserIDs)
	}

	e.log.Info("processed batch",
		zap.Int("input", len(in)),
		zap.Int("stored", len(toWrite)),
		zap.Int("chats", len(chatSet)),
		zap.Int("expired_topics", len(expired)),
		zap.Duration("elapsed", time.Since(started)),
	)
	return nil
}

func (e *Engine) Search(ctx context.Context, query string, limit int) ([]domain.SearchResult, error) {
	started := time.Now()
	hits := e.inv.Search(query, limit)
	if len(hits) == 0 {
		e.log.Info("search done", zap.String("query", query), zap.Int("hits", 0), zap.Duration("elapsed", time.Since(started)))
		return nil, nil
	}
	out := make([]domain.SearchResult, 0, len(hits))
	for _, h := range hits {
		ptr, ok := e.registry.Pointer(h.InternalID)
		if !ok {
			continue
		}
		msg, err := e.store.ReadByPointer(ptr)
		if err != nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		out = append(out, domain.SearchResult{Hit: h, Message: msg})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hit.Score == out[j].Hit.Score {
			return out[i].Hit.InternalID > out[j].Hit.InternalID
		}
		return out[i].Hit.Score > out[j].Hit.Score
	})

	e.log.Info("search done",
		zap.String("query", query),
		zap.Int("hits", len(out)),
		zap.Duration("elapsed", time.Since(started)),
	)
	return out, nil
}

func (e *Engine) SearchChat(ctx context.Context, accountID string, chatID int64, query string, limit int) ([]domain.SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}
	globalLimit := limit * 5
	if globalLimit < 50 {
		globalLimit = 50
	}
	results, err := e.Search(ctx, query, globalLimit)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SearchResult, 0, limit)
	for _, item := range results {
		if item.Message.AccountID != accountID || item.Message.ChatID != chatID {
			continue
		}
		out = append(out, item)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (e *Engine) AskChat(ctx context.Context, accountID string, chatID int64, userID int64, question string) (string, error) {
	e.mu.Lock()
	client := e.llmClient
	dig := e.llmDigDeeper
	var profile *domain.UserProfile
	if userID > 0 {
		if p, ok := e.profiles[profileKey(accountID, chatID, userID)]; ok {
			pp := p
			profile = &pp
		}
	}
	e.mu.Unlock()
	if client == nil {
		return "", errors.New("llm is not enabled")
	}

	var (
		msgs          []domain.MessageRecord
		err           error
		selectionMode string
	)
	if userID > 0 {
		selectionMode = "user_context"
		msgs, err = e.store.ReadChatUserContext(accountID, chatID, userID, 0, nil)
	} else {
		selectionMode = "chat_recent"
		msgs, err = e.store.ReadChat(accountID, chatID)
	}
	if err != nil {
		return "", err
	}
	if len(msgs) == 0 {
		return "", errors.New("chat not found")
	}

	chatMessages := msgs
	messageLimit := 120
	if userID > 0 {
		messageLimit = 220
	}
	if len(chatMessages) > messageLimit {
		chatMessages = chatMessages[len(chatMessages)-messageLimit:]
	}

	statsPayload := map[string]any{
		"selection_mode":          selectionMode,
		"selected_messages_total": len(chatMessages),
		"focus_user_id":           userID,
	}
	payload := map[string]any{
		"account_id": accountID,
		"chat_id":    chatID,
		"user_id":    userID,
		"stats":      statsPayload,
		"messages":   chatMessages,
	}
	if profile != nil {
		payload["profile"] = profile
	}
	e.log.Info("ask context prepared",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int64("user_id", userID),
		zap.String("selection_mode", selectionMode),
		zap.Int("messages", len(chatMessages)),
	)
	return client.Ask(ctx, question, payload, dig)
}

func (e *Engine) ChatStoredStatus(accountID string, chatID int64) (messages int, users int, latest time.Time, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	msgs, err := e.store.ReadChat(accountID, chatID)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if len(msgs) == 0 {
		return 0, 0, time.Time{}, nil
	}
	seenUsers := make(map[int64]struct{}, 256)
	latest = msgs[0].Date
	for _, m := range msgs {
		if m.FromUserID > 0 {
			seenUsers[m.FromUserID] = struct{}{}
		}
		if m.Date.After(latest) {
			latest = m.Date
		}
	}
	return len(msgs), len(seenUsers), latest, nil
}

func (e *Engine) BuildChatInsights(accountID string, chatID int64) (domain.ChatInsights, error) {
	return e.BuildChatInsightsWithOptions(accountID, chatID, DefaultSnapshotOptions())
}

func (e *Engine) BuildChatInsightsWithOptions(accountID string, chatID int64, opts SnapshotOptions) (domain.ChatInsights, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	opts = normalizeSnapshotOptions(opts)
	overallStarted := time.Now()
	e.log.Info("build chat insights started",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.String("profile", string(opts.Profile)),
		zap.Int("max_edges", opts.MaxEdges),
	)

	readStarted := time.Now()
	lastReadLog := readStarted
	msgs, err := e.store.ReadChatWithProgress(accountID, chatID, 10000, func(p jsonl.ChatReadProgress) {
		if !p.Done && time.Since(lastReadLog) < 10*time.Second {
			return
		}
		lastReadLog = time.Now()
		e.log.Info("snapshot read progress",
			zap.String("account_id", accountID),
			zap.Int64("chat_id", chatID),
			zap.Int("file_index", p.FileIndex),
			zap.Int("file_count", p.FileCount),
			zap.Int("file_messages", p.FileMessagesRead),
			zap.Int("messages_total", p.TotalMessagesRead),
			zap.Bool("file_done", p.Done),
			zap.String("file", p.FilePath),
		)
	})
	if err != nil {
		return domain.ChatInsights{}, err
	}
	e.log.Info("build chat insights read complete",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int("messages", len(msgs)),
		zap.Duration("elapsed", time.Since(readStarted)),
	)
	if len(msgs) == 0 {
		return domain.ChatInsights{}, errors.New("chat not found")
	}

	insights := e.buildInsightsLocked(accountID, chatID, msgs, opts)
	e.log.Info("build chat insights done",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int("messages", len(msgs)),
		zap.Int("users", len(insights.Users)),
		zap.Int("topics", len(insights.Topics)),
		zap.Duration("elapsed", time.Since(overallStarted)),
	)
	return insights, nil
}

func (e *Engine) ExportChatSnapshot(accountID string, chatID int64) (map[string]any, error) {
	insights, err := e.BuildChatInsights(accountID, chatID)
	if err != nil {
		return nil, err
	}
	snapshot := map[string]any{
		"generated_at":     insights.GeneratedAt,
		"summary":          insights.Summary,
		"chat_stats":       insights.ChatStats,
		"users":            insights.Users,
		"profiles":         insights.Profiles,
		"edges":            insights.Edges,
		"topics":           insights.Topics,
		"personas":         insights.Personas,
		"topic_contexts":   insights.TopicContexts,
		"persona_contexts": insights.PersonaContexts,
		"content":          insights.ContentStats,
		"mindmap":          insights.Mindmap,
		"llm":              insights.LLM,
	}
	return snapshot, nil
}

func (e *Engine) SaveChatSnapshot(accountID string, chatID int64) (string, error) {
	return e.SaveChatSnapshotWithOptions(accountID, chatID, DefaultSnapshotOptions())
}

func (e *Engine) SaveChatSnapshotWithOptions(accountID string, chatID int64, opts SnapshotOptions) (string, error) {
	opts = normalizeSnapshotOptions(opts)
	insights, err := e.BuildChatInsightsWithOptions(accountID, chatID, opts)
	if err != nil {
		return "", err
	}
	outPath := filepath.Join(e.root, "exports", accountID, fmt.Sprintf("%d_snapshot.json", chatID))
	if err := jsonl.WriteJSONWithOptions(outPath, insights, opts.PrettyOutput); err != nil {
		return "", err
	}
	return outPath, nil
}

func (e *Engine) SaveParticipantsSnapshots(accountID string, chatID int64, outDir string, maxRecent int) ([]string, error) {
	if maxRecent <= 0 {
		maxRecent = 200
	}
	opts := SnapshotOptionsFromProfile(SnapshotProfileBalanced)
	opts.IncludePersonaContext = true
	opts.IncludeContentStats = true
	return e.SaveParticipantsSnapshotsWithOptions(accountID, chatID, outDir, maxRecent, opts)
}

func (e *Engine) SaveParticipantsSnapshotsWithOptions(accountID string, chatID int64, outDir string, maxRecent int, opts SnapshotOptions) ([]string, error) {
	if maxRecent <= 0 {
		maxRecent = 200
	}
	opts = normalizeSnapshotOptions(opts)
	insights, err := e.BuildChatInsightsWithOptions(accountID, chatID, opts)
	if err != nil {
		return nil, err
	}
	if outDir == "" {
		outDir = filepath.Join(e.root, "exports", accountID, fmt.Sprintf("%d_users", chatID))
	}

	profileByUser := make(map[int64]domain.UserProfile, len(insights.Profiles))
	for _, p := range insights.Profiles {
		profileByUser[p.UserID] = p
	}

	edgesOut := make(map[int64][]domain.RelationEdge, len(insights.Users))
	edgesIn := make(map[int64][]domain.RelationEdge, len(insights.Users))
	for _, edge := range insights.Edges {
		edgesOut[edge.FromUserID] = append(edgesOut[edge.FromUserID], edge)
		edgesIn[edge.ToUserID] = append(edgesIn[edge.ToUserID], edge)
	}
	topicsByUser := make(map[int64][]domain.Topic, len(insights.Users))
	for _, topic := range insights.Topics {
		for _, uid := range topic.UserIDs {
			topicsByUser[uid] = append(topicsByUser[uid], topic)
		}
	}
	recentByUser := make(map[int64][]domain.MessageRecord, len(insights.PersonaContexts))
	personaByUser := make(map[int64]domain.Persona, len(insights.Personas))
	llmNoteByUser := make(map[int64]string)
	for _, p := range insights.PersonaContexts {
		msgs := p.RecentMsgs
		if len(msgs) > maxRecent {
			msgs = msgs[len(msgs)-maxRecent:]
		}
		recentByUser[p.UserID] = msgs
	}
	for _, persona := range insights.Personas {
		personaByUser[persona.UserID] = persona
	}
	if insights.LLM != nil {
		for uid, note := range insights.LLM.PersonaNotes {
			llmNoteByUser[uid] = note
		}
	}

	written := make([]string, 0, len(insights.Users))
	for _, u := range insights.Users {
		var profilePtr *domain.UserProfile
		var personaPtr *domain.Persona
		if p, ok := profileByUser[u.UserID]; ok {
			profile := p
			profilePtr = &profile
		}
		if p, ok := personaByUser[u.UserID]; ok {
			persona := p
			personaPtr = &persona
		}
		userFiles := insights.ContentStats.Files.ByUser[u.UserID]
		userMentions := insights.ContentStats.Mentions.ByUser[u.UserID]
		userURLs := insights.ContentStats.URLs.ByUser[u.UserID]
		payload := domain.ParticipantSnapshot{
			GeneratedAt:       insights.GeneratedAt,
			AccountID:         accountID,
			ChatID:            chatID,
			UserID:            u.UserID,
			Profile:           profilePtr,
			Stats:             u,
			OutgoingRelations: edgesOut[u.UserID],
			IncomingRelations: edgesIn[u.UserID],
			Topics:            topicsByUser[u.UserID],
			Persona:           personaPtr,
			LLMNote:           llmNoteByUser[u.UserID],
			RecentMessages:    recentByUser[u.UserID],
			Content: domain.ParticipantContentStats{
				Files:    userFiles,
				Mentions: userMentions,
				URLs:     userURLs,
			},
		}
		path := filepath.Join(outDir, fmt.Sprintf("user_%d.json", u.UserID))
		if err := jsonl.WriteJSONWithOptions(path, payload, opts.PrettyOutput); err != nil {
			return nil, err
		}
		written = append(written, path)
	}
	sort.Strings(written)
	return written, nil
}

func (e *Engine) SaveParticipantSnapshot(accountID string, chatID int64, userID int64, outPath string, maxRecent int) (string, error) {
	defaults := SnapshotOptionsFromProfile(SnapshotProfileBalanced)
	defaults.IncludePersonaContext = true
	defaults.IncludeContentStats = true
	return e.SaveParticipantSnapshotWithOptions(accountID, chatID, userID, outPath, maxRecent, defaults)
}

func (e *Engine) SaveParticipantSnapshotWithOptions(accountID string, chatID int64, userID int64, outPath string, maxRecent int, opts SnapshotOptions) (string, error) {
	if userID <= 0 {
		return "", errors.New("user_id must be positive")
	}
	if maxRecent <= 0 {
		maxRecent = 200
	}

	opts = normalizeSnapshotOptions(opts)
	started := time.Now()
	e.log.Info("build user snapshot started",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int64("user_id", userID),
		zap.String("profile", string(opts.Profile)),
	)

	e.mu.Lock()
	llmClient := e.llmClient
	llmDigDeeper := e.llmDigDeeper
	topicMode := e.topics.Mode()
	embeddingModel := e.topics.EmbeddingModel()
	llmFallbackModel := e.topics.LLMFallbackModel()
	profilesLocal := make(map[int64]domain.UserProfile, 128)
	for _, p := range e.profiles {
		if p.AccountID == accountID && p.ChatID == chatID {
			profilesLocal[p.UserID] = p
		}
	}
	e.mu.Unlock()

	lastReadLog := time.Now()
	msgs, err := e.store.ReadChatUserContext(accountID, chatID, userID, 10000, func(p jsonl.ChatReadProgress) {
		if !p.Done && time.Since(lastReadLog) < 10*time.Second {
			return
		}
		lastReadLog = time.Now()
		e.log.Info("user snapshot read progress",
			zap.String("account_id", accountID),
			zap.Int64("chat_id", chatID),
			zap.Int64("user_id", userID),
			zap.Int("pass", p.Pass),
			zap.Int("file_index", p.FileIndex),
			zap.Int("file_count", p.FileCount),
			zap.Int("file_messages", p.FileMessagesRead),
			zap.Int("messages_total", p.TotalMessagesRead),
			zap.Bool("file_done", p.Done),
		)
	})
	if err != nil {
		return "", err
	}
	if len(msgs) == 0 {
		return "", fmt.Errorf("user %d not found in chat context", userID)
	}

	if outPath == "" {
		outPath = filepath.Join(e.root, "exports", accountID, fmt.Sprintf("%d_users", chatID), fmt.Sprintf("user_%d.json", userID))
	}

	stats := analysis.NewStatsEngine()
	relations := analysis.NewRelationsEngine()
	enableTopics := opts.Profile != SnapshotProfileFast
	var topics *analysis.TopicsEngine
	if enableTopics {
		topics = analysis.NewTopicsEngine(6*time.Hour, 0.35)
		topics.SetMode(topicMode)
		topics.SetEmbeddingModel(embeddingModel)
		topics.SetLLMFallbackModel(llmFallbackModel)
	}

	aggregateStarted := time.Now()

	for idx := range msgs {
		m := msgs[idx]
		if m.AccountID == "" {
			m.AccountID = accountID
		}
		if m.ChatID == 0 {
			m.ChatID = chatID
		}
		if m.Date.IsZero() {
			m.Date = time.Now().UTC()
		}
		if len(m.URLs) == 0 && strings.TrimSpace(m.Text) != "" {
			m.URLs = analysis.ExtractURLs(m.Text)
		}
		m.ToxicityScore = analysis.ToxicityScore(m.Text)
		if enableTopics {
			m.DerivedTopicID = topics.Assign(m)
		}

		stats.Add(m)
		relations.AddMessage(m)
		upsertProfileMap(profilesLocal, m)
		msgs[idx] = m
		if (idx+1)%5000 == 0 {
			elapsed := time.Since(aggregateStarted)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(idx+1) / elapsed.Seconds()
			}
			e.log.Info("user snapshot aggregation progress",
				zap.String("account_id", accountID),
				zap.Int64("chat_id", chatID),
				zap.Int64("user_id", userID),
				zap.Int("processed", idx+1),
				zap.Int("total", len(msgs)),
				zap.Duration("elapsed", elapsed),
				zap.Float64("msg_per_sec", rate),
			)
		}
	}

	allTopics := make([]domain.Topic, 0, 64)
	if enableTopics {
		allTopics = topics.FinalizeExpired(time.Now().UTC().Add(365 * 24 * time.Hour))
		for _, t := range allTopics {
			relations.AddCoTopic(t.AccountID, t.ChatID, t.UserIDs)
		}
	}
	e.log.Info("user snapshot aggregation complete",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int64("user_id", userID),
		zap.Int("messages", len(msgs)),
		zap.Bool("topics_enabled", enableTopics),
		zap.Int("topics", len(allTopics)),
		zap.Duration("elapsed", time.Since(aggregateStarted)),
	)

	chat, _ := stats.Chat(accountID, chatID)
	users := stats.Users(accountID, chatID)
	edges := relations.TopEdges(accountID, chatID, opts.MaxEdges)
	users = applyIncomingCounters(users, edges)
	users = applyActivityMetrics(users, chat)

	if opts.IncludeSmartWords {
		chatSmartWords, userSmartWords := analysis.BuildSmartWordRanks(msgs, 20)
		chat.SmartWords = chatSmartWords
		for i := range users {
			users[i].SmartWords = userSmartWords[users[i].UserID]
		}
	}

	personas := analysis.BuildPersonas(accountID, chatID, users, edges)
	var contentStats domain.ContentStats
	if opts.IncludeContentStats {
		contentStats = analysis.BuildContentStats(msgs)
	}
	mindmap := buildMindmap(users, allTopics, edges)
	_ = heuristicSummary(chat, users, allTopics)

	var llmInfo *domain.LLMInsight
	if opts.IncludeLLM && llmClient != nil {
		enrichment, llmErr := llmClient.EnrichChat(context.Background(), llm.ChatInput{
			AccountID: accountID,
			ChatID:    chatID,
			ChatStats: chat,
			Messages:  msgs,
			Users:     users,
			Topics:    allTopics,
			Personas:  personas,
			Edges:     edges,
			Mindmap:   mindmap,
		}, llmDigDeeper)
		if llmErr != nil {
			llmInfo = &domain.LLMInsight{
				Enabled:   true,
				Provider:  "openai-compatible",
				Generated: time.Now().UTC(),
				Error:     llmErr.Error(),
			}
		} else {
			allTopics = applyTopicPatches(allTopics, enrichment.TopicPatches)
			personas = applyPersonaPatches(personas, enrichment.PersonaPatches)
			mindmap = mergeMindmap(mindmap, enrichment.Mindmap)

			topicPatchMeta := make(map[string]string, len(enrichment.TopicPatches))
			for topicID, patch := range enrichment.TopicPatches {
				if patch.Label != "" {
					topicPatchMeta[topicID] = patch.Label
				}
			}
			personaNoteMeta := make(map[int64]string, len(enrichment.PersonaPatches))
			for uid, patch := range enrichment.PersonaPatches {
				if patch.Note != "" {
					personaNoteMeta[uid] = patch.Note
				}
			}

			llmInfo = &domain.LLMInsight{
				Enabled:          true,
				Provider:         "openai-compatible",
				Models:           enrichment.UsedModels,
				Summary:          enrichment.ChatSummary,
				Generated:        time.Now().UTC(),
				TopicPatches:     topicPatchMeta,
				PersonaNotes:     personaNoteMeta,
				RelationInsights: enrichment.RelationInsights,
			}
		}
	}

	target := domain.UserStats{AccountID: accountID, ChatID: chatID, UserID: userID}
	for i := range users {
		if users[i].UserID == userID {
			target = users[i]
			break
		}
	}

	var profilePtr *domain.UserProfile
	if p, ok := profilesLocal[userID]; ok {
		profile := p
		profilePtr = &profile
	}

	var personaPtr *domain.Persona
	for i := range personas {
		if personas[i].UserID == userID {
			persona := personas[i]
			personaPtr = &persona
			break
		}
	}

	outgoing := make([]domain.RelationEdge, 0, 16)
	incoming := make([]domain.RelationEdge, 0, 16)
	for _, edge := range edges {
		if edge.FromUserID == userID {
			outgoing = append(outgoing, edge)
		}
		if edge.ToUserID == userID {
			incoming = append(incoming, edge)
		}
	}

	topicsForUser := make([]domain.Topic, 0, 32)
	for _, topic := range allTopics {
		for _, uid := range topic.UserIDs {
			if uid == userID {
				topicsForUser = append(topicsForUser, topic)
				break
			}
		}
	}

	recentMessages := msgs
	if len(recentMessages) > maxRecent {
		recentMessages = recentMessages[len(recentMessages)-maxRecent:]
	}

	userContent := domain.ParticipantContentStats{}
	if opts.IncludeContentStats {
		userContent.Files = contentStats.Files.ByUser[userID]
		userContent.Mentions = contentStats.Mentions.ByUser[userID]
		userContent.URLs = contentStats.URLs.ByUser[userID]
	}

	llmNote := ""
	if llmInfo != nil {
		llmNote = llmInfo.PersonaNotes[userID]
	}

	payload := domain.ParticipantSnapshot{
		GeneratedAt:       time.Now().UTC(),
		AccountID:         accountID,
		ChatID:            chatID,
		UserID:            userID,
		Profile:           profilePtr,
		Stats:             target,
		OutgoingRelations: outgoing,
		IncomingRelations: incoming,
		Topics:            topicsForUser,
		Persona:           personaPtr,
		LLMNote:           llmNote,
		RecentMessages:    recentMessages,
		Content:           userContent,
	}

	if err := jsonl.WriteJSONWithOptions(outPath, payload, opts.PrettyOutput); err != nil {
		return "", err
	}
	e.log.Info("user snapshot written",
		zap.String("account_id", accountID),
		zap.Int64("chat_id", chatID),
		zap.Int64("user_id", userID),
		zap.Int("messages_used", len(msgs)),
		zap.Int("edges", len(edges)),
		zap.Int("topics", len(topicsForUser)),
		zap.Duration("elapsed", time.Since(started)),
		zap.String("path", outPath),
	)
	return outPath, nil
}

func (e *Engine) ResolveUserID(accountID string, chatID int64, username string) (int64, error) {
	target := strings.TrimSpace(strings.ToLower(strings.TrimPrefix(username, "@")))
	if target == "" {
		return 0, errors.New("username is empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, p := range e.profiles {
		if p.AccountID != accountID || p.ChatID != chatID {
			continue
		}
		if strings.TrimSpace(strings.ToLower(p.Username)) == target && p.UserID > 0 {
			return p.UserID, nil
		}
	}

	msgs, err := e.store.ReadChat(accountID, chatID)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, errors.New("chat not found")
	}

	counts := make(map[int64]int, 16)
	for _, m := range msgs {
		if m.FromUserID <= 0 {
			continue
		}
		name := strings.TrimSpace(strings.ToLower(strings.TrimPrefix(m.FromUsername, "@")))
		if name == target {
			counts[m.FromUserID]++
		}
	}

	bestID := int64(0)
	bestCount := 0
	for uid, c := range counts {
		if c > bestCount {
			bestID = uid
			bestCount = c
		}
	}
	if bestID == 0 {
		return 0, fmt.Errorf("username @%s not found in chat", target)
	}
	return bestID, nil
}

func (e *Engine) MessagesInRange(accountID string, chatID int64, from, to time.Time, limit int) ([]domain.MessageRecord, error) {
	return e.MessagesFiltered(accountID, chatID, MessageFilter{From: from, To: to, Weekday: -1, YearDay: -1, Limit: limit})
}

func (e *Engine) LastMessageID(accountID string, chatID int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.MaxMessageID(accountID, chatID)
}

func (e *Engine) MessagesFiltered(accountID string, chatID int64, filter MessageFilter) ([]domain.MessageRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	msgs, err := e.store.ReadChat(accountID, chatID)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	if !filter.From.IsZero() && !filter.To.IsZero() && filter.To.Before(filter.From) {
		return nil, errors.New("invalid range: to < from")
	}
	if filter.Weekday < -1 || filter.Weekday > 6 {
		return nil, errors.New("invalid weekday, expected -1..6")
	}
	if filter.YearDay < -1 || filter.YearDay > 366 {
		return nil, errors.New("invalid year-day, expected -1..366")
	}
	out := make([]domain.MessageRecord, 0, minInt(filter.Limit, len(msgs)))
	for _, m := range msgs {
		if !filter.From.IsZero() && m.Date.Before(filter.From) {
			continue
		}
		if !filter.To.IsZero() && m.Date.After(filter.To) {
			continue
		}
		if filter.Weekday >= 0 && int(m.Date.Weekday()) != filter.Weekday {
			continue
		}
		if filter.YearDay >= 0 && m.Date.YearDay() != filter.YearDay {
			continue
		}
		out = append(out, m)
		if filter.Limit > 0 && len(out) >= filter.Limit {
			break
		}
	}
	return out, nil
}

func (e *Engine) buildInsightsLocked(accountID string, chatID int64, msgs []domain.MessageRecord, opts SnapshotOptions) domain.ChatInsights {
	opts = normalizeSnapshotOptions(opts)
	aggregateStarted := time.Now()
	e.log.Info("snapshot aggregation started", zap.Int("messages", len(msgs)))

	stats := analysis.NewStatsEngine()
	relations := analysis.NewRelationsEngine()
	topics := analysis.NewTopicsEngine(6*time.Hour, 0.35)
	topics.SetMode(e.topics.Mode())
	topics.SetEmbeddingModel(e.topics.EmbeddingModel())
	topics.SetLLMFallbackModel(e.topics.LLMFallbackModel())

	byMessageID := make(map[int]domain.MessageRecord, len(msgs))
	profilesLocal := make(map[int64]domain.UserProfile, 256)
	for _, p := range e.profiles {
		if p.AccountID == accountID && p.ChatID == chatID {
			profilesLocal[p.UserID] = p
		}
	}

	for idx := range msgs {
		m := msgs[idx]
		if m.AccountID == "" {
			m.AccountID = accountID
		}
		if m.ChatID == 0 {
			m.ChatID = chatID
		}
		if m.Date.IsZero() {
			m.Date = time.Now().UTC()
		}
		e.preprocessMessage(&m)
		m.ToxicityScore = analysis.ToxicityScore(m.Text)
		m.DerivedTopicID = topics.Assign(m)

		stats.Add(m)
		relations.AddMessage(m)
		upsertProfileMap(profilesLocal, m)
		msgs[idx] = m
		byMessageID[m.MessageID] = m
		if (idx+1)%10000 == 0 {
			elapsed := time.Since(aggregateStarted)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(idx+1) / elapsed.Seconds()
			}
			e.log.Info("snapshot aggregation progress",
				zap.Int("processed", idx+1),
				zap.Int("total", len(msgs)),
				zap.Duration("elapsed", elapsed),
				zap.Float64("msg_per_sec", rate),
			)
		}
	}
	e.log.Info("snapshot aggregation complete",
		zap.Int("messages", len(msgs)),
		zap.Duration("elapsed", time.Since(aggregateStarted)),
	)

	topicStageStarted := time.Now()
	allTopics := topics.FinalizeExpired(time.Now().UTC().Add(365 * 24 * time.Hour))
	for _, t := range allTopics {
		relations.AddCoTopic(t.AccountID, t.ChatID, t.UserIDs)
	}
	e.log.Info("snapshot topics finalized",
		zap.Int("topics", len(allTopics)),
		zap.Duration("elapsed", time.Since(topicStageStarted)),
	)

	summaryStageStarted := time.Now()
	chat, _ := stats.Chat(accountID, chatID)
	users := stats.Users(accountID, chatID)
	edges := relations.TopEdges(accountID, chatID, opts.MaxEdges)
	users = applyIncomingCounters(users, edges)
	users = applyActivityMetrics(users, chat)
	personas := analysis.BuildPersonas(accountID, chatID, users, edges)

	var (
		chatSmartWords []domain.WeightedWordScore
		userSmartWords map[int64][]domain.WeightedWordScore
	)
	if opts.IncludeSmartWords {
		chatSmartWords, userSmartWords = analysis.BuildSmartWordRanks(msgs, 20)
		chat.SmartWords = chatSmartWords
		for i := range users {
			users[i].SmartWords = userSmartWords[users[i].UserID]
		}
	}
	e.log.Info("snapshot stats/personas ready",
		zap.Int("users", len(users)),
		zap.Int("edges", len(edges)),
		zap.Int("personas", len(personas)),
		zap.Bool("smart_words", opts.IncludeSmartWords),
		zap.Duration("elapsed", time.Since(summaryStageStarted)),
	)

	contextStageStarted := time.Now()
	var (
		topicPackets   []domain.TopicContextPacket
		personaPackets []domain.PersonaContextPacket
		contentStats   domain.ContentStats
	)
	var postWG sync.WaitGroup
	if opts.IncludeTopicContexts {
		postWG.Add(1)
		go func() {
			defer postWG.Done()
			topicPackets = analysis.BuildTopicContextPackets(allTopics, byMessageID)
		}()
	}
	if opts.IncludePersonaContext {
		postWG.Add(1)
		go func() {
			defer postWG.Done()
			recentByUser := buildRecentByUser(msgs, 200)
			edgesByUser := buildEdgesByUser(edges)
			personaPackets = analysis.BuildPersonaContextPackets(chatID, users, recentByUser, edgesByUser)
		}()
	}
	if opts.IncludeContentStats {
		postWG.Add(1)
		go func() {
			defer postWG.Done()
			contentStats = analysis.BuildContentStats(msgs)
		}()
	}
	postWG.Wait()

	mindmap := buildMindmap(users, allTopics, edges)
	summary := heuristicSummary(chat, users, allTopics)
	e.log.Info("snapshot context/content ready",
		zap.Int("topic_packets", len(topicPackets)),
		zap.Int("persona_packets", len(personaPackets)),
		zap.Bool("content_stats", opts.IncludeContentStats),
		zap.Duration("elapsed", time.Since(contextStageStarted)),
	)

	var llmInfo *domain.LLMInsight
	if opts.IncludeLLM && e.llmClient != nil {
		llmStarted := time.Now()
		e.log.Info("snapshot llm enrichment started")
		enrichment, err := e.llmClient.EnrichChat(context.Background(), llm.ChatInput{
			AccountID: accountID,
			ChatID:    chatID,
			ChatStats: chat,
			Messages:  msgs,
			Users:     users,
			Topics:    allTopics,
			Personas:  personas,
			Edges:     edges,
			Mindmap:   mindmap,
		}, e.llmDigDeeper)
		if err != nil {
			e.log.Warn("llm enrichment failed", zap.Error(err))
			llmInfo = &domain.LLMInsight{
				Enabled:   true,
				Provider:  "openai-compatible",
				Generated: time.Now().UTC(),
				Error:     err.Error(),
			}
		} else {
			if enrichment.ChatSummary != "" {
				summary = enrichment.ChatSummary
			}
			allTopics = applyTopicPatches(allTopics, enrichment.TopicPatches)
			personas = applyPersonaPatches(personas, enrichment.PersonaPatches)
			mindmap = mergeMindmap(mindmap, enrichment.Mindmap)

			topicPatchMeta := make(map[string]string, len(enrichment.TopicPatches))
			for topicID, patch := range enrichment.TopicPatches {
				if patch.Label != "" {
					topicPatchMeta[topicID] = patch.Label
				}
			}
			personaNoteMeta := make(map[int64]string, len(enrichment.PersonaPatches))
			for uid, patch := range enrichment.PersonaPatches {
				if patch.Note != "" {
					personaNoteMeta[uid] = patch.Note
				}
			}

			llmInfo = &domain.LLMInsight{
				Enabled:          true,
				Provider:         "openai-compatible",
				Models:           enrichment.UsedModels,
				Summary:          enrichment.ChatSummary,
				Generated:        time.Now().UTC(),
				TopicPatches:     topicPatchMeta,
				PersonaNotes:     personaNoteMeta,
				RelationInsights: enrichment.RelationInsights,
			}
		}
		e.log.Info("snapshot llm enrichment complete", zap.Duration("elapsed", time.Since(llmStarted)))
	}

	profiles := make([]domain.UserProfile, 0, len(profilesLocal))
	for _, p := range profilesLocal {
		profiles = append(profiles, p)
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].UserID < profiles[j].UserID })

	return domain.ChatInsights{
		GeneratedAt:     time.Now().UTC(),
		AccountID:       accountID,
		ChatID:          chatID,
		Summary:         summary,
		ChatStats:       chat,
		Users:           users,
		Profiles:        profiles,
		Edges:           edges,
		Topics:          allTopics,
		Personas:        personas,
		TopicContexts:   topicPackets,
		PersonaContexts: personaPackets,
		ContentStats:    contentStats,
		Mindmap:         mindmap,
		LLM:             llmInfo,
	}
}

func (e *Engine) upsertProfileFromMessage(m domain.MessageRecord) {
	if m.FromUserID <= 0 {
		return
	}
	k := profileKey(m.AccountID, m.ChatID, m.FromUserID)
	existing := e.profiles[k]

	changed := false
	if m.FromUsername != "" && existing.Username != m.FromUsername {
		existing.Username = m.FromUsername
		changed = true
	}
	if m.FromFirstName != "" && existing.FirstName != m.FromFirstName {
		existing.FirstName = m.FromFirstName
		changed = true
	}
	if m.FromLastName != "" && existing.LastName != m.FromLastName {
		existing.LastName = m.FromLastName
		changed = true
	}
	if m.FromDisplayName != "" && existing.DisplayName != m.FromDisplayName {
		existing.DisplayName = m.FromDisplayName
		changed = true
	}
	if m.FromBio != "" && existing.Bio != m.FromBio {
		existing.Bio = m.FromBio
		changed = true
	}
	if m.FromAvatarRef != "" && existing.AvatarRef != m.FromAvatarRef {
		existing.AvatarRef = m.FromAvatarRef
		changed = true
	}
	if m.FromAvatarFile != "" && existing.AvatarFile != m.FromAvatarFile {
		existing.AvatarFile = m.FromAvatarFile
		changed = true
	}
	if !changed && existing.UserID != 0 {
		return
	}

	existing.AccountID = m.AccountID
	existing.ChatID = m.ChatID
	existing.UserID = m.FromUserID
	existing.UpdatedAt = time.Now().UTC()
	e.profiles[k] = existing
}

func (e *Engine) loadProfiles(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	state := make(map[string]domain.UserProfile)
	if err := json.Unmarshal(b, &state); err != nil {
		return err
	}
	e.profiles = state
	if e.profiles == nil {
		e.profiles = make(map[string]domain.UserProfile)
	}
	return nil
}

func (e *Engine) saveProfiles(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(e.profiles, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (e *Engine) loadFileHashCanonical(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	state := make(map[string]string)
	if err := json.Unmarshal(b, &state); err != nil {
		return err
	}
	e.fileHashCanonical = state
	if e.fileHashCanonical == nil {
		e.fileHashCanonical = make(map[string]string)
	}
	return nil
}

func (e *Engine) saveFileHashCanonical(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(e.fileHashCanonical, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (e *Engine) preprocessMessage(m *domain.MessageRecord) {
	if m == nil {
		return
	}
	m.AccountID = strings.TrimSpace(m.AccountID)
	if !m.Date.IsZero() {
		m.Date = m.Date.UTC()
	}
	m.FromUsername = normalizeUsername(m.FromUsername)
	m.FromFirstName = strings.TrimSpace(m.FromFirstName)
	m.FromLastName = strings.TrimSpace(m.FromLastName)
	m.FromDisplayName = strings.TrimSpace(m.FromDisplayName)
	m.Text = normalizeMessageText(m.Text)
	if m.FromDisplayName == "" {
		m.FromDisplayName = deriveDisplayName(m.FromFirstName, m.FromLastName, m.FromUsername)
	}
	m.MentionsUserIDs = dedupePositiveInt64(m.MentionsUserIDs)
	urlsFromText := analysis.ExtractURLs(m.Text)
	m.URLs = mergeUniqueURLs(m.URLs, urlsFromText)

	m.MediaType = strings.TrimSpace(strings.ToLower(m.MediaType))
	if !supportsMediaDedup(m.MediaType) {
		return
	}
	hash := strings.TrimSpace(m.MediaFileHash)
	if hash == "" {
		return
	}
	key := mediaHashKey(m.AccountID, m.ChatID, m.MediaType, hash)
	if key == "" {
		return
	}
	if canonical, ok := e.fileHashCanonical[key]; ok && canonical != "" {
		m.MediaCanonical = canonical
		return
	}
	canonical := firstNonEmpty(
		strings.TrimSpace(m.MediaCanonical),
		strings.TrimSpace(m.MediaID),
		"hash:"+hash,
	)
	if canonical == "" {
		canonical = "hash:" + hash
	}
	e.fileHashCanonical[key] = canonical
	m.MediaCanonical = canonical
}

func supportsMediaDedup(mediaType string) bool {
	switch strings.TrimSpace(strings.ToLower(mediaType)) {
	case "gif", "sticker", "document":
		return true
	default:
		return false
	}
}

func mediaHashKey(accountID string, chatID int64, mediaType, hash string) string {
	accountID = strings.TrimSpace(accountID)
	mediaType = strings.TrimSpace(strings.ToLower(mediaType))
	hash = strings.TrimSpace(hash)
	if accountID == "" || mediaType == "" || hash == "" {
		return ""
	}
	return accountID + ":" + formatInt64(chatID) + ":" + mediaType + ":" + hash
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func normalizeUsername(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "@")
	return strings.TrimSpace(v)
}

func normalizeMessageText(v string) string {
	v = strings.ReplaceAll(v, "\r\n", "\n")
	v = strings.ReplaceAll(v, "\r", "\n")
	v = strings.ReplaceAll(v, "\x00", "")
	return strings.TrimSpace(v)
}

func deriveDisplayName(firstName, lastName, username string) string {
	display := strings.TrimSpace(strings.TrimSpace(firstName) + " " + strings.TrimSpace(lastName))
	if display != "" {
		return display
	}
	username = strings.TrimSpace(username)
	if username != "" {
		return "@" + strings.TrimPrefix(username, "@")
	}
	return ""
}

func dedupePositiveInt64(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mergeUniqueURLs(existing, extracted []string) []string {
	if len(existing) == 0 && len(extracted) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(existing)+len(extracted))
	out := make([]string, 0, len(existing)+len(extracted))
	for _, raw := range append(existing, extracted...) {
		url := strings.TrimSpace(raw)
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		out = append(out, url)
	}
	sort.Strings(out)
	return out
}

func buildRecentByUser(msgs []domain.MessageRecord, maxPerUser int) map[int64][]domain.MessageRecord {
	if maxPerUser <= 0 {
		maxPerUser = 60
	}
	out := make(map[int64][]domain.MessageRecord, 256)
	for _, m := range msgs {
		bucket := out[m.FromUserID]
		bucket = append(bucket, m)
		if len(bucket) > maxPerUser {
			bucket = bucket[len(bucket)-maxPerUser:]
		}
		out[m.FromUserID] = bucket
	}
	return out
}

func buildEdgesByUser(edges []domain.RelationEdge) map[int64][]domain.RelationEdge {
	out := make(map[int64][]domain.RelationEdge, 256)
	for _, e := range edges {
		out[e.FromUserID] = append(out[e.FromUserID], e)
	}
	return out
}

func upsertProfileMap(store map[int64]domain.UserProfile, m domain.MessageRecord) {
	ex := store[m.FromUserID]
	changed := false
	if m.FromUsername != "" && ex.Username != m.FromUsername {
		ex.Username = m.FromUsername
		changed = true
	}
	if m.FromFirstName != "" && ex.FirstName != m.FromFirstName {
		ex.FirstName = m.FromFirstName
		changed = true
	}
	if m.FromLastName != "" && ex.LastName != m.FromLastName {
		ex.LastName = m.FromLastName
		changed = true
	}
	if m.FromDisplayName != "" && ex.DisplayName != m.FromDisplayName {
		ex.DisplayName = m.FromDisplayName
		changed = true
	}
	if m.FromBio != "" && ex.Bio != m.FromBio {
		ex.Bio = m.FromBio
		changed = true
	}
	if m.FromAvatarRef != "" && ex.AvatarRef != m.FromAvatarRef {
		ex.AvatarRef = m.FromAvatarRef
		changed = true
	}
	if m.FromAvatarFile != "" && ex.AvatarFile != m.FromAvatarFile {
		ex.AvatarFile = m.FromAvatarFile
		changed = true
	}
	if !changed && ex.UserID != 0 {
		return
	}
	ex.AccountID = m.AccountID
	ex.ChatID = m.ChatID
	ex.UserID = m.FromUserID
	ex.UpdatedAt = time.Now().UTC()
	store[m.FromUserID] = ex
}

func applyIncomingCounters(users []domain.UserStats, edges []domain.RelationEdge) []domain.UserStats {
	if len(users) == 0 || len(edges) == 0 {
		return users
	}
	idx := make(map[int64]int, len(users))
	for i := range users {
		idx[users[i].UserID] = i
	}
	for _, e := range edges {
		i, ok := idx[e.ToUserID]
		if !ok {
			continue
		}
		users[i].ReplyIn += e.Replies
		users[i].MentionIn += e.Mentions
	}
	return users
}

func applyActivityMetrics(users []domain.UserStats, chat domain.ChatStats) []domain.UserStats {
	if len(users) == 0 {
		return users
	}
	maxMeaningful := 0.0
	maxConsistency := 0.0
	maxWordsPerMessage := 0.0
	maxActiveDays := int64(1)
	for _, u := range users {
		if u.AvgMeaningfulWordsPerMessage > maxMeaningful {
			maxMeaningful = u.AvgMeaningfulWordsPerMessage
		}
		if u.AvgMessagesPerActiveDay > maxConsistency {
			maxConsistency = u.AvgMessagesPerActiveDay
		}
		if u.AvgWordsPerMessage > maxWordsPerMessage {
			maxWordsPerMessage = u.AvgWordsPerMessage
		}
		if u.ActiveDays > maxActiveDays {
			maxActiveDays = u.ActiveDays
		}
	}
	if maxMeaningful <= 0 {
		maxMeaningful = 1
	}
	if maxConsistency <= 0 {
		maxConsistency = 1
	}
	if maxWordsPerMessage <= 0 {
		maxWordsPerMessage = 1
	}

	total := chat.MessagesTotal
	for i := range users {
		u := &users[i]
		if total > 0 {
			u.MessageSharePct = 100 * float64(u.MessagesTotal) / float64(total)
		}

		share := clamp01(u.MessageSharePct / 100)
		semanticDepth := clamp01(u.AvgMeaningfulWordsPerMessage / maxMeaningful)
		semanticRate := clamp01(u.MeaningfulWordRate)
		semantic := clamp01(0.65*semanticDepth + 0.35*semanticRate)
		wordDepth := clamp01(u.AvgWordsPerMessage / maxWordsPerMessage)

		engagementRaw := 0.0
		if u.MessagesTotal > 0 {
			engagementRaw = float64(u.ReplyOut+u.ReplyIn+u.MentionOut+u.MentionIn) / float64(u.MessagesTotal)
		}
		u.EngagementRate = engagementRaw
		engagement := clamp01(engagementRaw)
		consistency := clamp01(u.AvgMessagesPerActiveDay / maxConsistency)
		coverage := 0.0
		if maxActiveDays > 0 {
			coverage = clamp01(float64(u.ActiveDays) / float64(maxActiveDays))
		}

		score := 0.38*share + 0.20*semantic + 0.15*engagement + 0.12*consistency + 0.10*coverage + 0.05*wordDepth
		if u.AvgToxicity > 0.8 {
			score *= 0.9
		} else if u.AvgToxicity > 0.6 {
			score *= 0.95
		}
		if u.MessagesTotal > 0 {
			emptyRatio := float64(u.EmptyMessages) / float64(u.MessagesTotal)
			if emptyRatio > 0.6 {
				score *= 0.92
			}
		}
		u.ActivityScore = clamp01(score)
	}

	sort.SliceStable(users, func(i, j int) bool {
		if users[i].ActivityScore == users[j].ActivityScore {
			return users[i].MessagesTotal > users[j].MessagesTotal
		}
		return users[i].ActivityScore > users[j].ActivityScore
	})
	return users
}

func heuristicSummary(chat domain.ChatStats, users []domain.UserStats, topics []domain.Topic) string {
	leader := int64(0)
	leaderShare := 0.0
	if len(users) > 0 {
		leader = users[0].UserID
		leaderShare = users[0].MessageSharePct
	}
	return fmt.Sprintf(
		"messages=%d, users=%d, topics=%d, leader_user=%d (%.1f%% share)",
		chat.MessagesTotal,
		len(users),
		len(topics),
		leader,
		leaderShare,
	)
}

func buildMindmap(users []domain.UserStats, topics []domain.Topic, edges []domain.RelationEdge) domain.Mindmap {
	nodes := make([]domain.MindmapNode, 0, len(users)+len(topics))
	edgesOut := make([]domain.MindmapEdge, 0, len(edges)+len(users))

	for _, u := range users {
		nodes = append(nodes, domain.MindmapNode{
			ID:     "u:" + strconv.FormatInt(u.UserID, 10),
			Label:  "user_" + strconv.FormatInt(u.UserID, 10),
			Type:   "user",
			Group:  "participants",
			Weight: u.ActivityScore,
		})
	}
	for _, t := range topics {
		label := t.TopicID
		if t.Summary != "" {
			label = t.Summary
		} else if len(t.Keywords) > 0 {
			label = strings.Join(t.Keywords[:minInt(len(t.Keywords), 4)], " ")
		}
		nodes = append(nodes, domain.MindmapNode{
			ID:     "t:" + t.TopicID,
			Label:  label,
			Type:   "topic",
			Group:  "topics",
			Weight: t.Confidence,
		})
		for _, uid := range t.UserIDs {
			edgesOut = append(edgesOut, domain.MindmapEdge{
				From:   "u:" + strconv.FormatInt(uid, 10),
				To:     "t:" + t.TopicID,
				Label:  "participates",
				Weight: t.Confidence,
			})
		}
	}
	for _, e := range edges {
		edgesOut = append(edgesOut, domain.MindmapEdge{
			From:   "u:" + strconv.FormatInt(e.FromUserID, 10),
			To:     "u:" + strconv.FormatInt(e.ToUserID, 10),
			Label:  "rel",
			Weight: e.Weight,
		})
	}

	return domain.Mindmap{Nodes: nodes, Edges: edgesOut}
}

func mergeMindmap(base domain.Mindmap, extra domain.Mindmap) domain.Mindmap {
	if len(extra.Nodes) == 0 && len(extra.Edges) == 0 {
		return base
	}
	nodeSeen := make(map[string]struct{}, len(base.Nodes)+len(extra.Nodes))
	mergedNodes := make([]domain.MindmapNode, 0, len(base.Nodes)+len(extra.Nodes))
	for _, n := range append(base.Nodes, extra.Nodes...) {
		k := n.ID + "|" + n.Type
		if _, ok := nodeSeen[k]; ok {
			continue
		}
		nodeSeen[k] = struct{}{}
		mergedNodes = append(mergedNodes, n)
	}

	edgeSeen := make(map[string]struct{}, len(base.Edges)+len(extra.Edges))
	mergedEdges := make([]domain.MindmapEdge, 0, len(base.Edges)+len(extra.Edges))
	for _, e := range append(base.Edges, extra.Edges...) {
		k := e.From + "|" + e.To + "|" + e.Label
		if _, ok := edgeSeen[k]; ok {
			continue
		}
		edgeSeen[k] = struct{}{}
		mergedEdges = append(mergedEdges, e)
	}
	return domain.Mindmap{Nodes: mergedNodes, Edges: mergedEdges}
}

func applyTopicPatches(topics []domain.Topic, patches map[string]llm.TopicPatch) []domain.Topic {
	if len(topics) == 0 || len(patches) == 0 {
		return topics
	}
	out := make([]domain.Topic, len(topics))
	copy(out, topics)
	for i := range out {
		patch, ok := patches[out[i].TopicID]
		if !ok {
			continue
		}
		if strings.TrimSpace(patch.Summary) != "" {
			out[i].Summary = strings.TrimSpace(patch.Summary)
		}
		if patch.Confidence > 0 {
			out[i].Confidence = patch.Confidence
		}
		if strings.TrimSpace(patch.Label) != "" {
			out[i].Keywords = append([]string{strings.TrimSpace(patch.Label)}, out[i].Keywords...)
		}
	}
	return out
}

func applyPersonaPatches(personas []domain.Persona, patches map[int64]llm.PersonaPatch) []domain.Persona {
	if len(personas) == 0 || len(patches) == 0 {
		return personas
	}
	out := make([]domain.Persona, len(personas))
	copy(out, personas)
	for i := range out {
		patch, ok := patches[out[i].UserID]
		if !ok {
			continue
		}
		if strings.TrimSpace(patch.Role) != "" {
			out[i].Role = strings.TrimSpace(patch.Role)
		}
		if strings.TrimSpace(patch.Tone) != "" {
			out[i].Tone = strings.TrimSpace(patch.Tone)
		}
		if len(patch.Traits) > 0 {
			out[i].Traits = patch.Traits
		}
		if len(patch.Interests) > 0 {
			out[i].Interests = patch.Interests
		}
		if len(patch.Triggers) > 0 {
			out[i].Triggers = patch.Triggers
		}
		if len(patch.TypicalPhrases) > 0 {
			out[i].TypicalPhrases = patch.TypicalPhrases
		}
		if strings.TrimSpace(patch.Summary) != "" {
			out[i].Summary = strings.TrimSpace(patch.Summary)
		}
		if patch.Confidence > 0 {
			out[i].Confidence = patch.Confidence
		}
	}
	return out
}

func profileKey(accountID string, chatID, userID int64) string {
	return accountID + ":" + formatInt64(chatID) + ":" + formatInt64(userID)
}

func formatInt64(v int64) string {
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

func minInt(a, b int) int {
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
