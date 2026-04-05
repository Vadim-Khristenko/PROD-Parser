package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type Config struct {
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

	ModelInfoURL  string
	ModelInfoFile string

	DefaultInputTokens  int
	DefaultOutputTokens int
	SafetyMarginTokens  int
	MinOutputTokens     int
	BatchMessages       int

	Timeout     time.Duration
	MaxRetries  int
	Temperature float64
}

type Client struct {
	cfg        Config
	httpClient *http.Client
	modelCaps  map[string]modelCapacity
}

type modelCapacity struct {
	InputTokens  int `json:"max_input_tokens"`
	OutputTokens int `json:"max_output_tokens"`
}

type ChatInput struct {
	AccountID string
	ChatID    int64

	ChatStats domain.ChatStats
	Messages  []domain.MessageRecord
	Users     []domain.UserStats
	Topics    []domain.Topic
	Personas  []domain.Persona
	Edges     []domain.RelationEdge
	Mindmap   domain.Mindmap
}

type ChatEnrichment struct {
	UsedModels []string

	ChatSummary string

	TopicPatches   map[string]TopicPatch
	PersonaPatches map[int64]PersonaPatch

	RelationInsights []domain.RelationInsight

	Mindmap domain.Mindmap

	Raw json.RawMessage
}

type TopicPatch struct {
	Label      string
	Summary    string
	Confidence float64
}

type PersonaPatch struct {
	Role           string
	Tone           string
	Traits         []string
	Interests      []string
	Triggers       []string
	TypicalPhrases []string
	Summary        string
	Confidence     float64
	Note           string
}

func NewClient(cfg Config) (*Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("llm base url is empty")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, errors.New("llm model is empty")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 45 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.Temperature <= 0 {
		cfg.Temperature = 0.2
	}
	if cfg.DefaultInputTokens <= 0 {
		cfg.DefaultInputTokens = 32000
	}
	if cfg.DefaultOutputTokens <= 0 {
		cfg.DefaultOutputTokens = 2048
	}
	if cfg.SafetyMarginTokens <= 0 {
		cfg.SafetyMarginTokens = 512
	}
	if cfg.MinOutputTokens <= 0 {
		cfg.MinOutputTokens = 256
	}
	if cfg.BatchMessages <= 0 {
		cfg.BatchMessages = 250
	}
	cfg.BaseURL = strings.TrimSuffix(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.RoutingMode == "" {
		cfg.RoutingMode = "single"
	}

	modelCaps := map[string]modelCapacity{}
	if strings.TrimSpace(cfg.ModelInfoURL) != "" {
		u, err := resolveModelInfoURL(cfg.BaseURL, cfg.ModelInfoURL)
		if err != nil {
			return nil, err
		}
		caps, err := loadModelCapsFromURL(context.Background(), &http.Client{Timeout: cfg.Timeout}, u, cfg.APIKey)
		if err != nil {
			return nil, err
		}
		mergeModelCaps(modelCaps, caps)
	}
	if strings.TrimSpace(cfg.ModelInfoFile) != "" {
		caps, err := loadModelCapsFromFile(cfg.ModelInfoFile)
		if err != nil {
			return nil, err
		}
		mergeModelCaps(modelCaps, caps)
	}

	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		modelCaps: modelCaps,
	}, nil
}

func (c *Client) Config() Config {
	return c.cfg
}

func (c *Client) EnrichChat(ctx context.Context, in ChatInput, digDeeper bool) (ChatEnrichment, error) {
	if c == nil {
		return ChatEnrichment{}, errors.New("llm client is nil")
	}
	if c.cfg.BatchMessages > 0 && len(in.Messages) > c.cfg.BatchMessages {
		return c.enrichBatched(ctx, in, digDeeper)
	}
	if strings.EqualFold(c.cfg.RoutingMode, "per-task") {
		return c.enrichPerTask(ctx, in, digDeeper)
	}
	return c.enrichSingle(ctx, in, digDeeper)
}

func (c *Client) Ask(ctx context.Context, question string, contextPayload any, digDeeper bool) (string, error) {
	if c == nil {
		return "", errors.New("llm client is nil")
	}
	question = strings.TrimSpace(question)
	if question == "" {
		return "", errors.New("question is empty")
	}
	model := c.pickModel(taskPersona, digDeeper)
	raw, err := c.completeJSON(ctx, model, askPrompt(), map[string]any{
		"question": question,
		"context":  contextPayload,
	})
	if err != nil {
		return "", err
	}
	parsed := struct {
		Answer     string   `json:"answer"`
		Confidence float64  `json:"confidence,omitempty"`
		Bullets    []string `json:"bullets,omitempty"`
	}{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("decode ask response: %w", err)
	}
	if strings.TrimSpace(parsed.Answer) == "" {
		return "", errors.New("ask answer is empty")
	}
	answer := strings.TrimSpace(parsed.Answer)
	if len(parsed.Bullets) > 0 {
		for _, b := range parsed.Bullets {
			b = strings.TrimSpace(b)
			if b == "" {
				continue
			}
			answer += "\n- " + b
		}
	}
	return answer, nil
}

func (c *Client) enrichBatched(ctx context.Context, in ChatInput, digDeeper bool) (ChatEnrichment, error) {
	limit := c.cfg.BatchMessages
	if limit <= 0 {
		limit = len(in.Messages)
	}

	agg := ChatEnrichment{
		TopicPatches:   map[string]TopicPatch{},
		PersonaPatches: map[int64]PersonaPatch{},
	}
	modelSeen := map[string]struct{}{}
	relSeen := map[string]domain.RelationInsight{}
	summaryParts := make([]string, 0, 8)
	rawParts := make([]json.RawMessage, 0, (len(in.Messages)+limit-1)/limit)

	for start := 0; start < len(in.Messages); start += limit {
		end := start + limit
		if end > len(in.Messages) {
			end = len(in.Messages)
		}
		sub := batchInput(in, in.Messages[start:end])
		var (
			partial ChatEnrichment
			err     error
		)
		if strings.EqualFold(c.cfg.RoutingMode, "per-task") {
			partial, err = c.enrichPerTask(ctx, sub, digDeeper)
		} else {
			partial, err = c.enrichSingle(ctx, sub, digDeeper)
		}
		if err != nil {
			return ChatEnrichment{}, err
		}
		if s := strings.TrimSpace(partial.ChatSummary); s != "" {
			summaryParts = appendUniqueString(summaryParts, s)
		}
		for _, model := range partial.UsedModels {
			model = strings.TrimSpace(model)
			if model == "" {
				continue
			}
			if _, ok := modelSeen[model]; ok {
				continue
			}
			modelSeen[model] = struct{}{}
			agg.UsedModels = append(agg.UsedModels, model)
		}
		for topicID, patch := range partial.TopicPatches {
			cur, ok := agg.TopicPatches[topicID]
			if !ok || patch.Confidence >= cur.Confidence {
				agg.TopicPatches[topicID] = patch
			}
		}
		for userID, patch := range partial.PersonaPatches {
			cur, ok := agg.PersonaPatches[userID]
			if !ok || patch.Confidence >= cur.Confidence {
				agg.PersonaPatches[userID] = patch
			}
		}
		for _, rel := range partial.RelationInsights {
			key := relationKey(rel)
			cur, ok := relSeen[key]
			if !ok || rel.Confidence >= cur.Confidence {
				relSeen[key] = rel
			}
		}
		agg.Mindmap = mergeMindmapParts(agg.Mindmap, partial.Mindmap)
		if len(partial.Raw) > 0 {
			rawParts = append(rawParts, partial.Raw)
		}
	}

	agg.ChatSummary = strings.Join(summaryParts, " | ")
	agg.RelationInsights = make([]domain.RelationInsight, 0, len(relSeen))
	for _, rel := range relSeen {
		agg.RelationInsights = append(agg.RelationInsights, rel)
	}
	sort.Slice(agg.RelationInsights, func(i, j int) bool {
		if agg.RelationInsights[i].Confidence == agg.RelationInsights[j].Confidence {
			if agg.RelationInsights[i].FromUserID == agg.RelationInsights[j].FromUserID {
				return agg.RelationInsights[i].ToUserID < agg.RelationInsights[j].ToUserID
			}
			return agg.RelationInsights[i].FromUserID < agg.RelationInsights[j].FromUserID
		}
		return agg.RelationInsights[i].Confidence > agg.RelationInsights[j].Confidence
	})
	agg.Raw = mergeRaw(rawParts...)
	return agg, nil
}

func batchInput(in ChatInput, messages []domain.MessageRecord) ChatInput {
	out := in
	out.Messages = messages
	userSet := make(map[int64]struct{}, len(messages)*2)
	msgIDs := make(map[int]struct{}, len(messages))
	for _, m := range messages {
		if m.FromUserID > 0 {
			userSet[m.FromUserID] = struct{}{}
		}
		msgIDs[m.MessageID] = struct{}{}
		for _, uid := range m.MentionsUserIDs {
			if uid > 0 {
				userSet[uid] = struct{}{}
			}
		}
	}

	out.Users = filterUsers(in.Users, userSet)
	out.Personas = filterPersonas(in.Personas, userSet)
	out.Edges = filterEdges(in.Edges, userSet)
	out.Topics = filterTopics(in.Topics, userSet, msgIDs)
	out.Mindmap = in.Mindmap
	return out
}

func filterUsers(users []domain.UserStats, userSet map[int64]struct{}) []domain.UserStats {
	if len(users) == 0 || len(userSet) == 0 {
		return users
	}
	out := make([]domain.UserStats, 0, len(users))
	for _, u := range users {
		if _, ok := userSet[u.UserID]; ok {
			out = append(out, u)
		}
	}
	if len(out) == 0 {
		return users
	}
	return out
}

func filterPersonas(personas []domain.Persona, userSet map[int64]struct{}) []domain.Persona {
	if len(personas) == 0 || len(userSet) == 0 {
		return personas
	}
	out := make([]domain.Persona, 0, len(personas))
	for _, p := range personas {
		if _, ok := userSet[p.UserID]; ok {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return personas
	}
	return out
}

func filterEdges(edges []domain.RelationEdge, userSet map[int64]struct{}) []domain.RelationEdge {
	if len(edges) == 0 || len(userSet) == 0 {
		return edges
	}
	out := make([]domain.RelationEdge, 0, len(edges))
	for _, e := range edges {
		_, fromOK := userSet[e.FromUserID]
		_, toOK := userSet[e.ToUserID]
		if fromOK || toOK {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return edges
	}
	return out
}

func filterTopics(topics []domain.Topic, userSet map[int64]struct{}, msgIDs map[int]struct{}) []domain.Topic {
	if len(topics) == 0 {
		return topics
	}
	out := make([]domain.Topic, 0, len(topics))
	for _, t := range topics {
		include := false
		for _, msgID := range t.MessageIDs {
			if _, ok := msgIDs[msgID]; ok {
				include = true
				break
			}
		}
		if !include {
			for _, uid := range t.UserIDs {
				if _, ok := userSet[uid]; ok {
					include = true
					break
				}
			}
		}
		if include {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return topics
	}
	return out
}

func mergeMindmapParts(base, extra domain.Mindmap) domain.Mindmap {
	if len(extra.Nodes) == 0 && len(extra.Edges) == 0 {
		return base
	}
	nodeSeen := make(map[string]struct{}, len(base.Nodes)+len(extra.Nodes))
	mergedNodes := make([]domain.MindmapNode, 0, len(base.Nodes)+len(extra.Nodes))
	for _, n := range append(base.Nodes, extra.Nodes...) {
		key := n.ID + "|" + n.Type
		if _, ok := nodeSeen[key]; ok {
			continue
		}
		nodeSeen[key] = struct{}{}
		mergedNodes = append(mergedNodes, n)
	}
	edgeSeen := make(map[string]struct{}, len(base.Edges)+len(extra.Edges))
	mergedEdges := make([]domain.MindmapEdge, 0, len(base.Edges)+len(extra.Edges))
	for _, e := range append(base.Edges, extra.Edges...) {
		key := e.From + "|" + e.To + "|" + e.Label
		if _, ok := edgeSeen[key]; ok {
			continue
		}
		edgeSeen[key] = struct{}{}
		mergedEdges = append(mergedEdges, e)
	}
	return domain.Mindmap{Nodes: mergedNodes, Edges: mergedEdges}
}

func relationKey(rel domain.RelationInsight) string {
	return strconv.FormatInt(rel.FromUserID, 10) + "|" + strconv.FormatInt(rel.ToUserID, 10) + "|" + strings.TrimSpace(strings.ToLower(rel.Label))
}

func appendUniqueString(in []string, value string) []string {
	for _, existing := range in {
		if existing == value {
			return in
		}
	}
	return append(in, value)
}

func (c *Client) enrichSingle(ctx context.Context, in ChatInput, digDeeper bool) (ChatEnrichment, error) {
	model := c.pickModel(taskOverview, digDeeper)
	req := map[string]any{
		"account_id": in.AccountID,
		"chat_id":    in.ChatID,
		"chat_stats": in.ChatStats,
		"messages":   in.Messages,
		"users":      in.Users,
		"topics":     in.Topics,
		"personas":   in.Personas,
		"relations":  in.Edges,
		"mindmap":    in.Mindmap,
	}
	raw, err := c.completeJSON(ctx, model, singleSystemPrompt(digDeeper), req)
	if err != nil {
		return ChatEnrichment{}, err
	}

	parsed := singleResponse{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ChatEnrichment{}, fmt.Errorf("decode single enrichment: %w", err)
	}

	out := ChatEnrichment{
		UsedModels:       []string{model},
		ChatSummary:      parsed.ChatSummary,
		TopicPatches:     make(map[string]TopicPatch, len(parsed.Topics)),
		PersonaPatches:   make(map[int64]PersonaPatch, len(parsed.Personas)),
		RelationInsights: parsed.Relations,
		Mindmap: domain.Mindmap{
			Nodes: parsed.Mindmap.Nodes,
			Edges: parsed.Mindmap.Edges,
		},
		Raw: raw,
	}
	for _, t := range parsed.Topics {
		if strings.TrimSpace(t.TopicID) == "" {
			continue
		}
		out.TopicPatches[t.TopicID] = TopicPatch{
			Label:      t.Label,
			Summary:    t.Summary,
			Confidence: clamp01(t.Confidence),
		}
	}
	for _, p := range parsed.Personas {
		if p.UserID == 0 {
			continue
		}
		out.PersonaPatches[p.UserID] = PersonaPatch{
			Role:           p.Role,
			Tone:           p.Tone,
			Traits:         p.Traits,
			Interests:      p.Interests,
			Triggers:       p.Triggers,
			TypicalPhrases: p.TypicalPhrases,
			Summary:        p.Summary,
			Confidence:     clamp01(p.Confidence),
			Note:           p.Note,
		}
	}

	return out, nil
}

func (c *Client) enrichPerTask(ctx context.Context, in ChatInput, digDeeper bool) (ChatEnrichment, error) {
	out := ChatEnrichment{
		TopicPatches:   map[string]TopicPatch{},
		PersonaPatches: map[int64]PersonaPatch{},
	}

	overviewModel := c.pickModel(taskOverview, digDeeper)
	overviewRaw, err := c.completeJSON(ctx, overviewModel, overviewPrompt(), map[string]any{
		"account_id": in.AccountID,
		"chat_id":    in.ChatID,
		"chat_stats": in.ChatStats,
		"messages":   in.Messages,
		"users":      in.Users,
		"topics":     in.Topics,
	})
	if err != nil {
		return ChatEnrichment{}, err
	}
	overview := overviewResponse{}
	if err := json.Unmarshal(overviewRaw, &overview); err != nil {
		return ChatEnrichment{}, fmt.Errorf("decode overview: %w", err)
	}
	out.UsedModels = append(out.UsedModels, overviewModel)
	out.ChatSummary = overview.ChatSummary
	for _, t := range overview.Topics {
		if strings.TrimSpace(t.TopicID) == "" {
			continue
		}
		out.TopicPatches[t.TopicID] = TopicPatch{
			Label:      t.Label,
			Summary:    t.Summary,
			Confidence: clamp01(t.Confidence),
		}
	}

	personaModel := c.pickModel(taskPersona, digDeeper)
	personaRaw, err := c.completeJSON(ctx, personaModel, personaPrompt(digDeeper), map[string]any{
		"messages": in.Messages,
		"personas": in.Personas,
		"users":    in.Users,
	})
	if err != nil {
		return ChatEnrichment{}, err
	}
	personas := personaResponse{}
	if err := json.Unmarshal(personaRaw, &personas); err != nil {
		return ChatEnrichment{}, fmt.Errorf("decode personas: %w", err)
	}
	out.UsedModels = append(out.UsedModels, personaModel)
	for _, p := range personas.Personas {
		if p.UserID == 0 {
			continue
		}
		out.PersonaPatches[p.UserID] = PersonaPatch{
			Role:           p.Role,
			Tone:           p.Tone,
			Traits:         p.Traits,
			Interests:      p.Interests,
			Triggers:       p.Triggers,
			TypicalPhrases: p.TypicalPhrases,
			Summary:        p.Summary,
			Confidence:     clamp01(p.Confidence),
			Note:           p.Note,
		}
	}

	relationModel := c.pickModel(taskRelation, digDeeper)
	relationRaw, err := c.completeJSON(ctx, relationModel, relationPrompt(), map[string]any{
		"messages":  in.Messages,
		"relations": in.Edges,
		"users":     in.Users,
	})
	if err != nil {
		return ChatEnrichment{}, err
	}
	relations := relationResponse{}
	if err := json.Unmarshal(relationRaw, &relations); err != nil {
		return ChatEnrichment{}, fmt.Errorf("decode relations: %w", err)
	}
	out.UsedModels = append(out.UsedModels, relationModel)
	out.RelationInsights = relations.Relations

	mindmapModel := c.pickModel(taskMindmap, digDeeper)
	mindmapRaw, err := c.completeJSON(ctx, mindmapModel, mindmapPrompt(), map[string]any{
		"mindmap":  in.Mindmap,
		"messages": in.Messages,
		"topics":   in.Topics,
		"users":    in.Users,
	})
	if err != nil {
		return ChatEnrichment{}, err
	}
	mindmap := mindmapResponse{}
	if err := json.Unmarshal(mindmapRaw, &mindmap); err != nil {
		return ChatEnrichment{}, fmt.Errorf("decode mindmap: %w", err)
	}
	out.UsedModels = append(out.UsedModels, mindmapModel)
	out.Mindmap = domain.Mindmap{Nodes: mindmap.Nodes, Edges: mindmap.Edges}

	out.Raw = mergeRaw(overviewRaw, personaRaw, relationRaw, mindmapRaw)
	return out, nil
}

type taskName string

const (
	taskOverview taskName = "overview"
	taskPersona  taskName = "persona"
	taskRelation taskName = "relation"
	taskMindmap  taskName = "mindmap"
)

func (c *Client) pickModel(task taskName, digDeeper bool) string {
	if digDeeper && strings.TrimSpace(c.cfg.DigDeeperModel) != "" {
		if task == taskPersona || !strings.EqualFold(c.cfg.RoutingMode, "per-task") {
			return strings.TrimSpace(c.cfg.DigDeeperModel)
		}
	}
	if strings.EqualFold(c.cfg.RoutingMode, "per-task") {
		switch task {
		case taskPersona:
			if v := strings.TrimSpace(c.cfg.PersonaModel); v != "" {
				return v
			}
		case taskOverview:
			if v := strings.TrimSpace(c.cfg.TopicModel); v != "" {
				return v
			}
		case taskRelation:
			if v := strings.TrimSpace(c.cfg.RelationModel); v != "" {
				return v
			}
		case taskMindmap:
			if v := strings.TrimSpace(c.cfg.MindmapModel); v != "" {
				return v
			}
		}
	}
	return strings.TrimSpace(c.cfg.Model)
}

type chatRequest struct {
	Model          string         `json:"model"`
	Messages       []chatMessage  `json:"messages"`
	Temperature    float64        `json:"temperature,omitempty"`
	MaxTokens      int            `json:"max_tokens,omitempty"`
	ResponseFormat responseFormat `json:"response_format"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`

	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (c *Client) completeJSON(ctx context.Context, model, system string, payload any) (json.RawMessage, error) {
	payloadBudget, outputBudget := c.requestBudgets(model, system)
	b, truncated, err := compactPayloadToBudget(payload, payloadBudget)
	if err != nil {
		return nil, err
	}
	if truncated {
		if compact, ok := markInputTruncated(b); ok {
			b = compact
		}
	}
	reqBody := chatRequest{
		Model: model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: string(b)},
		},
		Temperature: c.cfg.Temperature,
		MaxTokens:   outputBudget,
		ResponseFormat: responseFormat{
			Type: "json_object",
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := c.cfg.BaseURL + "/chat/completions"
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxRetries+1; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.cfg.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt <= c.cfg.MaxRetries {
				if !sleepContext(ctx, backoff(attempt)) {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}

		bodyData, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if attempt <= c.cfg.MaxRetries {
				if !sleepContext(ctx, backoff(attempt)) {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}

		if resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("llm status %d: %s", resp.StatusCode, truncate(string(bodyData), 512))
			if resp.StatusCode >= 500 && attempt <= c.cfg.MaxRetries {
				if !sleepContext(ctx, backoff(attempt)) {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}

		parsed := chatResponse{}
		if err := json.Unmarshal(bodyData, &parsed); err != nil {
			return nil, fmt.Errorf("decode llm response: %w", err)
		}
		if parsed.Error != nil && parsed.Error.Message != "" {
			lastErr = errors.New(parsed.Error.Message)
			if attempt <= c.cfg.MaxRetries {
				if !sleepContext(ctx, backoff(attempt)) {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}
		if len(parsed.Choices) == 0 {
			return nil, errors.New("llm response has no choices")
		}
		raw := strings.TrimSpace(parsed.Choices[0].Message.Content)
		if raw == "" {
			return nil, errors.New("llm response content is empty")
		}
		extracted, err := extractJSONObject(raw)
		if err != nil {
			return nil, err
		}
		return extracted, nil
	}
	if lastErr == nil {
		lastErr = errors.New("llm request failed")
	}
	return nil, lastErr
}

func extractJSONObject(raw string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(raw)
	if json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed), nil
	}

	start := strings.IndexByte(trimmed, '{')
	end := strings.LastIndexByte(trimmed, '}')
	if start < 0 || end <= start {
		return nil, errors.New("llm output is not valid json")
	}
	candidate := strings.TrimSpace(trimmed[start : end+1])
	if !json.Valid([]byte(candidate)) {
		return nil, errors.New("llm output json fragment invalid")
	}
	return json.RawMessage(candidate), nil
}

func mergeRaw(parts ...json.RawMessage) json.RawMessage {
	m := map[string]json.RawMessage{}
	for i, p := range parts {
		if len(p) == 0 {
			continue
		}
		m[strconv.Itoa(i)] = p
	}
	b, _ := json.Marshal(m)
	return b
}

func (c *Client) requestBudgets(model, system string) (payloadTokens, outputTokens int) {
	inputCap := c.cfg.DefaultInputTokens
	outputCap := c.cfg.DefaultOutputTokens
	if cap, ok := c.capacityForModel(model); ok {
		if cap.InputTokens > 0 {
			inputCap = cap.InputTokens
		}
		if cap.OutputTokens > 0 {
			outputCap = cap.OutputTokens
		}
	}

	available := inputCap - estimateTokens(system) - c.cfg.SafetyMarginTokens
	if available < 256 {
		available = 256
	}

	minOutput := c.cfg.MinOutputTokens
	if minOutput < 64 {
		minOutput = 64
	}
	maxOutputAllowed := available - 128
	if maxOutputAllowed < minOutput {
		maxOutputAllowed = minOutput
	}
	output := outputCap
	if output <= 0 {
		output = minOutput
	}
	if output > maxOutputAllowed {
		output = maxOutputAllowed
	}
	if output < minOutput && outputCap <= 0 {
		output = minOutput
	}
	if output < 1 {
		output = 1
	}

	payload := available - output
	if payload < 128 {
		payload = 128
	}
	return payload, output
}

func (c *Client) capacityForModel(model string) (modelCapacity, bool) {
	if len(c.modelCaps) == 0 {
		return modelCapacity{}, false
	}
	key := normalizeModelKey(model)
	candidates := []string{key}
	if idx := strings.LastIndexByte(key, '/'); idx >= 0 && idx+1 < len(key) {
		candidates = append(candidates, key[idx+1:])
	}
	if idx := strings.LastIndexByte(key, ':'); idx >= 0 && idx+1 < len(key) {
		candidates = append(candidates, key[idx+1:])
	}
	if idx := strings.LastIndexByte(key, '@'); idx > 0 {
		candidates = append(candidates, key[:idx])
	}
	for _, cand := range candidates {
		if cap, ok := c.modelCaps[cand]; ok {
			return cap, true
		}
	}
	return modelCapacity{}, false
}

func normalizeModelKey(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func estimateTokens(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	runes := utf8.RuneCountInString(s)
	if runes <= 0 {
		return 0
	}
	// Coarse token estimator for multilingual text and JSON payloads.
	return runes/4 + 1
}

func compactPayloadToBudget(payload any, maxTokens int) ([]byte, bool, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	if maxTokens <= 0 || estimateTokens(string(raw)) <= maxTokens {
		return raw, false, nil
	}

	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return compactFallback(raw, maxTokens), true, nil
	}

	candidate := generic
	for i := 0; i < 24; i++ {
		shrunk, changed := shrinkJSONAny(candidate)
		if !changed {
			break
		}
		candidate = shrunk
		b, err := json.Marshal(candidate)
		if err != nil {
			break
		}
		if estimateTokens(string(b)) <= maxTokens {
			return b, true, nil
		}
	}

	return compactFallback(raw, maxTokens), true, nil
}

func compactFallback(raw []byte, maxTokens int) []byte {
	maxChars := maxTokens * 4
	if maxChars < 160 {
		maxChars = 160
	}
	excerpt := string(raw)
	if len(excerpt) > maxChars {
		excerpt = excerpt[:maxChars]
	}
	b, _ := json.Marshal(map[string]any{
		"_input_truncated": true,
		"excerpt":          excerpt,
	})
	return b
}

func markInputTruncated(raw []byte) ([]byte, bool) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw, false
	}
	m["_input_truncated"] = true
	b, err := json.Marshal(m)
	if err != nil {
		return raw, false
	}
	return b, true
}

func shrinkJSONAny(v any) (any, bool) {
	switch cur := v.(type) {
	case map[string]any:
		priority := []string{"messages", "users", "topics", "personas", "relations", "edges", "nodes", "recent_messages", "topic_contexts", "persona_contexts"}
		for _, key := range priority {
			val, ok := cur[key]
			if !ok {
				continue
			}
			if shrunk, changed := shrinkPreferredValue(val); changed {
				cur[key] = shrunk
				return cur, true
			}
		}
		for k, val := range cur {
			shrunk, changed := shrinkJSONAny(val)
			if changed {
				cur[k] = shrunk
				return cur, true
			}
		}
		return cur, false
	case []any:
		if len(cur) > 1 {
			nextLen := (len(cur) + 1) / 2
			if nextLen < 1 {
				nextLen = 1
			}
			return cur[:nextLen], true
		}
		if len(cur) == 1 {
			shrunk, changed := shrinkJSONAny(cur[0])
			if changed {
				cur[0] = shrunk
				return cur, true
			}
		}
		return cur, false
	case string:
		if len(cur) > 256 {
			return cur[:len(cur)/2], true
		}
		return cur, false
	default:
		return v, false
	}
}

func shrinkPreferredValue(v any) (any, bool) {
	switch cur := v.(type) {
	case []any:
		if len(cur) > 1 {
			nextLen := (len(cur) + 1) / 2
			if nextLen < 1 {
				nextLen = 1
			}
			return cur[:nextLen], true
		}
	case string:
		if len(cur) > 256 {
			return cur[:len(cur)/2], true
		}
	}
	return shrinkJSONAny(v)
}

func resolveModelInfoURL(baseURL, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", fmt.Errorf("parse model info url: %w", err)
		}
		return u.String(), nil
	}
	if strings.TrimSpace(baseURL) == "" {
		return "", errors.New("llm base url is required when model info url is relative")
	}
	base := strings.TrimSuffix(strings.TrimSpace(baseURL), "/")
	if strings.HasPrefix(raw, "/") {
		return base + raw, nil
	}
	return base + "/" + strings.TrimPrefix(raw, "/"), nil
}

func loadModelCapsFromURL(ctx context.Context, httpClient *http.Client, endpoint, apiKey string) (map[string]modelCapacity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("model info status %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	return parseModelCapsJSON(body)
}

func loadModelCapsFromFile(path string) (map[string]modelCapacity, error) {
	b, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	return parseModelCapsJSON(b)
}

func mergeModelCaps(dst map[string]modelCapacity, src map[string]modelCapacity) {
	if len(src) == 0 {
		return
	}
	for k, v := range src {
		if v.InputTokens <= 0 && v.OutputTokens <= 0 {
			continue
		}
		dst[normalizeModelKey(k)] = v
	}
}

func parseModelCapsJSON(raw []byte) (map[string]modelCapacity, error) {
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("decode model info json: %w", err)
	}
	out := map[string]modelCapacity{}
	collectModelCaps(data, out, 0)
	if len(out) == 0 {
		return nil, errors.New("model info does not contain token capacities")
	}
	return out, nil
}

func collectModelCaps(v any, out map[string]modelCapacity, depth int) {
	if depth > 8 || v == nil {
		return
	}
	switch cur := v.(type) {
	case []any:
		for _, item := range cur {
			collectModelCaps(item, out, depth+1)
		}
	case map[string]any:
		modelID := ""
		for _, key := range []string{"id", "model", "name", "slug"} {
			if s, ok := cur[key].(string); ok && strings.TrimSpace(s) != "" {
				modelID = strings.TrimSpace(s)
				break
			}
		}
		input := pickInt(cur, "max_input_tokens", "max_prompt_tokens", "input_tokens", "context_length", "max_context_tokens", "context_window")
		output := pickInt(cur, "max_output_tokens", "max_completion_tokens", "output_tokens", "completion_tokens", "generated_tokens")
		if modelID != "" && (input > 0 || output > 0) {
			out[normalizeModelKey(modelID)] = modelCapacity{InputTokens: input, OutputTokens: output}
		}

		for key, val := range cur {
			if nested, ok := val.(map[string]any); ok && likelyModelKey(key) {
				in2 := pickInt(nested, "max_input_tokens", "max_prompt_tokens", "input_tokens", "context_length", "max_context_tokens", "context_window")
				out2 := pickInt(nested, "max_output_tokens", "max_completion_tokens", "output_tokens", "completion_tokens", "generated_tokens")
				if in2 > 0 || out2 > 0 {
					out[normalizeModelKey(key)] = modelCapacity{InputTokens: in2, OutputTokens: out2}
				}
			}
			collectModelCaps(val, out, depth+1)
		}
	}
}

func likelyModelKey(key string) bool {
	k := normalizeModelKey(key)
	if k == "" {
		return false
	}
	containers := map[string]struct{}{
		"data": {}, "models": {}, "model": {}, "result": {}, "results": {}, "items": {}, "meta": {}, "metadata": {}, "capabilities": {},
	}
	if _, ok := containers[k]; ok {
		return false
	}
	if strings.Contains(k, "token") {
		return false
	}
	return true
}

func pickInt(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if n, ok := intFromAny(v); ok {
				return n
			}
		}
	}
	return 0
}

func intFromAny(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		if n > 0 {
			return int(n), true
		}
	case int:
		if n > 0 {
			return n, true
		}
	case int64:
		if n > 0 {
			return int(n), true
		}
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err == nil && i > 0 {
			return i, true
		}
	}
	return 0, false
}

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
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

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Second * time.Duration(1<<(attempt-1))
	if d > 8*time.Second {
		d = 8 * time.Second
	}
	return d
}

type singleResponse struct {
	ChatSummary string `json:"chat_summary"`
	Topics      []struct {
		TopicID    string  `json:"topic_id"`
		Label      string  `json:"label"`
		Summary    string  `json:"summary"`
		Confidence float64 `json:"confidence"`
	} `json:"topics"`
	Personas []struct {
		UserID         int64    `json:"user_id"`
		Role           string   `json:"role"`
		Tone           string   `json:"tone"`
		Traits         []string `json:"traits"`
		Interests      []string `json:"interests"`
		Triggers       []string `json:"triggers"`
		TypicalPhrases []string `json:"typical_phrases"`
		Summary        string   `json:"summary"`
		Confidence     float64  `json:"confidence"`
		Note           string   `json:"note"`
	} `json:"personas"`
	Relations []domain.RelationInsight `json:"relations"`
	Mindmap   struct {
		Nodes []domain.MindmapNode `json:"nodes"`
		Edges []domain.MindmapEdge `json:"edges"`
	} `json:"mindmap"`
}

type overviewResponse struct {
	ChatSummary string `json:"chat_summary"`
	Topics      []struct {
		TopicID    string  `json:"topic_id"`
		Label      string  `json:"label"`
		Summary    string  `json:"summary"`
		Confidence float64 `json:"confidence"`
	} `json:"topics"`
}

type personaResponse struct {
	Personas []struct {
		UserID         int64    `json:"user_id"`
		Role           string   `json:"role"`
		Tone           string   `json:"tone"`
		Traits         []string `json:"traits"`
		Interests      []string `json:"interests"`
		Triggers       []string `json:"triggers"`
		TypicalPhrases []string `json:"typical_phrases"`
		Summary        string   `json:"summary"`
		Confidence     float64  `json:"confidence"`
		Note           string   `json:"note"`
	} `json:"personas"`
}

type relationResponse struct {
	Relations []domain.RelationInsight `json:"relations"`
}

type mindmapResponse struct {
	Nodes []domain.MindmapNode `json:"nodes"`
	Edges []domain.MindmapEdge `json:"edges"`
}

func singleSystemPrompt(digDeeper bool) string {
	if digDeeper {
		return "You are a principal socio-linguistic analyst for Telegram communities. Work only with provided evidence; do not invent facts. Return ONLY one valid JSON object with fields: chat_summary(string), topics(array of {topic_id,label,summary,confidence}), personas(array of {user_id,role,tone,traits[],interests[],triggers[],typical_phrases[],summary,confidence,note}), relations(array of {from_user_id,to_user_id,label,reason,confidence}), mindmap({nodes:[{id,label,type,group,weight}],edges:[{from,to,label,weight}]}). Requirements: 1) confidence in [0,1], 2) prefer concise labels, 3) keep arrays focused (no duplicates), 4) if evidence is weak use conservative wording and lower confidence, 5) output must be strict JSON without markdown/code fences/comments."
	}
	return "You are an expert chat analytics assistant. Use only the input data and avoid hallucinations. Return ONLY one valid JSON object with fields: chat_summary(string), topics(array of {topic_id,label,summary,confidence}), personas(array of {user_id,role,tone,traits[],interests[],triggers[],typical_phrases[],summary,confidence,note}), relations(array of {from_user_id,to_user_id,label,reason,confidence}), mindmap({nodes:[{id,label,type,group,weight}],edges:[{from,to,label,weight}]}). Rules: confidence in [0,1], no duplicate nodes/edges, no prose outside JSON, no markdown."
}

func overviewPrompt() string {
	return "Return ONLY a JSON object: {chat_summary:string, topics:[{topic_id,label,summary,confidence}]}. Keep topic labels short, summaries evidence-based, and confidence in [0,1]. If uncertain, reduce confidence instead of guessing."
}

func personaPrompt(digDeeper bool) string {
	if digDeeper {
		return "Return ONLY JSON object with field personas: [{user_id,role,tone,traits[],interests[],triggers[],typical_phrases[],summary,confidence,note}]. Deep mode: infer stable behavior patterns, conflict triggers, and communication style from evidence only. confidence in [0,1], concise but specific note, no duplicates."
	}
	return "Return ONLY JSON object with field personas: [{user_id,role,tone,traits[],interests[],triggers[],typical_phrases[],summary,confidence,note}]. Use neutral wording, confidence in [0,1], avoid unsupported claims."
}

func relationPrompt() string {
	return "Return ONLY JSON object with field relations: [{from_user_id,to_user_id,label,reason,confidence}]. Include relations only when supported by interaction signals. confidence in [0,1], reason should be short and factual."
}

func mindmapPrompt() string {
	return "Return ONLY JSON object with fields nodes and edges. nodes:[{id,label,type,group,weight}], edges:[{from,to,label,weight}]. Keep graph compact, deterministic IDs, and remove duplicate edges."
}

func askPrompt() string {
	return "You are an analyst assistant for Telegram data. Return ONLY JSON object: {answer:string, confidence:number, bullets:[string]}. Use only provided context, no hallucinations. If evidence is weak, say uncertainty. confidence must be in [0,1]."
}
