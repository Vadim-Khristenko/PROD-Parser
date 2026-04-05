package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/joho/godotenv"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/analysis"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/dashboard"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/logging"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/pipeline"
	outputschema "github.com/Vadim-Khristenko/PROD-Parser/internal/schema"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/telegramingest"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	loadDotEnv()

	log, cleanup, err := logging.NewFromEnv()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = cleanup()
	}()

	cmd := os.Args[1]
	var runErr error
	var failMsg string
	switch cmd {
	case "ingest":
		runErr = runIngest(log.With(zap.String("cmd", "ingest")), os.Args[2:])
		failMsg = "ingest failed"
	case "search":
		runErr = runSearch(log.With(zap.String("cmd", "search")), os.Args[2:])
		failMsg = "search failed"
	case "snapshot":
		runErr = runSnapshot(log.With(zap.String("cmd", "snapshot")), os.Args[2:])
		failMsg = "snapshot failed"
	case "participants":
		runErr = runParticipants(log.With(zap.String("cmd", "participants")), os.Args[2:])
		failMsg = "participants export failed"
	case "user-snapshot":
		runErr = runUserSnapshot(log.With(zap.String("cmd", "user-snapshot")), os.Args[2:])
		failMsg = "user snapshot export failed"
	case "ask":
		runErr = runAsk(log.With(zap.String("cmd", "ask")), os.Args[2:])
		failMsg = "ask failed"
	case "range":
		runErr = runRange(log.With(zap.String("cmd", "range")), os.Args[2:])
		failMsg = "range query failed"
	case "dashboard":
		runErr = runDashboard(log.With(zap.String("cmd", "dashboard")), os.Args[2:])
		failMsg = "dashboard failed"
	case "analyze-all":
		runErr = runAnalyzeAll(log.With(zap.String("cmd", "analyze-all")), os.Args[2:])
		failMsg = "analyze-all failed"
	case "schema":
		runErr = runSchema(log.With(zap.String("cmd", "schema")), os.Args[2:])
		failMsg = "schema generation failed"
	case "tg-fetch":
		runErr = runTGFetch(log.With(zap.String("cmd", "tg-fetch")), os.Args[2:])
		failMsg = "tg-fetch failed"
	default:
		usage()
		os.Exit(2)
	}

	if runErr != nil {
		log.Error(failMsg, zap.Error(runErr))
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("prod-parser <ingest|search|snapshot|participants|user-snapshot|ask|range|dashboard|analyze-all|schema|tg-fetch> [flags]")
	fmt.Println("  ingest       --state ./data --input ./messages.jsonl --batch 500")
	fmt.Println("  search       --state ./data --query \"keyword\" --limit 20")
	fmt.Println("  snapshot     --state ./data --account acc1 --chat 123 --profile fast")
	fmt.Println("  participants --state ./data --account acc1 --chat 123 --out ./data/exports/users")
	fmt.Println("  user-snapshot --state ./data --account acc1 --chat 123 --user-id 777")
	fmt.Println("  ask          --state ./data --account acc1 --chat 123 --user @username --question \"...\" --llm")
	fmt.Println("  range        --state ./data --account acc1 --chat 123 --from ... --to ... --weekday 1 --yearday 120")
	fmt.Println("  dashboard    --state ./data --host 127.0.0.1 --port 8787")
	fmt.Println("  analyze-all  --state ./data --account acc1 --chat 123 --profile full --participants --with-schema")
	fmt.Println("  schema       --out ./data/exports/schemas --pretty")
	fmt.Println("  tg-fetch     --state ./data --account acc1 --api-id 123 --api-hash ... --peer @channel")
	fmt.Println("               note: Telegram ingest requires build tag: -tags telegram")
}

func runIngest(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	input := fs.String("input", "", "input JSONL path")
	batchSize := fs.Int("batch", 500, "batch size")
	accountOverride := fs.String("account-override", "", "rewrite account_id for all imported messages")
	chatOverride := fs.Int64("chat-override", 0, "rewrite chat_id for all imported messages")
	fileMaxMessages := fs.Int("file-max-messages", envInt("JSONL_FILE_MAX_MESSAGES", 5000), "max messages per JSONL file segment (0 = single file)")
	topicModeRaw := fs.String("topic-mode", "heuristic", "topic mode: heuristic|embedding|llm-fallback")
	embeddingModel := fs.String("embedding-model", "", "embedding model name")
	llmFallbackModel := fs.String("llm-fallback-model", "", "llm fallback model")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		return errors.New("--input is required")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	engine.ConfigureStorage(*fileMaxMessages)
	engine.ConfigureTopicDetection(parseTopicMode(*topicModeRaw), *embeddingModel, *llmFallbackModel)

	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	started := time.Now()
	ctx := context.Background()
	batch := make([]domain.MessageRecord, 0, *batchSize)
	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		var msg domain.MessageRecord
		if err := json.Unmarshal(line, &msg); err != nil {
			return fmt.Errorf("unmarshal line %d: %w", count+1, err)
		}
		if strings.TrimSpace(*accountOverride) != "" {
			msg.AccountID = strings.TrimSpace(*accountOverride)
		}
		if *chatOverride != 0 {
			msg.ChatID = *chatOverride
		}
		if msg.Date.IsZero() {
			msg.Date = time.Now().UTC()
		}
		batch = append(batch, msg)
		if len(batch) >= *batchSize {
			if err := engine.ProcessBatch(ctx, batch); err != nil {
				return err
			}
			count += len(batch)
			log.Info("ingest batch committed", zap.Int("count", count))
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := engine.ProcessBatch(ctx, batch); err != nil {
			return err
		}
		count += len(batch)
	}

	log.Info("ingest done",
		zap.Int("messages", count),
		zap.Duration("elapsed", time.Since(started)),
	)
	return nil
}

func runSearch(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	query := fs.String("query", "", "search query")
	limit := fs.Int("limit", 20, "limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *query == "" {
		return errors.New("--query is required")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()

	results, err := engine.Search(context.Background(), *query, *limit)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		return err
	}
	log.Info("search output emitted", zap.Int("results", len(results)))
	return nil
}

func runSnapshot(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	chat := fs.Int64("chat", 0, "chat id")
	profileRaw := fs.String("profile", envString("SNAPSHOT_PROFILE", "fast"), "snapshot profile: fast|balanced|full")
	maxEdges := fs.Int("max-edges", envInt("SNAPSHOT_MAX_EDGES", 0), "max relation edges in snapshot (0 = profile default)")
	pretty := fs.Bool("pretty", envBool("SNAPSHOT_PRETTY", false), "pretty JSON output (slower, larger file)")
	heartbeatSec := fs.Int("heartbeat-sec", envInt("SNAPSHOT_HEARTBEAT_SEC", 20), "heartbeat interval in seconds while snapshot is running (0 = disabled)")
	llmCfg := addLLMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *chat == 0 {
		return errors.New("--account and --chat are required")
	}

	chatDir := filepath.Join(*state, "jsonl", *account, strconv.FormatInt(*chat, 10))
	legacyPath := filepath.Join(chatDir, "messages.jsonl")
	legacyFound := false
	if st, err := os.Stat(legacyPath); err == nil && !st.IsDir() {
		legacyFound = true
		log.Info("snapshot input legacy file detected",
			zap.String("path", legacyPath),
			zap.Int64("bytes", st.Size()),
		)
	}
	segmentPaths, segErr := filepath.Glob(filepath.Join(chatDir, "messages_*.jsonl"))
	if segErr == nil && len(segmentPaths) > 0 {
		log.Info("snapshot input segment files detected", zap.Int("segments", len(segmentPaths)))
	}
	if !legacyFound && (segErr != nil || len(segmentPaths) == 0) {
		log.Warn("snapshot input files were not found",
			zap.String("chat_dir", chatDir),
			zap.String("legacy", legacyPath),
		)
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	stopHeartbeat := startHeartbeat(log, "snapshot", time.Duration(*heartbeatSec)*time.Second)
	defer stopHeartbeat()
	log.Info("snapshot started",
		zap.String("account_id", *account),
		zap.Int64("chat_id", *chat),
		zap.String("profile", *profileRaw),
		zap.Int("heartbeat_sec", *heartbeatSec),
	)

	opts := pipeline.SnapshotOptionsFromProfile(pipeline.ParseSnapshotProfile(*profileRaw))
	if *maxEdges > 0 {
		opts.MaxEdges = *maxEdges
	}
	opts.PrettyOutput = *pretty
	if llmCfg != nil && llmCfg.enabled != nil && *llmCfg.enabled {
		opts.IncludeLLM = true
	}

	path, err := engine.SaveChatSnapshotWithOptions(*account, *chat, opts)
	if err != nil {
		return err
	}
	stopHeartbeat()
	fmt.Println(path)
	log.Info("snapshot written", zap.String("path", path))
	return nil
}

func runParticipants(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("participants", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	chat := fs.Int64("chat", 0, "chat id")
	out := fs.String("out", "", "output directory")
	recent := fs.Int("recent", 200, "recent messages per participant")
	llmCfg := addLLMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *chat == 0 {
		return errors.New("--account and --chat are required")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	paths, err := engine.SaveParticipantsSnapshots(*account, *chat, *out, *recent)
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(paths, "\n"))
	log.Info("participants exported", zap.Int("files", len(paths)))
	return nil
}

func runUserSnapshot(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("user-snapshot", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	chat := fs.Int64("chat", 0, "chat id")
	userID := fs.Int64("user-id", 0, "target user id")
	username := fs.String("user", "", "target username, e.g. @name")
	profileRaw := fs.String("profile", envString("USER_SNAPSHOT_PROFILE", "fast"), "snapshot profile: fast|balanced|full")
	maxEdges := fs.Int("max-edges", envInt("USER_SNAPSHOT_MAX_EDGES", 0), "max relation edges in snapshot (0 = profile default)")
	pretty := fs.Bool("pretty", envBool("USER_SNAPSHOT_PRETTY", true), "pretty JSON output")
	out := fs.String("out", "", "output file path")
	recent := fs.Int("recent", 200, "recent messages limit")
	llmCfg := addLLMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *chat == 0 {
		return errors.New("--account and --chat are required")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	resolvedUserID, err := resolveTargetUserID(engine, *account, *chat, *userID, *username)
	if err != nil {
		return err
	}

	opts := pipeline.SnapshotOptionsFromProfile(pipeline.ParseSnapshotProfile(*profileRaw))
	if *maxEdges > 0 {
		opts.MaxEdges = *maxEdges
	}
	opts.PrettyOutput = *pretty
	if llmCfg != nil && llmCfg.enabled != nil && *llmCfg.enabled {
		opts.IncludeLLM = true
	}

	path, err := engine.SaveParticipantSnapshotWithOptions(*account, *chat, resolvedUserID, strings.TrimSpace(*out), *recent, opts)
	if err != nil {
		return err
	}
	fmt.Println(path)
	log.Info("user snapshot written",
		zap.Int64("chat_id", *chat),
		zap.Int64("user_id", resolvedUserID),
		zap.String("path", path),
	)
	return nil
}

func runAsk(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("ask", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	chat := fs.Int64("chat", 0, "chat id")
	userID := fs.Int64("user-id", 0, "optional target user id")
	username := fs.String("user", "", "optional target username, e.g. @name")
	question := fs.String("question", "", "question for LLM")
	askTimeoutMS := fs.Int("ask-timeout-ms", envInt("ASK_TIMEOUT_MS", 240000), "overall ask command timeout in ms")
	llmCfg := addLLMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *chat == 0 {
		return errors.New("--account and --chat are required")
	}
	if strings.TrimSpace(*question) == "" {
		return errors.New("--question is required")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	resolvedUserID, err := resolveTargetUserID(engine, *account, *chat, *userID, *username)
	if err != nil {
		return err
	}

	askCtx := context.Background()
	cancel := func() {}
	if *askTimeoutMS > 0 {
		askCtx, cancel = context.WithTimeout(context.Background(), time.Duration(*askTimeoutMS)*time.Millisecond)
	}
	defer cancel()

	answer, err := engine.AskChat(askCtx, *account, *chat, resolvedUserID, strings.TrimSpace(*question))
	if err != nil {
		return err
	}
	fmt.Println(answer)
	log.Info("ask output emitted",
		zap.Int64("chat_id", *chat),
		zap.Int64("user_id", resolvedUserID),
	)
	return nil
}

func resolveTargetUserID(engine *pipeline.Engine, accountID string, chatID int64, userID int64, username string) (int64, error) {
	if userID > 0 {
		return userID, nil
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return 0, nil
	}
	resolved, err := engine.ResolveUserID(accountID, chatID, username)
	if err != nil {
		return 0, err
	}
	return resolved, nil
}

type llmFlags struct {
	enabled       *bool
	baseURL       *string
	apiKey        *string
	routing       *string
	model         *string
	personaModel  *string
	topicModel    *string
	relationModel *string
	mindmapModel  *string
	digModel      *string
	digDeeper     *bool
	modelInfoURL  *string
	modelInfoFile *string
	inputTokens   *int
	outputTokens  *int
	safetyTokens  *int
	minOutput     *int
	batchMessages *int
	timeoutMS     *int
	retryMax      *int
	temperature   *float64
}

func addLLMFlags(fs *flag.FlagSet) *llmFlags {
	return &llmFlags{
		enabled:       fs.Bool("llm", envBool("LLM_ENABLE", false), "enable OpenAI-compatible LLM enrichment"),
		baseURL:       fs.String("llm-base-url", envString("LLM_BASE_URL", ""), "OpenAI-compatible base URL (without /chat/completions)"),
		apiKey:        fs.String("llm-api-key", envString("LLM_API_KEY", ""), "LLM API key"),
		routing:       fs.String("llm-routing", envString("LLM_ROUTING_MODE", "single"), "model routing: single|per-task"),
		model:         fs.String("llm-model", envString("LLM_MODEL", ""), "default LLM model"),
		personaModel:  fs.String("llm-persona-model", envString("LLM_MODEL_PERSONA", ""), "persona model for per-task routing"),
		topicModel:    fs.String("llm-topic-model", envString("LLM_MODEL_TOPICS", ""), "topic/overview model for per-task routing"),
		relationModel: fs.String("llm-relation-model", envString("LLM_MODEL_RELATIONS", ""), "relation model for per-task routing"),
		mindmapModel:  fs.String("llm-mindmap-model", envString("LLM_MODEL_MINDMAP", ""), "mindmap model for per-task routing"),
		digModel:      fs.String("llm-dig-model", envString("LLM_DIG_DEEPER_MODEL", ""), "dig deeper model for persona deep analysis"),
		digDeeper:     fs.Bool("llm-dig-deeper", envBool("LLM_DIG_DEEPER", false), "enable deep persona analysis mode"),
		modelInfoURL:  fs.String("llm-model-info-url", envString("LLM_MODEL_INFO_URL", ""), "model metadata endpoint URL (absolute or relative, e.g. /models/info)"),
		modelInfoFile: fs.String("llm-model-info-file", envString("LLM_MODEL_INFO_FILE", ""), "path to local model metadata JSON file"),
		inputTokens:   fs.Int("llm-input-tokens", envInt("LLM_INPUT_TOKENS", 32000), "default input/context token capacity when model info is unavailable"),
		outputTokens:  fs.Int("llm-output-tokens", envInt("LLM_OUTPUT_TOKENS", 2048), "default output token budget when model info is unavailable"),
		safetyTokens:  fs.Int("llm-safety-tokens", envInt("LLM_SAFETY_TOKENS", 512), "reserved safety token margin"),
		minOutput:     fs.Int("llm-min-output-tokens", envInt("LLM_MIN_OUTPUT_TOKENS", 256), "minimum output token budget"),
		batchMessages: fs.Int("llm-batch-messages", envInt("LLM_BATCH_MESSAGES", 250), "max messages per LLM batch for large chats"),
		timeoutMS:     fs.Int("llm-timeout-ms", envInt("LLM_TIMEOUT_MS", 180000), "LLM request timeout in ms"),
		retryMax:      fs.Int("llm-retry-max", envInt("LLM_RETRY_MAX", 2), "LLM max retries"),
		temperature:   fs.Float64("llm-temperature", envFloat("LLM_TEMPERATURE", 0.2), "LLM temperature"),
	}
}

func applyLLMFlags(engine *pipeline.Engine, cfg *llmFlags) error {
	if cfg == nil {
		return nil
	}
	return engine.ConfigureLLM(pipeline.LLMOptions{
		Enabled:             *cfg.enabled,
		BaseURL:             *cfg.baseURL,
		APIKey:              *cfg.apiKey,
		RoutingMode:         *cfg.routing,
		Model:               *cfg.model,
		PersonaModel:        *cfg.personaModel,
		TopicModel:          *cfg.topicModel,
		RelationModel:       *cfg.relationModel,
		MindmapModel:        *cfg.mindmapModel,
		DigDeeperModel:      *cfg.digModel,
		DigDeeper:           *cfg.digDeeper,
		ModelInfoURL:        *cfg.modelInfoURL,
		ModelInfoFile:       *cfg.modelInfoFile,
		DefaultInputTokens:  *cfg.inputTokens,
		DefaultOutputTokens: *cfg.outputTokens,
		SafetyMarginTokens:  *cfg.safetyTokens,
		MinOutputTokens:     *cfg.minOutput,
		BatchMessages:       *cfg.batchMessages,
		Timeout:             time.Duration(*cfg.timeoutMS) * time.Millisecond,
		RetryMax:            *cfg.retryMax,
		Temperature:         *cfg.temperature,
	})
}

func loadDotEnv() {
	_ = godotenv.Load(".env")
}

func startHeartbeat(log *zap.Logger, task string, interval time.Duration) func() {
	if interval <= 0 {
		return func() {}
	}

	started := time.Now()
	done := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Info("task is still running",
					zap.String("task", task),
					zap.Duration("elapsed", time.Since(started)),
				)
			case <-done:
				return
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(done)
		})
	}
}

func envString(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func envBool(k string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(k)))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envInt64(k string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func envFloat(k string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func runRange(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("range", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	chat := fs.Int64("chat", 0, "chat id")
	fromRaw := fs.String("from", "", "from RFC3339")
	toRaw := fs.String("to", "", "to RFC3339")
	limit := fs.Int("limit", 1000, "max messages")
	weekday := fs.Int("weekday", -1, "weekday filter, 0=Sunday..6=Saturday, -1=off")
	yearDay := fs.Int("yearday", -1, "day of year filter, 1..366, -1=off")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *chat == 0 {
		return errors.New("--account and --chat are required")
	}

	from, err := parseRFC3339(*fromRaw)
	if err != nil {
		return fmt.Errorf("invalid --from: %w", err)
	}
	to, err := parseRFC3339(*toRaw)
	if err != nil {
		return fmt.Errorf("invalid --to: %w", err)
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()

	messages, err := engine.MessagesFiltered(*account, *chat, pipeline.MessageFilter{
		From:    from,
		To:      to,
		Weekday: *weekday,
		YearDay: *yearDay,
		Limit:   *limit,
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(messages); err != nil {
		return err
	}
	log.Info("range output emitted", zap.Int("messages", len(messages)))
	return nil
}

type chatTarget struct {
	AccountID string
	ChatID    int64
}

type analyzeChatResult struct {
	AccountID         string `json:"account_id"`
	ChatID            int64  `json:"chat_id"`
	SnapshotPath      string `json:"snapshot_path"`
	ParticipantsDir   string `json:"participants_dir,omitempty"`
	ParticipantFiles  int    `json:"participant_files"`
	ParticipantRecent int    `json:"participant_recent_messages"`
}

type analyzeAllSummary struct {
	GeneratedAt  time.Time           `json:"generated_at"`
	StateDir     string              `json:"state_dir"`
	Profile      string              `json:"profile"`
	ChatsTotal   int                 `json:"chats_total"`
	Results      []analyzeChatResult `json:"results"`
	SchemaFiles  []string            `json:"schema_files,omitempty"`
	DurationMS   int64               `json:"duration_ms"`
	IncludeUsers bool                `json:"include_users"`
}

func runAnalyzeAll(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("analyze-all", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "optional account id filter")
	chat := fs.Int64("chat", 0, "optional chat id filter")
	profileRaw := fs.String("profile", envString("ANALYZE_PROFILE", "full"), "snapshot profile: fast|balanced|full")
	maxEdges := fs.Int("max-edges", envInt("ANALYZE_MAX_EDGES", 0), "max relation edges in exports (0 = profile default)")
	pretty := fs.Bool("pretty", envBool("ANALYZE_PRETTY", true), "pretty JSON output")
	participants := fs.Bool("participants", envBool("ANALYZE_PARTICIPANTS", true), "export per-participant files")
	recent := fs.Int("recent", envInt("ANALYZE_RECENT_MESSAGES", 300), "recent messages per participant")
	withSchema := fs.Bool("with-schema", envBool("ANALYZE_WITH_SCHEMA", true), "also generate output schema files")
	schemaOut := fs.String("schema-out", envString("ANALYZE_SCHEMA_OUT", ""), "schema output directory")
	heartbeatSec := fs.Int("heartbeat-sec", envInt("ANALYZE_HEARTBEAT_SEC", 20), "heartbeat interval in seconds while analyze-all is running (0 = disabled)")
	llmCfg := addLLMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	targets, err := discoverChatTargets(*state, strings.TrimSpace(*account), *chat)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return errors.New("no chats found for analyze-all")
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	opts := pipeline.SnapshotOptionsFromProfile(pipeline.ParseSnapshotProfile(*profileRaw))
	if *maxEdges > 0 {
		opts.MaxEdges = *maxEdges
	}
	opts.PrettyOutput = *pretty
	if llmCfg != nil && llmCfg.enabled != nil && *llmCfg.enabled {
		opts.IncludeLLM = true
	}

	started := time.Now()
	stopHeartbeat := startHeartbeat(log, "analyze-all", time.Duration(*heartbeatSec)*time.Second)
	defer stopHeartbeat()

	results := make([]analyzeChatResult, 0, len(targets))
	for _, target := range targets {
		log.Info("analyze-all processing chat",
			zap.String("account_id", target.AccountID),
			zap.Int64("chat_id", target.ChatID),
			zap.String("profile", string(opts.Profile)),
		)

		snapshotPath, err := engine.SaveChatSnapshotWithOptions(target.AccountID, target.ChatID, opts)
		if err != nil {
			return err
		}

		entry := analyzeChatResult{
			AccountID:         target.AccountID,
			ChatID:            target.ChatID,
			SnapshotPath:      snapshotPath,
			ParticipantRecent: *recent,
		}
		if *participants {
			userPaths, err := engine.SaveParticipantsSnapshotsWithOptions(target.AccountID, target.ChatID, "", *recent, opts)
			if err != nil {
				return err
			}
			entry.ParticipantFiles = len(userPaths)
			if len(userPaths) > 0 {
				entry.ParticipantsDir = filepath.Dir(userPaths[0])
			}
		}
		results = append(results, entry)
	}

	var schemaPaths []string
	if *withSchema {
		outDir := strings.TrimSpace(*schemaOut)
		if outDir == "" {
			outDir = filepath.Join(*state, "exports", "schemas")
		}
		schemaPaths, err = outputschema.WriteOutputSchemas(outDir, *pretty)
		if err != nil {
			return err
		}
	}

	summary := analyzeAllSummary{
		GeneratedAt:  time.Now().UTC(),
		StateDir:     *state,
		Profile:      string(opts.Profile),
		ChatsTotal:   len(results),
		Results:      results,
		SchemaFiles:  schemaPaths,
		DurationMS:   time.Since(started).Milliseconds(),
		IncludeUsers: *participants,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return err
	}
	log.Info("analyze-all completed",
		zap.Int("chats", len(results)),
		zap.String("profile", string(opts.Profile)),
		zap.Duration("elapsed", time.Since(started)),
	)
	return nil
}

func runSchema(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	out := fs.String("out", envString("SCHEMA_OUT", "./data/exports/schemas"), "output directory for schema files")
	pretty := fs.Bool("pretty", envBool("SCHEMA_PRETTY", true), "pretty JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	paths, err := outputschema.WriteOutputSchemas(strings.TrimSpace(*out), *pretty)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{"files": paths}); err != nil {
		return err
	}
	log.Info("schema files generated",
		zap.Int("files", len(paths)),
		zap.String("out_dir", strings.TrimSpace(*out)),
	)
	return nil
}

func discoverChatTargets(stateDir, accountFilter string, chatFilter int64) ([]chatTarget, error) {
	jsonlRoot := filepath.Join(stateDir, "jsonl")
	if strings.TrimSpace(accountFilter) != "" {
		accountPath := filepath.Join(jsonlRoot, accountFilter)
		if _, err := os.Stat(accountPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("account %q not found in state directory", accountFilter)
			}
			return nil, err
		}
	}

	accounts := make([]string, 0)
	if strings.TrimSpace(accountFilter) != "" {
		accounts = append(accounts, accountFilter)
	} else {
		entries, err := os.ReadDir(jsonlRoot)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				accounts = append(accounts, entry.Name())
			}
		}
	}

	targets := make([]chatTarget, 0, 16)
	for _, accountID := range accounts {
		chatEntries, err := os.ReadDir(filepath.Join(jsonlRoot, accountID))
		if err != nil {
			continue
		}
		for _, entry := range chatEntries {
			if !entry.IsDir() {
				continue
			}
			chatID, err := strconv.ParseInt(strings.TrimSpace(entry.Name()), 10, 64)
			if err != nil || chatID == 0 {
				continue
			}
			if chatFilter != 0 && chatID != chatFilter {
				continue
			}
			if !chatDirHasMessages(filepath.Join(jsonlRoot, accountID, entry.Name())) {
				continue
			}
			targets = append(targets, chatTarget{AccountID: accountID, ChatID: chatID})
		}
	}

	sort.Slice(targets, func(i, j int) bool {
		if targets[i].AccountID == targets[j].AccountID {
			return targets[i].ChatID < targets[j].ChatID
		}
		return targets[i].AccountID < targets[j].AccountID
	})
	return targets, nil
}

func chatDirHasMessages(chatDir string) bool {
	legacy := filepath.Join(chatDir, "messages.jsonl")
	if st, err := os.Stat(legacy); err == nil && !st.IsDir() && st.Size() > 0 {
		return true
	}
	segments, err := filepath.Glob(filepath.Join(chatDir, "messages_*.jsonl"))
	if err != nil || len(segments) == 0 {
		return false
	}
	for _, path := range segments {
		if st, err := os.Stat(path); err == nil && !st.IsDir() && st.Size() > 0 {
			return true
		}
	}
	return false
}

func runDashboard(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("dashboard", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	host := fs.String("host", envString("DASHBOARD_HOST", "127.0.0.1"), "listen host")
	port := fs.Int("port", envInt("DASHBOARD_PORT", 8787), "listen port")
	readHeaderTimeoutMS := fs.Int("read-header-timeout-ms", envInt("DASHBOARD_READ_HEADER_TIMEOUT_MS", 10000), "read header timeout in ms")
	if err := fs.Parse(args); err != nil {
		return err
	}

	listenHost := strings.TrimSpace(*host)
	if listenHost == "" {
		listenHost = "127.0.0.1"
	}
	if *port <= 0 || *port > 65535 {
		return errors.New("--port must be between 1 and 65535")
	}

	handler, err := dashboard.NewHandler(*state, log)
	if err != nil {
		return err
	}

	addr := net.JoinHostPort(listenHost, strconv.Itoa(*port))
	publicHost := dashboardURLHost(listenHost)
	publicURL := fmt.Sprintf("http://%s", net.JoinHostPort(publicHost, strconv.Itoa(*port)))

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: time.Duration(*readHeaderTimeoutMS) * time.Millisecond,
	}

	log.Info("dashboard server started",
		zap.String("state_dir", *state),
		zap.String("listen_addr", addr),
		zap.String("url", publicURL),
	)
	fmt.Println(publicURL)

	err = srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func dashboardURLHost(host string) string {
	host = strings.TrimSpace(host)
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return "127.0.0.1"
	default:
		return host
	}
}

func runTGFetch(log *zap.Logger, args []string) error {
	fs := flag.NewFlagSet("tg-fetch", flag.ContinueOnError)
	state := fs.String("state", "./data", "state directory")
	account := fs.String("account", "", "account id")
	apiID := fs.Int("api-id", 0, "telegram API ID")
	apiHash := fs.String("api-hash", "", "telegram API hash")
	sessionPath := fs.String("session", "", "session file path")
	peer := fs.String("peer", "", "peer ref (@channel, t.me/.., phone, self)")
	chatID := fs.Int64("chat-id", 0, "forced chat id")
	startID := fs.Int("start-id", 0, "start offset message id")
	limit := fs.Int("limit", 0, "max messages to fetch (0 = all)")
	batch := fs.Int("batch", 100, "history page size (<=100)")
	rateMS := fs.Int("rate-ms", 100, "request rate interval in ms")
	burst := fs.Int("burst", 3, "rate limiter burst")
	requestTimeoutMS := fs.Int("request-timeout-ms", 0, "per-request timeout in ms (0=off)")
	retryMax := fs.Int("retry-max", 8, "max retries for transient 420/429/5xx")
	withRaw := fs.Bool("with-raw", false, "store raw message JSON payload")
	fetchBio := fs.Bool("fetch-bio", false, "fetch user bio via users.getFullUser")
	fetchAvatars := fs.Bool("fetch-avatars", false, "capture avatar refs for users")
	maxInfo := fs.Bool("max-info", envBool("TG_MAX_INFO", false), "enable max telegram metadata capture (bio, avatars, raw)")
	fileMaxMessages := fs.Int("file-max-messages", envInt("JSONL_FILE_MAX_MESSAGES", 5000), "max messages per JSONL file segment (0 = single file)")
	poll := fs.Bool("poll", envBool("TG_POLL", false), "enable persistent polling mode")
	pollWithBackfill := fs.Bool("poll-with-backfill", envBool("TG_POLL_WITH_BACKFILL", true), "when --poll is enabled, backfill history first, then switch to polling")
	pollIntervalMS := fs.Int("poll-interval-ms", envInt("TG_POLL_INTERVAL_MS", 5000), "polling interval in ms")
	cmdPolicy := fs.String("cmd-policy", envString("TG_CMD_POLICY", "owner"), "command policy: owner|admins|users|ids")
	cmdIDs := fs.String("cmd-ids", envString("TG_CMD_IDS", ""), "comma-separated allowed user IDs for cmd-policy=ids")
	cmdPrefix := fs.String("cmd-prefix", envString("TG_CMD_PREFIX", "/"), "command prefix in polling mode")
	searchLimit := fs.Int("search-limit", envInt("TG_SEARCH_LIMIT", 7), "result limit for /search command")
	ownerID := fs.Int64("owner-id", envInt64("TG_OWNER_ID", 0), "owner user id for final parser summary report")
	ownerUsername := fs.String("owner-username", envString("TG_OWNER_USERNAME", ""), "owner username for final parser summary report")
	avatarDir := fs.String("avatar-dir", "", "avatar file directory metadata")
	avatarCache := fs.String("avatar-cache", "", "avatar cache json path (default <avatar-dir>/cache.json)")
	avatarBig := fs.Bool("avatar-big", true, "download high quality avatars when --fetch-avatars is set")
	fetchMedia := fs.Bool("fetch-media", envBool("TG_FETCH_MEDIA", false), "download message media files (photo/document/video/audio/voice/sticker/gif)")
	mediaDir := fs.String("media-dir", envString("TG_MEDIA_DIR", ""), "downloaded media directory (default <state>/media when --fetch-media)")
	mediaCache := fs.String("media-cache", envString("TG_MEDIA_CACHE", ""), "media cache json path (default <media-dir>/cache.json)")
	bootstrapJSONL := fs.String("bootstrap-jsonl", envString("TG_BOOTSTRAP_JSONL", ""), "optional old JSONL file to import before Telegram fetch")
	bootstrapAccount := fs.String("bootstrap-account", envString("TG_BOOTSTRAP_ACCOUNT", ""), "rewrite account_id for bootstrap import (default --account)")
	bootstrapChatID := fs.Int64("bootstrap-chat-id", envInt64("TG_BOOTSTRAP_CHAT_ID", 0), "rewrite chat_id for bootstrap import (default --chat-id when set)")
	bootstrapBatch := fs.Int("bootstrap-batch", envInt("TG_BOOTSTRAP_BATCH", 1000), "batch size for bootstrap import")
	bootstrapOnly := fs.Bool("bootstrap-only", envBool("TG_BOOTSTRAP_ONLY", false), "import bootstrap JSONL and exit")
	resumeFromLastID := fs.Bool("resume-from-last-id", envBool("TG_RESUME_FROM_LAST_ID", true), "if --start-id is not set, auto-detect max stored message_id and continue from it")
	topicMode := fs.String("topic-mode", "heuristic", "topic mode: heuristic|embedding|llm-fallback")
	embeddingModel := fs.String("embedding-model", "", "embedding model name")
	llmFallbackModel := fs.String("llm-fallback-model", "", "llm fallback model name")
	llmCfg := addLLMFlags(fs)
	phone := fs.String("phone", os.Getenv("TG_PHONE"), "phone for auth")
	password := fs.String("password", os.Getenv("TG_PASSWORD"), "2FA password")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *account == "" || *apiID <= 0 || *apiHash == "" || *peer == "" {
		return errors.New("--account, --api-id, --api-hash and --peer are required")
	}
	if *sessionPath == "" {
		*sessionPath = filepath.Join(*state, "sessions", *account+".json")
	}
	if *phone == "" {
		return errors.New("--phone (or TG_PHONE env) is required")
	}

	if *maxInfo {
		*fetchBio = true
		*fetchAvatars = true
		*fetchMedia = true
		*withRaw = true
	}
	if *fetchAvatars && strings.TrimSpace(*avatarDir) == "" {
		*avatarDir = filepath.Join(*state, "avatars")
	}
	if strings.TrimSpace(*avatarCache) == "" && strings.TrimSpace(*avatarDir) != "" {
		*avatarCache = filepath.Join(*avatarDir, "cache.json")
	}
	if *fetchMedia && strings.TrimSpace(*mediaDir) == "" {
		*mediaDir = filepath.Join(*state, "media")
	}
	if strings.TrimSpace(*mediaCache) == "" && strings.TrimSpace(*mediaDir) != "" {
		*mediaCache = filepath.Join(*mediaDir, "cache.json")
	}

	allowedIDs, err := parseInt64CSV(*cmdIDs)
	if err != nil {
		return fmt.Errorf("invalid --cmd-ids: %w", err)
	}

	engine, err := pipeline.NewEngine(*state, log)
	if err != nil {
		return err
	}
	defer engine.Close()
	engine.ConfigureStorage(*fileMaxMessages)
	engine.ConfigureTopicDetection(parseTopicMode(*topicMode), *embeddingModel, *llmFallbackModel)
	if err := applyLLMFlags(engine, llmCfg); err != nil {
		return err
	}

	effectiveBootstrapAccount := strings.TrimSpace(*bootstrapAccount)
	if effectiveBootstrapAccount == "" {
		effectiveBootstrapAccount = *account
	}
	effectiveBootstrapChatID := *bootstrapChatID
	if effectiveBootstrapChatID == 0 && *chatID != 0 {
		effectiveBootstrapChatID = *chatID
	}
	if strings.TrimSpace(*bootstrapJSONL) != "" {
		imported, maxID, err := bootstrapImportJSONL(
			context.Background(),
			engine,
			strings.TrimSpace(*bootstrapJSONL),
			*bootstrapBatch,
			effectiveBootstrapAccount,
			effectiveBootstrapChatID,
		)
		if err != nil {
			return fmt.Errorf("bootstrap import failed: %w", err)
		}
		log.Info("bootstrap import completed",
			zap.String("path", strings.TrimSpace(*bootstrapJSONL)),
			zap.Int("messages", imported),
			zap.Int("max_message_id", maxID),
			zap.String("account_id", effectiveBootstrapAccount),
			zap.Int64("chat_id", effectiveBootstrapChatID),
		)
		if *bootstrapOnly {
			return nil
		}
	}

	if *startID <= 0 && *resumeFromLastID {
		resumeChatID := *chatID
		if resumeChatID == 0 {
			resumeChatID = effectiveBootstrapChatID
		}
		if resumeChatID > 0 {
			lastID, err := engine.LastMessageID(*account, resumeChatID)
			if err != nil {
				return fmt.Errorf("detect last message id: %w", err)
			}
			if lastID > 0 {
				*startID = lastID
				log.Info("auto resume enabled",
					zap.Int("start_id", *startID),
					zap.String("account_id", *account),
					zap.Int64("chat_id", resumeChatID),
				)
			}
		}
	}

	reader := bufio.NewReader(os.Stdin)
	codeFn := func(ctx context.Context, hint string) (string, error) {
		label := "Enter Telegram code"
		if hint != "" {
			label = hint
		}
		return promptLine(reader, label+": ")
	}

	searchHandler := func(ctx context.Context, accountID string, chatID int64, query string, limit int) (string, error) {
		items, err := engine.SearchChat(ctx, accountID, chatID, query, limit)
		if err != nil {
			return "", err
		}
		return formatTelegramSearch(items), nil
	}
	askHandler := func(ctx context.Context, accountID string, chatID int64, userID int64, question string) (string, error) {
		return engine.AskChat(ctx, accountID, chatID, userID, question)
	}
	statusHandler := func(ctx context.Context, accountID string, chatID int64, runtime telegramingest.PollRuntime) (string, error) {
		_ = ctx
		messages, users, latest, err := engine.ChatStoredStatus(accountID, chatID)
		if err != nil {
			return "", err
		}
		latestStr := "n/a"
		if !latest.IsZero() {
			latestStr = latest.UTC().Format(time.RFC3339)
		}
		return fmt.Sprintf("stored_messages=%d\nstored_users=%d\nlatest_message_at=%s", messages, users, latestStr), nil
	}
	summaryHandler := func(ctx context.Context, accountID string, chatID int64, runtime telegramingest.PollRuntime) (string, error) {
		_ = ctx
		messages, users, latest, err := engine.ChatStoredStatus(accountID, chatID)
		if err != nil {
			return "", err
		}
		latestStr := "n/a"
		if !latest.IsZero() {
			latestStr = latest.UTC().Format(time.RFC3339)
		}
		return fmt.Sprintf(
			"owner report\naccount=%s\nchat_id=%d\ningested=%d\ncommands=%d\nlast_seen_id=%d\nstored_messages=%d\nstored_users=%d\nlatest=%s",
			accountID,
			chatID,
			runtime.IngestedMessages,
			runtime.CommandsHandled,
			runtime.LastSeenID,
			messages,
			users,
			latestStr,
		), nil
	}

	start := time.Now()
	total, err := telegramingest.Run(context.Background(), telegramingest.Options{
		AccountID:        *account,
		APIID:            *apiID,
		APIHash:          *apiHash,
		SessionPath:      *sessionPath,
		Peer:             *peer,
		OwnerID:          *ownerID,
		OwnerUsername:    *ownerUsername,
		ForcedChatID:     *chatID,
		StartID:          *startID,
		Limit:            *limit,
		BatchSize:        *batch,
		FetchBio:         *fetchBio,
		FetchAvatars:     *fetchAvatars,
		PollCommands:     *poll,
		PollWithBackfill: *pollWithBackfill,
		PollInterval:     time.Duration(*pollIntervalMS) * time.Millisecond,
		CommandPolicy:    *cmdPolicy,
		CommandUserIDs:   allowedIDs,
		CommandPrefix:    *cmdPrefix,
		SearchLimit:      *searchLimit,
		AvatarDir:        *avatarDir,
		AvatarCachePath:  *avatarCache,
		AvatarBig:        *avatarBig,
		FetchMedia:       *fetchMedia,
		MediaDir:         *mediaDir,
		MediaCachePath:   *mediaCache,
		WithRaw:          *withRaw,
		RateInterval:     time.Duration(*rateMS) * time.Millisecond,
		RateBurst:        *burst,
		RequestTimeout:   time.Duration(*requestTimeoutMS) * time.Millisecond,
		RetryMax:         *retryMax,
		TopicMode:        *topicMode,
		EmbeddingModel:   *embeddingModel,
		LLMFallbackModel: *llmFallbackModel,
		SearchHandler:    searchHandler,
		AskHandler:       askHandler,
		StatusHandler:    statusHandler,
		SummaryHandler:   summaryHandler,
		Log:              log,
	}, telegramingest.Credentials{
		Phone:    *phone,
		Password: *password,
		CodeFunc: codeFn,
	}, engine.ProcessBatch)
	if err != nil {
		return err
	}

	log.Info("tg-fetch ingested",
		zap.Int("messages", total),
		zap.Duration("elapsed", time.Since(start)),
	)
	return nil
}

func parseRFC3339(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, v)
}

func promptLine(reader *bufio.Reader, label string) (string, error) {
	fmt.Fprint(os.Stderr, label)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func parseInt64CSV(raw string) ([]int64, error) {
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

func bootstrapImportJSONL(
	ctx context.Context,
	engine *pipeline.Engine,
	inputPath string,
	batchSize int,
	accountOverride string,
	chatOverride int64,
) (imported int, maxMessageID int, err error) {
	if strings.TrimSpace(inputPath) == "" {
		return 0, 0, errors.New("bootstrap input path is empty")
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	f, err := os.Open(strings.TrimSpace(inputPath))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	batch := make([]domain.MessageRecord, 0, batchSize)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		var msg domain.MessageRecord
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			return imported, maxMessageID, fmt.Errorf("bootstrap unmarshal line %d: %w", lineNo, err)
		}
		if strings.TrimSpace(accountOverride) != "" {
			msg.AccountID = strings.TrimSpace(accountOverride)
		}
		if chatOverride != 0 {
			msg.ChatID = chatOverride
		}
		if strings.TrimSpace(msg.AccountID) == "" {
			return imported, maxMessageID, fmt.Errorf("bootstrap line %d has empty account_id", lineNo)
		}
		if msg.ChatID == 0 {
			return imported, maxMessageID, fmt.Errorf("bootstrap line %d has empty chat_id", lineNo)
		}
		if msg.Date.IsZero() {
			msg.Date = time.Now().UTC()
		}
		if msg.MessageID > maxMessageID {
			maxMessageID = msg.MessageID
		}
		batch = append(batch, msg)
		if len(batch) >= batchSize {
			if err := engine.ProcessBatch(ctx, batch); err != nil {
				return imported, maxMessageID, err
			}
			imported += len(batch)
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return imported, maxMessageID, err
	}
	if len(batch) > 0 {
		if err := engine.ProcessBatch(ctx, batch); err != nil {
			return imported, maxMessageID, err
		}
		imported += len(batch)
	}

	return imported, maxMessageID, nil
}

func formatTelegramSearch(items []domain.SearchResult) string {
	if len(items) == 0 {
		return "nothing found"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("found %d message(s):", len(items)))
	for i, it := range items {
		line := strings.TrimSpace(it.Message.Text)
		if line == "" {
			line = "(empty text)"
		}
		if len([]rune(line)) > 180 {
			line = string([]rune(line)[:177]) + "..."
		}
		b.WriteString("\n")
		ts := "n/a"
		if !it.Message.Date.IsZero() {
			ts = it.Message.Date.UTC().Format(time.RFC3339)
		}
		b.WriteString(fmt.Sprintf("%d) #%d @ %s\n%s", i+1, it.Message.MessageID, ts, line))
	}
	return b.String()
}

func parseTopicMode(v string) analysis.TopicMode {
	switch strings.TrimSpace(strings.ToLower(v)) {
	case string(analysis.TopicModeEmbedding):
		return analysis.TopicModeEmbedding
	case string(analysis.TopicModeLLMFallback):
		return analysis.TopicModeLLMFallback
	default:
		return analysis.TopicModeHeuristic
	}
}
