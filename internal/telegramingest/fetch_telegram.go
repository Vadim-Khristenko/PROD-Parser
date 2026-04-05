//go:build telegram

package telegramingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/deeplink"
	msgpeer "github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/analysis"
	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type pollRuntimeState struct {
	StartedAt        time.Time
	IngestedMessages int
	LastSeenID       int
	CommandsHandled  int
	LastCommandAt    time.Time
}

func (s *pollRuntimeState) Snapshot(paused bool) PollRuntime {
	if s == nil {
		return PollRuntime{Paused: paused}
	}
	return PollRuntime{
		StartedAt:        s.StartedAt,
		IngestedMessages: s.IngestedMessages,
		LastSeenID:       s.LastSeenID,
		Paused:           paused,
		CommandsHandled:  s.CommandsHandled,
		LastCommandAt:    s.LastCommandAt,
	}
}

type mediaMeta struct {
	MediaType string
	HasVoice  bool
	MediaID   string
	FileHash  string
	FileName  string
	FileSize  int64
}

func Run(ctx context.Context, opts Options, creds Credentials, onBatch BatchHandler) (int, error) {
	if opts.AccountID == "" {
		return 0, errors.New("account id is empty")
	}
	if opts.APIID <= 0 || opts.APIHash == "" {
		return 0, errors.New("api credentials are required")
	}
	if opts.SessionPath == "" {
		return 0, errors.New("session path is empty")
	}
	if opts.Peer == "" {
		return 0, errors.New("peer is empty")
	}
	if creds.Phone == "" {
		return 0, errors.New("phone is required")
	}
	if creds.CodeFunc == nil {
		return 0, errors.New("code callback is required")
	}
	if onBatch == nil {
		return 0, errors.New("batch handler is nil")
	}

	log := opts.Log
	if log == nil {
		log = zap.NewNop()
	}
	log = log.With(
		zap.String("component", "telegram_ingest"),
		zap.String("account_id", opts.AccountID),
		zap.String("topic_mode", opts.TopicMode),
		zap.String("embedding_model", opts.EmbeddingModel),
		zap.String("llm_fallback_model", opts.LLMFallbackModel),
	)

	if opts.BatchSize <= 0 || opts.BatchSize > 100 {
		opts.BatchSize = 100
	}
	if opts.RateInterval <= 0 {
		opts.RateInterval = 100 * time.Millisecond
	}
	if opts.RateBurst <= 0 {
		opts.RateBurst = 3
	}
	if opts.RetryMax <= 0 {
		opts.RetryMax = 8
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Second
	}
	if strings.TrimSpace(opts.CommandPrefix) == "" {
		opts.CommandPrefix = "/"
	}
	if err := os.MkdirAll(filepath.Dir(opts.SessionPath), 0o700); err != nil {
		return 0, fmt.Errorf("create session dir: %w", err)
	}

	avatarDL, err := newAvatarDownloader(opts, log)
	if err != nil {
		return 0, fmt.Errorf("init avatar downloader: %w", err)
	}
	mediaDL, err := newMediaDownloader(opts, log)
	if err != nil {
		return 0, fmt.Errorf("init media downloader: %w", err)
	}
	defer func() {
		if avatarDL == nil {
		} else if err := avatarDL.Close(); err != nil {
			log.Warn("save avatar cache failed", zap.Error(err))
		}
		if mediaDL != nil {
			if err := mediaDL.Close(); err != nil {
				log.Warn("save media cache failed", zap.Error(err))
			}
		}
	}()

	waiter := floodwait.NewSimpleWaiter().WithMaxRetries(0)
	client := telegram.NewClient(opts.APIID, opts.APIHash, telegram.Options{
		Logger:         log,
		SessionStorage: &session.FileStorage{Path: opts.SessionPath},
		Middlewares: []telegram.Middleware{
			waiter,
			ratelimit.New(rate.Every(opts.RateInterval), opts.RateBurst),
		},
	})

	fetched := 0
	err = client.Run(ctx, func(ctx context.Context) error {
		flow := auth.NewFlow(promptAuth{creds: creds}, auth.SendCodeOptions{})
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		api := client.API()
		peer, err := resolvePeer(ctx, api, opts.Peer)
		if err != nil {
			return fmt.Errorf("resolve peer: %w", err)
		}

		chatID := opts.ForcedChatID
		if chatID == 0 {
			chatID = chatIDFromInputPeer(peer)
		}

		if opts.PollCommands {
			if opts.PollWithBackfill {
				backfilled, lastSeen, err := fetchHistoryBackfill(ctx, api, opts, peer, chatID, onBatch, avatarDL, mediaDL, log)
				fetched += backfilled
				if err != nil {
					return err
				}
				if lastSeen > opts.StartID {
					opts.StartID = lastSeen
				}
			}

			ownerID, err := resolveSelfUserID(ctx, api)
			if err != nil {
				return fmt.Errorf("resolve owner user id: %w", err)
			}
			mode, err := normalizeCommandPolicy(opts.CommandPolicy)
			if err != nil {
				return fmt.Errorf("invalid command policy: %w", err)
			}
			var adminIDs []int64
			if mode == commandAccessAdmins {
				adminIDs, err = resolveAdminIDsForPeer(ctx, api, peer)
				if err != nil {
					return fmt.Errorf("resolve admin user ids: %w", err)
				}
			}
			authorizer, err := newCommandAuthorizer(opts.CommandPolicy, ownerID, adminIDs, opts.CommandUserIDs)
			if err != nil {
				return fmt.Errorf("build command authorizer: %w", err)
			}
			polled, err := runPollingMode(ctx, api, opts, peer, chatID, onBatch, avatarDL, mediaDL, log, authorizer)
			fetched += polled
			return err
		}

		backfilled, _, err := fetchHistoryBackfill(ctx, api, opts, peer, chatID, onBatch, avatarDL, mediaDL, log)
		fetched += backfilled
		return err
	})
	if err != nil {
		return fetched, err
	}
	log.Info("telegram fetch done", zap.Int("messages", fetched))
	return fetched, nil
}

type promptAuth struct {
	creds Credentials
}

func (p promptAuth) Phone(ctx context.Context) (string, error) {
	return p.creds.Phone, nil
}

func (p promptAuth) Password(ctx context.Context) (string, error) {
	if p.creds.Password == "" {
		return "", auth.ErrPasswordNotProvided
	}
	return p.creds.Password, nil
}

func (p promptAuth) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	return nil
}

func (p promptAuth) SignUp(ctx context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign-up is not supported in parser mode")
}

func (p promptAuth) Code(ctx context.Context, sentCode *tg.AuthSentCode) (string, error) {
	return p.creds.CodeFunc(ctx, codeHint(sentCode))
}

func codeHint(sentCode *tg.AuthSentCode) string {
	if sentCode == nil {
		return "telegram code"
	}
	return "telegram code"
}

func resolvePeer(ctx context.Context, api *tg.Client, value string) (tg.InputPeerClass, error) {
	v := strings.TrimSpace(value)
	if strings.EqualFold(v, "self") || strings.EqualFold(v, "me") {
		return &tg.InputPeerSelf{}, nil
	}
	if deeplink.IsDeeplinkLike(v) {
		if join, err := deeplink.Expect(v, deeplink.Join); err == nil {
			inviteHash := strings.TrimSpace(join.Args.Get("invite"))
			if inviteHash == "" {
				return nil, errors.New("invite hash is empty")
			}
			return resolveInvitePeer(ctx, api, inviteHash)
		}
	}
	resolver := msgpeer.DefaultResolver(api)
	return msgpeer.Resolve(resolver, v)(ctx)
}

func resolveInvitePeer(ctx context.Context, api *tg.Client, inviteHash string) (tg.InputPeerClass, error) {
	inviteHash = strings.TrimSpace(inviteHash)
	if inviteHash == "" {
		return nil, errors.New("invite hash is empty")
	}

	if info, err := api.MessagesCheckChatInvite(ctx, inviteHash); err == nil {
		if already, ok := info.(*tg.ChatInviteAlready); ok && already.Chat != nil {
			if peer, ok := inputPeerFromChat(already.Chat); ok {
				return peer, nil
			}
		}
	}

	updates, err := api.MessagesImportChatInvite(ctx, inviteHash)
	if err != nil {
		if rpcErr, ok := tgerr.As(err); ok && rpcErr.IsType("USER_ALREADY_PARTICIPANT") {
			info, checkErr := api.MessagesCheckChatInvite(ctx, inviteHash)
			if checkErr == nil {
				if already, ok := info.(*tg.ChatInviteAlready); ok && already.Chat != nil {
					if peer, ok := inputPeerFromChat(already.Chat); ok {
						return peer, nil
					}
				}
			}
		}
		return nil, err
	}

	if peer, ok := inputPeerFromUpdates(updates); ok {
		return peer, nil
	}

	if info, err := api.MessagesCheckChatInvite(ctx, inviteHash); err == nil {
		if already, ok := info.(*tg.ChatInviteAlready); ok && already.Chat != nil {
			if peer, ok := inputPeerFromChat(already.Chat); ok {
				return peer, nil
			}
		}
	}

	return nil, errors.New("unable to resolve invite peer")
}

func inputPeerFromUpdates(updates tg.UpdatesClass) (tg.InputPeerClass, bool) {
	switch v := updates.(type) {
	case *tg.Updates:
		return inputPeerFromChats(v.Chats)
	case *tg.UpdatesCombined:
		return inputPeerFromChats(v.Chats)
	default:
		return nil, false
	}
}

func inputPeerFromChats(chats []tg.ChatClass) (tg.InputPeerClass, bool) {
	for _, chat := range chats {
		if peer, ok := inputPeerFromChat(chat); ok {
			return peer, true
		}
	}
	return nil, false
}

func inputPeerFromChat(chat tg.ChatClass) (tg.InputPeerClass, bool) {
	switch c := chat.(type) {
	case *tg.Channel:
		return &tg.InputPeerChannel{ChannelID: c.ID, AccessHash: c.AccessHash}, true
	case *tg.ChannelForbidden:
		return &tg.InputPeerChannel{ChannelID: c.ID, AccessHash: c.AccessHash}, true
	case *tg.Chat:
		return &tg.InputPeerChat{ChatID: c.ID}, true
	case *tg.ChatForbidden:
		return &tg.InputPeerChat{ChatID: c.ID}, true
	default:
		return nil, false
	}
}

func historyWithRetry(ctx context.Context, api *tg.Client, req *tg.MessagesGetHistoryRequest, maxRetries int) (tg.MessagesMessagesClass, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := api.MessagesGetHistory(ctx, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		rpcErr, ok := tgerr.As(err)
		if !ok || !(rpcErr.Code == 420 || rpcErr.Code == 429 || rpcErr.Code >= 500) {
			return nil, err
		}
		backoff := time.Second * time.Duration(1<<minInt(attempt-1, 6))
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		jitter := time.Duration(rand.Int63n(int64(backoff/2 + 1)))
		wait := backoff + jitter
		if !sleepContext(ctx, wait) {
			return nil, ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = errors.New("history retry limit exceeded")
	}
	return nil, lastErr
}

func fetchHistoryBackfill(
	ctx context.Context,
	api *tg.Client,
	opts Options,
	peer tg.InputPeerClass,
	chatID int64,
	onBatch BatchHandler,
	avatarDL *avatarDownloader,
	mediaDL *mediaDownloader,
	log *zap.Logger,
) (ingested int, lastSeenID int, err error) {
	bioCache := map[int64]string{}
	offsetID := opts.StartID
	remaining := opts.Limit
	lastSeenID = opts.StartID

	for {
		pageSize := opts.BatchSize
		if remaining > 0 && remaining < pageSize {
			pageSize = remaining
		}

		reqCtx := ctx
		cancelReq := func() {}
		if opts.RequestTimeout > 0 {
			reqCtx, cancelReq = context.WithTimeout(ctx, opts.RequestTimeout)
		}
		resp, reqErr := historyWithRetry(reqCtx, api, &tg.MessagesGetHistoryRequest{
			Peer:      peer,
			OffsetID:  offsetID,
			AddOffset: 0,
			Limit:     pageSize,
		}, opts.RetryMax)
		cancelReq()
		if reqErr != nil {
			if auth.IsUnauthorized(reqErr) {
				return ingested, lastSeenID, fmt.Errorf("session expired: %w", reqErr)
			}
			return ingested, lastSeenID, reqErr
		}

		batch, entities, mediaSource, last := normalizeBatch(opts, resp, chatID)
		if len(batch) == 0 {
			return ingested, lastSeenID, nil
		}
		if opts.FetchBio {
			enrichBatchBios(ctx, api, batch, entities, bioCache, log)
		}
		if avatarDL != nil {
			enrichBatchAvatars(ctx, api, batch, entities, avatarDL, log)
		}
		if mediaDL != nil {
			enrichBatchMedia(ctx, api, batch, mediaSource, mediaDL, log)
		}
		if batchMaxID := maxMessageID(batch); batchMaxID > lastSeenID {
			lastSeenID = batchMaxID
		}
		if err := onBatch(ctx, batch); err != nil {
			return ingested, lastSeenID, err
		}
		ingested += len(batch)

		if remaining > 0 {
			remaining -= len(batch)
			if remaining <= 0 {
				return ingested, lastSeenID, nil
			}
		}
		if last || len(batch) < pageSize {
			return ingested, lastSeenID, nil
		}

		minID := batch[0].MessageID
		for _, msg := range batch {
			if msg.MessageID < minID {
				minID = msg.MessageID
			}
		}
		offsetID = minID
	}
}

func runPollingMode(
	ctx context.Context,
	api *tg.Client,
	opts Options,
	peer tg.InputPeerClass,
	chatID int64,
	onBatch BatchHandler,
	avatarDL *avatarDownloader,
	mediaDL *mediaDownloader,
	log *zap.Logger,
	authorizer commandAuthorizer,
) (int, error) {
	lastSeenID := opts.StartID
	bioCache := map[int64]string{}
	state := &pollControlState{}
	runtime := &pollRuntimeState{StartedAt: time.Now().UTC()}
	knownUsers := make(map[int64]*tg.User, 256)
	ingested := 0

	if lastSeenID <= 0 {
		seedCtx := ctx
		cancelSeed := func() {}
		if opts.RequestTimeout > 0 {
			seedCtx, cancelSeed = context.WithTimeout(ctx, opts.RequestTimeout)
		}
		resp, err := historyWithRetry(seedCtx, api, &tg.MessagesGetHistoryRequest{
			Peer:  peer,
			Limit: 1,
		}, opts.RetryMax)
		cancelSeed()
		if err != nil {
			return ingested, err
		}
		seed, _, _, _ := normalizeBatch(opts, resp, chatID)
		if len(seed) > 0 {
			lastSeenID = maxMessageID(seed)
		}
	}
	runtime.LastSeenID = lastSeenID

	defer func() {
		if opts.OwnerID <= 0 && strings.TrimSpace(opts.OwnerUsername) == "" {
			return
		}
		reportCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		report := defaultSummaryReport(chatID, runtime.Snapshot(state.Paused))
		if opts.SummaryHandler != nil {
			if custom, err := opts.SummaryHandler(reportCtx, opts.AccountID, chatID, runtime.Snapshot(state.Paused)); err == nil && strings.TrimSpace(custom) != "" {
				report = strings.TrimSpace(custom)
			}
		}
		if err := sendOwnerReport(reportCtx, api, opts, report, knownUsers); err != nil {
			log.Warn("send owner report failed", zap.Error(err))
		}
	}()

	log.Info("polling mode enabled",
		zap.Int("start_id", lastSeenID),
		zap.String("command_policy", strings.TrimSpace(opts.CommandPolicy)),
		zap.String("command_prefix", opts.CommandPrefix),
		zap.Duration("poll_interval", opts.PollInterval),
	)

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	for {
		batchIngested, stopNow, err := pollOnce(ctx, api, opts, peer, chatID, onBatch, avatarDL, mediaDL, log, authorizer, bioCache, state, runtime, &lastSeenID, knownUsers)
		ingested += batchIngested
		runtime.IngestedMessages = ingested
		runtime.LastSeenID = lastSeenID
		if err != nil {
			return ingested, err
		}
		if stopNow || state.Stop {
			return ingested, nil
		}

		select {
		case <-ctx.Done():
			return ingested, ctx.Err()
		case <-ticker.C:
		}
	}
}

func pollOnce(
	ctx context.Context,
	api *tg.Client,
	opts Options,
	peer tg.InputPeerClass,
	chatID int64,
	onBatch BatchHandler,
	avatarDL *avatarDownloader,
	mediaDL *mediaDownloader,
	log *zap.Logger,
	authorizer commandAuthorizer,
	bioCache map[int64]string,
	state *pollControlState,
	runtime *pollRuntimeState,
	lastSeenID *int,
	knownUsers map[int64]*tg.User,
) (int, bool, error) {
	ingested := 0
	for {
		reqCtx := ctx
		cancelReq := func() {}
		if opts.RequestTimeout > 0 {
			reqCtx, cancelReq = context.WithTimeout(ctx, opts.RequestTimeout)
		}
		resp, err := historyWithRetry(reqCtx, api, &tg.MessagesGetHistoryRequest{
			Peer:  peer,
			Limit: opts.BatchSize,
			MinID: *lastSeenID,
		}, opts.RetryMax)
		cancelReq()
		if err != nil {
			if auth.IsUnauthorized(err) {
				return ingested, false, fmt.Errorf("session expired: %w", err)
			}
			return ingested, false, err
		}

		batch, entities, mediaSource, _ := normalizeBatch(opts, resp, chatID)
		for uid, u := range entities.Users() {
			knownUsers[uid] = u
		}
		fresh := filterFreshMessages(batch, *lastSeenID)
		if len(fresh) == 0 {
			return ingested, state.Stop, nil
		}
		sort.SliceStable(fresh, func(i, j int) bool { return fresh[i].MessageID < fresh[j].MessageID })

		if opts.FetchBio {
			enrichBatchBios(ctx, api, fresh, entities, bioCache, log)
		}
		if avatarDL != nil {
			enrichBatchAvatars(ctx, api, fresh, entities, avatarDL, log)
		}
		if mediaDL != nil {
			enrichBatchMedia(ctx, api, fresh, mediaSource, mediaDL, log)
		}

		toIngest := make([]domain.MessageRecord, 0, len(fresh))
		for _, msg := range fresh {
			cmd, isCmd := parsePollCommand(msg.Text, opts.CommandPrefix)
			if isCmd && authorizer.IsAllowed(msg.FromUserID) {
				runtime.CommandsHandled++
				runtime.LastCommandAt = time.Now().UTC()
				reply, handled, stopNow, err := executePollCommand(ctx, api, opts, peer, chatID, msg, cmd, state, runtime)
				if err != nil {
					log.Warn("command execution failed", zap.String("command", cmd.Name), zap.Error(err))
					reply = "command failed: " + err.Error()
					handled = true
				}
				if handled {
					if strings.TrimSpace(reply) != "" {
						if err := sendCommandReply(ctx, api, peer, msg.MessageID, reply); err != nil {
							log.Warn("send command reply failed", zap.String("command", cmd.Name), zap.Error(err))
						}
					}
					if stopNow {
						state.Stop = true
					}
					log.Info("poll command handled", zap.String("command", cmd.Name), zap.Int64("from_user_id", msg.FromUserID))
				}
			} else if isCmd && !authorizer.IsAllowed(msg.FromUserID) {
				log.Debug("poll command rejected by policy", zap.String("command", cmd.Name), zap.Int64("from_user_id", msg.FromUserID))
			}
			if !state.Paused {
				toIngest = append(toIngest, msg)
			}
		}

		if len(toIngest) > 0 {
			if err := onBatch(ctx, toIngest); err != nil {
				return ingested, false, err
			}
			ingested += len(toIngest)
		}
		if maxID := maxMessageID(fresh); maxID > *lastSeenID {
			*lastSeenID = maxID
			runtime.LastSeenID = *lastSeenID
		}

		if state.Stop {
			return ingested, true, nil
		}
		if len(fresh) < opts.BatchSize {
			return ingested, false, nil
		}
	}
}

func executePollCommand(
	ctx context.Context,
	api *tg.Client,
	opts Options,
	peer tg.InputPeerClass,
	chatID int64,
	msg domain.MessageRecord,
	cmd pollCommand,
	state *pollControlState,
	runtime *pollRuntimeState,
) (reply string, handled bool, stop bool, err error) {
	switch cmd.Name {
	case "ping":
		start := time.Now()
		_ = api // keep signature extensible for future command handlers needing API.
		latency := time.Since(start)
		return fmt.Sprintf("pong | latency=%s", latency.Round(time.Millisecond)), true, false, nil
	case "status":
		report := defaultStatusReport(chatID, runtime.Snapshot(state.Paused))
		if opts.StatusHandler != nil {
			custom, customErr := opts.StatusHandler(ctx, opts.AccountID, chatID, runtime.Snapshot(state.Paused))
			if customErr != nil {
				report += "\nstatus handler error: " + customErr.Error()
			} else if strings.TrimSpace(custom) != "" {
				report += "\n" + strings.TrimSpace(custom)
			}
		}
		return report, true, false, nil
	case "search":
		if opts.SearchHandler == nil {
			return "search handler is not configured", true, false, nil
		}
		query := strings.TrimSpace(strings.Join(cmd.Args, " "))
		if query == "" {
			return "usage: /search <query>", true, false, nil
		}
		limit := opts.SearchLimit
		if limit <= 0 {
			limit = 7
		}
		result, searchErr := opts.SearchHandler(ctx, opts.AccountID, chatID, query, limit)
		if searchErr != nil {
			return "search failed: " + searchErr.Error(), true, false, nil
		}
		if strings.TrimSpace(result) == "" {
			result = "no matches"
		}
		return result, true, false, nil
	case "ask":
		if opts.AskHandler == nil {
			return "ask handler is not configured", true, false, nil
		}
		if len(cmd.Args) == 0 {
			return "usage: /ask [user_id] <question>", true, false, nil
		}
		targetUserID := int64(0)
		questionArgs := cmd.Args
		if len(questionArgs) > 1 {
			if uid, parseErr := strconv.ParseInt(questionArgs[0], 10, 64); parseErr == nil && uid > 0 {
				targetUserID = uid
				questionArgs = questionArgs[1:]
			}
		}
		question := strings.TrimSpace(strings.Join(questionArgs, " "))
		if question == "" {
			return "usage: /ask [user_id] <question>", true, false, nil
		}
		answer, askErr := opts.AskHandler(ctx, opts.AccountID, chatID, targetUserID, question)
		if askErr != nil {
			return "ask failed: " + askErr.Error(), true, false, nil
		}
		if strings.TrimSpace(answer) == "" {
			answer = "no answer"
		}
		return answer, true, false, nil
	default:
		h, status := applyPollCommand(state, cmd)
		if !h {
			return "", false, false, nil
		}
		return status, true, state.Stop, nil
	}
}

func sendCommandReply(ctx context.Context, api *tg.Client, peer tg.InputPeerClass, replyToMsgID int, text string) error {
	text = truncateForTelegram(strings.TrimSpace(text), 3500)
	if text == "" {
		return nil
	}
	req := &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: rand.Int63(),
	}
	if replyToMsgID > 0 {
		req.ReplyTo = &tg.InputReplyToMessage{ReplyToMsgID: replyToMsgID}
	}
	_, err := api.MessagesSendMessage(ctx, req)
	return err
}

func sendOwnerReport(ctx context.Context, api *tg.Client, opts Options, report string, knownUsers map[int64]*tg.User) error {
	report = strings.TrimSpace(report)
	if report == "" {
		return nil
	}
	ownerPeer, err := resolveOwnerPeer(ctx, api, opts, knownUsers)
	if err != nil {
		return err
	}
	req := &tg.MessagesSendMessageRequest{
		Peer:     ownerPeer,
		Message:  truncateForTelegram(report, 3500),
		RandomID: rand.Int63(),
	}
	_, err = api.MessagesSendMessage(ctx, req)
	return err
}

func resolveOwnerPeer(ctx context.Context, api *tg.Client, opts Options, knownUsers map[int64]*tg.User) (tg.InputPeerClass, error) {
	if username := strings.TrimSpace(opts.OwnerUsername); username != "" {
		if !strings.HasPrefix(username, "@") {
			username = "@" + username
		}
		return resolvePeer(ctx, api, username)
	}
	if opts.OwnerID > 0 {
		if u, ok := knownUsers[opts.OwnerID]; ok {
			return &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}, nil
		}
		selfID, err := resolveSelfUserID(ctx, api)
		if err == nil && selfID == opts.OwnerID {
			return &tg.InputPeerSelf{}, nil
		}
	}
	return nil, errors.New("unable to resolve owner peer")
}

func defaultStatusReport(chatID int64, runtime PollRuntime) string {
	uptime := time.Since(runtime.StartedAt)
	if runtime.StartedAt.IsZero() {
		uptime = 0
	}
	status := "running"
	if runtime.Paused {
		status = "paused"
	}
	return fmt.Sprintf(
		"status=%s\nchat_id=%d\nuptime=%s\ningested=%d\nlast_seen_id=%d\ncommands_handled=%d",
		status,
		chatID,
		uptime.Round(time.Second),
		runtime.IngestedMessages,
		runtime.LastSeenID,
		runtime.CommandsHandled,
	)
}

func defaultSummaryReport(chatID int64, runtime PollRuntime) string {
	return "parser summary\n" + defaultStatusReport(chatID, runtime)
}

func truncateForTelegram(v string, max int) string {
	v = strings.TrimSpace(v)
	if max <= 0 || len(v) <= max {
		return v
	}
	return v[:max]
}

func filterFreshMessages(batch []domain.MessageRecord, minID int) []domain.MessageRecord {
	if len(batch) == 0 {
		return nil
	}
	out := make([]domain.MessageRecord, 0, len(batch))
	for _, m := range batch {
		if m.MessageID > minID {
			out = append(out, m)
		}
	}
	return out
}

func maxMessageID(batch []domain.MessageRecord) int {
	maxID := 0
	for _, m := range batch {
		if m.MessageID > maxID {
			maxID = m.MessageID
		}
	}
	return maxID
}

func resolveSelfUserID(ctx context.Context, api *tg.Client) (int64, error) {
	users, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err != nil {
		return 0, err
	}
	for _, u := range users {
		if user, ok := u.(*tg.User); ok {
			return user.GetID(), nil
		}
	}
	return 0, errors.New("unable to resolve self user id")
}

func resolveAdminIDsForPeer(ctx context.Context, api *tg.Client, peer tg.InputPeerClass) ([]int64, error) {
	// Admin lookup depends on peer type.
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		ids, err := fetchChannelAdminIDs(ctx, api, p)
		if err != nil {
			return nil, err
		}
		return uniqueInt64(ids), nil
	case *tg.InputPeerChat:
		ids, err := fetchChatAdminIDs(ctx, api, p)
		if err != nil {
			return nil, err
		}
		return uniqueInt64(ids), nil
	default:
		return nil, nil
	}
}

func fetchChannelAdminIDs(ctx context.Context, api *tg.Client, peer *tg.InputPeerChannel) ([]int64, error) {
	if peer == nil {
		return nil, nil
	}
	channel := &tg.InputChannel{ChannelID: peer.ChannelID, AccessHash: peer.AccessHash}
	const pageLimit = 200
	offset := 0
	ids := make([]int64, 0, 64)
	for {
		resp, err := api.ChannelsGetParticipants(ctx, &tg.ChannelsGetParticipantsRequest{
			Channel: channel,
			Filter:  &tg.ChannelParticipantsAdmins{},
			Offset:  offset,
			Limit:   pageLimit,
			Hash:    0,
		})
		if err != nil {
			return nil, err
		}
		list, ok := resp.(*tg.ChannelsChannelParticipants)
		if !ok || len(list.Participants) == 0 {
			return uniqueInt64(ids), nil
		}
		for _, part := range list.Participants {
			if uid := channelParticipantUserID(part); uid > 0 {
				ids = append(ids, uid)
			}
		}
		if len(list.Participants) < pageLimit {
			return uniqueInt64(ids), nil
		}
		offset += len(list.Participants)
	}
}

func fetchChatAdminIDs(ctx context.Context, api *tg.Client, peer *tg.InputPeerChat) ([]int64, error) {
	if peer == nil {
		return nil, nil
	}
	full, err := api.MessagesGetFullChat(ctx, peer.ChatID)
	if err != nil {
		return nil, err
	}
	chatFull, ok := full.FullChat.(*tg.ChatFull)
	if !ok {
		return nil, nil
	}
	participants, ok := chatFull.Participants.(*tg.ChatParticipants)
	if !ok {
		return nil, nil
	}
	ids := make([]int64, 0, len(participants.Participants))
	for _, p := range participants.Participants {
		switch v := p.(type) {
		case *tg.ChatParticipantAdmin:
			ids = append(ids, v.GetUserID())
		case *tg.ChatParticipantCreator:
			ids = append(ids, v.GetUserID())
		}
	}
	return uniqueInt64(ids), nil
}

func channelParticipantUserID(part tg.ChannelParticipantClass) int64 {
	switch v := part.(type) {
	case *tg.ChannelParticipant:
		return v.GetUserID()
	case *tg.ChannelParticipantSelf:
		return v.GetUserID()
	case *tg.ChannelParticipantAdmin:
		return v.GetUserID()
	case *tg.ChannelParticipantCreator:
		return v.GetUserID()
	default:
		return 0
	}
}

func uniqueInt64(in []int64) []int64 {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(in))
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func normalizeBatch(
	opts Options,
	resp tg.MessagesMessagesClass,
	fallbackChatID int64,
) ([]domain.MessageRecord, msgpeer.Entities, map[int]*tg.Message, bool) {
	var (
		messages tg.MessageClassArray
		entities msgpeer.Entities
		last     bool
	)
	switch r := resp.(type) {
	case *tg.MessagesMessages:
		messages = r.Messages
		entities = msgpeer.EntitiesFromResult(r)
		last = true
	case *tg.MessagesMessagesSlice:
		messages = r.Messages
		entities = msgpeer.EntitiesFromResult(r)
		last = len(r.Messages) < opts.BatchSize
	case *tg.MessagesChannelMessages:
		messages = r.Messages
		entities = msgpeer.EntitiesFromResult(r)
		last = len(r.Messages) < opts.BatchSize
	default:
		return nil, entities, nil, true
	}
	if len(messages) == 0 {
		return nil, entities, nil, true
	}

	sort.SliceStable(messages, func(i, j int) bool { return messages[i].GetID() > messages[j].GetID() })
	out := make([]domain.MessageRecord, 0, len(messages))
	mediaSource := make(map[int]*tg.Message, len(messages))
	for _, mc := range messages {
		nm, ok := mc.AsNotEmpty()
		if !ok {
			continue
		}
		msgID := nm.GetID()
		if msgID == 0 {
			continue
		}

		chatID := fallbackChatID
		if chatID == 0 {
			chatID = chatIDFromPeer(nm.GetPeerID())
		}

		fromUserID := int64(0)
		if from, ok := nm.GetFromID(); ok {
			fromUserID = userIDFromPeer(from)
		}
		if fromUserID == 0 {
			fromUserID = userIDFromPeer(nm.GetPeerID())
		}

		rec := domain.MessageRecord{
			AccountID:  opts.AccountID,
			ChatID:     chatID,
			MessageID:  msgID,
			Date:       time.Unix(int64(nm.GetDate()), 0).UTC(),
			FromUserID: fromUserID,
		}

		switch m := nm.(type) {
		case *tg.Message:
			mediaSource[msgID] = m
			rec.Text = m.Message
			rec.URLs = analysis.ExtractURLs(m.Message)
			rec.MentionsUserIDs = mentionIDs(m, entities)
			meta := mediaMetadata(m)
			rec.MediaType = meta.MediaType
			rec.HasVoice = meta.HasVoice
			rec.MediaID = meta.MediaID
			rec.MediaFileHash = meta.FileHash
			rec.MediaFileName = meta.FileName
			rec.MediaFileSize = meta.FileSize
			if rec.FromDisplayName == "" {
				rec.FromDisplayName = displayName(rec.FromFirstName, rec.FromLastName, rec.FromUsername)
			}
		case *tg.MessageService:
			rec.Text = m.Action.TypeName()
		}

		if fromUserID > 0 {
			if u, ok := entities.User(fromUserID); ok {
				rec.FromFirstName = u.FirstName
				rec.FromLastName = u.LastName
				if username, ok := u.GetUsername(); ok {
					rec.FromUsername = username
				}
				rec.FromDisplayName = displayName(rec.FromFirstName, rec.FromLastName, rec.FromUsername)
				rec.FromAvatarRef, rec.FromAvatarFile = avatarInfo(u, opts)
			}
		}

		if reply, ok := nm.GetReplyTo(); ok {
			if header, ok := reply.(*tg.MessageReplyHeader); ok {
				if rid, ok := header.GetReplyToMsgID(); ok {
					v := rid
					rec.ReplyToMsgID = &v
				}
				if tid, ok := header.GetReplyToTopID(); ok {
					v := tid
					rec.ReplyToTopID = &v
				}
				rec.ForumTopic = header.GetForumTopic()
			}
		}

		if opts.WithRaw {
			if raw, err := json.Marshal(mc); err == nil {
				rec.Raw = raw
			}
		}
		out = append(out, rec)
	}
	return out, entities, mediaSource, last
}

func enrichBatchBios(
	ctx context.Context,
	api *tg.Client,
	batch []domain.MessageRecord,
	entities msgpeer.Entities,
	bioCache map[int64]string,
	log *zap.Logger,
) {
	for i := range batch {
		uid := batch[i].FromUserID
		if uid <= 0 {
			continue
		}
		if bio, ok := bioCache[uid]; ok {
			batch[i].FromBio = bio
			continue
		}

		u, ok := entities.User(uid)
		if !ok {
			continue
		}
		full, err := api.UsersGetFullUser(ctx, u.AsInput())
		if err != nil {
			log.Debug("users.getFullUser failed", zap.Int64("user_id", uid), zap.Error(err))
			continue
		}
		about, _ := full.FullUser.GetAbout()
		bioCache[uid] = about
		batch[i].FromBio = about
	}
}

func avatarInfo(u *tg.User, opts Options) (ref string, file string) {
	_ = opts
	if u == nil {
		return "", ""
	}
	photoClass, ok := u.GetPhoto()
	if !ok {
		return "", ""
	}
	photo, ok := photoClass.AsNotEmpty()
	if !ok {
		return "", ""
	}
	ref = fmt.Sprintf("tg://userphoto/%d/%d", u.GetID(), photo.GetPhotoID())
	file = ""
	return ref, file
}

func enrichBatchAvatars(
	ctx context.Context,
	api *tg.Client,
	batch []domain.MessageRecord,
	entities msgpeer.Entities,
	avatarDL *avatarDownloader,
	log *zap.Logger,
) {
	if avatarDL == nil {
		return
	}
	type avatarResolved struct {
		ref  string
		file string
	}
	cache := make(map[int64]avatarResolved, 256)
	for i := range batch {
		uid := batch[i].FromUserID
		if uid <= 0 {
			continue
		}
		if av, ok := cache[uid]; ok {
			if batch[i].FromAvatarRef == "" {
				batch[i].FromAvatarRef = av.ref
			}
			if av.file != "" {
				batch[i].FromAvatarFile = av.file
			}
			continue
		}
		u, ok := entities.User(uid)
		if !ok {
			continue
		}
		ref, file, err := avatarDL.Resolve(ctx, api, u)
		if err != nil {
			log.Debug("avatar download failed", zap.Int64("user_id", uid), zap.Error(err))
			continue
		}
		cache[uid] = avatarResolved{ref: ref, file: file}
		if batch[i].FromAvatarRef == "" {
			batch[i].FromAvatarRef = ref
		}
		if file != "" {
			batch[i].FromAvatarFile = file
		}
	}
}

func enrichBatchMedia(
	ctx context.Context,
	api *tg.Client,
	batch []domain.MessageRecord,
	mediaSource map[int]*tg.Message,
	mediaDL *mediaDownloader,
	log *zap.Logger,
) {
	if mediaDL == nil || len(mediaSource) == 0 {
		return
	}
	for i := range batch {
		src, ok := mediaSource[batch[i].MessageID]
		if !ok || src == nil {
			continue
		}
		path, err := mediaDL.Resolve(ctx, api, &batch[i], src)
		if err != nil {
			log.Debug("media download failed", zap.Int("message_id", batch[i].MessageID), zap.Error(err))
			continue
		}
		if strings.TrimSpace(path) != "" {
			batch[i].MediaLocalPath = path
		}
	}
}

func mentionIDs(m *tg.Message, entities msgpeer.Entities) []int64 {
	if m == nil || len(m.Entities) == 0 {
		return mentionIDsByUsername(m, entities)
	}
	seen := map[int64]struct{}{}
	out := make([]int64, 0, 4)
	for _, entity := range m.Entities {
		switch v := entity.(type) {
		case *tg.MessageEntityMentionName:
			if v.UserID == 0 {
				continue
			}
			if _, ok := seen[v.UserID]; ok {
				continue
			}
			seen[v.UserID] = struct{}{}
			out = append(out, v.UserID)
		}
	}
	for _, uid := range mentionIDsByUsername(m, entities) {
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mentionIDsByUsername(m *tg.Message, entities msgpeer.Entities) []int64 {
	if m == nil || strings.TrimSpace(m.Message) == "" {
		return nil
	}
	usernameToID := make(map[string]int64, len(entities.Users()))
	for uid, u := range entities.Users() {
		if u == nil {
			continue
		}
		username, ok := u.GetUsername()
		if !ok {
			continue
		}
		username = strings.TrimSpace(strings.ToLower(username))
		if username == "" {
			continue
		}
		usernameToID[username] = uid
	}
	if len(usernameToID) == 0 {
		return nil
	}
	seen := map[int64]struct{}{}
	out := make([]int64, 0, 2)
	for _, token := range strings.Fields(m.Message) {
		if !strings.HasPrefix(token, "@") {
			continue
		}
		name := strings.TrimPrefix(token, "@")
		name = strings.Trim(name, "()[]{}<>,.;:!?\"'`")
		name = strings.TrimSpace(strings.ToLower(name))
		if name == "" {
			continue
		}
		uid, ok := usernameToID[name]
		if !ok || uid <= 0 {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	return out
}

func mediaMetadata(m *tg.Message) mediaMeta {
	if m == nil || m.Media == nil {
		return mediaMeta{}
	}
	switch v := m.Media.(type) {
	case *tg.MessageMediaPhoto:
		photo, ok := v.Photo.AsNotEmpty()
		if !ok {
			return mediaMeta{MediaType: "photo"}
		}
		id := strconv.FormatInt(photo.GetID(), 10)
		hash := hashString("photo|" + id)
		return mediaMeta{MediaType: "photo", MediaID: id, FileHash: hash}
	case *tg.MessageMediaDocument:
		doc, ok := v.Document.AsNotEmpty()
		if !ok {
			return mediaMeta{MediaType: "document"}
		}
		meta := mediaMeta{
			MediaType: "document",
			MediaID:   strconv.FormatInt(doc.GetID(), 10),
			FileSize:  doc.GetSize(),
		}
		isAnimated := false
		isSticker := false
		voice := false
		for _, attr := range doc.Attributes {
			switch a := attr.(type) {
			case *tg.DocumentAttributeAnimated:
				isAnimated = true
			case *tg.DocumentAttributeSticker:
				isSticker = true
			case *tg.DocumentAttributeFilename:
				meta.FileName = strings.TrimSpace(a.FileName)
			case *tg.DocumentAttributeAudio:
				if a.GetVoice() {
					voice = true
				}
			}
		}
		meta.HasVoice = voice
		mime := strings.TrimSpace(strings.ToLower(doc.MimeType))
		switch {
		case isSticker:
			meta.MediaType = "sticker"
		case isAnimated || mime == "image/gif":
			meta.MediaType = "gif"
		case voice:
			meta.MediaType = "voice"
		case strings.HasPrefix(mime, "video/"):
			meta.MediaType = "video"
		case strings.HasPrefix(mime, "audio/"):
			meta.MediaType = "audio"
		default:
			meta.MediaType = "document"
		}
		meta.FileHash = hashString(strings.Join([]string{
			meta.MediaType,
			mime,
			meta.FileName,
			strconv.FormatInt(meta.FileSize, 10),
		}, "|"))
		return meta
	default:
		return mediaMeta{MediaType: strings.TrimSpace(strings.ToLower(v.TypeName()))}
	}
}

func hashString(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	h := sha256.Sum256([]byte(v))
	return hex.EncodeToString(h[:])
}

func displayName(first, last, username string) string {
	full := strings.TrimSpace(first + " " + last)
	if full != "" {
		return full
	}
	if username != "" {
		return "@" + username
	}
	return ""
}

func chatIDFromInputPeer(p tg.InputPeerClass) int64 {
	switch v := p.(type) {
	case *tg.InputPeerChannel:
		return v.ChannelID
	case *tg.InputPeerChat:
		return v.ChatID
	case *tg.InputPeerUser:
		return v.UserID
	default:
		return 0
	}
}

func chatIDFromPeer(p tg.PeerClass) int64 {
	switch v := p.(type) {
	case *tg.PeerChannel:
		return v.ChannelID
	case *tg.PeerChat:
		return v.ChatID
	case *tg.PeerUser:
		return v.UserID
	default:
		return 0
	}
}

func userIDFromPeer(p tg.PeerClass) int64 {
	if u, ok := p.(*tg.PeerUser); ok {
		return u.UserID
	}
	return 0
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
