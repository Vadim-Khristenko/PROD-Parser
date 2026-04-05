package telegramingest

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type Options struct {
	AccountID     string
	APIID         int
	APIHash       string
	SessionPath   string
	Peer          string
	OwnerID       int64
	OwnerUsername string

	ForcedChatID     int64
	StartID          int
	Limit            int
	BatchSize        int
	FetchBio         bool
	FetchAvatars     bool
	PollCommands     bool
	PollWithBackfill bool
	PollInterval     time.Duration
	CommandPolicy    string
	CommandUserIDs   []int64
	CommandPrefix    string
	SearchLimit      int
	AvatarDir        string
	AvatarCachePath  string
	AvatarBig        bool
	FetchMedia       bool
	MediaDir         string
	MediaCachePath   string
	WithRaw          bool

	RateInterval   time.Duration
	RateBurst      int
	RequestTimeout time.Duration
	RetryMax       int

	TopicMode        string
	EmbeddingModel   string
	LLMFallbackModel string

	SearchHandler  CommandSearchHandler
	AskHandler     CommandAskHandler
	StatusHandler  CommandStatusHandler
	SummaryHandler CommandSummaryHandler

	Log *zap.Logger
}

type PollRuntime struct {
	StartedAt        time.Time
	IngestedMessages int
	LastSeenID       int
	Paused           bool
	CommandsHandled  int
	LastCommandAt    time.Time
}

type CommandSearchHandler func(ctx context.Context, accountID string, chatID int64, query string, limit int) (string, error)
type CommandAskHandler func(ctx context.Context, accountID string, chatID int64, userID int64, question string) (string, error)
type CommandStatusHandler func(ctx context.Context, accountID string, chatID int64, runtime PollRuntime) (string, error)
type CommandSummaryHandler func(ctx context.Context, accountID string, chatID int64, runtime PollRuntime) (string, error)

type Credentials struct {
	Phone    string
	Password string
	CodeFunc func(ctx context.Context, hint string) (string, error)
}

type BatchHandler func(ctx context.Context, messages []domain.MessageRecord) error
