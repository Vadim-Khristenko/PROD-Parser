//go:build !telegram

package telegramingest

import (
	"context"
	"errors"
)

func Run(ctx context.Context, opts Options, creds Credentials, onBatch BatchHandler) (int, error) {
	_ = ctx
	_ = opts
	_ = creds
	_ = onBatch
	return 0, errors.New("telegram ingestion is disabled in this build; rebuild with -tags telegram")
}
