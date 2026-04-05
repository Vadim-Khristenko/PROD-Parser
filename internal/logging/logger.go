package logging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLevel    = "info"
	defaultFilePath = "./logs/prod-parser.log"
)

type Options struct {
	Level          string
	ConsoleEnabled bool
	ConsolePretty  bool
	FileEnabled    bool
	FilePath       string
}

func NewFromEnv() (*zap.Logger, func() error, error) {
	return New(OptionsFromEnv())
}

func OptionsFromEnv() Options {
	return Options{
		Level:          readEnvString("LOG_LEVEL", defaultLevel),
		ConsoleEnabled: readEnvBool("LOG_CONSOLE_ENABLED", true),
		ConsolePretty:  readEnvBool("LOG_CONSOLE_PRETTY", true),
		FileEnabled:    readEnvBool("LOG_FILE_ENABLED", true),
		FilePath:       readEnvString("LOG_FILE_PATH", defaultFilePath),
	}
}

func New(opts Options) (*zap.Logger, func() error, error) {
	level := parseLevel(opts.Level)
	if !opts.ConsoleEnabled && !opts.FileEnabled {
		opts.ConsoleEnabled = true
	}

	cores := make([]zapcore.Core, 0, 2)
	cleanups := make([]func() error, 0, 2)

	if opts.ConsoleEnabled {
		consoleEncCfg := zap.NewDevelopmentEncoderConfig()
		consoleEncCfg.TimeKey = "time"
		consoleEncCfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05")
		if opts.ConsolePretty {
			consoleEncCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		} else {
			consoleEncCfg.EncodeLevel = zapcore.CapitalLevelEncoder
		}
		consoleEncoder := zapcore.NewConsoleEncoder(consoleEncCfg)
		cores = append(cores, zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level))
	}

	if opts.FileEnabled {
		filePath := strings.TrimSpace(opts.FilePath)
		if filePath == "" {
			filePath = defaultFilePath
		}
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			return nil, nil, fmt.Errorf("create log directory: %w", err)
		}
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file: %w", err)
		}

		jsonEncCfg := zap.NewProductionEncoderConfig()
		jsonEncCfg.TimeKey = "time"
		jsonEncCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		jsonEncoder := zapcore.NewJSONEncoder(jsonEncCfg)
		cores = append(cores, zapcore.NewCore(jsonEncoder, zapcore.AddSync(file), level))
		cleanups = append(cleanups, file.Close)
	}

	if len(cores) == 0 {
		return nil, nil, errors.New("no logging outputs configured")
	}

	logger := zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	cleanups = append(cleanups, logger.Sync)

	cleanup := func() error {
		var err error
		for i := len(cleanups) - 1; i >= 0; i-- {
			err = multierr.Append(err, cleanups[i]())
		}
		return err
	}

	return logger, cleanup, nil
}

func parseLevel(raw string) zapcore.Level {
	lvl := zapcore.InfoLevel
	if err := lvl.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(raw)))); err != nil {
		return zapcore.InfoLevel
	}
	return lvl
}

func readEnvString(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func readEnvBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
