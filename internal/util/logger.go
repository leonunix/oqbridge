package util

import (
	"log/slog"
	"os"
	"strings"
)

// SetupLogger initializes the default slog logger with the given level string.
func SetupLogger(level string) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: l,
	})
	slog.SetDefault(slog.New(handler))
}
