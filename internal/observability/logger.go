package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// LoggerConfig controls the default structured logger output.
type LoggerConfig struct {
	Level                 string
	ServiceName           string
	ServiceNamespace      string
	ServiceVersion        string
	ServiceInstanceID     string
	DeploymentEnvironment string
}

// NewLogger returns a JSON logger shaped so common log agents can map it to the
// OpenTelemetry log data model with minimal transformation.
//
// Resource attributes (service.*, deployment.environment.name, telemetry.sdk.*)
// are intentionally not embedded in each log line payload. In OTLP pipelines,
// those should come from resource metadata once, not duplicated in message.*
// fields after log ingestion.
func NewLogger(w io.Writer, cfg LoggerConfig) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	level := parseLevel(cfg.Level)
	_ = cfg.ServiceName
	_ = cfg.ServiceNamespace
	_ = cfg.ServiceVersion
	_ = cfg.ServiceInstanceID
	_ = cfg.DeploymentEnvironment

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				return slog.String("timestamp", a.Value.Time().Format(time.RFC3339Nano))
			case slog.LevelKey:
				lvl := a.Value.Any().(slog.Level)
				return slog.Group("severity",
					slog.String("text", strings.ToUpper(lvl.String())),
					slog.Int("number", severityNumber(lvl)),
				)
			case slog.MessageKey:
				return slog.String("body", a.Value.String())
			default:
				return a
			}
		},
	})
	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func severityNumber(level slog.Level) int {
	switch {
	case level <= slog.LevelDebug:
		return 5
	case level < slog.LevelWarn:
		return 9
	case level < slog.LevelError:
		return 13
	default:
		return 17
	}
}

func valueOrDefault(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}
