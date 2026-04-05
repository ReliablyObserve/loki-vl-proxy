package observability

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
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
func NewLogger(w io.Writer, cfg LoggerConfig) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	level := parseLevel(cfg.Level)
	attrs := []any{
		"service.name", valueOrDefault(cfg.ServiceName, "loki-vl-proxy"),
		"service.version", valueOrDefault(cfg.ServiceVersion, "dev"),
		"service.instance.id", valueOrDefault(cfg.ServiceInstanceID, defaultInstanceID()),
		"telemetry.sdk.name", "loki-vl-proxy",
		"telemetry.sdk.language", "go",
		"telemetry.sdk.version", runtime.Version(),
	}
	if strings.TrimSpace(cfg.ServiceNamespace) != "" {
		attrs = append(attrs, "service.namespace", strings.TrimSpace(cfg.ServiceNamespace))
	}
	if strings.TrimSpace(cfg.DeploymentEnvironment) != "" {
		attrs = append(attrs, "deployment.environment.name", strings.TrimSpace(cfg.DeploymentEnvironment))
	}

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
	return slog.New(handler).With(attrs...)
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

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func valueOrDefault(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}
