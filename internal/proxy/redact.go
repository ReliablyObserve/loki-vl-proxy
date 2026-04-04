package proxy

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
)

// secretPatterns matches common secret formats found in queries, URLs, and headers.
var secretPatterns = []*regexp.Regexp{
	// Bearer tokens
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9\-._~+/]+=*`),
	// Authorization header value
	regexp.MustCompile(`(?i)(authorization[=:]\s*")[^"]*`),
	// API keys in query strings: api_key=..., apikey=..., apiKey=...
	regexp.MustCompile(`(?i)((?:api[_-]?key|api[_-]?token|access[_-]?token|secret[_-]?key|auth[_-]?token|token|password|passwd|pwd|secret)\s*[=:]\s*)[^\s&"',;}\]]+`),
	// AWS access key (AKIA...)
	regexp.MustCompile(`(?:AKIA|ASIA)[A-Z0-9]{16}`),
	// AWS secret key patterns (40 chars base64-like after known prefix)
	regexp.MustCompile(`(?i)(aws[_-]?secret[_-]?access[_-]?key\s*[=:]\s*)[A-Za-z0-9/+=]{40}`),
	// Generic long hex/base64 tokens (32+ chars that look like secrets)
	regexp.MustCompile(`(?i)((?:x-api-key|x-auth-token|x-token)\s*[=:]\s*)[A-Za-z0-9\-._~+/]{32,}`),
	// Basic auth in URLs: user:password@host
	regexp.MustCompile(`(://[^:]+:)[^@]+(@)`),
	// GCP service account key
	regexp.MustCompile(`(?i)(private_key[=:"]\s*)[^\s"]+`),
	// Connection strings with passwords
	regexp.MustCompile(`(?i)((?:postgres|mysql|redis|mongo|amqp)://[^:]*:)[^@]+(@)`),
}

// redactReplacement is used for all redacted values.
const redactReplacement = "***REDACTED***"

// RedactSecrets replaces potential secrets in a string with a redaction marker.
func RedactSecrets(s string) string {
	for _, re := range secretPatterns {
		s = re.ReplaceAllStringFunc(s, func(match string) string {
			loc := re.FindStringSubmatchIndex(match)
			if loc == nil {
				return redactReplacement
			}
			// If there's a capturing group, preserve the prefix and redact the rest
			if len(loc) >= 4 && loc[2] >= 0 {
				prefix := match[:loc[3]-loc[0]]
				// Check if there's a suffix group (for URL patterns like user:pass@host)
				if len(loc) >= 6 && loc[4] >= 0 {
					suffix := match[loc[4]-loc[0] : loc[5]-loc[0]]
					return prefix + redactReplacement + suffix
				}
				return prefix + redactReplacement
			}
			return redactReplacement
		})
	}
	return s
}

// redactingHandler wraps an slog.Handler to redact secrets from log attributes.
type redactingHandler struct {
	inner slog.Handler
}

// NewRedactingHandler wraps an existing slog.Handler with secret redaction.
func NewRedactingHandler(inner slog.Handler) slog.Handler {
	return &redactingHandler{inner: inner}
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	// Redact the message itself
	r.Message = RedactSecrets(r.Message)

	// Redact all string attributes
	var redacted []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		redacted = append(redacted, redactAttr(a))
		return true
	})

	// Create a new record with redacted attributes
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	nr.AddAttrs(redacted...)
	return h.inner.Handle(ctx, nr)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		redacted[i] = redactAttr(a)
	}
	return &redactingHandler{inner: h.inner.WithAttrs(redacted)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	// Redact known sensitive attribute keys entirely
	lowerKey := strings.ToLower(a.Key)
	for _, sensitive := range []string{"password", "passwd", "secret", "token", "api_key", "apikey", "authorization", "credential"} {
		if strings.Contains(lowerKey, sensitive) {
			a.Value = slog.StringValue(redactReplacement)
			return a
		}
	}

	// Redact patterns within string values
	if a.Value.Kind() == slog.KindString {
		a.Value = slog.StringValue(RedactSecrets(a.Value.String()))
	}
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		redacted := make([]slog.Attr, len(attrs))
		for i, ga := range attrs {
			redacted[i] = redactAttr(ga)
		}
		a.Value = slog.GroupValue(redacted...)
	}
	return a
}
