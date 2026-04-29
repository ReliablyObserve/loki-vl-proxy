package proxy

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func parseFlexibleUnixSeconds(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.Unix(), true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.Unix(), true
	}
	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return normalizeUnixSeconds(integer), true
	}
	if floating, err := strconv.ParseFloat(value, 64); err == nil {
		abs := math.Abs(floating)
		switch {
		case abs >= 1_000_000_000_000_000_000:
			return int64(floating / 1_000_000_000), true
		case abs >= 1_000_000_000_000_000:
			return int64(floating / 1_000_000), true
		case abs >= 1_000_000_000_000:
			return int64(floating / 1_000), true
		default:
			return int64(floating), true
		}
	}
	return 0, false
}

func parseFlexibleUnixNanos(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UnixNano(), true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UnixNano(), true
	}
	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return normalizeUnixNanos(integer), true
	}
	if floating, err := strconv.ParseFloat(value, 64); err == nil {
		abs := math.Abs(floating)
		switch {
		case abs >= 1_000_000_000_000_000_000:
			return int64(floating), true
		case abs >= 1_000_000_000_000_000:
			return int64(floating * 1_000), true
		case abs >= 1_000_000_000_000:
			return int64(floating * 1_000_000), true
		default:
			return int64(floating * float64(time.Second)), true
		}
	}
	return 0, false
}

func normalizeUnixSeconds(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000:
		return v / 1_000_000_000
	case abs >= 1_000_000_000_000_000:
		return v / 1_000_000
	case abs >= 1_000_000_000_000:
		return v / 1_000
	default:
		return v
	}
}

func normalizeUnixNanos(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000:
		return v
	case abs >= 1_000_000_000_000_000:
		return v * 1_000
	case abs >= 1_000_000_000_000:
		return v * 1_000_000
	default:
		return v * int64(time.Second)
	}
}

var (
	prometheusDurationPartRE = regexp.MustCompile(`(?i)([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)`)
	grafanaRangeTokens       = []string{
		"$__rate_interval_ms",
		"$__interval_ms",
		"$__range_ms",
		"$__rate_interval",
		"$__range_s",
		"$__interval",
		"$__range",
		"$__auto",
	}
)

func parsePositiveStepDuration(step string) (time.Duration, bool) {
	value := strings.TrimSpace(step)
	if value == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return d, true
	}
	if d, ok := parsePrometheusStyleDuration(value); ok {
		return d, true
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds <= 0 {
		return 0, false
	}
	nanos := seconds * float64(time.Second)
	if nanos > float64(math.MaxInt64) {
		return 0, false
	}
	d := time.Duration(nanos)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

func parsePrometheusStyleDuration(value string) (time.Duration, bool) {
	parts := prometheusDurationPartRE.FindAllStringSubmatch(value, -1)
	if len(parts) == 0 {
		return 0, false
	}

	totalSeconds := 0.0
	var consumed strings.Builder
	for _, part := range parts {
		numeric, err := strconv.ParseFloat(part[1], 64)
		if err != nil || numeric <= 0 {
			return 0, false
		}

		switch strings.ToLower(part[2]) {
		case "ns":
			totalSeconds += numeric / 1e9
		case "us", "µs":
			totalSeconds += numeric / 1e6
		case "ms":
			totalSeconds += numeric / 1e3
		case "s":
			totalSeconds += numeric
		case "m":
			totalSeconds += numeric * 60
		case "h":
			totalSeconds += numeric * 3600
		case "d":
			totalSeconds += numeric * 86400
		case "w":
			totalSeconds += numeric * 7 * 86400
		case "y":
			totalSeconds += numeric * 365 * 86400
		default:
			return 0, false
		}

		consumed.WriteString(part[0])
	}

	if consumed.String() != value {
		return 0, false
	}

	nanos := totalSeconds * float64(time.Second)
	if nanos <= 0 || nanos > float64(math.MaxInt64) {
		return 0, false
	}

	d := time.Duration(nanos)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

func resolveGrafanaRangeTemplateTokens(query, start, end, step string) string {
	if !strings.Contains(query, "$__") && !strings.Contains(query, "${__") {
		return query
	}

	replacements := map[string]string{}
	for _, token := range grafanaRangeTokens {
		if duration, ok := resolveGrafanaTemplateTokenDuration(token, start, end, step); ok {
			replacements[token] = formatLogQLDuration(duration)
		}
	}

	if len(replacements) == 0 {
		return query
	}

	normalized := query
	for _, token := range grafanaRangeTokens {
		replacement, ok := replacements[token]
		if !ok {
			continue
		}
		braced := "${" + strings.TrimPrefix(token, "$") + "}"
		normalized = strings.ReplaceAll(normalized, braced, replacement)
		normalized = strings.ReplaceAll(normalized, token, replacement)
	}
	return normalized
}

func resolveGrafanaTemplateTokenDuration(token, start, end, step string) (time.Duration, bool) {
	canonicalToken, ok := canonicalGrafanaRangeToken(token)
	if !ok {
		return 0, false
	}

	stepDur, stepOK := parsePositiveStepDuration(step)
	if !stepOK {
		stepDur = time.Minute
	}

	rangeDur := stepDur
	startNanos, startOK := parseFlexibleUnixNanos(start)
	endNanos, endOK := parseFlexibleUnixNanos(end)
	if startOK && endOK && endNanos > startNanos {
		rangeDur = time.Duration(endNanos - startNanos)
	}
	if rangeDur <= 0 {
		rangeDur = stepDur
	}

	switch canonicalToken {
	case "$__auto", "$__interval", "$__interval_ms":
		return stepDur, true
	case "$__rate_interval", "$__rate_interval_ms":
		rateInterval := stepDur * 4
		if rateInterval < time.Minute {
			rateInterval = time.Minute
		}
		return rateInterval, true
	case "$__range", "$__range_s", "$__range_ms":
		return rangeDur, true
	default:
		return 0, false
	}
}

func canonicalGrafanaRangeToken(token string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(token))
	if normalized == "" {
		return "", false
	}
	if strings.HasPrefix(normalized, "${") && strings.HasSuffix(normalized, "}") {
		normalized = "$" + strings.TrimSuffix(strings.TrimPrefix(normalized, "${"), "}")
	}
	if strings.HasPrefix(normalized, "__") {
		normalized = "$" + normalized
	}

	switch normalized {
	case "$__auto",
		"$__interval",
		"$__interval_ms",
		"$__rate_interval",
		"$__rate_interval_ms",
		"$__range",
		"$__range_s",
		"$__range_ms":
		return normalized, true
	default:
		return "", false
	}
}

func formatLogQLDuration(d time.Duration) string {
	if d <= 0 {
		return "1s"
	}
	if d%time.Hour == 0 {
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	}
	if d%time.Minute == 0 {
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	}
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	if d%time.Millisecond == 0 {
		return strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

func parseStepSeconds(step string) (int64, bool) {
	d, ok := parsePositiveStepDuration(step)
	if !ok || d < time.Second {
		return 0, false
	}
	return int64(d / time.Second), true
}

func parseRequestedBucketRange(start, end, step string) (requestedBucketRange, bool) {
	startSeconds, ok := parseFlexibleUnixSeconds(start)
	if !ok {
		return requestedBucketRange{}, false
	}
	endSeconds, ok := parseFlexibleUnixSeconds(end)
	if !ok || endSeconds < startSeconds {
		return requestedBucketRange{}, false
	}
	stepSeconds, ok := parseStepSeconds(step)
	if !ok || stepSeconds <= 0 {
		return requestedBucketRange{}, false
	}
	count := int(((endSeconds - startSeconds) / stepSeconds) + 1)
	if count <= 0 || count > maxZeroFillBuckets {
		return requestedBucketRange{}, false
	}
	return requestedBucketRange{
		start: startSeconds,
		end:   endSeconds,
		step:  stepSeconds,
		count: count,
	}, true
}

func (br requestedBucketRange) bucketFor(ts int64) (int64, bool) {
	if ts < br.start || ts > br.end {
		return 0, false
	}
	offset := ts - br.start
	return br.start + ((offset / br.step) * br.step), true
}

func patternBackendQueryLimit(start, end, step string, patternLimit int) int {
	if patternLimit <= 0 {
		patternLimit = 50
	}
	if patternLimit > maxPatternResponseLimit {
		patternLimit = maxPatternResponseLimit
	}
	factor := 20
	if bucketRange, ok := parseRequestedBucketRange(start, end, step); ok {
		factor = bucketRange.count
		if factor < 20 {
			factor = 20
		}
		if factor > 200 {
			factor = 200
		}
	}
	limit := patternLimit * factor
	if limit < 1000 {
		limit = 1000
	}
	if limit > maxPatternBackendQueryLimit {
		limit = maxPatternBackendQueryLimit
	}
	return limit
}
