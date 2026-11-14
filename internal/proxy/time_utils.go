package proxy

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// lokiSampleSeconds is the time Loki writes for a sample evaluated at ns
// nanoseconds: its engine keeps whole milliseconds (the sample time is
// current/1e6, a truncation, pkg/logql/range_vector.go) and prints them as
// seconds, FormatFloat(ms/1000, 'f', -1, 64) (model.Time.String). Dividing the
// nanoseconds instead keeps a fraction Loki drops and, above 2^53, rounds the
// division the other way often enough to print 1793653191.6499999 for
// 1793653191.65.
func lokiSampleSeconds(ns int64) float64 {
	ms := ns / int64(time.Millisecond)
	if ns%int64(time.Millisecond) < 0 {
		ms--
	}
	return float64(ms) / 1e3
}

// exactVLDuration writes d as a VictoriaLogs duration that reads back as
// exactly d. VictoriaLogs scales a decimal by its unit and truncates, so a step
// written as float seconds ("1.003s") reads as 1002999999 ns and moves every
// bucket edge by a nanosecond; an integer count of a unit does not.
func exactVLDuration(d time.Duration) string {
	switch {
	case d%time.Second == 0:
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	case d%time.Millisecond == 0:
		return strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
	}
	return strconv.FormatInt(int64(d), 10) + "ns"
}

// lokiWholeMillisecondStep reads the step of a range request as Loki does
// (parsePositiveStepDuration: float seconds, else a duration) and reports,
// when it is not a whole number of milliseconds, the step Loki evaluates on:
// the codec keeps Milliseconds(), a truncation (queryrange/codec.go
// parseRangeQuery). subMs is false for a missing or unreadable step and for
// one that is whole milliseconds already, which are left as they are.
func lokiWholeMillisecondStep(r *http.Request) (wholeMs int64, subMs bool) {
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok || step%time.Millisecond == 0 {
		return 0, false
	}
	return step.Milliseconds(), true
}

// withLokiStep returns r with its step replaced by ms whole milliseconds. The
// form is copied: r's is shared with the callers that hold it.
func withLokiStep(r *http.Request, ms int64) *http.Request {
	_ = r.ParseForm()
	clone := r.Clone(r.Context())
	step := strconv.FormatInt(ms, 10) + "ms"
	clone.Form.Set("step", step)
	if clone.PostForm.Has("step") {
		clone.PostForm.Set("step", step)
	}
	if query := clone.URL.Query(); query.Has("step") {
		query.Set("step", step)
		clone.URL.RawQuery = query.Encode()
	}
	return clone
}

// restampInstantVector writes the evaluation time Loki prints into every
// sample of a Loki vector body. VictoriaLogs stamps an instant stats result
// with float64(ns)/1e9 of its own query time, one nanosecond early, which
// prints a value like 1793653199.5830002 for 1793653199.583.
func restampInstantVector(body []byte, evalNs int64) []byte {
	const marker = `"value":[`
	stamp := strconv.FormatFloat(lokiSampleSeconds(evalNs), 'f', -1, 64)
	// The buffer grows as it is written: its size is not computed from the
	// length of the body, which is the backend's.
	var out bytes.Buffer
	for {
		at := bytes.Index(body, []byte(marker))
		if at < 0 {
			out.Write(body)
			return out.Bytes()
		}
		head := at + len(marker)
		comma := bytes.IndexByte(body[head:], ',')
		if comma < 0 {
			out.Write(body)
			return out.Bytes()
		}
		out.Write(body[:head])
		out.WriteString(stamp)
		body = body[head+comma:]
	}
}

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
	// An RFC 3339 time always holds a colon and a number never does: a number skips the two time layouts, whose
	// failed parses allocate their errors.
	if strings.IndexByte(value, ':') >= 0 {
		if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return parsed.UnixNano(), true
		}
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			return parsed.UnixNano(), true
		}
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
			return lokiFloatSecondsToNanos(floating), true
		}
	}
	return 0, false
}

// lokiFloatSecondsToNanos reads a timestamp written as float seconds the way
// Loki does (pkg/loghttp/params.go parseTimestamp): the fraction is rounded to
// a millisecond. Multiplying the whole value by 1e9 instead lands below the
// millisecond about half the time, where float64 steps are 256 ns, and Loki's
// engine truncating it to milliseconds would then print .703 for .704.
func lokiFloatSecondsToNanos(seconds float64) int64 {
	whole, fraction := math.Modf(seconds)
	fraction = math.Round(fraction*1000) / 1000
	return int64(whole)*int64(time.Second) + int64(fraction*float64(time.Second))
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

// nanosToVLTimestamp converts a nanosecond Unix timestamp to the Unix-seconds
// string that VictoriaLogs expects for start/end params. Using this helper
// prevents accidentally forwarding nanosecond values to VL endpoints that
// only accept second precision, which would silently scan the wrong time range.
// A time that is not a whole second keeps its fraction, as decimal digits
// VictoriaLogs reads exactly (timeutil.TryParseUnixTimestamp): flooring it to
// the second moved an instant evaluation, or the end of a range read in steps
// under a second, up to a second early.
func nanosToVLTimestamp(nanos int64) string {
	seconds, fraction := nanos/int64(time.Second), nanos%int64(time.Second)
	if fraction <= 0 {
		return strconv.FormatInt(seconds, 10)
	}
	digits := strings.TrimRight(fmt.Sprintf("%09d", fraction), "0")
	return strconv.FormatInt(seconds, 10) + "." + digits
}

// formatVLStatsTimestamp normalizes any Loki/Grafana timestamp representation
// (Unix seconds, Unix nanoseconds, or RFC3339) to the Unix-seconds string that
// VictoriaLogs requires for stats_query and stats_query_range start/end/time params.
//
// Use this (not formatVLTimestamp) whenever building params for VL stats endpoints.
// formatVLTimestamp passes numeric inputs through unchanged — correct for log query
// endpoints that accept nanoseconds, but silently wrong for stats endpoints.
func formatVLStatsTimestamp(raw string) string {
	nanos, ok := parseLokiTimeToUnixNano(raw)
	if !ok {
		return raw
	}
	return nanosToVLTimestamp(nanos)
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
	grafanaRangeTokens = []string{
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

// parseStepToNanos converts a step parameter to nanoseconds. It extends
// parsePositiveStepDuration to also handle raw nanosecond integer values that
// Grafana occasionally sends (e.g. "60000000000" = 60s in nanoseconds) which
// parsePositiveStepDuration rejects because the numeric value is too large to
// be interpreted as seconds.
func parseStepToNanos(step string) (int64, bool) {
	if d, ok := parsePositiveStepDuration(step); ok {
		return int64(d), true
	}
	// Try raw nanosecond integer (e.g. from Grafana: 60000000000 = 60s).
	v := strings.TrimSpace(step)
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
		return n, true
	}
	return 0, false
}

// parsePrometheusStyleDuration reads a sum of number-unit parts ("1d", "2w3d")
// with the LogQL parser's own reading (logql.ParseUnitDuration).
func parsePrometheusStyleDuration(value string) (time.Duration, bool) {
	return logqlpkg.ParseUnitDuration(value)
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

func parseRequestedBucketRange(start, end, step string, maxBuckets int) (requestedBucketRange, bool) {
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
	if count <= 0 || count > maxBuckets {
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

func patternBackendQueryLimit(start, end, step string, patternLimit, maxRows, maxBuckets int) int {
	if patternLimit <= 0 {
		patternLimit = 50
	}
	if patternLimit > maxPatternResponseLimit {
		patternLimit = maxPatternResponseLimit
	}
	factor := 20
	if bucketRange, ok := parseRequestedBucketRange(start, end, step, maxBuckets); ok {
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
	if limit > maxRows {
		limit = maxRows
	}
	return limit
}
