package proxy

import (
	"bufio"
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

var (
	// ansiEscapeRe matches ANSI escape sequences (color codes, cursor movement, etc.)
	ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	// ipFilterRE matches LogQL ip() filter: label = ip("CIDR") or label == ip("CIDR")
	ipFilterRE = regexp.MustCompile(`(\w+)\s*==?\s*ip\(\s*"([^"]+)"\s*\)`)
)

const maxPatternResponseLimit = 1000
const (
	patternVarPlaceholder  = "<_>"
	patternNumPlaceholder  = "<num>"
	patternUUIDPlaceholder = "<uuid>"
	patternIPPlaceholder   = "<ip>"
	patternPathPlaceholder = "<path>"
	patternHexPlaceholder  = "<hex>"
	patternTSPlaceholder   = "<ts>"
	patternMinTokens       = 4
	patternMaxTokens       = 80
	patternSimThreshold    = 0.3
	patternMaxLineLength   = 3000
	patternPrefixDepth     = 4
)

type patternBucket struct {
	pattern     string
	level       string
	buckets     map[int64]int
	total       int
	tokens      []string
	spacesAfter []int
}

type patternExtractionStats struct {
	scannedLines  int
	observedLines int
	patternCount  int
}

func (s patternExtractionStats) hitLimit(limit int) bool {
	return limit > 0 && s.scannedLines >= limit
}

// decolorizeStreams strips ANSI escape sequences from all log lines in streams.
// Implements Loki's `| decolorize` pipe at the proxy level.
// TODO: Remove when VL adds native decolorize support.
func decolorizeStreams(streams []map[string]interface{}) {
	for _, stream := range streams {
		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}
		for i, val := range values {
			if len(val) >= 2 {
				values[i][1] = ansiEscapeRe.ReplaceAllString(val[1], "")
			}
		}
	}
}

// ipFilterStreams filters log entries where a label value matches a CIDR range.
// Implements Loki's `| label = ip("CIDR")` filter at the proxy level.
// TODO: Remove when VL adds native IP range filtering.
func ipFilterStreams(streams []map[string]interface{}, labelName, cidr string) []map[string]interface{} {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		// Not a valid CIDR — try as single IP
		ip := net.ParseIP(cidr)
		if ip == nil {
			return streams // invalid filter, return unfiltered
		}
		mask := net.CIDRMask(32, 32)
		if ip.To4() == nil {
			mask = net.CIDRMask(128, 128)
		}
		ipNet = &net.IPNet{IP: ip, Mask: mask}
	}

	result := make([]map[string]interface{}, 0, len(streams))
	for _, stream := range streams {
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			continue
		}

		ipStr, ok := labels[labelName]
		if !ok {
			continue
		}

		ip := net.ParseIP(ipStr)
		if ip != nil && ipNet.Contains(ip) {
			result = append(result, stream)
		}
	}
	return result
}

// parseIPFilter extracts the label name and CIDR from a LogQL ip() filter expression.
// Supports: `| addr = ip("192.168.0.0/16")` and `| addr == ip("10.0.0.0/8")`
func parseIPFilter(query string) (label, cidr string, ok bool) {
	m := ipFilterRE.FindStringSubmatch(query)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// applyLineFormatTemplate applies a Go text/template to log line values.
// Implements Loki's `| line_format "{{.status}} {{.method | ToUpper}}"` with full template support.
// TODO: Remove when VL adds equivalent template formatting.
func applyLineFormatTemplate(streams []map[string]interface{}, tmplStr string) ([]map[string]interface{}, error) {
	return applyLineFormatTemplateWithContext(context.Background(), streams, tmplStr, lineFormatAfter{})
}

// applyQueryLineFormat renders the line_format template of a log query into
// the entries of streams (applyLineFormatTemplateWithContext), with what the
// query's later stages do with the error labels of a failed template. A
// query without line_format returns streams as they are.
func applyQueryLineFormat(ctx context.Context, streams []map[string]interface{}, query string) ([]map[string]interface{}, error) {
	tmpl := extractLineFormatTemplate(query)
	if tmpl == "" {
		return streams, nil
	}
	return applyLineFormatTemplateWithContext(ctx, streams, tmpl, lineFormatAfterStages(query))
}

// Loki's line_format on an entry its template fails on
// (pkg/logql/log/fmt.go LineFormatter.Process): the line stays as it was
// and the entry gets __error__="TemplateFormatErr" and __error_details__
// with the template error, as parsed labels.
const (
	lineFormatErrorType = "TemplateFormatErr"
	errorLabel          = "__error__"
	errorDetailsLabel   = "__error_details__"
)

// lineFormatAfter is what the stages after a query's line_format do with
// the error labels a failed template adds.
type lineFormatAfter struct {
	// dropFailed: a later | __error__="" drops the entry.
	dropFailed bool
	// dropError, dropDetails: a later | drop or | keep removes the label.
	dropError, dropDetails bool
}

// lineFormatAfterStages reads the stages after the line_format of query, in
// order: | __error__="" drops an entry that still carries __error__, and
// | drop / | keep remove the error labels.
func lineFormatAfterStages(query string) lineFormatAfter {
	var after lineFormatAfter
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return after
	}
	seen := false
	for _, stage := range lq.Pipeline {
		switch st := stage.(type) {
		case *logqlpkg.LineFormatStage:
			seen = true
		case *logqlpkg.LabelFilterStage:
			if seen && !after.dropError && strings.Join(strings.Fields(st.Raw), "") == errorLabel+`=""` {
				after.dropFailed = true
			}
		case *logqlpkg.DropStage:
			if !seen {
				continue
			}
			for _, name := range st.Labels {
				after.dropError = after.dropError || name == errorLabel
				after.dropDetails = after.dropDetails || name == errorDetailsLabel
			}
		case *logqlpkg.KeepStage:
			if !seen {
				continue
			}
			kept := map[string]bool{}
			for _, name := range st.Labels {
				kept[name] = true
			}
			for _, m := range st.Matchers {
				kept[m.Name] = true
			}
			after.dropError = after.dropError || !kept[errorLabel]
			after.dropDetails = after.dropDetails || !kept[errorDetailsLabel]
		}
	}
	return after
}

// lineFormatParseError is Loki's answer to a template its parser rejects.
func lineFormatParseError(tmplStr string, err error) error {
	return fmt.Errorf("parse error : stage '| line_format %s' : invalid line template: %w", strconv.Quote(tmplStr), err)
}

// applyLineFormatTemplateWithContext renders tmplStr into the line of every
// entry of streams and returns the streams. An entry the template fails on
// keeps its line and gets Loki's error labels: in the stream labels of an
// uncategorized entry (it moves to a stream of its own) and in the parsed
// labels of a categorized one; after says what later stages do with them.
// A budget the proxy enforces (templateLimitError) or a canceled context
// fails the call.
func applyLineFormatTemplateWithContext(ctx context.Context, streams []map[string]interface{}, tmplStr string, after lineFormatAfter) ([]map[string]interface{}, error) {
	// Register Loki-compatible template functions
	funcMap := template.FuncMap{
		"printf": boundedTemplatePrintf,
		"print": func(args ...any) (string, error) {
			return boundedTemplatePrint(false, args...)
		},
		"println": func(args ...any) (string, error) {
			return boundedTemplatePrint(true, args...)
		},
		"html":       boundedTemplateEscape(template.HTMLEscapeString),
		"js":         boundedTemplateEscape(template.JSEscapeString),
		"urlquery":   boundedTemplateEscape(func(s string) string { return template.URLQueryEscaper(s) }),
		"ToUpper":    boundedTemplateString(strings.ToUpper),
		"ToLower":    boundedTemplateString(strings.ToLower),
		"Title":      boundedTemplateString(titleCase),
		"Replace":    boundedTemplateReplace,
		"TrimSpace":  strings.TrimSpace,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"HasPrefix":  strings.HasPrefix,
		"HasSuffix":  strings.HasSuffix,
		"Contains":   strings.Contains,
		"default": func(def string, val ...string) string {
			// In pipeline: {{.field | default "N/A"}} — val[0] is the piped value
			if len(val) > 0 && val[0] != "" {
				return val[0]
			}
			return def
		},
	}

	// A LogQL `| line_format "..."` template is client input by definition, and
	// Loki executes it the same way (text/template, not HTML). It runs sandboxed:
	// only the functions in funcMap, missingkey=zero, and every execution bounded
	// by instrumentTemplateBudget below. The template is named "line", as in
	// Loki, so error details read the same.
	tmpl, err := template.New("line").Option("missingkey=zero").Funcs(funcMap).Parse(tmplStr) // #nosec G708 -- LogQL line_format templates are client input by design; sandboxed and budgeted
	if err != nil {
		return nil, lineFormatParseError(tmplStr, err)
	}

	if err := instrumentTemplateBudget(tmpl, ctx); err != nil {
		return nil, err
	}
	run := &lineFormatRun{ctx: ctx, tmpl: tmpl, after: after, failedStreams: map[string]map[string]interface{}{}}
	out := streams[:0:0]
	for _, stream := range streams {
		if err := run.applyStream(stream); err != nil {
			return nil, err
		}
		if streamValueCount(stream) > 0 {
			out = append(out, stream)
		}
	}
	return append(out, run.failed...), nil
}

// lineFormatRun renders one template into the entries of a response.
type lineFormatRun struct {
	ctx   context.Context
	tmpl  *template.Template
	after lineFormatAfter
	// total is the formatted bytes so far (maxFormattedResponseBytes).
	total int
	// failed are the streams uncategorized failed entries moved to, by key.
	failed        []map[string]interface{}
	failedStreams map[string]map[string]interface{}
}

// format renders the template for one entry of a stream with labels.
func (r *lineFormatRun) format(labels map[string]string, line string, metadata any) (string, error) {
	data := make(map[string]string, safeAddCap(len(labels), 1))
	for k, v := range labels {
		data[k] = v
	}
	// Categorized tuples carry parsed fields outside the stream labels.
	if fields, ok := metadata.(map[string]interface{}); ok {
		for _, category := range []string{"structuredMetadata", "parsed"} {
			if values, ok := fields[category].(map[string]string); ok {
				for k, v := range values {
					data[k] = v
				}
			}
		}
	}
	inputBytes := len(line)
	for k, v := range data {
		inputBytes += len(k) + len(v)
	}
	if inputBytes > 1<<20 {
		return "", templateLimitError("line_format input limit exceeded")
	}
	data["_line"] = line
	buf := &templateOutput{remaining: min(maxFormattedLineBytes, maxFormattedResponseBytes-r.total)}
	if err := r.tmpl.Execute(buf, data); err != nil {
		return "", err
	}
	r.total += buf.Len()
	return buf.String(), nil
}

// entryFailure reports whether err is a template error Loki reports on the
// entry, rather than a failure of the whole request.
func (r *lineFormatRun) entryFailure(err error) bool {
	var limit templateLimitError
	return !errors.As(err, &limit) && r.ctx.Err() == nil
}

// errorLabels returns base with the error labels of a failed template that
// no later stage removes.
func (r *lineFormatRun) errorLabels(base map[string]string, err error) map[string]string {
	withErr := maps.Clone(base)
	if withErr == nil {
		withErr = map[string]string{}
	}
	if !r.after.dropError {
		withErr[errorLabel] = lineFormatErrorType
	}
	if !r.after.dropDetails {
		withErr[errorDetailsLabel] = err.Error()
	}
	return withErr
}

// failedStream returns the stream an uncategorized failed entry of a stream
// with labels moves to: its labels with the error labels.
func (r *lineFormatRun) failedStream(labels map[string]string, err error, tuples bool) map[string]interface{} {
	withErr := r.errorLabels(labels, err)
	key := canonicalLabelsKey(withErr)
	fs, ok := r.failedStreams[key]
	if !ok {
		fs = map[string]interface{}{"stream": withErr}
		if tuples {
			fs["values"] = [][]string{}
		} else {
			fs["values"] = []interface{}{}
		}
		r.failedStreams[key] = fs
		r.failed = append(r.failed, fs)
	}
	return fs
}

// failedTuple returns the tuple an entry the template failed on keeps in
// its stream: itself when the labels do not change, a copy with the error as
// parsed labels when it is categorized, and nil when a later stage drops it
// or it moved to a failed stream.
func (r *lineFormatRun) failedTuple(labels map[string]string, val []interface{}, err error) []interface{} {
	switch {
	case r.after.dropFailed:
		return nil
	case r.after.dropError && r.after.dropDetails:
		return val
	case len(val) > 2:
		// Categorized: the error labels are parsed labels. The metadata
		// maps may be shared read-only values.
		fields, _ := val[2].(map[string]interface{})
		withErr := maps.Clone(fields)
		if withErr == nil {
			withErr = map[string]interface{}{}
		}
		parsed, _ := fields["parsed"].(map[string]string)
		withErr["parsed"] = r.errorLabels(parsed, err)
		return []interface{}{val[0], val[1], withErr}
	}
	fs := r.failedStream(labels, err, false)
	fs["values"] = append(fs["values"].([]interface{}), val)
	return nil
}

// applyStream formats the entries of one stream in place; failed entries
// stay, move to a failed stream, or are dropped (failedTuple).
func (r *lineFormatRun) applyStream(stream map[string]interface{}) error {
	labels, _ := stream["stream"].(map[string]string)
	if labels == nil {
		labels = map[string]string{}
	}
	switch values := stream["values"].(type) {
	case [][]string:
		kept := values[:0]
		for _, val := range values {
			if len(val) < 2 {
				kept = append(kept, val)
				continue
			}
			line, err := r.format(labels, val[1], nil)
			if err == nil {
				val[1] = line
				kept = append(kept, val)
				continue
			}
			if !r.entryFailure(err) {
				return err
			}
			switch {
			case r.after.dropFailed:
			case r.after.dropError && r.after.dropDetails:
				kept = append(kept, val)
			default:
				fs := r.failedStream(labels, err, true)
				fs["values"] = append(fs["values"].([][]string), val)
			}
		}
		stream["values"] = kept
	case []interface{}:
		kept := values[:0]
		for _, value := range values {
			val, ok := value.([]interface{})
			if !ok || len(val) < 2 {
				return fmt.Errorf("invalid line_format tuple")
			}
			original, ok := val[1].(string)
			if !ok {
				return fmt.Errorf("invalid line_format line")
			}
			var metadata any
			if len(val) > 2 {
				metadata = val[2]
			}
			line, err := r.format(labels, original, metadata)
			if err == nil {
				val[1] = line
				kept = append(kept, val)
				continue
			}
			if !r.entryFailure(err) {
				return err
			}
			if tuple := r.failedTuple(labels, val, err); tuple != nil {
				kept = append(kept, tuple)
			}
		}
		stream["values"] = kept
	}
	return nil
}

// streamValueCount is the number of entries of a converted stream.
func streamValueCount(stream map[string]interface{}) int {
	switch values := stream["values"].(type) {
	case [][]string:
		return len(values)
	case []interface{}:
		return len(values)
	}
	return 0
}

// extractLineFormatTemplate extracts the template string from a line_format query.
// Returns empty string if not found.
var lineFormatTemplateRE = regexp.MustCompile("\\|\\s*line_format\\s+(\"(?:[^\"\\\\]|\\\\.)*\"|`[^`]*`)")

func extractLineFormatTemplate(query string) string {
	match := lineFormatTemplateRE.FindStringSubmatch(query)
	if len(match) != 2 {
		return ""
	}
	value, err := strconv.Unquote(match[1])
	if err != nil {
		return ""
	}
	return value
}

// extractLogPatterns implements Loki-style Drain-inspired pattern extraction.
// It tokenizes lines with punctuation-aware splitting, clusters by similarity,
// and returns Grafana Logs Drilldown compatible pattern buckets.
func extractLogPatterns(vlBody []byte, step string, limit int, levels logRowLevels) []map[string]interface{} {
	patterns, _ := extractLogPatternsWithStats(vlBody, step, limit, levels)
	return patterns
}

// defaultLogRowLevels is the level derivation used where no proxy
// configuration is in scope: Loki's defaults with the bounded body scan on.
func defaultLogRowLevels() logRowLevels {
	return (*Proxy)(nil).newLogRowLevels()
}

func extractLogPatternsWithStats(vlBody []byte, step string, limit int, levels logRowLevels) ([]map[string]interface{}, patternExtractionStats) {
	if len(vlBody) == 0 {
		return nil, patternExtractionStats{}
	}
	patterns, stats := extractLogPatternsStreamWithStats(bytes.NewReader(vlBody), step, limit, levels)
	if stats.observedLines > 0 {
		return patterns, stats
	}
	// Fallback: body may be a wrapped JSON object ({"results":[...]}) rather
	// than NDJSON. Try parsing with fastjson and collecting observations
	// recursively to avoid the full interface{} tree allocation.
	stepSeconds := parsePatternStepSeconds(step)
	miner := newPatternMiner()
	fjParser := vlFJParserPool.Get()
	defer vlFJParserPool.Put(fjParser)
	fjVal, err := fjParser.ParseBytes(vlBody)
	if err != nil {
		return nil, stats
	}
	collectPatternObservationsFromFJ(miner, fjVal, stepSeconds, "", &stats.observedLines, &levels)
	if stats.observedLines == 0 {
		return nil, stats
	}
	out := buildPatternResponse(miner, limit)
	stats.patternCount = len(out)
	return out, stats
}

// extractLogPatternsStreamWithStats scans VL NDJSON from r, extracts the
// message, timestamp, and level from each line, and feeds them to the pattern
// miner. Uses fastjson to avoid map[string]interface{} allocation per line.
func extractLogPatternsStreamWithStats(r io.Reader, step string, limit int, levels logRowLevels) ([]map[string]interface{}, patternExtractionStats) {
	stepSeconds := parsePatternStepSeconds(step)
	miner := newPatternMiner()
	stats := patternExtractionStats{}
	rows := levels
	if rows.streams == nil {
		rows.streams = make(map[string]logRowStream, 16)
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		stats.scannedLines++

		fjParser := vlFJParserPool.Get()
		fjVal, err := fjParser.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(fjParser)
			continue
		}

		// Extract all fields before returning the parser — fjVal memory is
		// owned by the parser's arena and becomes invalid after Put.
		msg := patternMessageFromFJ(fjVal)
		var timeStr, level string
		if msg != "" {
			for _, key := range []string{"_time", "time", "timestamp", "ts"} {
				if b := fjVal.GetStringBytes(key); len(b) > 0 {
					timeStr = string(b)
					break
				}
			}
			level = patternLevelFromFJ(fjVal, &rows)
		}
		vlFJParserPool.Put(fjParser)

		if msg == "" || timeStr == "" {
			continue
		}
		unixSeconds, ok := parsePatternUnixSecondsStr(timeStr)
		if !ok {
			continue
		}

		bucket := unixSeconds
		if stepSeconds > 0 {
			bucket = (bucket / stepSeconds) * stepSeconds
		}
		miner.Observe(level, msg, bucket)
		stats.observedLines++
	}
	if stats.observedLines > 0 {
		patterns := buildPatternResponse(miner, limit)
		stats.patternCount = len(patterns)
		return patterns, stats
	}
	return nil, stats
}

// patternMessageFromFJ extracts the log message from a fastjson value,
// trying the same field priority as patternMessageFromEntry.
func patternMessageFromFJ(v *fj.Value) string {
	for _, key := range []string{"_msg", "message", "msg", "line", "log"} {
		b := v.GetStringBytes(key)
		if len(b) > 0 {
			s := strings.TrimSpace(string(b))
			if s != "" {
				return s
			}
		}
	}
	return ""
}

// patternLevelFromFJ returns the pattern level of a VictoriaLogs row: Loki's
// lowercased detected_level. rows caches stream labels across calls; nil
// parses them per call.
func patternLevelFromFJ(v *fj.Value, rows *logRowLevels) string {
	obj, err := v.Object()
	if err != nil {
		return levelUnknown
	}
	if rows == nil {
		levels := defaultLogRowLevels()
		rows = &levels
	}
	if rows.streams == nil {
		rows.streams = make(map[string]logRowStream, 1)
	}
	return patternLevel(rows.fjRow(v, obj, rows.stream(v.GetStringBytes("_stream"))))
}

// parsePatternUnixSecondsStr parses a time string to unix seconds.
func parsePatternUnixSecondsStr(timeStr string) (int64, bool) {
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return parsed.Unix(), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return parsed.Unix(), true
	}
	if ns, ok := parseLokiTimeToUnixNano(timeStr); ok {
		return ns / int64(time.Second), true
	}
	return 0, false
}

// patternLevelFromEntry is patternLevelFromFJ for a decoded VL row.
func patternLevelFromEntry(entry map[string]interface{}, levels *logRowLevels) string {
	stream := parseStreamLabels(asString(entry["_stream"]))
	msg, _ := entry["_msg"].(string)
	rows := levels
	if rows == nil {
		defaults := defaultLogRowLevels()
		rows = &defaults
	}
	return patternLevel(rows.mapRow(entry, msg, logRowStream{labels: stream, levels: levelFieldsFromLabels(stream)}))
}

func extractLogPatternsFromWindowEntries(entries []queryRangeWindowEntry, step string, limit int) []map[string]interface{} {
	patterns, _ := extractLogPatternsFromWindowEntriesWithStats(entries, step, limit)
	return patterns
}

func extractLogPatternsFromWindowEntriesWithStats(entries []queryRangeWindowEntry, step string, limit int) ([]map[string]interface{}, patternExtractionStats) {
	if len(entries) == 0 {
		return nil, patternExtractionStats{}
	}
	stepSeconds := parsePatternStepSeconds(step)
	miner := newPatternMiner()
	stats := patternExtractionStats{}
	for _, entry := range entries {
		stats.scannedLines++
		if entry.Ts == "" {
			continue
		}
		unixSeconds, ok := parsePatternUnixSeconds(entry.Ts)
		if !ok {
			continue
		}
		msg := strings.TrimSpace(entry.Msg)
		if msg == "" {
			continue
		}
		level := strings.ToLower(windowEntryDetectedLevel(entry))
		if level == "" {
			level = levelUnknown
		}
		bucket := unixSeconds
		if stepSeconds > 0 {
			bucket = (bucket / stepSeconds) * stepSeconds
		}
		miner.Observe(level, msg, bucket)
		stats.observedLines++
	}
	patterns := buildPatternResponse(miner, limit)
	stats.patternCount = len(patterns)
	return patterns, stats
}

func parsePatternStepSeconds(step string) int64 {
	stepSeconds := int64(60)
	if step == "" {
		return stepSeconds
	}
	if d, ok := parsePositiveStepDuration(step); ok && d >= time.Second {
		return int64(d / time.Second)
	}
	return stepSeconds
}

func parsePatternUnixSeconds(raw interface{}) (int64, bool) {
	timeStr, ok := stringifyEntryValue(raw)
	if !ok {
		return 0, false
	}
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return parsed.Unix(), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return parsed.Unix(), true
	}
	// Reuse Loki-compatible parser for numeric, RFC3339, and relative ("now-24h")
	// boundaries so pattern sampling/filling stays stable with Drilldown ranges.
	if ns, ok := parseLokiTimeToUnixNano(timeStr); ok {
		return ns / int64(time.Second), true
	}
	return 0, false
}

func patternMessageFromEntry(entry map[string]interface{}) (string, bool) {
	for _, key := range []string{"_msg", "message", "msg", "line", "log"} {
		msg, ok := stringifyEntryValue(entry[key])
		if ok && strings.TrimSpace(msg) != "" {
			return msg, true
		}
	}
	return "", false
}

func patternUnixSecondsFromEntry(entry map[string]interface{}) (int64, bool) {
	for _, key := range []string{"_time", "time", "timestamp", "ts"} {
		if unixSeconds, ok := parsePatternUnixSeconds(entry[key]); ok {
			return unixSeconds, true
		}
	}
	return 0, false
}

func collectPatternObservationsFromJSON(miner *patternMiner, decoded interface{}, stepSeconds int64, inheritedLevel string, observed *int, levels *logRowLevels) {
	switch value := decoded.(type) {
	case map[string]interface{}:
		if msg, ok := patternMessageFromEntry(value); ok {
			if unixSeconds, ok := patternUnixSecondsFromEntry(value); ok {
				level := inheritedLevel
				if level == "" {
					level = patternLevelFromEntry(value, levels)
				}
				bucket := unixSeconds
				if stepSeconds > 0 {
					bucket = (bucket / stepSeconds) * stepSeconds
				}
				miner.Observe(level, msg, bucket)
				*observed = *observed + 1
			}
		}
		if rows, ok := value["results"].([]interface{}); ok {
			for _, row := range rows {
				collectPatternObservationsFromJSON(miner, row, stepSeconds, inheritedLevel, observed, levels)
			}
		}
		if rows, ok := value["values"].([]interface{}); ok {
			for _, row := range rows {
				collectPatternObservationsFromJSON(miner, row, stepSeconds, inheritedLevel, observed, levels)
			}
		}
		if dataMap, ok := value["data"].(map[string]interface{}); ok {
			if resultRows, ok := dataMap["result"].([]interface{}); ok {
				for _, row := range resultRows {
					rowMap, ok := row.(map[string]interface{})
					if !ok {
						continue
					}
					level := inheritedLevel
					if stream, ok := rowMap["stream"].(map[string]interface{}); ok {
						for _, name := range []string{detectedLevelExtractedLabel, detectedLevelLabel} {
							if v, ok := stringifyEntryValue(stream[name]); ok && v != "" {
								level = strings.ToLower(v)
								break
							}
						}
					}
					if level == "" {
						level = levelUnknown
					}
					values, _ := rowMap["values"].([]interface{})
					for _, pairRaw := range values {
						pair, ok := pairRaw.([]interface{})
						if !ok || len(pair) < 2 {
							continue
						}
						unixSeconds, ok := parsePatternUnixSeconds(pair[0])
						if !ok {
							continue
						}
						msg, ok := stringifyEntryValue(pair[1])
						if !ok || strings.TrimSpace(msg) == "" {
							continue
						}
						bucket := unixSeconds
						if stepSeconds > 0 {
							bucket = (bucket / stepSeconds) * stepSeconds
						}
						miner.Observe(level, msg, bucket)
						*observed = *observed + 1
					}
				}
			}
		}
	case []interface{}:
		for _, item := range value {
			collectPatternObservationsFromJSON(miner, item, stepSeconds, inheritedLevel, observed, levels)
		}
	}
}

// collectPatternObservationsFromFJ mirrors collectPatternObservationsFromJSON
// but operates on a *fj.Value to avoid the full interface{} tree allocation
// in the wrapped-JSON fallback path. It is read-only over the parser's arena
// memory; the caller owns parser lifecycle.
//
//nolint:gocyclo // recursive fastjson walker with per-type switch (object/array/leaf) and timestamp/level extraction; branching is inherent to the JSON shape.
func collectPatternObservationsFromFJ(miner *patternMiner, v *fj.Value, stepSeconds int64, inheritedLevel string, observed *int, levels *logRowLevels) {
	if v == nil {
		return
	}
	switch v.Type() {
	case fj.TypeObject:
		// Try leaf entry: msg + time on this object.
		if msg := patternMessageFromFJ(v); msg != "" {
			var timeStr string
			for _, key := range []string{"_time", "time", "timestamp", "ts"} {
				if b := v.GetStringBytes(key); len(b) > 0 {
					timeStr = string(b)
					break
				}
			}
			if timeStr != "" {
				if unixSeconds, ok := parsePatternUnixSecondsStr(timeStr); ok {
					level := inheritedLevel
					if level == "" {
						level = patternLevelFromFJ(v, levels)
					}
					bucket := unixSeconds
					if stepSeconds > 0 {
						bucket = (bucket / stepSeconds) * stepSeconds
					}
					miner.Observe(level, msg, bucket)
					*observed = *observed + 1
				}
			}
		}
		// Recurse into "results" array.
		if rows := v.GetArray("results"); rows != nil {
			for _, row := range rows {
				collectPatternObservationsFromFJ(miner, row, stepSeconds, inheritedLevel, observed, levels)
			}
		}
		// Recurse into "values" array.
		if rows := v.GetArray("values"); rows != nil {
			for _, row := range rows {
				collectPatternObservationsFromFJ(miner, row, stepSeconds, inheritedLevel, observed, levels)
			}
		}
		// Handle Loki-style {"data": {"result": [{stream:{}, values:[[ts,msg],...]}]}}.
		if dataObj := v.Get("data"); dataObj != nil && dataObj.Type() == fj.TypeObject {
			if resultRows := dataObj.GetArray("result"); resultRows != nil {
				for _, row := range resultRows {
					if row.Type() != fj.TypeObject {
						continue
					}
					level := inheritedLevel
					if stream := row.Get("stream"); stream != nil && stream.Type() == fj.TypeObject {
						for _, name := range []string{detectedLevelExtractedLabel, detectedLevelLabel} {
							if b := stream.GetStringBytes(name); len(b) > 0 {
								level = strings.ToLower(string(b))
								break
							}
						}
					}
					if level == "" {
						level = levelUnknown
					}
					values := row.GetArray("values")
					for _, pairRaw := range values {
						if pairRaw.Type() != fj.TypeArray {
							continue
						}
						pair, _ := pairRaw.Array()
						if len(pair) < 2 {
							continue
						}
						tsStr, ok := stringifyFJValue(pair[0])
						if !ok || tsStr == "" {
							continue
						}
						unixSeconds, ok := parsePatternUnixSecondsStr(tsStr)
						if !ok {
							continue
						}
						msg, ok := stringifyFJValue(pair[1])
						if !ok || strings.TrimSpace(msg) == "" {
							continue
						}
						bucket := unixSeconds
						if stepSeconds > 0 {
							bucket = (bucket / stepSeconds) * stepSeconds
						}
						miner.Observe(level, msg, bucket)
						*observed = *observed + 1
					}
				}
			}
		}
	case fj.TypeArray:
		items, _ := v.Array()
		for _, item := range items {
			collectPatternObservationsFromFJ(miner, item, stepSeconds, inheritedLevel, observed, levels)
		}
	}
}

func buildPatternResponse(miner *patternMiner, limit int) []map[string]interface{} {
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPatternResponseLimit {
		limit = maxPatternResponseLimit
	}
	clusters := miner.AllClusters()
	entries := make([]*patternBucket, 0, len(clusters))
	entries = append(entries, clusters...)
	for _, e := range entries {
		e.pattern = miner.tokenizer.Join(e.tokens, e.spacesAfter)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].total != entries[j].total {
			return entries[i].total > entries[j].total
		}
		if entries[i].level != entries[j].level {
			return entries[i].level < entries[j].level
		}
		return entries[i].pattern < entries[j].pattern
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}

	result := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		sampleTimes := make([]int64, 0, len(e.buckets))
		for bucket := range e.buckets {
			sampleTimes = append(sampleTimes, bucket)
		}
		sort.Slice(sampleTimes, func(i, j int) bool { return sampleTimes[i] < sampleTimes[j] })

		samples := make([][]interface{}, 0, len(sampleTimes))
		for _, bucket := range sampleTimes {
			samples = append(samples, []interface{}{bucket, e.buckets[bucket]})
		}

		item := map[string]interface{}{
			"pattern": e.pattern,
			"samples": samples,
		}
		if e.level != "" {
			item["level"] = e.level
		}
		result = append(result, item)
	}
	return result
}

type patternMiner struct {
	tokenizer *patternLineTokenizer
	// level -> tokenCount -> structural signature -> clusters
	groups map[string]map[int]map[string][]*patternBucket
}

func newPatternMiner() *patternMiner {
	return &patternMiner{
		tokenizer: newPatternLineTokenizer(),
		groups:    make(map[string]map[int]map[string][]*patternBucket),
	}
}

func (m *patternMiner) Observe(level, line string, bucket int64) {
	tokens, spacesAfter, ok := m.tokenizer.Tokenize(line)
	if !ok {
		return
	}
	tokenCount := len(tokens)

	levelGroups := m.groups[level]
	if levelGroups == nil {
		levelGroups = make(map[int]map[string][]*patternBucket)
		m.groups[level] = levelGroups
	}
	signatureGroups := levelGroups[tokenCount]
	if signatureGroups == nil {
		signatureGroups = make(map[string][]*patternBucket)
		levelGroups[tokenCount] = signatureGroups
	}
	signature := patternStructureSignature(tokens)
	candidates := signatureGroups[signature]
	cluster := bestPatternCluster(candidates, tokens)
	if cluster == nil {
		cluster = &patternBucket{
			level:       level,
			buckets:     make(map[int64]int),
			tokens:      cloneTokens(tokens),
			spacesAfter: cloneInts(spacesAfter),
		}
		signatureGroups[signature] = append(signatureGroups[signature], cluster)
	} else {
		cluster.tokens = mergePatternTemplate(cluster.tokens, tokens)
	}
	cluster.buckets[bucket]++
	cluster.total++
}

func (m *patternMiner) AllClusters() []*patternBucket {
	out := make([]*patternBucket, 0)
	for _, byCount := range m.groups {
		for _, bySignature := range byCount {
			for _, clusters := range bySignature {
				out = append(out, clusters...)
			}
		}
	}
	return out
}

func bestPatternCluster(candidates []*patternBucket, tokens []string) *patternBucket {
	var match *patternBucket
	maxSim := -1.0
	maxParamCount := -1

	for _, cluster := range candidates {
		if !patternPrefixCompatible(cluster.tokens, tokens) {
			continue
		}
		curSim, paramCount := getPatternSimilarity(cluster.tokens, tokens)
		if paramCount < 0 {
			continue
		}
		if curSim > maxSim || (curSim == maxSim && paramCount > maxParamCount) {
			maxSim = curSim
			maxParamCount = paramCount
			match = cluster
		}
	}
	if maxSim >= patternSimThreshold {
		return match
	}
	return nil
}

func patternPrefixCompatible(templateTokens, tokens []string) bool {
	if len(templateTokens) != len(tokens) || len(tokens) == 0 {
		return false
	}
	depth := patternPrefixDepth
	if depth > len(tokens) {
		depth = len(tokens)
	}
	for i := 0; i < depth; i++ {
		templateToken := templateTokens[i]
		inputToken := tokens[i]
		if patternPlaceholderMatchesToken(templateToken, inputToken) || templateToken == inputToken {
			continue
		}
		if samePatternPlaceholderKind(templateToken, inputToken) || patternTokenHasDigits(templateToken) || patternTokenHasDigits(inputToken) {
			continue
		}
		return false
	}
	return true
}

func patternTokenHasDigits(token string) bool {
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			return true
		}
	}
	return false
}

func getPatternSimilarity(templateTokens, tokens []string) (float64, int) {
	if len(templateTokens) != len(tokens) || len(templateTokens) == 0 {
		return 0, -1
	}
	simTokens := 0
	paramCount := 0
	for i := range templateTokens {
		switch {
		case patternPlaceholderMatchesToken(templateTokens[i], tokens[i]):
			paramCount++
		case templateTokens[i] == tokens[i]:
			simTokens++
		}
	}
	return float64(simTokens) / float64(len(templateTokens)), paramCount
}

func mergePatternTemplate(templateTokens, tokens []string) []string {
	if len(templateTokens) != len(tokens) {
		return templateTokens
	}
	for i := range templateTokens {
		if templateTokens[i] != tokens[i] {
			templateTokens[i] = commonPatternPlaceholder(templateTokens[i], tokens[i])
		}
	}
	return templateTokens
}

type patternLineTokenizer struct {
	includeDelimiters [128]rune
	excludeDelimiters [128]rune
}

// tokenizerBuf holds pre-allocated slices reused across Tokenize calls via a pool.
// The caller must call pool.Put after it is done with the returned slices.
type tokenizerBuf struct {
	tokens      []string
	spacesAfter []int
}

var patternJoinBuilderPool = sync.Pool{
	New: func() interface{} { return &strings.Builder{} },
}

var tokenizerBufPool = sync.Pool{
	New: func() interface{} {
		return &tokenizerBuf{
			tokens:      make([]string, 0, 128),
			spacesAfter: make([]int, 0, 64),
		}
	},
}

func newPatternLineTokenizer() *patternLineTokenizer {
	var included [128]rune
	var excluded [128]rune
	included['='] = 1
	excluded['_'] = 1
	excluded['-'] = 1
	excluded['.'] = 1
	excluded[':'] = 1
	excluded['/'] = 1
	return &patternLineTokenizer{
		includeDelimiters: included,
		excludeDelimiters: excluded,
	}
}

func (p *patternLineTokenizer) Tokenize(line string) ([]string, []int, bool) {
	if len(line) == 0 || len(line) > patternMaxLineLength {
		return nil, nil, false
	}
	buf := tokenizerBufPool.Get().(*tokenizerBuf)
	tokens := buf.tokens[:0]
	spacesAfter := buf.spacesAfter[:0]

	start := 0
	for i, char := range line {
		if len(tokens) >= 127 {
			break
		}
		if unicode.IsLetter(char) || unicode.IsNumber(char) || (char < 128 && p.excludeDelimiters[char] != 0) {
			continue
		}
		included := char < 128 && p.includeDelimiters[char] != 0
		if char == ' ' || included || unicode.IsPunct(char) {
			if i > start {
				tokens = append(tokens, line[start:i])
			}
			if char == ' ' {
				spacesAfter = append(spacesAfter, len(tokens)-1)
			} else {
				tokens = append(tokens, line[i:i+1])
			}
			start = i + 1
		}
	}
	if start < len(line) {
		tokens = append(tokens, line[start:])
	}

	if len(tokens) < patternMinTokens || len(tokens) > patternMaxTokens {
		buf.tokens = tokens
		buf.spacesAfter = spacesAfter
		tokenizerBufPool.Put(buf)
		return nil, nil, false
	}
	// Copy results to fresh slices before returning the pool buffer.
	// The caller holds the returned slices across multiple Tokenize calls.
	outTokens := make([]string, len(tokens))
	copy(outTokens, tokens)
	outSpaces := make([]int, len(spacesAfter))
	copy(outSpaces, spacesAfter)
	buf.tokens = tokens
	buf.spacesAfter = spacesAfter
	tokenizerBufPool.Put(buf)
	return outTokens, outSpaces, true
}

func (p *patternLineTokenizer) Join(tokens []string, spacesAfter []int) string {
	if len(tokens) == 0 {
		return ""
	}
	sb := patternJoinBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	spacesIdx := 0
	for i, token := range tokens {
		out := token
		if isPatternPlaceholder(token) {
			out = patternVarPlaceholder
		}
		sb.WriteString(out)
		for spacesIdx < len(spacesAfter) && i == spacesAfter[spacesIdx] {
			sb.WriteByte(' ')
			spacesIdx++
		}
	}
	result := deduplicatePlaceholders(sb.String(), patternVarPlaceholder)
	patternJoinBuilderPool.Put(sb)
	return result
}

func patternStructureSignature(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	parts := make([]string, len(tokens))
	for i, token := range tokens {
		if i > 0 {
			prev := tokens[i-1]
			if prev == "=" || prev == ":" {
				if placeholder := patternPlaceholderKind(token); placeholder != "" {
					parts[i] = placeholder
				} else {
					parts[i] = patternVarPlaceholder
				}
				continue
			}
		}
		if placeholder := patternPlaceholderKind(token); placeholder != "" {
			parts[i] = placeholder
			continue
		}
		parts[i] = token
	}
	return strings.Join(parts, "\x1e")
}

func isPatternPlaceholder(token string) bool {
	switch token {
	case patternVarPlaceholder, patternNumPlaceholder, patternUUIDPlaceholder, patternIPPlaceholder, patternPathPlaceholder, patternHexPlaceholder, patternTSPlaceholder:
		return true
	default:
		return false
	}
}

func patternPlaceholderKind(token string) string {
	if isPatternPlaceholder(token) {
		return token
	}
	return patternPlaceholderForToken(token)
}

func patternPlaceholderMatchesToken(placeholder, token string) bool {
	if placeholder == "" {
		return false
	}
	if placeholder == patternVarPlaceholder {
		return true
	}
	return placeholder == patternPlaceholderForToken(token)
}

func samePatternPlaceholderKind(left, right string) bool {
	leftKind := patternPlaceholderKind(left)
	rightKind := patternPlaceholderKind(right)
	return leftKind != "" && leftKind == rightKind
}

func commonPatternPlaceholder(left, right string) string {
	leftKind := patternPlaceholderKind(left)
	rightKind := patternPlaceholderKind(right)
	if leftKind != "" && leftKind == rightKind {
		return leftKind
	}
	return patternVarPlaceholder
}

// patternQuotePunct is the set of punctuation trimmed from tokens before classification.
// Stored as a lookup table to avoid strings.Trim's ASCIISet allocation per call.
var patternQuotePunct = [256]bool{
	'"': true, '\'': true, '(': true, ')': true, '[': true, ']': true,
	'{': true, '}': true, '<': true, '>': true, ',': true, ';': true,
}

func trimPatternPunct(s string) string {
	start := 0
	for start < len(s) && patternQuotePunct[s[start]] {
		start++
	}
	end := len(s)
	for end > start && patternQuotePunct[s[end-1]] {
		end--
	}
	return strings.TrimSpace(s[start:end])
}

func patternPlaceholderForToken(token string) string {
	token = trimPatternPunct(token)
	if token == "" {
		return ""
	}
	switch {
	case isIPLike(token):
		return patternIPPlaceholder
	case isUUIDLike(token):
		return patternUUIDPlaceholder
	case isTimestampLike(token):
		return patternTSPlaceholder
	case isPathLike(token):
		return patternPathPlaceholder
	case isNumericLike(token):
		return patternNumPlaceholder
	case len(token) >= 8 && isHexLike(token):
		return patternHexPlaceholder
	default:
		return ""
	}
}

func isUUIDLike(token string) bool {
	if len(token) != 36 {
		return false
	}
	for i, ch := range token {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

func isTimestampLike(token string) bool {
	return strings.Contains(token, "T") && strings.Contains(token, ":")
}

func isPathLike(token string) bool {
	return strings.HasPrefix(token, "/") && strings.Count(token, "/") >= 2
}

func isNumericLike(token string) bool {
	if token == "" {
		return false
	}
	if token[0] >= '0' && token[0] <= '9' {
		return true
	}
	digitCount := 0
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			digitCount++
		}
	}
	return len(token) > 3 && digitCount > len(token)/2
}

func deduplicatePlaceholders(line, placeholder string) string {
	first := strings.Index(line, placeholder+placeholder)
	if first == -1 {
		return line
	}
	var b strings.Builder
	b.Grow(len(line))
	i := 0
	for i < len(line) {
		if strings.HasPrefix(line[i:], placeholder+placeholder) {
			b.WriteString(placeholder)
			i += len(placeholder)
			for i < len(line) && strings.HasPrefix(line[i:], placeholder) {
				i += len(placeholder)
			}
			continue
		}
		b.WriteByte(line[i])
		i++
	}
	return b.String()
}

func cloneTokens(tokens []string) []string {
	out := make([]string, len(tokens))
	copy(out, tokens)
	return out
}

func cloneInts(src []int) []int {
	out := make([]int, len(src))
	copy(out, src)
	return out
}

// tokenizeToPattern converts a log line into a pattern by replacing variable parts with <_>.
// This is a simplified version of the drain log pattern mining algorithm.
func tokenizeToPattern(line string) string {
	// Try JSON first
	if strings.HasPrefix(line, "{") {
		return tokenizeJSONPattern(line)
	}

	tokens := strings.Fields(line)
	result := make([]string, len(tokens))
	for i, token := range tokens {
		if isVariableToken(token) {
			result[i] = "<_>"
		} else {
			result[i] = token
		}
	}
	return strings.Join(result, " ")
}

// isVariableToken returns true if a token is likely a variable value (not a structural constant).
func isVariableToken(token string) bool {
	// Numbers (possibly with decimals, units)
	if len(token) > 0 && (token[0] >= '0' && token[0] <= '9') {
		return true
	}

	// UUIDs, hashes, IDs
	if len(token) >= 8 && isHexLike(token) {
		return true
	}

	// IP addresses
	if isIPLike(token) {
		return true
	}

	// Timestamps (ISO, RFC3339)
	if strings.Contains(token, "T") && strings.Contains(token, ":") {
		return true
	}

	// Paths with many segments
	if strings.HasPrefix(token, "/") && strings.Count(token, "/") > 2 {
		return true
	}

	// Contains mostly digits
	digitCount := 0
	for _, ch := range token {
		if unicode.IsDigit(ch) {
			digitCount++
		}
	}
	if len(token) > 3 && digitCount > len(token)/2 {
		return true
	}

	return false
}

// titleCase uppercases the first letter of each word (simple ASCII replacement for deprecated strings.Title).
func titleCase(s string) string {
	prev := ' '
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(rune(prev)) || prev == ' ' {
			prev = r
			return unicode.ToTitle(r)
		}
		prev = r
		return r
	}, s)
}

func isHexLike(s string) bool {
	s = strings.TrimPrefix(s, "0x")
	for _, ch := range s {
		isHex := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') || ch == '-'
		if !isHex {
			return false
		}
	}
	return true
}

func isIPLike(s string) bool {
	// IPv4: 7 (1.1.1.1) to 15 (255.255.255.255) bytes, 3 dots, all-digit octets.
	// Avoid strings.Split to eliminate heap allocation per call.
	if len(s) < 7 || len(s) > 15 {
		return false
	}
	dots := 0
	partLen := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= '0' && ch <= '9' {
			partLen++
			if partLen > 3 {
				return false
			}
		} else if ch == '.' {
			if partLen == 0 {
				return false
			}
			partLen = 0
			dots++
		} else {
			return false
		}
	}
	return dots == 3 && partLen > 0
}

func tokenizeJSONPattern(line string) string {
	var data map[string]interface{}
	if err := stdjson.Unmarshal([]byte(line), &data); err != nil {
		return "<_>"
	}
	// Create pattern from JSON keys (sorted)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"=<_>")
	}
	return "{" + strings.Join(parts, " ") + "}"
}
