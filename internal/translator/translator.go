// Package translator converts LogQL queries to LogsQL queries.
//
// Based on: https://docs.victoriametrics.com/victorialogs/logql-to-logsql/
package translator

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// emptyByGrouping is a sentinel value used as the byLabels argument to buildStatsQuery
// to indicate an explicit "by ()" clause (aggregate all into one series).
// This is distinct from an empty string, which means "no grouping clause at all".
const emptyByGrouping = "__lvp_by_empty__"

// Marker suffixes embedded in translated queries for proxy post-processing.
// The proxy strips them before forwarding to VL and applies the corresponding transform.
const (
	labelReplacePrefix = physical.LabelReplacePrefix
)

// LabelReplaceSpec holds the parsed arguments of a label_replace() expression.
type LabelReplaceSpec = physical.LabelReplace

// ParseLabelReplaceMarker extracts a label_replace spec embedded in a translated query.
// Returns the clean query (marker stripped) and the spec, or nil if absent.
func ParseLabelReplaceMarker(query string) (string, *LabelReplaceSpec) {
	idx := strings.LastIndex(query, labelReplacePrefix)
	if idx < 0 {
		return query, nil
	}
	payload := strings.TrimSuffix(query[idx+len(labelReplacePrefix):], "__")
	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return query, nil
	}
	parts := strings.SplitN(string(b), "\x00", 4)
	if len(parts) != 4 {
		return query, nil
	}
	return query[:idx], &LabelReplaceSpec{
		DstLabel:    parts[0],
		Replacement: parts[1],
		SrcLabel:    parts[2],
		Regex:       parts[3],
	}
}

// ParseAllLabelReplaceMarkers extracts every label_replace marker embedded in a
// translated query, returning the clean query (all markers stripped) and the specs
// in left-to-right order (inner application first, outer last). This supports
// chained label_replace(label_replace(...)) calls: the translator embeds one marker
// per nesting level, and each must be applied in sequence.
func ParseAllLabelReplaceMarkers(query string) (string, []LabelReplaceSpec) {
	return parseLabelReplaceMarkersFrom(query, 0)
}

// parseLabelReplaceMarkersFrom is ParseAllLabelReplaceMarkers reading markers from offset from on: the text before it
// is kept as it is.
func parseLabelReplaceMarkersFrom(query string, from int) (string, []LabelReplaceSpec) {
	var specs []LabelReplaceSpec
	// The query is the pieces kept so far (they hold no marker prefix) followed by rest, the part not read yet. A
	// marker is cut out of the query and the search starts again from the start of what is left, so one that the cut
	// makes out of the text on both sides of it is found too: it begins in the last bytes of the pieces. Reading each
	// byte once keeps this linear in the query, where cutting the string for every marker was quadratic.
	var pieceBuf [4]string
	pieces, kept := pieceBuf[:0], 0 // kept is the length of the pieces together
	rest, removed := query, false
	// headOnly: the pieces are query[:kept] and rest is query[kept:], so a piece found next extends the first one.
	headOnly := false
	if from > 0 {
		// The kept text ends with a quote, which no marker prefix holds, so no marker starts in it.
		pieces, kept, rest, headOnly = append(pieces, query[:from]), from, query[from:], true
	}
	for {
		// k is where the marker starts in the kept text, and used the bytes of its prefix that rest supplies.
		k, used := kept, len(labelReplacePrefix)
		var last [len(labelReplacePrefix) - 1]byte // the last bytes of the kept text
		n := min(kept, len(last))
		for i, got := len(pieces)-1, n; got > 0; i-- {
			piece := pieces[i]
			take := min(len(piece), got)
			copy(last[got-take:got], piece[len(piece)-take:])
			got -= take
		}
		for from := 0; from < n; from++ {
			if m := n - from; string(last[from:n]) == labelReplacePrefix[:m] && len(rest) >= len(labelReplacePrefix)-m && rest[:len(labelReplacePrefix)-m] == labelReplacePrefix[m:] {
				k, used = kept-m, len(labelReplacePrefix)-m
				break
			}
		}
		if k == kept {
			idx := strings.Index(rest, labelReplacePrefix)
			if idx < 0 {
				break
			}
			if idx > 0 && headOnly {
				pieces[0], kept = query[:kept+idx], kept+idx
			} else if idx > 0 {
				pieces, kept = append(pieces, rest[:idx]), kept+idx
			}
			rest = rest[idx:]
			k, used = kept, len(labelReplacePrefix)
		}
		tail := rest[used:]
		// The marker ends at the next "__" that closes it. Base64 chars are
		// [A-Za-z0-9+/=], so "__" cannot appear inside the payload.
		endIdx := strings.Index(tail, "__")
		if endIdx < 0 {
			break
		}
		payload := tail[:endIdx]
		b, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			break
		}
		parts := strings.SplitN(string(b), "\x00", 4)
		if len(parts) != 4 {
			break
		}
		specs = append(specs, LabelReplaceSpec{
			DstLabel:    parts[0],
			Replacement: parts[1],
			SrcLabel:    parts[2],
			Regex:       parts[3],
		})
		// Cut this marker out of the query and continue looking for more.
		for kept > k {
			piece := pieces[len(pieces)-1]
			if drop := kept - k; drop < len(piece) {
				pieces[len(pieces)-1], kept = piece[:len(piece)-drop], k
			} else {
				pieces, kept = pieces[:len(pieces)-1], kept-len(piece)
			}
		}
		rest, removed, headOnly = tail[endIdx+len("__"):], true, false
	}
	if !removed {
		return query, specs
	}
	// A query with its markers at the end, or all of it markers, is one of the pieces already.
	if len(pieces) == 0 {
		return rest, specs
	}
	if len(pieces) == 1 && rest == "" {
		return pieces[0], specs
	}
	var clean strings.Builder
	clean.Grow(kept + len(rest))
	for _, piece := range pieces {
		clean.WriteString(piece)
	}
	clean.WriteString(rest)
	return clean.String(), specs
}

// DropCondition represents a matcher-form conditional drop from `| drop field=value`.
// Only entries where the field value satisfies the condition have the field removed.
type DropCondition struct {
	Field string
	Op    string // "=", "!=", "=~", "!~"
	Value string
	re    *regexp.Regexp // non-nil for =~ and !~
	ip    *ipCondition   // non-nil for a label filter `field = ip("...")` (NewIPLabelFilterCondition)
}

// Matches reports whether val satisfies the drop condition.
func (dc DropCondition) Matches(val string) bool {
	if dc.ip != nil {
		return dc.ip.matches(val, dc.Op == "!=")
	}
	switch dc.Op {
	case "=":
		return val == dc.Value
	case "!=":
		return val != dc.Value
	case "=~":
		if dc.re == nil {
			return false
		}
		return dc.re.MatchString(val)
	case "!~":
		if dc.re == nil {
			return false
		}
		return !dc.re.MatchString(val)
	}
	return false
}

// NewDropCondition creates a DropCondition with the regex compiled for =~ and !~ operators.
func NewDropCondition(field, op, value string) (DropCondition, error) {
	dc := DropCondition{Field: field, Op: op, Value: value}
	if op == "=~" || op == "!~" {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return dc, fmt.Errorf("invalid regex %q in drop/keep condition: %w", value, err)
		}
		dc.re = re
	}
	return dc, nil
}

// dropKeepResult holds the parsed contents of all | drop and | keep pipeline stages.
type dropKeepResult struct {
	dropBare  []string
	dropConds []DropCondition
	keepBare  []string
	keepConds []DropCondition
}

// walkDropKeepStages is the single shared walker for all ParseDrop*/ParseKeep*
// functions. It scans the pipeline stages of logqlQuery and returns bare field names
// and matcher conditions found in | drop and | keep stages. Malformed matcher items
// cannot reach this walker through the query handlers or rules-migrate: both validate
// queries with the typed LogQL AST validator (logql.ValidateLogQL) first, which
// rejects them with a Loki-style parse error.
func walkDropKeepStages(logqlQuery string) dropKeepResult {
	var res dropKeepResult
	remaining := strings.TrimSpace(logqlQuery)
	if strings.HasPrefix(remaining, "{") {
		end := findMatchingBrace(remaining)
		if end < 0 {
			return res
		}
		remaining = strings.TrimSpace(remaining[end+1:])
	}
	for remaining != "" {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}
		// Line filter operators: |= |~ |> — consume value and continue.
		if strings.HasPrefix(remaining, "|= ") || strings.HasPrefix(remaining, "|=\"") ||
			strings.HasPrefix(remaining, "|~ ") || strings.HasPrefix(remaining, "|~\"") ||
			strings.HasPrefix(remaining, "|> ") || strings.HasPrefix(remaining, "|>\"") ||
			strings.HasPrefix(remaining, "|>`") {
			_, remaining = extractPipelineStage(remaining[2:])
			continue
		}
		// Negative line filter operators: != !~ !>
		if len(remaining) >= 2 && remaining[0] == '!' && (remaining[1] == '=' || remaining[1] == '~' || remaining[1] == '>') {
			_, remaining = extractPipelineStage(remaining[2:])
			continue
		}
		if !strings.HasPrefix(remaining, "|") {
			break
		}
		remaining = strings.TrimSpace(remaining[1:])
		stage, rest := extractPipelineStage(remaining)
		remaining = rest
		stage = strings.TrimSpace(stage)
		var isDrop bool
		var spec string
		if strings.HasPrefix(stage, "drop ") {
			isDrop = true
			spec = stage[5:]
		} else if strings.HasPrefix(stage, "keep ") {
			spec = stage[5:]
		} else {
			continue
		}
		// Validate and collect items.
		for _, item := range splitDropItems(spec) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			// Parse a matcher form (field OP value) for ANY supported operator —
			// =, !=, =~, !~. parseDropMatcher must be tried first (mirroring
			// splitDropSpec): the !~ operator is the only one with no '=', so
			// gating on strings.Contains(item, "=") would misread a `field!~"re"`
			// conditional drop/keep as a bare field name and silently lose it.
			field, op, val, ok := parseDropMatcher(item)
			if !ok || field == "" {
				// Not a parseable matcher. A plain identifier (no operator) is a
				// bare field; an item that still contains "=" is a malformed
				// matcher (e.g. status!=~"5..") and is skipped — the upstream
				// LogQL validator already rejects such queries with a parse error.
				if !ok && !strings.Contains(item, "=") {
					if isDrop {
						res.dropBare = append(res.dropBare, item)
					} else {
						res.keepBare = append(res.keepBare, item)
					}
				}
				continue
			}
			dc, err := NewDropCondition(field, op, val)
			if err != nil {
				continue
			}
			if isDrop {
				res.dropConds = append(res.dropConds, dc)
			} else {
				res.keepConds = append(res.keepConds, dc)
			}
		}
	}
	return res
}

// ParseDropConditions extracts matcher-form drop filters from a LogQL query.
// Bare-field drops (`| drop field`) are handled by VL `| delete`; only
// matcher forms (`| drop field=value`) are returned here for proxy post-processing.
func ParseDropConditions(logqlQuery string) []DropCondition {
	return walkDropKeepStages(logqlQuery).dropConds
}

// ParseKeepConditions extracts matcher-form conditions from `| keep field=value` stages.
// These are used by the proxy to post-process entries: fields that are present but do not
// satisfy their keep condition are removed from structured metadata and parsed fields.
// Bare-field keeps (`| keep field`) are handled by VL `| fields` projection; only
// matcher forms (`| keep field=value`) are returned here.
func ParseKeepConditions(logqlQuery string) []DropCondition {
	return walkDropKeepStages(logqlQuery).keepConds
}

// ParseBareDropFields returns the bare field names from `| drop field` stages.
// These are fields that are unconditionally deleted at the VL level via | delete,
// but the proxy must also strip them from stream labels since _stream is not touched by VL's | delete.
func ParseBareDropFields(logqlQuery string) []string {
	fields := walkDropKeepStages(logqlQuery).dropBare
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// ParseBareKeepFields returns the bare field names from `| keep field` stages.
// VL projects these via | fields _time, _msg, _stream, ..., but _stream carries all
// original stream labels; the proxy must filter the stream label set to only the kept fields.
func ParseBareKeepFields(logqlQuery string) []string {
	fields := walkDropKeepStages(logqlQuery).keepBare
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// splitDropSpec parses a drop spec into bare field names and matcher conditions.
// Example: `level="debug", trace_id` → bareFields: ["trace_id"], conditions: [{level = debug}]
func splitDropSpec(spec string) (bareFields []string, conditions []DropCondition) {
	for _, item := range splitDropItems(spec) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		field, op, val, ok := parseDropMatcher(item)
		if !ok || field == "" {
			if !ok && !strings.Contains(item, "=") {
				// Plain identifier (no operator chars) — treat as a bare field name.
				// Items that contain "=" but failed to parse are malformed matchers; skip them.
				bareFields = append(bareFields, item)
			}
			continue
		}
		dc := DropCondition{Field: field, Op: op, Value: val}
		if op == "=~" || op == "!~" {
			if re, err := regexp.Compile(val); err == nil {
				dc.re = re
			}
		}
		conditions = append(conditions, dc)
	}
	return
}

// splitDropItems splits a drop spec by commas, respecting quoted strings.
func splitDropItems(s string) []string {
	var items []string
	var cur strings.Builder
	inQuote := rune(0)
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			cur.WriteByte(s[i])
			i++
			cur.WriteByte(s[i])
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if c == ',' && inQuote == 0 {
			items = append(items, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(s[i])
	}
	if cur.Len() > 0 {
		items = append(items, cur.String())
	}
	return items
}

// parseDropMatcher parses "field=value" into components. Returns ok=false for bare field names
// and for expressions using the invalid !=~ operator (not a valid Loki matcher operator).
func parseDropMatcher(s string) (field, op, value string, ok bool) {
	// !=~ is not a valid Loki drop/keep operator. Scan for it first to prevent
	// the =~ or != sub-strings from matching within a !=~ expression.
	for _, o := range []string{"!=~", "=~", "!~", "!=", "="} {
		idx := strings.Index(s, o)
		if idx < 0 {
			continue
		}
		if o == "!=~" {
			return "", "", "", false
		}
		f := strings.TrimSpace(s[:idx])
		val := strings.TrimSpace(s[idx+len(o):])
		if strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) && len(val) >= 2 {
			if unquoted, err := strconv.Unquote(val); err == nil {
				val = unquoted
			} else {
				val = val[1 : len(val)-1]
			}
		} else if strings.HasPrefix(val, "`") && strings.HasSuffix(val, "`") && len(val) >= 2 {
			val = val[1 : len(val)-1]
		}
		return f, o, val, true
	}
	return "", "", "", false
}

// TranslateLogQL converts a LogQL query string to a LogsQL query string.
// It handles stream selectors, line filters, label filters, parsers, and metric queries.
func TranslateLogQL(logql string) (string, error) {
	return TranslateLogQLWithLabels(logql, nil)
}

// TranslateLogQLWithStreamFields converts a LogQL query to LogsQL, using VL native stream
// selectors for labels known to be _stream_fields (faster index path), and field filters
// for everything else. If streamFields is nil or empty, all matchers use field filters.
func TranslateLogQLWithStreamFields(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool) (string, error) {
	return translateLogQLFull(logql, labelFn, streamFields, logsql.Capabilities{})
}

// TranslateLogQLWithLabels converts a LogQL query to LogsQL, applying label name
// translation in stream selectors and label filters.
func TranslateLogQLWithLabels(logql string, labelFn LabelTranslateFunc) (string, error) {
	return translateLogQLFull(logql, labelFn, nil, logsql.Capabilities{})
}

// TranslateLogQLWithCapabilities converts a LogQL query to LogsQL using VL-specific
// capability flags to select the most efficient filter constructs.
func TranslateLogQLWithCapabilities(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool, caps logsql.Capabilities) (string, error) {
	return translateLogQLFull(logql, labelFn, streamFields, caps)
}

func translateLogQLFull(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool, caps logsql.Capabilities) (string, error) {
	querycorpus.Record(querycorpus.LogQLTranslate, logql)
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return "*", nil
	}

	// label_replace is a transform wrapper around a complete metric
	// expression. Handle them before the without/binary/metric path so the inner
	// expression is translated correctly and the marker is appended last.
	if result, ok := tryTranslateLabelReplace(logql, labelFn); ok {
		return applyDerivedGroupings(result, caps, streamFields), nil
	}

	// A vector aggregation over another one keeps each level's own grouping, so it
	// is translated level by level before the without clauses are stripped.
	if result, without, foldOp, ok := tryTranslateVectorChain(logql, labelFn); ok {
		return appendWithoutMarker(applyDerivedGroupings(result, caps, streamFields), foldOp, without), nil
	}

	// Extract without() labels before translation.
	// The proxy will post-process VL results to remove these labels from metric grouping.
	withoutOp := foldingWithoutOp(logql)
	var withoutLabels []string
	logql, withoutLabels = extractWithoutLabels(logql)

	// Strip "bool" modifier from comparison operators before translation.
	// Loki: "A > bool B" returns 1/0 instead of filtering. Our applyOp always
	// returns 1/0 for comparisons, so "bool" is a no-op — just strip it.
	logql = boolModifierRE.ReplaceAllString(logql, " ")

	// count_values groups by the VALUES of the inner metric — VL cannot compute this.
	if outerAgg, _, _ := extractOuterAggregation(logql); outerAgg == "count_values" {
		return "", &UnsupportedError{Msg: "count_values is not translatable to LogsQL", Func: "count_values"}
	}

	// Check binary metric expressions FIRST — they may contain metric sub-expressions.
	// E.g., "rate({...}[5m]) > 0" is a binary expr, not just a metric query.
	if binResult, ok := tryTranslateBinaryMetricExpr(logql, labelFn); ok {
		return appendWithoutMarker(applyDerivedGroupings(binResult, caps, streamFields), withoutOp, withoutLabels), nil
	}

	// Check if this is a plain metric query (no binary operator at top level)
	if metricResult, ok := tryTranslateMetricQueryWithout(logql, labelFn, len(withoutLabels) > 0, withoutOp != ""); ok {
		return appendWithoutMarker(applyDerivedGroupings(metricResult, caps, streamFields), withoutOp, withoutLabels), nil
	}
	if unwrapFunc := missingUnwrapRangeMetricFunc(logql); unwrapFunc != "" {
		return "", &UnsupportedError{Msg: unwrapFunc + " requires `| unwrap <field>` for range aggregation", Func: unwrapFunc}
	}

	// Guard: metric aggregation (sum/avg/topk/etc) applied directly to a log stream
	// without a range vector has no meaning and would fall through to bare-text wrapping,
	// producing malformed VL queries. Return a clear error instead.
	if outerAgg, _, _ := extractOuterAggregation(logql); outerAgg != "" && !strings.Contains(logql, "[") {
		return "", &UnsupportedError{Msg: outerAgg + "() requires a range metric inside (e.g. rate({...}[5m]), count_over_time({...}[5m]))", Func: outerAgg}
	}

	return translateLogQuery(logql, labelFn, caps, streamFields)
}

// WithoutMarkerSuffix is appended to translated queries that need without() post-processing.
// Format: "...actual_query...__without__:pod,node" folds the rows with the
// aggregation of the whole query (the proxy reads it from the LogQL).
// "...actual_query...__without__:<op>:pod,node" says the query is a
// `<op> without (...)` the proxy folds itself, and with <op> (sum, min, max, avg
// or count): see foldingWithoutOp.
const WithoutMarkerSuffix = physical.WithoutMarkerSuffix

func appendWithoutMarker(query, op string, labels []string) string {
	return physical.AppendWithoutMarker(query, op, labels)
}

// ParseWithoutMarker extracts the aggregation and the without labels from a
// translated query. Returns the clean query, the operator of the `without`
// aggregation the proxy folds itself (empty for any other query) and the list
// of labels to exclude.
func ParseWithoutMarker(query string) (cleanQuery, op string, excludeLabels []string) {
	return parseWithoutMarkerFrom(query, 0)
}

// parseWithoutMarkerFrom is ParseWithoutMarker reading a marker that starts at offset start or later.
func parseWithoutMarkerFrom(query string, start int) (cleanQuery, op string, excludeLabels []string) {
	// The last marker, found with Index (which scans many bytes at a time, where LastIndex does not): most queries
	// carry none and are read once.
	idx := -1
	for from := start; from < len(query); {
		i := strings.Index(query[from:], WithoutMarkerSuffix)
		if i < 0 {
			break
		}
		idx = from + i
		from = idx + 1
	}
	if idx < 0 {
		return query, "", nil
	}
	cleanQuery = query[:idx]
	labelStr := query[idx+len(WithoutMarkerSuffix):]
	// A label_replace marker appended after this one is
	// not part of the label list: leave it in the clean query.
	end := len(labelStr)
	for _, other := range []string{labelReplacePrefix} {
		if i := strings.Index(labelStr, other); i >= 0 && i < end {
			end = i
		}
	}
	cleanQuery += labelStr[end:]
	labelStr = labelStr[:end]
	if opEnd := strings.Index(labelStr, ":"); opEnd >= 0 {
		op, labelStr = labelStr[:opEnd], labelStr[opEnd+1:]
	}
	for _, l := range strings.Split(labelStr, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			excludeLabels = append(excludeLabels, l)
		}
	}
	return cleanQuery, op, excludeLabels
}

// The aggregations whose `without` form the proxy folds itself are sum, min,
// max, avg and count: it asks VictoriaLogs for a row per series and combines the
// rows that share the labels left with the operator, which
// TestSumWithoutDifferential pins to Loki's answer. topk, bottomk, stddev and
// stdvar are not folded: their values are not a combination of per-series rows.
var (
	// withoutHeadAggRE: `<op> without (labels) (`, the clause before the vector.
	withoutHeadAggRE = regexp.MustCompile(`^(sum|min|max|avg|count)\s+without\s*\([^)]*\)\s*\(`)
	// withoutTailAggRE: `<op> (`, whose vector is followed by `without (labels)`.
	withoutTailAggRE = regexp.MustCompile(`^(sum|min|max|avg|count)\s*\(`)
	withoutTailRE    = regexp.MustCompile(`^without\s*\([^)]*\)$`)
)

// foldingWithoutOp returns the operator of the `<op> without (...)` the proxy
// folds, or "" when the query is anything else. The fold is only for the
// aggregation that is the outermost one: the query may be wrapped by the
// ranking, sorting and label transforms (topk, bottomk, sort, sort_desc,
// stddev, stdvar, label_replace), which the proxy applies to the
// folded series. A `without` under another aggregation (`max(sum without ...)`),
// a binary expression, another operator (topk without, bottomk without, stddev without), or more
// than one `without` clause is answered as it was before the fold existed.
func foldingWithoutOp(logql string) string {
	if !strings.Contains(logql, "without") || len(withoutMarkerRE.FindAllStringIndex(logql, 2)) != 1 {
		return ""
	}
	s := strings.TrimSpace(logql)
	for {
		inner, ok := peelWithoutWrapper(s)
		if !ok {
			break
		}
		s = inner
	}
	if m := withoutHeadAggRE.FindStringSubmatch(s); m != nil {
		rest := s[len(m[0]):]
		if end := findLastMatchingParen(rest); end >= 0 && strings.TrimSpace(rest[end+1:]) == "" {
			return m[1]
		}
		return ""
	}
	if m := withoutTailAggRE.FindStringSubmatch(s); m != nil {
		rest := s[len(m[0]):]
		if end := findLastMatchingParen(rest); end >= 0 && withoutTailRE.MatchString(strings.TrimSpace(rest[end+1:])) {
			return m[1]
		}
	}
	return ""
}

// FoldingWithoutOp returns the operator of the `<op> without (...)` the proxy
// folds when logql is that expression, or "" (see foldingWithoutOp).
func FoldingWithoutOp(logql string) string { return foldingWithoutOp(logql) }

// PeelWithoutWrappers returns the expression under the ranking, sorting and
// label transforms the proxy applies to a folded answer (see foldingWithoutOp),
// or logql itself when none wraps it.
func PeelWithoutWrappers(logql string) string {
	s := strings.TrimSpace(logql)
	for {
		inner, ok := peelWithoutWrapper(s)
		if !ok {
			return s
		}
		s = inner
	}
}

// peelWithoutWrapper returns the expression a ranking, sorting or label
// transform wraps, when s is exactly that call.
func peelWithoutWrapper(s string) (string, bool) {
	for _, name := range []string{"sort_desc", "sort", "stddev", "stdvar", "topk", "bottomk", "label_replace"} {
		if !strings.HasPrefix(s, name+"(") {
			continue
		}
		rest := s[len(name)+1:]
		end := findLastMatchingParen(rest)
		if end < 0 || strings.TrimSpace(rest[end+1:]) != "" {
			return "", false
		}
		args := rest[:end]
		switch name {
		case "topk", "bottomk":
			_, expr, ok := splitFuncFirstArg(args)
			return expr, ok && expr != ""
		case "label_replace":
			expr, _, ok := splitFuncFirstArg(args)
			return expr, ok && expr != ""
		}
		return strings.TrimSpace(args), true
	}
	return "", false
}

// extractWithoutLabels extracts the excluded labels stored by rewriteWithoutToMarker.
// Called by the translator to attach metadata to the translated query.
func extractWithoutLabels(logql string) (cleaned string, labels []string) {
	m := withoutMarkerRE.FindStringSubmatch(logql)
	if m == nil {
		return logql, nil
	}
	labelStr := m[1]
	for _, l := range strings.Split(labelStr, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			labels = append(labels, l)
		}
	}
	cleaned = withoutMarkerRE.ReplaceAllString(logql, "")
	for strings.Contains(cleaned, "  ") {
		cleaned = strings.ReplaceAll(cleaned, "  ", " ")
	}
	return cleaned, labels
}

// StoredLineField is the field TranslateLogQueryKeepingLine copies the
// stored line into before the query's first line_format.
const StoredLineField = translate.StoredLineField

// TranslateLogQueryKeepingLine translates a log query (no metric wrapper)
// like TranslateLogQLWithCapabilities, and has VictoriaLogs copy the stored
// line into StoredLineField before the first line_format rewrites it, so the
// response carries both the formatted and the stored line.
func TranslateLogQueryKeepingLine(logql string, labelFn LabelTranslateFunc, streamFields map[string]bool, caps logsql.Capabilities) (string, error) {
	querycorpus.Record(querycorpus.LogQLTranslate, logql)
	return translateLogQueryOpts(strings.TrimSpace(logql), labelFn, caps, sf0(streamFields), true, false)
}

func sf0(m map[string]bool) []map[string]bool {
	if m == nil {
		return nil
	}
	return []map[string]bool{m}
}

// translateLogQuery handles log queries (non-metric).
func translateLogQuery(logql string, labelFn LabelTranslateFunc, caps logsql.Capabilities, streamFields ...map[string]bool) (string, error) {
	return translateLogQueryOpts(logql, labelFn, caps, streamFields, false, false)
}

// translateMetricInnerQuery translates the log query inside a range aggregation.
func translateMetricInnerQuery(logql string, labelFn LabelTranslateFunc) (string, error) {
	return translateLogQueryOpts(logql, labelFn, logsql.Capabilities{}, nil, false, true)
}

//nolint:gocyclo // staged LogQL→LogsQL pipeline parser: stream selector, line filters, parser stages, label filters, formatters; branching is inherent to LogQL grammar coverage.
func translateLogQueryOpts(logql string, labelFn LabelTranslateFunc, caps logsql.Capabilities, streamFields []map[string]bool, keepLine, metric bool) (string, error) {
	var sf map[string]bool
	if len(streamFields) > 0 {
		sf = streamFields[0]
	}

	var parts []string
	var streamParts []string // VL native stream selectors for known _stream_fields

	remaining := logql

	// 1. Extract stream selector {key="value", ...}
	// In VL, stream selectors `{...}` only match labels that were declared as
	// _stream_fields at ingestion time. Labels like "level" are typically NOT
	// stream fields. If we pass {level="error"} to VL, the stream filter
	// returns zero results.
	//
	// Optimization: if streamFields is configured, use VL native stream selectors
	// for known _stream_fields (faster index path). Use field filters for the rest.
	if strings.HasPrefix(remaining, "{") {
		end := findMatchingBrace(remaining)
		if end < 0 {
			return "", &ParseError{Msg: "unmatched '{' in stream selector", Pos: -1}
		}
		streamContent := remaining[1:end]
		remaining = strings.TrimSpace(remaining[end+1:])

		matchers := splitStreamMatchers(streamContent)
		for _, m := range matchers {
			if !sf[detectedLevelLabel] {
				filter, ok, err := detectedLevelSelectorFilter(m)
				if err != nil {
					return "", err
				}
				if ok {
					if filter != "" {
						parts = append(parts, filter)
					}
					continue
				}
			}
			if sf != nil && canUseStreamSelector(m, sf, labelFn) {
				streamParts = append(streamParts, m)
			} else if ff := streamMatcherToFieldFilter(m, labelFn); ff != "" {
				parts = append(parts, ff)
			}
		}
	}

	// Track whether we've seen a parser pipe (json, logfmt, pattern, regexp).
	// After a parser, label filters must become VL `| filter` pipes.
	afterParser := false
	// Loki adds a non-empty detected_level at ingest. Bare drop/keep can
	// change whether later presence filters see it.
	detectedPresence := detectedLevelPresent
	// levelInputsIntact holds while every stage so far left the line and the
	// stored fields as VictoriaLogs stored them (label filters, a drop of the
	// parser error labels): a detected_level filter then reads Loki's
	// ingest-time value from them (detectedLevelStageFilter).
	levelInputsIntact := true
	// pendingLevel holds such filters until the next other pipe stage: the
	// line filters that follow them stay filters of the query, which
	// VictoriaLogs evaluates before every pipe (they commute with a level
	// filter, which reads no label a line filter sets), and consecutive level
	// filters share one chain.
	var pendingLevel []DetectedLevelFilterPlan
	levelMode := detectedLevelFilterMode(metric, caps)
	flushLevel := func() {
		if pipes := JoinDetectedLevelFilters(pendingLevel, detectedLevelKnownFields(nil), levelMode); pipes != "" {
			parts = append(parts, pipes)
		}
		pendingLevel = nil
	}
	// afterKeyParser is set once a json or logfmt parser ran: label filters after
	// it name sanitized keys (see parsedKeyVariants).
	afterKeyParser := false
	// keyParsers are the json and logfmt stages so far, with the part their
	// pipe holds; extractedBases are the name_extracted labels the filters name
	// (see extractedScratchPipes).
	trackExtracted := strings.Contains(logql, extractedSuffix)
	var keyParsers []extractedParser
	var keyParserParts []int
	var extractedBases, stageBases []string
	// jsonAliases tracks | json alias="field" mappings so subsequent filter stages
	// referencing the alias name can be rewritten to use the original JSON field name.
	// VL's unpack_json always uses original field names; aliases are not preserved.
	jsonAliases := make(map[string]string)
	captureLabels := make(map[string]bool)
	// svNames are the stream label names read as stream values after a parser
	// (see WithStreamLabels); formatLabels are the names label_format sets.
	svNames := plainStreamLabels(labelFn)
	if !metric && svNames != nil {
		svNames = filteredNames(remaining, svNames)
	}
	svCopied := false
	formatLabels := make(map[string]bool)
	// definedLabels are the names pattern and label_format stages create; they
	// are query-local and never a sanitized parser key.
	definedLabels := make(map[string]bool)
	// patternFilters numbers the pattern line filter alternatives that extract.
	patternFilters := 0
	pipelineLabelFn := func(label string) string {
		if captureLabels[label] || labelFn == nil {
			return label
		}
		return labelFn(label)
	}

	// 2. Process pipeline stages: | operator ...
	// LogQL line filters: |= "text", != "text", |~ "regexp", !~ "regexp"
	// LogQL pipe stages: | json, | logfmt, | label == "value", etc.
	for remaining != "" {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}

		// Check for line filter operators first (these are NOT pipe stages)
		// CRITICAL: Loki |= is SUBSTRING match, not word match.
		// VL's "text" is word-only; VL's ~"text" is substring/regexp.
		// We must use ~"text" to match Loki's substring semantics.
		// Loki matches the raw line; VictoriaLogs stores that line in _msg.

		// ip() line filter: Loki searches raw log text for IPs matching the pattern.
		// VL has no native ip() support; translate to a regex approximation.
		// Invalid IP patterns (e.g. octets > 255) are rejected with a parse error
		// matching Loki's behavior.
		if strings.HasPrefix(remaining, "|= ip(") || strings.HasPrefix(remaining, "|=ip(") {
			remaining = strings.TrimSpace(remaining[2:])
			arg, rest, ok := extractIPFilterArg(remaining)
			if alts, orRest, err := takeOrAlternatives(rest); err != nil {
				return "", err
			} else if len(alts) > 0 {
				// Loki builds an `or` chain from the text of each alternative, so the
				// ip() head matches as plain text (LineFilterExpr.newOrFilter).
				parts = append(parts, translate.OrLineFilter(false, false, append([]translate.OrAlternative{{Text: arg}}, alts...)))
				remaining = orRest
				continue
			}
			remaining = rest
			if ok {
				// The request validator rejects invalid IP patterns before translation.
				parts = append(parts, translate.IPLineFilter(arg, false).String())
			}
			continue
		}
		if strings.HasPrefix(remaining, "!= ip(") || strings.HasPrefix(remaining, "!=ip(") {
			remaining = strings.TrimSpace(remaining[2:])
			arg, rest, ok := extractIPFilterArg(remaining)
			alts, orRest, err := takeOrAlternatives(rest)
			if err != nil {
				return "", err
			}
			remaining = orRest
			switch {
			case ok && len(alts) == 0:
				parts = append(parts, translate.IPLineFilter(arg, true).String())
			case ok:
				parts = append(parts, translate.OrLineFilter(true, false, append([]translate.OrAlternative{{Text: arg, IP: true}}, alts...)))
			}
			continue
		}

		if strings.HasPrefix(remaining, "|=") {
			// Substring match: |= "text" → ~"text"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			literal, err := strconv.Unquote(val)
			if err != nil {
				return "", fmt.Errorf("invalid line filter: %w", err)
			}
			alts, rest, err := takeOrAlternatives(rest)
			if err != nil {
				return "", err
			}
			if !utf8.ValidString(literal) || !translate.TextsHeld(alts) {
				// A text that is not UTF-8 is in no line: it never matches (translate.TextLineFilter).
				if f := translate.TextLineFilter(false, append([]translate.OrAlternative{{Text: literal}}, alts...)); f != nil {
					parts = append(parts, f.String())
				}
			} else if len(alts) > 0 {
				parts = append(parts, translate.OrLineFilter(false, false, append([]translate.OrAlternative{{Text: literal}}, alts...)))
			} else {
				parts = append(parts, "~"+strconv.Quote(regexp.QuoteMeta(literal)))
			}
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "!=") {
			// Negative substring: != "text" → NOT ~"text"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			literal, err := strconv.Unquote(val)
			if err != nil {
				return "", fmt.Errorf("invalid line filter: %w", err)
			}
			alts, rest, err := takeOrAlternatives(rest)
			if err != nil {
				return "", err
			}
			if !utf8.ValidString(literal) || !translate.TextsHeld(alts) {
				// A text that is not UTF-8 is in no line: it never matches (translate.TextLineFilter).
				if f := translate.TextLineFilter(true, append([]translate.OrAlternative{{Text: literal}}, alts...)); f != nil {
					parts = append(parts, f.String())
				}
			} else if len(alts) > 0 {
				parts = append(parts, translate.OrLineFilter(true, false, append([]translate.OrAlternative{{Text: literal}}, alts...)))
			} else {
				parts = append(parts, "NOT ~"+strconv.Quote(regexp.QuoteMeta(literal)))
			}
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "|~ ") || strings.HasPrefix(remaining, "|~\"") {
			// Regexp match: |~ "regexp" → ~"regexp"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			alts, rest, err := takeOrAlternatives(rest)
			if err != nil {
				return "", err
			}
			if len(alts) > 0 {
				head, _ := strconv.Unquote(val)
				parts = append(parts, translate.OrLineFilter(false, true, append([]translate.OrAlternative{{Text: head}}, alts...)))
			} else {
				parts = append(parts, "~"+val)
			}
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "!~ ") || strings.HasPrefix(remaining, "!~\"") {
			// Negative regexp: !~ "regexp" → NOT ~"regexp"
			remaining = strings.TrimSpace(remaining[2:])
			val, rest := extractQuotedValue(remaining)
			alts, rest, err := takeOrAlternatives(rest)
			if err != nil {
				return "", err
			}
			if len(alts) > 0 {
				head, _ := strconv.Unquote(val)
				parts = append(parts, translate.OrLineFilter(true, true, append([]translate.OrAlternative{{Text: head}}, alts...)))
			} else {
				parts = append(parts, "NOT ~"+val)
			}
			remaining = rest
			continue
		}
		if strings.HasPrefix(remaining, "|> ") || strings.HasPrefix(remaining, "|>\"") || strings.HasPrefix(remaining, "|>`") ||
			strings.HasPrefix(remaining, "!> ") || strings.HasPrefix(remaining, "!>\"") || strings.HasPrefix(remaining, "!>`") {
			// Pattern filter: |> and !> keep or drop the lines Loki's pattern matcher
			// accepts (see translate.PatternLineFilterParts).
			// The operand and its `or` alternatives end where the next operator starts,
			// so a line filter that follows (!=, !~, |=) is translated too.
			negative := remaining[0] == '!'
			values, rest, err := takePatternValues(strings.TrimSpace(remaining[2:]))
			if err != nil {
				return "", err
			}
			parts = append(parts, translate.PatternLineFilterParts(values, negative, &patternFilters)...)
			remaining = rest
			continue
		}

		if !strings.HasPrefix(remaining, "|") {
			// Bare text after stream selector — treat as a phrase filter.
			//
			// Guard against a class of malformed inputs where a label matcher
			// arrives without surrounding `{...}` (e.g. `app="json-test"`
			// instead of `{app="json-test"}`). Loki rejects such queries with
			// a parse error and the bare-text fallback would otherwise emit
			// LogsQL like `"app="json-test""`, which VictoriaLogs cannot
			// parse. Surface this as a translation error so the proxy returns
			// 400, matching Loki's behavior.
			if looksLikeBareLabelMatcher(remaining) {
				return "", &ParseError{Msg: fmt.Sprintf("parse error : syntax error: stream selector must be wrapped in braces, got %q", remaining), Pos: -1}
			}
			parts = append(parts, translateBareFilter(remaining))
			break
		}

		// Skip the pipe character for pipe stages
		remaining = strings.TrimSpace(remaining[1:])

		// Determine the pipeline stage type
		stage, rest := extractPipelineStage(remaining)
		remaining = rest
		if strings.HasPrefix(stage, "(") {
			// A parenthesized single filter is that filter.
			if expr, err := logqlpkg.ParseLabelFilterExpr(stage); err == nil && expr.IsLeaf() {
				stage = expr.Leaf
			}
		}
		var levelPlan DetectedLevelFilterPlan
		levelStage := false
		if levelInputsIntact {
			levelPlan, levelStage = detectedLevelStageFilter(stage, metric, levelMode)
		}
		if !levelStage {
			flushLevel()
		}
		for _, label := range regexpCaptureLabels(stage) {
			if label != "" {
				captureLabels[label] = true
			}
		}
		for _, label := range stageDefinedLabels(stage) {
			definedLabels[label] = true
			if strings.HasPrefix(stage, "label_format ") {
				formatLabels[label] = true
			}
		}

		// Populate json alias map when the stage uses alias="field" syntax.
		if strings.HasPrefix(stage, "json ") {
			for alias, orig := range parseJSONFieldAliases(stage) {
				jsonAliases[alias] = orig
			}
		}
		// Rewrite filter stages that reference json-aliased field names so they
		// use the original JSON field name that VL's unpack_json actually extracts.
		if afterParser && len(jsonAliases) > 0 {
			stage = rewriteJSONAliasedFilter(stage, jsonAliases)
		}

		if svNames != nil && !svCopied && isPlainParserStage(stage) {
			svCopied = true
			parts = append(parts, svCopyPipes(svNames))
		}
		if isKeyParserStage(stage) {
			afterKeyParser = true
			var parser extractedParser
			if trackExtracted {
				parser = extractedParser{unpack: "unpack_json", targets: extractionTargets(stage)}
				if strings.HasPrefix(stage, "logfmt") {
					parser.unpack = "unpack_logfmt"
				}
			}
			if trackExtracted {
				keyParsers = append(keyParsers, parser)
				keyParserParts = append(keyParserParts, len(parts))
			}
		}
		stageLabelFn := pipelineLabelFn
		if afterKeyParser && isLabelFilterStage(stage) {
			stageLabelFn = withParsedKeyVariants(pipelineLabelFn, func(label string) bool {
				if captureLabels[label] || definedLabels[label] {
					return true
				}
				for alias, orig := range jsonAliases {
					if label == alias || label == orig {
						return true
					}
				}
				return false
			}, func(base string) bool {
				// The label may be a collision rename unless the query sets it
				// itself; the parsers so far say whether one extracts base.
				if captureLabels[base+extractedSuffix] || definedLabels[base+extractedSuffix] {
					return false
				}
				for _, parser := range keyParsers {
					if _, ok := parser.source(base); ok {
						if !containsStr(extractedBases, base) {
							extractedBases = append(extractedBases, base)
						}
						if !containsStr(stageBases, base) {
							stageBases = append(stageBases, base)
						}
						return true
					}
				}
				return false
			})
		}
		if svCopied && !metric && isLabelFilterStage(stage) {
			plain := stageLabelFn
			stageLabelFn = func(label string) string {
				if _, aliased := jsonAliases[label]; containsStr(svNames, label) && !formatLabels[label] && !aliased {
					return svScratch + label
				}
				return plain(label)
			}
		}
		if levelStage {
			bare := levelPlan.Prefilter
			if levelPlan.Never {
				bare = matchNoRows
			}
			switch {
			case bare == "":
			case len(parts) > 0 && strings.HasPrefix(strings.TrimSpace(parts[len(parts)-1]), "|"):
				parts = append(parts, "| filter "+bare) // a bare filter after a pipe is a pipe of its own
			default:
				parts = append(parts, bare)
			}
			if !levelPlan.Never {
				pendingLevel = append(pendingLevel, levelPlan)
			}
			continue
		}
		levelInputsIntact = levelInputsIntact && (isLabelFilterStage(stage) || dropsOnlyErrorLabels(stage))
		translated := translatePipelineStage(stage, stageLabelFn, caps)
		if !metric {
			if nonempty, ok := detectedLevelEmptyPresenceFilter(stage); ok && detectedPresence != detectedLevelUnknown {
				if (detectedPresence == detectedLevelPresent) == nonempty {
					translated = ""
				} else {
					translated = "| filter (detected_level:* -detected_level:*)"
				}
			}
		}
		if !metric && strings.HasPrefix(stage, "drop ") {
			// Log responses derive detected_level from the backend row. Keep
			// its stored sources until response projection applies the drop.
			translated = translateLogDropStage(stage[5:], stageLabelFn)
		}
		if metric && strings.HasPrefix(stage, "keep ") && keepStageDropsLevelSource(stage[5:]) {
			// The derived-level chain runs before pipeline stages. Keep the stored
			// level while it computes detected_level, then remove it just as Loki's
			// keep stage does before metric grouping reads the row.
			translated += " | delete level"
		}
		if svCopied && !metric && (strings.HasPrefix(stage, "label_format ") || strings.HasPrefix(stage, "line_format ")) {
			translated = svRewriteRefs(translated, svNamesNotFormatted(svNames, formatLabels))
		}
		if len(stageBases) > 0 {
			if !strings.HasPrefix(translated, "|") {
				translated = extractedStageMark + strings.Join(stageBases, ",") + extractedStageMark + translated
			}
			stageBases = nil
		}
		if keepLine && strings.HasPrefix(stage, "line_format ") && strings.HasPrefix(translated, "| format ") {
			translated = "| copy _msg as " + StoredLineField + " " + translated
			keepLine = false
		}
		aliasCopies := ""
		if translated == "| unpack_json" && strings.HasPrefix(stage, "json ") {
			// `| json a="x.y"` defines the label a from the key x.y: make it a
			// field, so grouping and unwrap on a read it as Loki does.
			aliasCopies = strings.TrimPrefix(jsonAliasCopies(stage), " ")
		}
		if strings.HasPrefix(translated, errUnknownParser) {
			parserName := strings.TrimPrefix(translated, errUnknownParser)
			return "", fmt.Errorf("unknown pipeline stage %q — not a valid LogQL parser or label filter", parserName)
		}
		if translated != "" {
			// Track parser state — after a parser, label filters become | filter
			if isParserStage(translated) {
				afterParser = true
			}
			// If this is a bare field filter after a parser, wrap it as | filter
			if afterParser && !strings.HasPrefix(translated, "|") && isFieldFilter(translated) {
				translated = "| filter " + translated
			}
			// detected_level in the pipeline before any parser must use logfmt unpacking.
			// Without a preceding parser, the translated filter checks _stream.level
			// (the push-time label), not the content-derived level in _msg.
			// Inject | unpack_logfmt so we filter on the logfmt-parsed level field.
			// The empty-value case (-level:*) is not a field filter, so it's excluded.
			if !afterParser && !strings.HasPrefix(translated, "|") && isFieldFilter(translated) && strings.Contains(stage, "detected_level") {
				translated = "| unpack_logfmt | filter " + translated
				afterParser = true
			}
			// Bare label filter NOT after a parser AND NOT detected_level: usually
			// composes as implicit-AND alongside the stream-selector filter (e.g.
			// `app:="api" status:="200"`). BUT if the immediately preceding stage
			// is a PIPE STAGE (`| format`, `| line_format`, `| label_format`,
			// `| drop`, `| keep`, `| decolorize`, `| extract`, etc.) then implicit
			// AND is invalid — VL parses `| format "..." status:="200"` as a
			// continuation of the format rename target and errors with
			// `unexpected token after [format ...]: "status"; expecting '|' or ')'`.
			// Wrap the bare filter with `| filter ` in that case so it composes
			// as its own pipe stage. We detect "preceding stage is a pipe stage"
			// by looking at the last accepted `parts` entry: if it starts with
			// `|`, prepend `| filter` to the current bare filter.
			if !afterParser && !strings.HasPrefix(translated, "|") && isFieldFilter(translated) {
				if len(parts) > 0 && strings.HasPrefix(strings.TrimSpace(parts[len(parts)-1]), "|") {
					translated = "| filter " + translated
				}
			}
			parts = append(parts, translated)
			if aliasCopies != "" {
				parts = append(parts, aliasCopies)
			}
			if svCopied && isPlainParserStage(stage) {
				parts = append(parts, svFillPipes(svNames, metric))
			}
		}
		if !metric {
			detectedPresence = nextDetectedLevelPresence(detectedPresence, stage)
		}
	}
	flushLevel()

	if svCopied {
		parts = append(parts, "| delete "+svScratch+"*")
	}
	if len(extractedBases) > 0 {
		parts = withExtractedScratch(parts, keyParsers, keyParserParts, extractedBases)
	}
	if metric && svNames != nil {
		parts = withStreamTagRestore(parts, svNames)
	}
	wrapBareFiltersAfterPipes(parts)
	result := strings.Join(parts, " ")

	// Prepend VL native stream selector for known _stream_fields
	if len(streamParts) > 0 {
		streamSelector := "{" + strings.Join(streamParts, ", ") + "}"
		if result == "" {
			result = streamSelector
		} else {
			result = streamSelector + " " + result
		}
	}

	if result == "" {
		return "*", nil
	}
	return result, nil
}

// wrapBareFiltersAfterPipes gives every bare filter part (a line filter such as
// ~"y", or -level:* for level="") that directly follows a non-filter pipe stage
// (| unpack_json, | format, | fields, | delete, | decolorize, | extract, ...)
// its own | filter stage. Left bare, every VictoriaLogs version rejects it with
// "unexpected token after [<pipe>]" or reads it as the pipe's argument. | filter
// is accepted by every supported VictoriaLogs version. A bare filter following a
// | filter stage (including the "| unpack_logfmt | filter" part emitted for
// detected_level) stays an implicit AND inside it.
func wrapBareFiltersAfterPipes(parts []string) {
	for i := 1; i < len(parts); i++ {
		cur := strings.TrimSpace(parts[i])
		if cur == "" || strings.HasPrefix(cur, "|") {
			continue
		}
		prev := strings.TrimSpace(parts[i-1])
		if strings.HasPrefix(prev, "|") && !strings.HasPrefix(prev, "| filter ") && !strings.HasPrefix(prev, "| unpack_logfmt | filter ") {
			parts[i] = "| filter " + cur
		}
	}
}

// knownParsers is the set of bare-word LogQL parser names.
// If a bare identifier stage doesn't match any of these it is an unknown parser
// and should surface as a 400 rather than silently passing through.
var knownParsers = map[string]bool{
	"json": true, "logfmt": true, "unpack": true, "labels": true,
	"pattern": true, "regexp": true, "grok": true, "clf": true,
	"nginx": true, "apache": true, "csv": true, "decolorize": true,
}

// errUnknownParser is the sentinel prefix used to propagate unknown-parser
// errors from translatePipelineStage through the string-returning call chain.
const errUnknownParser = "__ERR_UNKNOWN_PARSER__:"

// translatePipelineStage converts a single LogQL pipeline stage to LogsQL.
func translatePipelineStage(stage string, labelFn LabelTranslateFunc, caps logsql.Capabilities) string {
	stage = strings.TrimSpace(stage)

	// Note: Line filters (|=, !=, |~, !~) are handled in translateLogQuery
	// before this function is called. Only pipe stages reach here.

	// Unwrap — VL doesn't need unwrap, stats functions take field names directly.
	// | unwrap field_name → silently dropped (the field name is used in the stats function)
	// | unwrap (bare, no field) → also dropped; Grafana query builder emits this
	// while the user is still typing a field name.
	if stage == "unwrap" || strings.HasPrefix(stage, "unwrap ") {
		return "" // drop — VL handles this implicitly
	}

	// Parsers — handle both bare (| json) and field-specific (| json field1, field2)
	// VL's unpack_json/unpack_logfmt always extracts all fields,
	// so field-specific variants map to the same VL command.
	if stage == "json" || stage == "unpack" || strings.HasPrefix(stage, "json ") || strings.HasPrefix(stage, "unpack ") {
		return "| unpack_json"
	}
	if stage == "logfmt" || strings.HasPrefix(stage, "logfmt ") {
		return "| unpack_logfmt"
	}
	if strings.HasPrefix(stage, "pattern ") {
		// | pattern "..." → | extract "..."
		patternExpr := normalizeQuotedStageExpr(stage[8:])
		if isNoopPatternExpression(patternExpr) {
			// Defensive compatibility: some Drilldown flows emit wildcard-only
			// patterns like `(.*)`. VL extract requires named placeholders and
			// rejects these, so treat them as a no-op stage.
			return ""
		}
		return "| extract " + patternExpr
	}
	if strings.HasPrefix(stage, "regexp ") {
		// | regexp "..." → | extract_regexp "..."
		return "| extract_regexp " + normalizeQuotedStageExpr(stage[7:])
	}
	if strings.HasPrefix(stage, "extract ") {
		// Defensive pass-through for pre-translated queries.
		patternExpr := normalizeQuotedStageExpr(stage[8:])
		if isNoopPatternExpression(patternExpr) {
			return ""
		}
		return "| extract " + patternExpr
	}

	// Line formatting
	if strings.HasPrefix(stage, "line_format ") {
		tmpl := stage[12:]
		return "| format " + convertGoTemplate(tmpl)
	}
	// Label formatting
	if strings.HasPrefix(stage, "label_format ") {
		return translateLabelFormat(stage[13:])
	}

	// Drop / keep labels
	if strings.HasPrefix(stage, "drop ") {
		return translateDropStage(stage[5:], labelFn)
	}
	if strings.HasPrefix(stage, "keep ") {
		return translateKeepStage(stage[5:], labelFn)
	}

	// decolorize — strips ANSI color codes.
	// VL has native | decolorize support; emit the typed node for correctness.
	// The string output is identical ("| decolorize"), but using the typed node
	// ensures the representation stays in sync with the logsql AST definition.
	if stage == "decolorize" {
		return logsql.PipeDecolorize{}.String() // "| decolorize"
	}

	// ip() CIDR pipeline filter — emit VL-native ipv4_range(), or the regexp approximation for IPv6.
	if strings.HasPrefix(stage, `ip("`) && strings.HasSuffix(stage, `")`) {
		cidr := stage[4 : len(stage)-2]
		filter := logsql.NewBuilder(caps).BestIPv4Range("_msg", cidr)
		return logsql.PipeFilter{Expr: filter}.String()
	}
	if strings.HasPrefix(stage, "ip(") {
		return "| " + stage // unknown ip() form, keep as proxy marker
	}

	// Detect unknown bare-word parsers (e.g. `| badparser`).
	// A stage that is a bare identifier with no operator characters is very likely
	// an attempt to invoke a named parser. If it doesn't match any known parser,
	// return an error sentinel so the proxy can surface a 400 rather than silently
	// passing through to VL and returning 200 with wrong results.
	if isBareIdentifier(stage) && !knownParsers[stage] {
		return errUnknownParser + stage
	}

	// Label filters: label op value
	return translateLabelFilter(stage, labelFn, caps)
}

func isBareIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func isNoopPatternExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	if strings.HasPrefix(expr, "`") && strings.HasSuffix(expr, "`") && len(expr) >= 2 {
		expr = expr[1 : len(expr)-1]
	} else if strings.HasPrefix(expr, `"`) && strings.HasSuffix(expr, `"`) && len(expr) >= 2 {
		if unquoted, err := strconv.Unquote(expr); err == nil {
			expr = unquoted
		} else {
			expr = expr[1 : len(expr)-1]
		}
	}
	return translate.NoopPattern(expr)
}

// translateLabelFilter handles label comparison filters.
// translateLabelFilter converts a single LogQL pipeline-stage label filter to
// a LogsQL filter expression. Returns the BARE filter expression (e.g.
// `level:="error"` or `(a AND b)`) without a leading `| ` because the
// caller (translatePipelineStage) decides between two wrappings:
//   - After a parser (json / logfmt / pattern): wrap as `| filter <expr>`
//     for VL idiomatic clarity (the parser introduces fields and `| filter`
//     makes the "filter on extracted fields" intent explicit).
//   - As a standalone pipeline stage with no parser before it: wrap as
//     just `| <expr>` (plain filter pipe).
//
// The standalone-stage path is what was missing before: when a stage like
// `| line_format "..."` is followed by `| level="error"`, translateLabelFilter
// returned `level:="error"` and the assembly layer concatenated it directly
// to the format stage producing `| format "..." level:="error"` — invalid
// LogsQL (VL: `unexpected token after [format ...]: "level"; expecting '|' or ')'`).
// The fix lives in translatePipelineStage's assembly logic (the `wrap with
// "| "` branch added alongside the `wrap with "| filter "` branch), not here.
func translateLabelFilter(stage string, labelFn LabelTranslateFunc, caps logsql.Capabilities) string {
	if mayBeCompound(stage) {
		if expr, err := logqlpkg.ParseLabelFilterExpr(stage); err == nil {
			if !expr.IsLeaf() {
				if translated, ok := translateLabelFilterExpr(expr, labelFn, caps); ok {
					return translated
				}
			} else {
				stage = expr.Leaf
			}
		}
	}

	if translated, ok := translateSingleLabelFilter(stage, labelFn, caps); ok {
		return translated
	}

	if translated, ok := translateMalformedDottedStage(stage, labelFn); ok {
		return translated
	}

	// Unknown stage — pass through as-is with pipe (already a complete stage).
	return "| " + stage
}

// logqlSingleFilterOp is what a LogQL label filter operator means.
type logqlSingleFilterOp = translate.LabelFilterOp

func translateSingleLabelFilter(stage string, labelFn LabelTranslateFunc, caps logsql.Capabilities) (string, bool) {
	// Try: label == "value", label = "value", label != "value",
	//      label =~ "value", label !~ "value", label > value, etc.
	for _, entry := range translate.LabelFilterOps {
		idx := matcherOperatorIndex(stage, entry.LogQL)
		if idx > 0 {
			label := translate.SanitizeFieldIdentifier(stage[:idx])
			value := strings.TrimSpace(stage[idx+len(entry.LogQL):])
			if label == "" {
				return "", false
			}
			if label == "service_name" && !entry.Op.IsComp && !strings.HasPrefix(value, `ip("`) {
				// A label filter reads the stream's service_name, which the
				// proxy derives exactly as the selector does.
				return serviceNameLabelFilter(value, entry.Op.Negate, entry.Op.IsRe), true
			}
			if label == "detected_level" {
				label = "level"
			}
			if labelFn != nil {
				translated := labelFn(label)
				if strings.Contains(translated, parsedKeySep) && !strings.Contains(label, parsedKeySep) {
					return translateParsedKeyFilter(stage, strings.Split(translated, parsedKeySep), value, entry.Op, caps)
				}
				label = translate.SanitizeFieldIdentifier(translated)
				if label == "" {
					return "", false
				}
			}

			// VL requires quoting for dotted or hyphenated field names (e.g.
			// "service.name"): quote the label before passing it to FieldFilter
			// (or the ip() range filter) so the output is "service.name":="foo"
			// rather than service.name:="foo".
			if strings.ContainsAny(label, ".-") {
				label = `"` + label + `"`
			}

			// ip() label filter: label = ip("pattern") or label != ip("pattern")
			// (ipLabelFilter). Detect calls before decoding a quoted exact value
			// which may itself contain the literal text ip("...").
			isIP := strings.HasPrefix(value, `ip("`) && strings.HasSuffix(value, `")`)
			if isIP {
				value = value[4 : len(value)-2]
			} else {
				value = streamMatcherValue(value, entry.Op.IsRe || entry.Op.IsComp)
			}
			f, ok := translate.LabelFilterLeaf(label, entry.LogQL, value, isIP, caps)
			if !ok {
				return "", false
			}
			return f.String(), true
		}
	}

	return "", false
}

// translateDropStage translates a `drop <spec>` pipeline stage.
// Loki supports two forms:
//   - bare field names: `drop level, status` → `| delete level, status`
//   - label matchers: `drop level="debug"` → `| delete level`
//
// Note: Loki's matcher form conditionally drops the field only when the
// value matches. VL v1.50.0 lacks a working conditional-delete primitive
// (| if (filter) pipe without else filters out non-matching entries rather
// than passing them through). We translate both forms to unconditional
// | delete, which is semantically broader but maintains HTTP 200 parity.
// Multiple items are comma-separated and batched into a single `| delete`.
func translateDropStage(spec string, labelFn LabelTranslateFunc) string {
	items := splitCSV(spec)
	var fields []string

	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		// Detect a matcher operator (= != =~ !~ < > <= >=).
		hasOp := false
		for _, c := range item {
			if c == '=' || c == '!' || c == '<' || c == '>' {
				hasOp = true
				break
			}
		}
		if hasOp {
			// Matcher form (e.g. level="debug"): the field is only dropped when the
			// value matches. VL's `| delete` is unconditional, so we skip emitting it
			// here. The proxy post-processes each entry via ParseDropConditions /
			// applyDropConditions to implement the correct conditional semantics.
			continue
		}
		// Bare field name.
		fields = append(fields, item)
	}

	if len(fields) == 0 {
		return ""
	}
	return logsql.PipeDelete{Labels: fields}.String()
}

func translateLogDropStage(spec string, labelFn LabelTranslateFunc) string {
	items := splitCSV(spec)
	kept := items[:0]
	for _, item := range items {
		name := strings.TrimSpace(item)
		if slices.Contains(DetectedLevelFieldNames[1:], name) {
			continue
		}
		kept = append(kept, item)
	}
	return translateDropStage(strings.Join(kept, ","), labelFn)
}

type detectedLevelPresence uint8

const (
	detectedLevelUnknown detectedLevelPresence = iota
	detectedLevelPresent
	detectedLevelAbsent
)

func detectedLevelEmptyPresenceFilter(stage string) (nonempty, ok bool) {
	match := detectedLevelEmptyFilterRE.FindStringSubmatch(stage)
	if match == nil {
		return false, false
	}
	return match[1] == "!=", true
}

var detectedLevelEmptyFilterRE = regexp.MustCompile(`^\s*detected_level\s*(!=|==|=)\s*""\s*$`)

func nextDetectedLevelPresence(current detectedLevelPresence, stage string) detectedLevelPresence {
	if strings.HasPrefix(stage, "drop ") {
		for _, item := range splitCSV(stage[5:]) {
			item = strings.TrimSpace(item)
			if item == detectedLevelLabelName {
				return detectedLevelAbsent
			}
			if strings.HasPrefix(item, detectedLevelLabelName) {
				return detectedLevelUnknown // conditional drop
			}
		}
	}
	if strings.HasPrefix(stage, "keep ") {
		keepsDetected := false
		for _, item := range splitCSV(stage[5:]) {
			item = strings.TrimSpace(item)
			if strings.ContainsAny(item, "=!<>") {
				return detectedLevelUnknown // conditional keep
			}
			keepsDetected = keepsDetected || item == detectedLevelLabelName
		}
		if !keepsDetected {
			return detectedLevelAbsent
		}
	}
	if strings.HasPrefix(stage, "label_format ") {
		// The later stage can rename or replace detected_level, and the
		// response classifier must preserve Loki's label category as well.
		return detectedLevelUnknown
	}
	if current == detectedLevelAbsent && isKeyParserStage(stage) {
		return detectedLevelUnknown // a parser may create a new label
	}
	return current
}

// translateKeepStage translates a `keep <spec>` pipeline stage.
// Loki supports two forms:
//   - bare field names: `keep level, env` → `| fields _time, _msg, _stream, level, env`
//   - label matchers: `keep method="GET"` → `| fields _time, _msg, _stream, method`
//     VL field projection is unconditional; the proxy post-processes each entry via
//     ParseKeepConditions / applyKeepConditions to strip non-matching fields.
//
// Always includes _time, _msg, _stream so the proxy can reconstruct the Loki response.
func translateKeepStage(spec string, labelFn LabelTranslateFunc) string {
	items := splitCSV(spec)
	var fields []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		// Matcher form (e.g. method="GET"): project the field name unconditionally.
		// The proxy's applyKeepConditions handles per-entry conditional removal.
		if field, _, _, ok := parseDropMatcher(item); ok && field != "" {
			fields = append(fields, field)
			continue
		}
		// Bare field name — skip items that look like matchers with unsupported operators.
		if !strings.Contains(item, "=") {
			fields = append(fields, item)
		}
	}
	base := []string{"_time", "_msg", "_stream"}
	if len(fields) == 0 {
		return logsql.PipeFields{Labels: base}.String()
	}
	// A parsed label Loki renamed name_extracted (a collision with a stream
	// label) is the stored field name after unpack_*: keep that too, for the
	// proxy to rename.
	for _, field := range fields {
		if b := strings.TrimSuffix(field, extractedSuffix); b != field && isBareIdentifier(b) && !slices.Contains(fields, b) {
			fields = append(fields, b)
		}
	}
	// detected_level is derived from the stored level when that field exists.
	// Keep the source in the backend row; response processing removes it unless
	// the client also named level in the keep stage.
	if slices.Contains(fields, detectedLevelLabelName) && !slices.Contains(fields, "level") {
		fields = append(fields, "level")
	}
	// A Loki-compatible metadata label can name a different stored VL field
	// (for example deployment_environment -> deployment.environment). Retain
	// both spellings: some ingesters already store the Loki spelling.
	if labelFn != nil {
		for _, field := range slices.Clone(fields) {
			if field == detectedLevelLabelName || field == "service_name" {
				continue // derived labels have their own read-path projection
			}
			mapped := labelFn(field)
			if mapped != "" && mapped != field && !slices.Contains(fields, mapped) {
				fields = append(fields, mapped)
			}
		}
	}
	return logsql.PipeFields{Labels: append(base, fields...)}.String()
}

func keepStageDropsLevelSource(spec string) bool {
	var detected, level bool
	for _, item := range splitCSV(spec) {
		item = strings.TrimSpace(item)
		if field, _, _, ok := parseDropMatcher(item); ok {
			item = field
		}
		switch item {
		case detectedLevelLabelName:
			detected = true
		case "level":
			level = true
		}
	}
	return detected && !level
}

// splitCSV splits s on commas that are not inside parentheses or quotes.
func splitCSV(s string) []string {
	var result []string
	depth := 0
	inQuote := false
	quoteChar := byte(0)
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == quoteChar {
				inQuote = false
			}
			continue
		}
		switch c {
		case '"', '`', '\'':
			inQuote = true
			quoteChar = c
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				result = append(result, s[start:i])
				start = i + 1
			}
		}
	}
	result = append(result, s[start:])
	return result
}

func translateMalformedDottedStage(stage string, labelFn LabelTranslateFunc) (string, bool) {
	rawCandidate := translate.NormalizeFieldIdentifier(stage)
	if rawCandidate == "" {
		return "", false
	}
	if strings.ContainsAny(rawCandidate, `=<>!~`) {
		return "", false
	}
	trailingDot := strings.HasSuffix(rawCandidate, ".")
	candidate := strings.Trim(rawCandidate, ".")
	candidate = strings.Trim(candidate, ".")
	if !strings.Contains(candidate, ".") {
		return "", false
	}
	for _, r := range candidate {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-' {
			continue
		}
		return "", false
	}
	if labelFn != nil {
		candidate = translate.SanitizeFieldIdentifier(labelFn(candidate))
	}
	if candidate == "" {
		return "", false
	}
	if trailingDot {
		// Some upstream Drilldown paths emit malformed stages like:
		//   k8s . `cluster.`
		// Treat them as prefix regex filters to avoid generating an
		// impossible field matcher such as "k8s.cluster":!"".
		return fmt.Sprintf(`~"%s"`, regexp.QuoteMeta(candidate+".")), true
	}
	// Dotted field names must be quoted in VL; the :!"" form (non-empty check)
	// is not representable via FieldFilter, so we keep the literal format here.
	quotedField := candidate
	if strings.Contains(candidate, ".") {
		quotedField = `"` + candidate + `"`
	}
	return fmt.Sprintf(`%s:!""`, quotedField), true
}

// translateLabelFormat converts label_format expressions.
// Supports multiple renames: label_format a="{{.x}}", b="{{.y}}"
func translateLabelFormat(expr string) string {
	// Split by comma, respecting quotes
	assignments := splitLabelFormatAssignments(expr)
	var pipes []string
	for _, assign := range assignments {
		parts := strings.SplitN(assign, "=", 2)
		if len(parts) != 2 {
			continue
		}
		labelName := strings.TrimSpace(parts[0])
		template := strings.TrimSpace(parts[1])
		if template != "" && template[0] != '"' && template[0] != '`' {
			// dst=src renames: Loki sets dst to src's value only when src
			// exists, keeping dst otherwise, and removes src
			// (LabelsFormatter.Process). LogsQL rename would clear dst for a
			// missing src, so the copy is conditional. skip_empty_results
			// keeps dst where the condition fails on VictoriaLogs before
			// v1.35, which otherwise writes an empty dst there.
			pipes = append(pipes, fmt.Sprintf("| format if (%s:*) %q as %s skip_empty_results | delete %s", template, "<"+template+">", labelName, template))
			continue
		}
		// convertGoTemplate returns a quoted string like "<label>"; strip the
		// outer quotes before passing to PipeFormat, which re-applies %q quoting.
		converted := convertGoTemplate(template)
		unquoted, _ := strconv.Unquote(converted)
		pipes = append(pipes, logsql.PipeFormat{Template: unquoted, ResultField: labelName}.String())
	}
	if len(pipes) == 0 {
		return "| " + expr
	}
	return strings.Join(pipes, " ")
}

// splitLabelFormatAssignments splits "a=X, b=Y" respecting quoted values.
func splitLabelFormatAssignments(s string) []string {
	return splitDropItems(s)
}

// convertGoTemplate converts Go template syntax {{.label}} to LogsQL <label> syntax.
// Handles dotted field names like {{.service.name}} → <service.name>.
func convertGoTemplate(tmpl string) string {
	if unquoted, err := strconv.Unquote(strings.TrimSpace(tmpl)); err == nil {
		tmpl = unquoted
	}
	result := translate.GoTemplatePlaceholders(tmpl)
	return strconv.Quote(result)
}

// splitFuncFirstArg splits the first argument from a parenthesised function arg list,
// returning (firstArg, rest, ok). rest is the text after the first comma at depth 0.
// Input starts right after the opening '(' of the outer function.
func splitFuncFirstArg(s string) (first, rest string, ok bool) {
	n := logqlpkg.FirstArgumentLen(s)
	if n == len(s) {
		return "", "", false
	}
	if s[n] == ',' {
		return strings.TrimSpace(s[:n]), strings.TrimSpace(s[n+1:]), true
	}
	return strings.TrimSpace(s[:n]), "", true
}

// parseAllStringArgs parses all quoted string arguments (double- or back-tick-quoted)
// from a comma-separated list, stopping at ')' or unrecognised input.
func parseAllStringArgs(s string) []string {
	var args []string
	for {
		s = strings.TrimSpace(s)
		if s == "" || s[0] == ')' {
			break
		}
		if s[0] != '"' && s[0] != '`' {
			break
		}
		q := s[0]
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			break
		}
		args = append(args, s[1:end+1])
		s = strings.TrimSpace(s[end+2:])
		if len(s) > 0 && s[0] == ',' {
			s = strings.TrimSpace(s[1:])
		}
	}
	return args
}

// parseUnquotedStringArgs reads the comma-separated string arguments that
// follow the first argument of label_replace, unquoted as the
// LogQL lexer does: a double-quoted string has its escapes resolved, a
// backtick one is raw (pkg/logql/syntax/lex.go). It stops at ')' or at the
// first argument that is not a valid string.
func parseUnquotedStringArgs(s string) []string {
	var args []string
	for {
		s = strings.TrimSpace(s)
		quoted, err := strconv.QuotedPrefix(s)
		if err != nil {
			break
		}
		arg, err := strconv.Unquote(quoted)
		if err != nil {
			break
		}
		args = append(args, arg)
		s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s[len(quoted):]), ","))
	}
	return args
}

// SplitLabelReplace splits label_replace(v, "dst", "repl", "src", "regex") into
// the inner expression v and its label arguments.
func SplitLabelReplace(logql string) (inner string, spec LabelReplaceSpec, ok bool) {
	const prefix = "label_replace("
	logql = strings.TrimSpace(logql)
	if !strings.HasPrefix(logql, prefix) {
		return "", LabelReplaceSpec{}, false
	}
	inner, rest, ok := splitFuncFirstArg(logql[len(prefix):])
	if !ok {
		return "", LabelReplaceSpec{}, false
	}
	args := parseUnquotedStringArgs(rest)
	if len(args) != 4 {
		return "", LabelReplaceSpec{}, false
	}
	return inner, LabelReplaceSpec{DstLabel: args[0], Replacement: args[1], SrcLabel: args[2], Regex: args[3]}, true
}

// tryTranslateLabelReplace handles label_replace(v, "dst", "repl", "src", "regex").
// It translates the inner expression and embeds a marker for proxy post-processing.
func tryTranslateLabelReplace(logql string, labelFn LabelTranslateFunc) (string, bool) {
	const prefix = "label_replace("
	if !strings.HasPrefix(logql, prefix) {
		return "", false
	}
	inner, rest, ok := splitFuncFirstArg(logql[len(prefix):])
	if !ok {
		return "", false
	}
	args := parseUnquotedStringArgs(rest)
	if len(args) != 4 {
		return "", false
	}
	translated, err := translateLogQLFull(inner, labelFn, nil, logsql.Capabilities{})
	if err != nil {
		return "", false
	}
	return translated + labelReplaceMarker(LabelReplaceSpec{DstLabel: args[0], Replacement: args[1], SrcLabel: args[2], Regex: args[3]}), true
}

// labelReplaceMarker is the marker a translated query carries for a label_replace call:
// ParseAllLabelReplaceMarkers reads it back.
func labelReplaceMarker(spec LabelReplaceSpec) string {
	return string(appendLabelReplaceMarker(nil, spec))
}

// appendLabelReplaceMarker appends labelReplaceMarker(spec) to dst.
func appendLabelReplaceMarker(dst []byte, spec LabelReplaceSpec) []byte {
	return physical.AppendLabelReplaceMarker(dst, spec)
}

// tryTranslateMetricQuery attempts to translate a metric/aggregation query.
func tryTranslateMetricQuery(logql string, labelFn LabelTranslateFunc) (string, bool) {
	if chained, without, _, ok := tryTranslateVectorChain(logql, labelFn); ok && len(without) == 0 {
		return chained, true
	}
	return tryTranslateMetricQueryWithout(logql, labelFn, false, false)
}

// tryTranslateMetricQueryWithout is tryTranslateMetricQuery for a query whose
// `without (...)` clause the caller stripped: the outer aggregation then keeps
// the series identity in its grouping, so the proxy can drop the excluded
// labels and aggregate what remains.
func tryTranslateMetricQueryWithout(logql string, labelFn LabelTranslateFunc, hasWithout, foldWithout bool) (string, bool) { //nolint:gocyclo // multi-function metric dispatcher: quantile, rate, unwrap, stdvar, outer-agg, recursive nested — each branch is a distinct translation rule
	// Match patterns like: sum(rate({...}[5m])) by (label)
	// or: count_over_time({...}[5m])
	// or: rate({...}[5m])

	// VictoriaLogs has no first or last stats function, so "first" and "last" are
	// the stats functions only for the shapes firstLastRowStats does not translate
	// (stdvar by, topk by, bottomk by): they are sent as is and VictoriaLogs
	// rejects them, a loud failure rather than an answer without the aggregation.
	metricFuncs := map[string]string{
		"rate":             "count()",
		"count_over_time":  "count()",
		"bytes_over_time":  "sum_len(_msg)",
		"bytes_rate":       "sum_len(_msg)",
		"sum_over_time":    "sum",
		"avg_over_time":    "avg",
		"max_over_time":    "max",
		"min_over_time":    "min",
		"first_over_time":  "first",
		"last_over_time":   "last",
		"stddev_over_time": "stddev",
		"stdvar_over_time": "stddev",
		"absent_over_time": "count()",
		"rate_counter":     "__rate_counter__",
	}

	// Try to match outer aggregation: sum(...) by (labels)
	outerAgg, innerExpr, byLabels := extractOuterAggregation(logql)

	// count_values() groups by the VALUES of the inner metric — VL has no equivalent.
	if outerAgg == "count_values" {
		return "", false // caught by the count_values guard in translateLogQLFull
	}

	// If no outer aggregation, work with the raw expression
	if innerExpr == "" {
		innerExpr = logql
	}

	// Special handling for quantile_over_time(phi, {query} | unwrap field [duration])
	if result, ok := tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels, labelFn); ok {
		return result, true
	}

	// Try to match metric function: func({...} | pipeline [duration])
	for funcName, logsqlFunc := range metricFuncs {
		prefix := funcName + "("
		if !strings.HasPrefix(innerExpr, prefix) {
			continue
		}

		// Find the matching closing paren
		fullRest := innerExpr[len(prefix):]
		end := findLastMatchingParen(fullRest)
		if end < 0 {
			continue
		}
		inner := fullRest[:end]
		// Detect trailing by (...) modifier on the range aggregation, e.g.
		// avg_over_time({...} | json | unwrap field [5s]) by ()
		rangeByLabels, rangeByExplicit := extractRangeByClause(strings.TrimSpace(fullRest[end+1:]))

		// Extract the query and duration: {stream} | pipeline [5m]
		query, duration := extractQueryAndDuration(inner)
		if strings.TrimSpace(duration) == "" {
			continue
		}

		// Translate the inner log query part
		logsqlQuery, err := translateMetricInnerQuery(query, labelFn)
		if err != nil {
			continue
		}

		if funcName == "rate" || funcName == "bytes_rate" {
			rateExpr, rateGate, rateConv := logsqlFunc, "", ""
			if field := extractUnwrapField(inner); funcName == "rate" && field != "" {
				// rate over an unwrapped label is the sum of its values per
				// second (pkg/logql/range_vector.go rateLogs with computeValues).
				if gate, conv := unwrapGateField(inner, field); gate != "" {
					rateExpr, rateGate, rateConv = "sum("+field+")", gate, conv
				}
			}
			if rateResult, ok := buildRateLikeQuery(logsqlQuery, query, rateExpr, duration, outerAgg, byLabels, labelFn, hasWithout, foldWithout, rateGate, rateConv); ok {
				return rateResult, true
			}
		}

		// Build the LogsQL stats query
		var result string
		// unwrapByLabelsEmbedded tracks whether buildStatsQuery already embedded the
		// by-labels grouping via unwrapInnerGrouping — if so, skip the addByClause
		// call below to avoid emitting a duplicate "by (app) by (app)" clause.
		unwrapByLabelsEmbedded := false
		if isUnwrapFunc(funcName) {
			// For unwrap functions, extract the unwrap field
			unwrapField := extractUnwrapField(inner)
			if unwrapField == "" {
				continue
			}
			statsExpr := logsqlFunc + "(" + unwrapField + ")"
			// A plain unwrap keeps only the rows Loki makes samples of; a
			// conversion (duration(), bytes()) is parsed by the proxy's raw
			// evaluator, as VictoriaLogs reads the units differently.
			gate, gateConv := unwrapGateField(inner, unwrapField)
			innerBy := unwrapInnerGrouping(query, byLabels, outerAgg, labelFn, rangeByExplicit, rangeByLabels, foldWithout)

			// stdvar_over_time uses stddev + square, since VL doesn't have stdvar().
			if funcName == "stdvar_over_time" {
				if outerAgg != "" && byLabels == "" {
					innerAliased := buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "__lvp_inner", gate, gateConv)
					withVariance := innerAliased + " " + logsql.PipeMath{Alias: "__lvp_inner_var", Expr: "__lvp_inner*__lvp_inner"}.String()
					if outerResult, ok := applyOuterAggregation(withVariance, outerAgg, "__lvp_inner_var"); ok {
						return keepWithoutSeries(outerResult, innerBy, outerAgg, foldWithout), true
					}
				}
				baseStddev := buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "", gate, gateConv)
				return fmt.Sprintf("%s^:%s|||2", BinaryMetricPrefix, baseStddev), true
			}

			// first_over_time and last_over_time read each series' value through a
			// row function and unpack it (see firstLastRowStats).
			seriesExpr, seriesUnpack, firstLast := firstLastRowStats(funcName, statsExpr)
			// seriesValues is one value per series, grouped by what the query groups by.
			seriesValues := func(by string) string {
				if by == "" {
					by = defaultRateInnerGrouping(query, foldWithout)
				}
				return buildStatsQueryGated(logsqlQuery, seriesExpr, by, "__lvp_inner", gate, gateConv) + seriesUnpack +
					" | stats by (" + strings.ReplaceAll(by, emptyByGrouping, "") + ") max(__lvp_inner)"
			}

			seriesAgg, seriesAggOK := seriesLevelOuterAggregation(outerAgg)
			if fn, pow2 := outerAggregationStatsFn(outerAgg); firstLast && !seriesAggOK && fn != "" && !pow2 {
				// sum by (...) and stddev by (...) aggregate the per-series values, not
				// the rows of the group. stdvar, topk and bottomk with a grouping are not
				// translated (they stay the stats function VictoriaLogs rejects, a loud
				// error, rather than an answer that drops the aggregation).
				seriesAgg, seriesAggOK = fn+"(__lvp_inner)", true
			}
			if seriesAggOK && byLabels != "" {
				identity := defaultRateInnerGrouping(query, false)
				if firstLast {
					// The same series identity as the ungrouped aggregation.
					identity = unwrapInnerGrouping(query, "", outerAgg, labelFn, false, "", false)
				}
				innerAliased := buildStatsQueryGated(logsqlQuery, seriesExpr, joinByLabels(innerBy, identity), "__lvp_inner", gate, gateConv) + seriesUnpack
				result = addByClauseLast(innerAliased+" "+logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: seriesAgg}}}}.String(), byLabels, labelFn)
				unwrapByLabelsEmbedded = true
			} else if outerAgg != "" && byLabels == "" {
				innerAliased := buildStatsQueryGated(logsqlQuery, seriesExpr, innerBy, "__lvp_inner", gate, gateConv) + seriesUnpack
				if outerResult, ok := applyOuterAggregation(innerAliased, outerAgg, "__lvp_inner"); ok {
					result = keepWithoutSeries(outerResult, innerBy, outerAgg, foldWithout)
				} else if firstLast {
					result = seriesValues(innerBy)
				} else {
					result = buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "", gate, gateConv)
				}
			} else if firstLast && outerAgg == "" {
				// A bare function or a range-level by (...): the series' values.
				result = seriesValues(innerBy)
				unwrapByLabelsEmbedded = true
			} else {
				result = buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "", gate, gateConv)
				// buildStatsQuery already embedded byLabels via innerBy
				unwrapByLabelsEmbedded = byLabels != ""
			}
		} else if seriesAgg, ok := seriesLevelOuterAggregation(outerAgg); ok {
			// count/min/max/avg aggregate the VALUES of the inner range
			// aggregation, one per Loki series, so the inner stats keeps the
			// series identity and the outer stats aggregates those values.
			// Loki: count by (pod) (count_over_time(...)) is the number of
			// series per pod, not the number of lines.
			innerBy := joinByLabels(normalizeByLabels(byLabels, labelFn), seriesIdentityGrouping(query, foldWithout, labelFn))
			innerAliased := buildStatsQuery(logsqlQuery, logsqlFunc, innerBy, "__lvp_inner")
			result = innerAliased + " " + logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: seriesAgg}}}}.String()
			switch {
			case byLabels != "":
				result = addByClauseLast(result, byLabels, labelFn)
			case hasWithout:
				// One row per series: the proxy drops the excluded labels and
				// aggregates the rows of each remaining label set.
				result = addByClauseLast(result, innerBy, nil)
			}
			return result, true
		} else {
			result = logsqlQuery + " " + logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: logsqlFunc}}}}.String()
		}

		// Add by labels — skip for unwrap functions where the grouping was already
		// embedded in the stats clause by buildStatsQuery/unwrapInnerGrouping.
		if !unwrapByLabelsEmbedded && (byLabels != "" || outerAgg != "") {
			if byLabels != "" {
				result = addByClause(result, byLabels, labelFn)
			}
		}

		return result, true
	}

	// No funcPattern matched innerExpr. If there is an outer aggregation and innerExpr
	// looks like a metric expression (contains a range selector "["), try recursive
	// translation. This handles cases like count(sum by(app)(count_over_time(...))).
	if outerAgg != "" && innerExpr != "" && innerExpr != logql && strings.Contains(innerExpr, "[") {
		if translatedInner, ok := tryTranslateMetricQuery(innerExpr, labelFn); ok && translatedInner != "" {
			// Alias the last stats result so the outer aggregation can reference it.
			innerAliased := translatedInner + " as __lvp_inner"
			if outerResult, ok := applyOuterAggregation(innerAliased, outerAgg, "__lvp_inner"); ok {
				return outerResult, true
			}
		}
	}

	return "", false
}

// firstLastRowStats is the stats function and the pipes after it that read one
// value per series for first_over_time and last_over_time. VictoriaLogs has no
// first or last stats function: Loki's value is the sample with the earliest
// (first) or the latest (last) timestamp of the series in the window
// (pkg/logql/range_vector.go FirstOverTime, LastOverTime), which row_min(_time, v)
// and row_max(_time, v) return as a JSON row. VictoriaLogs reads a row_* result
// as a label, never as a metric, so the row's value is unpacked into a number
// that the stats pipe after it aggregates. ok is false for any other function.
func firstLastRowStats(funcName, statsExpr string) (expr, unpack string, ok bool) {
	var row string
	switch funcName {
	case "first_over_time":
		row = "row_min"
	case "last_over_time":
		row = "row_max"
	default:
		return statsExpr, "", false
	}
	field := statsExpr[strings.Index(statsExpr, "(")+1 : len(statsExpr)-1]
	return row + "(_time, " + field + ")", " | unpack_json from __lvp_inner fields (" + UnwrapValueAlias + ") | math " + UnwrapValueAlias + " as __lvp_inner", true
}

// seriesIdentityGrouping is the series identity of a range aggregation the
// outer stats pipe folds: the labels a trailing bare | keep leaves
// (trailingKeepGrouping), else defaultRateInnerGrouping.
func seriesIdentityGrouping(query string, foldWithout bool, labelFn LabelTranslateFunc) string {
	if kept, ok := trailingKeepGrouping(query); ok {
		if kept == "" {
			return emptyByGrouping
		}
		return normalizeByLabels(kept, labelFn)
	}
	return defaultRateInnerGrouping(query, foldWithout)
}

// trailingKeepGrouping returns the labels the last | keep stage of a log
// query names, comma-separated (Loki's error labels left out: keep never
// removes them), when that keep decides the labels of every sample: it has no
// matcher, and only line filters, label filters, line_format and decolorize
// follow it. Loki deletes every label keep does not name
// (pkg/logql/log/keep_labels.go:15-45), so the streams that keep the same
// labels are one series.
func trailingKeepGrouping(logqlQuery string) (string, bool) {
	remaining := strings.TrimSpace(logqlQuery)
	if !strings.HasPrefix(remaining, "{") {
		return "", false
	}
	end := findMatchingBrace(remaining)
	if end < 0 {
		return "", false
	}
	remaining = strings.TrimSpace(remaining[end+1:])
	var kept []string
	found := false
	for remaining != "" {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}
		if len(remaining) >= 2 && (remaining[0] == '|' || remaining[0] == '!') && strings.ContainsRune("=~>", rune(remaining[1])) {
			_, remaining = extractPipelineStage(remaining[2:]) // a line filter
			continue
		}
		if !strings.HasPrefix(remaining, "|") {
			return "", false
		}
		stage, rest := extractPipelineStage(strings.TrimSpace(remaining[1:]))
		remaining = rest
		stage = strings.TrimSpace(stage)
		word, _, _ := strings.Cut(stage, " ")
		switch word {
		case "keep":
			kept, found = kept[:0], true
			for _, item := range splitDropItems(stage[5:]) {
				item = strings.TrimSpace(item)
				if !isBareIdentifier(item) {
					return "", false // a matcher: the labels depend on the value
				}
				switch item {
				case "__error__", "__error_details__", "__preserve_error__":
					continue
				}
				kept = append(kept, item)
			}
		case "line_format", "decolorize":
		case "json", "logfmt", "regexp", "pattern", "unpack", "label_format", "drop", "unwrap":
			found = false
		default:
			if strings.HasPrefix(stage, "json") || strings.HasPrefix(stage, "logfmt") {
				found = false
			}
		}
	}
	if !found {
		return "", false
	}
	return strings.Join(kept, ", "), true
}

// defaultRateInnerGrouping is the series identity of a rate-like function:
// with a parser, the stream, plus (for the `without` fold only, foldWithout) the
// labels an extraction list names.
func defaultRateInnerGrouping(query string, foldWithout bool) string {
	if hasParserPipeline(query) {
		if foldWithout {
			if labels, ok := parsedSeriesLabels(query, extractUnwrapField(query)); ok {
				return joinByLabels("_stream", strings.Join(labels, ", "))
			}
		}
		return "_stream"
	}
	// Keep level-split streams distinct when level isn't configured as a VL _stream field.
	return "_stream, level"
}

func buildRateLikeQuery(logsqlQuery, originalQuery, statsExpr, duration, outerAgg, byLabels string, labelFn LabelTranslateFunc, hasWithout, foldWithout bool, gateField, gateConv string) (string, bool) {
	seconds := durationSeconds(duration)
	if seconds <= 0 {
		return "", false
	}

	innerBy := seriesIdentityGrouping(originalQuery, foldWithout, labelFn)
	seriesAgg, seriesLevel := seriesLevelOuterAggregation(outerAgg)
	switch {
	case seriesLevel:
		// count/min/max/avg read one rate value per Loki series, so the inner
		// stats keeps the series identity next to the requested labels.
		innerBy = joinByLabels(normalizeByLabels(byLabels, labelFn), innerBy)
	case byLabels != "":
		innerBy = normalizeByLabels(byLabels, labelFn)
	}

	innerAliased := buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "__lvp_inner", gateField, gateConv)
	if seriesLevel && seriesAgg == "count()" {
		// Counting series does not read the per-second value, so the rate
		// division is left out.
		result := innerAliased + " " + logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: seriesAgg}}}}.String()
		switch {
		case byLabels != "":
			result = addByClauseLast(result, byLabels, labelFn)
		case hasWithout:
			result = addByClauseLast(result, innerBy, nil)
		}
		return result, true
	}
	withRate := innerAliased + " " + logsql.PipeMath{Alias: "__lvp_rate", Expr: "__lvp_inner/" + strconv.FormatFloat(seconds, 'f', -1, 64)}.String()

	if seriesLevel {
		result := withRate + " " + logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: strings.ReplaceAll(seriesAgg, "__lvp_inner", "__lvp_rate")}}}}.String()
		switch {
		case byLabels != "":
			result = addByClauseLast(result, byLabels, labelFn)
		case hasWithout:
			result = addByClauseLast(result, innerBy, nil)
		}
		return result, true
	}

	if outerAgg != "" && byLabels == "" {
		if outerResult, ok := applyOuterAggregation(withRate, outerAgg, "__lvp_rate"); ok {
			return keepWithoutSeries(outerResult, innerBy, outerAgg, foldWithout), true
		}
	}

	return buildStatsQuery(withRate, "sum(__lvp_rate)", innerBy, ""), true
}

func buildStatsQuery(baseQuery, statsExpr, byLabels, alias string) string {
	return buildStatsQueryGated(baseQuery, statsExpr, byLabels, alias, "", "")
}

// buildStatsQueryGated is buildStatsQuery over an unwrapped label: gateField is
// the label statsExpr aggregates (empty for a stats function that reads no
// label). The rows are first reduced to the ones that make a Loki sample, and
// the label's value converted (UnwrapGate), once the pipes that resolve the label
// have run, and statsExpr then aggregates the converted value.
func buildStatsQueryGated(baseQuery, statsExpr, byLabels, alias, gateField, gateConv string) string {
	query := strings.TrimSpace(baseQuery)
	if query == "" {
		query = "*"
	}
	if gateField != "" {
		query = narrowUnwrapLogfmtUnpack(query, byLabels, gateField)
	}
	query = resolveStatsKeys(query, byLabels, statsExpr)
	if gateField != "" {
		query += UnwrapGateFor(gateField, gateConv)
		if i := strings.LastIndex(statsExpr, gateField+")"); i >= 0 {
			statsExpr = statsExpr[:i] + UnwrapValueAlias + ")" + statsExpr[i+len(gateField)+1:]
		}
	}
	// emptyByGrouping requires explicit "by ()" — PipeStats can't represent
	// an empty-but-explicit grouping without a dedicated struct field.
	if byLabels == emptyByGrouping {
		q := fmt.Sprintf("%s | stats by () %s", query, statsExpr)
		if alias != "" {
			q += " as " + alias
		}
		return q
	}

	fn := logsql.StatsFuncAlias{
		Func:  logsql.DeferredExpr{Raw: statsExpr},
		Alias: alias,
	}
	var by []logsql.GroupKey
	if byLabels != "" {
		for _, lbl := range strings.Split(byLabels, ",") {
			lbl = strings.TrimSpace(lbl)
			if lbl != "" {
				by = append(by, logsql.GroupKey{Field: lbl})
			}
		}
	}
	pipe := logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{fn}}
	return query + " " + pipe.String()
}

func outerAggregationStatsFn(outerAgg string) (statsFn string, pow2 bool) {
	switch outerAgg {
	case "sum":
		return "sum", false
	case "avg":
		return "avg", false
	case "max":
		return "max", false
	case "min":
		return "min", false
	case "count":
		return "count", false
	case "stddev":
		return "stddev", false
	case "stdvar":
		return "stddev", true
	default:
		return "", false
	}
}

// seriesLevelOuterAggregation returns the VictoriaLogs stats function for an
// outer aggregation that reads one value per inner series (count, min, max,
// avg), and false for sum, which is the same as aggregating the rows directly,
// and for the aggregations the proxy evaluates on the response.
func seriesLevelOuterAggregation(outerAgg string) (string, bool) {
	switch outerAgg {
	case "count":
		return "count()", true
	case "min", "max", "avg":
		return outerAgg + "(__lvp_inner)", true
	default:
		return "", false
	}
}

// joinByLabels concatenates two by-label lists, dropping duplicates and empty
// entries. An explicit empty grouping (by ()) wins over the identity labels.
func joinByLabels(byLabels, identity string) string {
	if byLabels == emptyByGrouping {
		return byLabels
	}
	seen := make(map[string]struct{}, 4)
	out := make([]string, 0, 4)
	for _, list := range []string{byLabels, identity} {
		for _, part := range strings.Split(list, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if _, ok := seen[part]; ok {
				continue
			}
			seen[part] = struct{}{}
			out = append(out, part)
		}
	}
	return strings.Join(out, ", ")
}

// addByClauseLast inserts by(labels) into the LAST stats pipe of query. It
// must run before any marker is appended to the translated query (the
// without marker, for one), because it locates that pipe by its last
// occurrence in the string.
func addByClauseLast(query, labels string, labelFn LabelTranslateFunc) string {
	labels = normalizeByLabels(labels, labelFn)
	if labels == "" {
		return query
	}
	idx := strings.LastIndex(query, "| stats ")
	if idx < 0 {
		return query + " | stats by (" + labels + ")"
	}
	statsStart := idx + len("| stats ")
	return query[:statsStart] + "by (" + labels + ") " + query[statsStart:]
}

// keepWithoutSeries keeps a row per series in the last stats pipe of an
// `<op> without (...)` translation, whose outer stats would otherwise fold every
// series into one. VictoriaLogs then answers a row per stream, and the proxy
// drops the excluded labels and folds the rows that share the labels left
// (applyWithoutGrouping), as Loki groups by every label but the excluded ones
// (pkg/logql/evaluator.go:451-466 VectorAggEvaluator.Next). `_msg`, which stands
// for the labels a parser extracted, stays out of that grouping: the proxy does
// not expose those labels, and a row per line would be raw-row volume. sum, max
// and min fold the rows of a stream to the same value either way; count and avg
// read one value per series, which only a grouping without `_msg` keeps.
func keepWithoutSeries(result, innerBy, outerAgg string, fold bool) string {
	if !fold || innerBy == emptyByGrouping {
		return result
	}
	keep := make([]string, 0, 2)
	perLine := false
	for _, label := range strings.Split(innerBy, ",") {
		switch label = strings.TrimSpace(label); label {
		case "":
		case "_msg":
			perLine = true
		default:
			keep = append(keep, label)
		}
	}
	switch {
	case len(keep) == 0:
		return result
	case outerAgg == "sum" || outerAgg == "max" || outerAgg == "min":
	case (outerAgg == "count" || outerAgg == "avg") && !perLine:
	default:
		return result
	}
	return addByClauseLast(result, strings.Join(keep, ", "), nil)
}

func applyOuterAggregation(baseQuery, outerAgg, field string) (string, bool) {
	statsFn, pow2 := outerAggregationStatsFn(outerAgg)
	if statsFn == "" {
		return "", false
	}
	var rawFunc string
	if statsFn == "count" {
		rawFunc = "count()"
	} else {
		rawFunc = statsFn + "(" + field + ")"
	}
	pipe := logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: rawFunc}}}}
	result := baseQuery + " " + pipe.String()
	if pow2 {
		result = fmt.Sprintf("%s^:%s|||2", BinaryMetricPrefix, result)
	}
	return result, true
}

var parserStageForUnwrapRE = regexp.MustCompile(`\|\s*(json|logfmt|pattern|regexp|unpack)\b`)

func hasParserPipeline(query string) bool {
	return parserStageForUnwrapRE.MatchString(query)
}

var rangeByClauseRE = regexp.MustCompile(`^by\s*\(([^)]*)\)`)

// Package-level compiled regexes — compiled once at program start, not per-request.
var (
	boolModifierRE  = regexp.MustCompile(`\s+bool\s+`)
	withoutMarkerRE = regexp.MustCompile(`\bwithout\s*\(([^)]+)\)`)
	vectorMatchRE   = regexp.MustCompile(`\s+(on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	aggByBeforeRE   = regexp.MustCompile(`^(sum|avg|max|min|count|topk|bottomk|stddev|stdvar|sort|sort_desc|count_values)\s+(?:by|without)\s*\(([^)]*)\)\s*\(`)
	aggFuncRE       = regexp.MustCompile(`^(sum|avg|max|min|count|topk|bottomk|stddev|stdvar|sort|sort_desc|count_values)\s*\(`)
	aggByAfterRE    = regexp.MustCompile(`^(?:by|without)\s*\(([^)]+)\)`)
	durationPartRE  = regexp.MustCompile(`([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)`)
)

// extractRangeByClause parses a trailing "by (...)" modifier that appears after
// the closing paren of a range aggregation, e.g. "avg_over_time(...[5s]) by ()".
// Returns the label list (empty string for "by ()") and whether the clause was present.
func extractRangeByClause(suffix string) (labels string, explicit bool) {
	m := rangeByClauseRE.FindStringSubmatch(suffix)
	if m == nil {
		return "", false
	}
	return strings.TrimSpace(m[1]), true
}

// unwrapInnerGrouping returns the by-labels string for buildStatsQuery.
// It selects the effective group-by labels for unwrap/range aggregations.
//
// foldWithout marks the `<op> without (...)` the proxy folds itself
// (foldingWithoutOp): only there a series is told apart by the labels an
// extraction list names. Every other query groups a parser's lines by `_msg`.
func unwrapInnerGrouping(query, byLabels, outerAgg string, labelFn LabelTranslateFunc, rangeByExplicit bool, rangeByLabels string, foldWithout bool) string {
	// Explicit range-level by (...) takes precedence over all heuristics.
	// by () means aggregate all entries into one series (no label grouping).
	if rangeByExplicit {
		if rangeByLabels == "" {
			// by () means aggregate all entries into one series — use sentinel so
			// buildStatsQuery emits "by ()" and the proxy can detect it downstream.
			return emptyByGrouping
		}
		return normalizeByLabels(rangeByLabels, labelFn)
	}
	if byLabels != "" {
		return normalizeByLabels(byLabels, labelFn)
	}
	if hasParserPipeline(query) {
		// A series is the stream plus the labels the parsers extract, less the
		// unwrapped one: _msg stands for them when a parser extracts every key
		// of every line.
		if foldWithout {
			if labels, ok := parsedSeriesLabels(query, extractUnwrapField(query)); ok {
				return joinByLabels("_stream", strings.Join(labels, ", "))
			}
		}
		return "_stream, _msg"
	}
	if outerAgg != "" {
		return "_stream"
	}
	return ""
}

func durationSeconds(duration string) float64 {
	duration = strings.TrimSpace(duration)
	if duration == "" {
		return 0
	}
	if d, err := time.ParseDuration(duration); err == nil {
		return d.Seconds()
	}

	parts := durationPartRE.FindAllStringSubmatch(duration, -1)
	if len(parts) == 0 {
		return 0
	}

	total := 0.0
	consumed := strings.Builder{}
	for _, part := range parts {
		value, err := strconv.ParseFloat(part[1], 64)
		if err != nil {
			return 0
		}
		switch part[2] {
		case "ns":
			total += value / 1e9
		case "us", "µs":
			total += value / 1e6
		case "ms":
			total += value / 1e3
		case "s":
			total += value
		case "m":
			total += value * 60
		case "h":
			total += value * 3600
		case "d":
			total += value * 86400
		case "w":
			total += value * 7 * 86400
		case "y":
			total += value * 365 * 86400
		default:
			return 0
		}
		consumed.WriteString(part[0])
	}
	if consumed.String() != duration {
		return 0
	}
	return total
}

// BinaryMetricOp represents a binary arithmetic expression between two metric queries.
// The proxy evaluates both sides against VL and combines the results.
const BinaryMetricPrefix = physical.BinaryPrefix

// tryTranslateBinaryMetricExpr handles expressions like:
//
//	sum(rate({app="x"}[5m])) / sum(rate({app="x",level="error"}[5m]))
//	rate({app="x"}[5m]) * 100
//
// Returns a special string "__binary__:op:leftQuery|||rightQuery" that the proxy
// parses and evaluates by running both queries independently.
func tryTranslateBinaryMetricExpr(logql string, labelFn LabelTranslateFunc) (string, bool) {
	logql = strings.TrimSpace(logql)

	// Strip the "bool" modifier from comparison operators.
	// Loki: "A > bool B" means return 1/0 instead of filtering.
	// We strip "bool" and let applyOp return 1/0 for all comparisons (matching Loki behavior).
	logql = boolModifierRE.ReplaceAllString(logql, " ")

	// Extract vector matching modifiers before stripping them.
	// on(labels), ignoring(labels) control join behavior.
	// group_left(labels), group_right(labels) control one-to-many cardinality.
	// We pass them through the binary expression format so the proxy can use them.
	var vectorMatchMeta []string
	for _, m := range vectorMatchRE.FindAllStringSubmatch(logql, -1) {
		if len(m) >= 3 {
			vectorMatchMeta = append(vectorMatchMeta, m[1]+":"+strings.TrimSpace(m[2]))
		}
	}
	logql = vectorMatchRE.ReplaceAllString(logql, "")
	for strings.Contains(logql, "  ") {
		logql = strings.ReplaceAll(logql, "  ", " ")
	}

	// Find a binary operator at the top level (not inside parens)
	// Includes: /, *, +, -, %, ^, ==, !=, >, <, >=, <=
	ops := []string{" / ", " * ", " + ", " - ", " % ", " ^ ", " == ", " != ", " >= ", " <= ", " > ", " < "}
	depth := 0
	for i, ch := range logql {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 {
			for _, op := range ops {
				if i+len(op) <= len(logql) && logql[i:i+len(op)] == op {
					left := strings.TrimSpace(logql[:i])
					right := strings.TrimSpace(logql[i+len(op):])
					operator := strings.TrimSpace(op)

					// Both sides must be valid metric queries
					leftQL, leftOK := tryTranslateMetricQuery(left, labelFn)
					rightQL, rightOK := tryTranslateMetricQuery(right, labelFn)

					vmSuffix := ""
					if len(vectorMatchMeta) > 0 {
						vmSuffix = "@@@" + strings.Join(vectorMatchMeta, "@@@")
					}

					if !leftOK || !rightOK {
						// One side might be a scalar (e.g., `rate(...) * 100`)
						if leftOK && IsScalar(right) {
							return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, leftQL, right, vmSuffix), true
						}
						if rightOK && IsScalar(left) {
							return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, left, rightQL, vmSuffix), true
						}
						continue
					}

					return fmt.Sprintf("%s%s:%s|||%s%s", BinaryMetricPrefix, operator, leftQL, rightQL, vmSuffix), true
				}
			}
		}
	}
	return "", false
}

// IsScalar returns true if the string is a numeric constant.
// Handles integers, floats, negatives, and scientific notation (e.g., 1e5, 1.5e-3, -42).
func IsScalar(s string) bool {
	_, ok := scalarValue(s)
	return ok
}

// scalarValue is the number IsScalar accepts.
func scalarValue(s string) (float64, bool) {
	// strconv.ParseFloat copies its input into the error it returns, so a query is turned away on the first byte
	// that no number, inf or nan has. A byte above ASCII goes on to the full check: TrimSpace trims Unicode spaces.
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 0x80, c >= '0' && c <= '9', c == '+' || c == '-' || c == '.' || c == '_',
			c == ' ' || (c >= '\t' && c <= '\r'),
			c >= 'a' && c <= 'f', c >= 'A' && c <= 'F', // hex digits
			c == 'i' || c == 'n' || c == 't' || c == 'y' || c == 'x' || c == 'p', // inf, infinity, nan, 0x, p exponent
			c == 'I' || c == 'N' || c == 'T' || c == 'Y' || c == 'X' || c == 'P':
		default:
			return 0, false
		}
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// Use strconv.ParseFloat which handles all numeric formats
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

// VectorMatchInfo holds vector matching modifiers for binary expressions.
type VectorMatchInfo struct {
	MatchOn    bool     // preserves an explicit empty on() modifier
	GroupSide  string   // preserves group_left()/group_right() without extra labels
	On         []string // on(labels) — match on these labels only
	Ignoring   []string // ignoring(labels) — match ignoring these labels
	GroupLeft  []string // group_left(extra_labels) — one-to-many, left side is "many"
	GroupRight []string // group_right(extra_labels) — one-to-many, right side is "many"
}

// ParseBinaryMetricExpr parses a "__binary__:op:left|||right[@@@modifier:labels...]" string.
// Returns the operator, left query, right query, vector matching info, and whether it's valid.
func ParseBinaryMetricExpr(s string) (op, left, right string, ok bool) {
	op, left, right, _, ok = ParseBinaryMetricExprFull(s)
	return op, left, right, ok
}

// ParseBinaryMetricExprFull parses binary expression with vector matching metadata.
func ParseBinaryMetricExprFull(s string) (op, left, right string, vm *VectorMatchInfo, ok bool) {
	if !strings.HasPrefix(s, BinaryMetricPrefix) {
		return "", "", "", nil, false
	}
	rest := s[len(BinaryMetricPrefix):]
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return "", "", "", nil, false
	}
	op = rest[:colonIdx]
	body := rest[colonIdx+1:]

	// Split off vector matching metadata at @@@
	vm = &VectorMatchInfo{}
	if atIdx := strings.Index(body, "@@@"); atIdx >= 0 {
		metaPart := body[atIdx+3:]
		body = body[:atIdx]
		for _, segment := range strings.Split(metaPart, "@@@") {
			parts := strings.SplitN(segment, ":", 2)
			if len(parts) != 2 {
				continue
			}
			modifier := parts[0]
			labels := splitLabels(parts[1])
			switch modifier {
			case "on":
				vm.MatchOn = true
				vm.On = labels
			case "ignoring":
				vm.Ignoring = labels
			case "group_left":
				vm.GroupSide = "group_left"
				vm.GroupLeft = labels
			case "group_right":
				vm.GroupSide = "group_right"
				vm.GroupRight = labels
			}
		}
	}

	sepIdx := strings.Index(body, "|||")
	if sepIdx < 0 {
		return "", "", "", nil, false
	}
	left = body[:sepIdx]
	right = body[sepIdx+3:]
	return op, left, right, vm, true
}

func splitLabels(s string) []string {
	var labels []string
	for _, l := range strings.Split(s, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			labels = append(labels, l)
		}
	}
	return labels
}

func isUnwrapFunc(name string) bool {
	unwrapFuncs := map[string]bool{
		"sum_over_time": true, "avg_over_time": true,
		"max_over_time": true, "min_over_time": true,
		"first_over_time": true, "last_over_time": true,
		"stddev_over_time": true, "stdvar_over_time": true,
		"quantile_over_time": true, "rate_counter": true,
	}
	return unwrapFuncs[name]
}

func missingUnwrapRangeMetricFunc(logql string) string {
	logql = strings.TrimSpace(logql)
	if logql == "" {
		return ""
	}

	if _, inner, _ := extractOuterAggregation(logql); inner != "" {
		logql = strings.TrimSpace(inner)
	}

	funcs := []string{
		"sum_over_time", "avg_over_time",
		"max_over_time", "min_over_time",
		"first_over_time", "last_over_time",
		"stddev_over_time", "stdvar_over_time",
		"quantile_over_time", "rate_counter",
	}

	for _, funcName := range funcs {
		prefix := funcName + "("
		if !strings.HasPrefix(logql, prefix) {
			continue
		}
		inner := logql[len(prefix):]
		end := findLastMatchingParen(inner)
		if end < 0 {
			return ""
		}
		inner = inner[:end]
		if funcName == "quantile_over_time" {
			if strings.Contains(inner, "[") && strings.Contains(inner, "]") && extractUnwrapField(inner) == "" {
				return funcName
			}
			return ""
		}
		_, duration := extractQueryAndDuration(inner)
		if strings.TrimSpace(duration) == "" {
			return ""
		}
		if strings.Contains(duration, ":") {
			return ""
		}
		if extractUnwrapField(inner) == "" {
			return funcName
		}
		return ""
	}

	return ""
}

func extractOuterAggregation(logql string) (agg, inner, byLabels string) {
	// Match two forms:
	// 1. sum(...) by (labels)     — by AFTER
	// 2. sum by (labels) (...)    — by BEFORE
	// 3. topk(K, sum by (labels) (...))  — nested

	// Try form 2 first: sum by (labels) (...) or sum without (labels) (...)
	bm := aggByBeforeRE.FindStringSubmatch(logql)
	if bm != nil {
		agg = bm[1]
		byLabels = bm[2]
		rest := logql[len(bm[0]):]
		end := findLastMatchingParen(rest)
		if end >= 0 {
			inner = rest[:end]
			// topk/bottomk carry a leading K arg: topk by (l) (K, inner_expr).
			// count_values carries a quoted label-name first arg: count_values("l", expr).
			// Strip both so innerExpr starts with the actual metric expression.
			switch agg {
			case "topk", "bottomk":
				if commaIdx := strings.Index(inner, ","); commaIdx >= 0 {
					inner = strings.TrimSpace(inner[commaIdx+1:])
				}
			case "count_values":
				args := parseAllStringArgs(inner)
				if len(args) >= 1 {
					q := `"` + args[0] + `"`
					if idx := strings.Index(inner, q); idx >= 0 {
						rest2 := strings.TrimSpace(inner[idx+len(q):])
						if strings.HasPrefix(rest2, ",") {
							inner = strings.TrimSpace(rest2[1:])
						}
					}
				}
			}
			return agg, inner, byLabels
		}
	}

	// Form 1: sum(...) by (labels) or sum(...) without (labels)
	m := aggFuncRE.FindStringSubmatch(logql)
	if m == nil {
		return "", "", ""
	}

	agg = m[1]
	rest := logql[len(m[0]):]

	// For topk/bottomk, skip the first numeric arg: topk(10, ...)
	// For count_values, skip the first quoted string arg: count_values("label", ...)
	switch agg {
	case "topk", "bottomk":
		commaIdx := strings.Index(rest, ",")
		if commaIdx >= 0 {
			rest = strings.TrimSpace(rest[commaIdx+1:])
		}
	case "count_values":
		args := parseAllStringArgs(rest)
		if len(args) >= 1 {
			q := `"` + args[0] + `"`
			if idx := strings.Index(rest, q); idx >= 0 {
				rest2 := strings.TrimSpace(rest[idx+len(q):])
				if strings.HasPrefix(rest2, ",") {
					rest = strings.TrimSpace(rest2[1:])
				}
			}
		}
	}

	// Find matching paren
	end := findLastMatchingParen(rest)
	if end < 0 {
		return "", "", ""
	}
	inner = rest[:end]
	rest = strings.TrimSpace(rest[end+1:])

	// Extract by or without clause after: ... by (labels) or ... without (labels)
	bm2 := aggByAfterRE.FindStringSubmatch(rest)
	if bm2 != nil {
		byLabels = bm2[1]
	}

	return agg, inner, byLabels
}

func extractQueryAndDuration(inner string) (query, duration string) {
	// Find [duration] at the end
	bracketIdx := strings.LastIndex(inner, "[")
	if bracketIdx >= 0 {
		closeBracket := strings.Index(inner[bracketIdx:], "]")
		if closeBracket >= 0 {
			duration = inner[bracketIdx+1 : bracketIdx+closeBracket]
			query = strings.TrimSpace(inner[:bracketIdx])
			return
		}
	}
	return inner, ""
}

func extractUnwrapField(inner string) string {
	// Find | unwrap field_name or | unwrap duration(field), duration_seconds(field) or bytes(field)
	idx := strings.Index(inner, "| unwrap ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(inner[idx+9:])
	// Field name is everything before the next | or [
	end := strings.IndexAny(rest, "|[")
	if end >= 0 {
		rest = strings.TrimSpace(rest[:end])
	} else {
		rest = strings.TrimSpace(rest)
	}

	// Strip conversion wrappers: duration(field), duration_seconds(field) and
	// bytes(field) → field
	for _, c := range unwrapConversions {
		if strings.HasPrefix(rest, c.call) && strings.HasSuffix(rest, ")") {
			rest = rest[len(c.call) : len(rest)-1]
			break
		}
	}

	return rest
}

// tryTranslateQuantileOverTime handles quantile_over_time(phi, {query} | unwrap field [duration]).
// Maps to VL: query | stats quantile(phi, field)
func tryTranslateQuantileOverTime(innerExpr, outerAgg, byLabels string, labelFn LabelTranslateFunc) (string, bool) {
	prefix := "quantile_over_time("
	if !strings.HasPrefix(innerExpr, prefix) {
		return "", false
	}

	rest := innerExpr[len(prefix):]
	end := findLastMatchingParen(rest)
	if end < 0 {
		return "", false
	}
	body := rest[:end]

	// Extract phi (first arg before comma): quantile_over_time(0.95, ...)
	commaIdx := strings.Index(body, ",")
	if commaIdx < 0 {
		return "", false
	}
	phi := strings.TrimSpace(body[:commaIdx])
	queryPart := strings.TrimSpace(body[commaIdx+1:])

	// Extract query and duration
	query, duration := extractQueryAndDuration(queryPart)
	if strings.TrimSpace(duration) == "" {
		return "", false
	}

	// Translate the inner log query
	logsqlQuery, err := translateMetricInnerQuery(query, labelFn)
	if err != nil {
		return "", false
	}

	// Extract unwrap field
	unwrapField := extractUnwrapField(queryPart)
	if unwrapField == "" {
		return "", false
	}

	statsExpr := "quantile(" + phi + ", " + unwrapField + ")"
	gate, gateConv := unwrapGateField(queryPart, unwrapField)
	rangeByLabels, rangeByExplicit := extractRangeByClause(strings.TrimSpace(rest[end+1:]))
	innerBy := unwrapInnerGrouping(query, byLabels, outerAgg, labelFn, rangeByExplicit, rangeByLabels, false)

	if outerAgg != "" && byLabels == "" {
		innerAliased := buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "__lvp_inner", gate, gateConv)
		if outerResult, ok := applyOuterAggregation(innerAliased, outerAgg, "__lvp_inner"); ok {
			return outerResult, true
		}
	}

	return buildStatsQueryGated(logsqlQuery, statsExpr, innerBy, "", gate, gateConv), true
}

func addByClause(query, labels string, labelFn LabelTranslateFunc) string {
	labels = normalizeByLabels(labels, labelFn)
	if labels == "" {
		return query
	}
	// Insert by(labels) into the stats pipe
	idx := strings.Index(query, "| stats ")
	if idx < 0 {
		return query + " | stats by (" + labels + ")"
	}
	statsStart := idx + len("| stats ")
	rest := query[statsStart:]
	// The grouping may name sanitized parser keys: resolve them before the stats.
	expr, _, _ := strings.Cut(rest, " as ")
	expr, _, _ = strings.Cut(expr, " | ")
	prefix := resolveStatsKeys(strings.TrimRight(query[:idx], " "), labels, expr)
	return prefix + " | stats by (" + labels + ") " + rest
}

func normalizeByLabels(labels string, labelFn LabelTranslateFunc) string {
	parts := strings.Split(labels, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// service_name and detected_level are derived per row
		// (applyDerivedGroupings), never translated source fields.
		if labelFn != nil && part != "service_name" && part != detectedLevelLabelName {
			part = strings.TrimSpace(labelFn(part))
		}
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return strings.Join(out, ", ")
}

// applyServiceNameGrouping makes `by (service_name)` group by the service name
// Loki assigns: every metric subquery whose stats pipe groups by service_name
// computes that field first, as the first non-empty source field in
// translate.SyntheticServiceNameFields order, else unknown_service (the value the
// selector filter matches). The pipes go before any parser stage, which reads
// fields but never changes a stream's service name in Loki. VictoriaLogs
// v1.51+ has the coalesce pipe; older versions get the same value from format
// pipes that keep a non-empty destination.
func applyServiceNameGrouping(query string, caps logsql.Capabilities) string {
	if !strings.Contains(query, "service_name") {
		return query
	}
	return applyDerivedGrouping(query, "service_name", func(q string) string {
		return WithDerivedServiceName(q, caps)
	})
}

// applyDerivedGroupings computes the labels Loki assigns at ingest —
// service_name and detected_level — for every metric subquery that groups by
// one of them.
func applyDerivedGroupings(query string, caps logsql.Capabilities, streamFields map[string]bool) string {
	query = applyServiceNameGrouping(query, caps)
	return ApplyStreamIdentityDetectedLevel(query, streamFields)
}

// ApplyStreamIdentityDetectedLevel makes the series a stream-identity grouping
// names carry Loki's detected_level. A bare metric, a without() grouping and
// the inner stage of count, min, max and avg group by `_stream, level`: the
// stream and its stored level. Loki's series are the stream plus the
// structured metadata of the entry, detected_level always among them, so the
// grouping also names detected_level and the chain below computes it per row.
// Without it a row that has no level field would be a series without the
// label, or the one series of every level the line body or severity fields
// give it.
func ApplyStreamIdentityDetectedLevel(query string, streamFields map[string]bool) string {
	return applyDetectedLevelGrouping(addDetectedLevelToStreamIdentity(query), detectedLevelKnownFields(streamFields))
}

// addDetectedLevelToStreamIdentity appends detected_level to the by() list of
// every stats pipe that groups by both _stream and level.
func addDetectedLevelToStreamIdentity(query string) string {
	const marker = "| stats by ("
	var b strings.Builder
	for rest := query; ; {
		idx := topLevelPipePrefix(rest, marker)
		if idx < 0 {
			b.WriteString(rest)
			return b.String()
		}
		listStart := idx + len(marker)
		end := strings.IndexByte(rest[listStart:], ')')
		if end < 0 {
			b.WriteString(rest)
			return b.String()
		}
		list := rest[listStart : listStart+end]
		b.WriteString(rest[:listStart+end])
		items := strings.Split(list, ",")
		var stream, level, detected bool
		for _, item := range items {
			switch strings.TrimSpace(item) {
			case "_stream":
				stream = true
			case "level":
				level = true
			case detectedLevelLabelName:
				detected = true
			}
		}
		if stream && level && !detected {
			b.WriteString(", " + detectedLevelLabelName)
		}
		rest = rest[listStart+end:]
	}
}

// detectedLevelKnownFields lists the level-deciding fields the chain treats as
// stored. Every name Loki reads is included: a field VictoriaLogs does not
// hold costs nothing to test.
func detectedLevelKnownFields(map[string]bool) []string {
	return DetectedLevelFieldNames
}

// applyDerivedGrouping inserts the pipes that compute a derived label into
// every metric subquery whose stats pipe groups by it.
func applyDerivedGrouping(query, label string, insert func(string) string) string {
	segments := strings.Split(query, binaryMetricSeparator)
	for i, segment := range segments {
		segments[i] = applyDerivedGroupingSegment(segment, label, insert)
	}
	return strings.Join(segments, binaryMetricSeparator)
}

// applyDetectedLevelGrouping makes `by (detected_level)` group by the value
// Loki attaches at ingest. VictoriaLogs stores whatever was pushed, so the
// chain reads the stored level fields in Loki's priority order, normalises the
// spellings it knows, maps severity_number, and for a row with no stored level
// reads the line as JSON, as logfmt and then as text, with unknown as the
// fallback — the same rule the log responses derive per row.
func applyDetectedLevelGrouping(query string, existingFields []string) string {
	if !strings.Contains(query, detectedLevelLabelName) {
		return query
	}
	chain := BuildDetectedLevelChain(existingFields, DetectedLevelChainGrouping, nil)
	return applyDerivedGrouping(query, detectedLevelLabelName, func(q string) string {
		if topLevelPipePrefix(q, detectedLevelChainMarker) >= 0 {
			return q // already derived: the chain must run once
		}
		pipeAt := firstTopLevelPipe(q)
		if pipeAt < 0 {
			return strings.TrimSpace(q) + " " + chain
		}
		return q[:pipeAt] + chain + " " + q[pipeAt:]
	})
}

const detectedLevelLabelName = "detected_level"

// detectedLevelStageFilter renders a `detected_level <op> "<value>"` stage that
// no parser, line rewrite or label stage precedes. Loki assigns the label at
// ingest (pkg/distributor/field_detection.go extractLogLevel: the stream
// labels first, then structured metadata, then the OTLP severity_number, then
// the line's JSON or logfmt level key, then its first bounded level keyword,
// else unknown), so the filter compares that value (BuildDetectedLevelFilter)
// rather than one stored field: a stream label decides over a level key in the
// line, and a line without any level is unknown. A metric query writes the
// value into detected_level with the grouping chain, which a grouping by it
// reuses; a log query reads it into a scratch field it deletes again; with
// -detected-level-body-scan=false the stored level fields alone decide, as
// the proxy derives the label then. ok is
// false for any other stage, for an invalid regexp, and for a log query's
// matcher on the empty value, which keep their rendering.
func detectedLevelStageFilter(stage string, metric bool, mode DetectedLevelFilterMode) (DetectedLevelFilterPlan, bool) {
	op, value, ok := detectedLevelStageMatcher(stage)
	if !ok || (!metric && value == "" && (op == "=" || op == "!=")) {
		// A log query reads `=""` and `!=""` from the presence of the label
		// (detectedLevelEmptyPresenceFilter), which drop and keep change.
		return DetectedLevelFilterPlan{}, false
	}
	plan, err := BuildDetectedLevelFilter(op, value, detectedLevelKnownFields(nil), mode)
	if err != nil {
		return DetectedLevelFilterPlan{}, false
	}
	return plan, true
}

// detectedLevelFilterMode is the form of a detected_level filter in a query.
func detectedLevelFilterMode(metric bool, caps logsql.Capabilities) DetectedLevelFilterMode {
	switch {
	case caps.NoDetectedLevelBodyScan:
		return DetectedLevelFilterStored
	case metric:
		return DetectedLevelFilterMetric
	}
	return DetectedLevelFilterLogs
}

// dropsOnlyErrorLabels reports whether stage is a drop of the parser error
// labels alone, which leaves the level inputs as stored.
func dropsOnlyErrorLabels(stage string) bool {
	spec, ok := strings.CutPrefix(stage, "drop ")
	if !ok {
		return false
	}
	for _, item := range splitCSV(spec) {
		if item = strings.TrimSpace(item); item != "__error__" && item != "__error_details__" {
			return false
		}
	}
	return true
}

// detectedLevelStageMatcher reads a label filter stage holding one matcher on
// detected_level with a single quoted value.
func detectedLevelStageMatcher(stage string) (op, value string, ok bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(stage), detectedLevelLabelName)
	if !found {
		return "", "", false
	}
	rest = strings.TrimSpace(rest)
	for _, candidate := range []string{"!=", "=~", "!~", "==", "="} {
		quoted, cut := strings.CutPrefix(rest, candidate)
		if !cut {
			continue
		}
		op = candidate
		if candidate == "==" {
			op = "="
		}
		quoted = strings.TrimSpace(quoted)
		if len(quoted) >= 2 && quoted[0] == '`' && quoted[len(quoted)-1] == '`' && !strings.Contains(quoted[1:len(quoted)-1], "`") {
			return op, quoted[1 : len(quoted)-1], true
		}
		if len(quoted) >= 2 && quoted[0] == '"' {
			if decoded, err := strconv.Unquote(quoted); err == nil {
				return op, decoded, true
			}
		}
		return "", "", false
	}
	return "", "", false
}

// detectedLevelChainMarker is the first pipe of BuildDetectedLevelChain.
const detectedLevelChainMarker = `| format if (detected_level:*) "<detected_level>" as __dl_s`

const binaryMetricSeparator = "|||"

func applyDerivedGroupingSegment(segment, label string, insert func(string) string) string {
	start := 0
	for strings.HasPrefix(segment[start:], BinaryMetricPrefix) {
		colon := strings.Index(segment[start+len(BinaryMetricPrefix):], ":")
		if colon < 0 {
			return segment
		}
		start += len(BinaryMetricPrefix) + colon + 1
	}
	if !statsGroupsByLabel(segment[start:], label) {
		return segment
	}
	return segment[:start] + insert(segment[start:])
}

// WithDerivedServiceName returns query with the pipes that set the
// service_name field to the service name Loki assigns (the first non-empty
// field of translate.SyntheticServiceNameFields, else unknown_service) inserted before
// its first pipe, or appended to a pipe-less filter.
func WithDerivedServiceName(query string, caps logsql.Capabilities) string {
	pipeAt := firstTopLevelPipe(query)
	if pipeAt < 0 {
		return strings.TrimSpace(query) + " " + serviceNameGroupingPipes(caps)
	}
	return query[:pipeAt] + serviceNameGroupingPipes(caps) + " " + query[pipeAt:]
}

// statsGroupsByLabel reports whether a stats pipe of query groups by label.
func statsGroupsByLabel(query, label string) bool {
	const marker = "| stats by ("
	for rest := query; ; {
		idx := topLevelPipePrefix(rest, marker)
		if idx < 0 {
			return false
		}
		rest = rest[idx+len(marker):]
		end := strings.IndexByte(rest, ')')
		if end < 0 {
			return false
		}
		for _, item := range strings.Split(rest[:end], ",") {
			if strings.TrimSpace(item) == label {
				return true
			}
		}
		rest = rest[end:]
	}
}

// topLevelPipePrefix finds a pipe stage outside LogsQL's quoted strings.
// Source selectors and filters may contain text that looks exactly like a
// stats or derivation stage; treating it as syntax mutates the user's data.
func topLevelPipePrefix(query, prefix string) int {
	for offset := 0; offset < len(query); {
		next := firstTopLevelPipe(query[offset:])
		if next < 0 {
			return -1
		}
		idx := offset + next
		if strings.HasPrefix(query[idx:], prefix) {
			return idx
		}
		offset = idx + 1
	}
	return -1
}

// FirstTopLevelPipe returns the index of the first '|' of a LogsQL query
// outside quoted strings (the end of its filters), or -1.
func FirstTopLevelPipe(query string) int { return firstTopLevelPipe(query) }

// firstTopLevelPipe returns the index of the first '|' outside quoted strings.
func firstTopLevelPipe(query string) int { return translate.FirstTopLevelPipe(query) }

// ServiceNameSourceFields returns the VictoriaLogs fields the proxy reads to
// derive service_name, in Loki's discover_service_name order. The result must
// not be modified.
func ServiceNameSourceFields() []string { return translate.SyntheticServiceNameFields }

// ServiceNameDerivationPipes returns the pipes that set the service_name field
// of every row to the service name Loki assigns.
func ServiceNameDerivationPipes(caps logsql.Capabilities) string {
	return serviceNameGroupingPipes(caps)
}

func serviceNameGroupingPipes(caps logsql.Capabilities) string {
	names := make([]string, len(translate.SyntheticServiceNameFields))
	for i, field := range translate.SyntheticServiceNameFields {
		names[i] = field
		if strings.Contains(field, ".") {
			names[i] = `"` + field + `"`
		}
	}
	if caps.PipeCoalesce {
		return "| coalesce(" + strings.Join(names, ", ") + ") default " + logsql.QuoteValue(unknownServiceNameValue) + " as service_name"
	}
	var b strings.Builder
	for _, field := range translate.SyntheticServiceNameFields[1:] {
		b.WriteString(`| format "<` + field + `>" as service_name keep_original_fields `)
	}
	b.WriteString(`| format ` + logsql.QuoteValue(unknownServiceNameValue) + ` as service_name keep_original_fields`)
	return b.String()
}

func findMatchingBrace(s string) int {
	depth := 0
	var inQuote rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		// Handle backslash escapes inside quotes
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++ // skip escaped character
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '{' {
			depth++
		}
		if c == '}' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func findLastMatchingParen(s string) int {
	depth := 0
	var inQuote rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '(' {
			depth++
		}
		if c == ')' {
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

func extractPipelineStage(s string) (stage, rest string) {
	// A pipeline stage goes until the next unquoted |
	var inQuote rune
	stuck := false
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && inQuote == '"' && i+1 < len(s) {
			i++
			continue
		}
		if (c == '"' || c == '`') && (inQuote == 0 || inQuote == c) {
			if inQuote == 0 {
				inQuote = c
			} else {
				inQuote = 0
			}
		}
		if inQuote != 0 {
			continue
		}
		if c == '|' {
			return strings.TrimSpace(s[:i]), s[i:]
		}
		// !> (pattern negation) acts as a stage boundary without a preceding |.
		// Stop here so the caller's loop can handle !> as its own operator. So do
		// != and !~ right after a bare parser (`| json != "x"`) or a complete label
		// filter (`| (a="1" or b="2") != "x"`), where they are a line filter and
		// not a label filter operator.
		if c == '!' && i+1 < len(s) && s[i+1] == '>' {
			return strings.TrimSpace(s[:i]), s[i:]
		}
		if c == '!' && i+1 < len(s) && (s[i+1] == '=' || s[i+1] == '~') && !stuck {
			prefix := strings.TrimSpace(s[:i])
			complete := isBareParserKeyword(prefix)
			if !complete {
				_, err := logqlpkg.ParseLabelFilterExpr(prefix)
				complete = err == nil
				// A prefix that fails before its end fails the same way with
				// anything appended: no later operator can follow a complete stage.
				stuck = err != nil && err.Offset < len(prefix)
			}
			if complete {
				return prefix, s[i:]
			}
		}
	}
	return strings.TrimSpace(s), ""
}

func isBareParserKeyword(stage string) bool {
	switch stage {
	case "json", "logfmt", "unpack", "decolorize":
		return true
	}
	return false
}

// extractQuotedValue extracts a quoted string (respecting escaped quotes) and returns (quoted_value, remaining).
func extractQuotedValue(s string) (string, string) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "\"") {
		// Skip escape pairs so a quote after an even number of backslashes closes
		// the literal, while a quote after an odd number remains part of it.
		for i := 1; i < len(s); i++ {
			if s[i] == '\\' {
				i++
				continue
			}
			if s[i] == '"' {
				return s[:i+1], strings.TrimSpace(s[i+1:])
			}
		}
		// No closing quote found — return everything
		return s, ""
	}
	if strings.HasPrefix(s, "`") {
		if i := strings.IndexByte(s[1:], '`'); i >= 0 {
			// Raw strings should behave like regular quoted literals in the translated query.
			return strconv.Quote(s[1 : i+1]), strings.TrimSpace(s[i+2:])
		}
		return strconv.Quote(s[1:]), ""
	}
	// Not quoted — take until next space or pipe
	end := strings.IndexAny(s, " |")
	if end >= 0 {
		return `"` + s[:end] + `"`, strings.TrimSpace(s[end:])
	}
	return `"` + s + `"`, ""
}

func normalizeQuotedStageExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return expr
	}
	quoted, rest := extractQuotedValue(expr)
	if strings.TrimSpace(rest) == "" && quoted != "" {
		return quoted
	}
	return expr
}

// takeOrAlternatives consumes the `or "b" or ip("c")` alternatives that follow a
// line filter operand and returns them with the rest of the query.
func takeOrAlternatives(rest string) ([]translate.OrAlternative, string, error) {
	var alts []translate.OrAlternative
	for len(rest) > 2 && strings.EqualFold(rest[:2], "or") && (unicode.IsSpace(rune(rest[2])) || rest[2] == '"' || rest[2] == '`') {
		rest = strings.TrimSpace(rest[2:])
		if arg, after, ok := extractIPFilterArg(rest); ok {
			alts = append(alts, translate.OrAlternative{Text: arg, IP: true})
			rest = after
			continue
		}
		quoted, after := extractQuotedValue(rest)
		text, err := strconv.Unquote(quoted)
		if err != nil {
			return nil, "", fmt.Errorf("invalid line filter: %w", err)
		}
		alts = append(alts, translate.OrAlternative{Text: text})
		rest = after
	}
	return alts, rest, nil
}

// patternFilterExtractRE matches the extract pipe of a pattern line filter.
var patternFilterExtractRE = regexp.MustCompile(`\| extract "(?:[^"\\]|\\.)*<plain:` + translate.PatternFilterScratch + `[0-9]+_(?:[0-9]+|r)>(?:[^"\\]|\\.)*" from _msg`)

// WithoutPatternLineFilterExtracts returns query without the extract pipes of
// its pattern line filters, for the heuristics that look for parser stages: a
// line filter parses nothing in Loki, so it adds no labels to a metric series.
func WithoutPatternLineFilterExtracts(query string) string {
	if !strings.Contains(query, translate.PatternFilterScratch) {
		return query
	}
	return patternFilterExtractRE.ReplaceAllString(query, "")
}

// takePatternValues reads the operand of a pattern line filter and its `or`
// alternatives (an ip("...") is plain pattern text, as in Loki's or chain) and
// returns the rest of the query, which starts at the next operator.
func takePatternValues(s string) (values []string, rest string, err error) {
	head, rest := "", ""
	if arg, after, ok := extractIPFilterArg(s); ok {
		head, rest = arg, after
	} else {
		var quoted string
		quoted, rest = extractQuotedValue(s)
		if head, err = strconv.Unquote(quoted); err != nil {
			return nil, "", fmt.Errorf("invalid line filter: %w", err)
		}
	}
	alts, rest, err := takeOrAlternatives(rest)
	if err != nil {
		return nil, "", err
	}
	values = []string{head}
	for _, alt := range alts {
		values = append(values, alt.Text)
	}
	return values, rest, nil
}

// splitStreamMatchers splits stream selector content like `app="x",level="error"`
// into individual matchers, respecting quotes.
func splitStreamMatchers(s string) []string {
	var matchers []string
	var quote rune
	start := 0
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		if c == '\\' && quote == '"' && i+1 < len(s) {
			i++
			continue
		}
		if (c == '"' || c == '`') && (quote == 0 || quote == c) {
			if quote == 0 {
				quote = c
			} else {
				quote = 0
			}
		}
		if c == ',' && quote == 0 {
			m := strings.TrimSpace(s[start:i])
			if m != "" {
				matchers = append(matchers, m)
			}
			start = i + 1
		}
	}
	m := strings.TrimSpace(s[start:])
	if m != "" {
		matchers = append(matchers, m)
	}
	return matchers
}

// matcherOperatorIndex finds op in matcher, skipping quoted and backquoted
// segments: a value may contain the operator text itself, as in
// {app="plain!~val"}, and splitting inside it would invent a field name. A
// field name may itself be backquoted, so the scan skips those too rather than
// stopping at the first quote.
func matcherOperatorIndex(matcher, op string) int {
	for i := 0; i < len(matcher); i++ {
		switch matcher[i] {
		case '\\':
			i++
			continue
		case '"', '`':
			quote := matcher[i]
			for i++; i < len(matcher); i++ {
				if matcher[i] == '\\' && quote == '"' {
					i++
					continue
				}
				if matcher[i] == quote {
					break
				}
			}
			continue
		}
		if strings.HasPrefix(matcher[i:], op) {
			return i
		}
	}
	return -1
}

// streamMatcherToFieldFilter converts a stream matcher like `level="error"`
// to a LogsQL field filter like `level:="error"`.
// Returns "" if the matcher can't be converted (shouldn't happen).
func streamMatcherToFieldFilter(matcher string, labelFn LabelTranslateFunc) string {
	matcher = strings.TrimSpace(matcher)

	for _, op := range translate.StreamMatcherOps {
		idx := matcherOperatorIndex(matcher, op.LogQL)
		if idx > 0 {
			label := translate.SanitizeFieldIdentifier(matcher[:idx])
			value := streamMatcherValue(matcher[idx+len(op.LogQL):], op.IsRe)
			if f := translate.MatcherFieldFilter(label, op.Negate, op.IsRe, value, labelFn); f != nil {
				return f.String()
			}
			return ""
		}
	}
	return ""
}

const (
	detectedLevelLabel = translate.DetectedLevelLabel
	// matchNoRows is a LogsQL filter no row passes: an empty in() list.
	matchNoRows = `_stream_id:in()`
)

// detectedLevelSelectorFilter handles a detected_level matcher in a stream
// selector when detected_level is not a configured stream field. Loki keeps
// detected_level in structured metadata, not in the index, so the matcher
// sees an absent label: a matcher that accepts the empty value selects every
// stream and needs no filter, any other selects no stream (matchNoRows).
// ok is false for matchers on other labels.
func detectedLevelSelectorFilter(matcher string) (filter string, ok bool, err error) {
	matcher = strings.TrimSpace(matcher)
	for _, op := range translate.StreamMatcherOps {
		idx := strings.Index(matcher, op.LogQL)
		if idx <= 0 {
			continue
		}
		if translate.SanitizeFieldIdentifier(matcher[:idx]) != detectedLevelLabel {
			return "", false, nil
		}
		value := streamMatcherValue(matcher[idx+len(op.LogQL):], false)
		matchesEmpty := value == ""
		if op.IsRe {
			re, reErr := regexp.Compile("^(?:" + value + ")$")
			if reErr != nil {
				return "", false, &ParseError{Msg: fmt.Sprintf("invalid regexp in stream matcher %s: %v", matcher, reErr), Pos: -1}
			}
			matchesEmpty = re.MatchString("")
		}
		if matchesEmpty != op.Negate {
			return "", true, nil
		}
		return matchNoRows, true, nil
	}
	return "", false, nil
}

// canUseStreamSelector returns true if a stream matcher can be converted to a VL native
// stream selector (faster index path) instead of a field filter.
// Only exact-match (=) on known _stream_fields qualifies.
// Regex (=~, !~) and negation (!=) always use field filters.
func canUseStreamSelector(matcher string, streamFields map[string]bool, labelFn LabelTranslateFunc) bool {
	matcher = strings.TrimSpace(matcher)
	// Only exact positive match qualifies for stream selectors
	// Reject regex and negation operators
	if strings.Contains(matcher, "!~") || strings.Contains(matcher, "=~") || strings.Contains(matcher, "!=") {
		return false
	}
	idx := strings.Index(matcher, "=")
	if idx <= 0 {
		return false
	}
	label := translate.SanitizeFieldIdentifier(matcher[:idx])
	if label == "" {
		return false
	}
	if label == "service_name" {
		return false
	}
	if labelFn != nil {
		label = translate.SanitizeFieldIdentifier(labelFn(label))
		if label == "" {
			return false
		}
	}
	return streamFields[label]
}

func streamMatcherValue(value string, isRegex bool) string {
	value = strings.TrimSpace(value)
	// Loki's raw literals retain carriage returns; strconv.Unquote applies
	// Go source's CR removal rule to backquoted strings instead.
	if len(value) >= 2 && value[0] == '`' && value[len(value)-1] == '`' {
		return value[1 : len(value)-1]
	}
	if decoded, err := strconv.Unquote(value); err == nil {
		return decoded
	}
	// A regexp written with an escape Go does not accept in a quoted string,
	// such as "a\d+", reaches Loki's parser as the raw text; keep it, minus the
	// quotes, so the pattern is what the user wrote either way.
	return strings.Trim(value, "\"`")
}

const (
	// unknownServiceNameValue is the service_name Loki assigns when no source
	// label exists (discover_service_name fallback).
	unknownServiceNameValue = translate.UnknownServiceNameValue
	// matchNoStreamsFilter is a LogsQL filter that matches no rows.
	matchNoStreamsFilter = "-*"
)

// serviceNameMatcherFilter translates a `service_name` stream matcher.
//
// Loki assigns service_name at ingest: an existing service_name, else the
// first non-empty label of discover_service_name, else unknown_service. The
// proxy has no ingest, so the value is derived on the read path from what
// VictoriaLogs stores: the first non-empty field of
// translate.SyntheticServiceNameFields (the same order the metric grouping coalesces),
// else unknown_service. The selector must select a row if and only if that
// derived value satisfies the matcher, so it is a pure filter with one OR
// branch per priority position; a branch requires every higher-priority
// field to be empty (`field:=""` also matches a missing field):
//
//	(f0:="v" OR (f0:="" f1:="v") OR ... OR (f0:="" ... fN:=""))
//
// The last branch selects unknown_service rows and is present only when
// unknown_service satisfies the matcher. Every row carries exactly one
// derived service_name, so a negated matcher is the negated positive filter.
// Plain field filters keep VictoriaLogs on its bloom-filter path and stay
// valid wherever the proxy strips pipes from the base query.
func serviceNameMatcherFilter(rawValue string, neg, isRegex bool) string {
	return translate.ServiceNameMatcherFilterExpr(streamMatcherValue(rawValue, false), neg, isRegex).String()
}

// serviceNameLabelFilter translates a `| service_name op "v"` stage. Loki
// simplifies label-matcher regexps before matching (pkg/logql/log/filter.go
// parseRegexpFilter with isLabel=true): a bare literal becomes an equality
// check, a literal wrapped in `.*` a substring check, and only what it cannot
// simplify stays an anchored match. serviceNameLabelFilterPattern reproduces
// that as one anchored pattern over the derived value.
func serviceNameLabelFilter(rawValue string, neg, isRegex bool) string {
	value := streamMatcherValue(rawValue, false)
	if isRegex {
		value = serviceNameLabelFilterPattern(value)
	}
	return serviceNameDerivedFilter(value, neg, isRegex)
}

// serviceNameDerivedFilter builds the filter for an already-effective value:
// an exact value, or a fully anchored regexp.
func serviceNameDerivedFilter(value string, neg, isRegex bool) string {
	return translate.ServiceNameDerivedFilterExpr(value, neg, isRegex).String()
}

// serviceNameLabelFilterPattern returns the anchored pattern that matches what
// Loki's label filter matches for the regexp value (lokiLabelFilterPattern).
func serviceNameLabelFilterPattern(value string) string {
	return translate.AnchorLabelFilterPattern(lokiLabelFilterPattern(value))
}

func isParserStage(translated string) bool {
	return translated == "| unpack_json" || translated == "| unpack_logfmt" ||
		strings.HasPrefix(translated, "| extract ") || strings.HasPrefix(translated, "| extract_regexp ")
}

func isFieldFilter(s string) bool { return translate.IsFieldFilter(s) }

func translateBareFilter(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Bare text after stream selector — treat as phrase filter
	return `"` + s + `"`
}

// looksLikeBareLabelMatcher reports whether s is an unbraced LogQL
// stream matcher such as `app="json-test"` or `level=~"warn|error"`. Such
// inputs must be rejected before falling into translateBareFilter, otherwise
// the translator emits malformed LogsQL like `"app="json-test""` that
// VictoriaLogs rejects.
//
// The check is intentionally conservative: it only matches strings that BEGIN
// with `<identifier> [op] "value"` (or backtick value). This avoids
// false-positives on text-with-equals or expressions that contain matchers
// nested inside parens (e.g. `sum(rate({app="x"}[5m])) by (app)` — those
// reach this code path only after the metric/binary extractors
// declined them, but the leading char would be `s` followed by `u`, not an
// identifier directly followed by `=`).
//
//nolint:gocyclo // character-by-character LogQL bare-label matcher heuristic with quoting, escapes, and operator detection; branching is inherent to the grammar probe.
func looksLikeBareLabelMatcher(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Must contain a quote and an `=`.
	if !strings.ContainsAny(s, "\"`") || !strings.Contains(s, "=") {
		return false
	}
	// Must START with an identifier (label name).
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '_' || c == '.' || c == '-' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') {
			i++
			continue
		}
		break
	}
	if i == 0 {
		// Doesn't start with an identifier (might start with `(`, `{`, `"`, etc.)
		return false
	}
	// Skip optional whitespace.
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return false
	}
	// Must be followed by an operator: `=`, `=~`, `!=`, `!~`.
	switch s[i] {
	case '=':
		i++
	case '!':
		i++
		if i >= len(s) || (s[i] != '=' && s[i] != '~') {
			return false
		}
		i++
	default:
		return false
	}
	// Tolerate `=~` after the leading `=`.
	if i < len(s) && s[i] == '~' {
		i++
	}
	// Skip optional whitespace.
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return false
	}
	// Must be followed by an opening quote (`"` or `` ` ``).
	return s[i] == '"' || s[i] == '`'
}

// extractIPFilterArg parses the argument from an ip("...") expression.
// Returns the unquoted argument, the rest of the query string after the closing ")",
// and ok=true. Returns ok=false if the expression cannot be parsed.
func extractIPFilterArg(s string) (arg, rest string, ok bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "ip(") {
		return "", s, false
	}
	s = strings.TrimSpace(s[3:]) // skip "ip("
	if len(s) == 0 || (s[0] != '"' && s[0] != '`') {
		return "", s, false
	}
	quoted, rest := extractQuotedValue(s)
	arg, err := strconv.Unquote(quoted)
	if err != nil {
		return "", s, false
	}
	if strings.HasPrefix(rest, ")") {
		rest = strings.TrimSpace(rest[1:])
	}
	return arg, rest, true
}

// parseJSONFieldAliases extracts alias→originalField mappings from a json pipeline
// stage that uses Loki's alias="field" syntax.
// E.g., "json http_code=\"status\", method=\"method\"" →
//
//	map["http_code"]="status", map["method"]="method"
//
// Bracket-notation paths like ["api"] are resolved to plain field names ("api").
func parseJSONFieldAliases(stage string) map[string]string {
	rest := strings.TrimSpace(strings.TrimPrefix(stage, "json "))
	aliases := make(map[string]string)
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		eqIdx := strings.IndexByte(part, '=')
		if eqIdx < 0 {
			continue
		}
		alias := strings.TrimSpace(part[:eqIdx])
		fieldPart := strings.TrimSpace(part[eqIdx+1:])
		if len(fieldPart) >= 2 && fieldPart[0] == '"' && fieldPart[len(fieldPart)-1] == '"' {
			orig := fieldPart[1 : len(fieldPart)-1]
			// Resolve bracket-notation JSON paths: ["fieldname"] → fieldname.
			// Without this, the raw path (including backslash-escaped quotes)
			// ends up in the VL filter as "[\fieldname\]" which VL cannot parse.
			orig = resolveJSONBracketPath(orig)
			if alias != "" && orig != "" {
				aliases[alias] = orig
			}
		}
	}
	return aliases
}

// resolveJSONBracketPath converts a Loki JSON path in bracket notation
// (pkg/logql/log/jsonexpr: `["a"]`, `["a"]["b"]`, `["a"].b`, the quotes
// escaped or not) to the field VictoriaLogs' unpack_json writes for it: the
// keys of a nested object joined with dots (`service.name`); a key holding a
// dot is written as it is (`["probe.verb"]`). A path through an array index,
// or with an empty key or one holding a bracket, quote or space, names no
// such field and resolves to "", which skips the alias. A path that does not start
// with a bracket is returned as is (already a plain or dotted name).
func resolveJSONBracketPath(orig string) string {
	orig = strings.TrimSpace(orig)
	if !strings.HasPrefix(orig, "[") {
		return orig // not bracket notation — already a plain field name or dot path
	}
	if strings.Contains(orig, `\"`) {
		// The quotes of the keys arrive escaped inside the stage's string.
		unescaped, err := strconv.Unquote(`"` + orig + `"`)
		if err != nil {
			return ""
		}
		orig = unescaped
	}
	var keys []string
	for i := 0; i < len(orig); {
		switch orig[i] {
		case '.':
			i++
		case '[':
			quoted, err := strconv.QuotedPrefix(orig[i+1:])
			if err != nil {
				return "" // an array index or an unquoted key
			}
			key, _ := strconv.Unquote(quoted)
			i += 1 + len(quoted)
			if i >= len(orig) || orig[i] != ']' {
				return ""
			}
			i++
			keys = append(keys, key)
		default:
			j := i
			for j < len(orig) && orig[j] != '.' && orig[j] != '[' {
				j++
			}
			keys = append(keys, orig[i:j])
			i = j
		}
	}
	for _, key := range keys {
		if key == "" || strings.ContainsAny(key, `[]"'\ `) {
			return ""
		}
	}
	return strings.Join(keys, ".")
}

// rewriteJSONAliasedFilter rewrites a label filter stage that references a
// json-aliased field name to use the original JSON field name, so that VL's
// unpack_json (which preserves original names) can match it.
// E.g., "http_code=\"200\"" with alias {"http_code":"status"} → "status=\"200\""
func rewriteJSONAliasedFilter(stage string, aliases map[string]string) string {
	if mayBeCompound(stage) {
		if expr, err := logqlpkg.ParseLabelFilterExpr(stage); err == nil && !expr.IsLeaf() {
			return expr.Render(func(leaf string) string { return rewriteJSONAliasedFilter(leaf, aliases) })
		}
	}
	for alias, orig := range aliases {
		if strings.HasPrefix(stage, alias) {
			after := stage[len(alias):]
			if len(after) > 0 && isLabelFilterOpByte(after[0]) {
				return orig + after
			}
		}
	}
	return stage
}

func isLabelFilterOpByte(c byte) bool {
	return c == '=' || c == '!' || c == '<' || c == '>' || c == '~'
}
