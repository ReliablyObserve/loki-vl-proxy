package proxy

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"
)

var (
	// drilldownFieldFilterRE matches field-not-empty existence filters in a base query.
	// Three formats: Loki-style "| filter field != \"\"" and VL-style with either
	// unquoted "| filter field:!\"\"" or quoted "| filter \"field.with.dots\":!\"\"".
	// The translator quotes dotted/OTel field names ("k8s.pod.name", "service.name")
	// because VL requires quoting for identifiers with special characters.
	drilldownFieldFilterRE = regexp.MustCompile(`\|\s*filter\s+(?:"([^"]+)"|([\w.]+))(?:\s*!=\s*""|:!"")`)
	// drilldownParserPipeRE matches parser-stage pipes to strip before building
	// fused conditional-stats queries. VL's field:* existence check works on
	// pre-indexed columns WITHOUT | json / | logfmt. Including them returns empty.
	drilldownParserPipeRE = regexp.MustCompile(`\|\s*(?:unpack_json|unpack_logfmt|json|logfmt)\b[^|]*`)
	// drilldownDeletePipeRE matches | delete pipes that are safe to drop when
	// rewriting to a conditional-count fast path (column-indexed fields don't
	// need __error__ cleanup before a count() if aggregation).
	drilldownDeletePipeRE = regexp.MustCompile(`\|\s*delete\b[^|]*`)
)

// extractStreamSelectorOnly returns the leading filter expression of a VL
// LogsQL query, stripping any pipe stages (parser, drop, filter, etc.) that
// follow. Used to feed VL's /select/logsql/hits endpoint, which accepts only
// a filter expression — pipe chains fail parse-time validation (VL rejects
// unknown pipe forms like `| trace_id!=""` before ignore_pipes=1 strips them).
//
// Handles both forms the proxy's translator produces:
//   - Bracketed Loki-style: `{namespace="prod"} | json | trace_id!=""`
//     → `{namespace="prod"}`
//   - Bare VL-native:        `namespace:="prod" | unpack_json | trace_id:!""`
//     → `namespace:="prod"`
//
// Returns "" only when the query has no recognisable selector (extremely rare;
// would imply a malformed translator output).
func extractStreamSelectorOnly(query string) string {
	q := strings.TrimSpace(query)
	if q == "" {
		return ""
	}
	// Find the first pipe stage boundary. Everything before it is the filter
	// expression (which VL accepts as a `query` parameter without pipes).
	if idx := strings.Index(q, "|"); idx > 0 {
		return strings.TrimSpace(q[:idx])
	}
	// No pipes — the whole query is the selector.
	return q
}

// allFiltersAreExistenceChecks returns true when every | filter clause in the
// VL base query is a pure field-not-empty existence check — Loki: field!="" or
// VL translated: field:!"". These are safe to evaluate without a preceding
// | unpack_json / | unpack_logfmt when the fields are stored in VL's column
// index (OTel / explicitly indexed).
// Used to decide whether stripping parser stages is safe for the stats fast path.
func allFiltersAreExistenceChecks(vlQuery string) bool {
	// Remove all existence-filter clauses; if any | filter remains, it's a
	// value-comparison filter that requires the parser stage to have run.
	return !strings.Contains(drilldownFieldFilterRE.ReplaceAllString(vlQuery, ""), "| filter ")
}

// fieldHasExistenceFilter returns true if field appears as an existence check
// (field:!"" or field!="") in baseQuery — either as a | filter pipe stage or
// directly in the stream selector (stream-label existence checks are written
// without the | filter prefix, e.g. env:="production" level:!"").
//
// Also matches the quoted form ("field.with.dots":!""), which the translator
// generates for OTel attributes and other identifiers containing characters
// that need quoting in VL LogsQL.
func fieldHasExistenceFilter(baseQuery, field string) bool {
	// Check common prefixes: space before field (stream selector or after filter keyword)
	// and comma (multi-label stream selectors). Use string operations to avoid per-call
	// regexp compilation; field names are already validated as [\w.]+ by the parser.
	quoted := `"` + field + `"`
	for _, pre := range []string{" ", ","} {
		if strings.Contains(baseQuery, pre+field+`:!""`) ||
			strings.Contains(baseQuery, pre+field+`!=""`) ||
			strings.Contains(baseQuery, pre+quoted+`:!""`) ||
			strings.Contains(baseQuery, pre+quoted+`!=""`) {
			return true
		}
	}
	// Check at the very start of the query (stream selector starts at position 0).
	return strings.HasPrefix(baseQuery, field+`:!""`) ||
		strings.HasPrefix(baseQuery, field+`!=""`) ||
		strings.HasPrefix(baseQuery, quoted+`:!""`) ||
		strings.HasPrefix(baseQuery, quoted+`!=""`)
}

// detectDrilldownSingleFieldWithParser is like detectDrilldownSingleField but
// accepts queries that contain parser stages (| unpack_json, | unpack_logfmt, etc.).
// Used by proxyStatsQueryRange to identify Drilldown single-field count queries
// for fields that are NOT column-indexed (JSON/logfmt-embedded fields such as
// trace_id, span_id, level) and must be queried with the parser stages preserved.
//
// cleanBase retains parser stages but has existence filters and delete pipes stripped.
// Returns ("", "", false) when the query does not match the Drilldown single-field
// count pattern.
func detectDrilldownSingleFieldWithParser(effectiveQuery string) (cleanBase, field string, ok bool) {
	spec, specOK := parseStatsCompatSpec(effectiveQuery)
	if !specOK || len(spec.GroupBy) != 1 || spec.Func != "count" {
		return "", "", false
	}
	// Unlike detectDrilldownSingleField, we do NOT reject queries with parser stages —
	// the caller must preserve the parser in the query it sends to VL.
	f := spec.GroupBy[0]
	if !allFiltersAreExistenceChecks(spec.BaseQuery) {
		return "", "", false
	}
	if !fieldHasExistenceFilter(spec.BaseQuery, f) {
		return "", "", false
	}
	// Strip existence filters and delete pipes; preserve parser stages.
	base := drilldownFieldFilterRE.ReplaceAllString(spec.BaseQuery, "")
	base = drilldownDeletePipeRE.ReplaceAllString(base, "")
	return strings.TrimSpace(base), f, true
}

// detectDrilldownSingleField returns the pure stream-selector base (no existence
// filters, no delete pipes) and the single grouped field when effectiveQuery is a
// Drilldown tumbling-window single-field count query whose filters are all pure
// existence checks and whose base has no parser stages. Returns ("", "", false)
// when the query does not match the pattern.
//
// Used to identify queries that are safe to fall back to count() if (field:*)
// when stats by (field) count() exceeds the per-request 16 MB response cap.
func detectDrilldownSingleField(effectiveQuery string) (cleanBase, field string, ok bool) {
	spec, specOK := parseStatsCompatSpec(effectiveQuery)
	if !specOK || len(spec.GroupBy) != 1 || spec.Func != "count" {
		return "", "", false
	}
	// count() if (field:*) works on column-indexed fields only; reject queries
	// that still require a parser stage (unpack_json / extract).
	if queryUsesParserStages(spec.BaseQuery) {
		return "", "", false
	}
	f := spec.GroupBy[0]
	// All remaining | filter clauses must be pure existence checks — value
	// comparisons (e.g. | filter status:>="400") require the parser to have run.
	// Stream-selector-style filters (e.g. level:!"" in the selector) are also
	// existence checks; allFiltersAreExistenceChecks handles only pipe-style ones,
	// but the stream-selector form never produces | filter so it passes that check.
	if !allFiltersAreExistenceChecks(spec.BaseQuery) {
		return "", "", false
	}
	// Require the grouped field to have some existence check — either a pipe-style
	// | filter f:!"" or a stream-selector-style f:!"" (written without | filter).
	// Without this guard a plain count-by-field query (no existence intent) would
	// be incorrectly routed through the drilldown fast path.
	if !fieldHasExistenceFilter(spec.BaseQuery, f) {
		return "", "", false
	}
	// Strip pipe-style existence filters and delete pipes. Stream-selector
	// existence filters (level:!"") remain in cleanBase — they are valid VL
	// selector predicates that the batcher query passes through unchanged.
	base := drilldownFieldFilterRE.ReplaceAllString(spec.BaseQuery, "")
	base = drilldownDeletePipeRE.ReplaceAllString(base, "")
	return strings.TrimSpace(base), f, true
}

// extractCommonBase strips the per-field filter and parser-stage pipes from a
// Drilldown Fields baseQuery, returning the pure stream selector and field name.
// Returns ("", "", false) if baseQuery does not match the Drilldown presence pattern.
//
// The regex captures the field name in group 1 (when quoted) OR group 2 (when
// unquoted) — exactly one of the two alternatives matches per filter clause.
func extractCommonBase(baseQuery string) (base, field string, ok bool) {
	m := drilldownFieldFilterRE.FindStringSubmatchIndex(baseQuery)
	if m == nil {
		return "", "", false
	}
	// Group 1 (m[2]:m[3]) is the quoted form, group 2 (m[4]:m[5]) is unquoted.
	// Exactly one matches; the other has index -1.
	switch {
	case m[2] >= 0:
		field = baseQuery[m[2]:m[3]]
	case m[4] >= 0:
		field = baseQuery[m[4]:m[5]]
	default:
		return "", "", false
	}
	prefix := baseQuery[:m[0]]
	prefix = drilldownParserPipeRE.ReplaceAllString(prefix, "")
	base = strings.TrimSpace(prefix)
	return base, field, true
}

type burstKey struct {
	orgID    string
	base     string
	startSec int64
	endSec   int64
	stepNs   int64
}

type fieldResult struct {
	series map[string]manualSeriesSamples
	err    error
}

type burstGroup struct {
	fields []string
	chans  []chan fieldResult
}

// DrilldownBurstCoalescer groups concurrent per-field count_over_time queries from
// Grafana Drilldown Fields into a single fused VL conditional-stats call, reducing
// ~30 VL round-trips to 1 per page refresh.
type DrilldownBurstCoalescer struct {
	mu        sync.Mutex
	pending   map[burstKey]*burstGroup
	window    time.Duration
	maxFields int
}

func newDrilldownBurstCoalescer(windowMs, maxFields int) *DrilldownBurstCoalescer {
	if windowMs <= 0 {
		windowMs = 50
	}
	if maxFields <= 0 {
		maxFields = 30
	}
	return &DrilldownBurstCoalescer{
		pending:   make(map[burstKey]*burstGroup),
		window:    time.Duration(windowMs) * time.Millisecond,
		maxFields: maxFields,
	}
}

// Submit registers field in the burst group for key, waits for the window to
// close, and returns the per-field result from the fused fireFn call.
func (c *DrilldownBurstCoalescer) Submit(
	ctx context.Context,
	key burstKey,
	field string,
	fireFn func(ctx context.Context, fields []string) (map[string]fieldResult, error),
) (map[string]manualSeriesSamples, error) {
	ch := make(chan fieldResult, 1)

	c.mu.Lock()
	g := c.pending[key]
	if g != nil && len(g.fields) >= c.maxFields {
		delete(c.pending, key)
		g = nil
	}
	if g == nil {
		g = &burstGroup{
			fields: []string{field},
			chans:  []chan fieldResult{ch},
		}
		c.pending[key] = g
		window := c.window
		time.AfterFunc(window, func() { c.fire(key, g, fireFn) })
	} else {
		g.fields = append(g.fields, field)
		g.chans = append(g.chans, ch)
	}
	c.mu.Unlock()

	select {
	case res := <-ch:
		return res.series, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *DrilldownBurstCoalescer) fire(
	key burstKey,
	g *burstGroup,
	fireFn func(ctx context.Context, fields []string) (map[string]fieldResult, error),
) {
	c.mu.Lock()
	if c.pending[key] == g {
		delete(c.pending, key)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := fireFn(ctx, g.fields)
	if err != nil {
		err = fmt.Errorf("burst coalescer fused query: %w", err)
	}
	for i, f := range g.fields {
		var r fieldResult
		if err != nil {
			r.err = err
		} else if res, ok := results[f]; ok {
			r = res
		} else {
			r.err = fmt.Errorf("burst coalescer: no result for field %q", f)
		}
		g.chans[i] <- r
	}
}

// isLogsQLIdentChar reports whether c is a valid bare LogsQL identifier character
// (ASCII letter, digit, or underscore).
func isLogsQLIdentChar(c rune) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_'
}

// quoteLogsQLIdent wraps a field name in backticks if it contains characters
// that are not safe as bare LogsQL identifiers (letters, digits, underscore).
func quoteLogsQLIdent(name string) string {
	for _, c := range name {
		if !isLogsQLIdentChar(c) {
			return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
		}
	}
	return name
}

// fusedFieldHits returns a fireFn for DrilldownBurstCoalescer.Submit.
// It fires one VL stats_query_range with count() if (field:*) conditional
// aggregations for all fields, parsing each alias's time series into fieldResult.
//
// VL's count() if (field:*) works on pre-indexed columns without | json / | logfmt.
// extractCommonBase must have stripped parser pipes from commonBase already.
func (p *Proxy) fusedFieldHits(
	orgID, commonBase string,
	start, end time.Time,
	step time.Duration,
) func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
	return func(ctx context.Context, fields []string) (map[string]fieldResult, error) {
		if len(fields) == 0 {
			return map[string]fieldResult{}, nil
		}

		aliasToField := make(map[string]string, len(fields))
		var sb strings.Builder
		for i, f := range fields {
			alias := fmt.Sprintf("_f%d", i)
			aliasToField[alias] = f
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "count() if (%s:*) as %s", quoteLogsQLIdent(f), alias)
		}
		fusedQuery := commonBase + " | stats " + sb.String()

		params := url.Values{}
		params.Set("query", fusedQuery)
		params.Set("start", strconv.FormatInt(start.Unix(), 10))
		params.Set("end", strconv.FormatInt(end.Unix(), 10))
		params.Set("step", strconv.FormatFloat(step.Seconds(), 'f', 0, 64)+"s")

		// Acquire the same concurrency slot used by individual stats_query_range calls
		// so burst-fused calls don't bypass the back-pressure contract.
		if sem := p.statsQueryRangeSem; sem != nil {
			select {
			case <-sem:
				defer func() { sem <- struct{}{} }()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		callCtx := context.WithValue(ctx, orgIDKey, orgID)
		resp, err := p.vlPost(callCtx, "/select/logsql/stats_query_range", params)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
			return nil, p.redactedBackendStatusError("fused stats_query_range", resp.StatusCode, body)
		}

		const maxFusedResponseBytes = 64 << 20
		body, err := readBodyLimited(resp.Body, maxFusedResponseBytes)
		if err != nil {
			return nil, err
		}

		v, parseErr := fj.ParseBytes(body)
		if parseErr != nil {
			return nil, fmt.Errorf("parse fused stats_query_range: %w", parseErr)
		}
		if status := string(v.GetStringBytes("status")); status != "success" {
			return nil, fmt.Errorf("fused stats_query_range non-success: %s", status)
		}

		out := make(map[string]fieldResult, len(fields))
		for _, res := range v.GetArray("data", "result") {
			field, ok := aliasToField[string(res.GetStringBytes("metric", "__name__"))]
			if !ok {
				continue
			}
			rawValues := res.GetArray("values")
			samples := make([]rangeMetricSample, 0, len(rawValues))
			for _, pair := range rawValues {
				arr := pair.GetArray()
				if len(arr) < 2 {
					continue
				}
				tsUnix, tsErr := arr[0].Int64()
				if tsErr != nil {
					continue
				}
				val, parseFloatErr := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64)
				if parseFloatErr != nil {
					continue
				}
				samples = append(samples, rangeMetricSample{
					ts:    tsUnix * int64(time.Second), // nanoseconds, exact integer arithmetic
					value: val,
				})
			}
			out[field] = fieldResult{
				series: map[string]manualSeriesSamples{
					"": {Metric: map[string]string{}, Samples: samples},
				},
			}
		}
		return out, nil
	}
}
