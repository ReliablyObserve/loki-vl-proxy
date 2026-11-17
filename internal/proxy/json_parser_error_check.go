package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// JSON parser errors under any metric function.
//
// Loki fails a metric query when a sample that reaches a range vector carries
// __error__: a line its JSON parser rejects, under a range aggregation whose
// parser runs (some label is required: Loki skips the parser only under a sum
// without grouping of rate, count_over_time or the bytes functions) and whose
// pipeline neither filters on __error__ nor drops it. Every function above the
// range aggregation keeps the failure: sort, topk, label_replace, binary
// operations, nested aggregations, offsets (recorded Loki answers:
// internal/querycorpus parser_error_wrappers_loki_diff_test.go). The ordered
// JSON evaluator answers the shapes it owns itself; this check covers the
// metrics the other routes answer.
//
// The decision is taken on the parsed query (jsonErrorProbes), and the
// detection rides on the metric's own VictoriaLogs queries, as the unwrap
// conversion check does, so a query whose lines all parse sends no other
// request: a stats query or a raw-row read that unpacks JSON marks the lines
// VictoriaLogs cannot vouch for (jsonErrorFlagPipes) in the same pass. A
// stats answer groups them apart, and the groups are taken out before any
// caller reads the answer; a raw-row read drops the mark from the row it
// counts. Only after such a mark, one `| limit` lookup per probe reads the
// stored rows of the marked span and evaluates them with the evaluator's
// Loki-faithful parser, and only a row Loki fails on answers Loki's error. A
// stats answer whose marks Loki does not fail on (a line Loki's parser accepts
// that the mark cannot vouch for, such as `: nan` inside a string) is fetched
// again without the mark, so its values are the metric's own.

// jsonErrorField marks a raw row of a rewritten VictoriaLogs read: "1" on a
// line VictoriaLogs cannot parse as one JSON object, absent otherwise. A stats
// query groups by the sentinel (jsonValidField) instead: its group is "1" for
// the lines VictoriaLogs parsed, empty for the marked ones.
const jsonErrorField = "__lvp_perr"

// jsonErrorProbe is one range aggregation whose JSON parser errors reach the
// result.
type jsonErrorProbe struct {
	// plan is the range aggregation as the ordered JSON evaluator compiles it,
	// under its sum when the sum pushes its grouping down (Loki's parser hints).
	plan           *orderedJSONMetricPlan
	window, offset time.Duration
}

// jsonErrorProbes returns the range aggregations in expr whose JSON parser
// errors reach the result, in evaluation order.
func jsonErrorProbes(expr logqlpkg.Expr) []jsonErrorProbe {
	var out []jsonErrorProbe
	var walk func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation, depth int)
	walk = func(e logqlpkg.Expr, parent *logqlpkg.VectorAggregation, depth int) {
		if depth > maxJSONErrorProbeDepth || len(out) >= maxJSONErrorProbes {
			return
		}
		switch n := e.(type) {
		case *logqlpkg.VectorAggregation:
			walk(n.Inner, n, depth+1)
		case *logqlpkg.BinOpExpr:
			walk(n.Left, nil, depth+1)
			walk(n.Right, nil, depth+1)
		case *logqlpkg.OpaqueMetricExpr:
			if n.Typed && n.Inner != nil {
				walk(n.Inner, nil, depth+1) // label_replace keeps its sample expression's errors
			}
		case *logqlpkg.RangeAggregation:
			if probe, ok := jsonErrorProbeFor(n, parent); ok {
				out = append(out, probe)
			}
		}
	}
	walk(expr, nil, 0)
	return out
}

// maxJSONErrorProbes and maxJSONErrorProbeDepth bound the walk over a crafted
// query: the parser bounds the query size, these the work per request.
const (
	maxJSONErrorProbes     = 16
	maxJSONErrorProbeDepth = 64
)

// jsonErrorProbeFor describes one range aggregation; ok is false when its
// samples never carry a JSON parser error: no JSON parser, a pipeline the
// evaluator does not compile, no required label (Loki does not parse), a
// filter on __error__ (Loki keeps the error as a label) or a drop of it after
// the parser.
func jsonErrorProbeFor(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation) (jsonErrorProbe, bool) {
	if ra.HasOffset() && ra.OffsetDuration <= 0 || !hasJSONParser(ra) {
		return jsonErrorProbe{}, false
	}
	leaf := *ra
	leaf.Offset, leaf.OffsetDuration = "", 0
	var expr logqlpkg.Expr = &leaf
	if parent != nil && parent.Op == logqlpkg.VectorSum {
		expr = &logqlpkg.VectorAggregation{Op: logqlpkg.VectorSum, Grouping: parent.Grouping, Inner: &leaf}
	}
	plan, ok := compileOrderedJSONMetricExpr(expr)
	if !ok || plan.parser != "json" || plan.noLabels || plan.preserveError || plan.dropsParserErrors() {
		return jsonErrorProbe{}, false
	}
	return jsonErrorProbe{plan: plan, window: ra.RangeDuration, offset: ra.OffsetDuration}, true
}

// hasJSONParser reports whether the range aggregation's pipeline holds a JSON
// parser stage, in any spelling the parser reads (`| json`, `| JSON`): the
// tree is read, so a query without one costs no compile.
func hasJSONParser(ra *logqlpkg.RangeAggregation) bool {
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok {
		return false
	}
	for _, stage := range lq.Pipeline {
		if p, isParser := stage.(*logqlpkg.ParserStage); isParser && p.Type == logqlpkg.ParserJSON {
			return true
		}
	}
	return false
}

// dropsParserErrors reports whether the pipeline intercepts the parser
// errors: a stage after the last parser drops __error__ for every line, or a
// label filter on __error__ follows a parser (Loki then keeps the error as a
// label, or filters the failing lines out).
func (plan *orderedJSONMetricPlan) dropsParserErrors() bool {
	dropped, parsed := false, false
	for _, stage := range plan.stages {
		switch {
		case stage.parser:
			dropped, parsed = false, true
		case stage.filter != nil && stage.filter.Field == "__error__" && parsed:
			return true
		case !stage.keep && len(stage.match) == 0 && stage.filter == nil && stage.line == nil && stage.fields["__error__"]:
			dropped = true
		}
	}
	return dropped
}

// jsonErrorCheck is the JSON parser error detection of one Loki request.
type jsonErrorCheck struct {
	probes []jsonErrorProbe

	mu sync.Mutex
	// rawHits are the marked rows the raw-row evaluators read, confirmed when
	// the answer is written; a stats answer confirms its own marks when it
	// arrives (confirmAnswer), each from its own hits, so the lookups a request
	// sends do not depend on the order its concurrent reads finish in.
	rawHits []jsonErrorHit
	found   []jsonFoundError
	// confirmedRaw counts the raw hits already looked up: the check decides
	// again on the hits that came later (an operand read after another one
	// recorded its status), as the unwrap check does.
	confirmedRaw int
	// looked holds the answer of each lookup the request sent (by lookup text
	// and span): a stats answer read again over the same data (a level
	// derivation's read after the first) confirms its marks without sending
	// the same lookup again.
	looked map[string]jsonLookedSpan
	err    *orderedJSONPipelineError
	errTS  int64

	// lookups counts the request's lookups against jsonMaxLookups, every stats
	// answer and the raw rows together. failure is the first lookup
	// VictoriaLogs did not answer: the request answers it as a failed backend,
	// since Loki's answer is unknown. unsure reports marks the budget left
	// unconfirmed: the answer is sent, and not kept in the response cache.
	lookups atomic.Int32
	failure error
	unsure  bool
}

// jsonLookedSpan is the answer of one lookup: the failing row it found, if any.
type jsonLookedSpan struct {
	found jsonFoundError
	ok    bool
}

// jsonFoundError is a confirmed failing row: Loki's error and the row's time.
type jsonFoundError struct {
	ts  int64
	err *orderedJSONPipelineError
}

// jsonErrorHit is a span of data time holding a marked line.
type jsonErrorHit struct{ from, to time.Time }

// jsonMaxHits bounds the spans a request records for confirmation.
const jsonMaxHits = 256

func (j *jsonErrorCheck) addRawHit(h jsonErrorHit) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.rawHits) < jsonMaxHits {
		j.rawHits = append(j.rawHits, h)
	}
}

func (j *jsonErrorCheck) addFound(f jsonFoundError) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.found = append(j.found, f)
}

// jsonErrorCheckFrom is the request's JSON parser error check, or nil.
func jsonErrorCheckFrom(ctx context.Context) *jsonErrorCheck {
	if c := unwrapCheckFrom(ctx); c != nil {
		return c.json
	}
	return nil
}

type jsonErrorMarkOffKey struct{}

// jsonErrorStartedKey marks a request whose JSON parser errors are checked:
// the operand requests of a binary operation are read by the outer check.
type jsonErrorStartedKey struct{}

// startJSONParserErrorCheck attaches the detection of logql's JSON parser
// errors to the request, after the ordered JSON evaluator declined it. The
// probes join the request's unwrap conversion check when it has one (its
// writer then decides for both); otherwise the returned writer, non-nil, holds
// the metric's answer until the detection decided.
func (p *Proxy) startJSONParserErrorCheck(w http.ResponseWriter, r *http.Request, logql string, isRange bool) (*http.Request, *unwrapLookupWriter) {
	if r.Context().Value(jsonErrorStartedKey{}) != nil {
		return r, nil
	}
	expr, ok := sharedLogQLExpr(logql)
	if !ok {
		return r, nil
	}
	probes := jsonErrorProbes(expr)
	if len(probes) == 0 {
		return r, nil
	}
	ctx := context.WithValue(r.Context(), jsonErrorStartedKey{}, true)
	if c := unwrapCheckFrom(ctx); c != nil {
		c.json = &jsonErrorCheck{probes: probes}
		if c.writer != nil {
			return r.WithContext(ctx), nil
		}
		// A check that only counts the unwrap nan values has no writer yet.
		l := &unwrapLookupWriter{p: p, ctx: ctx, parent: r.Context(), check: c, out: w, header: http.Header{}, query: logql}
		c.writer = l
		return r.WithContext(ctx), l
	}
	start, end, step, err := orderedJSONMetricTimes(r, isRange)
	if err != nil {
		return r, nil
	}
	if _, binary := expr.(*logqlpkg.BinOpExpr); binary && isRange && step > 0 {
		// Binary operands are evaluated on the step-aligned axis (alignBinaryRangeRequest).
		start, end = start.Add(-time.Duration(start.UnixNano()%int64(step))), end.Add(-time.Duration(end.UnixNano()%int64(step)))
	}
	c := &unwrapCheck{instant: !isRange, start: start, end: end, step: step, query: logql, json: &jsonErrorCheck{probes: probes}}
	ctx = context.WithValue(ctx, unwrapCheckKey{}, c)
	l := &unwrapLookupWriter{p: p, ctx: ctx, parent: r.Context(), check: c, out: w, header: http.Header{}, query: logql}
	c.writer = l
	return r.WithContext(ctx), l
}

// jsonErrorMarkSpec marks, around a query's own unpack_json, the lines
// VictoriaLogs cannot parse as one JSON object (logsql.JSONMark): a line that
// does not open with a brace and a broken object. Loki's parser rejects each
// of them, unless the line is one VictoriaLogs rejects and Loki accepts
// (leading whitespace on VictoriaLogs v1.49, `{ }`): the lookup tells them
// apart. A line both parsers read as JSON while Loki's rejects it (a value
// `NaN`, `.5` or `+1`, an invalid key escape: jsonLokiRejectHazard) is not
// marked: that filter costs VictoriaLogs about 9 ms to compile on every query
// and a regular expression per line, which is the success path's cost, and the
// lines are not JSON a logger writes (semantics/json-parser-error-under-wrappers).
var jsonErrorMarkSpec = logsql.JSONMark{Sentinel: jsonValidField, Flag: jsonErrorField}

// jsonMarkedTexts remembers the marked text of a query, by the read it is
// for: a metric sends the same few texts on every request, and the mark is a
// function of the text alone (no tenant, no time), so one parse serves them
// all. A full memo starts over.
var jsonMarkedTexts = &jsonMarkMemo{entries: map[string]jsonMarkedText{}, max: 4096}

type jsonMarkedText struct {
	text string
	ok   bool
}

type jsonMarkMemo struct {
	mu      sync.Mutex
	entries map[string]jsonMarkedText
	max     int
}

// mark is mark(query) through the memo; rows tells the raw-row form.
func (m *jsonMarkMemo) mark(query string, rows bool, mark func(string) (string, bool)) (string, bool) {
	if len(query) > validationCacheMaxQueryBytes {
		return mark(query) // a long text is marked each time: the memo stays small in bytes too
	}
	key := "s" + query
	if rows {
		key = "r" + query
	}
	m.mu.Lock()
	e, hit := m.entries[key]
	m.mu.Unlock()
	if hit {
		return e.text, e.ok
	}
	text, ok := mark(query)
	m.mu.Lock()
	if len(m.entries) >= m.max {
		clear(m.entries)
	}
	m.entries[key] = jsonMarkedText{text: text, ok: ok}
	m.mu.Unlock()
	return text, ok
}

// jsonCountingStatsQuery is the hook of the VictoriaLogs client for stats
// queries: the query to send and a function that takes the marked groups out
// of a 200 answer and returns the spans they cover.
func (j *jsonErrorCheck) jsonCountingStatsQuery(ctx context.Context, path string, params url.Values) (url.Values, func([]byte) ([]byte, []jsonErrorHit, error), bool) {
	if j == nil || (path != "/select/logsql/stats_query_range" && path != "/select/logsql/stats_query") {
		return params, nil, false
	}
	if off, _ := ctx.Value(jsonErrorMarkOffKey{}).(bool); off {
		return params, nil, false
	}
	rewritten, ok := jsonMarkedTexts.mark(params.Get("query"), false, jsonErrorMarkSpec.MarkStats)
	if !ok {
		return params, nil, false
	}
	sent := url.Values{}
	for k, v := range params {
		sent[k] = append([]string(nil), v...)
	}
	sent.Set("query", rewritten)
	step := parseVLStepParam(params.Get("step"))
	ranged := path == "/select/logsql/stats_query_range"
	strip := func(body []byte) ([]byte, []jsonErrorHit, error) {
		out, times, err := stripJSONErrorGroups(body)
		if err != nil {
			return nil, nil, err
		}
		hits := make([]jsonErrorHit, 0, len(times))
		for _, ts := range times {
			t := time.Unix(0, int64(ts*float64(time.Second)))
			h := jsonErrorHit{}
			if ranged && step > 0 {
				// A bucket labelled T holds [T, T+step) or, with the offset
				// argument, (T, T+step]: read a step on either side.
				h.from, h.to = t.Add(-step), t.Add(2*step)
			}
			if len(hits) < jsonMaxHits {
				hits = append(hits, h)
			}
		}
		return out, hits, nil
	}
	return sent, strip, true
}

// stripJSONErrorGroups removes the marked groups from a stats answer (the
// groups whose sentinel is empty) and the sentinel from the others; times are
// the timestamps of the marked groups' points.
func stripJSONErrorGroups(body []byte) ([]byte, []float64, error) {
	marked := []byte(`"` + jsonValidField + `":""`)
	kept := []byte(`"` + jsonValidField + `":"1"`)
	if !bytes.Contains(body, marked) {
		return dropKeptFlag(body, kept), nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, nil, err
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &data); err != nil {
		return nil, nil, err
	}
	var result []map[string]json.RawMessage
	if raw, ok := data["result"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, nil, err
		}
	}
	var times []float64
	out := result[:0]
	for _, series := range result {
		var metric map[string]string
		if err := json.Unmarshal(series["metric"], &metric); err != nil {
			return nil, nil, err
		}
		sentinel, grouped := metric[jsonValidField]
		if grouped && sentinel == "" {
			var rows [][]json.RawMessage
			if raw, ok := series["values"]; ok {
				_ = json.Unmarshal(raw, &rows)
			}
			if raw, ok := series["value"]; ok {
				var row []json.RawMessage
				if json.Unmarshal(raw, &row) == nil {
					rows = append(rows, row)
				}
			}
			for _, row := range rows {
				var ts float64
				if len(row) == 2 && json.Unmarshal(row[0], &ts) == nil {
					times = append(times, ts)
				}
			}
			continue
		}
		if grouped {
			delete(metric, jsonValidField)
			encoded, err := json.Marshal(metric)
			if err != nil {
				return nil, nil, err
			}
			series["metric"] = encoded
		}
		out = append(out, series)
	}
	encodedResult, err := json.Marshal(out)
	if err != nil {
		return nil, nil, err
	}
	data["result"] = encodedResult
	if top["data"], err = json.Marshal(data); err != nil {
		return nil, nil, err
	}
	encoded, err := json.Marshal(top)
	return encoded, times, err
}

// rawRowsQuery is the query a raw-row metric evaluator sends for base: base
// with the lines its unpack_json reads marked, or base when there is nothing
// to mark.
func (j *jsonErrorCheck) rawRowsQuery(base string) string {
	if j == nil {
		return base
	}
	marked, _ := jsonMarkedTexts.mark(base, true, jsonErrorMarkSpec.MarkRows)
	return marked
}

// takeRawRowFJ drops the mark from a raw row and records the row's time when
// it carries one.
func (j *jsonErrorCheck) takeRawRowFJ(v *fj.Value, ts int64) {
	if j == nil || v.Get(jsonErrorField) == nil {
		return
	}
	v.Del(jsonErrorField)
	t := time.Unix(0, ts)
	j.addRawHit(jsonErrorHit{from: t, to: t.Add(time.Nanosecond)})
}

// takeRawRowEntry is takeRawRowFJ for a decoded row.
func (j *jsonErrorCheck) takeRawRowEntry(entry map[string]interface{}, ts int64) {
	if j == nil {
		return
	}
	if _, ok := entry[jsonErrorField]; !ok {
		return
	}
	delete(entry, jsonErrorField)
	t := time.Unix(0, ts)
	j.addRawHit(jsonErrorHit{from: t, to: t.Add(time.Nanosecond)})
}

// confirmAnswer confirms the marks of one stats answer: whether a row Loki
// fails on is among them (the error is kept for the request's decision).
func (j *jsonErrorCheck) confirmAnswer(ctx context.Context, p *Proxy, c *unwrapCheck, hits []jsonErrorHit) bool {
	f, ok := j.confirmHits(ctx, p, c, hits)
	if ok {
		j.addFound(f)
	}
	return ok
}

// confirm returns Loki's error for the request: the earliest failing row among
// the stats answers' confirmed marks and the raw rows' marks, or nil. It runs
// whenever the writer decides before the answer went out, on the raw hits not
// looked up yet; once it found an error, that error stands.
func (j *jsonErrorCheck) confirm(ctx context.Context, p *Proxy, c *unwrapCheck) *orderedJSONPipelineError {
	j.mu.Lock()
	if j.err != nil {
		defer j.mu.Unlock()
		return j.err
	}
	raw := append([]jsonErrorHit(nil), j.rawHits[j.confirmedRaw:]...)
	j.confirmedRaw = len(j.rawHits)
	j.mu.Unlock()
	if len(raw) > 0 {
		if f, ok := j.confirmHits(ctx, p, c, raw); ok {
			j.addFound(f)
		}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, f := range j.found {
		if j.err == nil || f.ts < j.errTS || f.ts == j.errTS && f.err.message < j.err.message {
			j.err, j.errTS = f.err, f.ts
		}
	}
	return j.err
}

// confirmHits returns the earliest failing row the lookups of hits find, in
// the order of the hits' spans.
func (j *jsonErrorCheck) confirmHits(ctx context.Context, p *Proxy, c *unwrapCheck, hits []jsonErrorHit) (jsonFoundError, bool) {
	hits = append([]jsonErrorHit(nil), hits...)
	sort.SliceStable(hits, func(a, b int) bool { return hits[a].from.Before(hits[b].from) })
	seen := map[string]bool{}
	for _, h := range hits {
		for i := range j.probes {
			probe := &j.probes[i]
			from, to, _ := c.jsonWindows(probe)
			if !h.from.IsZero() {
				if h.from.After(from) {
					from = h.from
				}
				if h.to.Before(to) {
					to = h.to
				}
			}
			if !to.After(from) {
				continue
			}
			key := probe.plan.fetchQuery + "\x00" + from.String() + "\x00" + to.String()
			if seen[key] {
				continue
			}
			seen[key] = true
			j.mu.Lock()
			prev, done := j.looked[key]
			j.mu.Unlock()
			if done {
				if prev.ok {
					return prev.found, true
				}
				continue
			}
			if j.lookups.Add(1) > jsonMaxLookups {
				j.setUnsure()
				return jsonFoundError{}, false
			}
			f, ok, err := c.jsonLookup(ctx, p, probe, from, to)
			if errors.Is(err, errJSONLookupUnbuilt) {
				j.setUnsure() // no lookup to send: the marks stay unconfirmed
				continue
			}
			if err != nil {
				j.setFailure(err)
				return jsonFoundError{}, false
			}
			j.mu.Lock()
			if j.looked == nil {
				j.looked = map[string]jsonLookedSpan{}
			}
			j.looked[key] = jsonLookedSpan{found: f, ok: ok}
			j.mu.Unlock()
			if ok {
				return f, true
			}
		}
	}
	return jsonFoundError{}, false
}

// errJSONLookupUnbuilt is a lookup the proxy could not write for a probe.
var errJSONLookupUnbuilt = errors.New("json parser error lookup not built")

// jsonMaxLookups bounds the lookups one request spends confirming marks.
const jsonMaxLookups = 8

func (j *jsonErrorCheck) setUnsure() {
	j.mu.Lock()
	j.unsure = true
	j.mu.Unlock()
}

func (j *jsonErrorCheck) setFailure(err error) {
	j.mu.Lock()
	if j.failure == nil {
		j.failure = err
	}
	j.mu.Unlock()
}

// backendFailure is the lookup failure the request answers, once no row
// confirmed Loki's error.
func (j *jsonErrorCheck) backendFailure() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil {
		return nil
	}
	return j.failure
}

// uncertain reports an answer whose marks were not all confirmed.
func (j *jsonErrorCheck) uncertain() bool {
	if j == nil {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.unsure || j.failure != nil
}

// jsonWindows returns the data time Loki evaluates for probe, as VictoriaLogs
// bounds [from, to), and the last evaluation time.
func (c *unwrapCheck) jsonWindows(probe *jsonErrorProbe) (from, to, last time.Time) {
	last = c.start
	if !c.instant && c.step > 0 && !c.end.Before(c.start) {
		last = c.start.Add(c.end.Sub(c.start) / c.step * c.step)
	}
	// Loki's window is left-open, right-closed; VictoriaLogs' end is exclusive.
	return c.start.Add(-probe.offset - probe.window).Add(time.Nanosecond), last.Add(-probe.offset).Add(time.Nanosecond), last
}

// jsonVisible reports whether a line at data time ts is in a window Loki
// evaluates for probe.
func (c *unwrapCheck) jsonVisible(ts int64, probe *jsonErrorProbe, last time.Time) bool {
	ts += int64(probe.offset)
	if c.instant || c.step <= 0 {
		return ts > c.start.Add(-probe.window).UnixNano() && ts <= c.start.UnixNano()
	}
	return orderedJSONSampleVisible(ts, c.start.UnixNano(), last.UnixNano(), int64(c.step), int64(probe.window))
}

// jsonLookupRows is how many marked rows one lookup reads.
const jsonLookupRows = 8

// jsonLookup reads the stored rows of probe's selection in [from, to) that
// VictoriaLogs cannot vouch for, and returns Loki's error for the earliest one
// Loki fails on.
func (c *unwrapCheck) jsonLookup(ctx context.Context, p *Proxy, probe *jsonErrorProbe, from, to time.Time) (jsonFoundError, bool, error) {
	plan := probe.plan
	base, err := p.translateQueryWithContext(ctx, plan.statsQuery())
	if err != nil {
		return jsonFoundError{}, false, errJSONLookupUnbuilt
	}
	base += plan.levelRestriction()
	_, _, last := c.jsonWindows(probe)
	if !c.instant && c.step > probe.window {
		// Only the lines inside a window count; the phase is no label.
		base = withWindowPhaseFilter(ctx, base, c.start.Add(-probe.offset), c.step, probe.window)
	}
	// The phase is no label: the lookup drops it with its own fields.
	query, ok := jsonErrorMarkSpec.Lookup(base, jsonLookupRows, "__lvp_window_phase")
	if !ok {
		return jsonFoundError{}, false, errJSONLookupUnbuilt
	}
	params := url.Values{"query": {query}, "start": {from.UTC().Format(time.RFC3339Nano)}, "end": {to.UTC().Format(time.RFC3339Nano)}}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return jsonFoundError{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return jsonFoundError{}, false, p.redactedBackendStatusError("backend returned", resp.StatusCode, body)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		var row map[string]string
		if json.Unmarshal(scanner.Bytes(), &row) != nil {
			continue
		}
		ts, ok := parseFlexibleUnixNanos(row["_time"])
		if !ok || !c.jsonVisible(ts, probe, last) {
			continue
		}
		if pipelineErr := p.orderedJSONRowPipelineError(ctx, plan, row); pipelineErr != nil {
			return jsonFoundError{ts: ts, err: pipelineErr}, true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return jsonFoundError{}, false, err
	}
	return jsonFoundError{}, false, nil
}

// orderedJSONRowPipelineError is Loki's pipeline error for a stored row the
// plan evaluates, or nil when Loki makes no failing sample of it.
func (p *Proxy) orderedJSONRowPipelineError(ctx context.Context, plan *orderedJSONMetricPlan, row map[string]string) *orderedJSONPipelineError {
	dropIngestUnpackedFields(row)
	desc := p.logQueryStreamDescriptor(row["_stream"], row["level"], map[string]map[string]string{}, map[string]cachedLogQueryStreamDescriptor{})
	base, err := p.orderedJSONBaseLabels(row, desc, map[string][]metadataFieldExposure{})
	if err != nil {
		return nil
	}
	levels := p.newLogRowLevels()
	if !plan.acceptDerivedLevel(base, &levels, row, desc) {
		return nil
	}
	labels, keep, err := plan.processWithContext(ctx, row["_msg"], base, desc.translatedLabels, row)
	if err != nil || !keep || labels["__error__"] == "" {
		return nil
	}
	return lokiPipelineError(labels)
}

// jsonStripResponse applies strip to a 200 stats answer; when the answer held
// marked groups, the marks are confirmed at once, and an answer whose marks
// Loki does not fail on is fetched again without them.
func (p *Proxy) jsonStripResponse(ctx context.Context, resp *http.Response, strip func([]byte) ([]byte, []jsonErrorHit, error), path string, sent url.Values) (*http.Response, bool, error) {
	if resp.StatusCode != http.StatusOK {
		return resp, false, nil
	}
	var hits []jsonErrorHit
	err := p.unwrapStripResponse(resp, func(body []byte) ([]byte, error) {
		out, h, err := strip(body)
		hits = h
		return out, err
	})
	if err != nil {
		return nil, false, err
	}
	if len(hits) == 0 {
		return resp, false, nil
	}
	if c := unwrapCheckFrom(ctx); c == nil || c.json == nil || c.json.confirmAnswer(ctx, p, c, hits) || c.json.backendFailure() != nil {
		return resp, false, nil // Loki's error, or the lookup's failure, replaces the answer
	}
	_ = resp.Body.Close()
	resp, err = p.vlPostHTTP(context.WithValue(ctx, jsonErrorMarkOffKey{}, true), path, sent)
	return resp, true, err
}
