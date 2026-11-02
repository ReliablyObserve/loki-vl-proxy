package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The detected_level family of `| json` metrics without a drop of the parser
// errors is answered by the split pushdown (ordered_json_valid_split.go):
//
//   - `sum by (detected_level) (count_over_time({...} | json [1m]))` keeps the
//     errors, so Loki fails it on a line its parser rejects
//     (RangeVectorEvaluator.Next, logqlmodel.NewPipelineErr) and otherwise
//     counts every line by the level Loki's ingest gave it (jsonSplitStrict);
//   - `sum by (detected_level) (count_over_time({...} | json | __error__!="" [1m]))`
//     counts the lines Loki's parser rejects (jsonSplitErrors).
//
// Both read only the lines VictoriaLogs cannot vouch for as raw rows; neither
// reads every selected row, which failed DLV2 at 24h past
// -manual-range-metric-row-limit.

// jsonErrorSplitClass reports how a jsonSplitErrors plan files a line, with
// the predicates its stats query sends evaluated with VictoriaLogs semantics:
// counted as rejected by VictoriaLogs (no brace after JSON whitespace), read
// as a raw row (a brace line it cannot vouch for, or one holding a hazard), or
// left out (a line VictoriaLogs vouches for, which Loki accepts).
func jsonErrorSplitClass(t testing.TB, line string) (counted, rawRow bool) {
	if !fakeLogsQLMatch(t, jsonObjectStartFilter, map[string]string{"_msg": line}) {
		return true, false
	}
	vouched, raw := jsonSplitClass(t, line)
	return false, raw || !vouched
}

// jsonErrorSplitHintSets are the parser hints of the family: detected_level
// alone (strict), with __error__ (errors), none (no grouping), and a nested
// prefix of detected_level, so nested objects under "detected" are walked.
var jsonErrorSplitHintSets = [][]string{{"detected_level"}, {"detected_level", "__error__"}, {"__error__"}, nil}

// jsonErrorSplitSeeds are the lines of the edge table on top of the split's
// own seeds: no brace at all, empty and blank lines, JSON whitespace Loki does
// not skip, logfmt, keys that look like the level, escaped and nested keys.
var jsonErrorSplitSeeds = append(append([]string(nil), jsonSplitSeeds...),
	"", " ", "\t\n", "\f", "plain text line", "level=info msg=x", "[1,2]", `"string"`, "123", "null", " \f{\"a\":1}",
	`{"detected_level":"error","msg":"x"}`, `{"level":"warn"}`, `{"detected":{"level":"x"}}`, `{"detected_level":"x"}`,
	`{"detected_level":"error","msg": truncated`, `{"a":[1,{"b":NaN}]}`, `{"detected":{"level":NaN}}`, "\t{\"a\":1}", "\r\n{\"a\":1}")

// checkJSONErrorSplitLine fails when the errors split counts a line Loki's
// parser accepts, or leaves out a line it rejects, under any of the hints.
func checkJSONErrorSplitLine(t testing.TB, line string) (counted, rawRow bool) {
	counted, rawRow = jsonErrorSplitClass(t, line)
	for _, hints := range jsonErrorSplitHintSets {
		accepted := lokiJSONAccepts(line, hints)
		switch {
		case counted && accepted:
			t.Errorf("VictoriaLogs counts %q as rejected, Loki's parser accepts it (hints %q)", line, hints)
		case !counted && !rawRow && !accepted:
			t.Errorf("the split leaves out %q, Loki's parser rejects it (hints %q)", line, hints)
		}
	}
	return counted, rawRow
}

// The lines a `| json | __error__!=""` split lets VictoriaLogs count are lines
// Loki's JSON parser rejects under every hint set of the family, and the lines
// it leaves out are lines Loki accepts; every other line is read as a raw row
// and evaluated by the row evaluator. Seeded corpus of generated and mutated
// JSON plus the edge lines.
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation
func TestJSONErrorSplitPredicateDifferential(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 40_000
	}
	rng := rand.New(rand.NewSource(20261102))
	counted, rawRows := 0, 0
	for i := 0; i < n; i++ {
		line := jsonErrorSplitSeeds[i%len(jsonErrorSplitSeeds)]
		if i >= len(jsonErrorSplitSeeds) {
			line = jsonSplitLine(rng)
		}
		c, r := checkJSONErrorSplitLine(t, line)
		if c {
			counted++
		}
		if r {
			rawRows++
		}
		if t.Failed() {
			return
		}
	}
	if counted < n/100 || rawRows < n/100 {
		t.Fatalf("the corpus no longer exercises the split: counted=%d raw rows=%d of %d", counted, rawRows, n)
	}
}

// FuzzJSONErrorSplitPredicate runs the errors split differential over fuzzed lines.
func FuzzJSONErrorSplitPredicate(f *testing.F) {
	for _, seed := range jsonErrorSplitSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		checkJSONErrorSplitLine(t, line)
	})
}

// braceOnlyRows keeps the rows whose line opens with a brace after JSON
// whitespace, the only lines a strict plan can count.
func braceOnlyRows(t testing.TB, rows []pushdownRow, rejected bool) []pushdownRow {
	var out []pushdownRow
	for _, row := range rows {
		if !fakeLogsQLMatch(t, jsonObjectStartFilter, map[string]string{"_msg": row.msg}) {
			continue
		}
		if !rejected && !lokiJSONAccepts(row.msg, []string{"detected_level"}) {
			continue
		}
		out = append(out, row)
	}
	return out
}

var jsonDetectedLevelSplitQueries = []struct {
	query string
	split jsonSplitMode
}{
	{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`, jsonSplitErrors},
	{`sum(count_over_time({env="prod"} | json | __error__!="" [1m]))`, jsonSplitErrors},
	{`sum by (detected_level) (rate({env="prod"} | json | __error__!="" [2m]))`, jsonSplitErrors},
	{`sum by (detected_level) (bytes_over_time({env="prod"} | json | __error__!="" [1m]))`, jsonSplitErrors},
	{`sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, jsonSplitStrict},
	{`sum by (detected_level) (rate({env="prod"} | json [2m]))`, jsonSplitStrict},
	{`sum by (detected_level) (bytes_rate({env="prod"} | json [1m]))`, jsonSplitStrict},
}

// runDetectedLevelSplit answers query through the proxy over rows, and through
// the row evaluator (the route these shapes took before, whose parser follows
// Loki's); both the status and the body, or the matrix, must agree. It returns
// the fake backend's record of the request.
func runDetectedLevelSplit(t *testing.T, rows []pushdownRow, query string, start, end time.Time, step time.Duration, label string) *pushdownFakeVL {
	t.Helper()
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown || !plan.validOnly {
		t.Fatalf("%s: expected a split pushdown plan", label)
	}
	wantSeries, wantErr := p.collectOrderedJSONMetric(context.Background(), plan, start, end, step)
	var want map[string]map[int64]string
	if wantErr == nil {
		body, err := buildOrderedJSONMetric(context.Background(), plan, wantSeries, start, end, step, true, 1<<30)
		if err != nil {
			t.Fatalf("%s: row evaluator: %v", label, err)
		}
		want = decodeExtractedMatrix(t, body)
	}
	fake.mu.Lock()
	fake.raw, fake.rawQueries, fake.stats, fake.guards = 0, nil, nil, nil
	fake.mu.Unlock()
	code, got, body := runSplitQuery(t, p, query, start, end, step)
	switch {
	case wantErr != nil:
		status, _, text := lokiClientError(context.Background(), wantErr)
		if code != status || !strings.Contains(body, jsonQuote(text)) {
			t.Fatalf("%s: got %d %s, want the row evaluator's %d %q", label, code, body, status, text)
		}
	case code != http.StatusOK:
		t.Fatalf("%s: status %d: %s", label, code, body)
	case !reflect.DeepEqual(got, want):
		t.Fatalf("%s: differs from the row evaluator\n got: %v\nwant: %v", label, got, want)
	}
	return fake
}

// jsonQuote renders s as it appears inside a JSON string.
func jsonQuote(s string) string {
	q := fmt.Sprintf("%q", s)
	return q[1 : len(q)-1]
}

// assertBoundedRawReads fails when a request read every selected row instead
// of only the lines VictoriaLogs cannot vouch for.
func assertBoundedRawReads(t *testing.T, fake *pushdownFakeVL, label string) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stats) == 0 {
		t.Fatalf("%s: no stats query: the row evaluator answered", label)
	}
	if len(fake.rawQueries) > maxOrderedJSONRowSpans {
		t.Fatalf("%s: %d raw reads", label, len(fake.rawQueries))
	}
	for _, raw := range fake.rawQueries {
		if !strings.Contains(raw.query, " | filter "+jsonInvalidCondition(false)) && !strings.Contains(raw.query, " | filter "+jsonInvalidCondition(true)) {
			t.Fatalf("%s: raw read %q is not limited to the lines VictoriaLogs cannot vouch for", label, raw.query)
		}
	}
}

// The split answers every query of the family exactly as the row evaluator,
// over seeded rows with VictoriaLogs' parser in the fake backend, and reads
// only the lines VictoriaLogs cannot vouch for: a `__error__!=""` count over
// every kind of line, and a strict count over brace lines Loki accepts (an
// answer) or with a line it rejects among them (Loki's pipeline error). A
// strict plan over a line without a brace is Loki's error from the row
// evaluator's lookup, without a stats scan.
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation, severity-detected-level-derivation
func TestJSONDetectedLevelSplitMatchesRawEvaluator(t *testing.T) {
	seeds := 8
	if testing.Short() {
		seeds = 3
	}
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		hazards := seed%2 == 0
		mixed := withStoredLevels(jsonSplitRows(rng, s0, 400, hazards))
		for _, tc := range jsonDetectedLevelSplitQueries {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || plan.split != tc.split {
				t.Fatalf("%s: split mode %d, want %d", tc.query, plan.split, tc.split)
			}
			label := fmt.Sprintf("seed %d: %s", seed, tc.query)
			if tc.split == jsonSplitErrors {
				fake := runDetectedLevelSplit(t, mixed, tc.query, start, end, time.Minute, label)
				assertBoundedRawReads(t, fake, label)
				if strings.Contains(tc.query, "detected_level") {
					assertStoredLevelStats(t, fake, label)
				}
				continue
			}
			fake := runDetectedLevelSplit(t, braceOnlyRows(t, mixed, false), tc.query, start, end, time.Minute, label+" (accepted lines)")
			assertBoundedRawReads(t, fake, label)
			assertStoredLevelStats(t, fake, label)
			runDetectedLevelSplit(t, braceOnlyRows(t, mixed, true), tc.query, start, end, time.Minute, label+" (with rejected lines)")
			runDetectedLevelSplit(t, mixed, tc.query, start, end, time.Minute, label+" (with unbraced lines)")
		}
	}
}

// The edge table of the family: one line of each kind among ordinary JSON
// lines, on streams with a stored level, answered as Loki answers. A line
// Loki's parser rejects is counted by `__error__!=""` and fails the strict
// count with Loki's pipeline error; a line it accepts (a body key named
// detected_level or level, a nested or escaped key, an array, an empty object,
// JSON whitespace before the brace) is never an error, and the body key
// detected_level never sets the level (Loki renames it
// detected_level_extracted).
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation, severity-detected-level-derivation
func TestJSONDetectedLevelSplitEdgeTable(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	for _, tc := range []struct {
		line     string
		rejected bool // Loki's JSON parser rejects the line under the family's hints
	}{
		{`{"detected_level":"error","msg":"x"}`, false},
		{`{"level":"error","msg":"x"}`, false},
		{`{"detected":{"level":"error"}}`, false},
		{`{"detected_level":"error"}`, false},
		{`{"a":[1,"b",{"c":2}]}`, false},
		{`{}`, false},
		{`  {"a":1}`, false},
		{"\t{\"a\":1}", false},
		{`{"a":1} trailing`, false},
		{`{"detected_level":"error","msg": truncated`, true},
		{`{"a":NaN}`, true},
		{`{"a\qb":1}`, true},
		{"\f{\"a\":1}", true},
		{"plain text line", true},
		{"level=error msg=x", true},
		{"", true},
		{" ", true},
	} {
		if got := !lokiJSONAccepts(tc.line, []string{"detected_level"}); got != tc.rejected {
			t.Fatalf("%q: Loki's parser rejects it: %v, table says %v", tc.line, got, tc.rejected)
		}
		var rows []pushdownRow
		for i := 0; i < 60; i++ {
			msg := fmt.Sprintf(`{"status":"200","n":%d}`, i)
			if i == 30 {
				msg = tc.line
			}
			rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"env": "prod", "level": []string{"info", "error"}[i%2]}, msg: msg})
		}
		errorsQuery := `sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`
		fake := runDetectedLevelSplit(t, rows, errorsQuery, start, end, time.Minute, fmt.Sprintf("%q errors", tc.line))
		assertBoundedRawReads(t, fake, fmt.Sprintf("%q errors", tc.line))
		strictQuery := `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.jsonUnpack = vlFastjsonUnpackFields
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		code, got, body := runSplitQuery(t, p, strictQuery, start, end, time.Minute)
		if tc.rejected {
			if code != http.StatusBadRequest || !strings.Contains(body, "pipeline error: 'JSONParserErr'") {
				t.Fatalf("%q strict: got %d %s, want Loki's 400 JSONParserErr pipeline error", tc.line, code, body)
			}
			continue
		}
		if code != http.StatusOK {
			t.Fatalf("%q strict: status %d: %s", tc.line, code, body)
		}
		// Every line counts under its stream's level: six lines in each
		// one-minute window inside the data, the body key never a level.
		total := 0
		for series, points := range got {
			if series != `{detected_level="error"}` && series != `{detected_level="info"}` {
				t.Fatalf("%q strict: series %s, want the stream levels only", tc.line, series)
			}
			var n int
			fmt.Sscan(points[s0.Add(5*time.Minute).Unix()], &n)
			total += n
		}
		if total != 6 {
			t.Fatalf("%q strict: %d lines in the window ending at 5m, want 6: %v", tc.line, total, got)
		}
		runDetectedLevelSplit(t, rows, strictQuery, start, end, time.Minute, fmt.Sprintf("%q strict", tc.line))
	}
}

// Shapes outside the family keep their routes: a strict plan grouped by a
// parsed key or filtered, a `__error__!=""` followed by another label stage,
// an instant query and a step wider than the window.
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation
func TestJSONDetectedLevelSplitEligibility(t *testing.T) {
	for _, tc := range []struct {
		query string
		split jsonSplitMode
	}{
		{`sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, jsonSplitStrict},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`, jsonSplitErrors},
		{`sum(count_over_time({env="prod"} | json | __error__!="" [1m]))`, jsonSplitErrors},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__="" [1m]))`, jsonSplitValid},
		{`sum by (status) (count_over_time({env="prod"} | json [1m]))`, jsonSplitNone},
		{`sum by (detected_level, app) (count_over_time({env="prod"} | json [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | status="200" [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" | status="500" [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" | drop __error__ [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | drop __error__ | __error__!="" [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" | __error__="" [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | logfmt | __error__!="" [1m]))`, jsonSplitNone},
		{`sum by (detected_level) (count_over_time({env="prod"} | json | detected_level="error" [1m]))`, jsonSplitNone},
		{`sum without (app) (count_over_time({env="prod"} | json [1m]))`, jsonSplitNone},
	} {
		plan, ok := compileOrderedJSONMetric(tc.query)
		got := jsonSplitNone
		if ok && plan.pushdown {
			got = plan.split
		}
		if got != tc.split {
			t.Errorf("%s: split mode %d, want %d", tc.query, got, tc.split)
		}
	}
	s0 := time.Unix(1700000400, 0).UTC()
	rows := withStoredLevels([]pushdownRow{{ts: s0.Add(90 * time.Second), stream: map[string]string{"env": "prod"}, msg: `{"a":1}`}})
	for _, tc := range []struct {
		label string
		step  time.Duration
	}{{"step wider than the window", 2 * time.Minute}} {
		srv, fake := newPushdownFakeVL(t, rows, nil)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		code, _, body := runSplitQuery(t, p, `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, s0.Add(2*time.Minute), s0.Add(10*time.Minute), tc.step)
		fake.mu.Lock()
		stats, raw := len(fake.stats), fake.raw
		fake.mu.Unlock()
		if code != http.StatusOK || stats != 0 || raw == 0 {
			t.Fatalf("%s: status %d stats=%d raw=%d (%s), want the row evaluator", tc.label, code, stats, raw, body)
		}
	}
}

// The verdict of the unbraced-line lookup is remembered per tenant: a window
// found clean for one tenant is no verdict for another, whose own line is
// found, and each tenant's verdict is reused on its next request.
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation
func TestJSONUnparsedLineRiskPerTenant(t *testing.T) {
	var unbraced atomic.Bool
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if unbraced.Load() {
			_, _ = w.Write([]byte(`{"_msg":"plain","_time":"2023-11-14T22:21:30Z","_stream":"{env=\"prod\"}"}` + "\n"))
		}
	}))
	defer srv.Close()
	p := newFilterPushdownProxy(t, srv.URL)
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	check := func(tenant string, wantRisky bool, wantCalls int32) {
		t.Helper()
		ctx := context.WithValue(context.Background(), orgIDKey, tenant)
		before := calls.Load()
		risky, err := p.jsonUnparsedLineRisk(ctx, `env:="prod"`, start, end, time.Minute, time.Minute)
		if err != nil || risky != wantRisky || calls.Load()-before != wantCalls {
			t.Fatalf("tenant %s: risky=%v err=%v calls=%d, want risky=%v calls=%d", tenant, risky, err, calls.Load()-before, wantRisky, wantCalls)
		}
	}
	check("a", false, 1)
	unbraced.Store(true)
	check("b", true, 1)
	check("a", false, 0)
	check("b", true, 1) // a risky window is never remembered
}

// The rows a split of the family reads stay within
// -manual-range-metric-row-limit: past it the answer is the documented limit
// error naming the flag (400), never the row evaluator's read of every row or
// a 500.
// conformance: semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation
func TestJSONDetectedLevelSplitRawRowsStayWithinTheRowLimit(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	// unvouched is a brace line VictoriaLogs cannot vouch for: truncated (Loki
	// rejects it) or after JSON whitespace (Loki accepts it, so a strict count
	// counts it instead of failing).
	fixture := func(unvouched string) []pushdownRow {
		var rows []pushdownRow
		for i := 0; i < 60; i++ {
			msg := fmt.Sprintf(`{"status":"200","n":%d}`, i)
			if i%3 == 0 {
				msg = fmt.Sprintf(unvouched, i)
			}
			rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"env": "prod", "level": "info"}, msg: msg})
		}
		return rows
	}
	for _, tc := range []struct{ query, unvouched string }{
		{`sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`, `{"status":"200","msg":"truncated %d`},
		{`sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, `  {"status":"200","n":%d}`},
	} {
		srv, fake := newPushdownFakeVL(t, fixture(tc.unvouched), nil)
		fake.jsonUnpack = vlFastjsonUnpackFields
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		p.rangeMetricRowLimit = 5
		code, _, body := runSplitQuery(t, p, tc.query, s0.Add(2*time.Minute), s0.Add(10*time.Minute), time.Minute)
		if code != http.StatusBadRequest || !strings.Contains(body, "-manual-range-metric-row-limit") {
			t.Fatalf("%s: got %d %s, want the 400 limit error naming -manual-range-metric-row-limit", tc.query, code, body)
		}
		fake.mu.Lock()
		reads := len(fake.rawQueries)
		for _, raw := range fake.rawQueries {
			if !strings.Contains(raw.query, "__vlp:*") {
				t.Fatalf("%s: the row evaluator read every row: %q", tc.query, raw.query)
			}
		}
		fake.mu.Unlock()
		if reads == 0 {
			t.Fatalf("%s: no raw read", tc.query)
		}
	}
}
