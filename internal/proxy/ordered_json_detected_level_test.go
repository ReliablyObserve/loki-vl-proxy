package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// detectedLevelProbe is one stored line and the detected_level Loki gives it.
//
// The body-only answers are Loki v3.7.7's own: the logic of
// pkg/distributor/field_detection.go (extractLogLevelFromLogLine: the first
// string value of an allowed level key, at depth 0 or 1 in document order,
// counts only as a known spelling, otherwise the whole line is keyword
// scanned) was run on each line. The answers for a level held by the stream
// or by a stored field follow the log-line derivation (deriveRowLevel) and
// are pinned against real Loki by the e2e test
// TestRangeMetricCompatibilityJSONDetectedLevelProbes.
type detectedLevelProbe struct {
	name   string
	stream map[string]string // stream labels besides app and env
	vl     map[string]string // stored fields that are not stream labels
	msg    string
	want   string // the series' detected_level
	// dropOnly marks lines with a parser error: Loki fails the plain `| json`
	// query on them, so only the form that drops the error is valid.
	dropOnly bool
	// rowOnly marks a line the stats pushdown reads differently from Loki
	// without a guard able to see it (semantics/detected-level-pipes-known-
	// differences): only the raw-row evaluator's forms are asserted.
	rowOnly bool
	// rawRows marks the lines the stats pushdown must not answer: the pipes of
	// translator.BuildDetectedLevelChain would read them differently, or
	// might, so a guard probe sends the query to the raw-row evaluator.
	rawRows bool
}

var detectedLevelProbes = []detectedLevelProbe{
	{name: "pino numeric level", msg: `{"level":30,"msg":"hi"}`, want: "unknown"},
	{name: "pino numeric level with an error object", msg: `{"level":50,"msg":"boom","err":{"type":"Error"}}`, want: "error"},
	{name: "boolean level", msg: `{"level":true}`, want: "unknown"},
	{name: "null level", msg: `{"level":null}`, want: "unknown", rawRows: true},
	{name: "unknown spelling", msg: `{"level":"notice"}`, want: "unknown"},
	{name: "unknown spelling and an error word", msg: `{"level":"notice","msg":"error connecting"}`, want: "error"},
	{name: "padded level", msg: `{"level":" info"}`, want: "info"},
	{name: "severity key", msg: `{"severity":"ERROR"}`, want: "error"},
	{name: "Level key", msg: `{"Level":"Information"}`, want: "info"},
	{name: "LEVEL key", msg: `{"LEVEL":"warn"}`, want: "warn"},
	{name: "nested log.level", msg: `{"log":{"level":"warn"}}`, want: "warn"},
	{name: "no level key, error word", msg: `{"msg":"request failed with error"}`, want: "error"},
	{name: "object level", msg: `{"level":{"name":"info"}}`, want: "info", rawRows: true},
	{name: "array level", msg: `{"level":["info"]}`, want: "info"},
	{name: "body detected_level key", msg: `{"detected_level":"error","level":"info"}`, want: "info"},
	{name: "text after the closing brace", msg: `{"level":"info"} trailing`, want: "info", rawRows: true},
	{name: "no level at all", msg: `{"n":1}`, want: "unknown"},
	{name: "plain level key", msg: `{"level":"info"}`, want: "info"},
	{name: "severity before level", msg: `{"msg":"x","severity":"error","level":"info"}`, want: "error", rawRows: true},
	{name: "level before severity", msg: `{"level":"info","severity":"error"}`, want: "info", rawRows: true},
	{name: "stream level, unknown spelling", stream: map[string]string{"level": "notice"}, msg: `{"n":1}`, want: "notice"},
	{name: "stream severity", stream: map[string]string{"severity": "error"}, msg: `{"n":1}`, want: "error"},
	{name: "stream level wins over the body", stream: map[string]string{"level": "INFO"}, msg: `{"level":"error"}`, want: "info"},
	{name: "stream detected_level keeps its stored spelling", stream: map[string]string{"detected_level": "INFO"}, msg: `{"n":1}`, want: "INFO", rawRows: true},
	{name: "stream detected_level wins over a stream level", stream: map[string]string{"detected_level": "INFO", "level": "warn"}, msg: `{"n":1}`, want: "INFO", rawRows: true},
	{name: "stream detected_level wins over the body", stream: map[string]string{"detected_level": "Warning"}, msg: `{"level":"error"}`, want: "Warning", rawRows: true},
	{name: "stored level field", vl: map[string]string{"level": "WARN"}, msg: `{"level":"info"}`, want: "warn"},
	{name: "stored detected_level field", vl: map[string]string{"detected_level": "Error"}, msg: `{"level":"info"}`, want: "error"},
	{name: "body detected_level key alone", msg: `{"detected_level":"verbose"}`, want: "unknown"},
	// A mention of a level key inside a string is no second key.
	{name: "level key mentioned in the message", msg: `{"level":"info","msg":"set log level=debug"}`, want: "info"},
	{name: "level name followed by a colon inside a string", msg: `{"msg":"level: high","level":"warn"}`, want: "warn"},
	{name: "stream severity before a stored level field", stream: map[string]string{"severity": "error"}, vl: map[string]string{"level": "info"}, msg: `{"n":1}`, want: "error", rowOnly: true},
	{name: "stored severity field", vl: map[string]string{"severity": "error"}, msg: `{"n":1}`, want: "error"},
	// OTLP records. VictoriaLogs from v1.50 stores severity_text (the text, or
	// the name of the number when the record has none) with severity_number;
	// before v1.50 one severity field and no number. A synthesised text is no
	// level for Loki, which reads the number (and the line when it is 0).
	{name: "OTLP without severity", vl: map[string]string{"severity_text": "Unspecified", "severity_number": "0"}, msg: `hello`, want: "unknown", dropOnly: true},
	{name: "OTLP without severity, error word", vl: map[string]string{"severity_text": "Unspecified", "severity_number": "0"}, msg: `request error`, want: "error", dropOnly: true},
	{name: "OTLP fine severity number", vl: map[string]string{"severity_text": "Info2", "severity_number": "10"}, msg: `hello`, want: "info", dropOnly: true},
	{name: "OTLP severity number", vl: map[string]string{"severity_text": "Warn", "severity_number": "13"}, msg: `hello`, want: "warn", dropOnly: true},
	{name: "OTLP severity number out of range", vl: map[string]string{"severity_text": "Unspecified", "severity_number": "30"}, msg: `request error`, want: "unknown", dropOnly: true},
	{name: "OTLP own text", vl: map[string]string{"severity_text": "notice", "severity_number": "0"}, msg: `hello`, want: "notice", dropOnly: true},
	{name: "OTLP own text over its number", vl: map[string]string{"severity_text": "Fine", "severity_number": "9"}, msg: `hello`, want: "Fine", dropOnly: true},
	{name: "OTLP without severity, before v1.50", vl: map[string]string{"severity": "Unspecified"}, msg: `hello`, want: "unknown", dropOnly: true},
	{name: "OTLP without severity, before v1.50, error word", vl: map[string]string{"severity": "Unspecified"}, msg: `request error`, want: "error", dropOnly: true},
	{name: "OTLP fine severity number, before v1.50", vl: map[string]string{"severity": "Info2"}, msg: `hello`, want: "info", dropOnly: true},
	{name: "OTLP severity number, before v1.50", vl: map[string]string{"severity": "Warn"}, msg: `hello`, want: "warn", dropOnly: true},
	{name: "OTLP own text, before v1.50", vl: map[string]string{"severity": "notice"}, msg: `hello`, want: "notice", dropOnly: true},
	// Malformed JSON is not JSON for Loki's ingest: the keyword scan reads it.
	{name: "body detected_level key, syntax error", msg: `{"detected_level":"warn","x":{bad`, want: "warn", dropOnly: true},
	{name: "body detected_level and level keys, syntax error", msg: `{"detected_level":"warn","level":"info","x":{bad`, want: "warn", dropOnly: true, rawRows: true},
	{name: "level key, syntax error", msg: `{"level":"info","x":{bad`, want: "info", dropOnly: true, rawRows: true},
	// A line that is JSON for Loki (a brace first and last) never goes to
	// the logfmt parser, whatever its text holds.
	{name: "logfmt-looking text inside a JSON message", msg: `{"msg":"error: cannot set level=debug now"}`, want: "error"},
	{name: "logfmt-looking text inside a JSON message, level key", msg: `{"msg":"set level=debug","level":"info"}`, want: "info"},
	// One that only starts with a brace is logfmt for Loki.
	{name: "logfmt after a brace", msg: `{ error level=info`, want: "info", dropOnly: true, rawRows: true},
	{name: "logfmt level keys of two spellings", msg: `Level=info LEVEL=error`, want: "info", dropOnly: true, rawRows: true},
	// Loki reads the raw bytes of a JSON value: an escaped spelling is no known
	// level for it, where VictoriaLogs decodes it. Keys are decoded by both.
	{name: "escaped level value", msg: `{"level":"\u0069nfo"}`, want: "unknown", rawRows: true},
	{name: "escaped level key", msg: `{"\u006cevel":"warn"}`, want: "warn"},
	// A spelling the keyword scan cannot find (information is no whole word
	// of the scan): only the decoded key gives info, as in Loki v3.7.7.
	{name: "escaped level key, long spelling", msg: `{"\u006cevel":"Information"}`, want: "info"},
}

// storedLevelDecides reports whether a stored level field, and no stored
// detected_level, decides the line's level (a level field of the stream or of
// the structured metadata): the stats grouped by the stored fields answer it.
func storedLevelDecides(c detectedLevelProbe) bool {
	_, streamLevel := c.stream["level"]
	_, metaLevel := c.vl["level"]
	_, streamDL := c.stream["detected_level"]
	_, metaDL := c.vl["detected_level"]
	return (streamLevel || metaLevel) && !streamDL && !metaDL
}

func (c detectedLevelProbe) row(ts time.Time) pushdownRow {
	labels := map[string]string{"app": "api", "env": "prod"}
	for k, v := range c.stream {
		labels[k] = v
	}
	return pushdownRow{ts: ts, stream: labels, vl: c.vl, msg: c.msg}
}

// The detected_level of `sum by (detected_level) (... | json ...)` is the
// level Loki sets at ingest (distributor/field_detection.go), whatever shape
// the level has in the line: a stream label keeps its stored value, the first
// string value of a level key in document order counts only as a known
// spelling, anything else falls to a keyword scan of the line, and a body key
// named detected_level never replaces it (Loki renames it
// detected_level_extracted). Every route answers the same: the raw-row
// evaluator (plain `| json`) and the stats pushdown with its guard
// (`| json | drop __error__`).
// conformance: semantics/json-metric-detected-level-derivation
func TestOrderedJSONDetectedLevelLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(2*time.Minute)
	// Each line is asked twice: through the full chain, and through the chain
	// pruned to the fields and keys the window holds (the presence probe
	// answered from the rows), which must give the same answer.
	for _, pruned := range []bool{false, true} {
		for _, probe := range detectedLevelProbes {
			want := map[string]map[int64]string{
				canonicalLabelsKey(map[string]string{"detected_level": probe.want}): {end.Unix(): "1"},
			}
			for _, pipeline := range []string{`| json`, `| json | drop __error__`, `| json | __error__=""`} {
				if probe.dropOnly && pipeline != `| json | drop __error__` || probe.rowOnly && pipeline == `| json | drop __error__` {
					continue
				}
				name := probe.name + " " + pipeline
				if pruned {
					name += " pruned"
				}
				t.Run(name, func(t *testing.T) {
					query := `sum by (detected_level) (count_over_time({env="prod"} ` + pipeline + ` [5m]))`
					srv, fake := newPushdownFakeVL(t, []pushdownRow{probe.row(s0.Add(100 * time.Second))}, nil)
					fake.presence = pruned
					p := newFilterPushdownProxy(t, srv.URL)
					fake.stored = p.labelTranslator.ToVL
					if code, got, body := detectedLevelQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) {
						t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
					}
					if pipeline != `| json | drop __error__` {
						return
					}
					fake.mu.Lock()
					defer fake.mu.Unlock()
					if viaRaw := fake.raw > 0; viaRaw != probe.rawRows {
						t.Fatalf("raw rows read: %v, want %v (stats queries %d, probes %d)", viaRaw, probe.rawRows, len(fake.stats), len(fake.guards))
					}
					// A stored level (and no stored detected_level) decides in one stats
					// query grouped by the stored fields, with no chain, no guard and
					// no presence probe; any other line asks again with the chain.
					chain := chainStatsCalls(fake.stats)
					if storedLevelDecides(probe) {
						if len(fake.stats) != 1 || len(chain) != 0 || len(fake.presenceCalls) != 0 {
							t.Fatalf("a stored level should decide in one stats query: stats=%d chain=%d presence probes=%d", len(fake.stats), len(chain), len(fake.presenceCalls))
						}
						return
					}
					// (a guard that finds a risky line cancels the stats request, and with
					// it a presence probe not yet sent)
					if pruned && !probe.rawRows && len(fake.presenceCalls) != 1 {
						t.Fatalf("%d presence probes, want 1", len(fake.presenceCalls))
					}
					if pruned && len(chain) == 1 && len(chain[0]) >= len(levelChain) {
						t.Fatalf("the stats query carries the full chain (%d bytes)", len(chain[0]))
					}
				})
			}
		}
	}
}

// Two lines the VictoriaLogs pipes cannot read like Loki and no guard probe can
// see (semantics/detected-level-pipes-known-differences): Loki's logfmt decoder
// compares keys without case and stops at a syntax error, where unpack_logfmt
// is case-sensitive and skips over it. The raw-row evaluator reads them like
// Loki; the stats pushdown keeps the pipes' value, which the entry records.
// conformance: semantics/detected-level-pipes-known-differences
func TestOrderedJSONDetectedLevelKnownDifferences(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(2*time.Minute)
	for _, tc := range []struct{ name, msg, loki, pipes string }{
		{"logfmt level key of another case", `msg="error x" LvL=warn`, "warn", "error"},
		{"logfmt syntax error before the level", `msg="unterminated error level=info`, "error", "info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := pushdownRow{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: tc.msg}
			srv, fake := newPushdownFakeVL(t, []pushdownRow{row}, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			series := func(level string) map[string]map[int64]string {
				return map[string]map[int64]string{canonicalLabelsKey(map[string]string{"detected_level": level}): {end.Unix(): "1"}}
			}
			plan, ok := compileOrderedJSONMetric(`sum by (detected_level) (count_over_time({env="prod"} | logfmt | drop __error__ [5m]))`)
			if !ok {
				t.Fatal("no plan")
			}
			if got := detectedLevelRowMatrix(t, p, plan, start, end, time.Minute); !reflect.DeepEqual(got, series(tc.loki)) {
				t.Errorf("row evaluator: got %v, want Loki's %s", got, tc.loki)
			}
			query := `sum by (detected_level) (count_over_time({env="prod"} | logfmt | drop __error__ [5m]))`
			if code, got, body := detectedLevelQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, series(tc.pipes)) {
				t.Errorf("stats pushdown: status %d %.200s, got %v, want the pipes' %s", code, body, got, tc.pipes)
			}
		})
	}
}

// The raw-row evaluator groups by the same value whether the rows come in
// one response or many, for count and bytes alike, and a window mixing every
// shape yields one series per level.
func TestOrderedJSONDetectedLevelMixedRows(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	want := map[string]map[string]int{} // level -> function -> sum at the last step
	for i, probe := range detectedLevelProbes {
		if probe.dropOnly {
			continue // the plain `| json` query fails on them, as in Loki
		}
		rows = append(rows, probe.row(s0.Add(time.Duration(100+i)*time.Second)))
		if want[probe.want] == nil {
			want[probe.want] = map[string]int{}
		}
		want[probe.want]["count_over_time"]++
		want[probe.want]["bytes_over_time"] += len(probe.msg)
	}
	for _, function := range []string{"count_over_time", "bytes_over_time"} {
		t.Run(function, func(t *testing.T) {
			query := `sum by (detected_level) (` + function + `({env="prod"} | json [5m]))`
			plan, ok := compileOrderedJSONMetric(query)
			if !ok {
				t.Fatal("no plan")
			}
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			got := detectedLevelRowMatrix(t, p, plan, start, end, time.Minute)
			if len(got) != len(want) {
				t.Fatalf("%d series, want %d: %v", len(got), len(want), got)
			}
			for level, sums := range want {
				key := canonicalLabelsKey(map[string]string{"detected_level": level})
				if have, ok := got[key][end.Unix()]; !ok || have != strconv.Itoa(sums[function]) {
					t.Errorf("%s at %d: %q, want %d", level, end.Unix(), have, sums[function])
				}
			}
		})
	}
}

// detectedLevelRowMatrix answers plan with the row evaluator over every
// selected row.
func detectedLevelRowMatrix(t *testing.T, p *Proxy, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	series, err := p.collectOrderedJSONMetric(context.Background(), plan, start, end, step)
	if err != nil {
		t.Fatalf("row evaluator: %v", err)
	}
	body, err := buildOrderedJSONMetric(context.Background(), plan, series, start, end, step, true, 1<<30)
	if err != nil {
		t.Fatalf("row evaluator: %v", err)
	}
	return decodeExtractedMatrix(t, body)
}

// detectedLevelQuery runs query through the proxy's query_range handler.
func detectedLevelQuery(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) (int, map[string]map[int64]string, string) {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		return rec.Code, nil, rec.Body.String()
	}
	return rec.Code, decodeExtractedMatrix(t, rec.Body.Bytes()), ""
}

// A filter on detected_level compares Loki's structured metadata, on every
// line whether or not its body parses, so it reads the derived value: the
// raw-row evaluator filters on it, and the stats pushdown (also for Logs
// Drilldown's detected_level breakdown, which is not the native histogram's)
// applies the filter after the pipes that compute it, `!= ""` being always
// true.
// conformance: semantics/json-metric-detected-level-derivation
func TestOrderedJSONDetectedLevelFilters(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(5*time.Minute), s0.Add(5*time.Minute)
	pick := func(clean bool) (rows []pushdownRow, levels []string) {
		for i, probe := range detectedLevelProbes {
			if probe.dropOnly || clean && (probe.rawRows || probe.rowOnly) {
				continue
			}
			rows = append(rows, probe.row(s0.Add(time.Duration(100+i)*time.Second)))
			levels = append(levels, probe.want)
		}
		return rows, levels
	}
	for _, tc := range []struct {
		name, pipeline string
		keep           func(level string) bool
		pushdown       bool   // the stats pushdown serves lines the pipes read like Loki
		filter         string // the pipe the pushdown applies after the level pipes (a row the witness flags passes it)
	}{
		{"not empty", `| json | drop __error__, __error_details__ | detected_level != ""`, func(string) bool { return true }, true, ""},
		{"equals", `| json | drop __error__ | detected_level="info"`, func(l string) bool { return l == "info" }, true, `| filter (detected_level:="info" OR __lw:="1")`},
		{"regexp", `| json | drop __error__ | detected_level=~"warn|error"`, func(l string) bool { return l == "warn" || l == "error" }, true, `| filter (detected_level:~"^(?:warn|error)$" OR __lw:="1")`},
		{"not equals", `| json | drop __error__ | detected_level!="unknown"`, func(l string) bool { return l != "unknown" }, true, `| filter (-detected_level:="unknown" OR __lw:="1")`},
		// No error drop: a parse error survives a filter on detected_level, which
		// holds for every line, so the raw-row evaluator answers.
		{"without the error drop", `| json | detected_level="info"`, func(l string) bool { return l == "info" }, false, ""},
		{"error label", `| json | __error__="" | detected_level!=""`, func(string) bool { return true }, false, ""},
	} {
		for _, clean := range []bool{false, true} {
			name := tc.name + " all probes"
			if clean {
				name = tc.name + " lines the pipes read like Loki"
			}
			t.Run(name, func(t *testing.T) {
				rows, levels := pick(clean)
				want := map[string]map[int64]string{}
				counts := map[string]int{}
				for _, level := range levels {
					if tc.keep(level) {
						counts[level]++
					}
				}
				for level, n := range counts {
					want[canonicalLabelsKey(map[string]string{"detected_level": level})] = map[int64]string{end.Unix(): strconv.Itoa(n)}
				}
				if len(want) == 0 {
					t.Fatal("fixture: the filter keeps nothing")
				}
				query := `sum by (detected_level) (count_over_time({env="prod"} ` + tc.pipeline + ` [5m]))`
				for _, mode := range []struct{ drilldown, pruned bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
					drilldown := mode.drilldown
					srv, fake := newPushdownFakeVL(t, rows, nil)
					fake.presence = mode.pruned
					p := newFilterPushdownProxy(t, srv.URL)
					fake.stored = p.labelTranslator.ToVL
					params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"300"}}
					r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
					if drilldown {
						r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
					}
					rec := httptest.NewRecorder()
					p.handleQueryRange(rec, r)
					if rec.Code != http.StatusOK {
						t.Fatalf("drilldown=%v pruned=%v: status %d %s", drilldown, mode.pruned, rec.Code, rec.Body)
					}
					if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
						t.Fatalf("drilldown=%v pruned=%v: got %v, want %v", drilldown, mode.pruned, got, want)
					}
					fake.mu.Lock()
					// Logs Drilldown's detected_level breakdown has the shape of its
					// native histogram, but the label translator maps detected_level
					// to level, so the native route is never exact for it
					// (orderedJSONNativeGroupingIsExact): lines the pipes read like
					// Loki are answered by the pushdown, behind its guard probes,
					// with the filter applied after the pipes.
					// The probe lines have no stored level, so the stored-level attempt is
					// followed by the chain, whose answer is the guarded one.
					chain := chainStatsCalls(fake.stats)
					if clean && tc.pushdown && (fake.raw != 0 || len(chain) != 1 || len(fake.guards) == 0) {
						t.Errorf("drilldown=%v: expected a guarded pushdown answer without raw rows: raw=%d stats=%d guards=%d", drilldown, fake.raw, len(chain), len(fake.guards))
					}
					if clean && tc.pushdown && tc.filter != "" && len(chain) == 1 && !strings.Contains(chain[0], tc.filter+" | stats by (detected_level)") && !strings.Contains(chain[0], tc.filter+" | stats by (detected_level, __lw)") {
						t.Errorf("drilldown=%v: filter not applied after the level pipes: %s", drilldown, chain[0])
					}
					fake.mu.Unlock()
				}
			})
		}
	}
}
