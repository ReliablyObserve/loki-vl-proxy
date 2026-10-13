package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
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
	{name: "stream detected_level wins over the body", stream: map[string]string{"detected_level": "Warning"}, msg: `{"level":"error"}`, want: "Warning", rawRows: true},
	{name: "stored level field", vl: map[string]string{"level": "WARN"}, msg: `{"level":"info"}`, want: "warn"},
	{name: "stored detected_level field", vl: map[string]string{"detected_level": "Error"}, msg: `{"level":"info"}`, want: "error"},
	{name: "body detected_level key alone", msg: `{"detected_level":"verbose"}`, want: "unknown"},
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
	for _, probe := range detectedLevelProbes {
		want := map[string]map[int64]string{
			canonicalLabelsKey(map[string]string{"detected_level": probe.want}): {end.Unix(): "1"},
		}
		for _, pipeline := range []string{`| json`, `| json | drop __error__`, `| json | __error__=""`} {
			t.Run(probe.name+" "+pipeline, func(t *testing.T) {
				query := `sum by (detected_level) (count_over_time({env="prod"} ` + pipeline + ` [5m]))`
				srv, fake := newPushdownFakeVL(t, []pushdownRow{probe.row(s0.Add(100 * time.Second))}, nil)
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
			})
		}
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
