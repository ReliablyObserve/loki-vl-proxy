package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binaryAggregationAnswer sends query through the real handler against the
// emulated VictoriaLogs and returns label set -> timestamp (s) -> value.
func binaryAggregationAnswer(t *testing.T, emu *vlEmulator, query string, start, end int64, step time.Duration) (map[string]map[int64]float64, string) {
	t.Helper()
	return binaryAggregationAnswerFor(t, emu, "0", query, start, end, step)
}

// binaryAggregationAnswerFor is binaryAggregationAnswer for the tenants orgID names.
func binaryAggregationAnswerFor(t *testing.T, emu *vlEmulator, orgID, query string, start, end int64, step time.Duration, configure ...func(*Config)) (map[string]map[int64]float64, string) {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	cfg := Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second}
	for _, apply := range configure {
		apply(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(start/1e9, 10))
		params.Set("end", strconv.FormatInt(end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": orgID})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v %s", query, err, res.Body)
	}
	got := map[string]map[int64]float64{}
	for i, series := range resp.Data.Result {
		if i > 0 && !testLokiLabelsLess(resp.Data.Result[i-1].Metric, series.Metric) {
			t.Fatalf("%s: series out of Loki's label order: %s", query, res.Body)
		}
		key := binaryLabelKey(series.Metric)
		if got[key] != nil {
			t.Fatalf("%s: series %s twice in %s", query, key, res.Body)
		}
		got[key] = map[int64]float64{}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, err := strconv.ParseFloat(point[1].(string), 64)
			if err != nil {
				t.Fatalf("%s: value %v", query, point[1])
			}
			got[key][int64(point[0].(float64))] = v
		}
	}
	return got, resp.Data.ResultType
}

// binaryAggregationRows: three streams in two jobs, inside the minute before
// binaryAggregationAt. count_over_time per (job, pod) is p1=3, p2=1, p3=2 and
// sum_over_time of amount is p1=6, p2=4, p3=12.
const binaryAggregationAt = int64(1_800_000_060) * 1e9

func binaryAggregationRows() []emuRow {
	row := func(offset int64, stream string, value float64) emuRow {
		return emuRow{ts: binaryAggregationAt - offset*1e9, stream: stream, value: value}
	}
	p1, p2, p3 := `{app="uw",job="a",pod="p1"}`, `{app="uw",job="a",pod="p2"}`, `{app="uw",job="b",pod="p3"}`
	return []emuRow{
		row(50, p1, 1), row(30, p1, 2), row(10, p1, 3),
		row(40, p2, 4),
		row(45, p3, 5), row(5, p3, 7),
	}
}

// A named parser extraction exposes only its named labels to Loki. Fields
// VictoriaLogs stores beside the line must not become series labels after a
// comparison inside a without aggregation.
func TestAggregationOverBinaryWithoutNamedExtractionKeepsStreamIdentity(t *testing.T) {
	rows, end := extractionListFixture()
	for _, tc := range []struct {
		parser string
		groups int
		route  bool
	}{
		{"logfmt amount", 2, false},
		{"logfmt amount, route", 6, true},
	} {
		t.Run(tc.parser, func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
			query := `sum without (pod) (rate({app="uw"} | ` + tc.parser + ` | unwrap amount [5m]) > 0)`
			got, _ := binaryAggregationAnswerFor(t, emu, "0", query, end-900e9, end, time.Minute, func(cfg *Config) {
				cfg.LabelStyle = LabelStyleUnderscores
				cfg.MetadataFieldMode = MetadataFieldModeTranslated
			})
			if len(got) != tc.groups {
				t.Fatalf("got %d series, want %d; stats=%v raw=%v", len(got), tc.groups, emu.endpointCalls("/select/logsql/stats_query_range"), emu.endpointCalls("/select/logsql/query"))
			}
			for labels := range got {
				if strings.Contains(labels, `"id"`) || (strings.Contains(labels, `"route"`) != tc.route) {
					t.Fatalf("wrong parser exposure in series labels: %s", labels)
				}
			}
		})
	}
}

// A long comparison can create more per-stream samples than the manual
// response budget allows before sum without collapses them. It must still
// answer the full evaluation grid with the same values and labels.
func TestAggregationOverBinaryWideRangeRetriesWithinResponseBudget(t *testing.T) {
	const step = 20 * time.Second
	end := binaryAggregationAt
	start := end - int64(2*time.Hour)
	rows := make([]emuRow, 0, 5*361)
	for pod := 0; pod < 5; pod++ {
		stream := fmt.Sprintf(`{app="uw",job="a",pod="p%d"}`, pod)
		for at := start; at <= end; at += int64(step) {
			rows = append(rows, emuRow{ts: at, stream: stream, value: 1000})
		}
	}
	query := `sum without (pod) (rate({app="uw"} | logfmt amount | unwrap amount [20s]) > 40)`
	want, _ := binaryAggregationAnswer(t, &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}, query, start, end, step)
	if len(want) != 1 {
		t.Fatalf("fixture should return one folded series, got %v", want)
	}
	for _, points := range want {
		if len(points) < 300 {
			t.Fatalf("fixture should exercise the full evaluation grid, got %d points", len(points))
		}
	}
	got, _ := binaryAggregationAnswerFor(t, &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}, "0", query, start, end, step, func(cfg *Config) {
		cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 64 << 10
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("budgeted range differs: got %v, want %v", got, want)
	}
}

func TestAggregationOverBinaryBareLogMetricsUseStatsBuckets(t *testing.T) {
	emu := &vlEmulator{t: t, rows: binaryAggregationRows(), version: "v1.52.0"}
	query := `sum by (job) (rate({app="uw"}[1m]) / count_over_time({app="uw"}[1m]))`
	got, _ := binaryAggregationAnswer(t, emu, query, binaryAggregationAt-60e9, binaryAggregationAt, time.Minute)
	if len(got) == 0 {
		t.Fatal("expected nonempty ratio")
	}
	if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("bare log metrics read raw rows instead of stats buckets: %v", raw)
	}
}

func TestAggregationOverBinarySumRateScalarUsesGroupedStats(t *testing.T) {
	rows := binaryAggregationRows()
	query := `sum by (job) (rate({app="uw"}[1m]) / 2)`
	base := `sum by (job) (rate({app="uw"}[1m])) / 2`
	for _, step := range []time.Duration{0, time.Minute} {
		emu := &vlEmulator{t: t, rows: rows, version: "v1.52.0"}
		got, _ := binaryAggregationAnswer(t, emu, query, binaryAggregationAt-60e9, binaryAggregationAt, step)
		want, _ := binaryAggregationAnswer(t, &vlEmulator{t: t, rows: rows, version: "v1.52.0"}, base, binaryAggregationAt-60e9, binaryAggregationAt, step)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("step %v: got %v, want %v", step, got, want)
		}
		for _, stats := range emu.endpointCalls("/select/logsql/stats_query_range") {
			if strings.Contains(stats, "_stream") {
				t.Fatalf("step %v: retained expensive per-stream grouping: %s", step, stats)
			}
		}
	}
}

func TestAggregationOverBinaryRateCountRatioUsesActiveStreamCount(t *testing.T) {
	rows := binaryAggregationRows()
	query := `sum by (job) (rate({app="uw"}[1m]) / count_over_time({app="uw"}[1m]))`
	base := `count by (job) (count_over_time({app="uw"}[1m])) / 60`
	for _, step := range []time.Duration{0, time.Minute} {
		emu := &vlEmulator{t: t, rows: rows, version: "v1.52.0"}
		got, _ := binaryAggregationAnswer(t, emu, query, binaryAggregationAt-60e9, binaryAggregationAt, step)
		want, _ := binaryAggregationAnswer(t, &vlEmulator{t: t, rows: rows, version: "v1.52.0"}, base, binaryAggregationAt-60e9, binaryAggregationAt, step)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("step %v: got %v, want %v", step, got, want)
		}
		if step > 0 && len(emu.endpointCalls("/select/logsql/stats_query_range")) != 1 {
			t.Fatalf("step %v: expected one grouped stats read, got %v", step, emu.endpointCalls("/select/logsql/stats_query_range"))
		}
	}
}

// TestAggregationOverBinaryEdgeCases: a vector aggregation over a binary
// expression aggregates the binary's answer, per step, like Loki
// (pkg/logql/evaluator.go newBinOpStepEvaluator then VectorAggEvaluator.Next):
// both operands, vector matching on the full label set or on()/ignoring() with
// group_left, a scalar on either side, nested binaries, a comparison filtering
// series or answering 0/1 with bool, then sum, avg, min, max and count grouped
// by labels, without labels or not at all, and an aggregation over that.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryEdgeCases(t *testing.T) {
	const (
		count  = `sum by (job, pod) (count_over_time({app="uw"}[1m]))`
		amount = `sum by (job, pod) (sum_over_time({app="uw"} | unwrap amount [1m]))`
		perJob = `sum by (job) (count_over_time({app="uw"}[1m]))`
		// emptyCount reads a window an hour back, where no line is.
		emptyCount = `sum by (job, pod) (count_over_time({app="uw"}[1m] offset 1h))`
	)
	ja, jb, none := `{"job":"a"}`, `{"job":"b"}`, `{}`
	for _, tc := range []struct {
		name, query string
		want        map[string]float64
	}{
		{"sum by over metric/metric", `sum by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 6, jb: 6}},
		{"avg by", `avg by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 3, jb: 6}},
		{"min by", `min by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 6}},
		{"max by", `max by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 4, jb: 6}},
		{"count by", `count by (job) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 1}},
		{"ungrouped sum", `sum(` + amount + ` / ` + count + `)`, map[string]float64{none: 12}},
		{"sum by ()", `sum by () (` + amount + ` / ` + count + `)`, map[string]float64{none: 12}},
		{"sum without", `sum without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 6, jb: 6}},
		{"count without folds once", `count without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 2, jb: 1}},
		{"avg without", `avg without (pod) (` + amount + ` / ` + count + `)`, map[string]float64{ja: 3, jb: 6}},
		{"metric op scalar", `sum by (job) (` + count + ` * 2)`, map[string]float64{ja: 8, jb: 4}},
		{"scalar op metric", `sum(2 * ` + count + `)`, map[string]float64{none: 12}},
		{"metric divided by scalar", `sum(` + count + ` / 2)`, map[string]float64{none: 3}},
		{"comparison filters", `count(` + count + ` > 1)`, map[string]float64{none: 2}},
		{"comparison keeps the vector value", `sum(` + count + ` > 1)`, map[string]float64{none: 5}},
		{"scalar on the left of a comparison", `sum(1 < ` + count + `)`, map[string]float64{none: 5}},
		{"comparison with bool", `sum(` + count + ` > bool 1)`, map[string]float64{none: 2}},
		{"count over bool counts every series", `count(` + count + ` > bool 1)`, map[string]float64{none: 3}},
		{"vector comparison keeps the left value", `sum by (job) (` + amount + ` > ` + count + `)`, map[string]float64{ja: 10, jb: 12}},
		{"vector comparison with bool", `max by (job) (` + count + ` >= bool ` + amount + `)`, map[string]float64{ja: 0, jb: 0}},
		{"nested binary", `sum by (job) ((` + count + ` + ` + count + `) / 2)`, map[string]float64{ja: 4, jb: 2}},
		{"on group_left", `sum by (job) (` + count + ` / on (job) group_left ` + perJob + `)`, map[string]float64{ja: 1, jb: 1}},
		{"ignoring group_left", `max(` + count + ` / ignoring (pod) group_left ` + perJob + `)`, map[string]float64{none: 1}},
		// The shape of semantics/binary-ignoring-matching: the ignored label is on
		// no series, so both sides match on job and the outer sum adds them.
		{"ignoring an absent label under an outer sum", `sum by (job) (` + perJob + ` + ignoring(machine) sum by (job, machine) (count_over_time({app="uw"}[1m])))`, map[string]float64{ja: 8, jb: 4}},
		{"aggregation over the aggregation", `max(sum by (job) (` + amount + ` / ` + count + `))`, map[string]float64{none: 6}},
		{"empty after the filter", `sum(` + count + ` > 100)`, map[string]float64{}},
		// An operand with no series (its window, an hour back, holds no line):
		// the join has none, so the aggregation has none (vectorBinop matches
		// nothing; VectorAggEvaluator.Next folds an empty vector to an empty one).
		{"empty right operand", `sum by (job) (` + count + ` / ` + emptyCount + `)`, map[string]float64{}},
		{"empty left operand", `sum(` + emptyCount + ` * 2)`, map[string]float64{}},
		{"empty operand under count", `count(` + amount + ` > ` + emptyCount + `)`, map[string]float64{}},
		// A grouping by a label no series has is one group without labels.
		{"by a label no series has", `sum by (absent) (` + amount + ` / ` + count + `)`, map[string]float64{none: 12}},
		{"without every label", `count without (job, pod) (` + amount + ` / ` + count + `)`, map[string]float64{none: 3}},
		{"without a label no series has", `max without (absent) (` + amount + ` / ` + count + `)`, map[string]float64{`{"job":"a","pod":"p1"}`: 2, `{"job":"a","pod":"p2"}`: 4, `{"job":"b","pod":"p3"}`: 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, version := range []string{"v1.52.0", "v1.49.0"} {
				emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: version}
				got, resultType := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt, 0)
				if resultType != "vector" {
					t.Fatalf("%s %s: resultType %q, want vector", version, tc.query, resultType)
				}
				flat := map[string]float64{}
				for key, points := range got {
					for ts, v := range points {
						if ts != binaryAggregationAt/1e9 {
							t.Fatalf("%s %s: sample at %d, want %d", version, tc.query, ts, binaryAggregationAt/1e9)
						}
						flat[key] = v
					}
				}
				if !reflect.DeepEqual(flat, tc.want) {
					t.Fatalf("%s %s:\n got %v\nwant %v", version, tc.query, flat, tc.want)
				}
				// Each operand is a VictoriaLogs stats query: no raw rows are read.
				if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
					t.Fatalf("%s %s: read raw rows: %v", version, tc.query, raw)
				}
			}
		})
	}
}

// The same on query_range: every step aggregates the binary's samples at that
// step, and a step where the binary has no sample has no point.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query_range
func TestAggregationOverBinaryRange(t *testing.T) {
	rows := binaryAggregationRows()
	// A second minute: p1 once (amount 10), p3 once (amount 1); p2 is silent.
	rows = append(rows,
		emuRow{ts: binaryAggregationAt + 20e9, stream: `{app="uw",job="a",pod="p1"}`, value: 10},
		emuRow{ts: binaryAggregationAt + 30e9, stream: `{app="uw",job="b",pod="p3"}`, value: 1},
	)
	// Rates: the ratio of the two is amount per line, as above. A sum_over_time
	// range query is registered apart (semantics/unwrap-over-time-range-windows).
	const (
		count  = `sum by (job, pod) (rate({app="uw"}[1m]))`
		amount = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m]))`
	)
	t0, t1 := binaryAggregationAt/1e9, binaryAggregationAt/1e9+60
	for _, tc := range []struct {
		query string
		want  map[string]map[int64]float64
	}{
		{`sum by (job) (` + amount + ` / ` + count + `)`, map[string]map[int64]float64{`{"job":"a"}`: {t0: 6, t1: 10}, `{"job":"b"}`: {t0: 6, t1: 1}}},
		{`count(` + count + ` * 60 > bool 1)`, map[string]map[int64]float64{`{}`: {t0: 3, t1: 2}}},
		{`sum(60 * ` + count + ` > 1)`, map[string]map[int64]float64{`{}`: {t0: 5}}},
		{`avg without (pod) (` + count + ` * 180)`, map[string]map[int64]float64{`{"job":"a"}`: {t0: 6, t1: 3}, `{"job":"b"}`: {t0: 6, t1: 3}}},
	} {
		for _, version := range []string{"v1.52.0", "v1.49.0"} {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
			got, resultType := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt+60e9, time.Minute)
			if resultType != "matrix" {
				t.Fatalf("%s %s: resultType %q, want matrix", version, tc.query, resultType)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s %s:\n got %v\nwant %v", version, tc.query, got, tc.want)
			}
		}
	}
}

// A multi-tenant request (X-Scope-OrgID "0|1") is evaluated per tenant and
// merged with each series labelled by its tenant, as for any other metric
// query (multitenant.go): an aggregation over a binary is answered per tenant
// like a single tenant's, on both endpoints, not refused as a log query.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryMultiTenant(t *testing.T) {
	const (
		count  = `sum by (job, pod) (count_over_time({app="uw"}[1m]))`
		amount = `sum by (job, pod) (sum_over_time({app="uw"} | unwrap amount [1m]))`
	)
	t0 := binaryAggregationAt / 1e9
	for _, tc := range []struct {
		query string
		want  map[string]float64
	}{
		{`sum(2 * ` + count + `)`, map[string]float64{`{}`: 12}},
		{`sum by (job) (` + amount + ` / ` + count + `)`, map[string]float64{`{"job":"a"}`: 6, `{"job":"b"}`: 6}},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.49.0"}
			got, resultType := binaryAggregationAnswerFor(t, emu, "0|1", tc.query, binaryAggregationAt, binaryAggregationAt, step)
			if want := map[bool]string{true: "vector", false: "matrix"}[step == 0]; resultType != want {
				t.Fatalf("step=%v %s: resultType %q, want %q", step, tc.query, resultType, want)
			}
			want := map[string]map[int64]float64{}
			for _, tenant := range []string{"0", "1"} {
				for key, v := range tc.want {
					var labels map[string]string
					if err := json.Unmarshal([]byte(key), &labels); err != nil {
						t.Fatal(err)
					}
					labels["__tenant_id__"] = tenant
					want[binaryLabelKey(labels)] = map[int64]float64{t0: v}
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, tc.query, got, want)
			}
		}
	}
}

// The registry's discovered shape (cluster f2066991), with and without its label_replace:
// one operand parses JSON, the other logfmt. Each operand reaches VictoriaLogs
// with its own parser, though the whole query names both (the handler drops
// one of two parsers it finds in a query), and the answer is a sample type on
// both endpoints, not the 400 for a log query or a stream.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryKeepsEachOperandsParser(t *testing.T) {
	const inner = `sum by (job) ( sum_over_time( {app="uw"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] ) / count_over_time({app="uw"} | logfmt | label_format foo=bar[5m]) )`
	for _, step := range []time.Duration{0, time.Minute} {
		for _, query := range []string{inner, `label_replace( ` + inner + `, "foo", "$1", "service", "(.*):.*" )`} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			_, resultType := binaryAggregationAnswer(t, emu, query, binaryAggregationAt-300e9, binaryAggregationAt, step)
			if want := map[bool]string{true: "vector", false: "matrix"}[step == 0]; resultType != want {
				t.Fatalf("step=%v: resultType %q, want %q", step, resultType, want)
			}
			var jsonOperand, logfmtOperand bool
			for _, call := range emu.calls {
				if call.path == "/select/logsql/query" && strings.Contains(call.query, " / ") {
					continue // the parser probe of the whole query string
				}
				jsonOperand = jsonOperand || (strings.Contains(call.query, "unpack_json") && strings.Contains(call.query, "level=error"))
				logfmtOperand = logfmtOperand || (strings.Contains(call.query, "unpack_logfmt") && !strings.Contains(call.query, "level=error"))
			}
			if !jsonOperand || !logfmtOperand {
				t.Fatalf("step=%v: an operand lost its parser (json %v, logfmt %v): %v", step, jsonOperand, logfmtOperand, emu.calls)
			}
		}
	}
}

// The parser probe of a query that names both `| json` and `| logfmt` drops
// one of them from the handler's query (preferWorkingParser,
// removeParserStage). It rewrote a binary too, and when the dropped stage was
// the last one before an operand's range the rewrite cut the query there
// (`... count_over_time({app="uw"}`): an aggregation over the binary was no
// longer one, so /query answered 400 "log queries are not supported as an
// instant query type" (the e2e TestCompat_AggregationOverBinaryMixedParsers
// shape) and /query_range answered log streams, and a top-level binary
// answered its left operand alone with a 200. The probe now leaves a binary to
// its operands. The lines here are JSON, so the probe prefers json. Loki evaluates
// each operand with its own parser: sum_over_time of the JSON amount by job
// (a: 10, b: 12) over the lines by job (a: 4, b: 2); a logfmt parse error only
// labels a line, which `sum by (job)` drops.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryMixedParsersAfterTheParserProbe(t *testing.T) {
	const (
		amount = `sum by (job) (sum_over_time({app="uw"} | json | unwrap amount [5m]))`
		lines  = `sum by (job) (count_over_time({app="uw"} | logfmt [5m]))`
	)
	want := map[string]float64{`{"job":"a"}`: 2.5, `{"job":"b"}`: 6}
	for _, tc := range []struct {
		query string
		want  map[string]float64
	}{
		{`sum by (job) (` + amount + ` / ` + lines + `)`, want},
		{`max(` + amount + ` / ` + lines + `)`, map[string]float64{`{}`: 6}},
		{`label_replace(sum by (job) (` + amount + ` / ` + lines + `), "dst", "$1", "job", "(.*)")`, map[string]float64{`{"dst":"a","job":"a"}`: 2.5, `{"dst":"b","job":"b"}`: 6}},
		{amount + ` / ` + lines, want},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			rows := binaryAggregationRows()
			for i := range rows {
				rows[i].msg = fmt.Sprintf(`{"amount":%g}`, rows[i].value)
			}
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.49.0"}
			got, resultType := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt-300e9, binaryAggregationAt, step)
			if want := map[bool]string{true: "vector", false: "matrix"}[step == 0]; resultType != want {
				t.Fatalf("step=%v %s: resultType %q, want %q", step, tc.query, resultType, want)
			}
			flat := map[string]float64{}
			for key, points := range got {
				for ts, v := range points {
					if ts != binaryAggregationAt/1e9 {
						t.Fatalf("step=%v %s: sample at %d, want %d", step, tc.query, ts, binaryAggregationAt/1e9)
					}
					flat[key] = v
				}
			}
			if !reflect.DeepEqual(flat, tc.want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, tc.query, flat, tc.want)
			}
		}
	}
}

// The parser probe leaves a binary, and an aggregation over one, to its
// operands: each operand is its own handler's query and keeps its own parser
// (pkg/logql/evaluator.go:385). A query of one operand that names both
// parsers is still probed.
//
// conformance: semantics/aggregation-over-binary-dropped
func TestParserProbeLeavesBinariesToTheirOperands(t *testing.T) {
	const (
		amount = `sum by (job) (sum_over_time({app="uw"} | json | unwrap amount [5m]))`
		lines  = `sum by (job) (count_over_time({app="uw"} | logfmt [5m]))`
	)
	for _, tc := range []struct {
		query string
		keep  bool
	}{
		{amount + ` / ` + lines, true},
		{`sum by (job) (` + amount + ` / ` + lines + `)`, true},
		{`max(sum by (job) (` + amount + ` / ` + lines + `))`, true},
		{`label_replace(sum by (job) (` + amount + ` / ` + lines + `), "dst", "$1", "job", "(.*)")`, true},
		{`sum(` + amount + ` > bool 1) / ` + lines, true},
		{`sum by (job) (count_over_time({app="uw"} | json | logfmt [5m]))`, false},
		{`count_over_time({app="uw"} | json | logfmt [5m])`, false},
		{`topk(2, ` + amount + ` / ` + lines + `)`, false}, // no aggregation this path folds: unchanged routing
	} {
		if _, err := logqlpkg.Parse(tc.query); err != nil {
			t.Fatalf("%s: the input does not parse: %v", tc.query, err)
		}
		if got := operandsKeepTheirParsers(tc.query); got != tc.keep {
			t.Fatalf("%s: operandsKeepTheirParsers %v, want %v", tc.query, got, tc.keep)
		}
	}
}

// Loki takes an offset on one side of the binary only (each side is its own
// step evaluator); so does the proxy under an aggregation, as for a bare binary.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query
func TestAggregationOverBinaryMixedOffset(t *testing.T) {
	rows := append(binaryAggregationRows(),
		emuRow{ts: binaryAggregationAt + 20e9, stream: `{app="uw",job="a",pod="p1"}`, value: 10},
		emuRow{ts: binaryAggregationAt + 30e9, stream: `{app="uw",job="b",pod="p3"}`, value: 1},
	)
	// A minute earlier: p1=3, p2=1, p3=2; now: p1=1, p3=1.
	query := `sum(sum by (job, pod) (count_over_time({app="uw"}[1m] offset 1m)) / sum by (job, pod) (count_over_time({app="uw"}[1m])))`
	emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
	got, _ := binaryAggregationAnswer(t, emu, query, 0, binaryAggregationAt+60e9, 0)
	if want := map[string]map[int64]float64{`{}`: {binaryAggregationAt/1e9 + 60: 5}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A top-level binary whose first operand is a label_replace stays on the binary
// evaluator, and the operand applies its own transform: only a transform around
// the whole expression is applied to the binary's answer.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query
func TestBinaryWithLabelReplaceOperandStaysBinary(t *testing.T) {
	const count = `sum by (job, pod) (count_over_time({app="uw"}[1m]))`
	for query, want := range map[string]map[string]float64{
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") / on (job, pod) ` + count: {
			`{"job":"a","pod":"p1"}`: 1, `{"job":"a","pod":"p2"}`: 1, `{"job":"b","pod":"p3"}`: 1},
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") * on (job, pod) ` + count: {
			`{"job":"a","pod":"p1"}`: 9, `{"job":"a","pod":"p2"}`: 1, `{"job":"b","pod":"p3"}`: 4},
		`label_replace(` + count + `, "dst", "$1", "job", "(.*)") * 2`: {
			`{"dst":"a","job":"a","pod":"p1"}`: 6, `{"dst":"a","job":"a","pod":"p2"}`: 2, `{"dst":"b","job":"b","pod":"p3"}`: 4},
		`sum by (dst) (label_replace(` + count + `, "dst", "$1", "job", "(.*)") / ` + count + `)`: {},
		`sum by (dst) (label_replace(` + count + `, "dst", "$1", "job", "(.*)") / ignoring (dst) group_left ` + count + `)`: {
			`{"dst":"a"}`: 2, `{"dst":"b"}`: 1},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			got, _ := binaryAggregationAnswer(t, emu, query, binaryAggregationAt, binaryAggregationAt, step)
			flat := map[string]float64{}
			for key, points := range got {
				for _, v := range points {
					flat[key] = v
				}
			}
			if !reflect.DeepEqual(flat, want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, query, flat, want)
			}
		}
	}
}

// The same offset on both sides (the discovered shape's second form) shifts the
// windows and stamps the samples at the requested times, on both endpoints.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryUniformOffset(t *testing.T) {
	const (
		count  = `sum by (job, pod) (count_over_time({app="uw"}[1m] offset 1h))`
		amount = `sum by (job, pod) (sum_over_time({app="uw"} | unwrap amount [1m] offset 1h))`
		rate   = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m] offset 1h))`
	)
	at := binaryAggregationAt + 3600e9
	emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
	got, _ := binaryAggregationAnswer(t, emu, `sum by (job) (`+amount+` / `+count+`)`, 0, at, 0)
	if want := map[string]map[int64]float64{`{"job":"a"}`: {at / 1e9: 6}, `{"job":"b"}`: {at / 1e9: 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("instant: got %v, want %v", got, want)
	}
	emu = &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
	got, _ = binaryAggregationAnswer(t, emu, `max(`+rate+` / `+count+`)`, at-60e9, at, time.Minute)
	if want := map[string]map[int64]float64{`{}`: {at / 1e9: 6.0 / 60}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("range: got %v, want %v", got, want)
	}
}

// A label_replace around the aggregation (the discovered shape,
// cluster f2066991) applies to the folded answer, on both endpoints: the
// aggregation is not handed to the translator, which kept its first operand.
//
// conformance: semantics/aggregation-over-binary-dropped, loki_api_v1_query, loki_api_v1_query_range
func TestAggregationOverBinaryUnderLabelTransforms(t *testing.T) {
	const (
		count  = `sum by (job, pod) (rate({app="uw"}[1m]))`
		amount = `sum by (job, pod) (rate({app="uw"} | unwrap amount [1m]))`
	)
	inner := `sum by (job) (` + amount + ` / ` + count + `)`
	at := binaryAggregationAt / 1e9
	for _, tc := range []struct {
		query string
		want  map[string]float64
	}{
		{`label_replace(` + inner + `, "dst", "$1", "job", "(.*)")`, map[string]float64{`{"dst":"a","job":"a"}`: 6, `{"dst":"b","job":"b"}`: 6}},
		{`label_replace(label_replace(` + inner + `, "dst", "$1", "job", "(.*)"), "job", "x", "", "")`, map[string]float64{`{"dst":"a","job":"x"}`: 6, `{"dst":"b","job":"x"}`: 6}},
	} {
		for _, step := range []time.Duration{0, time.Minute} {
			emu := &vlEmulator{t: t, rows: binaryAggregationRows(), field: "amount", version: "v1.52.0"}
			got, _ := binaryAggregationAnswer(t, emu, tc.query, binaryAggregationAt, binaryAggregationAt, step)
			want := map[string]map[int64]float64{}
			for key, v := range tc.want {
				want[key] = map[int64]float64{at: v}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("step=%v %s:\n got %v\nwant %v", step, tc.query, got, want)
			}
		}
	}
}

// testLokiLabelsLess is labels.Compare(a, b) < 0 (Prometheus model/labels),
// written apart from the proxy's: the sorted (name, value) pairs compared in
// turn, a shorter list first when one is a prefix of the other.
func testLokiLabelsLess(a, b map[string]string) bool {
	pairs := func(m map[string]string) [][2]string {
		out := make([][2]string, 0, len(m))
		for k, v := range m {
			out = append(out, [2]string{k, v})
		}
		sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
		return out
	}
	pa, pb := pairs(a), pairs(b)
	for i := range pa {
		if i == len(pb) {
			return false
		}
		if pa[i] != pb[i] {
			if pa[i][0] != pb[i][0] {
				return pa[i][0] < pb[i][0]
			}
			return pa[i][1] < pb[i][1]
		}
	}
	return len(pa) < len(pb)
}

// A fold keeps every label set apart, a label value with a comma and an equals
// sign included, and answers the series in Loki's label order, which is not the
// order of their `name=value,...` text.
//
// conformance: semantics/aggregation-over-binary-dropped
func TestWithoutFoldLabelOrderAndKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		metric []map[string]string
		want   []map[string]string
	}{
		{"a name that extends another", []map[string]string{{"a0": "y"}, {"a": "x"}}, []map[string]string{{"a": "x"}, {"a0": "y"}}},
		{"a value that extends another", []map[string]string{{"route": "/a b"}, {"route": "/a", "status": "200"}}, []map[string]string{{"route": "/a", "status": "200"}, {"route": "/a b"}}},
		{"a value with a comma and an equals sign", []map[string]string{{"a": "b", "c": "d"}, {"a": "b,c=d"}}, []map[string]string{{"a": "b", "c": "d"}, {"a": "b,c=d"}}},
		{"a prefix first", []map[string]string{{"a": "x", "b": "y"}, {"a": "x"}}, []map[string]string{{"a": "x"}, {"a": "x", "b": "y"}}},
		// The typed fold's group key is length-prefixed text, whose order is
		// not Loki's either.
		{"an earlier name first, whatever the set's size", []map[string]string{{"x": "a"}, {"job": "a", "x": "a"}}, []map[string]string{{"job": "a", "x": "a"}, {"x": "a"}}},
		{"a longer value that sorts first", []map[string]string{{"pod": "b"}, {"pod": "ab"}, {"pod": "api-2"}, {"pod": "api-10"}}, []map[string]string{{"pod": "ab"}, {"pod": "api-10"}, {"pod": "api-2"}, {"pod": "b"}}},
	} {
		if tc.name == "a value with a comma and an equals sign" && metricKeyStr(tc.metric[0]) != metricKeyStr(tc.metric[1]) {
			t.Fatal("the two sets should share their name=value text, the key the fold no longer uses")
		}
		for _, resultType := range []string{"vector", "matrix"} {
			result := make([]map[string]any, 0, len(tc.metric))
			for _, metric := range tc.metric {
				entry := map[string]any{"metric": metric, "value": []any{1.0, "1"}}
				if resultType == "matrix" {
					entry = map[string]any{"metric": metric, "values": [][]any{{1.0, "1"}}}
				}
				result = append(result, entry)
			}
			body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Value  []any             `json:"value"`
						Values [][]any           `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(applyWithoutGrouping(body, []string{"absent"}, "count"), &resp); err != nil {
				t.Fatal(err)
			}
			var got []map[string]string
			for _, series := range resp.Data.Result {
				got = append(got, series.Metric)
				point := series.Value
				if resultType == "matrix" {
					point = series.Values[0]
				}
				if point[1] != "1" {
					t.Fatalf("%s %s: %v counts %v series, want 1 (folded with another)", tc.name, resultType, series.Metric, point[1])
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s %s: got %v, want %v", tc.name, resultType, got, tc.want)
			}
			for i := 1; i < len(tc.want); i++ {
				if !testLokiLabelsLess(tc.want[i-1], tc.want[i]) {
					t.Fatalf("%s: the expectation is not in Loki's order", tc.name)
				}
			}
		}
	}
}

// A label_replace rewrites the series of a vector or a matrix and keeps the
// answer's warnings and statistics.
//
// conformance: semantics/aggregation-over-binary-dropped
func TestLabelReplaceKeepsVectorAndWarnings(t *testing.T) {
	body := []byte(`{"status":"success","warnings":["partial"],"data":{"resultType":"vector","stats":{"summary":{"bytesProcessedPerSecond":1}},"result":[{"metric":{"job":"a"},"value":[1,"2"]}]}}`)
	got := applyLabelReplace(body, translator.LabelReplaceSpec{DstLabel: "dst", Replacement: "$1", SrcLabel: "job", Regex: "(.*)"})
	want := `{"status":"success","warnings":["partial"],"data":{"resultType":"vector","stats":{"summary":{"bytesProcessedPerSecond":1}},"result":[{"metric":{"dst":"a","job":"a"},"value":[1,"2"]}]}}`
	var g, w any
	if json.Unmarshal(got, &g) != nil || json.Unmarshal([]byte(want), &w) != nil || !reflect.DeepEqual(g, w) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
