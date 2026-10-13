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

// detected_level is the level Loki sets at ingest, normalized
// (distributor/field_detection.go: a level stream label, else a top-level
// level key of the JSON body, spelled info, INFO, inf or information alike).
// The row evaluator, which answers every `| json` shape the stats pushdown
// does not take and falls back for the ones it cannot serve, derives it the
// same way: from the stream's level, else from the body's level key, never
// from the spelling as stored.
// conformance: semantics/json-metric-detected-level-derivation
func TestOrderedJSONDetectedLevelLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	type line struct {
		stream map[string]string
		msg    string
		level  string // the detected_level Loki sets
	}
	stream := func(level string) map[string]string {
		labels := map[string]string{"app": "api", "env": "prod"}
		if level != "" {
			labels["level"] = level
		}
		return labels
	}
	lines := []line{
		{stream("INFO"), `{"n":1}`, "info"},
		{stream("information"), `{"n":2}`, "info"},
		{stream("Warning"), `{"n":3}`, "warn"},
		{stream(""), `{"level":"inf","n":4}`, "info"},
		{stream(""), `{"level":"ERR","n":5}`, "error"},
		{stream(""), `{"level":"WARN","n":6}`, "warn"},
		{stream("debug"), `{"level":"error","n":7}`, "debug"}, // the stream's level wins
		{stream(""), `{"n":8}`, "unknown"},
	}
	var rows []pushdownRow
	for i, l := range lines {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: l.stream, msg: l.msg})
	}
	reference := func(bytes bool) map[string]map[int64]string {
		want := map[string]map[int64]string{}
		for at := start; !at.After(end); at = at.Add(time.Minute) {
			sums := map[string]int{}
			for i, l := range lines {
				if ts := rows[i].ts; ts.After(at.Add(-5*time.Minute)) && !ts.After(at) {
					if bytes {
						sums[l.level] += len(l.msg)
					} else {
						sums[l.level]++
					}
				}
			}
			for level, n := range sums {
				key := canonicalLabelsKey(map[string]string{"detected_level": level})
				if want[key] == nil {
					want[key] = map[int64]string{}
				}
				want[key][at.Unix()] = strconv.Itoa(n)
			}
		}
		return want
	}
	for _, tc := range []struct {
		name, pipeline string
		function       string
		pushdown       bool
	}{
		{"plain json (row evaluator)", `| json`, "count_over_time", false},
		{"plain json bytes (row evaluator)", `| json`, "bytes_over_time", false},
		{"json with the error dropped (plain pushdown)", `| json | drop __error__`, "count_over_time", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := `sum by (detected_level) (` + tc.function + `({env="prod"} ` + tc.pipeline + ` [5m]))`
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || plan.pushdown != tc.pushdown {
				t.Fatalf("plan ok=%v pushdown=%v, want pushdown=%v", ok, plan != nil && plan.pushdown, tc.pushdown)
			}
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			want := reference(tc.function == "bytes_over_time")
			if len(want) != 5 {
				t.Fatalf("fixture: %d expected series, want 5", len(want))
			}
			if got := detectedLevelRowMatrix(t, p, plan, start, end, time.Minute); !reflect.DeepEqual(got, want) {
				t.Fatalf("row evaluator %v, want %v", got, want)
			}
			if code, got, body := detectedLevelQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("proxy status %d %.200s, got %v, want %v", code, body, got, want)
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
