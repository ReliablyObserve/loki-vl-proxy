package workload

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStepOf(t *testing.T) {
	cases := map[string]time.Duration{
		"":     0,
		"60":   time.Minute,
		"3600": time.Hour,
		"0.5":  500 * time.Millisecond,
		"1m":   time.Minute,
		"30s":  30 * time.Second,
		"0":    0,
		"-5":   0,
		"abc":  0,
	}
	for in, want := range cases {
		if got := StepOf(url.Values{"step": {in}}); got != want {
			t.Errorf("StepOf(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestFloorToStep(t *testing.T) {
	ns := time.Date(2026, 9, 14, 19, 47, 54, 100889666, time.UTC).UnixNano()
	got := FloorToStep(ns, time.Minute)
	if want := time.Date(2026, 9, 14, 19, 47, 0, 0, time.UTC).UnixNano(); got != want {
		t.Fatalf("floor to minute = %d, want %d", got, want)
	}
	if FloorToStep(ns, 0) != ns {
		t.Fatal("zero step must not change the timestamp")
	}
	if got := FloorToStep(-1, time.Second); got != -int64(time.Second) {
		t.Fatalf("negative floor = %d", got)
	}
}

// ref is deliberately not aligned to any step.
var ref = time.Date(2026, 9, 14, 19, 47, 54, 100889666, time.UTC)

func parseNs(t *testing.T, v string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", v, err)
	}
	return n
}

func allWorkloads() []Workload {
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	return append(ByName(names, ref), VLByName(names, ref)...)
}

func TestByNameAlignsRangeQueriesToStep(t *testing.T) {
	var ranged, instant int
	for _, w := range allWorkloads() {
		for _, q := range w.Queries {
			step := StepOf(q.Params)
			if step > 0 {
				ranged++
				for _, key := range []string{"start", "end"} {
					ns := parseNs(t, q.Params.Get(key))
					if ns%int64(step) != 0 {
						t.Errorf("%s/%s: %s=%d not aligned to step %s", w.Name, q.Name, key, ns, step)
					}
					if ns > ref.UnixNano() {
						t.Errorf("%s/%s: %s after the reference time", w.Name, q.Name, key)
					}
				}
				if end := parseNs(t, q.Params.Get("end")); ref.UnixNano()-end >= int64(step) {
					t.Errorf("%s/%s: end floored by more than one step", w.Name, q.Name)
				}
			}
			if v := q.Params.Get("time"); v != "" {
				instant++
				if parseNs(t, v) != ref.UnixNano() {
					t.Errorf("%s/%s: instant time changed by alignment", w.Name, q.Name)
				}
			}
		}
	}
	if ranged == 0 || instant == 0 {
		t.Fatalf("expected ranged and instant queries, got %d/%d", ranged, instant)
	}
}

func TestByNamePinsWindowsToReferenceTime(t *testing.T) {
	for _, w := range allWorkloads() {
		for _, q := range w.Queries {
			if StepOf(q.Params) > 0 {
				continue
			}
			if v := q.Params.Get("end"); v != "" && parseNs(t, v) != ref.UnixNano() {
				t.Errorf("%s/%s: end %s is not the reference time", w.Name, q.Name, v)
			}
		}
	}
}

func TestAlignToStepDoesNotMutateInput(t *testing.T) {
	w := Workload{Name: "x", Queries: []Query{{Name: "q", Params: url.Values{"start": {"61000000001"}, "end": {"125000000001"}, "step": {"60"}}}}}
	out := AlignToStep([]Workload{w})
	if w.Queries[0].Params.Get("start") != "61000000001" {
		t.Fatal("input params mutated")
	}
	if got := out[0].Queries[0].Params.Get("start"); got != "60000000000" {
		t.Fatalf("aligned start = %s", got)
	}
	if got := out[0].Queries[0].Params.Get("end"); got != "120000000000" {
		t.Fatalf("aligned end = %s", got)
	}
}

func TestLokiWorkloadsDoNotGroupOrBrowseByLevelLabel(t *testing.T) {
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	for _, w := range ByName(names, ref) {
		for _, q := range w.Queries {
			if strings.Contains(q.Path, "/label/level/") {
				t.Errorf("%s/%s browses the level label, which is not an index label in Loki", w.Name, q.Name)
			}
			if strings.Contains(q.Params.Get("query"), "by (level)") {
				t.Errorf("%s/%s groups by level without a parser", w.Name, q.Name)
			}
		}
	}
}

func TestVolumeAndIndexStatsAreExcludedFromComparison(t *testing.T) {
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	for _, w := range ByName(names, ref) {
		for _, q := range w.Compared().Queries {
			if strings.Contains(q.Path, "/index/volume") || strings.Contains(q.Path, "/index/stats") {
				t.Errorf("%s/%s (%s) is in the compared set", w.Name, q.Name, q.Path)
			}
		}
		for _, q := range w.ExcludedQueries() {
			if q.Excluded == "" {
				t.Errorf("%s/%s excluded without a reason", w.Name, q.Name)
			}
		}
	}
}

func TestVLWorkloadsUseLogsQLParserPipes(t *testing.T) {
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality"}
	for _, w := range VLByName(names, ref) {
		for _, q := range w.Queries {
			query := q.Params.Get("query")
			if strings.Contains(query, "| json") || strings.Contains(query, "| logfmt") {
				t.Errorf("%s/%s uses a LogQL parser stage in LogsQL: %s", w.Name, q.Name, query)
			}
		}
	}
}

func TestLookback(t *testing.T) {
	cases := map[string]time.Duration{
		`{app="a"}`:                                                                  0,
		`{app="a"} |~ "status.:(4|5)[0-9][0-9]"`:                                     0,
		`sum by (app) (rate({namespace="prod"}[5m]))`:                                5 * time.Minute,
		`sum(bytes_rate({namespace="prod"}[1h]))`:                                    time.Hour,
		`sum(rate({a="1"} |= "[2h]" [5m])) / sum(rate({a="1"}[10m])) * 100`:          10 * time.Minute,
		"sum(rate({a=\"1\"} |~ `[7d]` [1m]))":                                        time.Minute,
		`max_over_time(sum(rate({a="1"}[5m]))[1h:1m])`:                               time.Hour + 5*time.Minute,
		`sum(count_over_time({a="1"}[1h30m] offset 2h))`:                             3*time.Hour + 30*time.Minute,
		`quantile_over_time(0.99, {a="1"} | json | unwrap latency_ms [5m]) by (app)`: 5 * time.Minute,
		`count_over_time({a="1"}[2d])`:                                               48 * time.Hour,
		`{a="1"} | line_format "{{.a}} [5m]"`:                                        0,
	}
	for q, want := range cases {
		if got := Lookback(q); got != want {
			t.Errorf("Lookback(%s) = %s, want %s", q, got, want)
		}
	}
}

func TestEarliestEvaluated(t *testing.T) {
	p := url.Values{"query": {`sum(rate({a="1"}[1h]))`}, "start": {"7200000000000"}, "end": {"9000000000000"}, "step": {"60"}}
	got, ok := EarliestEvaluated(p)
	if !ok || got != 3600000000000 {
		t.Fatalf("got %d %v", got, ok)
	}
	inst := url.Values{"query": {`count_over_time({a="1"}[5m])`}, "time": {"600000000000"}}
	if got, _ := EarliestEvaluated(inst); got != 300000000000 {
		t.Fatalf("instant got %d", got)
	}
}

func TestLongestWorkloadFitsFourSeededDays(t *testing.T) {
	// The documented --days=4 must cover every default and long-range window,
	// including step flooring and range-vector lookback.
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	dataStart := ref.Add(-4 * 24 * time.Hour)
	var longest time.Duration
	for _, w := range append(ByName(names, ref), VLByName(names, ref)...) {
		for _, q := range w.Queries {
			if e, ok := EarliestEvaluated(q.Params); ok {
				if d := time.Duration(ref.UnixNano() - e); d > longest {
					longest = d
				}
				if e < dataStart.UnixNano() {
					t.Errorf("%s/%s reads %s before a 4-day seed starts", w.Name, q.Name, time.Duration(dataStart.UnixNano()-e))
				}
			}
		}
	}
	if longest < 72*time.Hour || longest > 75*time.Hour {
		t.Fatalf("longest evaluated window %s; update the documented --days if workloads changed", longest)
	}
}

func TestRangeQueriesStayWithinLokiResolutionLimit(t *testing.T) {
	// Loki and the proxy answer 400 when (end-start)/step exceeds 11,000
	// points per series; no Loki workload may send such a request.
	names := []string{"small", "heavy", "long_range", "compute", "unindexed_scan", "high_cardinality", "machinery"}
	for _, w := range ByName(names, ref) {
		for _, q := range w.Queries {
			step := StepOf(q.Params)
			if !strings.HasPrefix(q.Path, "/loki/") || step <= 0 {
				continue
			}
			start, errS := strconv.ParseInt(q.Params.Get("start"), 10, 64)
			end, errE := strconv.ParseInt(q.Params.Get("end"), 10, 64)
			if errS != nil || errE != nil {
				t.Fatalf("%s/%s: unparseable start/end", w.Name, q.Name)
			}
			if points := time.Duration(end-start) / step; points > 11000 {
				t.Errorf("%s/%s: %d points per series exceeds Loki's 11,000 limit", w.Name, q.Name, points)
			}
		}
	}
}

func TestEarliestTime(t *testing.T) {
	if _, ok := EarliestTime(url.Values{"query": {"x"}}); ok {
		t.Fatal("no bounds must report false")
	}
	got, ok := EarliestTime(url.Values{"start": {"100"}, "end": {"200"}})
	if !ok || got != 100 {
		t.Fatalf("got %d %v", got, ok)
	}
	got, ok = EarliestTime(url.Values{"time": {"300"}})
	if !ok || got != 300 {
		t.Fatalf("got %d %v", got, ok)
	}
}
