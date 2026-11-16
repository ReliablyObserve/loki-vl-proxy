package querycorpus_test

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The Loki differential of the series a range function reads after a parser that names the keys it extracts
// (`| logfmt amount, route`, `| json amount`): real Loki (the engine oracle, test/oracle/loki) and real VictoriaLogs
// (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) over the same data, through the proxy's real query and
// query_range handlers, as the metric differential runs them (plan_loki_vl_metric_diff_test.go).
//
// A Loki series after an extraction list is the stream plus the labels the list extracts that the line holds (an
// empty value is no label), less the unwrapped one; every other key of the line is no label. A vector aggregation
// over the range function folds those series.
//
//   - Data (extractionListFixture): logfmt lines on four streams of two namespaces and json lines on two more, a line
//     every 13 seconds per stream, each on a phase of its own; every line has an amount, a route of three, a unique
//     id and a message, and every fifth line no route.
//   - Requests (metricRequests): instant, and query_range with the step equal to the range, a fifth of it and three
//     times it.
//
// The answers are recorded (extractionListAnswersPath; LVP_RUN_ORACLES=1 records them again). Every run must match
// Loki, or be listed in extractionListKnown with the registry case that holds the difference open; a listed run that
// matches fails too, so the list only shrinks.

const extractionListAnswersPath = "testdata/extraction-list-identity-loki-diff.jsonl.gz"

// extractionListFuncs are the range functions over an unwrapped label.
var extractionListFuncs = []string{
	"max_over_time", "min_over_time", "avg_over_time", "sum_over_time", "first_over_time", "last_over_time",
	"quantile_over_time", "stddev_over_time", "rate", "rate_counter",
}

// extractionListParsers are the extraction lists, %s the selector's app.
var extractionListParsers = []string{
	`{app="%s"} | logfmt amount | unwrap amount [5m]`,
	`{app="%s"} | logfmt amount, route | unwrap amount [5m]`,
}

// extractionListHead is the call head of a function.
func extractionListHead(fn string) string {
	if fn == "quantile_over_time" {
		return fn + "(0.9, "
	}
	return fn + "("
}

// extractionListQueryTexts are the differential's queries.
func extractionListQueryTexts() []string {
	var out []string
	for _, fn := range extractionListFuncs {
		for _, p := range extractionListParsers {
			inner := extractionListHead(fn) + fmt.Sprintf(p, "uw") + ")"
			out = append(out, inner)
			out = append(out, "sum("+inner+")", "sum by (ns) ("+inner+")", "count by (ns) ("+inner+")")
		}
	}
	for _, p := range []string{
		`{app="uj"} | json amount | unwrap amount [5m]`,
		`{app="uj"} | json amount, route | unwrap amount [5m]`,
		`{app="uj"} | json a="amount", route | unwrap a [5m]`,
	} {
		for _, fn := range []string{"max_over_time", "rate", "quantile_over_time"} {
			inner := extractionListHead(fn) + p + ")"
			out = append(out, inner, "sum("+inner+")", "count by (ns) ("+inner+")")
		}
	}
	inner := `max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m])`
	out = append(out,
		`avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`max by (route) (`+inner+`)`,
		`min without (route) (`+inner+`)`,
		`topk(2, `+inner+`)`,
		`sort_desc(`+inner+`)`,
		`max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]) by (route)`,
		`max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]) without (pod)`,
		`max_over_time({app="uw"} | logfmt amount, route | __error__="" | unwrap amount [5m])`,
		`max_over_time({app="uw"} | logfmt amount, route | route="/x" | unwrap amount [5m])`,
		`count_over_time({app="uw"} | logfmt route [5m])`,
		`count by (ns) (count_over_time({app="uw"} | logfmt route [5m]))`,
		`sum(count_over_time({app="uw"} | logfmt route [5m]))`,
		`rate({app="uw"} | logfmt route [5m])`,
		`count by (ns) (rate({app="uw"} | logfmt route [5m]))`,
		`bytes_over_time({app="uw"} | logfmt route [5m])`,
		`count_over_time({app="uj"} | json route [5m])`,
		`count by (ns) (count_over_time({app="uj"} | json route [5m]))`,
		// The unwrapped label carries a value Loki cannot convert: the error answers.
		`max_over_time({app="ub"} | logfmt amount | unwrap amount [5m])`,
		`sum by (ns) (max_over_time({app="ub"} | logfmt amount | unwrap amount [5m]))`,
		`max_over_time({app="ub"} | logfmt amount | __error__="" | unwrap amount [5m])`,
	)
	return out
}

// extractionListQueries are the differential's queries over extractionListFixture.
func extractionListQueries(t *testing.T) []metricQuery {
	t.Helper()
	fix := extractionListFixture()
	var out []metricQuery
	for _, q := range extractionListQueryTexts() {
		p, err := planOfText(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out = append(out, metricQuery{q: q, id: queryID("logql", q), plan: p, fix: fix, reqs: metricRequests(q, firstRangeOf(p.Root))})
	}
	return out
}

// extractionListFixture is the data of every query.
func extractionListFixture() metricFixture {
	f := metricFixture{Streams: []map[string]string{
		{"app": "uw", "ns": "x", "pod": "a", "service_name": "uw"},
		{"app": "uw", "ns": "x", "pod": "b", "service_name": "uw"},
		{"app": "uw", "ns": "y", "pod": "a", "service_name": "uw"},
		{"app": "uw", "ns": "y", "pod": "c", "service_name": "uw"},
		{"app": "uj", "ns": "x", "pod": "d", "service_name": "uj"},
		{"app": "uj", "ns": "y", "pod": "e", "service_name": "uj"},
		{"app": "ub", "ns": "x", "pod": "f", "service_name": "ub"},
	}}
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 13 * time.Second
	routes := []string{"/x", "/y", "/z"}
	for s, labels := range f.Streams {
		for i := 0; ; i++ {
			at := first.Add(time.Duration(i)*every + time.Duration(s)*2*time.Second + time.Duration((i*7919+s*31)%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			amount := strconv.Itoa((i*7919+s*104729)%500 + 1)
			if labels["app"] == "ub" && i%7 == 3 {
				amount = "abc"
			}
			route := routes[(i/4+s)%3]
			id := "r" + strconv.Itoa(s) + "-" + strconv.Itoa(i)
			var line string
			if labels["app"] == "uj" {
				line = `{"amount":"` + amount + `","id":"` + id + `","msg":"paid"`
				if i%5 != 0 {
					line += `,"route":"` + route + `"`
				}
				line += "}"
			} else {
				line = "amount=" + amount + " id=" + id + " msg=paid"
				if i%5 != 0 {
					line += " route=" + route
				}
			}
			f.Entries = append(f.Entries, metricEntry{Stream: s, TS: at.UnixNano(), Line: line,
				Metadata: map[string]string{"detected_level": metricStreamLevels[s%len(metricStreamLevels)]}})
		}
	}
	sort.SliceStable(f.Entries, func(i, j int) bool { return f.Entries[i].TS < f.Entries[j].TS })
	f.Selected = len(f.Entries)
	return f
}

// extractionListKnown are the runs that differ from Loki, by query and request kind, with the registry case that
// holds the difference open.
var extractionListKnown = func() map[string]map[string]string {
	known := map[string]map[string]string{}
	for _, e := range extractionListKnownRuns {
		if known[e.q] == nil {
			known[e.q] = map[string]string{}
		}
		for _, k := range e.kinds {
			known[e.q][k] = e.id
		}
	}
	return known
}()

// extractionListKnownRuns are the differences of extractionListKnown, with the registry case of each:
//   - semantics/unwrap-extraction-list-series-identity: what the extraction list's identity does not reach yet: count
//     over the series, first, last, quantile, rate and rate_counter under an aggregation, a json alias, a ranking,
//     sort and a grouping by an extracted label, a count of lines;
//   - semantics/unwrap-per-series-identity-raw-rows: a range longer than the step keeps the stream's series;
//   - semantics/extraction-list-empty-label: Loki labels a line without an extracted key with the key empty;
//   - semantics/unwrap-rate-counter-window-extrapolation: rate_counter's sixth digit;
//   - semantics/unwrap-extraction-list-conversion-error: a value the unwrap cannot convert after an extraction list.
var extractionListKnownRuns = []struct {
	id, q string
	kinds []string
}{
	{"semantics/unwrap-extraction-list-series-identity", `sum(max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(min_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(min_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (min_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(avg_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (sum_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(first_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (first_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (first_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(first_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (first_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (first_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(last_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (last_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (last_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(last_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (last_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (last_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(quantile_over_time(0.9, {app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (quantile_over_time(0.9, {app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (quantile_over_time(0.9, {app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(stddev_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(stddev_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (stddev_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (rate({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(rate_counter({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (rate_counter({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(rate_counter({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum by (ns) (rate_counter({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (rate_counter({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(max_over_time({app="uj"} | json amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(max_over_time({app="uj"} | json amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (max_over_time({app="uj"} | json amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (rate({app="uj"} | json amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(quantile_over_time(0.9, {app="uj"} | json amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (quantile_over_time(0.9, {app="uj"} | json amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(max_over_time({app="uj"} | json a="amount", route | unwrap a [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (max_over_time({app="uj"} | json a="amount", route | unwrap a [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (rate({app="uj"} | json a="amount", route | unwrap a [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sum(quantile_over_time(0.9, {app="uj"} | json a="amount", route | unwrap a [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (quantile_over_time(0.9, {app="uj"} | json a="amount", route | unwrap a [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `max by (route) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `topk(2, max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `sort_desc(max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]) by (route)`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]) without (pod)`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `max_over_time({app="uw"} | logfmt amount, route | route="/x" | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count_over_time({app="uw"} | logfmt route [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (count_over_time({app="uw"} | logfmt route [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (rate({app="uw"} | logfmt route [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count_over_time({app="uj"} | json route [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-series-identity", `count by (ns) (count_over_time({app="uj"} | json route [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (min_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (min_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (avg_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (stddev_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `sum by (ns) (stddev_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/unwrap-per-series-identity-raw-rows", `avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`, []string{"step<range"}},
	{"semantics/extraction-list-empty-label", `max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `min_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `max_over_time({app="uj"} | json amount, route | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `max_over_time({app="uj"} | json a="amount", route | unwrap a [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `max_over_time({app="uw"} | logfmt amount, route | __error__="" | unwrap amount [5m])`, []string{"step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `rate({app="uw"} | logfmt route [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/extraction-list-empty-label", `bytes_over_time({app="uw"} | logfmt route [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-rate-counter-window-extrapolation", `rate_counter({app="uw"} | logfmt amount | unwrap amount [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-rate-counter-window-extrapolation", `rate_counter({app="uw"} | logfmt amount, route | unwrap amount [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-conversion-error", `max_over_time({app="ub"} | logfmt amount | unwrap amount [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-conversion-error", `sum by (ns) (max_over_time({app="ub"} | logfmt amount | unwrap amount [5m]))`, []string{"instant", "step=range", "step<range", "step>range"}},
	{"semantics/unwrap-extraction-list-conversion-error", `max_over_time({app="ub"} | logfmt amount | __error__="" | unwrap amount [5m])`, []string{"instant", "step=range", "step<range", "step>range"}},
}

// TestExtractionListIdentityMatchLoki replays the recorded answers through the handlers and holds every run to Loki's
// answer, or to its listed difference.
//
// conformance: semantics/unwrap-extraction-list-series-identity, semantics/unwrap-bare-range-exposes-parsed-labels
func TestExtractionListIdentityMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every shape on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	queries := extractionListQueries(t)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordMetricAnswers(t, extractionListAnswersPath, queries)
	}
	answers := readMetricAnswersFrom(t, extractionListAnswersPath)
	report := os.Getenv("LVP_XLIST_REPORT") == "1"
	type row struct{ match, runs int }
	table := map[string]*row{}
	seenKnown := map[string]bool{}
	for _, mq := range queries {
		rec := answers[mq.q]
		if rec == nil || rec.Inputs != mq.inputsHash() {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", mq.q)
		}
		for _, prof := range diffProfiles() {
			for _, mr := range mq.reqs {
				loki, ok := rec.Loki[mr.kind]
				if !ok {
					t.Fatalf("no Loki answer for %s %s: record them again", mr.kind, mq.q)
				}
				run := runMetricHandler(t, prof, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
				if missing := answerRequests(run.missing); len(missing) > 0 {
					t.Fatalf("%s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mr.kind, mq.q, missing[0])
				}
				if os.Getenv("LVP_XLIST_SENT") != "" && strings.Contains(mq.q, os.Getenv("LVP_XLIST_SENT")) && prof.name == diffProfiles()[0].name && mr.kind == "instant" {
					for _, k := range run.sent {
						t.Logf("SENT %s | %.400s", mq.q, k)
					}
				}
				d := compareMetric(loki, run)
				key := prof.name + " " + mr.kind
				if table[key] == nil {
					table[key] = &row{}
				}
				table[key].runs++
				known := extractionListKnown[mq.q][mr.kind]
				seenKnown[mq.q+"\x00"+mr.kind] = true
				switch {
				case report:
					if len(d.classes) == 0 {
						table[key].match++
						continue
					}
					if prof.name == diffProfiles()[0].name {
						var cs []string
						for c, ex := range d.classes {
							cs = append(cs, c+": "+ex)
						}
						sort.Strings(cs)
						t.Logf("DIFF %s | %s | %.600s", mr.kind, mq.q, strings.Join(cs, "; "))
						if os.Getenv("LVP_XLIST_EMIT") == "1" {
							b, _ := json.Marshal([]string{mq.q, mr.kind, strings.Join(cs, "; ")})
							t.Logf("EMIT %s", b)
						}
					}
				case len(d.classes) == 0 && known != "":
					t.Errorf("%s %s %s matches Loki: remove it from extractionListKnown (%s)", prof.name, mr.kind, mq.q, known)
				case len(d.classes) == 0:
					table[key].match++
				case known == "":
					for c, ex := range d.classes {
						t.Errorf("%s %s %s: %s: %s", prof.name, mr.kind, mq.q, c, ex)
					}
				}
			}
		}
	}
	if !report {
		for q, kinds := range extractionListKnown {
			for kind := range kinds {
				if !seenKnown[q+"\x00"+kind] {
					t.Errorf("extractionListKnown lists %s %s, which the differential does not run", kind, q)
				}
			}
		}
	}
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %-32s %3d of %3d match Loki", k, table[k].match, table[k].runs)
	}
	t.Logf("extraction lists against Loki:%s", b.String())
}
