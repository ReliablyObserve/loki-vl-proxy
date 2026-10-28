//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type ignoringFixture struct {
	absent, present string // service_name of the streams without and with a machine label
	evaluation      time.Time
}

func ingestIgnoringStream(t *testing.T, ts time.Time, labels map[string]string, lines int) {
	t.Helper()
	fields := make([]string, 0, len(labels))
	for name := range labels {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	values := make([][]string, 0, lines)
	for i := 0; i < lines; i++ {
		at := ts.Add(time.Duration(i) * time.Millisecond)
		line := fmt.Sprintf("binary-ignoring-%d", i)
		row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line}
		for name, value := range labels {
			row[name] = value
		}
		encoded, _ := json.Marshal(row)
		status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields="+strings.Join(fields, ","), string(encoded)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest %d %s", status, body)
		}
		values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": values}}})
	status, body := hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != 204 {
		t.Fatalf("Loki ingest %d %s", status, body)
	}
}

// ensureIgnoringFixture ingests two data sets into both stores: one service
// whose streams carry cluster only (the label named by ignoring() is absent
// on both operands) and one whose streams also carry a machine label (the
// right operand then holds several series per cluster).
func ensureIgnoringFixture(t *testing.T) ignoringFixture {
	t.Helper()
	suffix := time.Now().UnixNano()
	fx := ignoringFixture{
		absent:     fmt.Sprintf("binary-ignoring-absent-%d", suffix),
		present:    fmt.Sprintf("binary-ignoring-present-%d", suffix),
		evaluation: time.Now().Add(-4 * time.Minute).Truncate(time.Minute),
	}
	stamp := fx.evaluation.Add(-30 * time.Second)
	ingestIgnoringStream(t, stamp, map[string]string{"service_name": fx.absent, "cluster": "c1"}, 3)
	ingestIgnoringStream(t, stamp, map[string]string{"service_name": fx.absent, "cluster": "c2"}, 1)
	ingestIgnoringStream(t, stamp, map[string]string{"service_name": fx.present, "cluster": "c1", "machine": "m1"}, 1)
	ingestIgnoringStream(t, stamp, map[string]string{"service_name": fx.present, "cluster": "c1", "machine": "m2"}, 2)
	hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	waitForLokiMetricDataSelector(t, `{service_name="`+fx.absent+`"}`)
	waitForLokiMetricDataSelector(t, `{service_name="`+fx.present+`"}`)
	return fx
}

type ignoringAnswer struct {
	status int
	series map[string][]string // labels -> values
	error  string
}

func ignoringQuery(t *testing.T, base string, fx ignoringFixture, query string, instant bool) ignoringAnswer {
	t.Helper()
	start := fx.evaluation.UTC().Format(time.RFC3339Nano)
	endpoint, params := "query_range", url.Values{"query": {query}, "start": {start}, "end": {fx.evaluation.Add(time.Minute).UTC().Format(time.RFC3339Nano)}, "step": {"60"}}
	if instant {
		endpoint, params = "query", url.Values{"query": {query}, "time": {start}}
	}
	status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0", "X-Loki-Response-Encoding-Flags": "categorize-labels"})
	answer := ignoringAnswer{status: status, series: map[string][]string{}}
	if status != 200 {
		answer.error = strings.TrimSpace(string(body))
		var failure struct{ Message, Error string }
		if json.Unmarshal(body, &failure) == nil {
			if answer.error = failure.Message; answer.error == "" {
				answer.error = failure.Error
			}
		}
		return answer
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Value  []any
				Values [][]any
			}
		}
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("%s: %v: %s", base, err, body)
	}
	for _, series := range response.Data.Result {
		points := series.Values
		if instant {
			points = [][]any{series.Value}
		}
		key, _ := json.Marshal(series.Metric)
		for _, point := range points {
			answer.series[string(key)] = append(answer.series[string(key)], fmt.Sprintf("%v=%v", point[0], point[1]))
		}
	}
	return answer
}

type ignoringCase struct {
	name, query string
	wantSeries  int // series Loki must return (0: Loki must fail)
	wantError   string
}

// TestCompat_BinaryIgnoringAbsentLabel: `+ ignoring(machine)` between two sums
// joins on the labels the sides share, whether the ignored label is absent on
// both operands (one series per cluster holding the sum of both sides), on one
// side, or on the right operand only with several values (Loki fails the join
// with its duplicate-series text).
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_BinaryIgnoringAbsentLabel(t *testing.T) {
	fx := ensureIgnoringFixture(t)
	rate := func(service string) string { return `rate({service_name="` + service + `"}[5m])` }
	runIgnoringCases(t, fx, []ignoringCase{
		{"top_level_join",
			`sum by (cluster) (` + rate(fx.absent) + `) + ignoring(machine) sum by (cluster, machine) (` + rate(fx.absent) + `)`, 2, ""},
		{"ignored_label_on_the_left_only",
			`sum by (cluster, machine) (` + rate(fx.absent) + `) + ignoring(machine) sum by (cluster) (` + rate(fx.absent) + `)`, 2, ""},
		{"comparison_filter",
			`sum by (cluster) (` + rate(fx.absent) + `) > ignoring(machine) sum by (cluster, machine) (` + rate(fx.absent) + `) / 2`, 2, ""},
		{"comparison_bool",
			`sum by (cluster) (` + rate(fx.absent) + `) == bool ignoring(machine) sum by (cluster, machine) (` + rate(fx.absent) + `)`, 2, ""},
		{"group_left",
			`sum by (cluster, machine) (` + rate(fx.present) + `) / ignoring(machine) group_left sum by (cluster) (` + rate(fx.present) + `)`, 2, ""},
		{"right_operand_holds_several_per_cluster",
			`sum by (cluster) (` + rate(fx.present) + `) + ignoring(machine) sum by (cluster, machine) (` + rate(fx.present) + `)`, 0,
			"found duplicate series on the right hand-side;many-to-many matching not allowed: matching labels must be unique on one side"},
		{"left_operand_holds_several_per_cluster",
			`sum by (cluster, machine) (` + rate(fx.present) + `) + ignoring(machine) sum by (cluster) (` + rate(fx.present) + `)`, 0,
			"multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"},
	})
}

// TestCompat_BinaryIgnoringOuterSum: the registry row's shape, the join under
// an outer `sum by (cluster)`, on streams without the ignored label. It stays
// red until an outer aggregation over a binary expression is evaluated from both
// operands (the proxy answers the first operand's raw count today).
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_BinaryIgnoringOuterSum(t *testing.T) {
	t.Skip("needs the aggregation over a binary expression to evaluate both operands (semantics/aggregation-over-binary-dropped, #741); unskip when it lands")
	fx := ensureIgnoringFixture(t)
	rate := `rate({service_name="` + fx.absent + `"}[5m])`
	runIgnoringCases(t, fx, []ignoringCase{
		{"row_shape", `sum by (cluster) (sum by (cluster) (` + rate + `) + ignoring(machine) sum by (cluster, machine) (` + rate + `))`, 2, ""},
		{"ungrouped_outer_sum", `sum(sum by (cluster) (` + rate + `) + ignoring(machine) sum by (cluster, machine) (` + rate + `))`, 1, ""},
	})
}

func runIgnoringCases(t *testing.T, fx ignoringFixture, cases []ignoringCase) {
	t.Helper()
	for _, tc := range cases {
		for _, instant := range []bool{false, true} {
			name := tc.name + "/range"
			if instant {
				name = tc.name + "/instant"
			}
			t.Run(name, func(t *testing.T) {
				var loki ignoringAnswer
				for deadline := time.Now().Add(90 * time.Second); ; {
					// Loki answers a fresh stack with empty 200s for a while.
					loki = ignoringQuery(t, lokiURL, fx, tc.query, instant)
					if len(loki.series) == tc.wantSeries || tc.wantSeries == 0 && loki.status != 200 || time.Now().After(deadline) {
						break
					}
					time.Sleep(500 * time.Millisecond)
				}
				if tc.wantSeries > 0 && len(loki.series) != tc.wantSeries {
					t.Fatalf("Loki sanity: %d series, want %d: %+v", len(loki.series), tc.wantSeries, loki)
				}
				if tc.wantError != "" && (loki.status == 200 || !strings.Contains(loki.error, tc.wantError)) {
					t.Fatalf("Loki sanity: status %d error %q, want %q", loki.status, loki.error, tc.wantError)
				}
				proxy := ignoringQuery(t, proxyURL, fx, tc.query, instant)
				if proxy.status != loki.status || proxy.error != loki.error {
					t.Fatalf("status/error: proxy %d %q, Loki %d %q", proxy.status, proxy.error, loki.status, loki.error)
				}
				if len(proxy.series) != len(loki.series) {
					t.Fatalf("series: proxy %v, Loki %v", proxy.series, loki.series)
				}
				for key, want := range loki.series {
					got := proxy.series[key]
					if len(got) != len(want) {
						t.Fatalf("series %s: proxy %v, Loki %v", key, got, want)
					}
					for i := range want {
						if !ignoringPointsEqual(got[i], want[i]) {
							t.Fatalf("series %s point %d: proxy %s, Loki %s", key, i, got[i], want[i])
						}
					}
				}
			})
		}
	}
}

func ignoringPointsEqual(a, b string) bool {
	at, av, _ := strings.Cut(a, "=")
	bt, bv, _ := strings.Cut(b, "=")
	x, errA := strconv.ParseFloat(av, 64)
	y, errB := strconv.ParseFloat(bv, 64)
	return at == bt && errA == nil && errB == nil && math.Abs(x-y) <= 1e-9*math.Max(1, math.Abs(y))
}
