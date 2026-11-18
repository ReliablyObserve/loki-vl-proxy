package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The registry row's own query: an outer `sum by (cluster)` over the join of two sums with
// `ignoring(machine)`. Loki evaluates the join first, then the outer sum, so over streams without a `machine`
// label the answer is one series per cluster holding the sum of both operands, and over streams that carry it the
// join's duplicate-series error. The live differential of 2026-11-14 (Loki 3.7.9, VictoriaLogs v1.54.0, build
// 726dfb48f) answered 2 series of 61 points on both sides, with every value equal.
//
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range, loki_api_v1_query
func TestBinaryIgnoringUnderOuterSum(t *testing.T) {
	const (
		left   = `sum by (cluster) (rate({namespace="prod"}[5m]))`
		right  = `sum by (cluster, machine) (rate({namespace="prod"}[5m]))`
		joined = left + ` + ignoring(machine) ` + right
		outer  = `sum by (cluster) (` + joined + `)`
	)
	vector := func(series ...string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}}`
	}
	sample := func(labels, value string) string {
		return `{"metric":{"__name__":"c",` + labels + `},"value":[1700000160,"` + value + `"]}`
	}
	// The operands differ in value, so reading one of them twice, or dropping one, fails.
	absentRange := map[string]string{
		"stats by (cluster, machine)": statsMatrix(statsSeries(`"cluster":"c1"`, "1500", "3000"), statsSeries(`"cluster":"c2"`, "750", "900")),
		"stats by (cluster)":          statsMatrix(statsSeries(`"cluster":"c1"`, "300", "600"), statsSeries(`"cluster":"c2"`, "150", "150")),
	}
	presentRange := map[string]string{
		"stats by (cluster, machine)": statsMatrix(statsSeries(`"cluster":"c1","machine":"m1"`, "300", "300"), statsSeries(`"cluster":"c1","machine":"m2"`, "300", "300")),
		"stats by (cluster)":          statsMatrix(statsSeries(`"cluster":"c1"`, "300", "600")),
	}
	absentInstant := map[string]string{
		"stats by (cluster, machine)": vector(sample(`"cluster":"c1"`, "1500"), sample(`"cluster":"c2"`, "750")),
		"stats by (cluster)":          vector(sample(`"cluster":"c1"`, "300"), sample(`"cluster":"c2"`, "150")),
	}
	presentInstant := map[string]string{
		"stats by (cluster, machine)": vector(sample(`"cluster":"c1","machine":"m1"`, "300"), sample(`"cluster":"c1","machine":"m2"`, "300")),
		"stats by (cluster)":          vector(sample(`"cluster":"c1"`, "300")),
	}
	for _, instant := range []bool{false, true} {
		absent, present, name := absentRange, presentRange, "range"
		if instant {
			absent, present, name = absentInstant, presentInstant, "instant"
		}
		run := func(t *testing.T, vl map[string]string, query string) *httptest.ResponseRecorder {
			p := newTestProxy(t, vlOperandServer(t, vl).URL)
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			endpoint, params := "query_range", url.Values{"query": {query}, "start": {"1700000100"}, "end": {"1700000160"}, "step": {"60"}}
			if instant {
				endpoint, params = "query", url.Values{"query": {query}, "time": {"1700000160"}}
			}
			r := httptest.NewRequest("GET", "/loki/api/v1/"+endpoint+"?"+params.Encode(), nil)
			r.Header.Set("X-Scope-OrgID", "0")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			return w
		}
		points := func(t *testing.T, w *httptest.ResponseRecorder) map[string][][]any {
			t.Helper()
			var response struct {
				Data struct {
					Result []struct {
						Metric map[string]string
						Value  []any
						Values [][]any
					}
				}
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			got := map[string][][]any{}
			for _, series := range response.Data.Result {
				values := series.Values
				if instant {
					values = [][]any{series.Value}
				}
				got[binaryLabelKey(series.Metric)] = values
			}
			return got
		}
		t.Run(name+"/machine_absent", func(t *testing.T) {
			lAns, rAns, joinAns, sum := points(t, run(t, absent, left)), points(t, run(t, absent, right)), points(t, run(t, absent, joined)), points(t, run(t, absent, outer))
			if len(lAns) != 2 || len(sum) != 2 {
				t.Fatalf("want one series per cluster: left %v outer sum %v", lAns, sum)
			}
			for key, values := range lAns {
				if len(sum[key]) != len(values) || len(joinAns[key]) != len(values) {
					t.Fatalf("series %s: left %v join %v outer sum %v", key, values, joinAns[key], sum[key])
				}
				for i := range values {
					l, r, s := parsePointValue(values[i][1]), parsePointValue(rAns[key][i][1]), parsePointValue(sum[key][i][1])
					// The join is one series per cluster, so the outer sum is the join: the sum of both operands,
					// not the first operand's raw count nor its value alone.
					if l == r || s != l+r || s != parsePointValue(joinAns[key][i][1]) {
						t.Fatalf("series %s point %d: outer sum = %v, want left %v + right %v", key, i, s, l, r)
					}
				}
			}
		})
		t.Run(name+"/machine_present_on_the_right", func(t *testing.T) {
			w := run(t, present, outer)
			if w.Code != 500 || !strings.Contains(w.Body.String(), lokiDuplicateRight) {
				t.Fatalf("status %d, want 500 with %q: %s", w.Code, lokiDuplicateRight, w.Body)
			}
		})
	}
}
