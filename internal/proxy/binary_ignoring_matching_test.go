package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The vector matching errors, byte for byte as Loki v3.7.7 raises them in
// pkg/logql/evaluator.go vectorBinop (:974-975 joins the two halves of the
// duplicate-series text with ";" and no space; :998 and :1007).
const (
	lokiDuplicateRight = "found duplicate series on the right hand-side;many-to-many matching not allowed: matching labels must be unique on one side"
	lokiDuplicateLeft  = "found duplicate series on the left hand-side;many-to-many matching not allowed: matching labels must be unique on one side"
	lokiManyToOne      = "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
	lokiGroupingUnique = "multiple matches for labels: grouping labels must ensure unique matches"
)

type matchSeries struct {
	labels map[string]string
	points [][]any
}

func matchBody(series ...matchSeries) []byte {
	labels := make([]map[string]string, len(series))
	points := make([][][]any, len(series))
	for i, s := range series {
		labels[i], points[i] = s.labels, s.points
	}
	return binaryTestBody(labels, points...)
}

func at(ts int, value string) [][]any { return [][]any{{ts, value}} }

func pts(pairs ...any) [][]any {
	out := make([][]any, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, []any{pairs[i], pairs[i+1]})
	}
	return out
}

func decodeMatchResult(t *testing.T, body []byte) map[string][][]any {
	t.Helper()
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Values [][]any
			}
		}
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	got := map[string][][]any{}
	for _, series := range response.Data.Result {
		got[binaryLabelKey(series.Metric)] = series.Values
	}
	return got
}

// Loki evaluates one step at a time and stops at the first step whose join
// fails (pkg/logql/engine.go: Error() is read after every Next()). Within a
// step vectorBinop rejects a duplicate on the right hand-side before it
// looks at the left, and names the side that is the "one" side of the
// grouping.
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range
func TestBinaryMatchingErrorTextAndStepOrder(t *testing.T) {
	s := func(labels map[string]string, points [][]any) matchSeries { return matchSeries{labels, points} }
	a, b := map[string]string{"app": "a", "pod": "1"}, map[string]string{"app": "a", "pod": "2"}
	one := map[string]string{"app": "a"}
	ignoringPod := &translator.VectorMatchInfo{Ignoring: []string{"pod"}}
	onApp := &translator.VectorMatchInfo{MatchOn: true, On: []string{"app"}}
	for _, tc := range []struct {
		name        string
		left, right []byte
		vm          *translator.VectorMatchInfo
		want        string
	}{
		{"duplicate_right_one_step", matchBody(s(one, at(1, "1"))), matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), ignoringPod, lokiDuplicateRight},
		{"duplicate_left_is_many_to_one", matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), matchBody(s(one, at(1, "1"))), ignoringPod, lokiManyToOne},
		{"duplicate_both_sides_same_step_names_right", matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), onApp, lokiDuplicateRight},
		{"earlier_step_many_to_one_wins_over_later_right_duplicate",
			matchBody(s(a, at(1, "1")), s(b, at(1, "2")), s(one, at(2, "1"))),
			matchBody(s(one, at(1, "1")), s(a, at(2, "1")), s(b, at(2, "2"))), ignoringPod, lokiManyToOne},
		{"earlier_step_right_duplicate_wins_over_later_many_to_one",
			matchBody(s(one, at(1, "1")), s(a, at(2, "1")), s(b, at(2, "2"))),
			matchBody(s(a, at(1, "1")), s(b, at(1, "2")), s(one, at(2, "1"))), ignoringPod, lokiDuplicateRight},
		{"unmatched_left_duplicates_are_not_an_error_before_a_later_right_duplicate",
			matchBody(s(map[string]string{"app": "x", "pod": "1"}, at(1, "1")), s(map[string]string{"app": "x", "pod": "2"}, at(1, "2"))),
			matchBody(s(a, at(1, "1")), s(a, at(2, "1"))), ignoringPod, ""},
		{"group_left_duplicate_right_is_the_one_side",
			matchBody(s(a, at(1, "1"))), matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), &translator.VectorMatchInfo{MatchOn: true, On: []string{"app"}, GroupSide: "group_left"}, lokiDuplicateRight},
		{"group_right_duplicate_left_is_the_one_side",
			matchBody(s(a, at(1, "1")), s(b, at(1, "2"))), matchBody(s(a, at(1, "1"))), &translator.VectorMatchInfo{MatchOn: true, On: []string{"app"}, GroupSide: "group_right"}, lokiDuplicateLeft},
		{"group_left_colliding_include",
			matchBody(s(map[string]string{"app": "a", "zone": "one"}, at(1, "1")), s(map[string]string{"app": "a", "zone": "two"}, at(1, "2"))),
			matchBody(s(map[string]string{"app": "a", "zone": "same"}, at(1, "1"))),
			&translator.VectorMatchInfo{MatchOn: true, On: []string{"app"}, GroupSide: "group_left", GroupLeft: []string{"zone"}}, lokiGroupingUnique},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := matchBinaryMetricResults(tc.left, tc.right, "+", "matrix", tc.vm, false)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error %q", err)
			case tc.want != "" && (err == nil || err.Error() != tc.want):
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// ignoring(label) over operands where the label is absent, present on one
// side, or present on both: the join key and the labels of the result follow
// Loki's matchingSignature and resultMetric (evaluator.go:945-953, :1096-1128).
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range
func TestBinaryIgnoringMatchingEdgeTable(t *testing.T) {
	s := func(labels map[string]string, points [][]any) matchSeries { return matchSeries{labels, points} }
	c1, c2 := map[string]string{"cluster": "c1"}, map[string]string{"cluster": "c2"}
	c1m1, c1m2 := map[string]string{"cluster": "c1", "machine": "m1"}, map[string]string{"cluster": "c1", "machine": "m2"}
	ignoring := func(labels ...string) *translator.VectorMatchInfo {
		return &translator.VectorMatchInfo{Ignoring: labels}
	}
	for _, tc := range []struct {
		name        string
		left, right []byte
		op          string
		vm          *translator.VectorMatchInfo
		boolean     bool
		want        map[string][][]any
	}{
		{"label_absent_on_both_sides",
			matchBody(s(c1, pts(1, "3", 2, "4")), s(c2, at(1, "5"))), matchBody(s(c1, pts(1, "30", 2, "40")), s(c2, at(1, "50"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33", float64(2), "44"), `{"cluster":"c2"}`: pts(float64(1), "55")}},
		{"label_empty_on_the_right_counts_as_absent",
			matchBody(s(c1, at(1, "3"))), matchBody(s(map[string]string{"cluster": "c1", "machine": ""}, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"label_on_the_right_only_is_dropped_from_the_key",
			matchBody(s(c1, at(1, "3"))), matchBody(s(c1m1, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"label_on_the_left_only_is_dropped_from_the_result",
			matchBody(s(c1m1, at(1, "3"))), matchBody(s(c1, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"different_values_of_the_ignored_label_still_join",
			matchBody(s(c1m1, at(1, "3"))), matchBody(s(c1m2, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"unmatched_series_are_dropped",
			matchBody(s(c1, at(1, "3")), s(c2, at(1, "4"))), matchBody(s(c1, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"ignoring_a_label_both_sides_carry_with_the_same_value",
			matchBody(s(c1m1, at(1, "3"))), matchBody(s(c1m1, at(1, "30"))), "+", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"ignoring_nothing_needs_equal_labels",
			matchBody(s(c1, at(1, "3"))), matchBody(s(c1m1, at(1, "30"))), "+", ignoring(), false,
			map[string][][]any{}},
		{"comparison_filter_keeps_the_left_value_and_drops_the_ignored_label",
			matchBody(s(c1m1, pts(1, "5", 2, "1"))), matchBody(s(c1, pts(1, "3", 2, "3"))), ">", ignoring("machine"), false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "5")}},
		{"comparison_bool_returns_zero_and_one_and_drops_the_ignored_label",
			matchBody(s(c1m1, pts(1, "5", 2, "1"))), matchBody(s(c1, pts(1, "3", 2, "3"))), ">", ignoring("machine"), true,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "1", float64(2), "0")}},
		{"on_keeps_only_the_on_labels_of_the_left",
			matchBody(s(c1m1, at(1, "3"))), matchBody(s(c1, at(1, "30"))), "+", &translator.VectorMatchInfo{MatchOn: true, On: []string{"cluster"}}, false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
		{"group_left_keeps_every_left_label_and_adds_the_included_one",
			matchBody(s(c1m1, at(1, "3")), s(c1m2, at(1, "4"))), matchBody(s(map[string]string{"cluster": "c1", "dc": "x"}, at(1, "30"))), "+",
			&translator.VectorMatchInfo{Ignoring: []string{"machine", "dc"}, GroupSide: "group_left", GroupLeft: []string{"dc"}}, false,
			map[string][][]any{`{"cluster":"c1","dc":"x","machine":"m1"}`: pts(float64(1), "33"), `{"cluster":"c1","dc":"x","machine":"m2"}`: pts(float64(1), "34")}},
		{"group_left_include_missing_on_the_one_side_removes_the_label",
			matchBody(s(map[string]string{"cluster": "c1", "dc": "y"}, at(1, "3"))), matchBody(s(c1, at(1, "30"))), "+",
			&translator.VectorMatchInfo{Ignoring: []string{"dc"}, GroupSide: "group_left", GroupLeft: []string{"dc"}}, false,
			map[string][][]any{`{"cluster":"c1"}`: pts(float64(1), "33")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := matchBinaryMetricResults(tc.left, tc.right, tc.op, "matrix", tc.vm, tc.boolean)
			if err != nil {
				t.Fatal(err)
			}
			if got := decodeMatchResult(t, body); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %s, want %v", body, tc.want)
			}
		})
	}
}

// vlOperandServer answers each operand of a binary expression with the
// VictoriaLogs stats_query_range body registered for its `stats by (...)`
// clause. Counts are per five minute window, so a rate is the count / 300.
func vlOperandServer(t *testing.T, byClause map[string]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		for clause, body := range byClause {
			if strings.Contains(query, clause) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
				return
			}
		}
		t.Errorf("unexpected VictoriaLogs query %q", query)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	return server
}

func statsMatrix(series ...string) string {
	return `{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(series, ",") + `]}}`
}

func statsSeries(labels string, values ...string) string {
	points := make([]string, len(values))
	for i, v := range values {
		points[i] = `[` + []string{"1700000100", "1700000160"}[i] + `,"` + v + `"]`
	}
	return `{"metric":{"__name__":"c",` + labels + `},"values":[` + strings.Join(points, ",") + `]}`
}

// The operands of the registry row, `sum by (cluster) (rate(..)) + ignoring
// (machine) sum by (cluster, machine) (rate(..))`, over a stream set that has
// no `machine` label: one series per cluster holding the sum of both operands,
// each read from its own VictoriaLogs query. With a `machine` label the right
// operand holds several series per cluster, and Loki rejects the join with its
// duplicate-series text.
//
// conformance: semantics/binary-ignoring-matching, loki_api_v1_query_range, loki_api_v1_query
func TestBinaryIgnoringThroughHandler(t *testing.T) {
	const (
		left   = `sum by (cluster) (rate({namespace="prod"}[5m]))`
		right  = `sum by (cluster, machine) (rate({namespace="prod"}[5m]))`
		joined = left + ` + ignoring(machine) ` + right
	)
	// The operands differ in value so that reading one of them twice fails.
	absentRange := map[string]string{
		"stats by (cluster, machine)": statsMatrix(statsSeries(`"cluster":"c1"`, "1500", "3000"), statsSeries(`"cluster":"c2"`, "750", "900")),
		"stats by (cluster)":          statsMatrix(statsSeries(`"cluster":"c1"`, "300", "600"), statsSeries(`"cluster":"c2"`, "150", "150")),
	}
	presentRange := map[string]string{
		"stats by (cluster, machine)": statsMatrix(statsSeries(`"cluster":"c1","machine":"m1"`, "300", "300"), statsSeries(`"cluster":"c1","machine":"m2"`, "300", "300")),
		"stats by (cluster)":          statsMatrix(statsSeries(`"cluster":"c1"`, "300", "600")),
	}
	vector := func(series ...string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}}`
	}
	sample := func(labels, value string) string {
		return `{"metric":{"__name__":"c",` + labels + `},"value":[1700000160,"` + value + `"]}`
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
			r.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
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
			a, b, both := points(t, run(t, absent, left)), points(t, run(t, absent, right)), points(t, run(t, absent, joined))
			if len(a) != 2 || len(b) != 2 || len(both) != 2 {
				t.Fatalf("want one series per cluster: %v %v %v", a, b, both)
			}
			for key, values := range a {
				if len(values) == 0 || len(b[key]) != len(values) || len(both[key]) != len(values) {
					t.Fatalf("series %s: left %v right %v join %v", key, values, b[key], both[key])
				}
				for i := range values {
					l, r, j := parsePointValue(values[i][1]), parsePointValue(b[key][i][1]), parsePointValue(both[key][i][1])
					if l == r || j != l+r {
						t.Fatalf("series %s point %d: join = %v, want left %v + right %v", key, i, j, l, r)
					}
				}
			}
		})
		t.Run(name+"/machine_present_on_the_right", func(t *testing.T) {
			w := run(t, present, joined)
			if w.Code != 500 || !strings.Contains(w.Body.String(), lokiDuplicateRight) {
				t.Fatalf("status %d, want 500 with %q: %s", w.Code, lokiDuplicateRight, w.Body)
			}
		})
	}
}
