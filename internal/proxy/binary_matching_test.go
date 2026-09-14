package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func binaryTestBody(labels []map[string]string, points ...[][]any) []byte {
	result := make([]any, len(labels))
	for i, metric := range labels {
		result[i] = map[string]any{"metric": metric, "values": points[i]}
	}
	b, _ := json.Marshal(map[string]any{"data": map[string]any{"result": result}})
	return b
}

func TestBinaryMatchingMatrixValuesAndLabels(t *testing.T) {
	left := binaryTestBody([]map[string]string{{"app": "a", "level": "info"}, {"app": "a", "level": "error"}}, [][]any{{1, "2"}, {2, "4"}}, [][]any{{2, "8"}})
	right := binaryTestBody([]map[string]string{{"app": "a", "region": "eu"}}, [][]any{{1, "2"}, {2, "4"}, {3, "8"}})
	for _, tc := range []struct {
		name, op string
		vm       *translator.VectorMatchInfo
		boolean  bool
		want     map[string][][]any
	}{
		{"group_left", "/", &translator.VectorMatchInfo{On: []string{"app"}, GroupSide: "group_left", GroupLeft: []string{"region"}}, false, map[string][][]any{
			`{"app":"a","level":"info","region":"eu"}`:  {{float64(1), "1"}, {float64(2), "1"}},
			`{"app":"a","level":"error","region":"eu"}`: {{float64(2), "2"}}}},
		{"comparison_filter", ">", &translator.VectorMatchInfo{Ignoring: []string{"level", "region"}, GroupSide: "group_left"}, false, map[string][][]any{
			`{"app":"a","level":"error"}`: {{float64(2), "8"}}}},
		{"comparison_bool", ">", &translator.VectorMatchInfo{On: []string{"app"}, GroupSide: "group_left"}, true, map[string][][]any{
			`{"app":"a","level":"info"}`:  {{float64(1), "0"}, {float64(2), "0"}},
			`{"app":"a","level":"error"}`: {{float64(2), "1"}}}},
		{"many_to_many_set", "and", &translator.VectorMatchInfo{MatchOn: true}, false, map[string][][]any{
			`{"app":"a","level":"info"}`:  {{float64(1), "2"}, {float64(2), "4"}},
			`{"app":"a","level":"error"}`: {{float64(2), "8"}}}},
		{"unless", "unless", &translator.VectorMatchInfo{MatchOn: true}, false, map[string][][]any{}},
		{"or", "or", &translator.VectorMatchInfo{On: []string{"app"}}, false, map[string][][]any{
			`{"app":"a","level":"info"}`:  {{float64(1), "2"}, {float64(2), "4"}},
			`{"app":"a","level":"error"}`: {{float64(2), "8"}},
			`{"app":"a","region":"eu"}`:   {{float64(3), "8"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := matchBinaryMetricResults(left, right, tc.op, "matrix", tc.vm, tc.boolean)
			if err != nil {
				t.Fatal(err)
			}
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
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %s, want %v", body, tc.want)
			}
		})
	}
	groupRight := &translator.VectorMatchInfo{On: []string{"app"}, GroupSide: "group_right", GroupRight: []string{"region"}}
	body, err := matchBinaryMetricResults(right, left, "/", "matrix", groupRight, false)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Values [][]any
			}
		}
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	got := map[string][][]any{}
	for _, series := range result.Data.Result {
		got[binaryLabelKey(series.Metric)] = series.Values
	}
	want := map[string][][]any{
		`{"app":"a","level":"info","region":"eu"}`:  {{float64(1), "1"}, {float64(2), "1"}},
		`{"app":"a","level":"error","region":"eu"}`: {{float64(2), "0.5"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("group_right result=%s want=%v", body, want)
	}
}

func TestBinaryGroupingRejectsDuplicateOutputLabels(t *testing.T) {
	left := binaryTestBody([]map[string]string{{"app": "a", "zone": "one"}, {"app": "a", "zone": "two"}}, [][]any{{1, "1"}}, [][]any{{1, "2"}})
	right := binaryTestBody([]map[string]string{{"app": "a", "zone": "same"}}, [][]any{{1, "1"}})
	for _, op := range []string{"+", "<"} {
		_, err := matchBinaryMetricResults(left, right, op, "matrix", &translator.VectorMatchInfo{On: []string{"app"}, GroupSide: "group_left", GroupLeft: []string{"zone"}}, false)
		if err == nil {
			t.Fatal("duplicate grouped output accepted")
		}
	}
}

func TestBinaryMatchingEmptyLabelsCannotBypassCardinality(t *testing.T) {
	left := binaryTestBody([]map[string]string{{}}, [][]any{{1, "1"}})
	right := binaryTestBody([]map[string]string{{"app": ""}, {}}, [][]any{{1, "2"}}, [][]any{{1, "3"}})
	for _, vm := range []*translator.VectorMatchInfo{{MatchOn: true, On: []string{"app"}}, {Ignoring: []string{"other"}}} {
		if _, err := matchBinaryMetricResults(left, right, "+", "matrix", vm, false); err == nil {
			t.Fatal("empty/missing label collision bypassed cardinality")
		}
	}
}

func TestBinaryChildBoundsAndHTTPError(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=vector(1)&start=1&end=61&step=60", nil)
	response := p.evaluateBinaryLogQLOperand(r, &logqlpkg.OpaqueMetricExpr{Raw: "{"}, "matrix")
	if response.status != 400 {
		t.Fatalf("child status=%d body=%s", response.status, response.body.String())
	}
	remaining := 10
	r = r.WithContext(context.WithValue(r.Context(), binaryEvaluationKey{}, binaryEvaluationState{depth: 64, remaining: &remaining}))
	response = p.evaluateBinaryLogQLOperand(r, &logqlpkg.OpaqueMetricExpr{Raw: "vector(1)"}, "matrix")
	if response.status != 400 || remaining != 10 {
		t.Fatalf("depth budget ignored: %d %d", response.status, remaining)
	}
	remaining = 0
	r = r.WithContext(context.WithValue(r.Context(), binaryEvaluationKey{}, binaryEvaluationState{remaining: &remaining}))
	response = p.evaluateBinaryLogQLOperand(r, &logqlpkg.OpaqueMetricExpr{Raw: "vector(1)"}, "matrix")
	if response.status != 400 {
		t.Fatalf("work budget ignored: %d", response.status)
	}
	w := &binaryOperandResponse{header: make(http.Header), limit: 3}
	if _, err := w.Write([]byte("1234")); err == nil || w.body.Len() != 0 {
		t.Fatal("response limit truncated or accepted excess data")
	}
}

func TestBinarySetRejectsScalarOperand(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	for _, query := range []string{"1 and 2", "vector(1) or (1+2)", "1 unless vector(2)"} {
		expr, err := logqlpkg.Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("GET", "/loki/api/v1/query?"+url.Values{"query": {query}}.Encode(), nil)
		w := httptest.NewRecorder()
		p.proxyBinaryLogQL(w, r, expr.(*logqlpkg.BinOpExpr), "vector")
		if w.Code != 400 {
			t.Fatalf("%s: %d %s", query, w.Code, w.Body)
		}
	}
}

func TestBinaryASTPreservesBoolAndGrouping(t *testing.T) {
	query := `(sum(rate({app="a"}[5m])) + sum(rate({app="b"}[5m]))) > bool on() sum(rate({app="c"}[5m]))`
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := logqlpkg.Parse(expr.String())
	if err != nil {
		t.Fatal(err)
	}
	bin := parsed.(*logqlpkg.BinOpExpr)
	if !bin.ReturnBool || bin.Op != ">" {
		t.Fatalf("lost bool/operator: %s", parsed.String())
	}
	if _, ok := bin.Left.(*logqlpkg.BinOpExpr); !ok {
		t.Fatalf("lost nested left operand: %s", parsed.String())
	}
}

func TestBinaryOperandsDoNotRepeatUniformOffset(t *testing.T) {
	for _, tc := range []struct {
		query      string
		wantOffset bool
	}{
		{`sum(rate({app="a"}[5m] offset 1h)) / on() sum(rate({app="b"}[5m] offset 1h))`, false},
		{`sum(rate({app="a"}[5m] offset 1h)) / on() sum(rate({app="b"}[5m] offset 2h))`, true},
	} {
		r := httptest.NewRequest("GET", "/loki/api/v1/query?"+url.Values{"query": {tc.query}}.Encode(), nil)
		expr := binaryExprForRequest(r)
		if expr == nil {
			t.Fatal("missing binary expression")
		}
		_, _, err := extractLogQLOffset(expr.String())
		if tc.wantOffset && err == nil {
			t.Fatal("mixed offsets were stripped")
		}
		if !tc.wantOffset {
			if offset, _, _ := extractLogQLOffset(expr.String()); offset != 0 {
				t.Fatal("uniform offset would be repeated")
			}
		}
	}
}

func TestBinaryInstantUsesSingleDefaultTimestamp(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	r := httptest.NewRequest("GET", "/loki/api/v1/query?query=vector(2)%2Bon()%20vector(3)", nil)
	expr, err := logqlpkg.Parse(`vector(2) + on() vector(3)`)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	p.proxyBinaryLogQL(w, r, expr.(*logqlpkg.BinOpExpr), "vector")
	var response struct {
		Data struct{ Result []struct{ Value []any } }
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Data.Result) != 1 || response.Data.Result[0].Value[1] != "5" {
		t.Fatalf("default timestamp mismatched: %d %s", w.Code, w.Body)
	}
}

func TestBinaryScalarSubtreesAndIEEEValues(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	for _, tc := range []struct{ query, want string }{
		{"1 + 2 * 3", "7"}, {"2 ^ 3 ^ 2", "512"}, {"1 / 0", "+Inf"}, {"0 / 0", "NaN"}, {"1 < bool 2", "1"}, {"1 > 2", "0"},
		// Loki renders sample values with model.SampleValue.String ('f', -1):
		// never exponent notation for large or small magnitudes.
		{"1234 * 1000", "1234000"}, {"1 / 60000", "0.000016666666666666667"},
	} {
		for _, resultType := range []string{"vector", "matrix"} {
			t.Run(tc.query+"/"+resultType, func(t *testing.T) {
				r := httptest.NewRequest("GET", "/loki/api/v1/query?"+url.Values{"query": {tc.query}, "time": {"1700000000"}, "start": {"1700000000"}, "end": {"1700000060"}, "step": {"60"}}.Encode(), nil)
				expr, err := logqlpkg.Parse(tc.query)
				if err != nil {
					t.Fatal(err)
				}
				w := httptest.NewRecorder()
				p.proxyBinaryLogQL(w, r, expr.(*logqlpkg.BinOpExpr), resultType)
				var response struct {
					Data struct {
						ResultType string
						Result     json.RawMessage
					}
				}
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
					t.Fatalf("%d %s", w.Code, w.Body)
				}
				if resultType == "vector" {
					var point []any
					if response.Data.ResultType != "scalar" || json.Unmarshal(response.Data.Result, &point) != nil || !reflect.DeepEqual(point, []any{float64(1700000000), tc.want}) {
						t.Fatalf("scalar response %s", w.Body)
					}
				} else {
					var series []struct{ Values [][]any }
					if json.Unmarshal(response.Data.Result, &series) != nil || len(series) != 1 || !reflect.DeepEqual(series[0].Values, [][]any{{float64(1700000000), tc.want}, {float64(1700000060), tc.want}}) {
						t.Fatalf("range response %s", w.Body)
					}
				}
			})
		}
	}
}

// on(app) group_left matches many left series against one right series and
// keeps each left series' labels (Loki resultMetric for CardManyToOne). The
// implicit one-to-one form of the same data must still be rejected.
func TestOnGroupLeftMatchesManyLeftSeriesBySubset(t *testing.T) {
	left := binaryTestBody([]map[string]string{{"app": "a", "id": "1"}, {"app": "a", "id": "2"}}, [][]any{{1700000040, "4"}}, [][]any{{1700000040, "6"}})
	right := binaryTestBody([]map[string]string{{"app": "a", "team": "x"}}, [][]any{{1700000040, "2"}})
	ctx := binaryEvaluationContext(context.Background())
	vm := &translator.VectorMatchInfo{On: []string{"app"}, MatchOn: true, GroupSide: "group_left", GroupLeft: []string{"team"}}
	body, err := matchBinaryMetricResultsContext(ctx, left, right, "/", "matrix", vm, false)
	if err != nil {
		t.Fatalf("group_left subset match rejected: %v", err)
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	got := map[string]string{}
	for _, series := range response.Data.Result {
		if series.Metric["team"] != "x" || len(series.Values) != 1 {
			t.Fatalf("group_left must copy team from the one side: %s", body)
		}
		got[series.Metric["id"]] = series.Values[0][1].(string)
	}
	if len(got) != 2 || got["1"] != "2" || got["2"] != "3" {
		t.Fatalf("want id 1 -> 2 and id 2 -> 3, got %v from %s", got, body)
	}
	oneToOne := &translator.VectorMatchInfo{On: []string{"app"}, MatchOn: true}
	if _, err := matchBinaryMetricResultsContext(binaryEvaluationContext(context.Background()), left, right, "/", "matrix", oneToOne, false); err == nil {
		t.Fatal("implicit many-to-one on(app) must be rejected like Loki")
	}
}
