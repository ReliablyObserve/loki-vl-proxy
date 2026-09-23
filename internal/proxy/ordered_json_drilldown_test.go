package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOrderedJSONPreservesNativeDrilldownHistogram(t *testing.T) {
	canonical := `sum by(trace_id)(count_over_time({app="api"}|json|drop __error__,__error_details__|trace_id!=""[1m]))`
	for _, tc := range []struct {
		name, query, step string
		tagged, native    bool
	}{
		{"canonical", canonical, "60", true, true},
		{"duration_step", canonical, "1m", true, true},
		{"direct_exact", canonical, "60", false, false},
		{"overlapping_windows", canonical, "30", true, false},
		{"no_error_drop", `sum by(trace_id)(count_over_time({app="api"}|json|trace_id!=""[1m]))`, "60", true, false},
		{"only_error_details_drop", `sum by(trace_id)(count_over_time({app="api"}|json|drop __error_details__|trace_id!=""[1m]))`, "60", true, false},
		{"value_filter", `sum by(trace_id)(count_over_time({app="api"}|json|drop __error__|trace_id="specific"[1m]))`, "60", true, false},
		{"error_filter", `sum by(trace_id)(count_over_time({app="api"}|json|__error__=""|trace_id!=""[1m]))`, "60", true, false},
		{"drop_data_label", `sum by(trace_id)(count_over_time({app="api"}|json|drop __error__,trace_id|trace_id!=""[1m]))`, "60", true, false},
		{"second_parser", `sum by(trace_id)(count_over_time({app="api"}|json|json|drop __error__|trace_id!=""[1m]))`, "60", true, false},
		{"filter_before_parser", `sum by(trace_id)(count_over_time({app="api"}|drop __error__|trace_id!=""|json|drop __error__[1m]))`, "60", true, false},
		{"rate", `sum by(trace_id)(rate({app="api"}|json|drop __error__|trace_id!=""[1m]))`, "60", true, false},
		{"multiple_groups", `sum by(trace_id,app)(count_over_time({app="api"}|json|drop __error__|trace_id!=""[1m]))`, "60", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				t.Fatal("expected ordered JSON plan")
			}
			r := httptest.NewRequest("GET", "/loki/api/v1/query_range?step="+url.QueryEscape(tc.step), nil)
			if tc.tagged {
				r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			if got := plan.isNativeDrilldownHistogram(r); got != tc.native {
				t.Fatalf("native=%v want=%v", got, tc.native)
			}
			if tc.native {
				p := newTestProxy(t, "http://127.0.0.1:1")
				if p.handleOrderedJSONMetric(httptest.NewRecorder(), r, time.Now(), tc.query, true) {
					t.Fatal("native histogram intercepted by raw collector")
				}
			}
		})
	}
}

// statsMatrixBody renders a VictoriaLogs stats_query_range matrix with one
// bucket at ts for each of the given label sets.
func statsMatrixBody(ts time.Time, metrics []map[string]string) string {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, metric := range metrics {
		if i > 0 {
			sb.WriteByte(',')
		}
		labels, _ := json.Marshal(metric)
		sb.WriteString(`{"metric":` + string(labels) + `,"values":[[` + strconv.FormatInt(ts.Unix(), 10) + `,"1"]]}`)
	}
	sb.WriteString(`]}}`)
	return sb.String()
}

// An exact (untagged) field histogram is answered from stats buckets, and a
// series overflow still fails closed with Loki's series-limit error instead of
// a truncated result.
func TestOrderedJSONExactHistogramLimitStillFailsClosed(t *testing.T) {
	evaluation := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	rows := 3
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case r.URL.Path == "/select/logsql/query" && strings.HasSuffix(r.Form.Get("query"), " | limit 1"):
			// The parse-risk probe finds no line the parsers read differently.
		case r.URL.Path == "/select/logsql/stats_query_range":
			var metrics []map[string]string
			for i := 0; i < rows; i++ {
				metrics = append(metrics, map[string]string{"__name__": "c", "trace_id": strconv.Itoa(i)})
			}
			_, _ = w.Write([]byte(statsMatrixBody(evaluation.Add(-time.Minute), metrics)))
		default:
			t.Errorf("exact metric left the stats pushdown: %s %s", r.URL.Path, r.Form.Get("query"))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.maxStatsQuerySeries = 2
	query := `sum by(trace_id)(count_over_time({app="api"}|json|drop __error__,__error_details__|trace_id!=""[1m]))`
	params := url.Values{"query": {query}, "start": {evaluation.Format(time.RFC3339Nano)}, "end": {evaluation.Add(time.Minute).Format(time.RFC3339Nano)}, "step": {"60"}}
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
		w := httptest.NewRecorder()
		if !p.handleOrderedJSONMetric(w, r, time.Now(), query, true) {
			t.Fatal("untagged exact histogram skipped")
		}
		return w
	}
	w := request()
	if w.Code < 400 || !strings.Contains(w.Body.String(), "maximum number of series (2) reached") || strings.Contains(w.Body.String(), `"result"`) {
		t.Fatalf("overflow returned partial result: %d %s", w.Code, w.Body)
	}
	rows = 2
	w = request()
	var response struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Data.Result) != 2 {
		t.Fatalf("small query did not recover: %d %s", w.Code, w.Body)
	}
}

// A Drilldown field histogram over a label VictoriaLogs stores under another
// spelling (-field-mapping request.method=http_method) is answered from stats
// buckets that read the stored field wherever the line carries it, as Loki
// reads structured metadata before a parsed key of the same name.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-translated-label
func TestOrderedJSONDrilldownKeepsTranslatedFieldGrouping(t *testing.T) {
	evaluation := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	var stats []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case r.URL.Path == "/select/logsql/query" && strings.HasSuffix(r.Form.Get("query"), " | limit 1"):
		case r.URL.Path == "/select/logsql/stats_query_range":
			stats = append(stats, r.Form.Get("query"))
			_, _ = w.Write([]byte(statsMatrixBody(evaluation.Add(-time.Minute), []map[string]string{{"__name__": "c", "http_method": "GET"}})))
		default:
			t.Errorf("translated field left the stats pushdown: %s %s", r.URL.Path, r.Form.Get("query"))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{{VLField: "request.method", LokiLabel: "http_method"}})
	query := `sum by(http_method)(count_over_time({app="api"}|json|drop __error__,__error_details__|http_method!=""[1m]))`
	params := url.Values{"start": {evaluation.Format(time.RFC3339Nano)}, "end": {evaluation.Add(time.Minute).Format(time.RFC3339Nano)}, "step": {"60"}}
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	w := httptest.NewRecorder()
	if !p.handleOrderedJSONMetric(w, r, time.Now(), query, true) || w.Code != 200 || !strings.Contains(w.Body.String(), `"http_method":"GET"`) {
		t.Fatalf("translated grouping lost: %d %s", w.Code, w.Body)
	}
	want := " | unpack_json fields (http_method) keep_original_fields | format if (`request.method`:*) \"<request.method>\" as http_method | filter -http_method:=\"\" | stats by (http_method) count() as c"
	if len(stats) != 1 || !strings.HasSuffix(stats[0], want) {
		t.Fatalf("stats query %q does not end with %q", stats, want)
	}
}
