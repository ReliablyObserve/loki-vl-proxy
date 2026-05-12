package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseStatsCompatSpec(t *testing.T) {
	spec, ok := parseStatsCompatSpec(`app:=nginx | unpack_json | stats by (namespace, app) first(duration)`)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if spec.BaseQuery != `app:=nginx | unpack_json` {
		t.Fatalf("unexpected base query: %q", spec.BaseQuery)
	}
	if spec.Func != "first" || spec.Field != "duration" {
		t.Fatalf("unexpected function spec: %+v", spec)
	}
	if len(spec.GroupBy) != 2 || spec.GroupBy[0] != "namespace" || spec.GroupBy[1] != "app" {
		t.Fatalf("unexpected by labels: %v", spec.GroupBy)
	}
}

func TestParseStatsCompatSpecByEmpty(t *testing.T) {
	spec, ok := parseStatsCompatSpec(`app:=nginx | unpack_json | stats by () avg(confidence)`)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if !spec.ByExplicit {
		t.Fatalf("expected ByExplicit=true for by () clause")
	}
	if len(spec.GroupBy) != 0 {
		t.Fatalf("expected empty GroupBy, got %v", spec.GroupBy)
	}
	if spec.Func != "avg" || spec.Field != "confidence" {
		t.Fatalf("unexpected function spec: %+v", spec)
	}
}

func TestQueryRange_AvgOverTimeByEmptyReturnsSingleSeries(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query_range" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		// avg_over_time with by() routes to VL stats_query_range.
		// VL returns a Prometheus-compatible matrix with one series (no labels).
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"resultType":"matrix","result":[{"metric":{},"values":[[%d,"0.7"]]}]}}`,
			base.Add(60*time.Second).Unix())
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `avg_over_time({env="prod"} | json | unwrap confidence [5m]) by ()`)
	params.Set("start", strconv.FormatInt(base.Add(60*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	// by () must produce exactly ONE series with empty metric labels.
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected 1 series from by (), got %d: %s", len(resp.Data.Result), rec.Body.String())
	}
	if len(resp.Data.Result[0].Metric) != 0 {
		t.Fatalf("expected empty metric labels, got %v", resp.Data.Result[0].Metric)
	}
}

func TestParseOriginalRangeMetricSpecRate(t *testing.T) {
	spec, ok := parseOriginalRangeMetricSpec(`rate({app="nginx"}[5m])`)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if spec.Func != "rate" {
		t.Fatalf("unexpected func: %q", spec.Func)
	}
	if spec.Window != 5*time.Minute {
		t.Fatalf("unexpected window: %v", spec.Window)
	}
}

func TestQueryRange_RateManualFallback(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			lines := []string{
				fmt.Sprintf(`{"_time":%q,"_msg":"a","_stream":"{app=\"nginx\",level=\"info\"}"}`, base.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"_time":%q,"_msg":"b","_stream":"{app=\"nginx\",level=\"info\"}"}`, base.Add(60*time.Second).Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"_time":%q,"_msg":"c","_stream":"{app=\"nginx\",level=\"error\"}"}`, base.Add(120*time.Second).Format(time.RFC3339Nano)),
			}
			_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
		case "/select/logsql/stats_query_range":
			t.Fatalf("expected manual fallback, stats_query_range must not be called")
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="nginx"} | json [2m])`)
	params.Set("start", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) == 0 {
		t.Fatalf("expected non-empty series, got %s", rec.Body.String())
	}
}

func TestQueryRange_CountOverTimeParserUsesDirectStatsRange(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	var (
		manualCalled bool
		statsCalled  bool
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			// Parser probe may hit this endpoint, but manual metric fallback
			// should not for parser count_over_time.
			if r.Form.Get("limit") == "1000000" {
				manualCalled = true
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintf(
				w,
				`{"_time":%q,"_msg":"{\"method\":\"GET\",\"status\":200}","_stream":"{service_name=\"api-gateway\"}","service_name":"api-gateway","level":"info"}`+"\n",
				base.Format(time.RFC3339Nano),
			)
		case "/select/logsql/stats_query_range":
			statsCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[1700000000,"2"]]},{"metric":{"level":"error"},"values":[[1700000000,"1"]]}]}}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `sum by (detected_level) (count_over_time({service_name="api-gateway"} | json | logfmt | drop __error__, __error_details__ [1m]))`)
	params.Set("start", strconv.FormatInt(base.Add(-time.Minute).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if manualCalled {
		t.Fatalf("unexpected manual fallback for parser count_over_time")
	}
	if !statsCalled {
		t.Fatalf("expected stats_query_range to be called")
	}

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	seen := map[string]bool{}
	for _, series := range resp.Data.Result {
		seen[series.Metric["detected_level"]] = true
	}
	if !seen["info"] || !seen["error"] {
		t.Fatalf("expected detected_level info/error series, got %v", resp.Data.Result)
	}
}

func TestQueryRange_BytesRateCompatScalesFromSumLen(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	msg := strings.Repeat("x", 100)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		if got := r.FormValue("query"); !strings.Contains(got, `app:=nginx`) {
			t.Fatalf("expected translated query, got %q", got)
		}
		lines := []string{
			fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream":"{app=\"nginx\"}"}`, base.Format(time.RFC3339Nano), msg),
			fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream":"{app=\"nginx\"}"}`, base.Add(60*time.Second).Format(time.RFC3339Nano), msg),
			fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream":"{app=\"nginx\"}"}`, base.Add(120*time.Second).Format(time.RFC3339Nano), msg),
		}
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `bytes_rate({app="nginx"} | json [5m])`)
	params.Set("start", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "success" || len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
	got := resp.Data.Result[0].Values[0][1]
	if got != "1" {
		t.Fatalf("expected bytes_rate value 1 after normalization, got %v", got)
	}
}

func TestQueryRange_StdvarCompatSquaresStddev(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query_range" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		_, _ = fmt.Fprintf(
			w,
			`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"nginx"},"values":[[%d,"1"]]}]}}`,
			base.Add(120*time.Second).Unix(),
		)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `stdvar_over_time({app="nginx"} | unwrap latency [5m])`)
	params.Set("start", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got := resp.Data.Result[0].Values[0][1]
	if got != "1" {
		t.Fatalf("expected stdvar value 1, got %v", got)
	}
}

func TestQueryRange_FirstOverTimeManualFallback(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		lines := []string{
			fmt.Sprintf(`{"_time":%q,"_msg":"a","_stream":"{app=\"nginx\"}","latency":10}`, base.Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"b","_stream":"{app=\"nginx\"}","latency":20}`, base.Add(60*time.Second).Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"c","_stream":"{app=\"nginx\"}","latency":30}`, base.Add(120*time.Second).Format(time.RFC3339Nano)),
		}
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `first_over_time({app="nginx"} | json | unwrap latency [2m])`)
	params.Set("start", strconv.FormatInt(base.Add(60*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
	for _, pair := range resp.Data.Result[0].Values {
		if pair[1] != "10" {
			t.Fatalf("expected first_over_time value 10, got %v", pair[1])
		}
	}
}

func TestQueryInstant_RateCounterRequiresUnwrap(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	params := url.Values{}
	params.Set("query", `rate_counter({app="nginx"}[5m])`)
	params.Set("time", "1700000180")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unwrap") {
		t.Fatalf("expected unwrap error, got %s", rec.Body.String())
	}
}

func TestQueryInstant_RateCounterManualFallback(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		lines := []string{
			fmt.Sprintf(`{"_time":%q,"_msg":"a","_stream":"{app=\"nginx\"}","counter":100}`, base.Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"b","_stream":"{app=\"nginx\"}","counter":130}`, base.Add(60*time.Second).Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"c","_stream":"{app=\"nginx\"}","counter":10}`, base.Add(120*time.Second).Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"d","_stream":"{app=\"nginx\"}","counter":30}`, base.Add(180*time.Second).Format(time.RFC3339Nano)),
		}
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate_counter({app="nginx"} | unwrap counter [5m])`)
	params.Set("time", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Value) != 2 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
	if got := resp.Data.Result[0].Value[1]; got != "0.2" {
		t.Fatalf("expected rate_counter value 0.2, got %v", got)
	}
}

func TestQueryRange_SumOverTimeRequiresUnwrap(t *testing.T) {
	p := newGapTestProxy(t, "http://unused")
	params := url.Values{}
	params.Set("query", `sum_over_time({app="nginx"}[5m])`)
	params.Set("start", "1700000000")
	params.Set("end", "1700000300")
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unwrap") {
		t.Fatalf("expected unwrap error, got %s", rec.Body.String())
	}
}

func TestQueryRange_QuantileManualFallback(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		lines := []string{
			fmt.Sprintf(`{"_time":%q,"_msg":"a","_stream":"{app=\"nginx\",level=\"info\"}","latency":10}`, base.Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"b","_stream":"{app=\"nginx\",level=\"info\"}","latency":20}`, base.Add(60*time.Second).Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"c","_stream":"{app=\"nginx\",level=\"info\"}","latency":30}`, base.Add(120*time.Second).Format(time.RFC3339Nano)),
		}
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `quantile_over_time(0.5, {app="nginx"} | json | unwrap latency [5m])`)
	params.Set("start", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(180*time.Second).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
	if got := resp.Data.Result[0].Values[0][1]; got != "20" {
		t.Fatalf("expected median quantile value 20, got %v", got)
	}
}

func TestHasPostParserPipeStage(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{`{app="a"} | json`, false},
		{`{app="a"} | logfmt`, false},
		{`{app="a"} | regexp "(?P<f>.+)"`, false},
		{`{app="a"} | json | status >= 400`, true},
		{`{app="a"} | logfmt | level="error"`, true},
		{`{app="a"} | json | method="GET" | path="/api"`, true},
		{`{app="a"}`, false},
		{`{app="a"} |= "error"`, false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := hasPostParserPipeStage(tc.query); got != tc.want {
				t.Fatalf("hasPostParserPipeStage(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestHasDropErrorOnlyPostParserStage(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{`{app="a"} | json | drop __error__`, true},
		{`{app="a"} | logfmt | drop __error__, __error_details__`, true},
		{`{app="a"} | json | drop __error_details__, __error__`, true},
		// No post-parser stage — not a drop-error opt-in.
		{`{app="a"} | json`, false},
		// Filtering stage — not a drop-error opt-in.
		{`{app="a"} | json | status >= 400`, false},
		// __error__ filter (not drop) — not a drop-error opt-in.
		{`{app="a"} | json | __error__ = ""`, false},
		// No parser at all.
		{`{app="a"}`, false},
		// drop __error__ but also another filter after — not a drop-error-only stage.
		{`{app="a"} | json | drop __error__ | status >= 400`, false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := hasDropErrorOnlyPostParserStage(tc.query); got != tc.want {
				t.Fatalf("hasDropErrorOnlyPostParserStage(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestStripParserStages(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`{app="a"} | json`, `{app="a"}`},
		{`{app="a"} | logfmt`, `{app="a"}`},
		{`{app="a"}`, `{app="a"}`},
		{`{app="a"} | json | logfmt`, `{app="a"}`},
		{`{app="a"} |= "error" | json`, `{app="a"} |= "error"`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := stripParserStages(tc.query); got != tc.want {
				t.Fatalf("stripParserStages(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

func TestBuildManualMetricLabels_StreamExpansion(t *testing.T) {
	// streamLabels is already expanded from the _stream field (e.g. {app="api",env="prod"})
	// plus level enrichment. "_stream" itself is not a key in this map.
	streamLabels := map[string]string{
		"app":            "api",
		"env":            "prod",
		"level":          "info",
		"detected_level": "info",
	}

	// groupBy=["_stream","level"] is produced by addStatsByStreamClause.
	// "_stream" must expand to all stream labels so applyWithoutGrouping can
	// remove individual keys rather than collapsing all series to {}.
	got := buildManualMetricLabels(streamLabels, []string{"_stream", "level"}, false)

	for _, want := range []string{"app", "env", "level", "detected_level"} {
		if _, ok := got[want]; !ok {
			t.Errorf("label %q missing from result %v", want, got)
		}
	}
	if _, bad := got["_stream"]; bad {
		t.Errorf("_stream sentinel must not appear in result labels, got %v", got)
	}
}

func TestBuildManualMetricLabels_NoStreamExpansion(t *testing.T) {
	streamLabels := map[string]string{"app": "api", "level": "info"}
	// Explicit by(app) — _stream not in groupBy, only "app" should survive.
	got := buildManualMetricLabels(streamLabels, []string{"app"}, false)
	if len(got) != 1 || got["app"] != "api" {
		t.Errorf("expected {app:api}, got %v", got)
	}
}

func TestShouldUseManualRangeMetricCompat_WithoutSlidingWindowNowUsesManualPath(t *testing.T) {
	// Sliding window (range > step) + without() must use the manual path now that
	// buildManualMetricLabels correctly expands _stream into all stream labels.
	// Previously this fell back to native VL tumbling stats, producing wrong results.
	want := true
	got := shouldUseManualRangeMetricCompat("app:=api", "rate", false /* sliding */, `sum without(level) (rate({app="api"}[10m]))`)
	if got != want {
		t.Errorf("sliding-window without() should use manual path, got %v", got)
	}
}

func TestQueryRange_SlidingWindowWithout_UsesManualPath(t *testing.T) {
	// Verify that sum without(level)(rate[10m]) with step=1m (sliding window) goes
	// through the manual log-fetch path, not native VL tumbling stats.
	manualPathHit := false

	base := time.Unix(1700000000, 0).UTC()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			// Manual path: log-fetch endpoint
			manualPathHit = true
			w.Header().Set("Content-Type", "application/stream+json")
			// Two log entries with different app labels — rate over 10m window
			fmt.Fprintf(w, "{\"_msg\":\"hit\",\"_time\":\"%s\",\"_stream\":\"{app=\\\"api\\\"}\"}\n",
				base.Format(time.RFC3339Nano))
			fmt.Fprintf(w, "{\"_msg\":\"hit\",\"_time\":\"%s\",\"_stream\":\"{app=\\\"web\\\"}\"}\n",
				base.Add(time.Second).Format(time.RFC3339Nano))
			return
		}
		if r.URL.Path == "/select/logsql/stats_query_range" {
			t.Error("should NOT hit stats_query_range (native tumbling path) for sliding window without()")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"resultType":"matrix","result":[]}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p, err := New(Config{BackendURL: vlBackend.URL, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}

	startNs := strconv.FormatInt(base.UnixNano(), 10)
	endNs := strconv.FormatInt(base.Add(2*time.Minute).UnixNano(), 10)
	// step=60s, range=10m → sliding window (range > step)
	r := httptest.NewRequest("GET", fmt.Sprintf(
		`/loki/api/v1/query_range?query=sum+without(level)(rate({app%%3D"api"}[10m]))&start=%s&end=%s&step=60`,
		startNs, endNs), nil)
	w := httptest.NewRecorder()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	if !manualPathHit {
		t.Error("expected manual log-fetch path to be used for sliding window without()")
	}
}

func FuzzParseOriginalRangeMetricSpec(f *testing.F) {
	seeds := []string{
		`rate({app="api"}[5m])`,
		`bytes_rate({app="api"}[1m])`,
		`sum_over_time({app="api"} | unwrap duration_ms [5m])`,
		`quantile_over_time(0.95, {app="api"} | unwrap latency [5m])`,
		`not a metric`,
		``,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, query string) {
		spec, ok := parseOriginalRangeMetricSpec(query)
		if !ok {
			return
		}
		if strings.TrimSpace(spec.Func) == "" {
			t.Fatalf("parsed spec must include function: %+v", spec)
		}
		if spec.Window < 0 {
			t.Fatalf("window must be non-negative: %+v", spec)
		}
	})
}

// TestBareParserCountOverTime_MixedInput_SkipsFastPath ensures that a bare
// parser count_over_time query WITHOUT explicit __error__ handling routes to
// the slow log-fetch path, not native VL stats. VL stats would count ALL lines
// regardless of parse success; Loki excludes lines that fail parsing.
func TestBareParserCountOverTime_MixedInput_SkipsFastPath(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()

	var statsCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			// The fast path must NOT be taken without __error__ handling.
			statsCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case "/select/logsql/query":
			// Slow path: return 3 valid JSON lines + 2 invalid ones.
			w.Header().Set("Content-Type", "application/x-ndjson")
			for i, msg := range []string{
				`{"method":"GET","status":200}`,
				`{"method":"POST","status":201}`,
				`not-json-line-1`,
				`{"method":"DELETE","status":204}`,
				`not-json-line-2`,
			} {
				ts := base.Add(time.Duration(i) * time.Second)
				_, _ = fmt.Fprintf(w,
					`{"_time":%q,"_msg":%q,"_stream":"{app=\"api\"}"}`+"\n",
					ts.Format(time.RFC3339Nano), msg,
				)
			}
		default:
			// Allow the parser-detection probe (limit=1) silently.
			if r.FormValue("limit") == "1" {
				w.Header().Set("Content-Type", "application/x-ndjson")
				return
			}
			t.Errorf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// No "__error__" in query — must not take fast path.
	params.Set("query", `count_over_time({app="api"} | json [60s])`)
	params.Set("start", strconv.FormatInt(base.Add(-time.Minute).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if statsCalled {
		t.Fatalf("stats_query_range was called — fast path taken without __error__ handling; " +
			"this violates Loki's error model (parse failures must be excluded from metric counts)")
	}
}

// TestBareParserCountOverTime_WithErrorHandling_UsesFastPath ensures that when
// __error__ is explicitly handled, the tumbling-window fast path IS taken.
func TestBareParserCountOverTime_WithErrorHandling_UsesFastPath(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()

	var statsCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000000,"3"]]}]}}`))
		case "/select/logsql/query":
			// Should not be called on the fast path.
			if r.FormValue("limit") != "1" {
				t.Errorf("slow-path query endpoint called unexpectedly (limit=%s)", r.FormValue("limit"))
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
		default:
			t.Errorf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// Explicit __error__ handling — fast path allowed.
	params.Set("query", `count_over_time({app="api"} | json | drop __error__, __error_details__ [60s])`)
	params.Set("start", strconv.FormatInt(base.Add(-time.Minute).Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !statsCalled {
		t.Fatalf("stats_query_range was NOT called — fast path should be taken when __error__ is handled")
	}
}

// TestSumByCountOverTime_NoParser_UsesStatsQueryRange checks that the outer
// sum-by count_over_time fast path (collectRangeMetricHits) activates for pure
// stream-selector queries (no parser) and correctly returns multi-series output.
// This is the primary benchmark hot path: pprof showed raw-log scan was 39% CPU.
func TestSumByCountOverTime_NoParser_UsesStatsQueryRange(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	step := 60

	var statsCalled, queryCalled bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsCalled = true
			// Verify the stats query appends the count() as c clause.
			q := r.Form.Get("query")
			if !strings.Contains(q, "| stats by (app) count() as c") {
				t.Errorf("unexpected stats query: %q", q)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w,
				`{"status":"success","data":{"resultType":"matrix","result":[`+
					`{"metric":{"__name__":"c","app":"api-gateway"},"values":[[%d,"10"],[%d,"20"]]},`+
					`{"metric":{"__name__":"c","app":"auth"},"values":[[%d,"5"],[%d,"8"]]}]}}`,
				base.Unix(), base.Add(time.Duration(step)*time.Second).Unix(),
				base.Unix(), base.Add(time.Duration(step)*time.Second).Unix(),
			)
		case "/select/logsql/query":
			if r.Form.Get("limit") == "1000000" {
				queryCalled = true
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
		default:
			t.Errorf("unexpected backend path: %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `sum by (app) (count_over_time({env="prod"}[5m]))`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(2*time.Duration(step)*time.Second).Unix(), 10))
	params.Set("step", strconv.Itoa(step))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !statsCalled {
		t.Fatalf("stats_query_range was NOT called — fast path inactive for sum by (app) (count_over_time)")
	}
	if queryCalled {
		t.Fatalf("raw log scan was triggered — fast path did not prevent slow path")
	}

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected 2 series (api-gateway, auth), got %d: %s", len(resp.Data.Result), rec.Body.String())
	}
	apps := map[string]bool{}
	for _, s := range resp.Data.Result {
		apps[s.Metric["app"]] = true
		if len(s.Values) == 0 {
			t.Errorf("series %v has no values", s.Metric)
		}
	}
	if !apps["api-gateway"] || !apps["auth"] {
		t.Fatalf("unexpected series labels: %v", apps)
	}
}
