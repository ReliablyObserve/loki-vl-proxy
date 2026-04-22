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
	params.Set("query", `rate({app="nginx"}[2m])`)
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
	params.Set("query", `bytes_rate({app="nginx"}[5m])`)
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
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		lines := []string{
			fmt.Sprintf(`{"_time":%q,"_msg":"a","_stream":"{app=\"nginx\"}","latency":1}`, base.Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"_time":%q,"_msg":"b","_stream":"{app=\"nginx\"}","latency":3}`, base.Add(60*time.Second).Format(time.RFC3339Nano)),
		}
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
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
	params.Set("query", `first_over_time({app="nginx"} | unwrap latency [2m])`)
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
	if !strings.Contains(rec.Body.String(), "without unwrap") {
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
	if !strings.Contains(rec.Body.String(), "without unwrap") {
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
	params.Set("query", `quantile_over_time(0.5, {app="nginx"} | unwrap latency [5m])`)
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
