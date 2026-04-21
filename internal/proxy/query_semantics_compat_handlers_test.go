package proxy

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestContract_QueryRange_BareParserMetricCompatPreservesSeriesLabels(t *testing.T) {
	startNanos := int64(1704067200 * 1e9)
	endNanos := startNanos + int64(2*time.Minute)

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(
			`{"_time":"` + strconv.FormatInt(startNanos+int64(30*time.Second), 10) + `","_stream":"{app=\"api-gateway\"}","level":"error","_msg":"{\"status\":500}","status":"500"}` + "\n" +
				`{"_time":"` + strconv.FormatInt(startNanos+int64(90*time.Second), 10) + `","_stream":"{app=\"api-gateway\"}","level":"error","_msg":"{\"status\":500}","status":"500"}` + "\n",
		))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api-gateway"} | json | status >= 500 [5m])`)+"&start="+strconv.FormatInt(startNanos, 10)+"&end="+strconv.FormatInt(endNanos, 10)+"&step=60", nil)
	resp := httptest.NewRecorder()

	p.handleQueryRange(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	var body map[string]interface{}
	mustUnmarshal(t, resp.Body.Bytes(), &body)
	data := assertDataIsObject(t, body)
	if got := data["resultType"]; got != "matrix" {
		t.Fatalf("expected matrix resultType, got %v", got)
	}
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one result series, got %d", len(result))
	}
	series := result[0].(map[string]interface{})
	metric := series["metric"].(map[string]interface{})
	if metric["app"] != "api-gateway" {
		t.Fatalf("expected app label, got %v", metric)
	}
	if metric["status"] != "500" {
		t.Fatalf("expected parser-derived status label, got %v", metric)
	}
	values := series["values"].([]interface{})
	if len(values) == 0 {
		t.Fatalf("expected matrix values, got empty series: %v", series)
	}
}

func TestContract_Query_BareParserMetricCompatUnwrapVector(t *testing.T) {
	evalNanos := int64(1704067200 * 1e9)

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(
			`{"_time":"` + strconv.FormatInt(evalNanos-int64(2*time.Minute), 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"first","duration_ms":"10"}` + "\n" +
				`{"_time":"` + strconv.FormatInt(evalNanos-int64(1*time.Minute), 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"second","duration_ms":"20"}` + "\n",
		))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`)+"&time="+strconv.FormatInt(evalNanos, 10), nil)
	resp := httptest.NewRecorder()

	p.handleQuery(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	var body map[string]interface{}
	mustUnmarshal(t, resp.Body.Bytes(), &body)
	data := assertDataIsObject(t, body)
	if got := data["resultType"]; got != "vector" {
		t.Fatalf("expected vector resultType, got %v", got)
	}
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one vector result, got %d", len(result))
	}
	entry := result[0].(map[string]interface{})
	value := entry["value"].([]interface{})
	if value[1] != "15" {
		t.Fatalf("expected averaged unwrap value 15, got %v", value)
	}
}

func TestContract_Query_AbsentOverTimeCompatReturnsSyntheticVector(t *testing.T) {
	evalNanos := int64(1704067200 * 1e9)

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/stats_query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`absent_over_time({app="missing",env="dev",cluster=~"ops-.*"}[5m])`)+"&time="+strconv.FormatInt(evalNanos, 10), nil)
	resp := httptest.NewRecorder()

	p.handleQuery(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	var body map[string]interface{}
	mustUnmarshal(t, resp.Body.Bytes(), &body)
	data := assertDataIsObject(t, body)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected single absent vector sample, got %d", len(result))
	}
	entry := result[0].(map[string]interface{})
	metric := entry["metric"].(map[string]interface{})
	if metric["app"] != "missing" || metric["env"] != "dev" {
		t.Fatalf("expected exact-match labels only, got %v", metric)
	}
	if _, found := metric["cluster"]; found {
		t.Fatalf("did not expect regex matcher label in absent vector metric: %v", metric)
	}
	value := entry["value"].([]interface{})
	if value[1] != "1" {
		t.Fatalf("expected absent_over_time to return 1, got %v", value)
	}
}

func TestContract_Query_AbsentOverTimeCompatReturnsEmptyWhenPresent(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"demo"},"value":[1704067200,"2"]}]}}`))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`absent_over_time({app="demo"}[5m])`)+"&time=1704067200000000000", nil)
	resp := httptest.NewRecorder()

	p.handleQuery(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	var body map[string]interface{}
	mustUnmarshal(t, resp.Body.Bytes(), &body)
	data := assertDataIsObject(t, body)
	result := assertResultIsArray(t, data)
	if len(result) != 0 {
		t.Fatalf("expected empty absent_over_time vector when source exists, got %v", result)
	}
}

func TestProxyHelpers_FetchBareParserMetricSeries_PropagatesBackendStatus(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `backend overloaded`, http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	_, err := p.fetchBareParserMetricSeries(t.Context(), `count_over_time({app="api-gateway"} | json | status >= 500 [5m])`, bareParserMetricCompatSpec{
		funcName:    "count_over_time",
		baseQuery:   `{app="api-gateway"} | json | status >= 500`,
		rangeWindow: 5 * time.Minute,
	}, "1704067200000000000", "1704067500000000000")
	var apiErr *vlAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected vlAPIError, got %v", err)
	}
	if apiErr.status != http.StatusBadGateway {
		t.Fatalf("expected propagated 502, got %d", apiErr.status)
	}
}

func TestProxyHelpers_MetricValueFormattingAndWindowMath(t *testing.T) {
	if got := formatMetricSampleValue(math.NaN()); got != "NaN" {
		t.Fatalf("expected NaN formatting, got %q", got)
	}
	if got := formatMetricSampleValue(math.Inf(1)); got != "+Inf" {
		t.Fatalf("expected +Inf formatting, got %q", got)
	}
	if got := formatMetricSampleValue(math.Inf(-1)); got != "-Inf" {
		t.Fatalf("expected -Inf formatting, got %q", got)
	}
	if got := metricWindowValue("rate", 30, 10*time.Second); got != 3 {
		t.Fatalf("expected rate window value 3, got %v", got)
	}
	window := []bareParserMetricSample{{value: 10}, {value: 20}, {value: 30}}
	if got := bareParserMetricWindowValue("avg_over_time", window, bareParserMetricCompatSpec{rangeWindow: 5 * time.Minute}); got != 20 {
		t.Fatalf("expected avg_over_time 20, got %v", got)
	}
	if got := bareParserMetricWindowValue("quantile_over_time", window, bareParserMetricCompatSpec{rangeWindow: 5 * time.Minute, quantile: 0.5}); got != 20 {
		t.Fatalf("expected median quantile 20, got %v", got)
	}
	if got := metricWindowValue("count_over_time", 30, 10*time.Second); got != 30 {
		t.Fatalf("expected non-rate metric window value 30, got %v", got)
	}
}

func TestProxyHelpers_ParseBareParserMetricCompatSpec_QuantileAndRejects(t *testing.T) {
	spec, ok := parseBareParserMetricCompatSpec(`quantile_over_time(0.95, {app="api-gateway"} | json | unwrap duration_ms [5m])`)
	if !ok {
		t.Fatal("expected quantile_over_time unwrap query to be recognized")
	}
	if spec.funcName != "quantile_over_time" || spec.unwrapField != "duration_ms" || spec.quantile != 0.95 {
		t.Fatalf("unexpected quantile compat spec: %+v", spec)
	}

	if _, ok := parseBareParserMetricCompatSpec(`count_over_time({app="api-gateway"} |= "GET" [5m])`); ok {
		t.Fatal("expected query without extracting parser stage to bypass bare compat handling")
	}
	if _, ok := parseBareParserMetricCompatSpec(`quantile_over_time(1.5, {app="api-gateway"} | json | unwrap duration_ms [5m])`); ok {
		t.Fatal("expected invalid quantile to be rejected")
	}
	if got := extractBareParserUnwrapField(`{app="api-gateway"} | json | unwrap duration(duration_ms)`); got != "duration_ms" {
		t.Fatalf("expected duration(...) unwrap field extraction, got %q", got)
	}
}

func TestProxyHelpers_BareParserMetricWindowValue_CoversMetricFunctions(t *testing.T) {
	window := []bareParserMetricSample{{value: 10}, {value: 20}, {value: 30}}
	spec := bareParserMetricCompatSpec{rangeWindow: 10 * time.Second, quantile: 0.5}

	tests := map[string]float64{
		"count_over_time":  60,
		"rate":             6,
		"bytes_over_time":  60,
		"bytes_rate":       6,
		"sum_over_time":    60,
		"avg_over_time":    20,
		"max_over_time":    30,
		"min_over_time":    10,
		"first_over_time":  10,
		"last_over_time":   30,
		"stdvar_over_time": 66.66666666666667,
	}

	for funcName, want := range tests {
		if got := bareParserMetricWindowValue(funcName, window, spec); math.Abs(got-want) > 1e-9 {
			t.Fatalf("%s: got %v want %v", funcName, got, want)
		}
	}
	if got := bareParserMetricWindowValue("stddev_over_time", window, spec); math.Abs(got-math.Sqrt(66.66666666666667)) > 1e-9 {
		t.Fatalf("stddev_over_time: got %v", got)
	}
}

func TestContract_Query_AbsentOverTimeCompatReturnsBackendError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `backend unavailable`, http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`absent_over_time({app="missing"}[5m])`)+"&time=1704067200000000000", nil)
	resp := httptest.NewRecorder()

	p.handleQuery(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "backend unavailable") {
		t.Fatalf("expected backend error body, got %s", resp.Body.String())
	}
}

func TestContract_QueryRange_BareParserMetricCompatValidationAndBackendError(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	spec := bareParserMetricCompatSpec{
		funcName:    "count_over_time",
		baseQuery:   `{app="api-gateway"} | json | status >= 500`,
		rangeWindow: 5 * time.Minute,
	}

	badStartReq := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api-gateway"} | json | status >= 500 [5m])`)+"&start=bad&end=1704067500000000000&step=60", nil)
	badStartResp := httptest.NewRecorder()
	p.proxyBareParserMetricQueryRange(badStartResp, badStartReq, time.Now(), badStartReq.URL.Query().Get("query"), spec)
	if badStartResp.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid start to return 400, got %d", badStartResp.Code)
	}

	badStepReq := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api-gateway"} | json | status >= 500 [5m])`)+"&start=1704067200000000000&end=1704067500000000000&step=0", nil)
	badStepResp := httptest.NewRecorder()
	p.proxyBareParserMetricQueryRange(badStepResp, badStepReq, time.Now(), badStepReq.URL.Query().Get("query"), spec)
	if badStepResp.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid step to return 400, got %d", badStepResp.Code)
	}

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `upstream timeout`, http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p = newTestProxy(t, vlBackend.URL)
	errReq := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api-gateway"} | json | status >= 500 [5m])`)+"&start=1704067200000000000&end=1704067500000000000&step=60", nil)
	errResp := httptest.NewRecorder()
	p.proxyBareParserMetricQueryRange(errResp, errReq, time.Now(), errReq.URL.Query().Get("query"), spec)
	if errResp.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected timeout-shaped upstream error to map to 504, got %d body=%s", errResp.Code, errResp.Body.String())
	}
}

func TestContract_Query_BareParserMetricCompatPropagatesBackendError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `backend unavailable`, http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`)+"&time=1704067200000000000", nil)
	resp := httptest.NewRecorder()

	p.handleQuery(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("expected upstream 502 to propagate, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestProxyHelpers_FetchBareParserMetricSeries_SkipsMalformedEntriesAndSortsSeries(t *testing.T) {
	startNanos := int64(1704067200 * 1e9)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(strings.Join([]string{
			`not-json`,
			`{"_time":"bad","_stream":"{app=\"api-gateway\"}","_msg":"{\"status\":500}","status":"500"}`,
			`{"_time":"` + strconv.FormatInt(startNanos, 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"{\"status\":500}","status":"500"}`,
			`{"_time":"` + strconv.FormatInt(startNanos+int64(time.Second), 10) + `","_stream":"{app=\"worker\"}","_msg":"{\"status\":404}","status":"404"}`,
		}, "\n")))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	series, err := p.fetchBareParserMetricSeries(t.Context(), `count_over_time({app=~"api-gateway|worker"} | json | status >= 400 [5m])`, bareParserMetricCompatSpec{
		funcName:    "count_over_time",
		baseQuery:   `{app=~"api-gateway|worker"} | json | status >= 400`,
		rangeWindow: 5 * time.Minute,
	}, strconv.FormatInt(startNanos, 10), strconv.FormatInt(startNanos+int64(2*time.Minute), 10))
	if err != nil {
		t.Fatalf("expected successful bare parser series fetch, got %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected two valid series after skipping malformed entries, got %d", len(series))
	}
	if series[0].metric["app"] != "api-gateway" || series[1].metric["app"] != "worker" {
		t.Fatalf("expected canonical app ordering, got %+v", series)
	}
}

func TestProxyHelpers_FetchBareParserMetricSeries_UnwrapSkipsMissingAndNonNumericValues(t *testing.T) {
	startNanos := int64(1704067200 * 1e9)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(strings.Join([]string{
			`{"_time":"` + strconv.FormatInt(startNanos, 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"first"}`,
			`{"_time":"` + strconv.FormatInt(startNanos+int64(time.Second), 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"second","duration_ms":"not-a-number"}`,
			`{"_time":"` + strconv.FormatInt(startNanos+int64(2*time.Second), 10) + `","_stream":"{app=\"api-gateway\"}","_msg":"third","duration_ms":"25"}`,
		}, "\n")))
	}))
	defer vlBackend.Close()

	p := newTestProxy(t, vlBackend.URL)
	series, err := p.fetchBareParserMetricSeries(t.Context(), `avg_over_time({app="api-gateway"} | json | unwrap duration_ms [5m])`, bareParserMetricCompatSpec{
		funcName:    "avg_over_time",
		baseQuery:   `{app="api-gateway"} | json | unwrap duration_ms`,
		rangeWindow: 5 * time.Minute,
		unwrapField: "duration_ms",
	}, strconv.FormatInt(startNanos, 10), strconv.FormatInt(startNanos+int64(3*time.Second), 10))
	if err != nil {
		t.Fatalf("expected successful unwrap fetch, got %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("expected one surviving series, got %d", len(series))
	}
	if len(series[0].samples) != 1 || series[0].samples[0].value != 25 {
		t.Fatalf("expected only numeric unwrap sample to survive, got %+v", series[0].samples)
	}
	if _, found := series[0].metric["duration_ms"]; found {
		t.Fatalf("did not expect unwrap field to remain as a metric label: %+v", series[0].metric)
	}
}

func TestProxyHelpers_ParseAbsentOverTimeCompatSpec_RejectsInvalidInput(t *testing.T) {
	if _, ok := parseAbsentOverTimeCompatSpec(`absent_over_time([5m])`); ok {
		t.Fatal("expected empty absent_over_time query to be rejected")
	}
	if _, ok := parseAbsentOverTimeCompatSpec(`absent_over_time({app="demo"}[bad])`); ok {
		t.Fatal("expected invalid absent_over_time range to be rejected")
	}
}

func TestProxyHelpers_BuildAbsentInstantVector_UsesCurrentTimeFallback(t *testing.T) {
	before := time.Now().Unix()
	body := buildAbsentInstantVector("bad-time", map[string]string{"app": "missing"})
	after := time.Now().Unix()

	data := body["data"].(map[string]interface{})
	result := data["result"].([]lokiVectorResult)
	if len(result) != 1 {
		t.Fatalf("expected one absent vector sample, got %d", len(result))
	}
	ts := result[0].Value[0].(float64)
	if ts < float64(before) || ts > float64(after)+1 {
		t.Fatalf("expected fallback timestamp between %d and %d, got %v", before, after, ts)
	}
}

func TestContract_Query_AbsentOverTimeCompatTranslateErrorReturnsBadRequest(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query=ignored&time=1704067200000000000", nil)
	resp := httptest.NewRecorder()

	p.proxyAbsentOverTimeQuery(resp, req, time.Now(), `{app="demo"`, absentOverTimeCompatSpec{
		baseQuery:   `{app="demo"`,
		rangeWindow: 5 * time.Minute,
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected translate error to return 400, got %d body=%s", resp.Code, resp.Body.String())
	}
}
