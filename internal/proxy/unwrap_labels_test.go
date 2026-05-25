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

func TestUnwrapLabelsArePreserved(t *testing.T) {
    // sum_over_time({app="api-gateway"} | json | unwrap latency [5m]) with step=60s
    // The returned matrix must include stream labels (app="api-gateway") in each series.
    base := time.Unix(1700000000, 0).UTC()
    statsCalled := false
    vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        switch r.URL.Path {
        case "/select/logsql/stats_query_range":
            statsCalled = true
            w.Header().Set("Content-Type", "application/json")
            // VL returns _stream as a serialized label set string
            _, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
                `{"metric":{"_stream":"{app=\"api-gateway\",env=\"prod\"}"},` +
                `"values":[[` + strconv.FormatInt(base.Unix(), 10) + `,"150"],[` +
                strconv.FormatInt(base.Add(60*time.Second).Unix(), 10) + `,"200"]]}]}}`))
        default:
            fmt.Fprintf(w, `{}`)
        }
    }))
    defer vlBackend.Close()

    p := newGapTestProxy(t, vlBackend.URL)
    params := url.Values{}
    params.Set("query", `sum_over_time({app="api-gateway"} | json | unwrap latency [5m])`)
    params.Set("start", strconv.FormatInt(base.Add(-60*time.Second).Unix(), 10))
    params.Set("end", strconv.FormatInt(base.Add(120*time.Second).Unix(), 10))
    params.Set("step", "60")
    req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
    rec := httptest.NewRecorder()
    p.handleQueryRange(rec, req)

    t.Logf("response: %s", rec.Body.String())

    if rec.Code != http.StatusOK {
        t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
    }
    if !statsCalled {
        t.Error("expected stats_query_range to be called for sum_over_time unwrap")
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
        t.Fatalf("expected at least one series, got empty result: %s", rec.Body.String())
    }
    metric := resp.Data.Result[0].Metric
    t.Logf("metric labels: %v", metric)
    if metric["app"] != "api-gateway" {
        t.Errorf("expected app=api-gateway label in metric, got %v", metric)
    }
    if metric["env"] != "prod" {
        t.Errorf("expected env=prod label in metric, got %v", metric)
    }
}

// TestQueryRange_IncompleteUnwrapStub_ReturnsLogResults verifies that a query with
// | unwrap [range] (no field name) returns 200 with log streams rather than a 400
// parse error. Grafana's metric builder emits this stub via getDataSamples while
// the unwrap field picker is open — a 400 leaves the picker empty.
func TestQueryRange_IncompleteUnwrapStub_ReturnsLogResults(t *testing.T) {
    vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path == "/select/logsql/query" {
            w.Header().Set("Content-Type", "application/x-ndjson")
            // Return two sample log lines so the response has data for field extraction.
            fmt.Fprint(w, `{"_time":"2023-11-14T22:13:20Z","_stream":"{app=\"api-gateway\"}","_msg":"msg1","duration_ms":"150"}`+"\n")
            fmt.Fprint(w, `{"_time":"2023-11-14T22:13:21Z","_stream":"{app=\"api-gateway\"}","_msg":"msg2","duration_ms":"200"}`+"\n")
        }
    }))
    defer vlBackend.Close()

    p := newGapTestProxy(t, vlBackend.URL)
    params := url.Values{}
    // Grafana sends the inner log query with | unwrap [1s] — no field name after unwrap.
    params.Set("query", `{app="api-gateway"} | json | unwrap [1s]`)
    params.Set("start", "1700000000")
    params.Set("end", "1700000120")
    params.Set("step", "60")
    req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
    rec := httptest.NewRecorder()
    p.handleQueryRange(rec, req)

    if rec.Code != http.StatusOK {
        t.Fatalf("incomplete unwrap stub must return 200, got %d: %s", rec.Code, rec.Body.String())
    }
    if strings.Contains(rec.Body.String(), `"errorType"`) {
        t.Errorf("response must not contain error fields, got: %s", rec.Body.String())
    }
}

// TestQueryRange_IncompleteUnwrapConversion_ReturnsLogResults verifies that a query
// with | unwrap duration_seconds() [range] (empty conversion function, no field) returns
// 200 rather than 400. Grafana sends this when the user has picked a conversion format
// but hasn't yet chosen a field name in the unwrap builder.
func TestQueryRange_IncompleteUnwrapConversion_ReturnsLogResults(t *testing.T) {
    for _, stub := range []string{
        `{app="api-gateway"} | json | unwrap duration_seconds() [1s]`,
        `{app="api-gateway"} | json | unwrap bytes() [1s]`,
        `{app="api-gateway"} | json | unwrap duration() [1s]`,
    } {
        t.Run(stub, func(t *testing.T) {
            vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                if r.URL.Path == "/select/logsql/query" {
                    w.Header().Set("Content-Type", "application/x-ndjson")
                    fmt.Fprint(w, `{"_time":"2023-11-14T22:13:20Z","_stream":"{app=\"api-gateway\"}","_msg":"msg1"}`+"\n")
                }
            }))
            defer vlBackend.Close()

            p := newGapTestProxy(t, vlBackend.URL)
            params := url.Values{}
            params.Set("query", stub)
            params.Set("start", "1700000000")
            params.Set("end", "1700000120")
            params.Set("step", "60")
            req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
            rec := httptest.NewRecorder()
            p.handleQueryRange(rec, req)

            if rec.Code != http.StatusOK {
                t.Errorf("incomplete unwrap conversion stub %q must return 200, got %d: %s", stub, rec.Code, rec.Body.String())
            }
        })
    }
}
