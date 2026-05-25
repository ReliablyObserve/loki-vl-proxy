package proxy

import (
    "encoding/json"
    "fmt"
    "net/http"
    "net/http/httptest"
    "net/url"
    "strconv"
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
