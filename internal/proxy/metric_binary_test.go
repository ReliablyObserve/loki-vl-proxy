package proxy

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseTopKWrapper(t *testing.T) {
	tests := []struct {
		name     string
		logql    string
		wantK    int
		wantDesc bool
		wantOK   bool
	}{
		{"topk basic", `topk(5, sum by (app) (rate({app="nginx"}[5m])))`, 5, true, true},
		{"bottomk basic", `bottomk(3, sum by (app) (count_over_time({app="nginx"}[5m])))`, 3, false, true},
		{"topk k=1", `topk(1, rate({app="nginx"}[5m]))`, 1, true, true},
		{"not topk — sum", `sum by (app) (rate({app="nginx"}[5m]))`, 0, false, false},
		{"not topk — rate bare", `rate({app="nginx"}[5m])`, 0, false, false},
		{"not topk — binary op", `topk(5, rate({a="b"}[5m])) / 2`, 0, false, false},
		{"topk k=0 invalid", `topk(0, rate({a="b"}[5m]))`, 0, false, false},
		{"empty string", ``, 0, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotK, gotDesc, gotOK := parseTopKWrapper(tc.logql)
			if gotOK != tc.wantOK {
				t.Errorf("parseTopKWrapper(%q) ok=%v, want %v", tc.logql, gotOK, tc.wantOK)
				return
			}
			if !tc.wantOK {
				return
			}
			if gotK != tc.wantK {
				t.Errorf("parseTopKWrapper(%q) k=%d, want %d", tc.logql, gotK, tc.wantK)
			}
			if gotDesc != tc.wantDesc {
				t.Errorf("parseTopKWrapper(%q) descending=%v, want %v", tc.logql, gotDesc, tc.wantDesc)
			}
		})
	}
}

func TestApplyTopKToMatrix(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"app":"high"},"values":[[1700000300,"10.0"],[1700000600,"12.0"]]},` +
		`{"metric":{"app":"mid"},"values":[[1700000300,"5.0"],[1700000600,"6.0"]]},` +
		`{"metric":{"app":"low"},"values":[[1700000300,"1.0"],[1700000600,"2.0"]]}` +
		`]}}`)

	// topk(2) → keep "high" and "mid" (max 12 and 6), drop "low" (max 2).
	got := applyTopKToMatrix(input, 2, true)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2): got %d series, want 2", len(resp.Data.Result))
	}
	apps := map[string]bool{}
	for _, s := range resp.Data.Result {
		apps[s.Metric["app"]] = true
	}
	if !apps["high"] || !apps["mid"] {
		t.Errorf("topk(2): expected high+mid, got %v", apps)
	}

	// bottomk(1) → keep "low" (max 2), drop others.
	got2 := applyTopKToMatrix(input, 1, false)
	var resp2 struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got2, &resp2); err != nil {
		t.Fatalf("unmarshal bottomk: %v", err)
	}
	if len(resp2.Data.Result) != 1 {
		t.Fatalf("bottomk(1): got %d series, want 1", len(resp2.Data.Result))
	}
	if resp2.Data.Result[0].Metric["app"] != "low" {
		t.Errorf("bottomk(1): expected low, got %v", resp2.Data.Result[0].Metric)
	}

	// k >= number of series → return all.
	got3 := applyTopKToMatrix(input, 10, true)
	var resp3 struct {
		Data struct {
			Result []struct{} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got3, &resp3); err != nil {
		t.Fatalf("unmarshal k>=n: %v", err)
	}
	if len(resp3.Data.Result) != 3 {
		t.Fatalf("topk(10) of 3: got %d series, want 3", len(resp3.Data.Result))
	}
}

func TestApplyTopKToVector(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"app":"a"},"value":[1700000300,"10.0"]},` +
		`{"metric":{"app":"b"},"value":[1700000300,"5.0"]},` +
		`{"metric":{"app":"c"},"value":[1700000300,"1.0"]}` +
		`]}}`)

	got := applyTopKToVector(input, 2, true)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2): got %d, want 2", len(resp.Data.Result))
	}
}

func TestQueryRange_TopKFiltersToKSeries(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"app":"high"},"values":[[1700000300,"10.0"],[1700000600,"12.0"]]},`+
				`{"metric":{"app":"mid"},"values":[[1700000300,"5.0"],[1700000600,"6.0"]]},`+
				`{"metric":{"app":"low"},"values":[[1700000300,"1.0"],[1700000600,"2.0"]]}]}}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `topk(2, sum by (app) (rate({app=~".*"}[5m])))`)
	params.Set("start", "1700000000")
	params.Set("end", "1700001800")
	params.Set("step", "300")
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
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("topk(2) got %d series, want 2", len(resp.Data.Result))
	}
	apps := map[string]bool{}
	for _, s := range resp.Data.Result {
		apps[s.Metric["app"]] = true
	}
	if !apps["high"] || !apps["mid"] {
		t.Errorf("topk(2) kept %v, want high+mid", apps)
	}
}

func TestQueryRange_BottomKFiltersToKSeries(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
				`{"metric":{"app":"high"},"values":[[1700000300,"10.0"]]},`+
				`{"metric":{"app":"low"},"values":[[1700000300,"1.0"]]}]}}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `bottomk(1, sum by (app) (count_over_time({app=~".*"}[5m])))`)
	params.Set("start", "1700000000")
	params.Set("end", "1700001800")
	params.Set("step", "300")
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
			} `json:"result"`
		} `json:"data"`
	}
	if err := stdjson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("bottomk(1) got %d series, want 1", len(resp.Data.Result))
	}
	if resp.Data.Result[0].Metric["app"] != "low" {
		t.Errorf("bottomk(1) got app=%s, want low", resp.Data.Result[0].Metric["app"])
	}
}
