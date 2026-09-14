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

// Operands with different range windows reach different execution paths: a
// sliding window is evaluated at start+k*step, while a tumbling window is served
// from VictoriaLogs buckets on the epoch-aligned step grid. When the request start
// is not a multiple of the step, joining the two by timestamp matched nothing and
// the expression returned no series while Loki returned one (regression:
// sum(rate({env="production"}[5m])) - sum(rate({env="production"}[1m]))).
// Binary operands are evaluated on the step-aligned grid, as Loki's
// query frontend does with align_queries_with_step.
func TestBinaryMixedWindowsShareStepAlignedAxis(t *testing.T) {
	const step = int64(60)
	start := time.Unix(1700000000+17, 0) // not a multiple of the step
	end := start.Add(10 * time.Minute)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/select/logsql/stats_query_range" {
			http.Error(w, "unexpected endpoint "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		_ = r.ParseForm()
		from, _ := parseLokiTimeToUnixNano(r.FormValue("start"))
		to, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
		var points []string
		// One line per second: every complete 60s bucket counts 60.
		for ts := (from / 1e9) - (from/1e9)%step; ts <= to/1e9; ts += step {
			points = append(points, fmt.Sprintf(`[%d,"60"]`, ts))
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[%s]}]}}`, strings.Join(points, ","))
	}))
	defer backend.Close()

	p := newGapTestProxy(t, backend.URL)
	params := url.Values{
		"query": {`sum(rate({app="api"}[5m])) - sum(rate({app="api"}[1m]))`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatInt(step, 10)},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	if response.Data.ResultType != "matrix" || len(response.Data.Result) != 1 || len(response.Data.Result[0].Values) == 0 {
		t.Fatalf("want one populated series, got %s", rec.Body.String())
	}
	for _, point := range response.Data.Result[0].Values {
		ts, ok := point[0].(float64)
		if !ok || int64(ts)%step != 0 {
			t.Fatalf("point %v is not on the step-aligned grid: %s", point, rec.Body.String())
		}
	}
}
