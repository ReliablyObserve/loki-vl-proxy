package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// An instant `sum(sum_over_time(... | json | unwrap x [r]))` collapses every
// stream into one series without labels, as Loki's bare outer sum does; the
// translated `stats by (_stream, _msg)` identity is not a label of the result.
//
// conformance: profiles/sanitized-json-key-grouping
func TestInstantUnwrapSumCollapsesStreams(t *testing.T) {
	rows := `{"_time":"2026-01-01T00:59:00Z","_msg":"{\"code\":200}","_stream":"{app=\"web\",pod=\"a\"}","app":"web","pod":"a","code":"200"}` + "\n" +
		`{"_time":"2026-01-01T00:59:01Z","_msg":"{\"code\":201}","_stream":"{app=\"web\",pod=\"b\"}","app":"web","pod":"b","code":"201"}` + "\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(rows))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	q := url.Values{"query": {`sum(sum_over_time({app="web"} | json | unwrap code [1h]))`}, "time": {"1767229200"}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query?"+q.Encode(), nil)
	if res.Code != http.StatusOK {
		t.Fatalf("%d %s", res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Metric) != 0 || resp.Data.Result[0].Value[1] != "401" {
		t.Fatalf("want one unlabelled series worth 401: %s", res.Body)
	}
}

// Only the additive outer sum merges the per-stream values; every other outer
// operator still reads one value per stream.
//
// conformance: profiles/sanitized-json-key-grouping
func TestInstantUnwrapOtherAggregationsKeepPerStreamValues(t *testing.T) {
	rows := `{"_time":"2026-01-01T00:59:00Z","_msg":"{\"code\":200}","_stream":"{app=\"web\",pod=\"a\"}","app":"web","pod":"a","code":"200"}` + "\n" +
		`{"_time":"2026-01-01T00:59:01Z","_msg":"{\"code\":201}","_stream":"{app=\"web\",pod=\"b\"}","app":"web","pod":"b","code":"201"}` + "\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(rows))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, query := range []string{
		`count(sum_over_time({app="web"} | json | unwrap code [1h]))`,
		`max(sum_over_time({app="web"} | json | unwrap code [1h]))`,
		`avg(sum_over_time({app="web"} | json | unwrap code [1h]))`,
	} {
		q := url.Values{"query": {query}, "time": {"1767229200"}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query?"+q.Encode(), nil)
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, res.Code, res.Body)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Data.Result) == 1 && len(resp.Data.Result[0].Metric) == 0 {
			t.Fatalf("%s was collapsed into one unlabelled series like a sum: %s", query, res.Body)
		}
	}
}
