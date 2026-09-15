package verify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func TestVLRows(t *testing.T) {
	cases := []struct {
		name, path, body string
		want             int
		wantErr          bool
	}{
		{"ndjson rows", "/select/logsql/query", "{\"_msg\":\"a\"}\n{\"_msg\":\"b\"}\n", 2, false},
		{"ndjson empty", "/select/logsql/query", "", 0, false},
		{"ndjson invalid", "/select/logsql/query", "not json\n", 0, true},
		{"hits with data", "/select/logsql/hits", `{"hits":[{"fields":{"level":"info"},"values":[0,3]},{"fields":{"level":"warn"},"values":[2]}]}`, 5, false},
		{"hits all zero", "/select/logsql/hits", `{"hits":[{"fields":{},"values":[0,0,0]}]}`, 0, false},
		{"hits none", "/select/logsql/hits", `{"hits":[]}`, 0, false},
		{"stats range series", "/select/logsql/stats_query_range", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1,"2"]]}]}}`, 1, false},
		{"stats no rows", "/select/logsql/stats_query_range", `{"status":"success","data":{"resultType":"matrix","result":[]}}`, 0, false},
		{"field values", "/select/logsql/field_values", `{"values":[{"value":"a","hits":1}]}`, 1, false},
		{"stream ids empty", "/select/logsql/stream_ids", `{"values":[]}`, 0, false},
	}
	for _, tc := range cases {
		got, err := VLRows(tc.path, []byte(tc.body))
		if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
			t.Errorf("%s: got %d err=%v, want %d err=%v", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestCheckVL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("query") {
		case "ok":
			w.Write([]byte("{\"_msg\":\"x\"}\n"))
		case "empty":
		case "bad":
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`cannot parse query`))
		case "zero-hits":
			w.Write([]byte(`{"hits":[{"values":[0,0]}]}`))
		case "warned":
			w.Header().Set("Warning", `199 - "partial"`)
			w.Write([]byte("{\"_msg\":\"x\"}\n"))
		}
	}))
	defer srv.Close()
	q := func(name, path string, allowEmpty bool) workload.Query {
		return workload.Query{Name: name, Path: path, Params: map[string][]string{"query": {name}}, Shape: workload.Tolerance{AllowEmpty: allowEmpty}}
	}
	results := CheckVL(context.Background(), srv.URL, []workload.Query{
		q("ok", "/select/logsql/query", false),
		q("empty", "/select/logsql/query", false),
		q("bad", "/select/logsql/query", false),
		q("zero-hits", "/select/logsql/hits", false),
		q("empty", "/select/logsql/query", true),
		q("warned", "/select/logsql/query", false),
	}, 5*time.Second)
	passed := []bool{true, false, false, false, true, false}
	for i, r := range results {
		if r.Passed != passed[i] {
			t.Errorf("%d %s: passed=%v err=%v", i, r.QueryName, r.Passed, r.Err)
		}
	}
	summary := VLSummary("heavy", results)
	for _, want := range []string{"heavy/empty", "empty result", "heavy/bad", "HTTP 400", "heavy/zero-hits", "heavy/warned", "degraded answer"} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "heavy/ok") {
		t.Errorf("passing query in summary:\n%s", summary)
	}
}
