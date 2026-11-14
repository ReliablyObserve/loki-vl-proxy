package vlemu

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func newTenants() *Tenants {
	return &Tenants{Rows: map[string][]Row{
		"0:0": {
			{TS: sec(0), Stream: `{app="a"}`, Msg: "plain line", Labels: map[string]string{"k": "v0"}},
			{TS: sec(10), Stream: `{app="a"}`, Msg: "second"},
			{TS: sec(20), Stream: `{app="b"}`, Msg: "third"},
		},
		"7:3": {
			{TS: sec(5), Stream: `{app="x"}`, Msg: "secret-tenant-seven", Labels: map[string]string{"k": "v7"}},
		},
	}}
}

func values(t *testing.T, body string) []valueHits {
	t.Helper()
	var out struct {
		Values []valueHits `json:"values"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return out.Values
}

// Each tenant reads its own rows: the AccountID and ProjectID headers pick them, and absent headers are tenant 0:0.
func TestTenantsKeepRowsApart(t *testing.T) {
	tn := newTenants()
	h := tn.Handler()
	count := func(headers ...string) int {
		rec := serve(h, "/select/logsql/query", url.Values{"query": {"*"}}, headers...)
		return strings.Count(rec.Body.String(), "\n")
	}
	if got := count(); got != 3 {
		t.Errorf("no headers read tenant 0:0: %d rows", got)
	}
	if got := count("AccountID", "0", "ProjectID", "0"); got != 3 {
		t.Errorf("0:0: %d rows", got)
	}
	if got := count("AccountID", "7", "ProjectID", "3"); got != 1 {
		t.Errorf("7:3: %d rows", got)
	}
	if got := count("AccountID", "7"); got != 0 {
		t.Errorf("7:0 holds no rows: %d", got)
	}
	if tn.Calls("0:0") != 2 || tn.Calls("7:3") != 1 || tn.Calls("7:0") != 1 || tn.Calls("9:9") != 0 {
		t.Errorf("calls: 0:0=%d 7:3=%d 7:0=%d 9:9=%d", tn.Calls("0:0"), tn.Calls("7:3"), tn.Calls("7:0"), tn.Calls("9:9"))
	}
	rec := serve(h, "/select/logsql/query", url.Values{"query": {"*"}}, "AccountID", "7", "ProjectID", "3")
	var row map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(rec.Body.String())), &row); err != nil {
		t.Fatal(err)
	}
	if row["_msg"] != "secret-tenant-seven" || row["app"] != "x" || row["k"] != "v7" || row["_stream"] != `{app="x"}` {
		t.Errorf("a row carries its stream labels and fields: %v", row)
	}
	if rec.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Errorf("content type %q", rec.Header().Get("Content-Type"))
	}
}

func TestTenantsIndexStatsAndMetadata(t *testing.T) {
	tn := newTenants()
	h := tn.Handler()
	// The index stats pipe: the rows and the distinct streams of the tenant under the names the query gives them.
	rec := serve(h, "/select/logsql/query", url.Values{"query": {"* | stats count() entries, count_uniq_hash(_stream_id) streams"}})
	var stats map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(rec.Body.String())), &stats); err != nil || !reflect.DeepEqual(stats, map[string]string{"entries": "3", "streams": "2"}) {
		t.Errorf("index stats = %v, %v", stats, err)
	}
	names := func(path string, headers ...string) []string {
		var out []string
		for _, v := range values(t, serve(h, path, nil, headers...).Body.String()) {
			out = append(out, v.Value)
		}
		return out
	}
	if got := names("/select/logsql/stream_field_names"); !reflect.DeepEqual(got, []string{"app"}) {
		t.Errorf("stream field names = %v", got)
	}
	if got := names("/select/logsql/field_names"); !reflect.DeepEqual(got, []string{"_msg", "_stream", "_stream_id", "_time", "app", "k"}) {
		t.Errorf("field names = %v", got)
	}
	if got := names("/select/logsql/field_names", "AccountID", "7", "ProjectID", "3"); !contains(got, "k") || !contains(got, "app") {
		t.Errorf("tenant 7 field names = %v", got)
	}
	hitsOf := func(path string, form url.Values, headers ...string) []valueHits {
		return values(t, serve(h, path, form, headers...).Body.String())
	}
	if got := hitsOf("/select/logsql/field_values", url.Values{"field": {"app"}}); !reflect.DeepEqual(got, []valueHits{{"a", 2}, {"b", 1}}) {
		t.Errorf("field values = %v", got)
	}
	if got := hitsOf("/select/logsql/stream_field_values", url.Values{"field": {"app"}}, "AccountID", "7", "ProjectID", "3"); !reflect.DeepEqual(got, []valueHits{{"x", 1}}) {
		t.Errorf("tenant 7 stream field values = %v", got)
	}
	if got := hitsOf("/select/logsql/field_values", url.Values{"field": {"missing"}}); len(got) != 0 {
		t.Errorf("a field no row has: %v", got)
	}
	if got := hitsOf("/select/logsql/streams", nil); !reflect.DeepEqual(got, []valueHits{{`{app="a"}`, 2}, {`{app="b"}`, 1}}) {
		t.Errorf("streams = %v", got)
	}
	if got := hitsOf("/select/logsql/stream_ids", nil, "AccountID", "7", "ProjectID", "3"); !reflect.DeepEqual(got, []valueHits{{`{app="x"}`, 1}}) {
		t.Errorf("tenant 7 stream ids = %v", got)
	}
	// hits: one series with a time and a count per row, none for a tenant without rows.
	var hits struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
			Values     []int    `json:"values"`
			Total      int      `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(serve(h, "/select/logsql/hits", nil).Body.Bytes(), &hits); err != nil || len(hits.Hits) != 1 || hits.Hits[0].Total != 3 || len(hits.Hits[0].Timestamps) != 3 {
		t.Errorf("hits = %+v, %v", hits, err)
	}
	if err := json.Unmarshal(serve(h, "/select/logsql/hits", nil, "AccountID", "42").Body.Bytes(), &hits); err != nil || len(hits.Hits) != 0 {
		t.Errorf("hits of a tenant without rows = %+v, %v", hits, err)
	}
	for _, c := range []struct {
		path string
		code int
		body string
	}{
		{"/health", http.StatusOK, "ok"},
		{"/metrics", http.StatusOK, ""},
		{"/select/logsql/delete", http.StatusOK, ""},
		{"/something/else", http.StatusOK, `{"values":[]}`},
	} {
		if rec := serve(h, c.path, nil); rec.Code != c.code || rec.Body.String() != c.body {
			t.Errorf("%s = %d %q", c.path, rec.Code, rec.Body)
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// A stats request is answered with one series per stream of the tenant: its labels and the row count.
func TestTenantsStats(t *testing.T) {
	tn := newTenants()
	h := tn.Handler()
	type series struct {
		Metric map[string]string `json:"metric"`
		Values [][2]any          `json:"values"`
		Value  [2]any            `json:"value"`
	}
	read := func(path string, form url.Values, headers ...string) (string, []series) {
		var body struct {
			Data struct {
				ResultType string   `json:"resultType"`
				Result     []series `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(serve(h, path, form, headers...).Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Data.ResultType, body.Data.Result
	}
	kind, out := read("/select/logsql/stats_query", url.Values{"time": {"2023-11-14T22:13:20Z"}})
	if kind != "vector" || len(out) != 2 || out[0].Metric["app"] != "a" || out[0].Value[1] != "2" || out[1].Metric["app"] != "b" || out[1].Value[1] != "1" || out[0].Metric["__name__"] != "count(*)" {
		t.Fatalf("instant: %s %+v", kind, out)
	}
	if got := out[0].Value[0]; got != float64(1700000000) {
		t.Errorf("the time of an instant answer is the request's: %v", got)
	}
	kind, out = read("/select/logsql/stats_query_range", url.Values{"start": {"1700000000000000000"}}, "AccountID", "7", "ProjectID", "3")
	if kind != "matrix" || len(out) != 1 || out[0].Metric["app"] != "x" || out[0].Values[0][1] != "1" || out[0].Values[0][0] != float64(1700000000) {
		t.Fatalf("range: %s %+v", kind, out)
	}
	if _, out = read("/select/logsql/stats_query", url.Values{"time": {"1700000123.9"}}, "AccountID", "7", "ProjectID", "3"); out[0].Value[0] != float64(1700000123) {
		t.Errorf("a time in seconds: %v", out[0].Value[0])
	}
	if _, out = read("/select/logsql/stats_query", nil, "AccountID", "42"); len(out) != 0 {
		t.Errorf("a tenant without rows has no series: %+v", out)
	}
}

// The markers of another tenant's rows found in a body are a leak.
func TestTenantsLeakedMarkers(t *testing.T) {
	tn := newTenants()
	body := `{"streams":[{"values":[["1","secret-tenant-seven"]]}]}`
	leaks := tn.LeakedMarkers("0:0", body)
	sort.Strings(leaks)
	if len(leaks) != 1 || leaks[0] != "7:3:secret-tenant-seven" {
		t.Errorf("leaks of tenant 7 into tenant 0:0 = %v", leaks)
	}
	if got := tn.LeakedMarkers("7:3", body); len(got) != 0 {
		t.Errorf("a tenant's own rows are no leak: %v", got)
	}
	if got := tn.LeakedMarkers("0:0", `{"streams":[]}`); len(got) != 0 {
		t.Errorf("a body without a marker: %v", got)
	}
}
