package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// volumeVL is a fake VictoriaLogs that records every call and answers stats
// requests through a handler; /hits (line counts) must never be used for volume.
type volumeVL struct {
	*httptest.Server
	mu      sync.Mutex
	paths   []string
	queries []string
	params  []url.Values
}

func newVolumeVL(t *testing.T, answer func(path, query string) (int, string)) *volumeVL {
	t.Helper()
	vl := &volumeVL{}
	vl.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		vl.mu.Lock()
		vl.paths = append(vl.paths, r.URL.Path)
		vl.queries = append(vl.queries, r.Form.Get("query"))
		vl.params = append(vl.params, r.Form)
		vl.mu.Unlock()
		if r.URL.Path == "/select/logsql/hits" {
			t.Errorf("volume must not use /select/logsql/hits line counts")
		}
		status, body := answer(r.URL.Path, r.Form.Get("query"))
		if status != http.StatusOK {
			http.Error(w, body, status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(vl.Close)
	return vl
}

func (vl *volumeVL) lastQuery() string {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	if len(vl.queries) == 0 {
		return ""
	}
	return vl.queries[len(vl.queries)-1]
}

type volumeTestResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
	Values [][]interface{}   `json:"values"`
}

type volumeTestResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string             `json:"resultType"`
		Result     []volumeTestResult `json:"result"`
	} `json:"data"`
}

func serveVolumeTest(t *testing.T, p *Proxy, rangeQuery bool, params url.Values) (int, volumeTestResponse) {
	t.Helper()
	path := "/loki/api/v1/index/volume"
	handler := p.handleVolume
	if rangeQuery {
		path = "/loki/api/v1/index/volume_range"
		handler = p.handleVolumeRange
	}
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil))
	var resp volumeTestResponse
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode volume response: %v body=%s", err, w.Body.String())
		}
	}
	return w.Code, resp
}

func volumeParams(kv ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return v
}

func TestVolume_InstantReportsLineBytesAtRequestEnd(t *testing.T) {
	vl := newVolumeVL(t, func(path, _ string) (int, string) {
		if path != "/select/logsql/stats_query" {
			return http.StatusNotFound, "unexpected path " + path
		}
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","app":"b"},"value":[1789451580,"5040"]},` +
			`{"metric":{"__name__":"_b","app":"a"},"value":[1789451580,"11880"]}]}}`
	})
	p := newTestProxy(t, vl.URL)

	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app=~"a|b"}`, "start", "1789450500", "end", "1789451580.5"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	q := vl.lastQuery()
	for _, want := range []string{"sum_len(_msg) as _b", "stats by (app)", "sort by (_b desc, app) limit 100"} {
		if !strings.Contains(q, want) {
			t.Fatalf("stats query %q missing %q", q, want)
		}
	}
	if resp.Data.ResultType != "vector" || len(resp.Data.Result) != 2 {
		t.Fatalf("unexpected response %+v", resp)
	}
	want := []volumeTestResult{
		{Metric: map[string]string{"app": "a"}, Value: []interface{}{1789451580.5, "11880"}},
		{Metric: map[string]string{"app": "b"}, Value: []interface{}{1789451580.5, "5040"}},
	}
	if !reflect.DeepEqual(resp.Data.Result, want) {
		t.Fatalf("got %+v, want %+v (bytes, ordered by volume, stamped at end)", resp.Data.Result, want)
	}
}

func TestVolume_RangeStampsBucketEndsLikeLoki(t *testing.T) {
	vl := newVolumeVL(t, func(path, _ string) (int, string) {
		if path != "/select/logsql/stats_query_range" {
			return http.StatusNotFound, "unexpected path " + path
		}
		return http.StatusOK, `{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"__name__":"_b","app":"a"},"values":[[1789450500,"3000"],[1789450800,"3300"],[1789451100,"3300"],[1789451400,"1900"]]}]}}`
	})
	p := newTestProxy(t, vl.URL)

	code, resp := serveVolumeTest(t, p, true, volumeParams("query", `{app="a"}`, "start", "1789450535", "end", "1789451575", "step", "300"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	q := vl.lastQuery()
	if !strings.Contains(q, "| filter _time:[2026-09-15T05:35:35Z, 2026-09-15T05:52:55Z)") {
		t.Fatalf("stats query %q must bound partial edge buckets to the requested range", q)
	}
	want := [][]interface{}{
		{1789450799.999, "3000"},
		{1789451099.999, "3300"},
		{1789451399.999, "3300"},
		{1789451575.0, "1900"},
	}
	if resp.Data.ResultType != "matrix" || len(resp.Data.Result) != 1 || !reflect.DeepEqual(resp.Data.Result[0].Values, want) {
		t.Fatalf("got %+v, want values %v", resp, want)
	}
}

// conformance: loki_api_v1_index_volume, drilldown/index_volume/single-sample-returns-vector
func TestVolume_RangeWithOneSamplePerSeriesIsVector(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"__name__":"_b","app":"a"},"values":[[1789450000,"42"]]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, true, volumeParams("query", `{app="a"}`, "start", "1789450500", "end", "1789451580", "step", "2000"))
	if code != http.StatusOK || resp.Data.ResultType != "vector" || len(resp.Data.Result) != 1 {
		t.Fatalf("Loki answers a single-sample volume_range as a vector, got %d %+v", code, resp)
	}
	if got := resp.Data.Result[0].Value; !reflect.DeepEqual(got, []interface{}{1789451580.0, "42"}) {
		t.Fatalf("value %v, want [end, bytes]", got)
	}
}

func TestVolume_SeriesNamedBySelectorLabels(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","app":"a","pod":"p1"},"value":[1,"10"]},` +
			`{"metric":{"__name__":"_b","app":"b","pod":""},"value":[1,"7"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app=~"a|b", pod!="x"}`, "start", "1", "end", "2"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); !strings.Contains(q, "stats by (app, pod)") || strings.Contains(q, "filter pod:*") {
		t.Fatalf("selector labels must group without requiring presence, got %q", q)
	}
	want := []map[string]string{{"app": "a", "pod": "p1"}, {"app": "b"}}
	for i, r := range resp.Data.Result {
		if !reflect.DeepEqual(r.Metric, want[i]) {
			t.Fatalf("result %d metric %v, want %v", i, r.Metric, want[i])
		}
	}
}

// conformance: loki_api_v1_index_volume, status-400
func TestVolume_TargetLabelsAreRequired(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","pod":"p3","team":"t1"},"value":[1,"3600"]},` +
			`{"metric":{"__name__":"_b","pod":"p1","team":""},"value":[1,"9000"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app=~"vol.*"}`, "start", "1", "end", "2", "targetLabels", "pod,team"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); !strings.Contains(q, "| filter pod:* team:* | stats by (pod, team)") {
		t.Fatalf("target labels must be pushed down as presence filters, got %q", q)
	}
	if len(resp.Data.Result) != 1 || !reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"pod": "p3", "team": "t1"}) {
		t.Fatalf("streams without every target label must be skipped, got %+v", resp.Data.Result)
	}
}

func TestVolume_AggregateByLabelsSumsPerLabelName(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\",pod=\"p1\"}"},"value":[1,"100"]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"b\"}"},"value":[1,"40"]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"c\",pod=\"p3\",team=\"t1\"}"},"value":[1,"10"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app=~".+"}`, "start", "1", "end", "2", "aggregateBy", "labels"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); !strings.Contains(q, "stats by (_stream)") || strings.Contains(q, "limit") {
		t.Fatalf("aggregateBy=labels without targets groups by stream without pushdown limit, got %q", q)
	}
	got := map[string]string{}
	var order []string
	for _, r := range resp.Data.Result {
		if len(r.Metric) != 1 {
			t.Fatalf("labels aggregation names a result by one label, got %v", r.Metric)
		}
		for name, value := range r.Metric {
			if value != "" {
				t.Fatalf("labels aggregation metric value must be empty, got %v", r.Metric)
			}
			got[name] = r.Value[1].(string)
			order = append(order, name)
		}
	}
	want := map[string]string{"app": "150", "service_name": "150", "pod": "110", "team": "10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if !reflect.DeepEqual(order, []string{"app", "service_name", "pod", "team"}) {
		t.Fatalf("order %v: volume desc, name asc", order)
	}

	code, resp = serveVolumeTest(t, p, false, volumeParams("query", `{app=~".+"}`, "start", "1", "end", "3", "aggregateBy", "labels", "targetLabels", "pod"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(resp.Data.Result) != 1 || !reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"pod": ""}) || resp.Data.Result[0].Value[1] != "110" {
		t.Fatalf("labels aggregation with targetLabels=pod, got %+v (query %q)", resp.Data.Result, vl.lastQuery())
	}
}

func TestVolume_LimitKeepsTopVolumesPerBucket(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\"}"},"values":[[1789450500,"50"],[1789450800,"10"]]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"b\"}"},"values":[[1789450500,"50"],[1789450800,"30"]]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"c\"}"},"values":[[1789450500,"20"],[1789450800,"40"]]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, true, volumeParams("query", `{app=~".+"}`, "start", "1789450500", "end", "1789451100", "step", "300", "limit", "1", "aggregateBy", "labels"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	// app and the synthetic service_name tie on every bucket; app wins by name.
	if len(resp.Data.Result) != 1 || !reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"app": ""}) ||
		!reflect.DeepEqual(resp.Data.Result[0].Values, [][]interface{}{{1789450799.999, "120"}, {1789451100.0, "80"}}) {
		t.Fatalf("limit=1 keeps one label per bucket, got %+v", resp.Data.Result)
	}

	vl2 := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"__name__":"_b","app":"a"},"values":[[1789450500,"50"],[1789450800,"10"]]},` +
			`{"metric":{"__name__":"_b","app":"b"},"values":[[1789450500,"50"],[1789450800,"30"]]},` +
			`{"metric":{"__name__":"_b","app":"c"},"values":[[1789450500,"20"],[1789450800,"40"]]}]}}`
	})
	p2 := newTestProxy(t, vl2.URL)
	code, resp = serveVolumeTest(t, p2, true, volumeParams("query", `{app=~".+"}`, "start", "1789450500", "end", "1789451100", "step", "300", "limit", "1"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl2.lastQuery(); !strings.Contains(q, "sort by (_b desc, app) limit 1") {
		t.Fatalf("series limit must be pushed down per bucket, got %q", q)
	}
	// Bucket 1: a and b tie at 50, a wins by name. Bucket 2: c leads.
	if len(resp.Data.Result) != 2 ||
		!reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"app": "a"}) ||
		!reflect.DeepEqual(resp.Data.Result[1].Metric, map[string]string{"app": "c"}) {
		t.Fatalf("per-bucket top-1 with name tie-break, got %+v", resp.Data.Result)
	}
	// Every kept series has one sample, so Loki answers with a vector.
	if resp.Data.ResultType != "vector" ||
		!reflect.DeepEqual(resp.Data.Result[0].Value, []interface{}{1789450799.999, "50"}) ||
		!reflect.DeepEqual(resp.Data.Result[1].Value, []interface{}{1789451100.0, "40"}) {
		t.Fatalf("unexpected values %+v", resp.Data.Result)
	}
}

func TestVolume_DerivedServiceNameMergesSources(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","app":"api","service.name":""},"value":[1,"70"]},` +
			`{"metric":{"__name__":"_b","app":"","service.name":"api"},"value":[1,"30"]},` +
			`{"metric":{"__name__":"_b","app":"","service.name":""},"value":[1,"9"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", "{service_name=~`.+`}", "start", "1", "end", "2", "targetLabels", "service_name"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); strings.Contains(q, "limit") || !strings.Contains(q, "sum_len(_msg)") {
		t.Fatalf("derived service_name rows merge in the proxy, so no pushdown limit: %q", q)
	}
	// Loki names streams without a service label unknown_service.
	if len(resp.Data.Result) != 2 ||
		!reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"service_name": "api"}) || resp.Data.Result[0].Value[1] != "100" ||
		!reflect.DeepEqual(resp.Data.Result[1].Metric, map[string]string{"service_name": "unknown_service"}) || resp.Data.Result[1].Value[1] != "9" {
		t.Fatalf("got %+v, want api with 100 bytes then unknown_service with 9 and no empty service bucket", resp.Data.Result)
	}
}

func TestVolume_RejectsInvalidParametersLikeLoki(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	for name, params := range map[string]url.Values{
		"aggregateBy":  volumeParams("query", `{app="a"}`, "aggregateBy", "streams"),
		"limit_neg":    volumeParams("query", `{app="a"}`, "limit", "-1"),
		"limit_nonint": volumeParams("query", `{app="a"}`, "limit", "abc"),
		"end_before":   volumeParams("query", `{app="a"}`, "start", "20", "end", "10"),
	} {
		t.Run(name, func(t *testing.T) {
			if code, _ := serveVolumeTest(t, p, false, params); code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", code)
			}
		})
	}
}

func TestVolume_OptimisedRewriteRejectionRetriesPlainStreamStats(t *testing.T) {
	vl := newVolumeVL(t, func(_ string, query string) (int, string) {
		if strings.Contains(query, "filter pod:*") {
			return http.StatusBadRequest, "cannot parse `query` arg: unexpected token"
		}
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\",pod=\"p1\"}"},"value":[1,"10"]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\"}"},"value":[1,"5"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app="a"}`, "start", "1", "end", "2", "targetLabels", "pod"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); !strings.HasSuffix(q, "| stats by (_stream) sum_len(_msg) as _b") {
		t.Fatalf("fallback must be the plain stream stats query, got %q", q)
	}
	if len(resp.Data.Result) != 1 || !reflect.DeepEqual(resp.Data.Result[0].Metric, map[string]string{"pod": "p1"}) || resp.Data.Result[0].Value[1] != "10" {
		t.Fatalf("got %+v", resp.Data.Result)
	}
}

func TestVolumeSelectorLabelNames(t *testing.T) {
	cases := map[string][]string{
		`{app="a", pod=~"p.+", team!="x!=y"}`: {"app", "pod", "team"},
		`{app="a,b"} |= "x"`:                  {"app"},
		`*`:                                   nil,
		`{}`:                                  nil,
	}
	for query, want := range cases {
		if got := volumeSelectorLabelNames(query); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %v, want %v", query, got, want)
		}
	}
}

func TestLokiVolumeSeriesName(t *testing.T) {
	got := lokiVolumeSeriesName(map[string]string{"pod": "p\"1", "app": "a"})
	if want := `{app="a", pod="p\"1"}`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// Loki names volumes differently with and without targetLabels, and per
// aggregateBy, so those requests must not share a cache entry.
func TestVolume_CacheKeyKeepsNamingParameters(t *testing.T) {
	calls := 0
	vl := newVolumeVL(t, func(_ string, query string) (int, string) {
		calls++
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\",pod=\"p\"}","app":"a","pod":"p"},"value":[1,"10"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	query := `{app="a", pod="p"}`
	variants := []url.Values{
		volumeParams("query", query, "start", "1", "end", "2"),
		volumeParams("query", query, "start", "1", "end", "2", "targetLabels", "app"),
		volumeParams("query", query, "start", "1", "end", "2", "aggregateBy", "labels"),
	}
	metrics := make([]map[string]string, 0, len(variants))
	for _, params := range variants {
		code, resp := serveVolumeTest(t, p, false, params)
		if code != http.StatusOK || len(resp.Data.Result) == 0 {
			t.Fatalf("status %d %+v", code, resp)
		}
		metrics = append(metrics, resp.Data.Result[0].Metric)
	}
	if calls != len(variants) {
		t.Fatalf("each naming variant must reach the backend, got %d calls for %d variants", calls, len(variants))
	}
	if reflect.DeepEqual(metrics[0], metrics[1]) || reflect.DeepEqual(metrics[1], metrics[2]) {
		t.Fatalf("variants must be named differently, got %v", metrics)
	}
}

// A synthetic fan-out label is filled in after the backend answers, so rows
// merge into fewer volumes and the limit must not be pushed to VictoriaLogs.
func TestVolume_SyntheticTargetLabelDoesNotPushLimit(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"a\"}"},"value":[1,"10"]},` +
			`{"metric":{"__name__":"_b","_stream":"{app=\"b\"}"},"value":[1,"5"]}]}}`
	})
	p := newTestProxy(t, vl.URL)
	code, resp := serveVolumeTest(t, p, false, volumeParams("query", `{app=~".+"}`, "start", "1", "end", "2", "targetLabels", "__tenant_id__", "limit", "1"))
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if q := vl.lastQuery(); strings.Contains(q, "limit") {
		t.Fatalf("limit must stay in the proxy for synthetic labels, got %q", q)
	}
	if len(resp.Data.Result) != 1 || resp.Data.Result[0].Value[1] != "15" {
		t.Fatalf("every stream must count toward the synthetic volume, got %+v", resp.Data.Result)
	}
}

// Volumes used to be cached as line counts; those entries must not be served.
func TestVolume_CacheKeysAreVersionedForBytes(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, endpoint := range []string{"volume", "volume_range"} {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/"+endpoint+"?query=%7Bapp%3D%22a%22%7D&start=1&end=2&step=60", nil)
		if key := p.canonicalReadCacheKey(endpoint, "0", r); !strings.Contains(key, ":"+volumeCacheKeyVersion+":") {
			t.Fatalf("%s cache key %q must carry %s", endpoint, key, volumeCacheKeyVersion)
		}
	}
}

func TestVolume_LevelUnpackKeepsLineAndTime(t *testing.T) {
	vl := newVolumeVL(t, func(string, string) (int, string) {
		return http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[]}}`
	})
	p := newTestProxy(t, vl.URL)
	if code, _ := serveVolumeTest(t, p, false, volumeParams("query", `{app="a"}`, "start", "1", "end", "2", "targetLabels", "detected_level")); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	q := vl.lastQuery()
	if !strings.Contains(q, "unpack_json from _msg fields (level, detected_level) keep_original_fields") ||
		!strings.Contains(q, "unpack_logfmt from _msg fields (level, detected_level) keep_original_fields") {
		t.Fatalf("level unpack must be limited to level fields, got %q", q)
	}
}
