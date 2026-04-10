package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func recorderWithJSON(body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "application/json")
	rec.WriteString(body)
	return rec
}

func TestEmptyMultiTenantResponseShapes(t *testing.T) {
	cases := []string{
		"labels",
		"label_values",
		"series",
		"query",
		"query_range",
		"index_stats",
		"volume",
		"volume_range",
		"detected_fields",
		"detected_field_values",
		"detected_labels",
		"patterns",
		"unknown",
	}
	for _, endpoint := range cases {
		resp := emptyMultiTenantResponse(endpoint)
		if _, ok := resp["status"]; endpoint != "index_stats" && endpoint != "unknown" && !ok {
			t.Fatalf("expected status key for %s", endpoint)
		}
	}
}

func TestMergeIndexStatsResponses(t *testing.T) {
	body, contentType, err := mergeIndexStatsResponses([]*httptest.ResponseRecorder{
		recorderWithJSON(`{"streams":2,"chunks":3,"bytes":4,"entries":5}`),
		recorderWithJSON(`{"streams":1,"chunks":2,"bytes":3,"entries":4}`),
	})
	if err != nil {
		t.Fatalf("mergeIndexStatsResponses failed: %v", err)
	}
	if contentType != "application/json" {
		t.Fatalf("unexpected content type %q", contentType)
	}
	var got map[string]int
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode merged stats: %v", err)
	}
	if got["streams"] != 3 || got["chunks"] != 5 || got["bytes"] != 7 || got["entries"] != 9 {
		t.Fatalf("unexpected merged stats: %#v", got)
	}
}

func TestMergeDetectedFieldsResponses(t *testing.T) {
	body, _, err := mergeDetectedFieldsResponses([]*httptest.ResponseRecorder{
		recorderWithJSON(`{"fields":[{"label":"status","type":"number","cardinality":2,"parsers":["json"]}]}`),
		recorderWithJSON(`{"fields":[{"label":"status","type":"number","cardinality":1,"parsers":["logfmt"]},{"label":"method","type":"string","cardinality":3,"parsers":["json"]}]}`),
	})
	if err != nil {
		t.Fatalf("mergeDetectedFieldsResponses failed: %v", err)
	}
	var resp struct {
		Fields []struct {
			Label       string   `json:"label"`
			Cardinality int      `json:"cardinality"`
			Parsers     []string `json:"parsers"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(resp.Fields))
	}
	if resp.Fields[1].Label != "status" || resp.Fields[1].Cardinality != 3 {
		t.Fatalf("unexpected merged status field: %#v", resp.Fields[1])
	}
}

func TestMergeDetectedFieldValuesResponses(t *testing.T) {
	body, _, err := mergeDetectedFieldValuesResponses([]*httptest.ResponseRecorder{
		recorderWithJSON(`{"values":["warn","info"]}`),
		recorderWithJSON(`{"values":["error","info"]}`),
	})
	if err != nil {
		t.Fatalf("mergeDetectedFieldValuesResponses failed: %v", err)
	}
	var resp struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []string{"error", "info", "warn"}
	for i, v := range want {
		if resp.Values[i] != v {
			t.Fatalf("unexpected merged values: %#v", resp.Values)
		}
	}
}

func TestMergeDetectedLabelsResponses(t *testing.T) {
	body, _, err := mergeDetectedLabelsResponses([]string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"detectedLabels":[{"label":"cluster","cardinality":1},{"label":"service_name","cardinality":1}]}`),
		recorderWithJSON(`{"detectedLabels":[{"label":"cluster","cardinality":2}]}`),
	})
	if err != nil {
		t.Fatalf("mergeDetectedLabelsResponses failed: %v", err)
	}
	var resp struct {
		DetectedLabels []struct {
			Label       string `json:"label"`
			Cardinality int    `json:"cardinality"`
		} `json:"detectedLabels"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.DetectedLabels[0].Label != "service_name" {
		t.Fatalf("expected service_name to be first, got %#v", resp.DetectedLabels)
	}
}

func TestMergePatternsResponses(t *testing.T) {
	body, _, err := mergePatternsResponses([]*httptest.ResponseRecorder{
		recorderWithJSON(`{"data":[{"pattern":"{status=<_>}","samples":[[1,2]],"level":"info"}]}`),
		recorderWithJSON(`{"data":[{"pattern":"{status=<_>}","samples":[[1,3],[2,1]],"level":"info"}]}`),
	})
	if err != nil {
		t.Fatalf("mergePatternsResponses failed: %v", err)
	}
	var resp struct {
		Data []struct {
			Pattern string          `json:"pattern"`
			Samples [][]interface{} `json:"samples"`
			Level   string          `json:"level"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 1 || len(resp.Data[0].Samples) != 2 {
		t.Fatalf("unexpected merged patterns: %#v", resp.Data)
	}
}

func TestMergePatternsResponses_SkipsInvalidSamples(t *testing.T) {
	body, _, err := mergePatternsResponses([]*httptest.ResponseRecorder{
		recorderWithJSON(`{"data":[{"pattern":"p","samples":[["bad",2],[1,"bad"],[2,3]],"level":"warn"}]}`),
	})
	if err != nil {
		t.Fatalf("mergePatternsResponses failed: %v", err)
	}
	var resp struct {
		Data []struct {
			Pattern string          `json:"pattern"`
			Samples [][]interface{} `json:"samples"`
			Level   string          `json:"level"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 1 || len(resp.Data[0].Samples) != 1 {
		t.Fatalf("expected only valid samples to remain, got %#v", resp.Data)
	}
}

func TestNumberConversions(t *testing.T) {
	if got, ok := numberToInt64(float64(12)); !ok || got != 12 {
		t.Fatalf("float64 to int64 failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt64(int64(13)); !ok || got != 13 {
		t.Fatalf("int64 passthrough failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt64(int(14)); !ok || got != 14 {
		t.Fatalf("int to int64 failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt64(json.Number("15")); !ok || got != 15 {
		t.Fatalf("json.Number to int64 failed: got=%d ok=%v", got, ok)
	}
	if _, ok := numberToInt64("nope"); ok {
		t.Fatal("expected invalid int64 conversion to fail")
	}

	if got, ok := numberToInt(float64(21)); !ok || got != 21 {
		t.Fatalf("float64 to int failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt(int(22)); !ok || got != 22 {
		t.Fatalf("int passthrough failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt(int64(23)); !ok || got != 23 {
		t.Fatalf("int64 to int failed: got=%d ok=%v", got, ok)
	}
	if got, ok := numberToInt(json.Number("24")); !ok || got != 24 {
		t.Fatalf("json.Number to int failed: got=%d ok=%v", got, ok)
	}
	if _, ok := numberToInt("nope"); ok {
		t.Fatal("expected invalid int conversion to fail")
	}
}

func TestMergeMultiTenantResponsesForSeries(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("series", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":[{"app":"api"}]}`),
		recorderWithJSON(`{"status":"success","data":[{"app":"api"}]}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected separate series per tenant, got %#v", resp.Data)
	}
}

func TestMergeMultiTenantResponsesUnsupportedEndpoint(t *testing.T) {
	if _, _, err := mergeMultiTenantResponses("nope", nil, nil); err == nil {
		t.Fatal("expected unsupported endpoint error")
	}
}

func TestApplyTenantSelectorFilter_QueryPath(t *testing.T) {
	p, _ := New(Config{BackendURL: "http://example.com", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query_range?query={app="api",__tenant_id__=~"tenant-(a|c)"}&start=1&end=2`, nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b|tenant-c")

	filteredReq, filteredTenants, err := p.applyTenantSelectorFilter(req, []string{"tenant-a", "tenant-b", "tenant-c"})
	if err != nil {
		t.Fatalf("applyTenantSelectorFilter failed: %v", err)
	}
	if got := filteredReq.URL.Query().Get("query"); got != `{app="api"}` {
		t.Fatalf("unexpected rewritten query: %q", got)
	}
	if got := filteredReq.Header.Get("X-Scope-OrgID"); got != "tenant-a|tenant-c" {
		t.Fatalf("unexpected narrowed tenant header: %q", got)
	}
	if len(filteredTenants) != 2 || filteredTenants[0] != "tenant-a" || filteredTenants[1] != "tenant-c" {
		t.Fatalf("unexpected narrowed tenants: %#v", filteredTenants)
	}
}

func TestApplyTenantSelectorFilter_MatchPath(t *testing.T) {
	p, _ := New(Config{BackendURL: "http://example.com", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/series?match[]={app="api",__tenant_id__!="tenant-b"}&match[]={namespace="prod",__tenant_id__!~"tenant-c"}&start=1&end=2`, nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b|tenant-c")

	filteredReq, filteredTenants, err := p.applyTenantSelectorFilter(req, []string{"tenant-a", "tenant-b", "tenant-c"})
	if err != nil {
		t.Fatalf("applyTenantSelectorFilter failed: %v", err)
	}
	matchers := filteredReq.URL.Query()["match[]"]
	if len(matchers) != 2 {
		t.Fatalf("expected 2 matchers, got %#v", matchers)
	}
	if matchers[0] != `{app="api"}` || matchers[1] != `{namespace="prod"}` {
		t.Fatalf("unexpected rewritten matchers: %#v", matchers)
	}
	if got := filteredReq.Header.Get("X-Scope-OrgID"); got != "tenant-a" {
		t.Fatalf("unexpected narrowed tenant header: %q", got)
	}
	if len(filteredTenants) != 1 || filteredTenants[0] != "tenant-a" {
		t.Fatalf("unexpected narrowed tenants: %#v", filteredTenants)
	}
}

func TestApplyTenantSelectorFilter_InvalidRegex(t *testing.T) {
	p, _ := New(Config{BackendURL: "http://example.com", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query_range?query={__tenant_id__=~"("}`, nil)

	if _, _, err := p.applyTenantSelectorFilter(req, []string{"tenant-a"}); err == nil {
		t.Fatal("expected invalid tenant regex error")
	}
}

func TestFilterTenantMatchers_PreservesPipelineWhenSelectorEmpties(t *testing.T) {
	query, tenants, err := filterTenantMatchers(`{__tenant_id__="tenant-a"} |= "error" | json`, []string{"tenant-a", "tenant-b"})
	if err != nil {
		t.Fatalf("filterTenantMatchers failed: %v", err)
	}
	if query != `* |= "error" | json` {
		t.Fatalf("unexpected rebuilt query: %q", query)
	}
	if len(tenants) != 1 || tenants[0] != "tenant-a" {
		t.Fatalf("unexpected filtered tenants: %#v", tenants)
	}
}

func TestApplyTenantMatchVariants(t *testing.T) {
	tenants := []string{"tenant-a", "tenant-b", "prod"}
	cases := []struct {
		name string
		op   string
		raw  string
		want []string
	}{
		{name: "equal", op: "=", raw: "tenant-b", want: []string{"tenant-b"}},
		{name: "not-equal", op: "!=", raw: "tenant-b", want: []string{"tenant-a", "prod"}},
		{name: "regex", op: "=~", raw: "tenant-.*", want: []string{"tenant-a", "tenant-b"}},
		{name: "not-regex", op: "!~", raw: "tenant-.*", want: []string{"prod"}},
	}
	for _, tc := range cases {
		got, handled, err := applyTenantMatch(tenants, tc.op, tc.raw)
		if err != nil {
			t.Fatalf("%s: applyTenantMatch failed: %v", tc.name, err)
		}
		if !handled {
			t.Fatalf("%s: expected matcher to be handled", tc.name)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s: unexpected tenants %#v", tc.name, got)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: unexpected tenants %#v", tc.name, got)
			}
		}
	}
	if _, _, err := applyTenantMatch(tenants, "=~", "("); err == nil {
		t.Fatal("expected regex compile error")
	}
}

func TestMergeMultiTenantResponsesLabelsAddsTenantLabel(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("labels", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":["cluster","app"]}`),
		recorderWithJSON(`{"status":"success","data":["app","namespace"]}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []string{"__tenant_id__", "app", "cluster", "namespace"}
	if len(resp.Data) != len(want) {
		t.Fatalf("unexpected merged labels: %#v", resp.Data)
	}
	for i, value := range want {
		if resp.Data[i] != value {
			t.Fatalf("unexpected merged labels: %#v", resp.Data)
		}
	}
}

func TestMergeMultiTenantResponsesLabelValues(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("label_values", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":["api","web"]}`),
		recorderWithJSON(`{"status":"success","data":["api","worker"]}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []string{"api", "web", "worker"}
	for i, value := range want {
		if resp.Data[i] != value {
			t.Fatalf("unexpected merged label values: %#v", resp.Data)
		}
	}
}

func TestMergeMultiTenantResponsesQueryInjectsTenantLabel(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("query", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[["2","line-a"]]}]}}`),
		recorderWithJSON(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api","__tenant_id__":"backend"},"values":[["1","line-b"]]}]}}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected 2 result streams, got %#v", resp.Data.Result)
	}
	if resp.Data.Result[0].Stream["__tenant_id__"] == "" || resp.Data.Result[1].Stream["__tenant_id__"] == "" {
		t.Fatalf("expected synthetic tenant ids in merged streams: %#v", resp.Data.Result)
	}
	if resp.Data.Result[1].Stream["original___tenant_id__"] != "backend" {
		t.Fatalf("expected original tenant id to be preserved: %#v", resp.Data.Result[1].Stream)
	}
}

func TestMergeMultiTenantResponsesQueryPreservesThreeTupleValues(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("query", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[["2","line-a",{"structuredMetadata":{"service.name":"orders"}}]]}]}}`),
		recorderWithJSON(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[["1","line-b",{"parsed":{"status":"200"}}]]}]}}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected 2 merged streams, got %#v", resp.Data.Result)
	}
	for _, item := range resp.Data.Result {
		if len(item.Values) == 0 || len(item.Values[0]) != 3 {
			t.Fatalf("expected merged stream values to preserve 3-tuple shape, got %#v", item.Values)
		}
	}
}

func TestMergeMultiTenantResponsesQueryVectorInjectsTenantLabel(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("query", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[1,"2"]}],"stats":{"summary":"ok"}}}`),
		recorderWithJSON(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api","__tenant_id__":"backend"},"value":[1,"3"]}]}}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
			Stats map[string]string `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.ResultType != "vector" || len(resp.Data.Result) != 2 {
		t.Fatalf("unexpected merged vector response: %#v", resp.Data)
	}
	if resp.Data.Result[0].Metric["__tenant_id__"] == "" || resp.Data.Result[1].Metric["__tenant_id__"] == "" {
		t.Fatalf("expected synthetic tenant labels in vector results: %#v", resp.Data.Result)
	}
	if resp.Data.Result[1].Metric["original___tenant_id__"] != "backend" {
		t.Fatalf("expected original tenant label to be preserved: %#v", resp.Data.Result[1].Metric)
	}
	if resp.Data.Stats["summary"] != "ok" {
		t.Fatalf("expected stats payload to survive merge, got %#v", resp.Data.Stats)
	}
}

func TestMergeMultiTenantResponsesQueryMatrixInjectsTenantLabel(t *testing.T) {
	body, _, err := mergeMultiTenantResponses("query_range", []string{"tenant-a", "tenant-b"}, []*httptest.ResponseRecorder{
		recorderWithJSON(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[1,"2"]]}]}}`),
		recorderWithJSON(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api","__tenant_id__":"backend"},"values":[[1,"3"]]}]}}`),
	})
	if err != nil {
		t.Fatalf("mergeMultiTenantResponses failed: %v", err)
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.ResultType != "matrix" || len(resp.Data.Result) != 2 {
		t.Fatalf("unexpected merged matrix response: %#v", resp.Data)
	}
	if resp.Data.Result[0].Metric["__tenant_id__"] == "" || resp.Data.Result[1].Metric["__tenant_id__"] == "" {
		t.Fatalf("expected synthetic tenant labels in matrix results: %#v", resp.Data.Result)
	}
}

func TestLatestStreamTimestampStrings(t *testing.T) {
	if got := latestStreamTimestampStrings(nil); got != "" {
		t.Fatalf("expected empty timestamp for nil input, got %q", got)
	}
	if got := latestStreamTimestampStrings([][]interface{}{{}}); got != "" {
		t.Fatalf("expected empty timestamp for empty pair, got %q", got)
	}
	if got := latestStreamTimestampStrings([][]interface{}{{"42", "line"}}); got != "42" {
		t.Fatalf("expected first timestamp, got %q", got)
	}
}

func TestMultiTenantHelpers(t *testing.T) {
	if !isMultiTenantQueryPath("/loki/api/v1/query_range") {
		t.Fatal("expected query_range to allow multi-tenant fanout")
	}
	if !isMultiTenantQueryPath("/loki/api/v1/labels") {
		t.Fatal("expected labels to allow multi-tenant fanout")
	}
	if isMultiTenantQueryPath("/loki/api/v1/tail") {
		t.Fatal("expected tail to reject multi-tenant fanout")
	}

	p, _ := New(Config{BackendURL: "http://example.com", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	getReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?start=1&end=2", nil)
	getReq.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	if key, ok := p.multiTenantCacheKey(getReq, "labels"); !ok || key == "" {
		t.Fatalf("expected cache key for multi-tenant GET labels, got %q %v", key, ok)
	}
	postReq := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range?query=*", nil)
	postReq.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	if key, ok := p.multiTenantCacheKey(postReq, "query_range"); ok || key != "" {
		t.Fatalf("expected POST query_range to skip cache key, got %q %v", key, ok)
	}
}

func TestInjectTenantLabelPreservesOriginal(t *testing.T) {
	labels := injectTenantLabel(map[string]string{"__tenant_id__": "backend", "app": "api"}, "tenant-a")
	if labels["__tenant_id__"] != "tenant-a" {
		t.Fatalf("unexpected injected tenant id: %#v", labels)
	}
	if labels["original___tenant_id__"] != "backend" {
		t.Fatalf("expected original tenant id to be preserved: %#v", labels)
	}
}

func TestInterfaceStringMapVariants(t *testing.T) {
	direct := interfaceStringMap(map[string]string{"app": "api"})
	if direct["app"] != "api" {
		t.Fatalf("unexpected direct string map conversion: %#v", direct)
	}
	got := interfaceStringMap(map[string]interface{}{"app": "api", "count": 3})
	if got["app"] != "api" || got["count"] != "3" {
		t.Fatalf("unexpected converted map: %#v", got)
	}
	if result := interfaceStringMap("nope"); len(result) != 0 {
		t.Fatalf("expected empty map for unsupported type, got %#v", result)
	}
}

func TestIsMultiTenantQueryPathCoverage(t *testing.T) {
	allowed := []string{
		"/loki/api/v1/query",
		"/loki/api/v1/query_range",
		"/loki/api/v1/series",
		"/loki/api/v1/labels",
		"/loki/api/v1/label/app/values",
		"/loki/api/v1/index/stats",
		"/loki/api/v1/index/volume",
		"/loki/api/v1/index/volume_range",
		"/loki/api/v1/detected_fields",
		"/loki/api/v1/detected_field/level/values",
		"/loki/api/v1/detected_labels",
		"/loki/api/v1/patterns",
	}
	for _, path := range allowed {
		if !isMultiTenantQueryPath(path) {
			t.Fatalf("expected path to allow multi-tenant fanout: %s", path)
		}
	}
	if isMultiTenantQueryPath("/loki/api/v1/tail") {
		t.Fatal("expected tail path to reject multi-tenant fanout")
	}
}
