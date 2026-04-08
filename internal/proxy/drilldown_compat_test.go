package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestDrilldown_QueryRange_ServiceNameSelectorAndSyntheticLabel(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		receivedQuery = r.Form.Get("query")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"status\":200}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T17:18:50.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users/999\",\"status\":404}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"error"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	params := url.Values{}
	params.Set("query", `{service_name="api-gateway"}`)
	params.Set("start", "1")
	params.Set("end", "2")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	p.handleQueryRange(w, r)

	if !strings.Contains(receivedQuery, "app:=api-gateway") {
		t.Fatalf("expected synthetic service_name selector to include app matcher, got %q", receivedQuery)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 2 {
		t.Fatalf("expected level-aware stream split, got %v", result)
	}
	levels := map[string]map[string]interface{}{}
	for _, item := range result {
		stream := item.(map[string]interface{})["stream"].(map[string]interface{})
		levels[stream["level"].(string)] = stream
	}
	if levels["info"]["service_name"] != "api-gateway" {
		t.Fatalf("expected synthetic service_name label in response, got %v", levels)
	}
	if levels["error"]["detected_level"] != "error" {
		t.Fatalf("expected detected_level to follow effective stream level, got %v", levels["error"])
	}
}

func TestDrilldown_QueryRange_ParsedFieldsStayOutOfStreamLabels(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		receivedQuery = r.Form.Get("query")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"status\":200}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info","method":"GET","path":"/api/v1/users","status":"200"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	params := url.Values{}
	params.Set("query", `{service_name="api-gateway"} | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	p.handleQueryRange(w, r)

	if strings.Contains(receivedQuery, "unpack_logfmt") {
		t.Fatalf("expected proxy to drop non-working logfmt parser for JSON drilldown query, got %q", receivedQuery)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected a single logical stream, got %v", result)
	}

	entry := result[0].(map[string]interface{})
	stream := entry["stream"].(map[string]interface{})
	if _, ok := stream["method"]; ok {
		t.Fatalf("parsed method must not be emitted as a stream label: %v", stream)
	}
	if _, ok := stream["path"]; ok {
		t.Fatalf("parsed path must not be emitted as a stream label: %v", stream)
	}
	if stream["service_name"] != "api-gateway" {
		t.Fatalf("expected synthetic service_name label, got %v", stream)
	}

	values := entry["values"].([]interface{})
	if len(values) != 1 {
		t.Fatalf("expected one log value, got %v", values)
	}
	pair := values[0].([]interface{})
	if len(pair) != 2 {
		t.Fatalf("expected canonical 2-tuple Loki values for Grafana compatibility, got %v", pair)
	}
}

func TestDrilldown_QueryRange_RawVLFieldsDoNotPolluteStreamLabels(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"request completed","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info","trace_id":"abc123","user_id":"usr-42"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	params := url.Values{}
	params.Set("query", `{service_name="api-gateway"}`)
	params.Set("start", "1")
	params.Set("end", "2")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil)
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected a single stream, got %v", result)
	}

	entry := result[0].(map[string]interface{})
	stream := entry["stream"].(map[string]interface{})
	if _, ok := stream["trace_id"]; ok {
		t.Fatalf("raw VL metadata must not be emitted as stream labels: %v", stream)
	}

	values := entry["values"].([]interface{})
	pair := values[0].([]interface{})
	if len(pair) != 2 {
		t.Fatalf("expected canonical 2-tuple Loki values for Grafana compatibility, got %v", pair)
	}
}

func TestDrilldown_DetectLabels_SupplementsLevelFromScannedLogs(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": `{service.name="otel-auth-service",service.namespace="prod",k8s.pod.name="otel-auth-0"}`},
				},
			})
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"auth ok","_stream":"{service.name=\"otel-auth-service\",service.namespace=\"prod\",k8s.pod.name=\"otel-auth-0\"}","level":"info"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	labels, summaries, err := p.detectLabels(context.Background(), `{service_name="otel-auth-service"}`, "1", "2", 50)
	if err != nil {
		t.Fatalf("detectLabels returned error: %v", err)
	}
	if summaries["level"] == nil {
		t.Fatalf("expected detected label summaries to include level, got %v", summaries)
	}
	seen := map[string]bool{}
	for _, item := range labels {
		label, _ := item["label"].(string)
		seen[label] = true
	}
	if !seen["service_name"] {
		t.Fatalf("expected detectLabels output to include service_name, got %v", labels)
	}
	if !seen["level"] {
		t.Fatalf("expected detectLabels output to include level, got %v", labels)
	}
	if !seen["service_namespace"] && !seen["service.namespace"] {
		t.Fatalf("expected detectLabels output to include service namespace label, got %v", labels)
	}
	if !seen["k8s_pod_name"] && !seen["k8s.pod.name"] {
		t.Fatalf("expected detectLabels output to include pod label, got %v", labels)
	}
}

func TestDrilldown_DetectedLabelSupplementHelpers(t *testing.T) {
	if !needsDetectedLabelScanSupplement(nil) {
		t.Fatal("nil summaries should require scan supplement")
	}
	if !needsDetectedLabelScanSupplement(map[string]*detectedLabelSummary{
		"service_name": {label: "service_name", values: map[string]struct{}{"svc": {}}},
	}) {
		t.Fatal("summaries without level should require scan supplement")
	}
	if needsDetectedLabelScanSupplement(map[string]*detectedLabelSummary{
		"level": {label: "level", values: map[string]struct{}{"info": {}}},
	}) {
		t.Fatal("summaries with level should not require scan supplement")
	}

	dst := map[string]*detectedLabelSummary{
		"service_name": {label: "service_name", values: map[string]struct{}{"svc": {}}},
	}
	scanned := map[string]*detectedLabelSummary{
		"level":        {label: "level", values: map[string]struct{}{"info": {}}},
		"service_name": {label: "service_name", values: map[string]struct{}{"other": {}}},
	}
	mergeDetectedLabelSupplements(dst, scanned)
	if dst["level"] == nil {
		t.Fatalf("expected mergeDetectedLabelSupplements to backfill level, got %v", dst)
	}
	if len(dst["service_name"].values) != 1 {
		t.Fatalf("expected existing non-derived labels to remain unchanged, got %v", dst["service_name"].values)
	}
}

func TestDrilldown_IndexVolume_ServiceNameBacktickRegexGroupsByDerivedService(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			receivedQuery = r.Form.Get("query")
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.000000Z","_msg":"ok","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:50.000000Z","_msg":"ok","_stream":"{service.name=\"payment-service\",cluster=\"us-east-1\"}","service.name":"payment-service","cluster":"us-east-1","level":"error"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bservice_name%3D~%60.%2B%60%7D&start=1&end=2", nil)
	p.handleVolume(w, r)

	if !strings.Contains(receivedQuery, `service_name:~".+"`) || !strings.Contains(receivedQuery, `app:~".+"`) {
		t.Fatalf("expected service_name regex expansion with backtick support, got %q", receivedQuery)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 2 {
		t.Fatalf("expected grouped service volumes, got %v", result)
	}
}

func TestDrilldown_IndexVolume_TargetLabelsServiceNameUsesDerivedAggregation(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T17:18:49.000000Z","_msg":"ok","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T17:18:50.000000Z","_msg":"ok","_stream":"{service.name=\"payment-service\",cluster=\"us-east-1\"}","service.name":"payment-service","cluster":"us-east-1","level":"error"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T17:18:51.000000Z","_msg":"ok","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bservice_name%3D~%60.%2B%60%7D&start=2026-04-04T17:18:00Z&end=2026-04-04T17:19:00Z&targetLabels=service_name", nil)
	p.handleVolume(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 2 {
		t.Fatalf("expected derived service_name buckets, got %v", result)
	}

	got := map[string]string{}
	for _, item := range result {
		obj := item.(map[string]interface{})
		metric := obj["metric"].(map[string]interface{})
		value := obj["value"].([]interface{})
		got[metric["service_name"].(string)] = value[1].(string)
	}
	if got["api-gateway"] != "2" || got["payment-service"] != "1" {
		t.Fatalf("expected service_name aggregation to count derived services, got %v", got)
	}
}

func TestDrilldown_InferPrimaryTargetLabel(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "empty", query: "", want: ""},
		{name: "wildcard", query: "*", want: ""},
		{name: "simple regex", query: `{cluster=~` + "`.+`" + `}`, want: "cluster"},
		{name: "first matcher wins", query: `{cluster=~` + "`.+`" + `,namespace="prod"}`, want: "cluster"},
		{name: "translated alias", query: `{k8s_pod_name="api-1",namespace="prod"}`, want: "k8s_pod_name"},
		{name: "quoted dotted", query: `{service.name="auth"}`, want: "service.name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inferPrimaryTargetLabel(tt.query); got != tt.want {
				t.Fatalf("inferPrimaryTargetLabel(%q) = %q, want %q", tt.query, got, tt.want)
			}
		})
	}
}

func TestDrilldown_IndexVolume_InfersPrimaryTargetLabelForAdditionalTabs(t *testing.T) {
	var receivedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		receivedField = r.URL.Query().Get("field")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hints": map[string]interface{}{},
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"cluster": "us-east-1"},
					"timestamps": []string{"2026-04-04T17:18:49Z"},
					"values":     []int{12},
				},
				{
					"fields":     map[string]string{"cluster": "us-west-2"},
					"timestamps": []string{"2026-04-04T17:18:49Z"},
					"values":     []int{8},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bcluster%3D~%60.%2B%60%7D&start=1&end=2", nil)
	p.handleVolume(w, r)

	if receivedField != "cluster" {
		t.Fatalf("expected inferred volume field=cluster, got %q", receivedField)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 2 {
		t.Fatalf("expected cluster buckets, got %v", result)
	}
}

func TestDrilldown_IndexVolume_TranslatesInferredTargetLabelMetrics(t *testing.T) {
	var receivedField string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		receivedField = r.URL.Query().Get("field")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hints": map[string]interface{}{},
			"hits": []map[string]interface{}{
				{
					"fields":     map[string]string{"k8s.pod.name": "api-1"},
					"timestamps": []string{"2026-04-04T17:18:49Z"},
					"values":     []int{5},
				},
			},
		})
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bk8s_pod_name%3D~%60.%2B%60%7D&start=1&end=2", nil)
	p.handleVolume(w, r)

	if receivedField != "k8s.pod.name" {
		t.Fatalf("expected translated backend field k8s.pod.name, got %q", receivedField)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one pod bucket, got %v", result)
	}
	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	if metric["k8s_pod_name"] != "api-1" {
		t.Fatalf("expected translated metric key k8s_pod_name, got %v", metric)
	}
}

func TestDrilldown_IndexVolumeRange_TargetLabelsDetectedLevelUsesDerivedAggregation(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"_time":"2026-04-04T17:18:10Z","_msg":"ok","_stream":"{app=\"api-gateway\"}","app":"api-gateway","level":"info"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T17:18:20Z","_msg":"boom","_stream":"{app=\"api-gateway\"}","app":"api-gateway","level":"error"}` + "\n"))
		w.Write([]byte(`{"_time":"2026-04-04T17:19:05Z","_msg":"ok","_stream":"{app=\"api-gateway\"}","app":"api-gateway","level":"info"}` + "\n"))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7Bservice_name%3D%22api-gateway%22%7D&start=2026-04-04T17:18:00Z&end=2026-04-04T17:20:00Z&step=60&targetLabels=detected_level", nil)
	p.handleVolumeRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 2 {
		t.Fatalf("expected detected_level matrix series, got %v", result)
	}

	got := map[string][]interface{}{}
	for _, item := range result {
		obj := item.(map[string]interface{})
		metric := obj["metric"].(map[string]interface{})
		got[metric["detected_level"].(string)] = obj["values"].([]interface{})
	}
	if len(got["info"]) == 0 || len(got["error"]) == 0 {
		t.Fatalf("expected detected_level values for info and error, got %v", got)
	}
}

func TestDrilldown_LabelValues_ServiceNameDerivedFromStreams(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []map[string]interface{}{
				{"value": `{app="api-gateway",cluster="us-east-1"}`, "hits": 12},
				{"value": `{container="worker"}`, "hits": 5},
			},
		})
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/service_name/values")
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "api-gateway")
	assertContains(t, data, "worker")
}

func TestDrilldown_DetectedFields_ParseStructuredLogsInsteadOfIndexedLabels(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":2},{"value":"cluster","hits":2}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"status\":200,\"duration_ms\":15,\"trace_id\":\"abc123\"}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"info"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:50.971082Z","_msg":"level=error msg=\"database timeout\" trace_id=err002 upstream=payment-service","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","level":"error"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api-gateway%22%7D")
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("expected fields array, got %v", resp)
	}

	got := make(map[string]map[string]interface{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = obj
	}

	if _, ok := got["app"]; ok {
		t.Fatalf("indexed label app must not appear in detected_fields: %v", got)
	}
	if _, ok := got["cluster"]; ok {
		t.Fatalf("indexed label cluster must not appear in detected_fields: %v", got)
	}
	if _, ok := got["method"]; !ok {
		t.Fatalf("expected parsed json field method, got %v", got)
	}
	if _, ok := got["method_extracted"]; ok {
		t.Fatalf("detected_fields must expose raw field names, got %v", got)
	}
	if _, ok := got["status"]; !ok {
		t.Fatalf("expected parsed json/logfmt field status, got %v", got)
	}
	if _, ok := got["detected_level"]; !ok {
		t.Fatalf("expected detected_level summary, got %v", got)
	}
}

func TestDrilldown_DetectedFields_ExposeStructuredMetadataWithDottedNames(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"cluster","hits":1},{"value":"service.name","hits":1},{"value":"service.namespace","hits":1},{"value":"k8s.pod.name","hits":1},{"value":"deployment.environment","hits":1},{"value":"trace_id","hits":1}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"token validated","_stream":"{app=\"otel-auth-service\",cluster=\"us-east-1\",service.name=\"otel-auth-service\"}","app":"otel-auth-service","cluster":"us-east-1","service.name":"otel-auth-service","service.namespace":"auth","k8s.pod.name":"auth-svc-123","deployment.environment":"prod","trace_id":"abc123"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22otel-auth-service%22%7D", nil)
	p.handleDetectedFields(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("expected fields array, got %v", resp)
	}

	got := make(map[string]map[string]interface{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = obj
	}

	for _, want := range []string{
		"service.name",
		"service_name",
		"service.namespace",
		"service_namespace",
		"k8s.pod.name",
		"k8s_pod_name",
		"deployment.environment",
		"deployment_environment",
		"trace_id",
	} {
		if _, ok := got[want]; !ok {
			t.Fatalf("expected structured metadata field %q, got %v", want, got)
		}
	}
	if got["service.name"]["parsers"] != nil {
		t.Fatalf("structured metadata field service.name must expose parsers as null, got %v", got["service.name"])
	}
	if _, ok := got["app"]; ok {
		t.Fatalf("indexed label app must not leak into detected_fields: %v", got)
	}
	if _, ok := got["cluster"]; ok {
		t.Fatalf("indexed label cluster must not leak into detected_fields: %v", got)
	}
}

func TestDrilldown_DetectedFields_TranslatedModeExposesOnlyAliases(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"cluster","hits":1},{"value":"service.name","hits":1},{"value":"service.namespace","hits":1},{"value":"k8s.pod.name","hits":1}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"token validated","_stream":"{app=\"otel-auth-service\",cluster=\"us-east-1\",service.name=\"otel-auth-service\"}","app":"otel-auth-service","cluster":"us-east-1","service.name":"otel-auth-service","service.namespace":"auth","k8s.pod.name":"auth-svc-123"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:        vlBackend.URL,
		Cache:             c,
		LogLevel:          "error",
		LabelStyle:        LabelStyleUnderscores,
		MetadataFieldMode: MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22otel-auth-service%22%7D", nil)
	p.handleDetectedFields(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("expected fields array, got %v", resp)
	}

	got := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = struct{}{}
	}

	for _, want := range []string{"service_name", "service_namespace", "k8s_pod_name"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("expected translated-only field %q, got %v", want, got)
		}
	}
	for _, forbidden := range []string{"service.name", "service.namespace", "k8s.pod.name"} {
		if _, ok := got[forbidden]; ok {
			t.Fatalf("translated-only mode must not expose native dotted field %q: %v", forbidden, got)
		}
	}
}

func TestDrilldown_DetectedLabels_ReturnDirectLikeShapeAndCardinality(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"values":[{"value":"{app=\"api-gateway\",cluster=\"us-east-1\",namespace=\"prod\",pod=\"api-1\",container=\"api-gateway\",level=\"info\"}","hits":1},{"value":"{app=\"api-gateway\",cluster=\"us-east-1\",namespace=\"prod\",pod=\"api-2\",container=\"api-gateway\",level=\"error\"}","hits":1}]}`))
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_labels?query=%7Bservice_name%3D%22api-gateway%22%7D")
	items, ok := resp["detectedLabels"].([]interface{})
	if !ok {
		t.Fatalf("expected detectedLabels array, got %v", resp)
	}

	got := map[string]float64{}
	for _, item := range items {
		obj := item.(map[string]interface{})
		got[obj["label"].(string)] = obj["cardinality"].(float64)
	}

	if got["service_name"] != 1 {
		t.Fatalf("expected service_name cardinality 1, got %v", got)
	}
	if got["pod"] != 2 {
		t.Fatalf("expected pod cardinality 2, got %v", got)
	}
	if got["level"] != 2 {
		t.Fatalf("expected level cardinality 2, got %v", got)
	}
	if _, ok := got["detected_level"]; ok {
		t.Fatalf("detected_level must not be exposed as detected label: %v", got)
	}
}

func TestDrilldown_DetectedLabels_ExcludeRawStructuredMetadata(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"values":[{"value":"{app=\"otel-auth-service\",cluster=\"us-east-1\",service.name=\"otel-auth-service\",level=\"info\"}","hits":1}]}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_labels?query=%7Bservice_name%3D%22otel-auth-service%22%7D", nil)
	p.handleDetectedLabels(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	items, ok := resp["detectedLabels"].([]interface{})
	if !ok {
		t.Fatalf("expected detectedLabels array, got %v", resp)
	}

	got := map[string]float64{}
	for _, item := range items {
		obj := item.(map[string]interface{})
		got[obj["label"].(string)] = obj["cardinality"].(float64)
	}

	if _, ok := got["service_name"]; !ok {
		t.Fatalf("expected service_name in detected labels, got %v", got)
	}
	if _, ok := got["trace_id"]; ok {
		t.Fatalf("raw metadata trace_id must not appear as detected label: %v", got)
	}
	if _, ok := got["user_id"]; ok {
		t.Fatalf("raw metadata user_id must not appear as detected label: %v", got)
	}
	if _, ok := got["service_namespace"]; ok {
		t.Fatalf("raw structured metadata must not become detected label: %v", got)
	}
}

func TestDrilldown_DetectedFieldValues_ReturnParsedValues(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":2}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"status\":200}","_stream":"{app=\"api-gateway\"}","app":"api-gateway"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:50.971082Z","_msg":"{\"method\":\"POST\",\"path\":\"/api/v1/orders\",\"status\":201}","_stream":"{app=\"api-gateway\"}","app":"api-gateway"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_field/method/values?query=%7Bapp%3D%22api-gateway%22%7D")
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	var got []string
	for _, value := range values {
		got = append(got, value.(string))
	}
	if len(got) != 2 || !contains(got, "GET") || !contains(got, "POST") {
		t.Fatalf("expected parsed method values, got %v", got)
	}
}

func TestDrilldown_DetectedFieldValues_AmbiguousAliasFallsBackToScan(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"foo.bar","hits":1},{"value":"foo-bar","hits":1}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"status\":\"ok\"}","_stream":"{app=\"api-gateway\"}","app":"api-gateway","foo.bar":"alpha","foo-bar":"beta"}` + "\n"))
		case "/select/logsql/field_values":
			t.Fatalf("ambiguous alias must not use native field_values fast path")
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/foo_bar/values?query=%7Bapp%3D%22api-gateway%22%7D", nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	if len(values) != 2 {
		t.Fatalf("expected merged values from ambiguous alias scan fallback, got %v", resp)
	}

	got := map[string]bool{}
	for _, value := range values {
		got[value.(string)] = true
	}
	if !got["alpha"] || !got["beta"] {
		t.Fatalf("expected alpha and beta values, got %v", resp)
	}
}

func TestDrilldown_DetectedFieldValues_ReturnStructuredMetadataValues(t *testing.T) {
	var sawFieldValues bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"service.name","hits":2}]}`))
		case "/select/logsql/field_values":
			sawFieldValues = true
			if got := r.URL.Query().Get("field"); got != "service.name" {
				t.Fatalf("expected native service.name field lookup, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"otel-auth-service","hits":2}]}`))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      c,
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/service.name/values?query=%7Bservice_name%3D%22otel-auth-service%22%7D", nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	if len(values) != 1 || values[0].(string) != "otel-auth-service" {
		t.Fatalf("expected structured metadata values for service.name, got %v", values)
	}
	if !sawFieldValues {
		t.Fatalf("expected native field_values fast path to be used")
	}
}

func TestDrilldown_InstantMetricQueriesPreferSingleWorkingParser(t *testing.T) {
	var (
		sampleQueries []string
		statsQuery    string
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			sampleQueries = append(sampleQueries, r.Form.Get("query"))
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"status\":200}","_stream":"{app=\"api-gateway\"}","app":"api-gateway","level":"info"}` + "\n"))
			w.Write([]byte(`{"_time":"2026-04-04T17:18:50.971082Z","_msg":"{\"method\":\"POST\",\"path\":\"/api/v1/orders\",\"status\":201}","_stream":"{app=\"api-gateway\"}","app":"api-gateway","level":"info"}` + "\n"))
		case "/select/logsql/stats_query":
			statsQuery = r.Form.Get("query")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
				"data": map[string]interface{}{
					"resultType": "vector",
					"result": []map[string]interface{}{
						{"metric": map[string]string{"level": "info"}, "value": []interface{}{float64(2), "2"}},
					},
				},
			})
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	q := url.Values{}
	q.Set("query", `sum by (detected_level) (count_over_time({service_name="api-gateway"} | json | logfmt | drop __error__, __error_details__ [15m]))`)
	q.Set("time", "2026-04-04T17:30:00Z")
	r := httptest.NewRequest("GET", "/loki/api/v1/query?"+q.Encode(), nil)
	p.handleQuery(w, r)

	if len(sampleQueries) == 0 {
		t.Fatal("expected parser probe query to be issued")
	}
	if strings.Contains(sampleQueries[0], "count_over_time") {
		t.Fatalf("expected parser probe to use extracted inner log query, got %q", sampleQueries[0])
	}
	if strings.Contains(statsQuery, "unpack_logfmt") {
		t.Fatalf("expected stats query to keep only the working parser, got %q", statsQuery)
	}
	if !strings.Contains(statsQuery, "unpack_json") {
		t.Fatalf("expected stats query to keep json parser, got %q", statsQuery)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
