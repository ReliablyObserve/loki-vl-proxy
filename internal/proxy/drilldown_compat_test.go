package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

func TestDrilldown_ServiceNameValuesFromDetectedLabels_UsesNativeDetectedLabels(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/streams" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"{service.name=\"api\",level=\"info\"}","hits":10},{"value":"{service.name=\"worker\",level=\"warn\"}","hits":7}]}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	values, err := p.serviceNameValuesFromDetectedLabels(context.Background(), `{service_name=~".+"}`, "", "")
	if err != nil {
		t.Fatalf("serviceNameValuesFromDetectedLabels returned error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 service_name values, got %v", values)
	}
	if values[0] != "api" || values[1] != "worker" {
		t.Fatalf("unexpected service_name values: %v", values)
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
	if !needsDetectedLabelScanSupplement(map[string]*detectedLabelSummary{
		"level": {label: "level", values: map[string]struct{}{"info": {}}},
	}) {
		t.Fatal("summaries without service_name should require scan supplement")
	}
	if !needsDetectedLabelScanSupplement(map[string]*detectedLabelSummary{
		"level":        {label: "level", values: map[string]struct{}{"info": {}}},
		"service_name": {label: "service_name", values: map[string]struct{}{unknownServiceName: {}}},
	}) {
		t.Fatal("summaries with only unknown_service should require scan supplement")
	}
	if needsDetectedLabelScanSupplement(map[string]*detectedLabelSummary{
		"level":        {label: "level", values: map[string]struct{}{"info": {}}},
		"service_name": {label: "service_name", values: map[string]struct{}{"svc": {}}},
	}) {
		t.Fatal("summaries with level and service_name should not require scan supplement")
	}

	dst := map[string]*detectedLabelSummary{
		"level":        {label: "level", values: map[string]struct{}{"info": {}}},
		"service_name": {label: "service_name", values: map[string]struct{}{unknownServiceName: {}}},
	}
	scanned := map[string]*detectedLabelSummary{
		"level":        {label: "level", values: map[string]struct{}{"warn": {}}},
		"service_name": {label: "service_name", values: map[string]struct{}{"svc": {}}},
	}
	mergeDetectedLabelSupplements(dst, scanned)
	if dst["service_name"] == nil || len(dst["service_name"].values) != 1 {
		t.Fatalf("expected mergeDetectedLabelSupplements to backfill service_name, got %v", dst)
	}
	if _, ok := dst["service_name"].values["svc"]; !ok {
		t.Fatalf("expected mergeDetectedLabelSupplements to replace unknown_service, got %v", dst["service_name"].values)
	}
	if len(dst["level"].values) != 1 {
		t.Fatalf("expected existing non-derived labels to remain unchanged, got %v", dst["level"].values)
	}
}

func TestDrilldown_IndexVolume_ServiceNameBacktickRegexGroupsByDerivedService(t *testing.T) {
	var receivedQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			receivedQuery = r.URL.Query().Get("query")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"hints":{},"hits":[` +
				`{"fields":{"app":"api-gateway","cluster":"us-east-1","level":"info"},"timestamps":["2026-04-04T17:18:49Z"],"values":[1]},` +
				`{"fields":{"service.name":"payment-service","cluster":"us-east-1","level":"error"},"timestamps":["2026-04-04T17:18:50Z"],"values":[1]}` +
				`]}`))
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
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hints":{},"hits":[` +
			`{"fields":{"app":"api-gateway","cluster":"us-east-1","level":"info"},"timestamps":["2026-04-04T17:18:49Z"],"values":[2]},` +
			`{"fields":{"service.name":"payment-service","cluster":"us-east-1","level":"error"},"timestamps":["2026-04-04T17:18:50Z"],"values":[1]}` +
			`]}`))
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
	for _, item := range result {
		metric := item.(map[string]interface{})["metric"].(map[string]interface{})
		if _, ok := metric["service_name"]; ok {
			t.Fatalf("expected inferred cluster volume metrics to omit synthetic unknown service_name, got %v", metric)
		}
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

func TestDrilldown_IndexVolume_UsesDrilldownFieldByFallbackForTargetLabels(t *testing.T) {
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
					"fields":     map[string]string{"method": "GET"},
					"timestamps": []string{"2026-04-04T17:18:49Z"},
					"values":     []int{4},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/index/volume?query=%7Bcluster%3D~%60.%2B%60%7D&start=1&end=2&var-fieldBy=method&drillDownLabel=method",
		nil,
	)
	p.handleVolume(w, r)

	if receivedField != "method" {
		t.Fatalf("expected drilldown fieldBy fallback to drive target label mapping, got %q", receivedField)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one method bucket, got %v", result)
	}
	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	if metric["method"] != "GET" {
		t.Fatalf("expected method grouping bucket in metric, got %v", metric)
	}
}

func TestDrilldown_IndexVolumeRange_InfersPrimaryTargetLabelWithoutUnknownService(t *testing.T) {
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
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7Bcluster%3D~%60.%2B%60%7D&start=1&end=2&step=60", nil)
	p.handleVolumeRange(w, r)

	if receivedField != "cluster" {
		t.Fatalf("expected inferred volume_range field=cluster, got %q", receivedField)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one cluster series, got %v", result)
	}
	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	if _, ok := metric["service_name"]; ok {
		t.Fatalf("expected inferred cluster volume_range metric to omit synthetic unknown service_name, got %v", metric)
	}
}

func TestDrilldown_IndexVolumeRange_UsesDrilldownFieldByFallbackForTargetLabels(t *testing.T) {
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
					"fields":     map[string]string{"method": "POST"},
					"timestamps": []string{"2026-04-04T17:18:49Z"},
					"values":     []int{6},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(
		"GET",
		"/loki/api/v1/index/volume_range?query=%7Bcluster%3D~%60.%2B%60%7D&start=1&end=2&step=60&var-fieldBy=method&drillDownLabel=method",
		nil,
	)
	p.handleVolumeRange(w, r)

	if receivedField != "method" {
		t.Fatalf("expected drilldown fieldBy fallback to drive volume_range target label mapping, got %q", receivedField)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one method series, got %v", result)
	}
	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	if metric["method"] != "POST" {
		t.Fatalf("expected method grouping series in volume_range metric, got %v", metric)
	}
}

func TestDrilldown_IndexVolumeRange_TargetLabelsDetectedLevelUsesDerivedAggregation(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hints":{},"hits":[` +
			`{"fields":{"level":"info","app":"api-gateway"},"timestamps":["2026-04-04T17:18:10Z","2026-04-04T17:19:05Z"],"values":[1,1]},` +
			`{"fields":{"level":"error","app":"api-gateway"},"timestamps":["2026-04-04T17:18:20Z"],"values":[1]}` +
			`]}`))
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

func TestDrilldown_IndexVolume_DerivedTargetLabelsSendRepeatedFieldParams(t *testing.T) {
	var receivedFields []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		receivedFields = append([]string(nil), r.URL.Query()["field"]...)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hints":{},"hits":[` +
			`{"fields":{"app":"api-gateway","level":"info"},"timestamps":["2026-04-04T17:18:49Z"],"values":[1]}` +
			`]}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume?query=%7Bservice_name%3D~%60.%2B%60%7D&start=1&end=2&targetLabels=service_name", nil)
	p.handleVolume(w, r)

	if len(receivedFields) < 2 {
		t.Fatalf("expected repeated field params for derived service_name grouping, got %v", receivedFields)
	}
	for _, field := range receivedFields {
		if strings.Contains(field, ",") {
			t.Fatalf("expected each field in its own query param, got combined field %q (%v)", field, receivedFields)
		}
	}
	for _, want := range []string{"service_name", "service.name", "app"} {
		if !contains(receivedFields, want) {
			t.Fatalf("expected derived field %q in query params, got %v", want, receivedFields)
		}
	}
}

func TestDrilldown_IndexVolumeRange_MultipleTargetLabelsSendRepeatedFieldParams(t *testing.T) {
	var receivedFields []string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		receivedFields = append([]string(nil), r.URL.Query()["field"]...)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hints":{},"hits":[` +
			`{"fields":{"cluster":"us-east-1","namespace":"prod"},"timestamps":["2026-04-04T17:18:49Z"],"values":[1]}` +
			`]}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7Bcluster%3D~%60.%2B%60%7D&start=1&end=2&step=60&targetLabels=cluster,namespace", nil)
	p.handleVolumeRange(w, r)

	if len(receivedFields) != 2 {
		t.Fatalf("expected two repeated field params for cluster+namespace grouping, got %v", receivedFields)
	}
	for _, field := range receivedFields {
		if strings.Contains(field, ",") {
			t.Fatalf("expected each field in its own query param, got combined field %q (%v)", field, receivedFields)
		}
	}
	if !contains(receivedFields, "cluster") || !contains(receivedFields, "namespace") {
		t.Fatalf("expected cluster and namespace field params, got %v", receivedFields)
	}
}

func TestDrilldown_IndexVolumeRange_TargetLabelsServiceName_FillsFullRangeBuckets(t *testing.T) {
	const (
		start = "2026-04-01T00:00:00Z"
		end   = "2026-04-08T00:00:00Z"
	)

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hints":{},"hits":[` +
			`{"fields":{"app":"api-gateway","level":"info"},"timestamps":["2026-04-01T00:00:00Z","2026-04-04T00:00:00Z","2026-04-08T00:00:00Z"],"values":[5,4,3]}` +
			`]}`))
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/index/volume_range?query=%7Bservice_name%3D%22api-gateway%22%7D&start="+url.QueryEscape(start)+"&end="+url.QueryEscape(end)+"&step=3600&targetLabels=service_name", nil)
	p.handleVolumeRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one service_name matrix series, got %v", result)
	}
	series := result[0].(map[string]interface{})
	values := series["values"].([]interface{})
	if len(values) != (7*24 + 1) {
		t.Fatalf("expected full 7-day hourly bucket coverage (169 points), got %d", len(values))
	}
	first := values[0].([]interface{})
	last := values[len(values)-1].([]interface{})
	if first[1].(string) != "5" {
		t.Fatalf("expected first bucket count=5, got %v", first)
	}
	if last[1].(string) != "3" {
		t.Fatalf("expected last bucket count=3, got %v", last)
	}
}

func TestDrilldown_LabelValues_ServiceNameUsesNativeFieldValues(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.NotFound(w, r)
		case "/select/logsql/field_names":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "service.name", "hits": 12},
				},
			})
		case "/select/logsql/field_values":
			if got := r.URL.Query().Get("field"); got != "service.name" {
				t.Fatalf("expected service_name fast path to use service.name field first, got %q", got)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "api-gateway", "hits": 12},
					{"value": "worker", "hits": 5},
				},
			})
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/service_name/values")
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "api-gateway")
	assertContains(t, data, "worker")
}

func TestDrilldown_LabelValues_ServiceNamePrefersConcreteNativeFieldInventory(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.NotFound(w, r)
		case "/select/logsql/field_names":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "service_name", "hits": 3},
					{"value": "app", "hits": 12},
				},
			})
		case "/select/logsql/field_values":
			switch got := r.URL.Query().Get("field"); got {
			case "app":
				json.NewEncoder(w).Encode(map[string]interface{}{
					"values": []map[string]interface{}{
						{"value": "api-gateway", "hits": 12},
						{"value": "worker", "hits": 5},
					},
				})
			case "service_name":
				json.NewEncoder(w).Encode(map[string]interface{}{
					"values": []map[string]interface{}{
						{"value": "otel-auth-service", "hits": 2},
					},
				})
			default:
				t.Fatalf("expected service_name values to use concrete app inventory and merge any sparse service_name values, got %q", got)
			}
		case "/select/logsql/streams", "/select/logsql/query":
			t.Fatalf("service_name native inventory must not fall back to stream scans when app inventory is available, got %s", r.URL.Path)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/service_name/values")
	data := assertDataIsStringArray(t, resp)
	assertContains(t, data, "api-gateway")
	assertContains(t, data, "worker")
}

func TestDrilldown_LabelValues_ServiceNameMergesStreamAndStructuredMetadataInventory(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "app", "hits": 12},
				},
			})
		case "/select/logsql/stream_field_values":
			if got := r.URL.Query().Get("field"); got != "app" {
				t.Fatalf("expected stream service inventory lookup to use app field, got %q", got)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "api-gateway", "hits": 12},
					{"value": "worker", "hits": 5},
				},
			})
		case "/select/logsql/field_names":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "service.name", "hits": 4},
				},
			})
		case "/select/logsql/field_values":
			if got := r.URL.Query().Get("field"); got != "service.name" {
				t.Fatalf("expected structured metadata lookup to use service.name field, got %q", got)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"values": []map[string]interface{}{
					{"value": "otel-auth-service", "hits": 4},
				},
			})
		case "/select/logsql/streams", "/select/logsql/query":
			t.Fatalf("service_name label values must be resolved from metadata inventory before stream scans, got %s", r.URL.Path)
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/label/service_name/values?query=*")
	data := assertDataIsStringArray(t, resp)
	if len(data) != 3 {
		t.Fatalf("expected merged service inventory from stream and structured metadata fields, got %v", data)
	}
	assertContains(t, data, "api-gateway")
	assertContains(t, data, "worker")
	assertContains(t, data, "otel-auth-service")
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

// TestDrilldown_DetectedFields_NoExtractedSuffixForJSONFields guards the compliance decision
// that the proxy exposes raw VL field names (service.name, session_id, load_ms) rather than
// adding Loki's _extracted suffix convention (service_name_extracted, level_extracted, etc.).
// Loki renames JSON fields that conflict with stream labels using _extracted; the proxy does not.
// This behavior is intentionally different and must not be changed. Stamped compliant 2026-04-25.
func TestDrilldown_DetectedFields_NoExtractedSuffixForJSONFields(t *testing.T) {
	// VL returns flat indexed fields plus _msg containing the original nested JSON.
	// This mirrors what VictoriaLogs v1.50+ returns when JSON logs with nested objects
	// like {"service": {"name": "frontend-ssr"}} are auto-parsed.
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"service_name","hits":10},{"value":"service.name","hits":10},{"value":"session_id","hits":10},{"value":"load_ms","hits":10},{"value":"level","hits":10}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			// VL response: _msg = original JSON (without _msg key), plus all fields indexed flat.
			// level is both a VL indexed field and a stream label — exactly the case where
			// Loki would emit level_extracted, but the proxy must not.
			line := `{"_time":"2026-04-25T19:00:00Z","_msg":"{\"service\":{\"name\":\"frontend-ssr\"},\"session_id\":\"abc123\",\"load_ms\":500,\"level\":\"info\"}","_stream":"{level=\"info\",service_name=\"frontend-ssr\"}","service.name":"frontend-ssr","service_name":"frontend-ssr","session_id":"abc123","load_ms":"500","level":"info"}` + "\n"
			w.Write([]byte(line))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22frontend-ssr%22%7D")
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("expected fields array, got %v", resp)
	}

	got := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = struct{}{}
	}

	// Proxy must NOT produce any _extracted suffix — that is Loki's convention, not ours.
	for label := range got {
		if strings.HasSuffix(label, "_extracted") {
			t.Errorf("proxy must not emit _extracted suffix (Loki convention); got label %q in %v", label, got)
		}
	}

	// Core JSON fields must appear under their original names.
	for _, want := range []string{"session_id", "load_ms"} {
		if _, ok := got[want]; !ok {
			t.Errorf("expected JSON field %q to appear in detected_fields; got %v", want, got)
		}
	}
}

// TestDrilldown_DetectedFields_LevelStreamConflictDroppedNotRenamedExtracted guards that when
// the stream label "level" conflicts with a JSON field "level" in _msg, the proxy drops the
// JSON-parsed level from detected_fields entirely — it does NOT rename it to level_extracted
// (which is Loki's behaviour). detected_level must still be synthesised from the stream label.
func TestDrilldown_DetectedFields_LevelStreamConflictDroppedNotRenamedExtracted(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":5},{"value":"level","hits":5},{"value":"status","hits":5}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			// level is a stream label AND present inside _msg JSON — conflict case.
			line := `{"_time":"2026-04-25T19:00:00Z","_msg":"{\"level\":\"warn\",\"status\":500}","_stream":"{app=\"api-gw\",level=\"warn\"}","app":"api-gw","level":"warn","status":"500"}` + "\n"
			w.Write([]byte(line))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api-gw%22%7D")
	fields, ok := resp["fields"].([]interface{})
	if !ok {
		t.Fatalf("expected fields array, got %v", resp)
	}

	got := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = struct{}{}
	}

	if _, exists := got["level_extracted"]; exists {
		t.Errorf("proxy must not emit level_extracted (Loki convention); it must drop level entirely when it conflicts with a stream label; got %v", got)
	}
	if _, exists := got["detected_level"]; !exists {
		t.Errorf("detected_level must still be synthesised even when level conflicts with stream label; got %v", got)
	}
}

func TestDrilldown_DetectedFields_SuppressHighCardinalityTimestampTerminals(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"app","hits":1},{"value":"cluster","hits":1},{"value":"timestamp_end","hits":1},{"value":"observed_timestamp_end","hits":1}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/users\",\"timestamp_end\":\"2026-04-04T17:18:49.971082Z\",\"observed_timestamp_end\":\"2026-04-04T17:18:49.971082Z\"}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\"}","app":"api-gateway","cluster":"us-east-1","timestamp_end":"2026-04-04T17:18:49.971082Z","observed_timestamp_end":"2026-04-04T17:18:49.971082Z","level":"info"}` + "\n"))
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

	got := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		obj := field.(map[string]interface{})
		got[obj["label"].(string)] = struct{}{}
	}

	if _, exists := got["timestamp_end"]; exists {
		t.Fatalf("timestamp_end must be suppressed from detected_fields: %v", got)
	}
	if _, exists := got["observed_timestamp_end"]; exists {
		t.Fatalf("observed_timestamp_end must be suppressed from detected_fields: %v", got)
	}
	if _, exists := got["method"]; !exists {
		t.Fatalf("expected parsed field method to remain discoverable, got %v", got)
	}
	if _, exists := got["detected_level"]; !exists {
		t.Fatalf("expected detected_level summary to remain discoverable, got %v", got)
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

func TestDrilldown_DetectedLabels_BackfillsServiceNameFromScanWhenNativeStreamsMissIt(t *testing.T) {
	var streamCalls, scanCalls int
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			streamCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[{"value":"{cluster=\"ops-sand\",k8s.cluster.name=\"ops-sand\",level=\"info\"}","hits":5}]}`))
		case "/select/logsql/query":
			scanCalls++
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-19T07:00:00Z","_msg":"request ok","k8s.cluster.name":"ops-sand","service.name":"otel-auth-service","level":"info"}` + "\n"))
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
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_labels?query=%7Bk8s_cluster_name%3D%22ops-sand%22%7D&limit=25", nil)
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
		t.Fatalf("expected service_name in detected labels after scan supplement, got %v", got)
	}
	if got["service_name"] != 1 {
		t.Fatalf("expected service_name cardinality 1 after scan supplement, got %v", got)
	}
	if streamCalls == 0 || scanCalls == 0 {
		t.Fatalf("expected both native streams and scan supplement paths, got streamCalls=%d scanCalls=%d", streamCalls, scanCalls)
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

func TestDrilldown_DetectedFieldValues_ServiceNameUsesFastPath(t *testing.T) {
	var (
		sawFieldNames  bool
		sawFieldValues bool
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stream_field_names":
			http.NotFound(w, r)
		case "/select/logsql/field_names":
			sawFieldNames = true
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"service.name","hits":2}]}`))
		case "/select/logsql/field_values":
			sawFieldValues = true
			if r.URL.Query().Get("field") != "service.name" {
				t.Fatalf("expected service_name fast path to use service.name field first, got %q", r.URL.Query().Get("field"))
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"grafana","hits":2}]}`))
		case "/select/logsql/streams", "/select/logsql/query":
			t.Fatalf("service_name detected field values must avoid stream scans, got %s", r.URL.Path)
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

	params := url.Values{}
	params.Set("query", `{deployment_environment="dev"} | json | logfmt | source_message_bytes="89"`)
	params.Set("start", "1")
	params.Set("end", "2")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/service_name/values?"+params.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	if len(values) != 1 || values[0].(string) != "grafana" {
		t.Fatalf("expected service_name values from fast path, got %v", values)
	}
	if !sawFieldNames || !sawFieldValues {
		t.Fatalf("expected field_names + field_values fast path, got fieldNames=%v fieldValues=%v", sawFieldNames, sawFieldValues)
	}
}

func TestDrilldown_DetectedFieldValues_IgnoreZeroHitNativeValues(t *testing.T) {
	var sawFieldValues bool
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"shared_log_type","hits":4}]}`))
		case "/select/logsql/field_values":
			sawFieldValues = true
			if got := r.URL.Query().Get("field"); got != "shared_log_type" {
				t.Fatalf("expected shared_log_type lookup, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"access","hits":4},{"value":"common","hits":0}]}`))
		case "/select/logsql/streams", "/select/logsql/query":
			t.Fatalf("positive-hit native values should avoid fallback scans, got %s", r.URL.Path)
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

	params := url.Values{}
	params.Set("query", `{deployment_environment="dev",k8s_cluster_name="ops-sand"}`)
	params.Set("start", "1")
	params.Set("end", "2")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/shared_log_type/values?"+params.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	if !sawFieldValues {
		t.Fatalf("expected native field_values fast path to be used")
	}
	if len(values) != 1 || values[0].(string) != "access" {
		t.Fatalf("expected only positive-hit native values, got %v", values)
	}
}

func TestDrilldown_DetectedFields_ServesStaleCacheOnBackendError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22cached-svc%22%7D", nil)
	cacheKey := p.canonicalReadCacheKey("detected_fields", "", req)
	p.setJSONCacheWithTTL(cacheKey, time.Millisecond, map[string]interface{}{
		"status": "success",
		"data": []map[string]interface{}{
			{"label": "cached_field", "type": "string", "cardinality": 1},
		},
		"fields": []map[string]interface{}{
			{"label": "cached_field", "type": "string", "cardinality": 1},
		},
		"limit": 1000,
	})
	time.Sleep(5 * time.Millisecond)

	w := httptest.NewRecorder()
	r := req
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected stale cached detected_fields response, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	fields := resp["fields"].([]interface{})
	if len(fields) != 1 || fields[0].(map[string]interface{})["label"] != "cached_field" {
		t.Fatalf("expected stale cached detected_fields payload, got %v", resp)
	}
}

func TestDrilldown_DetectedFields_ReturnsErrorWithoutCacheOnBackendFailure(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bservice_name%3D%22svc%22%7D", nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 when detected_fields has no cache fallback, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestDrilldown_DetectedLabels_ServesStaleCacheOnBackendError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/detected_labels?query=%7Bk8s_cluster_name%3D%22ops-sand%22%7D", nil)
	cacheKey := p.canonicalReadCacheKey("detected_labels", "", req)
	p.setJSONCacheWithTTL(cacheKey, time.Millisecond, map[string]interface{}{
		"status": "success",
		"data": []map[string]interface{}{
			{"label": "service_name", "cardinality": 1},
		},
		"detectedLabels": []map[string]interface{}{
			{"label": "service_name", "cardinality": 1},
		},
		"limit": 1000,
	})
	time.Sleep(5 * time.Millisecond)

	w := httptest.NewRecorder()
	r := req
	p.handleDetectedLabels(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected stale cached detected_labels response, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	items := resp["detectedLabels"].([]interface{})
	if len(items) != 1 || items[0].(map[string]interface{})["label"] != "service_name" {
		t.Fatalf("expected stale cached detected_labels payload, got %v", resp)
	}
}

func TestDrilldown_DetectedFieldValues_ServesStaleCacheOnBackendError(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	req := httptest.NewRequest("GET", "/loki/api/v1/detected_field/service_name/values?query=%7Bservice_name%3D%22cached-svc%22%7D", nil)
	cacheKey := p.canonicalReadCacheKey("detected_field_values", "", req, "service_name")
	p.setJSONCacheWithTTL(cacheKey, time.Millisecond, map[string]interface{}{
		"status": "success",
		"data":   []string{"cached-svc"},
		"values": []string{"cached-svc"},
		"limit":  1000,
	})
	time.Sleep(5 * time.Millisecond)

	w := httptest.NewRecorder()
	r := req
	p.handleDetectedFieldValues(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected stale cached detected_field_values response, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values := resp["values"].([]interface{})
	if len(values) != 1 || values[0].(string) != "cached-svc" {
		t.Fatalf("expected stale cached detected_field_values payload, got %v", resp)
	}
}

func TestDetectedLabelValuesForField_ResolvesKnownUnderscoreAlias(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/streams":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"{k8s.cluster.name=\"prod-cluster\",app=\"api\"}","hits":1}]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"_time":"2026-04-04T17:18:49.971082Z","_msg":"ok","_stream":"{k8s.cluster.name=\"prod-cluster\",app=\"api\"}"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL: backend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	values := p.detectedLabelValuesForField(context.Background(), "k8s_cluster_name", `{app="api"}`, "1", "2", 25)
	if len(values) != 1 || values[0] != "prod-cluster" {
		t.Fatalf("expected known underscore alias to resolve to dotted label values, got %v", values)
	}
}

func TestDrilldown_InstantMetricQueriesPreferSingleWorkingParser(t *testing.T) {
	var (
		sampleQueries []string
		statsQuery    string
		manualQuery   string
	)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			if r.Form.Get("limit") == "1000000" {
				manualQuery = r.Form.Get("query")
			}
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

	effectiveMetricQuery := statsQuery
	if effectiveMetricQuery == "" {
		effectiveMetricQuery = manualQuery
	}
	if effectiveMetricQuery == "" {
		t.Fatalf("expected either stats query or manual metric query to be issued")
	}
	if strings.Contains(effectiveMetricQuery, "unpack_logfmt") {
		t.Fatalf("expected metric query to keep only the working parser, got %q", effectiveMetricQuery)
	}
	if !strings.Contains(effectiveMetricQuery, "unpack_json") {
		t.Fatalf("expected metric query to keep json parser, got %q", effectiveMetricQuery)
	}
}

// TestDrilldown_SumCountOverTimeWithParserAndDropError_UsesNativeStats is a regression
// test for the "logs not counted" bug. Grafana Drilldown sends
// sum(count_over_time({...} | json | logfmt | drop __error__, __error_details__ [interval]))
// as an instant query to get the total log count. Before the fix, handleStatsCompatInstant
// routed all parser-stage queries to the manual log-fetch path, which returned per-stream
// results (one series per stream, each with count=1) instead of a single aggregated count.
func TestDrilldown_SumCountOverTimeWithParserAndDropError_UsesNativeStats(t *testing.T) {
	var statsQueryCalled bool
	var manualQueryCalled bool
	var statsQueryBody string

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/query":
			// Only flag as manual metric fetch when limit=1000000 (collectRangeMetricSamples).
			// The preferWorkingParser probe also hits this path with a small limit.
			if r.Form.Get("limit") == "1000000" {
				manualQueryCalled = true
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
		case "/select/logsql/stats_query":
			statsQueryCalled = true
			statsQueryBody = r.Form.Get("query")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
				"data": map[string]interface{}{
					"resultType": "vector",
					"result": []map[string]interface{}{
						{"metric": map[string]string{"__name__": "count(*)"}, "value": []interface{}{float64(1700000000), "92880"}},
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
	q.Set("query", `sum(count_over_time({env="production"} | json | logfmt | drop __error__, __error_details__ [10800s]))`)
	q.Set("time", "1700000000")
	r := httptest.NewRequest("GET", "/loki/api/v1/query?"+q.Encode(), nil)
	p.handleQuery(w, r)

	if manualQueryCalled {
		t.Error("sum() without by() should NOT use the manual log-fetch path (regression: per-stream results instead of single sum)")
	}
	if !statsQueryCalled {
		t.Fatal("sum() without by() must use native VL stats_query (not manual path)")
	}
	if !strings.Contains(statsQueryBody, "stats count()") {
		t.Errorf("stats query should contain 'stats count()' without a by() clause, got: %q", statsQueryBody)
	}

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Data.ResultType != "vector" {
		t.Errorf("expected resultType=vector, got %q", result.Data.ResultType)
	}
	if len(result.Data.Result) != 1 {
		t.Errorf("expected single result (total sum), got %d results (per-stream regression)", len(result.Data.Result))
	}
	if len(result.Data.Result) > 0 && len(result.Data.Result[0].Metric) > 0 {
		t.Errorf("expected empty metric {} for sum() without by(), got %v", result.Data.Result[0].Metric)
	}
}

func TestDrilldown_LabelCardMetricQuery_ServiceNameNonEmptyFilterUsesSyntheticAnyMatch(t *testing.T) {
	var statsQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsQuery = r.Form.Get("query")
			if strings.Contains(statsQuery, `service_name:!"" "service.name":!""`) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "success",
					"data": map[string]interface{}{
						"resultType": "matrix",
						"result":     []map[string]interface{}{},
					},
				})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
				"data": map[string]interface{}{
					"resultType": "matrix",
					"result": []map[string]interface{}{
						{
							"metric": map[string]string{"service.name": "argocd"},
							"values": [][]interface{}{{float64(1712538000), "42"}},
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	q := url.Values{}
	q.Set("query", `sum(count_over_time({service_name="argocd",service_name != ""}[5s])) by (service_name)`)
	q.Set("start", "2026-04-04T17:00:00Z")
	q.Set("end", "2026-04-04T17:30:00Z")
	q.Set("step", "300")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	if statsQuery == "" {
		t.Fatal("expected stats query_range request to be issued")
	}
	if !strings.Contains(statsQuery, `(service_name:!"" OR "service.name":!""`) {
		t.Fatalf("expected synthetic service_name non-empty matcher to use OR across source fields, got %q", statsQuery)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected one matrix series, got %v", result)
	}
	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	if metric["service_name"] != "argocd" {
		t.Fatalf("expected translated service_name label in metric response, got %v", metric)
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

// =============================================================================
// detected_level inference from _msg content (Loki parity)
//
// Loki 3.x adds detected_level as a stream label at ingest time by parsing
// the log line. The proxy must replicate this on the read path so Explore and
// Drilldown show detected_level even when level is not a VL stream field.
// =============================================================================

func TestExtractLevelFromMsg_JSON(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
		ok    bool
	}{
		{"json_level_error", `{"level":"error","message":"failed"}`, "error", true},
		{"json_level_warn", `{"level":"warn","msg":"slow"}`, "warn", true},
		{"json_severity_key", `{"severity":"INFO","msg":"ok"}`, "INFO", true},
		{"json_lvl_key", `{"lvl":"debug","msg":"trace"}`, "debug", true},
		{"json_no_level", `{"message":"no level here"}`, "", false},
		{"plain_text_no_level", `something happened at the server`, "", false},
		{"empty", ``, "", false},
		{"json_empty_level", `{"level":""}`, "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractLevelFromMsg(tc.input)
			if ok != tc.ok {
				t.Errorf("%s: ok=%v want %v (got level=%q)", tc.name, ok, tc.ok, got)
			}
			if ok && got != tc.want {
				t.Errorf("%s: level=%q want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestExtractLevelFromMsg_Logfmt(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
		ok    bool
	}{
		{"logfmt_level", `level=error ts=2024-01-01 msg="bad"`, "error", true},
		{"logfmt_level_quoted", `level="warn" msg="slow"`, "warn", true},
		{"logfmt_severity", `severity=info msg=ok`, "info", true},
		{"logfmt_lvl", `lvl=debug caller=main.go`, "debug", true},
		{"logfmt_no_level", `msg=ok caller=main.go`, "", false},
		{"logfmt_level_in_middle", `ts=2024 level=fatal msg=crash`, "fatal", true},
		{"logfmt_partial_key", `notlevel=error msg=ok`, "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractLevelFromMsg(tc.input)
			if ok != tc.ok {
				t.Errorf("%s: ok=%v want %v (got level=%q)", tc.name, ok, tc.ok, got)
			}
			if ok && got != tc.want {
				t.Errorf("%s: level=%q want %q", tc.name, got, tc.want)
			}
		})
	}
}

// TestBuildEntryLabels_InfersDetectedLevelFromJSONMsg verifies that
// buildEntryLabels populates detected_level from a JSON _msg field even when
// VL has not extracted level as a top-level NDJSON field.
func TestBuildEntryLabels_InfersDetectedLevelFromJSONMsg(t *testing.T) {
	entry := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="json-test"}`,
		"_msg":    `{"level":"error","message":"something failed","code":500}`,
	}
	labels := buildEntryLabels(entry)
	if labels["detected_level"] != "error" {
		t.Errorf("expected detected_level=error from JSON _msg, got %q", labels["detected_level"])
	}
	if labels["level"] != "error" {
		t.Errorf("expected level=error synthesized, got %q", labels["level"])
	}
}

// TestBuildEntryLabels_InfersDetectedLevelFromLogfmtMsg verifies logfmt _msg
// parsing for detected_level synthesis.
func TestBuildEntryLabels_InfersDetectedLevelFromLogfmtMsg(t *testing.T) {
	entry := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="payment-service"}`,
		"_msg":    `level=warn ts=2024-01-01 msg="slow query"`,
	}
	labels := buildEntryLabels(entry)
	if labels["detected_level"] != "warn" {
		t.Errorf("expected detected_level=warn from logfmt _msg, got %q", labels["detected_level"])
	}
}

// TestBuildEntryLabels_NativeLevelTakesPrecedenceOverMsg verifies that when VL
// surfaces level as a top-level NDJSON field (native VL or OTel severity),
// it takes precedence over parsing _msg.
func TestBuildEntryLabels_NativeLevelTakesPrecedenceOverMsg(t *testing.T) {
	entry := map[string]interface{}{
		"_time":   "2024-01-01T00:00:00Z",
		"_stream": `{app="api"}`,
		"_msg":    `{"level":"error","message":"should be overridden"}`,
		"level":   "info", // native VL field wins
	}
	labels := buildEntryLabels(entry)
	if labels["detected_level"] != "info" {
		t.Errorf("expected native level=info to win over JSON _msg level=error, got detected_level=%q", labels["detected_level"])
	}
}

// TestVlLogsToLokiStreams_SplitsStreamsByDetectedLevelFromJSONMsg verifies that
// vlLogsToLokiStreams creates separate Loki streams per detected_level when
// entries have JSON _msg with different level values — matching Loki's ingest
// behavior of splitting streams by detected_level.
func TestVlLogsToLokiStreams_SplitsStreamsByDetectedLevelFromJSONMsg(t *testing.T) {
	records := []map[string]interface{}{
		{"_time": "2024-01-01T00:00:01Z", "_stream": `{app="json-test"}`, "_msg": `{"level":"error","message":"failed"}`},
		{"_time": "2024-01-01T00:00:02Z", "_stream": `{app="json-test"}`, "_msg": `{"level":"warn","message":"slow"}`},
		{"_time": "2024-01-01T00:00:03Z", "_stream": `{app="json-test"}`, "_msg": `{"level":"info","message":"ok"}`},
	}
	body := buildNDJSON(records)

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams (one per level), got %d: %v", len(streams), streamsDebug(streams))
	}
	levels := map[string]bool{}
	for _, s := range streams {
		labels := s["stream"].(map[string]string)
		lvl := labels["detected_level"]
		if lvl == "" {
			t.Errorf("stream missing detected_level: %v", labels)
			continue
		}
		levels[lvl] = true
	}
	for _, want := range []string{"error", "warn", "info"} {
		if !levels[want] {
			t.Errorf("missing stream for detected_level=%q; got levels: %v", want, levels)
		}
	}
}

// TestVlLogsToLokiStreams_SplitsStreamsByDetectedLevelFromLogfmtMsg verifies
// the same stream-splitting behavior for logfmt _msg entries.
func TestVlLogsToLokiStreams_SplitsStreamsByDetectedLevelFromLogfmtMsg(t *testing.T) {
	records := []map[string]interface{}{
		{"_time": "2024-01-01T00:00:01Z", "_stream": `{app="svc"}`, "_msg": `level=error msg="db timeout"`},
		{"_time": "2024-01-01T00:00:02Z", "_stream": `{app="svc"}`, "_msg": `level=info msg="started"`},
	}
	body := buildNDJSON(records)

	streams := vlLogsToLokiStreams(body)
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams (error+info), got %d: %v", len(streams), streamsDebug(streams))
	}
	for _, s := range streams {
		labels := s["stream"].(map[string]string)
		if labels["detected_level"] == "" {
			t.Errorf("stream missing detected_level for logfmt _msg: %v", labels)
		}
	}
}

// =============================================================================
// detected_field/{name}/values — hits=0 regression guards
//
// VL returns hits=0 for valid non-indexed fields (trace_id, amount, ttl, etc.)
// where it found the values but did not count occurrences. The proxy must
// include these instead of filtering them out, matching Loki's behavior of
// returning all discovered values regardless of hit counts.
// =============================================================================

// TestFetchNativeFieldValues_AllZeroHitsIncluded guards against the regression
// where all-zero-hit values (VL's response for high-cardinality non-indexed
// fields like trace_id) were filtered out, leaving the Drilldown fields panel
// showing repeated field names instead of actual values.
func TestFetchNativeFieldValues_AllZeroHitsIncluded(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"trace_id","hits":0}]}`))
		case "/select/logsql/field_values":
			w.Header().Set("Content-Type", "application/json")
			// All hits=0: VL found the values but didn't count occurrences.
			w.Write([]byte(`{"values":[{"value":"abc-123","hits":0},{"value":"def-456","hits":0},{"value":"","hits":0}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", `{env="production"}`)
	params.Set("start", "1")
	params.Set("end", "2")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/trace_id/values?"+params.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %T: %v", resp["values"], resp)
	}
	// Must include all non-blank values even with hits=0; blank ("") must be excluded.
	if len(values) != 2 {
		t.Fatalf("expected 2 values (abc-123, def-456), got %d: %v", len(values), values)
	}
	got := map[string]bool{}
	for _, v := range values {
		got[v.(string)] = true
	}
	for _, want := range []string{"abc-123", "def-456"} {
		if !got[want] {
			t.Errorf("expected value %q in response, got %v", want, values)
		}
	}
	if got[""] {
		t.Errorf("blank value must not be included in response")
	}
}

// TestFetchNativeFieldValues_MixedHitsStillFiltersZeros guards the other side:
// when VL returns a mix of hits>0 and hits=0, only positive-hit values are
// returned (hits=0 in a mixed set means stale indexed names with no data).
func TestFetchNativeFieldValues_MixedHitsStillFiltersZeros(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"values":[{"value":"status","hits":10}]}`))
		case "/select/logsql/field_values":
			w.Header().Set("Content-Type", "application/json")
			// Mix: active=10, stale=0. Stale must be excluded.
			w.Write([]byte(`{"values":[{"value":"active","hits":10},{"value":"stale","hits":0}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	params := url.Values{}
	params.Set("query", `{env="production"}`)
	params.Set("start", "1")
	params.Set("end", "2")
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_field/status/values?"+params.Encode(), nil)
	p.handleDetectedFieldValues(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	values, ok := resp["values"].([]interface{})
	if !ok {
		t.Fatalf("expected values array, got %v", resp)
	}
	if len(values) != 1 || values[0].(string) != "active" {
		t.Fatalf("expected only positive-hit value 'active', got %v", values)
	}
}

// =============================================================================
// parsers inference: VL auto-extracted JSON body fields
// =============================================================================

// TestDetectFieldSummaries_VLAutoExtractedFieldsGetJSONParser guards that
// top-level VL fields auto-extracted from JSON log bodies (not in _stream,
// not dotted OTel names) receive parsers:["json"] and jsonPath:[field].
// Without this, Grafana Drilldown uses label_values (stream-label path)
// instead of detected_field/values, showing repeated field names as values.
func TestDetectFieldSummaries_VLAutoExtractedFieldsGetJSONParser(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			// VL auto-extracted: _msg is the inner plain-text message, but
			// confidence/latency_ms/model came from the original JSON body.
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49Z","_msg":"inference ok 330ms","_stream":"{app=\"ml-serving\",cluster=\"us-east-1\",service_name=\"ml-serving\"}","app":"ml-serving","cluster":"us-east-1","service_name":"ml-serving","confidence":"0.9295","latency_ms":"330","model":"rec-v3"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newCompatTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bapp%3D%22ml-serving%22%7D&start=1&end=2", nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	fields, _ := resp["fields"].([]interface{})
	byLabel := make(map[string]map[string]interface{}, len(fields))
	for _, f := range fields {
		obj := f.(map[string]interface{})
		byLabel[obj["label"].(string)] = obj
	}

	for _, name := range []string{"confidence", "latency_ms", "model"} {
		f, ok := byLabel[name]
		if !ok {
			t.Errorf("expected field %q in detected_fields, got labels: %v", name, func() []string {
				keys := make([]string, 0, len(byLabel))
				for k := range byLabel {
					keys = append(keys, k)
				}
				return keys
			}())
			continue
		}
		parsers, _ := f["parsers"].([]interface{})
		if len(parsers) == 0 || parsers[0] != "json" {
			t.Errorf("VL auto-extracted field %q must have parsers:[\"json\"], got %v", name, f["parsers"])
		}
		jp, _ := f["jsonPath"].([]interface{})
		if len(jp) == 0 {
			t.Errorf("VL auto-extracted field %q must have jsonPath:[%q], got %v", name, name, f["jsonPath"])
		}
	}

	// Stream labels must keep parsers:null
	for _, name := range []string{"service_name"} {
		if f, ok := byLabel[name]; ok {
			if f["parsers"] != nil {
				t.Errorf("stream label %q must have parsers:null, got %v", name, f["parsers"])
			}
		}
	}
}

// TestDetectFieldSummaries_DottedOTelFieldsKeepNullParsers guards that
// OTel dotted-name fields (service.name, k8s.pod.name) are NOT inferred as
// json-parser fields. They are structured metadata, not JSON body fields.
func TestDetectFieldSummaries_DottedOTelFieldsKeepNullParsers(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49Z","_msg":"trace ok","_stream":"{app=\"otel-svc\",service.name=\"otel-svc\"}","app":"otel-svc","service.name":"otel-svc","k8s.pod.name":"pod-123","confidence":"0.85"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newCompatTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bapp%3D%22otel-svc%22%7D&start=1&end=2", nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	fields, _ := resp["fields"].([]interface{})
	byLabel := make(map[string]map[string]interface{}, len(fields))
	for _, f := range fields {
		obj := f.(map[string]interface{})
		byLabel[obj["label"].(string)] = obj
	}

	// OTel dotted fields must keep parsers:null
	for _, name := range []string{"service.name", "k8s.pod.name"} {
		if f, ok := byLabel[name]; ok {
			if f["parsers"] != nil {
				t.Errorf("OTel dotted field %q must keep parsers:null (not json), got %v", name, f["parsers"])
			}
		}
	}

	// But plain scalar fields like confidence (no dot) must get parsers:["json"]
	if f, ok := byLabel["confidence"]; ok {
		parsers, _ := f["parsers"].([]interface{})
		if len(parsers) == 0 || parsers[0] != "json" {
			t.Errorf("field confidence must have parsers:[\"json\"], got %v", f["parsers"])
		}
	}
}

// TestDetectFieldSummaries_LogfmtMsgFieldsKeepLogfmtParser guards that fields
// detected from logfmt _msg content keep parsers:["logfmt"], not overridden to
// "json" by the VL auto-extraction inference.
func TestDetectFieldSummaries_LogfmtMsgFieldsKeepLogfmtParser(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/field_names":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			// Logfmt log: _msg is logfmt, amount is NOT a top-level VL field
			_, _ = w.Write([]byte(`{"_time":"2026-04-04T17:18:49Z","_msg":"level=info msg=\"payment processed\" amount=42.50 currency=USD","_stream":"{app=\"payment-service\"}","app":"payment-service"}` + "\n"))
		default:
			t.Fatalf("unexpected backend path %s", r.URL.Path)
		}
	}))
	defer vlBackend.Close()

	p := newCompatTestProxy(t, vlBackend.URL)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/detected_fields?query=%7Bapp%3D%22payment-service%22%7D&start=1&end=2", nil)
	p.handleDetectedFields(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	fields, _ := resp["fields"].([]interface{})
	byLabel := make(map[string]map[string]interface{}, len(fields))
	for _, f := range fields {
		obj := f.(map[string]interface{})
		byLabel[obj["label"].(string)] = obj
	}

	if f, ok := byLabel["amount"]; ok {
		parsers, _ := f["parsers"].([]interface{})
		if len(parsers) == 0 || parsers[0] != "logfmt" {
			t.Errorf("logfmt-detected field 'amount' must keep parsers:[\"logfmt\"], got %v", f["parsers"])
		}
	} else {
		t.Errorf("expected 'amount' in detected_fields for logfmt log, got %v", byLabel)
	}
}

func streamsDebug(streams []map[string]interface{}) []string {
	out := make([]string, len(streams))
	for i, s := range streams {
		labels, _ := s["stream"].(map[string]string)
		out[i] = fmt.Sprintf("%v", labels)
	}
	return out
}

// =============================================================================
// Regression: Drilldown log count display bug (underscore proxy + Loki-push data)
//
// When data is ingested via Loki push, "service_name" is a stream label stored
// under the underscore name. The underscore proxy translated "service_name" to
// "service.name" in the VL stats by() clause. VL couldn't find "service.name" for
// Loki-push data and returned an empty value, causing the log count to display as
// a label string rather than a numeric count in Grafana Logs Drilldown.
//
// Fix: the proxy now emits both "service.name" and "service_name" in the by() clause,
// and coalesces them in the response (non-empty wins), so either data format works.
// =============================================================================

func TestDrilldownLogCountUnderscokeProxyLokiPushData(t *testing.T) {
	t.Parallel()

	var receivedStatsQuery string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if r.URL.Path != "/select/logsql/stats_query_range" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		receivedStatsQuery = r.Form.Get("query")

		// Simulate Loki-push data: VL returns service.name="" (not found) but
		// service_name="payment-service" (found as stream label in by clause).
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "matrix",
				"result": []map[string]interface{}{
					{
						"metric": map[string]string{
							"service.name": "",                // OTel field not found
							"service_name": "payment-service", // stream label found
						},
						"values": [][]interface{}{{float64(1712538000), "13920"}},
					},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	q := url.Values{}
	q.Set("query", `sum by (service_name) (count_over_time({app="payment-service"}[60s]))`)
	q.Set("start", "2026-04-08T10:00:00Z")
	q.Set("end", "2026-04-08T11:00:00Z")
	q.Set("step", "60")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	// The by() clause must include both "service.name" and "service_name" so that
	// VL can group correctly for both OTel and Loki-push data.
	if !strings.Contains(receivedStatsQuery, "service_name") {
		t.Fatalf("expected stats query to include underscore fallback 'service_name', got: %q", receivedStatsQuery)
	}

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected 1 matrix series, got %d", len(result))
	}

	series := result[0].(map[string]interface{})
	metric := series["metric"].(map[string]interface{})

	// service_name must be "payment-service" (not "" — that was the bug)
	if got := fmt.Sprintf("%v", metric["service_name"]); got != "payment-service" {
		t.Fatalf("log count display bug: service_name=%q, want \"payment-service\" (not empty string)", got)
	}

	// The count value must be a numeric string, not a label name
	values, ok := series["values"].([]interface{})
	if !ok || len(values) == 0 {
		t.Fatalf("expected values array, got %T %v", series["values"], series["values"])
	}
	pair := values[0].([]interface{})
	if len(pair) < 2 {
		t.Fatalf("expected [ts, value] pair, got %v", pair)
	}
	countStr := fmt.Sprintf("%v", pair[1])
	// Must be numeric — if it's a label string the count display is broken
	if _, err := strconv.ParseFloat(countStr, 64); err != nil {
		t.Fatalf("count value %q is not numeric (Drilldown log count display bug): %v", countStr, err)
	}
}

func TestDrilldownLogCountUnderscokeProxyOTelData(t *testing.T) {
	t.Parallel()

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if r.URL.Path != "/select/logsql/stats_query_range" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}

		// Simulate OTel data: service.name has value, service_name is empty.
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "matrix",
				"result": []map[string]interface{}{
					{
						"metric": map[string]string{
							"service.name": "otel-service", // OTel field found
							"service_name": "",             // stream label not found
						},
						"values": [][]interface{}{{float64(1712538000), "4200"}},
					},
				},
			},
		})
	}))
	defer vlBackend.Close()

	p, err := New(Config{
		BackendURL: vlBackend.URL,
		Cache:      cache.New(60*time.Second, 1000),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	w := httptest.NewRecorder()
	q := url.Values{}
	q.Set("query", `sum by (service_name) (count_over_time({app="otel-service"}[60s]))`)
	q.Set("start", "2026-04-08T10:00:00Z")
	q.Set("end", "2026-04-08T11:00:00Z")
	q.Set("step", "60")
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+q.Encode(), nil)
	p.handleQueryRange(w, r)

	var resp map[string]interface{}
	mustUnmarshal(t, w.Body.Bytes(), &resp)
	data := assertDataIsObject(t, resp)
	result := assertResultIsArray(t, data)
	if len(result) != 1 {
		t.Fatalf("expected 1 matrix series, got %d", len(result))
	}

	metric := result[0].(map[string]interface{})["metric"].(map[string]interface{})
	// OTel data: service.name="otel-service" must coalesce to service_name="otel-service"
	if got := fmt.Sprintf("%v", metric["service_name"]); got != "otel-service" {
		t.Fatalf("OTel coalescing: service_name=%q, want \"otel-service\"", got)
	}
}
