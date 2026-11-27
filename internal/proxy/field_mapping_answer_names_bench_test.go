package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkFieldMappingAnswerNames measures the answer routes whose names the
// -field-mapping-label-mode sets, per mode (unset, original, alias, both),
// with no response cache: 200 log rows with mapped and unmapped stream fields
// and structured metadata (query_range with categorize-labels), an ungrouped
// instant metric, /series and detected_fields. It uses only the API of builds
// before the mode change, so the unset mode compares with the base build.
func BenchmarkFieldMappingAnswerNames(b *testing.B) {
	var rows strings.Builder
	enc := json.NewEncoder(&rows)
	for i := 0; i < 200; i++ {
		pod := fmt.Sprintf("api-%d", i%4)
		_ = enc.Encode(map[string]string{
			"_time": fmt.Sprintf("2026-01-01T00:00:%02d.%03dZ", i%60, i), "_msg": fmt.Sprintf("request %d done", i),
			"_stream":      `{k8s.node.name="node-a",k8s.pod.name="` + pod + `",service.name="api"}`,
			"_stream_id":   pod,
			"k8s.pod.name": pod, "k8s.node.name": "node-a", "service.name": "api",
			"k8s.namespace.name": "prod", "custom.team": "core", "host.name": "host-a", "trace_id": fmt.Sprintf("t%d", i),
		})
	}
	body := rows.String()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			_, _ = w.Write([]byte(body))
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"_stream":"{k8s.node.name=\"node-a\",k8s.pod.name=\"api-0\",service.name=\"api\"}"},"value":[1767225600,"50"]},` +
				`{"metric":{"_stream":"{k8s.node.name=\"node-a\",k8s.pod.name=\"api-1\",service.name=\"api\"}"},"value":[1767225600,"50"]}]}}`))
		case "/select/logsql/streams":
			_, _ = w.Write([]byte(`{"values":[{"value":"{k8s.node.name=\"node-a\",k8s.pod.name=\"api-0\",service.name=\"api\"}","hits":1},{"value":"{k8s.node.name=\"node-a\",k8s.pod.name=\"api-1\",service.name=\"api\"}","hits":1}]}`))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	b.Cleanup(srv.Close)
	mappings := []FieldMapping{
		{VLField: "k8s.pod.name", LokiLabel: "pod"},
		{VLField: "k8s.node.name", LokiLabel: "node"},
		{VLField: "k8s.node.name", LokiLabel: "nodename"},
		{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
		{VLField: "custom.team", LokiLabel: "team"},
	}
	window := url.Values{"start": {"1767222000000000000"}, "end": {"1767225601000000000"}}
	params := func(extra ...string) string {
		v := url.Values{}
		for k, vs := range window {
			v[k] = vs
		}
		for i := 0; i+1 < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		return v.Encode()
	}
	routes := []struct {
		name, path string
		categorize bool
	}{
		{"logs", "/loki/api/v1/query_range?" + params("query", `{service_name="api"}`, "limit", "200"), true},
		{"metric", "/loki/api/v1/query?" + params("query", `count_over_time({service_name="api"}[5m])`, "time", "1767225600"), false},
		{"series", "/loki/api/v1/series?" + params("match[]", `{service_name="api"}`), false},
		{"detectedFields", "/loki/api/v1/detected_fields?" + params("query", `{service_name="api"}`), false},
	}
	for _, mode := range []string{"", "original", "alias", "both"} {
		name := mode
		if name == "" {
			name = "unset"
		}
		p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", LabelStyle: LabelStyleUnderscores,
			MetadataFieldMode: MetadataFieldModeTranslated, EmitStructuredMetadata: true,
			FieldMappingLabelMode: FieldMappingLabelMode(mode), FieldMappings: mappings})
		if err != nil {
			b.Fatal(err)
		}
		p.storeBackendVersion("v1.54.0", "v1.54.0")
		mux := http.NewServeMux()
		p.RegisterProxyRoutes(mux)
		for _, route := range routes {
			b.Run(route.name+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodGet, route.path, nil)
					if route.categorize {
						req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
					}
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("%s: %d %s", route.path, rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}
