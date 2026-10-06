package proxy

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// stageBenchBody returns 500 VictoriaLogs rows as the UI log generator's
// Loki pushes are stored: a JSON line whose keys are also stored fields,
// beside OTel structured metadata.
func stageBenchBody() []byte {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var body []byte
	for i := 0; i < 500; i++ {
		user := "u" + strconv.Itoa(i%7)
		line := `{"msg":"request served","user":"` + user + `","status":200,"latency_ms":` + strconv.Itoa(i%300) + `,"svc":{"name":"api","zone":"z1"}}`
		row := map[string]string{
			"_time":        start.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			"_msg":         line,
			"_stream":      `{app="api",env="production"}`,
			"app":          "api",
			"env":          "production",
			"msg":          "request served",
			"user":         user,
			"status":       "200",
			"latency_ms":   strconv.Itoa(i % 300),
			"svc.name":     "api",
			"svc.zone":     "z1",
			"k8s.pod.name": "api-" + strconv.Itoa(i%5),
			"trace_id":     strconv.FormatInt(int64(i)*7919, 16),
		}
		encoded, _ := json.Marshal(row)
		body = append(append(body, encoded...), '\n')
	}
	return body
}

// BenchmarkLogQueryStreams_LokiProfileStages converts 500 rows of a
// categorize-labels log query in the Loki-compatible profile for the stage
// shapes whose field exposure the profile decides.
func BenchmarkLogQueryStreams_LokiProfileStages(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error",
		EmitStructuredMetadata: true, LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated})
	if err != nil {
		b.Fatal(err)
	}
	body := stageBenchBody()
	for _, bc := range []struct{ name, query string }{
		{"plain", `{app="api"}`},
		{"json", `{app="api"} | json`},
		{"json_list", `{app="api"} | json user, latency_ms`},
		{"logfmt_on_json", `{app="api"} | logfmt`},
		{"metadata_filter", `{app="api"} | k8s_pod_name="api-1"`},
		{"line_key_filter", `{app="api"} | user="u1"`},
		{"json_filter", `{app="api"} | json | user="u1"`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), bc.query, "", true, true, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// stageCollisionBenchBody returns 500 rows whose JSON line holds keys named
// like three stream labels (app, level, service_name), as VictoriaLogs holds
// them after unpack_json: the stored fields carry the line's values.
func stageCollisionBenchBody() []byte {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var body []byte
	for i := 0; i < 500; i++ {
		user := "u" + strconv.Itoa(i%7)
		line := `{"level":"debug","app":"inner","service_name":"inner","msg":"request served","user":"` + user + `","status":200}`
		row := map[string]string{
			"_time":        start.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			"_msg":         line,
			"_stream":      `{app="api",env="production",level="info",service_name="api"}`,
			"app":          "inner",
			"env":          "production",
			"level":        "debug",
			"service_name": "inner",
			"msg":          "request served",
			"user":         user,
			"status":       "200",
			"k8s.pod.name": "api-" + strconv.Itoa(i%5),
		}
		encoded, _ := json.Marshal(row)
		body = append(append(body, encoded...), '\n')
	}
	return body
}

// BenchmarkLogQueryStreams_ExtractedCollision converts rows whose parsed keys
// collide with stream labels: with | json each collision is one more label
// (name_extracted) in the response, which is the added work; without a parser
// the stored fields are hidden and cost the repeated-label check only.
func BenchmarkLogQueryStreams_ExtractedCollision(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error",
		EmitStructuredMetadata: true, LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated})
	if err != nil {
		b.Fatal(err)
	}
	body := stageCollisionBenchBody()
	for _, bc := range []struct{ name, query string }{
		{"plain", `{app="api"}`},
		{"json", `{app="api"} | json`},
		{"json_filter", `{app="api"} | json | user="u1"`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), bc.query, "", true, true, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
