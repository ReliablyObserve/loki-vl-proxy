package proxy

import (
	"net/http/httptest"
	"testing"
)

// BenchmarkWriteLokiStream_* compares the custom direct-write serializer
// (writeLokiStreamQueryResponse) against the legacy encoding/json reflection
// path (marshalJSON with map[string]interface{}). Custom avoids reflection on
// the nested value array, reducing allocs ~8-9× on typical responses.

func BenchmarkWriteLokiStream_Custom_Heavy(b *testing.B) {
	streams := makeBenchStreams(10, 200)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		writeLokiStreamQueryResponse(w, streams, false)
	}
}

func BenchmarkWriteLokiStream_Reflect_Heavy(b *testing.B) {
	streams := makeBenchStreams(10, 200)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		marshalJSON(w, map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "streams",
				"result":     streams,
				"stats":      map[string]interface{}{},
			},
		})
	}
}

func BenchmarkWriteLokiStream_Custom_Small(b *testing.B) {
	streams := makeBenchStreams(3, 30)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		writeLokiStreamQueryResponse(w, streams, false)
	}
}

func BenchmarkWriteLokiStream_Reflect_Small(b *testing.B) {
	streams := makeBenchStreams(3, 30)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		marshalJSON(w, map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "streams",
				"result":     streams,
				"stats":      map[string]interface{}{},
			},
		})
	}
}

func makeBenchStreams(nStreams, nEntries int) []map[string]interface{} {
	streams := make([]map[string]interface{}, nStreams)
	for i := range streams {
		values := make([]interface{}, nEntries)
		for j := range values {
			values[j] = []interface{}{"1614961000000000000", `{"level":"info","msg":"request processed","service":"api","trace_id":"abc123"}`}
		}
		streams[i] = map[string]interface{}{
			"stream": map[string]string{"app": "myapp", "env": "prod", "job": "service"},
			"values": values,
		}
	}
	return streams
}
