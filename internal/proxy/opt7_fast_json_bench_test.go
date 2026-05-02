package proxy

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	gojson "github.com/goccy/go-json"
)

const benchNDJSONEntry = `{"_time":"1746100000000000000","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/health\",\"status\":200,\"latency_ms\":12}","_stream":"{app=\"api-gateway\",namespace=\"prod\",cluster=\"us-east-1\"}","app":"api-gateway","namespace":"prod","cluster":"us-east-1","method":"GET","path":"/api/v1/health","status":"200","latency_ms":"12","level":"info"}`

func BenchmarkJSON_StdlibUnmarshalToMap(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := make(map[string]interface{})
		_ = stdjson.Unmarshal(data, &entry)
	}
}

func BenchmarkJSON_GoccyUnmarshalToMap(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := make(map[string]interface{})
		_ = gojson.Unmarshal(data, &entry)
	}
}

func BenchmarkJSON_PooledEntryStdlib(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		_ = stdjson.Unmarshal(data, &entry)
		vlEntryPool.Put(entry)
	}
}

func BenchmarkJSON_PooledEntryGoccy(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		_ = gojson.Unmarshal(data, &entry)
		vlEntryPool.Put(entry)
	}
}

func BenchmarkJSON_ScannerHotPath(b *testing.B) {
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = benchNDJSONEntry
	}
	body := strings.Join(lines, "\n")

	b.Run("stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, line := range lines {
				entry := vlEntryPool.Get().(map[string]interface{})
				for k := range entry {
					delete(entry, k)
				}
				_ = stdjson.Unmarshal([]byte(line), &entry)
				vlEntryPool.Put(entry)
			}
		}
		_ = body
	})

	b.Run("goccy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, line := range lines {
				entry := vlEntryPool.Get().(map[string]interface{})
				for k := range entry {
					delete(entry, k)
				}
				_ = gojson.Unmarshal([]byte(line), &entry)
				vlEntryPool.Put(entry)
			}
		}
		_ = body
	})
}
