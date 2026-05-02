package proxy

// Benchmark: goccy/go-json (current) vs valyala/fastjson for the vlReaderToLokiStreams hot path.
//
// The hot loop does three distinct things for every NDJSON line:
//   1. Parse the full entry into a Go value
//   2. Direct-access four reserved fields: _time, _msg, _stream, level
//   3. Iterate all remaining fields to classify metadata (classifyEntryMetadataFields)
//
// A benchmark measuring only Unmarshal misses (3), which is the dominant work.
// These benchmarks simulate all three steps to produce a realistic comparison.

import (
	"testing"

	fj "github.com/valyala/fastjson"
	gojson "github.com/goccy/go-json"
)

// Realistic VL NDJSON entry: 4 reserved fields + 8 application fields.
// Modelled on what vlReaderToLokiStreams actually receives from VictoriaLogs.
var benchEntries = [][]byte{
	[]byte(`{"_time":"2026-05-02T10:00:00.123456789Z","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/health\",\"status\":200}","_stream":"{app=\"api-gateway\",namespace=\"prod\",cluster=\"us-east-1\"}","level":"info","app":"api-gateway","namespace":"prod","cluster":"us-east-1","method":"GET","path":"/api/v1/health","status":"200","latency_ms":"12"}`),
	[]byte(`{"_time":"2026-05-02T10:00:00.234567890Z","_msg":"user login successful","_stream":"{app=\"auth-service\",namespace=\"prod\"}","level":"info","app":"auth-service","namespace":"prod","user_id":"u-42","request_id":"req-abc-123","trace_id":"t-xyz-789","span_id":"s-111"}`),
	[]byte(`{"_time":"2026-05-02T10:00:00.345678901Z","_msg":"database query slow","_stream":"{app=\"db-proxy\",namespace=\"prod\"}","level":"warn","app":"db-proxy","namespace":"prod","query_ms":"342","table":"users","rows_examined":"100000","index_used":"false"}`),
}

// vlInternalField mirrors isVLInternalField — avoids importing internal code.
func vjInternalField(k []byte) bool {
	s := string(k)
	return s == "_time" || s == "_msg" || s == "_stream" || s == "_stream_id"
}

// --- Current approach: pool + gojson.Unmarshal + map access ---

func BenchmarkHotPath_Goccy_FullWork(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		line := benchEntries[i%len(benchEntries)]

		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		if err := gojson.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		// Step 2: direct field access (mirrors lines 499-511 in stream_processing.go)
		timeStr, _ := stringifyEntryValue(entry["_time"])
		_ = timeStr
		msg, _ := stringifyEntryValue(entry["_msg"])
		_ = msg
		rawStream := asString(entry["_stream"])
		_ = rawStream
		lvl, _ := stringifyEntryValue(entry["level"])
		_ = lvl

		// Step 3: full iteration to classify metadata fields
		// (mirrors classifyEntryMetadataFields inner loop)
		for k, v := range entry {
			if isVLInternalField(k) || k == "_stream_id" || k == "level" {
				continue
			}
			sv, ok := stringifyEntryValue(v)
			if !ok || sv == "" {
				continue
			}
			_ = sv
		}

		vlEntryPool.Put(entry)
	}
}

// --- fastjson approach: pooled parser + zero-alloc ParseBytes + Visit ---

func BenchmarkHotPath_Fastjson_FullWork(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		line := benchEntries[i%len(benchEntries)]

		p := vlFJParserPool.Get()
		v, err := p.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(p)
			continue
		}

		// Step 2: direct field access via GetStringBytes (zero-alloc, slices into buffer)
		timeStr := v.GetStringBytes("_time")
		_ = timeStr
		msg := v.GetStringBytes("_msg")
		_ = msg
		rawStream := v.GetStringBytes("_stream")
		_ = rawStream
		lvl := v.GetStringBytes("level")
		_ = lvl

		// Step 3: full iteration via Object.Visit (no key/value allocations)
		obj, err2 := v.Object()
		if err2 == nil {
			obj.Visit(func(k []byte, val *fj.Value) {
				if vjInternalField(k) || string(k) == "_stream_id" || string(k) == "level" {
					return
				}
				sv := val.GetStringBytes()
				_ = sv
			})
		}

		vlFJParserPool.Put(p)
	}
}

// --- Isolated: just parse + 4 direct lookups, no iteration ---

func BenchmarkHotPath_Goccy_ParseAndLookup(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		line := benchEntries[i%len(benchEntries)]
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		if err := gojson.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}
		timeStr, _ := stringifyEntryValue(entry["_time"])
		msg, _ := stringifyEntryValue(entry["_msg"])
		rawStream := asString(entry["_stream"])
		lvl, _ := stringifyEntryValue(entry["level"])
		_, _, _, _ = timeStr, msg, rawStream, lvl
		vlEntryPool.Put(entry)
	}
}

func BenchmarkHotPath_Fastjson_ParseAndLookup(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		line := benchEntries[i%len(benchEntries)]
		p := vlFJParserPool.Get()
		v, err := p.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(p)
			continue
		}
		timeStr := v.GetStringBytes("_time")
		msg := v.GetStringBytes("_msg")
		rawStream := v.GetStringBytes("_stream")
		lvl := v.GetStringBytes("level")
		_, _, _, _ = timeStr, msg, rawStream, lvl
		vlFJParserPool.Put(p)
	}
}

// --- Isolated: just the full-iteration step (classifyEntryMetadataFields shape) ---

func BenchmarkHotPath_Goccy_IterateFields(b *testing.B) {
	// Pre-parse once outside the loop — isolate the iteration cost.
	entry := make(map[string]interface{})
	_ = gojson.Unmarshal(benchEntries[0], &entry)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range entry {
			if isVLInternalField(k) || k == "_stream_id" || k == "level" {
				continue
			}
			sv, ok := stringifyEntryValue(v)
			if !ok || sv == "" {
				continue
			}
			_ = sv
		}
	}
}

func BenchmarkHotPath_Fastjson_IterateFields(b *testing.B) {
	// Pre-parse once outside the loop — isolate the iteration cost.
	var p fj.Parser
	v, _ := p.ParseBytes(benchEntries[0])
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj, _ := v.Object()
		obj.Visit(func(k []byte, val *fj.Value) {
			if vjInternalField(k) || string(k) == "_stream_id" || string(k) == "level" {
				return
			}
			sv := val.GetStringBytes()
			_ = sv
		})
	}
}
