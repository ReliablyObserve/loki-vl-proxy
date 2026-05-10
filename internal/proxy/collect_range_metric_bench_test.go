package proxy

// Benchmark: old collectRangeMetricSamples (io.ReadAll + bytes.Split + json.Unmarshal into map)
// vs new implementation (bufio.Scanner + fastjson + per-request stream cache).
//
// Simulates realistic VL NDJSON responses at sizes that appear under load:
//   - small:  500 lines  (typical short-range query)
//   - medium: 5 000 lines  (heavy workload)
//   - large:  50 000 lines  (compute workload at limit)
//
// Run:
//   go test -bench=BenchmarkCollectRangeMetric -benchmem -count=5 ./internal/proxy/

import (
	"bufio"
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// ---------------------------------------------------------------------------
// Test corpus generation
// ---------------------------------------------------------------------------

// makeNDJSONLines produces n log lines across numStreams distinct streams.
func makeNDJSONLines(n, numStreams int) []byte {
	base := time.Unix(1700000000, 0).UTC()
	streams := make([]string, numStreams)
	for i := 0; i < numStreams; i++ {
		streams[i] = fmt.Sprintf(`{app="svc%02d",namespace="prod"}`, i)
	}
	levels := []string{"info", "warn", "error"}

	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		stream := streams[i%numStreams]
		level := levels[i%len(levels)]
		latency := 10 + i%500
		_, _ = fmt.Fprintf(&buf,
			`{"_time":%q,"_msg":"request handled","_stream":%q,"level":%q,"latency_ms":%d}`+"\n",
			ts.Format(time.RFC3339Nano), stream, level, latency,
		)
	}
	return buf.Bytes()
}

var (
	ndJSON500   = makeNDJSONLines(500, 5)
	ndJSON5000  = makeNDJSONLines(5_000, 10)
	ndJSON50000 = makeNDJSONLines(50_000, 20)
)

// ---------------------------------------------------------------------------
// Old approach: io.ReadAll + bytes.Split + json.Unmarshal into map
// (mirrors the pre-PR collectRangeMetricSamples hot loop)
// ---------------------------------------------------------------------------

func collectOld(body io.Reader) (int, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return 0, err
	}
	lines := bytes.Split(raw, []byte("\n"))
	var count int
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var entry map[string]interface{}
		if err2 := stdjson.Unmarshal(line, &entry); err2 != nil {
			continue
		}
		ts, _ := entry["_time"].(string)
		if ts == "" {
			continue
		}
		_ = entry["_stream"]
		_ = entry["level"]
		_ = entry["latency_ms"]
		count++
	}
	return count, nil
}

// ---------------------------------------------------------------------------
// New approach: bufio.Scanner + fastjson pool (mirrors new collectRangeMetricSamples)
// ---------------------------------------------------------------------------

func collectNew(body io.Reader) (int, error) {
	fjp := vlFJParserPool.Get() //nolint:staticcheck // fastjson pool
	defer vlFJParserPool.Put(fjp)

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)

	var count int
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		v, err := fjp.ParseBytes(line)
		if err != nil {
			continue
		}
		if len(v.GetStringBytes("_time")) == 0 {
			continue
		}
		_ = v.GetStringBytes("_stream")
		_ = v.GetStringBytes("level")
		_ = v.Get("latency_ms")
		count++
	}
	return count, scanner.Err()
}

// ---------------------------------------------------------------------------
// Micro-benchmarks: old vs new parsing + field access
// ---------------------------------------------------------------------------

func BenchmarkCollectRangeMetric_Old_500(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON500)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectOld(bytes.NewReader(ndJSON500)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectRangeMetric_New_500(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON500)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectNew(bytes.NewReader(ndJSON500)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectRangeMetric_Old_5000(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON5000)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectOld(bytes.NewReader(ndJSON5000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectRangeMetric_New_5000(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON5000)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectNew(bytes.NewReader(ndJSON5000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectRangeMetric_Old_50000(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON50000)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectOld(bytes.NewReader(ndJSON50000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectRangeMetric_New_50000(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(ndJSON50000)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collectNew(bytes.NewReader(ndJSON50000)); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// End-to-end: full handleQueryRange through the proxy (rate query → manual path)
// This captures the complete proxy overhead including stream cache, label building.
// ---------------------------------------------------------------------------

func makeBenchProxy(b *testing.B, corpus []byte) (*Proxy, *httptest.Server) {
	b.Helper()
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(corpus)
	}))
	c := cache.New(60*time.Second, 1000)
	p, err := New(Config{
		BackendURL:                   vlBackend.URL,
		Cache:                        c,
		LogLevel:                     "error",
		MetricsExportSensitiveLabels: true,
	})
	if err != nil {
		b.Fatalf("failed to create proxy: %v", err)
	}
	return p, vlBackend
}

func runE2EBench(b *testing.B, corpus []byte) {
	b.Helper()
	p, backend := makeBenchProxy(b, corpus)
	defer backend.Close()

	base := time.Unix(1700000000, 0).UTC()
	params := url.Values{}
	params.Set("query", `rate({namespace="prod"}[2m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix(), 10))
	params.Set("step", "30")
	rawURL := "/loki/api/v1/query_range?" + params.Encode()

	b.ReportAllocs()
	b.SetBytes(int64(len(corpus)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, rawURL, nil)
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkCollectRangeMetric_E2E_500(b *testing.B)   { runE2EBench(b, ndJSON500) }
func BenchmarkCollectRangeMetric_E2E_5000(b *testing.B)  { runE2EBench(b, ndJSON5000) }
func BenchmarkCollectRangeMetric_E2E_50000(b *testing.B) { runE2EBench(b, ndJSON50000) }

// ---------------------------------------------------------------------------
// seriesKeyFromMetric: old O(n²) bubble sort vs new sort.Strings
// ---------------------------------------------------------------------------

var benchMetric10 = map[string]string{
	"app": "api-gateway", "namespace": "prod", "cluster": "us-east-1",
	"region": "east", "env": "prod", "team": "platform",
	"version": "v1.2.3", "tier": "frontend", "dc": "dc1", "zone": "zone-a",
}

// BenchmarkSeriesKeyOld is the O(n²) bubble sort on k=v strings (pre-PR).
func BenchmarkSeriesKeyOld(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := benchMetric10
		if len(m) == 0 {
			_ = "{}"
			continue
		}
		var parts []string
		for k, v := range m {
			parts = append(parts, k+"="+v)
		}
		for ii := 0; ii < len(parts); ii++ {
			for j := ii + 1; j < len(parts); j++ {
				if parts[ii] > parts[j] {
					parts[ii], parts[j] = parts[j], parts[ii]
				}
			}
		}
		_ = "{" + strings.Join(parts, ",") + "}"
	}
}

func BenchmarkSeriesKeyNew(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = seriesKeyFromMetric(benchMetric10)
	}
}
