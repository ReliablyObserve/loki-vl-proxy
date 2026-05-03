package proxy

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func makePatternNDJSON(n int) []byte {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb,
			"{\"_time\":\"2026-05-03T10:%02d:%02dZ\",\"_msg\":\"POST /api/v1/query HTTP/1.1 status=200 duration_ms=%d\",\"detected_level\":\"info\",\"app\":\"api-gw\",\"service_name\":\"api-gateway\"}\n",
			(i/60)%60, i%60, 50+i%200)
	}
	return []byte(sb.String())
}

func BenchmarkExtractLogPatternsStream_100(b *testing.B) {
	data := makePatternNDJSON(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = extractLogPatternsStreamWithStats(bytes.NewReader(data), "1m", 50)
	}
}

func BenchmarkExtractLogPatternsStream_1000(b *testing.B) {
	data := makePatternNDJSON(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = extractLogPatternsStreamWithStats(bytes.NewReader(data), "1m", 50)
	}
}

func BenchmarkExtractLogPatternsStream_5000(b *testing.B) {
	data := makePatternNDJSON(5000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = extractLogPatternsStreamWithStats(bytes.NewReader(data), "1m", 50)
	}
}
