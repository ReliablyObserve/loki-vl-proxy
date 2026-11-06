package proxy

import (
	"fmt"
	"testing"
)

// The miner's answer for 20k varied access-log lines, wildcarding included.
func BenchmarkExtractLogPatternsVariedLines(b *testing.B) {
	var lines []string
	for i := 0; i < 20000; i++ {
		lines = append(lines, fmt.Sprintf(`10.%d.%d.%d - u%d [06/Nov/2026:10:%02d:%02d +0000] "%s /api/v%d/item/%d HTTP/1.1" %d %d "-" "agent-%d/%d.%d" %d`,
			i%200, i%97, i%13, i%31, i%60, i%59, []string{"GET", "POST", "PUT", "DELETE"}[i%4], i%3, i, 200+i%5, i*7, i%50, i%9, i%7, i%11))
	}
	body := patternRowsOf("info", lines...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractLogPatterns(body, "1m", 1000, defaultLogRowLevels())
	}
}
