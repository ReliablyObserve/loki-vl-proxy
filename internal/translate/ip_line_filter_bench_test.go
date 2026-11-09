package translate

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// ipBenchLines are log lines as a service writes them: most hold other addresses or none.
func ipBenchLines() []string {
	var out []string
	for i := 0; i < 400; i++ {
		switch i % 4 {
		case 0:
			out = append(out, fmt.Sprintf(`level=info msg="request served" path=/api/v1/items/%d status=200 duration=%dms`, i, i%97))
		case 1:
			out = append(out, fmt.Sprintf(`level=info msg="request served" client=172.16.%d.%d status=200 bytes=%d`, i%250, i%200, i*7))
		case 2:
			out = append(out, fmt.Sprintf(`{"level":"warn","msg":"slow upstream","upstream":"192.168.%d.%d:8080","ms":%d}`, i%250, i%250, i))
		default:
			out = append(out, fmt.Sprintf(`level=error msg="dial failed" addr=10.0.%d.%d:443 err="connection refused"`, i%3, i%250))
		}
	}
	return out
}

// The per-line cost of the IPv4 ip() line filter VictoriaLogs runs: the literal it searches first, then the exact
// regexp on the lines that hold the literal (IPLineFilter).
func BenchmarkIPv4LineFilter(b *testing.B) {
	lines := ipBenchLines()
	for _, p := range []string{"10.0.0.1", "10.0.0.0/24", "10.0.0.1-10.0.0.5"} {
		boxes, _ := ipv4Boxes(p)
		lit, exact := ipv4Literal(boxes), regexp.MustCompile(IPLineFilterRegex(p))
		b.Run(p, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				l := lines[i%len(lines)]
				_ = strings.Contains(l, lit) && exact.MatchString(l)
			}
		})
	}
}
