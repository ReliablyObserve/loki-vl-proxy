package translate

import (
	"fmt"
	"regexp"
	"regexp/syntax"
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

// ipHexBenchLines are lines full of hex digits and colons (trace ids, MAC and IPv6 addresses, times), the text the
// walk's stretches cross.
func ipHexBenchLines() []string {
	var out []string
	for i := 0; i < 400; i++ {
		out = append(out, fmt.Sprintf(`ts=12:%02d:%02d.%03d trace_id=4bf92f3577b34da6a3ce929d0e0e%04x mac=00:1a:2b:3c:4d:%02x peer=[2001:db8::%x]:443 client=10.0.%d.%d`,
			i%60, i%60, i, i, i%256, i, i%3, i%250))
	}
	return out
}

// The exact regexp alone on every line, as VictoriaLogs runs it on the label values of a block that holds text
// and on the lines that hold the literal.
func BenchmarkIPv4LineRegexp(b *testing.B) {
	for _, set := range []struct {
		name  string
		lines []string
	}{{"service", ipBenchLines()}, {"hex", ipHexBenchLines()}} {
		for _, p := range []string{"10.0.0.1", "10.0.0.0/24", "0.0.0.0/0"} {
			exact := regexp.MustCompile(IPLineFilterRegex(p))
			b.Run(set.name+"/"+p, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = exact.MatchString(set.lines[i%len(set.lines)])
				}
			})
		}
	}
}

// The cost of building the regexp, and of VictoriaLogs reading it on every query: regexp/syntax Parse, Simplify and
// String, as its regexp simplification does.
func BenchmarkIPv4LineRegexpParse(b *testing.B) {
	for _, p := range []string{"10.0.0.1", "10.0.0.0/24", "0.0.0.0/0", "10.0.1.250-10.0.3.4"} {
		b.Run(p, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				re, err := syntax.Parse(IPLineFilterRegex(p), syntax.Perl)
				if err != nil {
					b.Fatal(err)
				}
				_ = re.Simplify().String()
			}
		})
	}
}
