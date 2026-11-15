package translate

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
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

// ipLongBenchLines are JSON lines of about n bytes with an address at the end: one of the pattern (10.0.0.1) or a
// near miss (10.0.0.12), which holds the pattern's literal too.
func ipLongBenchLines(n int) []string {
	var out []string
	for i := 0; i < 50; i++ {
		var b strings.Builder
		b.WriteString(`{"level":"info","msg":"request served",`)
		for b.Len() < n {
			fmt.Fprintf(&b, `"k%d":"value-%x-%d deadbeef 12:30:%02d",`, b.Len(), i*977, i, i%60)
		}
		fmt.Fprintf(&b, `"client":"10.0.0.%d"}`, []int{1, 12}[i%2])
		out = append(out, b.String())
	}
	return out
}

// ipBenchSets are the line sets of the benchmarks.
func ipBenchSets() []struct {
	name  string
	lines []string
} {
	return []struct {
		name  string
		lines []string
	}{{"service", ipBenchLines()}, {"hex", ipHexBenchLines()}, {"json1500", ipLongBenchLines(1500)}, {"json4500", ipLongBenchLines(4500)}}
}

// compiledLineFilter is a line filter tree with its regexps compiled, evaluated the way VictoriaLogs evaluates it: an
// AND stops at the first filter that drops the line, an OR at the first that keeps it.
func compiledLineFilter(b *testing.B, f logsql.FilterExpr) func(string) bool {
	switch n := f.(type) {
	case logsql.Regexp:
		re := regexp.MustCompile(n.Pattern)
		if lit, complete := re.LiteralPrefix(); complete {
			return func(l string) bool { return strings.Contains(l, lit) }
		}
		return re.MatchString
	case logsql.AllOf:
		fs := make([]func(string) bool, len(n))
		for i, e := range n {
			fs[i] = compiledLineFilter(b, e)
		}
		return func(l string) bool {
			for _, f := range fs {
				if !f(l) {
					return false
				}
			}
			return true
		}
	case logsql.AnyOf:
		fs := make([]func(string) bool, len(n))
		for i, e := range n {
			fs[i] = compiledLineFilter(b, e)
		}
		return func(l string) bool {
			for _, f := range fs {
				if f(l) {
					return true
				}
			}
			return false
		}
	}
	b.Fatalf("unexpected filter node %T", f)
	return nil
}

// The per-line cost of the IPv4 ip() line filter VictoriaLogs runs (IPLineFilter): the literal it searches first,
// then the regexps on the lines that hold the literal.
func BenchmarkIPv4LineFilter(b *testing.B) {
	for _, set := range ipBenchSets() {
		for _, p := range []string{"10.0.0.1", "10.0.0.0/24", "10.0.0.1-10.0.0.5", "0.0.0.0/0"} {
			match := compiledLineFilter(b, IPLineFilter(p, false))
			b.Run(set.name+"/"+p, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = match(set.lines[i%len(set.lines)])
				}
			})
		}
	}
}

// The exact regexp alone on every line (IPLineFilterRegex), which the Go row condition of a label filter runs on a
// label value.
func BenchmarkIPv4LineRegexp(b *testing.B) {
	values := []string{"10.0.0.5:443", "client 10.0.0.1", "192.168.1.7", "a0:10.0.0.1", "n/a"}
	for _, set := range append(ipBenchSets(), struct {
		name  string
		lines []string
	}{"value", values}) {
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

// The cost of building the line filter, and of VictoriaLogs reading its regexps on every query: regexp/syntax
// Parse, Simplify and String of each, as its regexp simplification does.
func BenchmarkIPv4LineRegexpParse(b *testing.B) {
	for _, p := range []string{"10.0.0.1", "10.0.0.0/24", "0.0.0.0/0", "10.0.1.250-10.0.3.4"} {
		b.Run(p, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var walk func(f logsql.FilterExpr)
				walk = func(f logsql.FilterExpr) {
					switch n := f.(type) {
					case logsql.Regexp:
						re, err := syntax.Parse(n.Pattern, syntax.Perl)
						if err != nil {
							b.Fatal(err)
						}
						_ = re.Simplify().String()
					case logsql.AllOf:
						for _, e := range n {
							walk(e)
						}
					case logsql.AnyOf:
						for _, e := range n {
							walk(e)
						}
					}
				}
				walk(IPLineFilter(p, false))
			}
		})
	}
}
