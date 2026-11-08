package queryplan

import (
	"encoding/json"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The cost gate of the logical build, over the queries of the benchmark shapes and the semantics matrix: at most
// twice the allocations of logql.Parse of the same query (TestBuildAllocsGate, always run), and a p50 of Build of at
// most 2 µs. The p50 is wall-clock time, so BenchmarkBuild only reports it (ns/p50, ns/p99) and checks it when asked:
//
//	LVP_QUERYPLAN_BENCH_GATE=1 go test ./internal/queryplan -run '^$' -bench 'BenchmarkBuild$' -benchtime=10x
const (
	buildP50Gate    = 2 * time.Microsecond
	buildAllocsGate = 2.0
	benchGateEnv    = "LVP_QUERYPLAN_BENCH_GATE"
)

// seedExprs parses every query string of the shapes and the matrix that holds a selector.
func seedExprs(tb testing.TB) (queries []string, exprs []logql.Expr) {
	tb.Helper()
	seen := map[string]bool{}
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case string:
			if strings.Contains(v, "{") && !seen[v] {
				seen[v] = true
				if e, err := logql.Parse(v); err == nil {
					queries, exprs = append(queries, v), append(exprs, e)
				}
			}
		case []any:
			for _, e := range v {
				walk(e)
			}
		case map[string]any:
			for _, e := range v {
				walk(e)
			}
		}
	}
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			tb.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			tb.Fatalf("%s: %v", path, err)
		}
		walk(doc)
	}
	if len(exprs) < 300 {
		tb.Fatalf("the seed has %d parsed queries", len(exprs))
	}
	return queries, exprs
}

func BenchmarkBuild(b *testing.B) {
	_, exprs := seedExprs(b)
	in := Inputs{Kind: KindRange}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Build(exprs[i%len(exprs)], in); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	p50, p99 := buildPercentiles(exprs, in)
	b.ReportMetric(float64(p50.Nanoseconds()), "ns/p50")
	b.ReportMetric(float64(p99.Nanoseconds()), "ns/p99")
	if os.Getenv(benchGateEnv) == "1" && p50 > buildP50Gate {
		b.Fatalf("Build p50 is %v, the gate is %v", p50, buildP50Gate)
	}
}

// buildPercentiles is the p50 and p99 over the queries of the best of three timings of each one's Build.
func buildPercentiles(exprs []logql.Expr, in Inputs) (p50, p99 time.Duration) {
	const reps = 200
	per := make([]time.Duration, len(exprs))
	for i, e := range exprs {
		best := time.Duration(1 << 62)
		for round := 0; round < 3; round++ {
			start := time.Now()
			for r := 0; r < reps; r++ {
				_, _ = Build(e, in)
			}
			best = min(best, time.Since(start)/reps)
		}
		per[i] = best
	}
	sort.Slice(per, func(i, j int) bool { return per[i] < per[j] })
	return per[len(per)/2], per[len(per)*99/100]
}

func BenchmarkParseSeed(b *testing.B) {
	queries, _ := seedExprs(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := logql.Parse(queries[i%len(queries)]); err != nil {
			b.Fatal(err)
		}
	}
}

// raceBuild is whether the binary has the race detector, which slows Build and adds allocations.
func raceBuild() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, s := range info.Settings {
		if s.Key == "-race" {
			return s.Value == "true"
		}
	}
	return false
}

// TestBuildAllocsGate: every seed query builds with at most twice the allocations logql.Parse makes for it.
func TestBuildAllocsGate(t *testing.T) {
	if raceBuild() {
		t.Skip("the race detector changes allocation counts")
	}
	queries, exprs := seedExprs(t)
	in := Inputs{Kind: KindRange}
	over := 0
	for i, e := range exprs {
		parse := testing.AllocsPerRun(5, func() { _, _ = logql.Parse(queries[i]) })
		build := testing.AllocsPerRun(5, func() { _, _ = Build(e, in) })
		if build > buildAllocsGate*parse {
			over++
			if over <= 5 {
				t.Errorf("%q: Build allocates %.0f, logql.Parse %.0f", queries[i], build, parse)
			}
		}
	}
	if over > 0 {
		t.Errorf("%d of %d seed queries are over the allocation gate", over, len(exprs))
	}
}
