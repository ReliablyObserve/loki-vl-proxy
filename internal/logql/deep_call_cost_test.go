package logql

import (
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
)

// deepCallShape is a query shape nested to any depth: base, wrapped depth times by wrap (a format with one %s).
type deepCallShape struct {
	name, base, wrap string
}

// deepCallShapes are the deeply nested queries the parse, the validation and the request rewrite must read in
// time linear in their size. Loki accepts each nesting at every depth below its input size limit (its parse
// oracle: up to 4,518 levels, 129 KB); the last two are rejected at the innermost call.
var deepCallShapes = []deepCallShape{
	{"label_replace", `sum by (app) (rate({app="api"}[1m]))`, `label_replace(%s, "dst", "$1", "src", "(.*)")`},
	{"sum-label_replace", `rate({app="api"}[1m])`, `sum(label_replace(%s, "dst", "$1", "src", "(.*)"))`},
	{"spaced", `sum by (app) (rate({app="api"}[1m]))`, `label_replace( %s , "dst", "$1", "src", "(.*)")`},
	{"binary-vector", `rate({app="api"}[1m])`, `label_replace(%s + vector(1), "dst", "$1", "src", "(.*)")`},
	{"three-arguments-innermost", `label_replace(rate({app="api"}[1m]), "dst", "$1", "src")`, `label_replace(%s, "dst", "$1", "src", "(.*)")`},
	{"bad-regex-innermost", `label_replace(rate({app="api"}[1m]), "dst", "$1", "src", "(")`, `label_replace(%s, "dst", "$1", "src", "(.*)")`},
}

// deepCall is shape nested depth times.
func deepCall(shape deepCallShape, depth int) string {
	prefix, suffix, _ := strings.Cut(shape.wrap, "%s")
	var b strings.Builder
	b.Grow(depth*len(shape.wrap) + len(shape.base))
	for i := 0; i < depth; i++ {
		b.WriteString(prefix)
	}
	b.WriteString(shape.base)
	for i := 0; i < depth; i++ {
		b.WriteString(suffix)
	}
	return b.String()
}

// deepCallMaxDepth is the deepest nesting of shape below Loki's input size limit.
func deepCallMaxDepth(shape deepCallShape) int {
	return (MaxInputSize - 1 - len(shape.base)) / (len(shape.wrap) - len("%s"))
}

// deepCallPhases are the readers of query text the sweep times.
var deepCallPhases = []struct {
	name string
	run  func(q string)
}{
	{"parse", func(q string) { _, _ = Parse(q) }},
	{"validate", func(q string) { _ = ValidateLogQL(q) }},
	{"spelling", func(q string) { _ = CanonicalSpelling(q) }},
}

// deepCallCost is one run of a phase: its time, allocations, bytes allocated and the peak of the live heap above
// where it started.
type deepCallCost struct {
	took             time.Duration
	allocs, bytes    uint64
	peakHeapAboveRun uint64
}

// measureDeepCall runs run(q) once and measures it.
func measureDeepCall(run func(string), q string) deepCallCost {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
	readHeap := func() uint64 {
		metrics.Read(sample)
		return sample[1].Value.Uint64()
	}
	base := readHeap()
	peak := base
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(200 * time.Microsecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				peak = max(peak, readHeap())
			}
		}
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	run(q)
	took := time.Since(start)
	runtime.ReadMemStats(&after)
	close(stop)
	<-done
	peak = max(peak, readHeap())
	return deepCallCost{
		took:             took,
		allocs:           after.Mallocs - before.Mallocs,
		bytes:            after.TotalAlloc - before.TotalAlloc,
		peakHeapAboveRun: peak - base,
	}
}

// TestDeepCallCostSweep writes the depth and size sweep of every nested shape and phase as a Markdown table (to
// the file LVP_DEEP_CALL_SWEEP names). Each phase of a shape stops at the first depth over its time budget.
func TestDeepCallCostSweep(t *testing.T) {
	out := os.Getenv("LVP_DEEP_CALL_SWEEP")
	if out == "" {
		t.Skip("set LVP_DEEP_CALL_SWEEP=<file> to write the sweep")
	}
	const budget = 2 * time.Second
	var b strings.Builder
	b.WriteString("| shape | phase | depth | bytes | time | ns/level | allocs | allocs/level | B | peak heap |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, shape := range deepCallShapes {
		maxDepth := deepCallMaxDepth(shape)
		depths := []int{}
		for d := 1; d < maxDepth; d *= 2 {
			depths = append(depths, d)
		}
		depths = append(depths, maxDepth)
		for _, phase := range deepCallPhases {
			for _, d := range depths {
				q := deepCall(shape, d)
				c := measureDeepCall(phase.run, q)
				fmt.Fprintf(&b, "| %s | %s | %d | %d | %s | %d | %d | %.1f | %d | %d |\n", shape.name, phase.name, d, len(q),
					c.took.Round(time.Microsecond), c.took.Nanoseconds()/int64(d), c.allocs, float64(c.allocs)/float64(d), c.bytes,
					c.peakHeapAboveRun)
				if c.took > budget {
					fmt.Fprintf(&b, "| %s | %s | >%d | | over the %s budget: deeper rows not run | | | | | |\n", shape.name, phase.name, d, budget)
					break
				}
			}
		}
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkDeepCalls times each phase over each shape at a few depths: ns/op divided by the depth is the cost of
// one level.
func BenchmarkDeepCalls(b *testing.B) {
	for _, shape := range deepCallShapes {
		for _, phase := range deepCallPhases {
			for _, d := range []int{16, 256, 2048} {
				q := deepCall(shape, d)
				b.Run(fmt.Sprintf("%s/%s/depth=%d", shape.name, phase.name, d), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						phase.run(q)
					}
				})
			}
		}
	}
}
