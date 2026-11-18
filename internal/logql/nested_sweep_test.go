package logql

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// nestedSweepPhases are the steps of the request path measured by the nesting sweep.
var nestedSweepPhases = []struct {
	name string
	run  func(q string)
}{
	{"canonicalize", func(q string) { _ = CanonicalizeLogRanges(q) }},
	{"parse", func(q string) { _, _ = parseForValidation(q) }},
	{"validate", func(q string) { _ = ValidateLogQL(q) }},
}

// nestedSweepCost runs f once and returns its time, allocations, bytes and the highest heap seen while it ran.
func nestedSweepCost(f func()) (elapsed time.Duration, allocs, bytes, peak uint64) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var high atomic.Uint64
	high.Store(before.HeapAlloc)
	done := make(chan struct{})
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Millisecond):
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > high.Load() {
					high.Store(m.HeapAlloc)
				}
			}
		}
	}()
	start := time.Now()
	f()
	elapsed = time.Since(start)
	close(done)
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > high.Load() {
		high.Store(after.HeapAlloc)
	}
	return elapsed, after.Mallocs - before.Mallocs, after.TotalAlloc - before.TotalAlloc, high.Load() - before.HeapAlloc
}

// TestNestedCallsCostSweep prints, with LVP_NESTED_SWEEP=1, a markdown table of what each phase costs per depth of
// every nested shape, up to the deepest nesting under the input size limit (the median time of three runs). A
// phase stops at the first depth that takes more than LVP_NESTED_SWEEP_BUDGET (default 2s).
func TestNestedCallsCostSweep(t *testing.T) {
	if os.Getenv("LVP_NESTED_SWEEP") != "1" {
		t.Skip("set LVP_NESTED_SWEEP=1")
	}
	budget := 2 * time.Second
	if v, err := time.ParseDuration(os.Getenv("LVP_NESTED_SWEEP_BUDGET")); err == nil {
		budget = v
	}
	// LVP_NESTED_SWEEP_PHASES and LVP_NESTED_SWEEP_DEPTHS (comma-separated) narrow the sweep, for a build whose cost
	// doubles per level: such a phase is run at chosen depths only.
	phases := os.Getenv("LVP_NESTED_SWEEP_PHASES")
	var depths []int
	for _, f := range strings.Split(os.Getenv("LVP_NESTED_SWEEP_DEPTHS"), ",") {
		if d, err := strconv.Atoi(strings.TrimSpace(f)); err == nil {
			depths = append(depths, d)
		}
	}
	var b strings.Builder
	b.WriteString("| shape | depth | bytes | phase | time | allocs | B | peak heap |\n|---|---:|---:|---|---:|---:|---:|---:|\n")
	for _, s := range nestedDepthShapes {
		for _, ph := range nestedSweepPhases {
			if phases != "" && !strings.Contains(","+phases+",", ","+ph.name+",") {
				continue
			}
			sweep := nestedDepthSweep(s)
			if depths != nil {
				sweep = depths
			}
			for _, d := range sweep {
				q := s.query(d)
				var times []time.Duration
				var allocs, bytes, peak uint64
				for i := 0; i < 3; i++ {
					var el time.Duration
					el, allocs, bytes, peak = nestedSweepCost(func() { ph.run(q) })
					times = append(times, el)
					if el > budget {
						break
					}
				}
				sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
				med := times[len(times)/2]
				fmt.Fprintf(&b, "| %s | %d | %d | %s | %v | %d | %d | %d |\n", s.name, d, len(q), ph.name, med.Round(time.Microsecond), allocs, bytes, peak)
				if med > budget {
					fmt.Fprintf(&b, "| %s | >%d | | %s | over the %v budget: deeper rows not run | | | |\n", s.name, d, ph.name, budget)
					break
				}
			}
		}
	}
	t.Log("\n" + b.String())
}
