package queryplan

import (
	"bytes"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The allocations of Build, by the shape of the query: the plan (with the first selector, range and vector node
// inside it) and the key, plus one for each further node. The numbers are locks: a change that adds an allocation
// to the common path fails here, and one that removes some lowers the number.
var allocShapes = []struct {
	name, q string
	allocs  float64
}{
	{"selector", `{app="x", env=~"p.*"}`, 2},
	{"pipeline", `{app="x"} |= "e" | json | status>=500 | drop a | line_format "{{.m}}"`, 2},
	{"range aggregation", `rate({app="x"} | json [5m])`, 2},
	{"range aggregation with offset and grouping", `quantile_over_time(0.9, {app="x"} | unwrap n [5m] offset 1h) by (a)`, 2},
	{"vector aggregation", `sum by (app) (rate({app="x"} |= "e" [5m]))`, 2},
	{"rank", `topk(3, sum by (app) (rate({app="x"}[5m])))`, 3},
	{"scalar", `1 + 2`, 5},
	{"binary operation", `sum(rate({a="x"}[5m])) / on (a) group_left sum(rate({a="y"}[5m]))`, 6},
	{"opaque", `label_replace(rate({a="x"}[5m]), "a", "b", "c", "d")`, 3},
}

func TestBuildAllocLocks(t *testing.T) {
	if raceBuild() || testing.CoverMode() != "" {
		t.Skip("the race detector and coverage change allocation counts")
	}
	in := Inputs{Kind: KindRange, StreamLabels: []string{"app", "env"}}
	for _, c := range allocShapes {
		t.Run(c.name, func(t *testing.T) {
			e, err := logql.Parse(c.q)
			if err != nil {
				t.Fatal(err)
			}
			got := testing.AllocsPerRun(200, func() { _, _ = Build(e, in) })
			if got > c.allocs {
				t.Errorf("Build allocates %.0f, the lock is %.0f", got, c.allocs)
			}
			if got < c.allocs {
				t.Logf("Build allocates %.0f, below the lock of %.0f: lower the lock", got, c.allocs)
			}
			p, _ := Build(e, in)
			dst := make([]byte, 0, 2*len(p.Key))
			if got := testing.AllocsPerRun(200, func() { dst = p.AppendKey(dst[:0]) }); got != 0 {
				t.Errorf("AppendKey allocates %.0f into a buffer with room", got)
			}
			if !bytes.Equal(dst, p.Key) {
				t.Errorf("AppendKey differs from Key")
			}
		})
	}
}

// AppendKey appends to what dst holds and grows it only when it must.
func TestAppendKey(t *testing.T) {
	p := mustBuild(t, `sum(rate({a="b"}[1m]))`, Inputs{})
	if got := p.AppendKey([]byte("prefix:")); string(got[:7]) != "prefix:" || !bytes.Equal(got[7:], p.Key) {
		t.Errorf("AppendKey = %q", got)
	}
	if got := p.AppendKey(nil); !bytes.Equal(got, p.Key) || &got[0] == &p.Key[0] {
		t.Errorf("AppendKey(nil) shares the plan's key or differs from it")
	}
}

// Build neither changes the Expr nor keeps a hold on anything it writes to: a second build of the same tree, and a
// build of a copy of it, give the same plan.
func TestBuildLeavesTheExprAlone(t *testing.T) {
	texts, exprs := corpus(t)
	in := Inputs{Kind: KindRange, StreamLabels: []string{"app"}}
	for i, e := range exprs {
		if i%3 != 0 {
			continue
		}
		fresh, err := parseCanon(texts[i])
		if err != nil {
			t.Fatal(err)
		}
		a, err := Build(e, in)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(e, fresh) {
			t.Fatalf("%q: Build changed the Expr", texts[i])
		}
		b, _ := Build(e, in)
		c, _ := Build(fresh, in)
		if !bytes.Equal(a.Key, b.Key) || !bytes.Equal(a.Key, c.Key) {
			t.Fatalf("%q: two builds, two keys", texts[i])
		}
		if a == b || a.Root == b.Root {
			t.Fatalf("%q: two builds share a plan", texts[i])
		}
		// Writing into one plan's key does not reach the other's: the scratch buffer is not shared.
		a.Key[0] ^= 0xff
		if b.Key[0] == a.Key[0] {
			t.Fatalf("%q: two plans share their key bytes", texts[i])
		}
	}
}

// Many goroutines build the same Expr and others at once: no data race, and the same key every time.
func TestBuildConcurrent(t *testing.T) {
	texts, exprs := corpus(t)
	in := Inputs{Kind: KindRange, StreamLabels: []string{"app"}}
	want := make([][]byte, len(exprs))
	for i, e := range exprs {
		p, err := Build(e, in)
		if err != nil {
			t.Fatal(err)
		}
		want[i] = p.Key
	}
	var wg sync.WaitGroup
	errs := make(chan string, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for round := 0; round < 3; round++ {
				for i := g; i < len(exprs); i += 5 {
					p, err := Build(exprs[i], in)
					if err != nil || !bytes.Equal(p.Key, want[i]) {
						select {
						case errs <- texts[i]:
						default:
						}
						return
					}
					checkSubtreeKeys(p)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for q := range errs {
		t.Errorf("a concurrent build of %q differs", q)
	}
}

// checkSubtreeKeys walks a plan's nodes reading each key, as concurrent readers of a shared plan do.
func checkSubtreeKeys(p *Plan) {
	var walk func(n Node)
	walk = func(n Node) {
		_ = n.Props().Key
		switch x := n.(type) {
		case *RangeAgg:
			walk(x.In)
		case *VectorAgg:
			walk(x.In)
		case *Rank:
			walk(x.In)
		case *BinOp:
			walk(x.L)
			walk(x.R)
		}
	}
	walk(p.Root)
}

// A crafted query costs time and memory in proportion to its size: a deep or wide one builds into a plan of a
// number of nodes, bytes and allocations that grows with the query, with a bound a constant per node.
func TestBuildResourceBounds(t *testing.T) {
	if raceBuild() || testing.CoverMode() != "" {
		t.Skip("the race detector and coverage change allocation counts")
	}
	r := `rate({a="b"}[1m])`
	shapes := []struct {
		name  string
		build func(n int) string
		nodes func(n int) int
	}{
		{"nested aggregations", func(n int) string { return strings.Repeat("sum(", n) + r + strings.Repeat(")", n) }, func(n int) int { return n + 2 }},
		{"left binary chain", func(n int) string { return strings.Repeat(r+" + ", n) + "1" }, func(n int) int { return 3*n + 1 }},
		{"right binary chain", func(n int) string { return strings.Repeat("1 + (", n) + "1" + strings.Repeat(")", n) }, func(n int) int { return 2*n + 1 }},
		{"matchers", func(n int) string { return `{` + strings.Repeat(`a="b",`, n) + `a="b"}` }, func(n int) int { return 1 }},
		{"stages", func(n int) string { return `{a="b"}` + strings.Repeat(` | json`, n) }, func(n int) int { return 1 }},
		{"long value", func(n int) string { return `{a="` + strings.Repeat("v", n) + `"}` }, func(n int) int { return 1 }},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			// The largest input that fits under Loki's input size (the parser rejects a longer query), and a quarter.
			unit := len(s.build(2)) - len(s.build(1))
			large := (logql.MaxInputSize - 1 - len(s.build(0))) / unit
			var perUnit [2]float64
			for i, n := range []int{large / 4, large} {
				q := s.build(n)
				e, err := logql.Parse(q)
				if err != nil {
					t.Fatalf("n=%d: %v", n, err)
				}
				var p *Plan
				allocs := testing.AllocsPerRun(3, func() { p, _ = Build(e, Inputs{}) })
				if p == nil {
					t.Fatalf("n=%d: no plan", n)
				}
				nodes := countNodes(p.Root)
				if nodes < s.nodes(n)-2 || nodes > s.nodes(n)+2 {
					t.Errorf("n=%d: %d nodes, want about %d", n, nodes, s.nodes(n))
				}
				// One allocation per node beyond the first of each kind, the plan and the key; the key buffer of a
				// query larger than the pool keeps grows by doubling, so a logarithmic number more.
				if limit := float64(2*nodes + 40); allocs > limit {
					t.Errorf("n=%d: %.0f allocations for %d nodes", n, allocs, nodes)
				}
				if limit := 8*len(q) + 64*nodes + 4096; len(p.Key) > limit {
					t.Errorf("n=%d: key of %d bytes for a query of %d", n, len(p.Key), len(q))
				}
				perUnit[i] = allocs / float64(n)
			}
			// Linear, not quadratic: four times the input costs about four times the allocations.
			if perUnit[0] > 0 && perUnit[1] > 1.5*perUnit[0]+0.001 {
				t.Errorf("allocations per unit grew from %.4f to %.4f", perUnit[0], perUnit[1])
			}
		})
	}
}
