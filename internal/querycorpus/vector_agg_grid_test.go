package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// vectorAggGrid is the generated grid of vector aggregations over the range aggregations the lowering writes
// natively: every aggregation (sum, count, min, max, avg, and stddev and stdvar, which the lowering leaves to the
// string translator) with every grouping spelling (none, by and without, labels that are renamed, derived
// (service_name, detected_level), repeated or dotted), in both positions of the clause, over each range function,
// a few selectors, line filters and ranges, plus the text a translator regexp could read as syntax and the shapes
// that stay delegated. Deterministic; the lowering and the string translator are held to each other on every row
// under every profile (TestVectorAggGridShadow).
func vectorAggGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	ops := []string{"sum", "count", "min", "max", "avg", "stddev", "stdvar"}
	groupings := []string{
		``, `by ()`, `by (app)`, `by (app, app)`, `by (app, env)`, `by (level)`, `by (detected_level)`, `by (service_name)`,
		`by (app, service_name)`, `by (service_name, detected_level)`, `by (k8s.pod.name)`, `by (k8s_pod_name)`, `by (_stream)`,
		`by (app, level, detected_level)`, `by (__error__)`, `by (a,b,c)`, `by ( app )`,
		`without (app)`, `without (a, b)`, `without (a, a)`, `without ()`, `without (k8s.pod.name)`, `without (service_name)`,
		`without (detected_level)`, `without (level)`,
	}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	selectors := []string{`{app="a"}`, `{app=~".*"}`, `{service_name="s"}`, `{detected_level="error"}`, `{app!="a", env=~"p.*"}`, `{k8s.pod.name="p"}`}
	inner := func(f, sel, line, rng string) string { return fmt.Sprintf(`%s(%s%s%s)`, f, sel, line, rng) }
	wrap := func(op, g, in string) {
		switch {
		case g == "":
			add(fmt.Sprintf(`%s(%s)`, op, in))
		default:
			add(fmt.Sprintf(`%s %s (%s)`, op, g, in))
			add(fmt.Sprintf(`%s(%s) %s`, op, in, g))
		}
	}
	// every aggregation x grouping x function, on the plain selector.
	for _, op := range ops {
		for _, g := range groupings {
			for _, f := range funcs {
				wrap(op, g, inner(f, `{app="a"}`, ``, `[5m]`))
			}
		}
	}
	// every selector and line filter, on the lowered aggregations and a few groupings.
	for _, sel := range selectors {
		for _, line := range []string{``, ` |= "x"`, ` != "x" |~ "y.+"`, ` |> "<_> x"`, ` | status >= 500`, ` | status="200" |= "k"`} {
			for _, op := range []string{"sum", "count", "max", "avg"} {
				for _, g := range []string{``, `by (app)`, `by (service_name)`, `by (detected_level)`, `without (app)`} {
					for _, f := range funcs {
						wrap(op, g, inner(f, sel, line, `[5m]`))
					}
				}
			}
		}
	}
	// the range spellings the rate's division reads, and offsets.
	for _, rng := range []string{`[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[1d]`, `[1ms]`, `[1h30m]`, `[2w]`, `[0s]`, `[1.5h]`, `[1y]`} {
		for _, op := range []string{"sum", "count", "min", "stddev"} {
			for _, g := range []string{``, `by (app)`, `without (app)`} {
				for _, f := range []string{"rate", "bytes_rate", "count_over_time"} {
					wrap(op, g, inner(f, `{app="a"}`, ``, rng))
				}
			}
		}
	}
	// text a translator regexp reads as syntax: a line filter that spells a grouping, a stats pipe, a derived name.
	for _, line := range []string{
		` |= "without (x)"`, ` |= "by (x)"`, ` |= "| stats by (x)"`, ` |= "| stats "`, ` |= "service_name"`, ` |= "detected_level"`, ` |= "a bool b"`, ` |~ "|||"`, ` |= "a|||b"`,
		` |= "sum by (x) (rate(y))"`, ` |= "__without__:sum:app"`, ` |~ "without\\s*\\((a|b)\\)"`, " |= `without (z)`",
	} {
		for _, op := range []string{"sum", "count", "avg"} {
			for _, g := range []string{``, `by (app)`, `without (app)`} {
				for _, f := range []string{"count_over_time", "rate"} {
					wrap(op, g, inner(f, `{app="a"}`, line, `[5m]`))
				}
			}
		}
	}
	// the stages the log query lowering leaves out, and the shapes that stay delegated.
	for _, st := range []string{` | json`, ` | logfmt`, ` | json | level="error"`, ` | line_format "{{.x}}"`, ` | drop a`, ` | keep a`, ` | unwrap x`,
		` | detected_level="error"`, ` | detected_level=~"error|warn"`, ` | service_name="s"`, ` | label_format a=b`, ` | regexp "(?P<a>x)"`} {
		for _, op := range []string{"sum", "count", "max"} {
			for _, g := range []string{``, `by (app)`, `without (app)`} {
				wrap(op, g, fmt.Sprintf(`rate({app="a"}%s[5m])`, st))
				wrap(op, g, fmt.Sprintf(`count_over_time({app="a"}%s[5m])`, st))
			}
		}
	}
	for _, q := range []string{
		`sum by (app) (sum_over_time({app="a"} | unwrap n [5m]))`, `sum(avg_over_time({app="a"} | unwrap n [5m]) by (pod))`,
		`sum(quantile_over_time(0.9, {app="a"} | unwrap n [5m]))`,
		`sum(sum by (app) (rate({app="a"}[5m])))`, `max by (app) (sum by (app, env) (count_over_time({app="a"}[5m])))`,
		`sum without (a) (sum without (b) (rate({app="a"}[5m])))`, `count(sum by (app) (rate({app="a"}[5m]))) by (env)`,
		`topk(3, sum by (app) (rate({app="a"}[5m])))`, `bottomk(2, rate({app="a"}[5m]))`, `sort(sum by (app) (rate({app="a"}[5m])))`,
		`sort_desc(count_over_time({app="a"}[5m]))`, `sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`,
		`sum by (app) (rate({app="a"}[5m])) > 1`, `sum(rate({app="a"}[5m])) + 2`, `label_replace(sum by (app) (rate({app="a"}[5m])), "x", "$1", "app", "(.*)")`,
		`sum(rate({app="a"}[5m] offset 1h))`, `sum without (x) (rate({x!="  "}[5m]))`, `count without (x) (count_over_time({app="a"} |= "a  b"[5m]))`,
		`sum by (x) (rate({x!="  "}[5m]))`, `sum by ( bool ) (rate({app="a"}[5m]))`, `sum by (a, bool , b) (count_over_time({app="a"}[5m]))`,
		`count(rate({app="a"}[5m])) by ( bool )`, `avg by (bool) (bytes_rate({app="a"}[5m]))`, `sum without ( bool ) (rate({app="a"}[5m]))`, `sum without (x) (rate({app="a"} |= "without(0)\xa2"[5m]))`,
		`count_values("v", count_over_time({app="a"}[5m]))`, `sum by (app) (count_over_time({app="a"}[5m]) by (x))`,
		`sum(vector(1))`, `sum(rate({app="a"}[5m])) by (app) by (env)`,
		`avg without (a) (bytes_rate({app="a"} |= "x" [1m]))`, `sum without (a) (count_over_time({app="a"}[5m])) offset 1h`,
	} {
		add(q)
	}
	return out
}

// TestVectorAggGridShadow holds the lowering of vector aggregations to the string translator on the generated grid
// under every profile: 0 mismatches, the text byte for byte, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestVectorAggGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := vectorAggGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 2000))
	}
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
