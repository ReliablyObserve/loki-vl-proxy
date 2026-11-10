package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// rangeAggGrid is the generated grid of range aggregations over log queries with no parser: each function the
// lowering writes natively and those it leaves to the string translator, over every selector shape, line filter
// and label filter of the log query grids, with the range and offset spellings and the parenthesised inner query.
// Deterministic; the lowering and the string translator are held to each other on every row under every profile
// (TestRangeAggGridShadow).
func rangeAggGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	selectors := []string{
		`{app="a"}`, `{app="a", env="prod"}`, `{app!="a"}`, `{app=~"a|b"}`, `{app!~"a.*"}`, `{app=~".+"}`, `{app=~".*"}`,
		`{app=""}`, `{app!=""}`, `{service_name="s"}`, `{service_name=~"s|t"}`, `{service_name!="s"}`, `{service_name=""}`,
		`{detected_level="error"}`, `{detected_level=~"error|warn"}`, `{detected_level!="info"}`, `{k8s.pod.name="p"}`, `{k8s_pod_name="p"}`,
		`{a="1", b=~"x.*", c!="y", d!~"z|w"}`, `{__error__=""}`, `{level="error"}`, `{namespace="prod", pod=~"api-.+"}`,
	}
	lines := []string{
		``, ` |= "x"`, ` != "x"`, ` |~ "x.+y"`, ` !~ "x|y"`, ` |= "a" |= "b" != "c"`, ` |= "x" or "y"`, ` != "x" or "y"`,
		` |= ip("10.0.0.0/8")`, ` != ip("10.0.0.1-10.0.0.5")`, ` |= ip("::1")`, ` |> "<_> x <_> y"`, ` !> "<_> x <_> y"`,
		` |> "<_> x <_> y" |= "k"`, ` |= "k" |> "<_> x <_>" != "j"`, ` |= ""`, ` |~ ""`, ` != ""`, ` |= "é\n"`, " |= `raw\\d`",
		` |= "a b"`, ` |~ "(?i)error"`, ` |~ "a|b|c"`, ` |= "[x]"`, ` |= "x[1m]"`,
	}
	ranges := []string{`[5m]`, `[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[1d]`, `[1ms]`, `[1h30m]`, `[300s]`, `[2w]`,
		// the spellings the division of a rate by the range's seconds reads: units apart and together, fractions, days
		// and weeks and years that Go's parser does not know, a range of no length, the shortest and a long one.
		`[1m30s]`, `[90s]`, `[1w1d]`, `[1y]`, `[3d12h]`, `[0s]`, `[1ms]`, `[999ms]`, `[1500ms]`, `[1.5h]`, `[1.1d]`, `[0.5d]`, `[1h1m1s1ms]`, `[10000h]`, `[1ns]`, `[1us]`, `[1µs]`, `[2.5s]`}
	// every selector against every line filter, one function and range each way.
	for _, s := range selectors {
		for _, l := range lines {
			for _, f := range funcs {
				add(fmt.Sprintf(`%s(%s%s[5m])`, f, s, l))
			}
		}
	}
	// every function against every range spelling, on a few queries, parenthesised and not.
	for _, f := range funcs {
		for _, r := range ranges {
			for _, q := range []string{`{app="a"}`, `{app="a"} |= "x"`, `{service_name="s"} != "y"`, `{app="a"} | level="error"`} {
				add(fmt.Sprintf(`%s(%s%s)`, f, q, r))
				add(fmt.Sprintf(`%s((%s)%s)`, f, q, r))
				add(fmt.Sprintf(`%s( %s %s )`, f, q, r))
			}
		}
	}
	// the label filter grid inside the functions the lowering writes, and rate once.
	for i, q := range labelFilterGrid() {
		for _, f := range funcs[:3] {
			add(fmt.Sprintf(`%s(%s[5m])`, f, q))
		}
		if i%7 == 0 {
			add(fmt.Sprintf(`rate(%s[1m] offset 5m)`, q))
		}
	}
	// stages the lowering leaves out, by function.
	for _, f := range funcs {
		for _, st := range []string{
			` | json`, ` | logfmt`, ` | json | level="error"`, ` | logfmt | drop foo`, ` | line_format "{{.x}}"`, ` | label_format a=b`,
			` | keep a`, ` | unwrap x`, ` | json | unwrap x`, ` | regexp "(?P<a>x)"`, ` | pattern "<a> <b>"`, ` | unpack`, ` | decolorize`,
			` | drop a`, ` | json a="b"`, ` | level="error" | json`,
		} {
			add(fmt.Sprintf(`%s({app="a"}%s[5m])`, f, st))
		}
	}
	// quoted text the string translator reads as a without clause or a boolean modifier.
	for _, line := range []string{` |= "without (x)"`, " |= `without (z)`", ` |= "a bool b"`, ` |~ "without\\s*\\(a\\)"`, ` |= "without(0)\xa2"`, ` |~ "|||"`, ` |= "a|||b"`} {
		for _, f := range funcs {
			add(fmt.Sprintf(`%s({app="a"}%s[5m])`, f, line))
		}
	}
	// values the string translator sends in the client's spelling (a regexp line filter with an escape), and
	// spellings it quotes as the lowering does.
	for _, in := range []string{
		`{app="a"} |~ "\x41b"`, `{app="a"} !~ "\x41b"`, `{app="a"} |~ "\101b"`, `{app="a"} |~ "\u00e9"`, `{app="a"} |~ "\x41" or "b"`,
		`{app="\x41"}`, "{app=`a`}", `{app="a"} |= "\x41b"`, `{app="a"} | b="\x41"`, `{app="a\tb"}`,
	} {
		for _, f := range funcs {
			add(fmt.Sprintf(`%s(%s [5m])`, f, in))
		}
	}
	// functions that read an unwrapped value, and groupings.
	for _, q := range []string{
		`sum_over_time({app="a"} | unwrap x [5m])`, `avg_over_time({app="a"} | unwrap x [5m]) by (pod)`, `quantile_over_time(0.9, {app="a"} | unwrap x [5m])`,
		`count_over_time({app="a"}[5m]) by (x)`, `count_over_time({app="a"}[5m]) by ()`, `count_over_time({app="a"}[5m]) without (x)`,
		`bytes_over_time({app="a"}[5m]) without (x, y)`, `absent_over_time({app="a"}[5m]) without ()`, `rate({app="a"}[5m]) by (x)`,
		`first_over_time({app="a"} | unwrap x [5m])`, `rate_counter({app="a"} | unwrap x [5m])`, `stddev_over_time({app="a"} | unwrap x [5m])`,
		`count_over_time({app="a"}[5m]) offset 1h`, `count_over_time({app="a"} |= "x" [5m]) > 5`,
	} {
		add(q)
	}
	return out
}

// TestRangeAggGridShadow holds the lowering of range aggregations to the string translator on the generated grid
// under every profile: 0 mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestRangeAggGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := rangeAggGrid()
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
