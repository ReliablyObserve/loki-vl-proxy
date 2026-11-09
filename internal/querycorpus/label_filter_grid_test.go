package querycorpus_test

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// labelFilterGrid is the generated grid of log queries with label filters and no parser: every comparison
// operator against every kind and quoting of value, the regexps the simplifier rewrites and the ones it leaves,
// the empty values, ip() patterns, plain, hyphenated and dotted names, and/or/comma trees, and the filters among
// line filters, after a pattern filter that adds pipes and in a row. Deterministic; the lowering and the string
// translator are held to each other on every row under every profile (TestLabelFilterGridShadow).
func labelFilterGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	names := []string{"status", "level", "a_b", "k8s_pod_name", "k8s.pod.name", "a-b", "app", "detected_level", "service_name", "_x", "Foo"}
	strs := []string{
		`"error"`, `"a\"b"`, `"a\\b"`, `"a\nb"`, `"a\tb"`, `"\x41"`, `"é"`, "`raw`", "`a\"b`", "`a\\d`", `"a\d"`, `""`, `"a b"`, `"a|b"`,
		`"5"`, `"A"`, `"a.b"`, `"\u00e9"`, `"with space and \"quote\""`, "`a\nb`",
	}
	regexps := []string{
		`"a|b"`, `"error|warn"`, `".*"`, `"a.*"`, `".*a"`, `".*a.*"`, `"(?i)x"`, `"(?i)x.*"`, `".+"`, `"^a$"`, `"a+"`, `"[ab]"`, `"a|"`,
		`"prod-eu|prod"`, `"api|api-gateway"`, `"a.b"`, `"(a|b)c"`, `"ab|ac"`, `""`, `"x{2}"`, `"\\d+"`, `"a\\.b"`, `"(?s).*"`, `"^$"`, `"a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s"`,
		"`a\\d+`", `"("`,
	}
	numbers := []string{`5`, `5.0`, `-1`, `1e3`, `0`, `0.5`, `5s`, `1m30s`, `1h`, `1KB`, `20MiB`, `1.5GB`, `"5"`, `""`}
	ips := []string{`ip("10.0.0.0/8")`, `ip("10.0.0.1-10.0.0.5")`, `ip("10.0.0.1")`, `ip("::1")`, `ip("fe80::/10")`, `ip("2001:db8::1-2001:db8::9")`, `ip("0.0.0.0/0")`, `ip("192.168.1.0/24")`}
	// every operator against every value, on the two names the corpus uses most.
	for _, n := range []string{"status", "level"} {
		for _, op := range []string{"=", "==", "!="} {
			for _, v := range append(append([]string{}, strs...), numbers...) {
				add(fmt.Sprintf(`{app="a"} | %s%s%s`, n, op, v))
			}
			for _, v := range ips {
				add(fmt.Sprintf(`{app="a"} | %s%s%s`, n, op, v))
			}
		}
		for _, op := range []string{"=~", "!~"} {
			for _, v := range append(append([]string{}, regexps...), strs...) {
				add(fmt.Sprintf(`{app="a"} | %s%s%s`, n, op, v))
			}
		}
		for _, op := range []string{">", ">=", "<", "<="} {
			for _, v := range append(append([]string{}, numbers...), `"abc"`, "`abc`") {
				add(fmt.Sprintf(`{app="a"} | %s%s%s`, n, op, v))
			}
		}
	}
	// every name against a representative of each kind.
	reps := []string{`="error"`, `!="error"`, `=""`, `!=""`, `=="x"`, `=~"a|b"`, `!~"a|b"`, `=~".*"`, `=~"a.*"`, `=~"(?i)x"`, `>5`, `>=1KB`, `<5s`, `<=1.5`, `=5`, `!=5s`, `=ip("10.0.0.0/8")`, `!=ip("10.0.0.1-10.0.0.5")`}
	for _, n := range names {
		for _, r := range reps {
			add(`{app="a"} | ` + n + r)
		}
	}
	// the spaces and parentheses a stage can carry.
	for _, q := range []string{
		`{app="a"} | status = "x"`, `{app="a"} | status   >=   500`, `{app="a"} | (status="x")`, `{app="a"} | ((status="x"))`,
		`{app="a"} | ( status >= 500 )`, `{app="a"} |status="x"`, `{app="a"} | status=~"a|b"`,
	} {
		add(q)
	}
	// and/or/comma trees over a leaf set.
	leaves := []string{`level="error"`, `status>=500`, `a=~"x|y"`, `b!="2"`, `ip_a=ip("10.0.0.0/8")`, `c!~"p.*"`, `d=""`, `level=""`, `e!=""`, `k8s.pod.name="p"`, `f=1m30s`, `g=~".*"`}
	for _, x := range leaves {
		add(`{app="a"} | (` + x + `)`)
		for _, y := range leaves {
			for _, c := range []string{" and ", " or ", ", ", " "} {
				add(`{app="a"} | ` + x + c + y)
			}
			add(`{app="a"} | (` + x + ` or ` + y + `) and ` + leaves[0])
			add(`{app="a"} | ` + x + ` and (` + y + ` or ` + leaves[1] + `)`)
			add(`{app="a"} | (` + x + ` and ` + y + `) or (` + leaves[2] + ` and ` + leaves[3] + `)`)
			add(`{app="a"} | ` + x + ` or ` + y + ` or ` + leaves[4])
			add(`{app="a"} | ` + x + ` and ` + y + ` and ` + leaves[5])
			add(`{app="a"} | ((` + x + `) and (` + y + `))`)
			add(`{app="a"} | ` + x + `, ` + y + ` or ` + leaves[6])
		}
	}
	// the filters among other stages: line filters, a pattern filter that adds pipes, and several in a row.
	reps2 := []string{`level="error"`, `status>=500`, `a=~"x|y"`, `a="1" and b!="2"`, `d=""`, `level=""`, `ip_a=ip("10.0.0.0/8")`, `a!=""`, `x=~"p.*" or y="2"`, `m=1KB`}
	lines := []string{`|= "x"`, `!= "y"`, `|~ "z.+"`, `|> "<_> x <_> y"`, `!> "<_> a <_> b"`, `|> "<_>"`}
	for _, f := range reps2 {
		add(`{app="a"} | ` + f)
		add(`{app="a", env=~"prod|stg"} | ` + f)
		add(`{app!="a", env="x"} | ` + f)
		for _, l := range lines {
			add(`{app="a"} ` + l + ` | ` + f)
			add(`{app="a"} | ` + f + ` ` + l)
			add(`{app="a"} ` + l + ` | ` + f + ` ` + l)
			add(`{app="a"} ` + l + ` | ` + f + ` | ` + reps2[0] + ` ` + l)
		}
		for _, g := range reps2 {
			add(`{app="a"} | ` + f + ` | ` + g)
			add(`{app="a"} |> "<_> x <_> y" | ` + f + ` | ` + g)
			add(`{app="a"} |> "<_> x <_> y" | ` + f + ` |= "k" | ` + g + ` != "j"`)
			add(`{app="a"} |= "k" | ` + f + ` |> "<_> x <_> y" | ` + g)
		}
	}
	for _, q := range []string{
		`{app="a"} | level="error" | status>=500 | app!="x"`,
		`{app="a"} | a="1" | a="2"`, `{app="a"} | a="1" | a!="2"`, `{app="a"} | a>5 | a>5`, `{app="a"} | a=~"x" | a=~"y"`,
		`{app="a"} | level="x" | detected_level="y"`, `{app="a"} | a="1" | b="2" | a="3"`,
		`{app="a"} | a="1" or b="2" | a="3"`, `{app="a"} | a=ip("10.0.0.0/8") | a=ip("10.0.0.0/8")`,
		`{app="a"} | a="1" |= "x" | a="2"`, `{app="a"} | k8s.pod.name="p" | k8s_pod_name="q"`,
		`{app="a"} | a!="x" | a!="x"`, `{app="a"} | a="" | a!=""`,
	} {
		add(q)
	}
	return out
}

// labelFilterShadow holds the lowering to the string translator on the grid under every profile and returns the
// counts: rows both write, rows lowered natively, rows delegated by construct.
func labelFilterShadow(t *testing.T, grid []string) (rows, native int, delegated map[string]int, problems []string) {
	t.Helper()
	delegated = map[string]int{}
	for _, q := range grid {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		for _, prof := range planProfiles() {
			got, lerr := lower.Plan(plan, prof.lowerProfile())
			legacy, ok := prof.translate(q)
			if !ok {
				if lerr == nil {
					problems = append(problems, fmt.Sprintf("%s: %q: lowered, the string translator rejects it", prof.name, q))
				}
				continue
			}
			rows++
			var nl *lower.NotLoweredError
			if errors.As(lerr, &nl) {
				delegated[nl.Construct]++
				continue
			}
			if lerr != nil {
				problems = append(problems, fmt.Sprintf("%s: %q: lowering failed: %v", prof.name, q, lerr))
				continue
			}
			want, err := translator.DecodePhysical(legacy)
			if err != nil {
				t.Fatalf("%s: DecodePhysical(%q): %v", prof.name, legacy, err)
			}
			// The text is the translator's byte for byte (what a flipped query serves), which includes the structure.
			if text := translator.EncodePhysical(got); text != legacy || !physical.Equal(want, got) {
				problems = append(problems, fmt.Sprintf("%s: %q:\n lowered %s\n  string %s", prof.name, q, text, legacy))
				continue
			}
			native++
		}
	}
	return rows, native, delegated, problems
}

// TestLabelFilterGridShadow holds the lowering of label filters to the string translator on the generated grid
// under every profile: 0 mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestLabelFilterGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := labelFilterGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 20))
	}
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
