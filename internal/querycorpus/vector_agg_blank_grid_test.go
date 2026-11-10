package querycorpus_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// blankPaddedVectorQueries pads the vector aggregation spellings with blanks, tabs and newlines at every place two
// tokens meet (and widens each blank the query already has), to measure which paddings the string translator reads
// unlike the plan.
func blankPaddedVectorQueries() []string {
	bases := []string{
		`sum by (app) (rate({app="a"}[5m]))`,
		`sum(count_over_time({app="a"} |= "x"[5m])) by (app, env)`,
		`count without (pod) (bytes_over_time({app="a"}[1m] offset 5m))`,
		`avg(bytes_rate({app="a"}[5m]))`,
		`sum without (a, b) (rate({app="a"} != "y" [5m]))`,
		`min by (detected_level) (count_over_time({app="a"}[5m]))`,
		`sum(rate({app="a"}[5m])) without (pod)`,
		`max by (service_name) (absent_over_time({app="a"}[5m]))`,
	}
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	pads := []string{" ", "\t", "\n", "  ", " \t "}
	for _, b := range bases {
		add(b)
		// positions: every boundary outside a quoted string
		inQuote := false
		var cuts []int
		for i := 0; i <= len(b); i++ {
			if i < len(b) && b[i] == '"' {
				inQuote = !inQuote
			}
			if inQuote || i == 0 || i == len(b) {
				continue
			}
			prev, cur := b[i-1], b[i]
			if strings.ContainsRune("()[]{},=", rune(prev)) || strings.ContainsRune("()[]{},=", rune(cur)) || prev == ' ' || cur == ' ' {
				cuts = append(cuts, i)
			}
		}
		for _, i := range cuts {
			for _, p := range pads {
				add(b[:i] + p + b[i:])
			}
		}
		// leading and trailing blanks, and two paddings at once
		for _, p := range pads {
			add(p + b)
			add(b + p)
		}
		for x, i := range cuts {
			for _, j := range cuts[x+1:] {
				for _, p := range []string{" ", "\t", "\n"} {
					add(b[:i] + p + b[i:j] + p + b[j:])
				}
			}
		}
		// every pad at every cut at once
		for _, p := range pads {
			var sb strings.Builder
			last := 0
			for _, i := range cuts {
				sb.WriteString(b[last:i])
				sb.WriteString(p)
				last = i
			}
			sb.WriteString(b[last:])
			add(sb.String())
		}
	}
	return out
}

// TestVectorAggBlankPaddingShadow holds the lowering to the string translator on the blank-padded spellings of
// vector aggregations under every profile: where the text screen lets the plan answer, the text is the translator's
// byte for byte (0 mismatches), and where it turns the query away, the unscreened lowering really differs from the
// translator (the screen delegates no more than it must).
// conformance: architecture-syntax-package-layering
func TestVectorAggBlankPaddingShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := blankPaddedVectorQueries()
	var rows, native, screened int
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
			legacy, ok := prof.translate(q)
			if !ok {
				continue
			}
			rows++
			raw, rerr := lower.Plan(plan, prof.lowerProfile())
			got, lerr := lower.PlanQuery(q, plan, prof.lowerProfile())
			var nl *lower.NotLoweredError
			if errors.As(lerr, &nl) && nl.Construct == "VectorAgg blank padding" {
				screened++
				if rerr == nil && physical.Encode(raw) == legacy {
					t.Errorf("%s: %q: turned away though the texts agree", prof.name, q)
				}
				continue
			}
			if lerr != nil {
				continue
			}
			if text := physical.Encode(got); text != legacy {
				t.Errorf("%s: %q:\n lowered %s\n  string %s", prof.name, q, text, legacy)
				continue
			}
			native++
		}
	}
	t.Logf("%d padded queries x profiles: %d rows, %d lowered, %d turned away by the text screen", len(grid), rows, native, screened)
	if screened == 0 || native == 0 {
		t.Fatalf("the grid misses a side: %d lowered, %d turned away", native, screened)
	}
}
