package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/translatorspelling"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// blankPaddedVectorQueries pads the vector aggregation spellings with blanks, tabs and newlines at every place two
// tokens meet (and widens each blank the query already has), to measure which paddings the string translator reads
// unlike the plan.
func blankPaddedVectorQueries() []string {
	return blankPadded([]string{
		`sum by (app) (rate({app="a"}[5m]))`,
		`sum(count_over_time({app="a"} |= "x"[5m])) by (app, env)`,
		`count without (pod) (bytes_over_time({app="a"}[1m] offset 5m))`,
		`avg(bytes_rate({app="a"}[5m]))`,
		`sum without (a, b) (rate({app="a"} != "y" [5m]))`,
		`(sum by (app) (rate({app="a"}[5m])))`,
		`sum((rate({app="a"}[5m])))`,
		`min by (detected_level) (count_over_time({app="a"}[5m]))`,
		`sum(rate({app="a"}[5m])) without (pod)`,
		`max by (service_name) (absent_over_time({app="a"}[5m]))`,
	})
}

// blankPaddedRangeQueries pads the spellings of a range aggregation at the top of a query, as an operand of a
// binary operation and as the first argument of a label_replace call, the same way: the blank before the parenthesis
// of the call (`rate ({app="a"}[5m])`) is a phrase to the string translator.
func blankPaddedRangeQueries() []string {
	return blankPadded([]string{
		`rate({app="a"}[5m])`,
		`count_over_time({app="a"} |= "x"[5m] offset 1h)`,
		`bytes_over_time({app="a"} != "y" [1m])`,
		`absent_over_time({app="a"}[5m])`,
		`bytes_rate({app="a"}[5m])`,
		`sum_over_time({app="a"} | json | unwrap x [5m])`,
		`quantile_over_time(0.5, {app="a"} | json | unwrap x [5m])`,
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")`,
		`rate({app="a"}[5m]) / rate({app="b"}[5m])`,
		`2 * count_over_time({app="a"}[5m])`,
		`label_replace(count_over_time({app="a"}[5m]) / 2, "d", "$1", "app", "(.*)")`,
	})
}

// blankPadded pads each base at every place two tokens meet.
func blankPadded(bases []string) []string {
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

// shadowBlankGrid holds the lowering to the string translator on a grid of blank-padded spellings under every
// profile, as the handlers read each spelling (handlerText: the request's rewrite writes the metric calls and
// parentheses in one spelling): every spelling lowers, and the text is the translator's text of what the handlers
// translate byte for byte (0 mismatches), or a spelling the translator reads otherwise than the plan that
// translatorspelling names (a blank around a binary operator, which the rewrite keeps). It returns the rows, the rows
// lowered to the translator's text, the rows of such a spelling, and the rows whose spelling as written the
// translator reads otherwise (respelled: the plan and the rewrite read them as Loki does, the translator alone would
// not).
func shadowBlankGrid(t *testing.T, grid []string) (rows, native, known, respelled int) {
	t.Helper()
	for _, q := range grid {
		h := handlerText(q)
		if rejectedByValidation(h) {
			continue // Loki's parse error, answered before anything is planned or translated
		}
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(h))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		for _, prof := range planProfiles() {
			legacy, ok := prof.translate(h)
			if !ok {
				continue
			}
			rows++
			got, err := lower.PlanQuery(h, plan, prof.lowerProfile())
			if err != nil {
				t.Errorf("%s: %q: not lowered: %v", prof.name, q, err)
				continue
			}
			text := physical.Encode(got)
			if raw, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.labelFn, prof.stream, prof.caps); err != nil || raw != legacy {
				respelled++
			}
			if text != legacy {
				if translatorspelling.Case(h, plan) != "" {
					known++
					continue
				}
				t.Errorf("%s: %q:\n lowered %s\n  string %s", prof.name, q, text, legacy)
				continue
			}
			native++
		}
	}
	return rows, native, known, respelled
}

// TestVectorAggBlankPaddingShadow holds the lowering to the string translator on the blank-padded spellings of
// vector aggregations under every profile (shadowBlankGrid).
// conformance: architecture-syntax-package-layering
func TestVectorAggBlankPaddingShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := blankPaddedVectorQueries()
	rows, native, known, respelled := shadowBlankGrid(t, grid)
	t.Logf("%d padded queries x profiles: %d rows, %d lowered to the translator's text, %d of a spelling it reads otherwise, %d it would read otherwise as written", len(grid), rows, native, known, respelled)
	if respelled == 0 || native == 0 {
		t.Fatalf("the grid misses a side: %d lowered, %d respelled", native, respelled)
	}
}

// TestRangeCallBlankSpellingShadow is the same for a range aggregation at the top of a query, as an operand of a
// binary operation and as the argument of a label_replace call: a blank before the call's parenthesis, which the
// translator read as a phrase, is respelled by the request's rewrite, and the lowering writes the translator's text of
// the respelled query byte for byte.
// conformance: semantics/translator-reads-blank-before-range-call-as-text
func TestRangeCallBlankSpellingShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := blankPaddedRangeQueries()
	rows, native, known, respelled := shadowBlankGrid(t, grid)
	t.Logf("%d padded queries x profiles: %d rows, %d lowered to the translator's text, %d of a spelling it reads otherwise, %d it would read otherwise as written", len(grid), rows, native, known, respelled)
	if respelled == 0 || native == 0 {
		t.Fatalf("the grid misses a side: %d lowered, %d respelled", native, respelled)
	}
}

// TestRangeCallBlankSpellingMatchesLoki records what Loki does with a blank before the parenthesis of a range
// function: its parser accepts `rate ({app="a"}[5m])` (also with a tab or a newline, for every range function, as an
// operand of a binary operation and inside label_replace) and reads the same function as without the blank. The
// string translator writes the text as a phrase search, so the difference is the proxy's
// (semantics/translator-reads-blank-before-range-call-as-text). LVP_RUN_ORACLES=1 runs it (it builds the oracle).
// conformance: semantics/translator-reads-blank-before-range-call-as-text
func TestRangeCallBlankSpellingMatchesLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	t.Cleanup(querycorpus.PauseRecording())
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	// Loki must accept each of these, whatever the grid holds.
	must := []string{
		`rate ({app="a"}[5m])`, "rate\t({app=\"a\"}[5m])", "rate\n({app=\"a\"}[5m])", `count_over_time ( {app="a"}[5m])`,
		`bytes_over_time  ({app="a"}[5m])`, `bytes_rate ({app="a"}[5m])`, `absent_over_time ({app="a"}[5m])`,
		`sum_over_time ({app="a"} | json | unwrap x [5m])`, `quantile_over_time (0.5, {app="a"} | json | unwrap x [5m])`,
		`rate ({app="a"}[5m]) / 2`, `1 + rate ({app="a"}[5m])`, `label_replace(rate ({app="a"}[5m]), "d", "$1", "app", "(.*)")`,
	}
	type row struct {
		q     string
		outer string
		must  bool
	}
	var rows []row
	in := filepath.Join(work, "facts-in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	seen := map[string]bool{}
	for _, q := range append(slices.Clone(must), blankPaddedRangeQueries()...) {
		if seen[q] {
			continue
		}
		seen[q] = true
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		// The operation Loki reports for the query: the range function at the top (under label_replace calls), none
		// for a binary operation.
		x := p.Root
		for {
			lr, ok := x.(*queryplan.LabelReplace)
			if !ok {
				break
			}
			x = lr.In
		}
		var outer string
		if ra, ok := x.(*queryplan.RangeAgg); ok {
			outer = string(ra.Op)
		}
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(rows)), "q": q})
		w.Write(append(b, '\n'))
		rows = append(rows, row{q, outer, slices.Contains(must, q)})
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	out := filepath.Join(work, "facts.jsonl")
	if err := oraclerun.Run(bin, "facts", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	accepted, mustSeen := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf struct {
			ID     string `json:"id"`
			Accept bool   `json:"accept"`
			Outer  string `json:"outer"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		r := rows[i]
		if r.must {
			mustSeen++
			if !lf.Accept {
				t.Errorf("%q: Loki rejects it: %s", r.q, lf.Error)
			}
		}
		if !lf.Accept {
			continue
		}
		accepted++
		if lf.Outer != r.outer {
			t.Errorf("%q: Loki reads %q, the plan %q", r.q, lf.Outer, r.outer)
		}
	}
	if mustSeen != len(must) || accepted < len(must) {
		t.Fatalf("%d of %d spellings checked, %d accepted", mustSeen, len(must), accepted)
	}
	t.Logf("%d spellings: Loki accepts %d and reads the same function as the plan", len(rows), accepted)
}

// The VictoriaLogs oracle's typed-selector pairs are made on the text the handlers translate: a range aggregation
// spelled with a blank before the function's parenthesis makes the pair of its canonical spelling (the request's
// rewrite writes that spelling before any handler reads the query).
// conformance: semantics/translator-reads-blank-before-range-call-as-text
func TestTypedPairOfBlankBeforeRangeCallIsTheCanonicalOne(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for q, canon := range map[string]string{
		`rate({app="a"}[5m])`:                          `rate({app="a"}[5m])`,
		`count_over_time( {app="a"} |= "x" [5m])`:      `count_over_time({app="a"} |= "x" [5m])`,
		`rate ({app="a"}[5m])`:                         `rate({app="a"}[5m])`,
		"rate\t({app=\"a\"}[5m])":                      `rate({app="a"}[5m])`,
		"count_over_time\n({app=\"a\"} |= \"x\" [5m])": `count_over_time({app="a"} |= "x" [5m])`,
		`bytes_rate  ({app="a"}[5m] offset 1h)`:        `bytes_rate({app="a"}[5m] offset 1h)`,
	} {
		for _, v := range []string{"v1.49.0", "v1.53.0"} {
			typed, built, ok := typedQueryPair(q, v)
			wtyped, wbuilt, wok := typedQueryPair(canon, v)
			if !ok || !wok || typed != wtyped || built != wbuilt {
				t.Errorf("%s: %q: pair %v %q %q, its canonical spelling %q: %v %q %q", v, q, ok, typed, built, canon, wok, wtyped, wbuilt)
			}
		}
	}
}

// The oracle's typed pairs are what VictoriaLogs would receive from the proxy: the text the handlers translate (a
// quantile_over_time phi above 1 is clamped to 1, which VictoriaLogs accepts), no pair for rate_counter (its stats
// function is a marker the proxy answers itself), and, for a ranking the proxy evaluates, the operand's request the
// plan lowers to against the operand request the proxy sends (the whole query is never translated on that route).
// conformance: architecture-syntax-package-layering
func TestTypedPairsAreWhatTheProxySends(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		typed, built, ok := typedQueryPair(`quantile_over_time(2.0, {app="a"} | unwrap duration_ms [5m])`, v)
		if !ok || typed != built || !strings.Contains(typed, "quantile(1,") {
			t.Errorf("%s: phi above 1: pair %v, typed %q, built %q", v, ok, typed, built)
		}
		if _, _, ok := typedQueryPair(`rate_counter({app="a"} | unwrap n [5m])`, v); ok {
			t.Errorf("%s: rate_counter makes a pair", v)
		}
		for _, q := range []string{
			`topk(3, rate({app="a"}[5m]))`, `bottomk(2, count_over_time({app="a"} |= "x" [1m]))`, `sort(bytes_rate({app="a"}[5m]))`,
			`sort_desc(rate({app="a"} | json | unwrap n [5m]))`,
		} {
			typed, built, ok := typedQueryPair(q, v)
			if !ok || typed != built || typed == "" {
				t.Errorf("%s: %q: ranking pair %v\n typed %q\n built %q", v, q, ok, typed, built)
			}
		}
		// A ranking spelled with a blank before its operand's call makes the pair of its canonical spelling, which the
		// request's rewrite gives the handlers.
		typed, built, ok = typedQueryPair(`topk(3, rate ({app="a"}[5m]))`, v)
		wtyped, wbuilt, wok := typedQueryPair(`topk(3, rate({app="a"}[5m]))`, v)
		if !ok || !wok || typed != wtyped || built != wbuilt {
			t.Errorf("%s: a respelled ranking makes the pair %v %q %q, its canonical spelling %v %q %q", v, ok, typed, built, wok, wtyped, wbuilt)
		}
	}
}
