package lower

import (
	"errors"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// detectedLevelProfiles are the profiles a detected_level filter changes under: the body read (log and metric), the
// stored level fields alone (-detected-level-body-scan=false), the tenant's stream labels and the stream fields.
func detectedLevelProfiles() map[string]Profile {
	noScan := logsql.CapabilitiesFor("v1.53.0")
	noScan.NoDetectedLevelBodyScan = true
	return map[string]Profile{
		"plain":         {Caps: logsql.CapabilitiesFor("v1.53.0")},
		"v1.49":         {Caps: logsql.CapabilitiesFor("v1.49.0")},
		"no body scan":  {Caps: noScan},
		"stream labels": {Caps: logsql.CapabilitiesFor("v1.53.0"), Label: benchStreamLabels},
		"stream fields": {Caps: logsql.CapabilitiesFor("v1.53.0"), StreamFields: map[string]bool{"app": true, "level": true}},
	}
}

// The filters on detected_level the lowering writes are the string translator's text, byte for byte, under every
// profile: the prefilter next to the query's filters or in a filter pipe after a pipe, the chain held back until the
// next stage that is not a level filter (line filters stay in front of it, consecutive filters share one chain), the
// stored level fields alone without the body scan, and a metric row's chain reused by a grouping by detected_level. The rest is delegated under a named construct.
//
// conformance: architecture-syntax-package-layering
func TestDetectedLevelFilterLoweringIsTheTranslatorsText(t *testing.T) {
	native := []string{
		`{app="a"} | detected_level="error"`, `{app="a"} | detected_level!="info"`, `{app="a"} | detected_level=~"error|warn"`,
		`{app="a"} | detected_level!~"debug|trace"`, `{app="a"} | detected_level="unknown"`, `{app="a"} | detected_level="x"`, `{app="a"} | detected_level=~".*"`,
		"{app=\"a\"} | detected_level=`ERROR`", `{app="a"} | (detected_level="warn")`, `{app="a"} |= "x" | detected_level="error" != "y"`,
		`{app="a"} | detected_level="error" | detected_level!="x"`, `{app="a"} | detected_level="error" |= "k" | detected_level!="x"`,
		`{app="a"} | detected_level="error" | app="b" | detected_level!="x"`, `{app="a"} | drop __error__ | detected_level="warn"`,
		`{app="a"} | drop __error__ | app="b" | detected_level="warn" |= "x"`,
		`{app="a"} |> "<_> x <_> y" | detected_level="error"`, `{app="a"} | detected_level="error" |> "<_> x <_> y"`,
		`{app="a"} | detected_level="error" | json | status>=500`, `{app="a"} | detected_level="info" | line_format "{{.app}}"`,
		`{service_name="api"} | detected_level="error"`, `{detected_level="info"} | detected_level!="info"`,
		`count_over_time({app="a"} | detected_level="error" [5m])`, `rate({app="a"} | detected_level!="info" [5m])`,
		`sum by (detected_level) (count_over_time({app="a"} | detected_level=~"error|warn" [1m]))`,
		`sum by (service_name) (rate({app="a"} | detected_level="error" [5m]))`, `sum(count_over_time({app="a"} | detected_level=~"" [5m]))`,
		`sum by (level, detected_level) (count_over_time({app="a"} | detected_level="info" | json | drop __error__ [1m]))`,
		`sum by (detected_level) (rate({app="a"} | detected_level=~"" [5m]))`,
	}
	delegated := map[string]string{
		`{app="a"} | json | detected_level="error"`:                      levelAfterStageConstruct,
		`{app="a"} | logfmt | drop __error__ | detected_level="info"`:    levelAfterStageConstruct,
		`{app="a"} | drop level | detected_level="info"`:                 levelAfterStageConstruct,
		`{app="a"} | line_format "x" | detected_level="info"`:            levelAfterStageConstruct,
		`{app="a"} | detected_level>5`:                                   levelFormConstruct,
		`{app="a"} | detected_level=ip("10.0.0.1")`:                      levelFormConstruct,
		`{app="a"} | detected_level=~"("`:                                levelFormConstruct,
		"{app=\"a\"} | detected_level=``":                                levelFormConstruct,
		`{app="a"} | detected_level="error" or detected_level="warn"`:    levelTreeConstruct,
		`{app="a"} | app="x", detected_level="warn"`:                     levelTreeConstruct,
		`{app="a"} | detected_level.="error"`:                            levelSpellingConstruct,
		`{app="a"} | detected_level=""`:                                  levelPresenceConstruct,
		`{app="a"} | detected_level!=""`:                                 levelPresenceConstruct,
		`sum(count_over_time({app="a"} | detected_level="" [5m]))`:       "VectorAgg " + levelPresenceConstruct,
		`count_over_time({app="a"} | json | detected_level="info" [1m])`: "RangeAgg " + levelAfterStageConstruct,
	}
	for name, prof := range detectedLevelProfiles() {
		for _, q := range native {
			p := rangeAggPlan(q)
			if p == nil {
				t.Fatalf("%q does not parse", q)
			}
			op, err := PlanQuery(q, p, prof)
			if err != nil {
				t.Errorf("%s: %q: %v", name, q, err)
				continue
			}
			want, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.Label, prof.StreamFields, prof.Caps)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(want, "|") {
				want = "* " + want
			}
			if got := physical.Encode(op); got != want {
				t.Errorf("%s: %q:\n lowered %s\n  string %s", name, q, got, want)
			}
		}
		for q, construct := range delegated {
			p := rangeAggPlan(q)
			if p == nil {
				t.Fatalf("%q does not parse", q)
			}
			_, err := PlanQuery(q, p, prof)
			var nl *NotLoweredError
			if !errors.As(err, &nl) || nl.Construct != construct {
				t.Errorf("%s: %q: %v, want %q delegated", name, q, err, construct)
			}
		}
	}
}

// A metric row writes the chain once: the filter's chain is the one a grouping by detected_level and a rate's series
// read, never a second copy.
//
// conformance: architecture-syntax-package-layering
func TestDetectedLevelFilterChainOnceInAMetricRow(t *testing.T) {
	marker := `| format if (detected_level:*) "<detected_level>" as __dl_s`
	for _, q := range []string{
		`rate({app="a"} | detected_level="error" [5m])`,
		`sum by (detected_level) (count_over_time({app="a"} | detected_level!="info" [1m]))`,
		`sum by (detected_level) (rate({app="a"} | detected_level=~"error|warn" | detected_level!="warn" [1m]))`,
	} {
		op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		if n := strings.Count(physical.Encode(op), marker); n != 1 {
			t.Errorf("%q: %d chains, want 1", q, n)
		}
	}
}

// The classification answers Loki's reading of the filter, not its text: the leaf's operator, value kind and
// spelling decide (the edge table of detectedLevelStages).
//
// conformance: architecture-syntax-package-layering
func TestDetectedLevelStagesEdges(t *testing.T) {
	for _, tc := range []struct {
		q         string
		metric    bool
		construct string
	}{
		{`{a="b"} | detected_level="error"`, false, ""},
		{`{a="b"} | detected_level=""`, false, levelPresenceConstruct},
		{`{a="b"} | detected_level!=""`, false, levelPresenceConstruct},
		{`{a="b"} | detected_level=""`, true, levelPresenceConstruct},
		{`{a="b"} | detected_level!=""`, true, levelPresenceConstruct},
		{`{a="b"} | detected_level=~""`, false, ""},
		{"{a=\"b\"} | detected_level=``", false, levelFormConstruct},
		{"{a=\"b\"} | detected_level=``", true, levelPresenceConstruct},
		{`{a="b"} | detected_level="a\d"`, false, levelFormConstruct},
		{`{a="b"} | detected_level=~"("`, true, levelFormConstruct},
		{`{a="b"} | detected_level>=1`, false, levelFormConstruct},
		{`{a="b"} | drop __error__, __error_details__ | detected_level="x"`, false, ""},
		{`{a="b"} | drop __error__="x" | detected_level="x"`, false, levelAfterStageConstruct},
		{`{a="b"} | keep detected_level | detected_level="x"`, false, levelAfterStageConstruct},
		{`{a="b"} | decolorize | detected_level="x"`, false, levelAfterStageConstruct},
		{`{a="b"} | Detected_level="x"`, false, ""},
		{`{a="b"} | detected_level_x="x"`, false, ""},
		{`{a="b"} | detected_level.="x"`, false, levelSpellingConstruct},
		{`{a="b"} | (detected_level="x" or a="y")`, false, levelTreeConstruct},
	} {
		p := rangeAggPlan(tc.q)
		if p == nil {
			t.Fatalf("%q does not parse", tc.q)
		}
		sel := p.Root.(*queryplan.LogSelect)
		res, c := detectedLevelStages(sel, Profile{metric: tc.metric})
		if c != tc.construct {
			t.Errorf("%q (metric %v): construct %q, want %q", tc.q, tc.metric, c, tc.construct)
			continue
		}
		lbl := sel.Stages[len(sel.Stages)-1].(*queryplan.LabelFilter)
		if want := c == "" && isLevelLeaf(lbl.Expr); res.any != want {
			t.Errorf("%q (metric %v): a level filter %v, want %v", tc.q, tc.metric, res.any, want)
		}
	}
}

// A detected_level filter the lowering writes allocates no more than the string translator does for the same text
// (Parse, Build, Resolve and Plan against the translation), under every profile, on the log and metric shapes below:
// cold, with the memo of the plans emptied before every call (the typed chain of each mode is built once per
// process, as the translator's chain is memoised), with a value too long for the memo to keep, and warm, with the
// plan in the memo.
//
// conformance: architecture-syntax-package-layering
func TestDetectedLevelLoweringAllocatesNoMoreThanTheTranslator(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	t.Cleanup(resetLevelPlans)
	measure := func(q string, prof Profile, cold bool) (lowered, translated float64) {
		cq := logql.CanonicalizeLogRanges(q)
		translated = testing.AllocsPerRun(5, func() {
			_, _ = translator.TranslateLogQLWithCapabilities(cq, prof.Label, prof.StreamFields, prof.Caps)
		})
		lowered = testing.AllocsPerRun(5, func() {
			if cold {
				resetLevelPlans()
			}
			expr, _ := logql.Parse(cq)
			pl, _ := queryplan.Build(expr, queryplan.Inputs{})
			queryplan.Resolve(pl)
			_, _ = PlanQuery(q, pl, prof)
		})
		return lowered, translated
	}
	long := strings.Repeat("e", maxLevelPlanValue)
	shapes := append(append([]string{}, detectedLevelBenchQueries...),
		`{app="a"} | detected_level="`+long+`"`, `{app="a"} | detected_level=~"error|`+long+`"`,
		`{app="a"} | detected_level!="`+long+`"`, `count_over_time({app="a"} | detected_level="`+long+`" [5m])`)
	checked, margin := 0, math.Inf(1)
	for _, name := range []string{"plain", "v1.49", "no body scan", "stream labels", "stream fields"} {
		prof := detectedLevelProfiles()[name]
		for _, q := range shapes {
			if _, err := PlanQuery(q, rangeAggPlan(q), prof); err != nil {
				t.Fatalf("%s: %q: %v", name, q, err)
			}
			cold, tr := measure(q, prof, true)
			warm, _ := measure(q, prof, false)
			checked++
			margin = min(margin, tr-max(cold, warm))
			if name == "plain" {
				t.Logf("| %.60s | %.0f | %.0f | %.0f |", q, tr, cold, warm)
			}
			if cold > tr || warm > tr {
				t.Errorf("%s: %s: lowered cold %.0f, warm %.0f allocs, translator %.0f", name, q, cold, warm, tr)
			}
		}
	}
	t.Logf("%d shapes checked cold and warm, the closest %.0f allocs below the translator", checked, margin)
}

// The memo of the plans is safe for concurrent use and answers what a build answers, for keys it keeps, keys past
// its bound and values too long to keep (go test -race runs it with the race detector).
//
// conformance: architecture-syntax-package-layering
func TestLevelPlanMemoConcurrentUse(t *testing.T) {
	resetLevelPlans()
	t.Cleanup(resetLevelPlans)
	values := []string{"error", "warn", "info|debug", "(", "", strings.Repeat("x", maxLevelPlanValue+1)}
	for i := 0; i < maxLevelPlans+20; i++ {
		values = append(values, "v"+strconv.Itoa(i))
	}
	modes := []translate.DetectedLevelFilterMode{translate.DetectedLevelFilterLogs, translate.DetectedLevelFilterMetric, translate.DetectedLevelFilterStored}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i, v := range values {
				op := [...]string{"=", "!=", "=~", "!~"}[(i+g)%4]
				mode := modes[(i+g)%3]
				got, ok := levelFilterPlan(op, v, mode)
				want, err := translate.BuildDetectedLevelFilter(op, v, translate.DetectedLevelFieldNames, mode)
				if ok != (err == nil) || ok && nodesText(got) != want {
					t.Errorf("%s %q mode %d: memo %v %+v, build %v %+v", op, v, mode, ok, got, err, want)
				}
			}
		}(g)
	}
	wg.Wait()
	levelPlans.RLock()
	n := len(levelPlans.m)
	levelPlans.RUnlock()
	if n > maxLevelPlans {
		t.Fatalf("the memo holds %d plans, more than %d", n, maxLevelPlans)
	}
}

// detectedLevelBenchQueries are the shapes of the allocation test and the benchmarks: Grafana's level filters on
// Explore and Logs Drilldown (one value, a negation, an alternation, unknown, several in a row, with line filters),
// and the metric rows Drilldown sends with them.
var detectedLevelBenchQueries = []string{
	`{app="a"} | detected_level="error"`, `{app="a"} | detected_level!="info"`, `{app="a"} | detected_level=~"error|warn"`,
	`{app="a"} | detected_level=~"unknown|error"`, `{app="a"} |= "timeout" | detected_level="error"`,
	`{app="a"} | detected_level!="debug" | detected_level!="info"`, `sum(count_over_time({app="a"} | detected_level=~"" [5m]))`,
	`{service_name="api"} | detected_level="warn" | json`,
	`count_over_time({app="a"} | detected_level="error" [5m])`,
	`sum by (detected_level) (count_over_time({app="a"} | detected_level!="info" [1m]))`,
	`sum by (service_name) (rate({app="a"} | detected_level="error" [5m]))`,
}

// Grafana's level filters: the lowered path (parse, plan, lower) on the shapes above.
func BenchmarkDetectedLevelFilterLowered(b *testing.B) {
	plans := make([]*queryplan.Plan, len(detectedLevelBenchQueries))
	for i, q := range detectedLevelBenchQueries {
		plans[i] = rangeAggPlan(q)
	}
	canon := make([]string, len(detectedLevelBenchQueries))
	for i, q := range detectedLevelBenchQueries {
		canon[i] = logql.CanonicalizeLogRanges(q)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(canon)
		expr, _ := logql.Parse(canon[j])
		pl, _ := queryplan.Build(expr, queryplan.Inputs{})
		queryplan.Resolve(pl)
		if _, err := PlanQuery(detectedLevelBenchQueries[j], pl, Profile{}); err != nil {
			b.Fatal(err)
		}
	}
}

// Grafana's level filters: the string translator on the shapes above.
func BenchmarkDetectedLevelFilterTranslator(b *testing.B) {
	canon := make([]string, len(detectedLevelBenchQueries))
	for i, q := range detectedLevelBenchQueries {
		canon[i] = logql.CanonicalizeLogRanges(q)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := translator.TranslateLogQLWithCapabilities(canon[i%len(canon)], nil, nil, logsql.Capabilities{}); err != nil {
			b.Fatal(err)
		}
	}
}

// holdsLevelFilter reports a log query, or the log query under a range aggregation and the vector aggregations
// above it, with a label filter on detected_level: the benchmarks over corpus sets leave them out, so a set measures
// the same queries before and after the filters were lowered.
func holdsLevelFilter(n queryplan.Node) bool {
	for {
		switch v := n.(type) {
		case *queryplan.VectorAgg:
			n = v.In
		case *queryplan.RangeAgg:
			n = v.In
		case *queryplan.LogSelect:
			for _, st := range v.Stages {
				if lf, ok := st.(*queryplan.LabelFilter); ok && lf.Expr != nil && namesLevel(lf.Expr) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
}

// FuzzDetectedLevelLowering holds the lowering of label filters on detected_level to the string translator on any
// operator, value and quoting, alone or in a row with a line filter, another label filter or a parser, as a log query
// and under a range and a vector aggregation, under every profile of detectedLevelProfiles: both write the same text,
// or the lowering leaves the query to the translator. A value or label name holding detected_level or service_name
// is left out: the translator reads the query text for those words (semantics/translator-reads-quoted-text-as-syntax).
//
// conformance: architecture-syntax-package-layering
func FuzzDetectedLevelLowering(f *testing.F) {
	f.Add("error", "warn", uint8(0), uint8(0))
	f.Add("unknown|error", "info", uint8(2), uint8(17))
	f.Add("", "(?i)DEBU.*", uint8(10), uint8(35))
	f.Add("e\"r`r", "(", uint8(57), uint8(200))
	f.Fuzz(func(t *testing.T, v1, v2 string, ops, shape uint8) {
		if len(v1) > 64 || len(v2) > 64 {
			return // the filters read values of any length alike; a short one keeps each run fast
		}
		quote := func(v string, raw bool) string {
			if raw && !strings.Contains(v, "`") {
				return "`" + v + "`"
			}
			return strconv.Quote(v)
		}
		leaf := func(v string, op uint8) string {
			return "detected_level" + [...]string{"=", "!=", "=~", "!~", "=", "!="}[op%6] + quote(v, op%6 >= 4)
		}
		l1, l2 := leaf(v1, ops), leaf(v2, ops>>3)
		var stage string
		switch shape % 6 {
		case 0:
			stage = l1
		case 1:
			stage = l1 + " | " + l2
		case 2:
			stage = l1 + ` |= "k" | ` + l2
		case 3:
			stage = l1 + ` | app="b" | ` + l2
		case 4:
			stage = `drop __error__ | ` + l1 + ` | json | ` + l2
		default:
			stage = l1 + ` |> "<_> x" != "j"`
		}
		q := `{app="a"} | ` + stage
		switch (shape >> 3) % 3 {
		case 1:
			q = `count_over_time(` + q + ` [5m])`
		case 2:
			q = `sum by (detected_level) (rate(` + q + ` [5m]))`
		}
		for _, s := range []string{v1, v2} {
			if strings.Contains(s, "detected_level") || strings.Contains(s, "service_name") || translatorReadsAsSyntax(s) {
				return
			}
		}
		p := rangeAggPlan(q)
		if p == nil {
			return
		}
		for name, prof := range detectedLevelProfiles() {
			op, lerr := PlanQuery(q, p, prof)
			legacy, terr := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.Label, prof.StreamFields, prof.Caps)
			if errors.Is(lerr, ErrNotLowered) || terr != nil {
				if lerr == nil && terr != nil {
					t.Fatalf("%s: %s: lowered, the translator rejects it: %v", name, q, terr)
				}
				continue
			}
			if lerr != nil {
				t.Fatalf("%s: %s: %v", name, q, lerr)
			}
			if strings.HasPrefix(legacy, "|") {
				legacy = "* " + legacy
			}
			if got := physical.Encode(op); got != QuoteKeywordFieldText(legacy) {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", name, q, got, legacy)
			}
		}
	})
}
