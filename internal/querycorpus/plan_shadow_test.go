package querycorpus_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The lowering shadow: every plan-corpus row under each of the 12 profiles, the operators the native lowering
// (lower.Plan) writes against the operators the string translator's output decodes to (translator.DecodePhysical),
// compared by structure (physical.Equal: the parsed LogsQL with its default spellings folded, and the operator tree). A
// construct with no native lowering yet is delegated to the string translator and counted by construct. The
// shadow section of query-plan.json holds the counts: mismatches stay 0 and native only rises.

type shadowSection struct {
	// Rows are the corpus rows × profiles the string translator translates.
	Rows     int `json:"rows"`
	Profiles int `json:"profiles"`
	// Native are the rows the lowering answers, each equal to the string translator's operators, or, for a root the
	// proxy evaluates itself, to the proxy's route.
	Native int `json:"native"`
	// NativeProxyEvaluated are the native rows whose root the proxy evaluates itself (a ranking): held to the
	// operand's request and the evaluation the proxy applies to its answer (proxy.PostAggregationPhysical), not to
	// the string translator's text for the whole query. Included in Native.
	NativeProxyEvaluated int `json:"native_proxy_evaluated"`
	// Mismatches are the rows where the lowering answers differently, or answers a query the translator rejects.
	Mismatches int `json:"mismatches"`
	// DelegatedByConstruct counts, by the construct at the top of the plan, the rows answered by the string
	// translator because the lowering does not cover them yet (reported, not gated: it grows with the corpus).
	DelegatedByConstruct map[string]int `json:"delegated_by_construct"`
	// Flipped are the constructs the proxy serves from the lowering instead of the string translator
	// (proxy.QueryPlanFlippedConstructs): each is native on the corpus with 0 mismatches, text included.
	Flipped []string `json:"flipped"`
}

// shadowSlice is one profile × construct of a shadow run: the slice a failure names.
type shadowSlice struct {
	rows, native, mismatches, delegated int
	problems                            []string
}

// lowering is what the harness holds to the string translator: the native lowering, or a test's stand-in.
type lowering func(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error)

func nativeLowering(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error) {
	return lower.PlanQuery(q, p, prof.lowerProfile())
}

func (p planProfile) lowerProfile() lower.Profile {
	return lower.Profile{Caps: p.caps, Label: p.labelFn, StreamFields: p.stream, KeepLine: p.keepLine}
}

// runShadow compares lowerFn with the string translator on the first limit corpus rows (all when limit is 0) and
// returns the counts and the mismatching rows.
func runShadow(t *testing.T, lowerFn lowering, limit int) (shadowSection, []string) {
	sec, problems, _ := runShadowSlices(t, lowerFn, limit)
	return sec, problems
}

// runShadowSlices is runShadow with the counts of each profile × construct (the construct at the top of the plan).
func runShadowSlices(t *testing.T, lowerFn lowering, limit int) (shadowSection, []string, map[[2]string]*shadowSlice) {
	t.Helper()
	corpus := planCorpus(t)
	if limit > 0 && limit < len(corpus) {
		corpus = corpus[:limit]
	}
	profiles := planProfiles()
	sec := shadowSection{Profiles: len(profiles), DelegatedByConstruct: map[string]int{}}
	var problems []string
	slices := map[[2]string]*shadowSlice{}
	for _, q := range corpus {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		construct := lower.Construct(plan.Root)
		for _, prof := range profiles {
			sl := slices[[2]string{prof.name, construct}]
			if sl == nil {
				sl = &shadowSlice{}
				slices[[2]string{prof.name, construct}] = sl
			}
			fail := func(msg string) {
				sec.Mismatches++
				sl.mismatches++
				problems = append(problems, msg)
				sl.problems = append(sl.problems, msg)
			}
			got, lerr := lowerFn(q, plan, prof)
			legacy, ok := prof.translate(q)
			legacy = lower.QuoteKeywordFieldText(legacy) // the translator writes a keyword field name bare, the plan quotes it
			if !ok {
				if lerr == nil {
					// Counted in the slice's rows (not the section's) so the slice's sum still holds.
					sl.rows++
					fail(fmt.Sprintf("%s: %q: lowered, the string translator rejects it", prof.name, q))
				}
				continue
			}
			sec.Rows++
			sl.rows++
			var nl *lower.NotLoweredError
			switch {
			case errors.As(lerr, &nl):
				sec.DelegatedByConstruct[nl.Construct]++
				sl.delegated++
				continue
			case errors.Is(lerr, lower.ErrNotLowered):
				sec.DelegatedByConstruct["unnamed"]++
				sl.delegated++
				continue
			case lerr != nil:
				fail(fmt.Sprintf("%s: %q: lowering failed: %v", prof.name, q, lerr))
				continue
			}
			// A root the proxy evaluates is held to the proxy's route, which strips the parentheses that wrap the whole
			// query before it translates the operand: the translator's misreading of them is not in that reference.
			routed, divergence := evaluatedRoot(plan)
			caseID, ok := knownTypedDivergences()[queryID("logql", q)]
			if !ok && translatorMisreadsQuotedText(q) {
				caseID, ok = "semantics/translator-reads-quoted-text-as-syntax", true
			}
			if !ok && !routed && translatorMisreadsParenthesizedMetric(q) {
				caseID, ok = "semantics/translator-reads-parenthesized-metric-as-text", true
			}
			if !ok && translatorReadsStageMarker(q) {
				caseID, ok = "semantics/translator-reads-marker-bytes-in-quoted-value", true
			}
			if !ok && lower.ReadsReservedField(plan, prof.lowerProfile()) {
				// A label called _time, _stream, _stream_id or _msg: the plan filters the stream tag, the
				// translator a field VictoriaLogs rejects or reads as its own filter.
				caseID, ok = "semantics/translator-writes-reserved-field-names", true
			}
			if ok {
				// The translator is the wrong one here (knownTypedDivergencesPath, with its registry case):
				// not a mismatch of the lowering, and not native until the plan serves it.
				sec.DelegatedByConstruct["known divergence "+caseID]++
				sl.delegated++
				continue
			}
			if routed {
				if msg := checkProxyEvaluated(q, prof, got); msg != "" {
					fail(msg)
					continue
				}
				sec.Native++
				sec.NativeProxyEvaluated++
				sl.native++
				continue
			} else if divergence != "" {
				// The proxy does not take the shape as one evaluation, and the translator's text for it drops a level
				// (evaluatedRoot): the plan is right, not served yet.
				sec.DelegatedByConstruct["known divergence "+divergence]++
				sl.delegated++
				continue
			}
			want, err := translator.DecodePhysical(legacy)
			if err != nil {
				t.Fatalf("%s: DecodePhysical(%q): %v", prof.name, legacy, err)
			}
			if lower.UnwrapsReservedFieldOtherCase(plan) {
				// The lowering writes the unwrap of _TIME and the like in the outbound guard's form: it is held to
				// the translator's text guarded, which is what the proxy sends for it (semantics/reserved-field-name-case-outbound).
				want = lower.GuardOutbound(want)
				legacy = translator.EncodePhysical(want)
			}
			if !physical.Equal(want, got) {
				fail(fmt.Sprintf("%s: %q:\n lowered %s\n  string %s", prof.name, q, translator.EncodePhysical(got), legacy))
				continue
			}
			// The flip serves the lowering's text, so a native row must also be the translator's text, byte for byte.
			if text := translator.EncodePhysical(got); text != legacy {
				fail(fmt.Sprintf("%s: %q:\n lowered text %s\n   string text %s", prof.name, q, text, legacy))
				continue
			}
			sec.Native++
			sl.native++
		}
	}
	return sec, problems, slices
}

// TestPlanShadow holds the native lowering to the string translator on the whole corpus under every profile.
// conformance: architecture-syntax-package-layering
func TestPlanShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	got, problems, slices := runShadowSlices(t, nativeLowering, 0)
	got.Flipped = proxy.QueryPlanFlippedConstructs
	// A flipped construct is served from the lowering with the corpus as its proof: it must be native under every
	// profile, and its rows must all equal the string translator's text (a mismatch fails the run above).
	for _, c := range got.Flipped {
		for _, prof := range planProfiles() {
			if sl := slices[[2]string{prof.name, c}]; sl == nil || sl.native == 0 || sl.mismatches != 0 {
				t.Errorf("%s is served from the lowering, but is not native on the corpus under %q: %+v", c, prof.name, sl)
			}
		}
	}
	// One subtest per profile and construct: a mismatch names the slice it is in.
	for _, prof := range planProfiles() {
		t.Run(prof.name, func(t *testing.T) {
			for _, c := range []string{"LogSelect", "RangeAgg", "VectorAgg", "Rank", "BinOp", "LabelReplace", "VectorLit", "Scalar", "Opaque"} {
				sl := slices[[2]string{prof.name, c}]
				if sl == nil {
					continue
				}
				t.Run(c, func(t *testing.T) {
					t.Logf("rows %d, native %d, delegated %d", sl.rows, sl.native, sl.delegated)
					if sl.mismatches != 0 {
						t.Errorf("%d rows lower unlike the string translator:\n%s", sl.mismatches, joinFirst(sl.problems, 10))
					}
					if sl.native+sl.delegated+sl.mismatches != sl.rows {
						t.Errorf("rows %d are not native %d + delegated %d + mismatches %d", sl.rows, sl.native, sl.delegated, sl.mismatches)
					}
				})
			}
		})
	}
	file, err := readPlanFile()
	if err != nil {
		t.Fatal(err)
	}
	var want shadowSection
	if raw, ok := file["shadow"]; ok {
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
	}
	constructs := make([]string, 0, len(got.DelegatedByConstruct))
	for c := range got.DelegatedByConstruct {
		constructs = append(constructs, c)
	}
	sort.Strings(constructs)
	for _, c := range constructs {
		t.Logf("delegated %-12s %d", c, got.DelegatedByConstruct[c])
	}
	t.Logf("rows %d (%d profiles), native %d, mismatches %d", got.Rows, got.Profiles, got.Native, got.Mismatches)
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		writePlanSection(t, file, "shadow", got)
	}
	if got.Mismatches != 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", got.Mismatches, joinFirst(problems, 20))
	}
	if got.Native < want.Native {
		t.Fatalf("native fell from %d to %d: a construct that was lowered no longer is", want.Native, got.Native)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && !reflect.DeepEqual(got.Flipped, want.Flipped) {
		t.Fatalf("the constructs served from the lowering are %v, %s records %v: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestPlanShadow and commit it", got.Flipped, planPath, want.Flipped)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && got.Native > want.Native {
		t.Fatalf("native rose from %d to %d: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestPlanShadow and commit %s", want.Native, got.Native, planPath)
	}
}

// The proxy asks two cheap screens before it plans a query to serve from the lowering (a stream selector first,
// logql.MayHaveNonFilterStage): neither may turn away a filter-only query the lowering writes natively, or the flip
// would silently leave it to the string translator.
// conformance: architecture-syntax-package-layering
func TestFlipScreenAdmitsEveryNativeRow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	admitted, turned := 0, 0
	for _, q := range append(planCorpus(t), labelFilterGrid()...) {
		canon := logql.CanonicalizeLogRanges(q)
		expr, err := logql.Parse(canon)
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		if !filterOnly(plan) {
			continue
		}
		passes := canon != "" && canon[0] == '{' && !logql.MayHaveNonFilterStage(canon)
		for _, prof := range planProfiles() {
			if _, err := lower.Plan(plan, prof.lowerProfile()); err != nil {
				continue
			}
			if !passes {
				t.Errorf("%s: %q is lowered natively, but the flip's screen turns it away", prof.name, q)
				turned++
				continue
			}
			admitted++
		}
	}
	t.Logf("native rows admitted by the screen: %d, turned away: %d", admitted, turned)
	if admitted == 0 {
		t.Fatal("no native row")
	}
}

// The harness itself: a lowering that answers what the string translator answers is native everywhere, and one
// that answers anything else is a mismatch on every row it answers, a plain wrong query and a subtly changed one
// alike. A lowering that answers a query the translator rejects is a mismatch too.
func TestPlanShadowCatchesAWrongLowering(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	const rows = 300
	echo := func(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error) {
		// A ranking or stddev is evaluated by the proxy: the operators it runs are the route's, not the translator's text.
		if routed, _ := evaluatedRoot(p); routed {
			op, _, routed, err := proxyReference(q, prof)
			if err != nil || !routed {
				return nil, &lower.NotLoweredError{Construct: "rejected"}
			}
			return op, nil
		}
		s, ok := prof.translate(q)
		if !ok {
			return nil, &lower.NotLoweredError{Construct: "rejected"}
		}
		op, err := translator.DecodePhysical(s)
		if err == nil && lower.UnwrapsReservedFieldOtherCase(p) {
			op = lower.GuardOutbound(op) // the translator's text as the proxy sends it
		}
		return op, err
	}
	sec, problems := runShadow(t, echo, rows)
	// A row the translator misreads (known-typed-divergences, the quoted-text and parenthesized-metric recognisers)
	// is delegated, not compared.
	known := 0
	for c, n := range sec.DelegatedByConstruct {
		if strings.HasPrefix(c, "known divergence") {
			known += n
		}
	}
	t.Run("own operators are native", func(t *testing.T) {
		if sec.Rows == 0 || sec.Native+known != sec.Rows || sec.Mismatches != 0 {
			t.Fatalf("the string translator's own operators: %+v\n%s", sec, joinFirst(problems, 5))
		}
	})
	wrong := func(string, *queryplan.Plan, planProfile) (physical.Op, error) {
		return physical.VLQuery{Text: "*"}, nil
	}
	t.Run("a wrong query is a mismatch", func(t *testing.T) {
		if bad, _ := runShadow(t, wrong, rows); bad.Native != 0 || bad.Mismatches < sec.Rows-known {
			t.Fatalf("a lowering that answers `*` for everything: %+v", bad)
		}
	})
	subtle := func(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error) {
		op, err := echo(q, p, prof)
		if err != nil {
			return nil, err
		}
		return physical.LabelMap{Replace: []physical.LabelReplace{{DstLabel: "x"}}, In: op}, nil
	}
	t.Run("a changed post-processing is a mismatch", func(t *testing.T) {
		if bad, _ := runShadow(t, subtle, rows); bad.Native != 0 || bad.Mismatches != sec.Rows-known {
			t.Fatalf("a lowering that adds a label_replace to each answer: %+v", bad)
		}
	})
}

func joinFirst(lines []string, n int) string {
	if len(lines) > n {
		lines = append(lines[:n:n], fmt.Sprintf("... and %d more", len(lines)-n))
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}
