package querycorpus_test

import (
	"context"
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
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankgrouping"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/translatorspelling"
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
	// NativeLokiHeld are the native rows of a recorded known divergence that the proxy serves from the plan
	// (proxy.PlanServedText): the plan writes Loki's reading where the string translator writes another text, so the
	// row is held to the text the proxy serves (the lowering's, byte for byte) and its answers to Loki by the
	// differential its registry case names (servedDivergences), not to the translator. Included in Native.
	NativeLokiHeld int `json:"native_loki_held"`
	// Mismatches are the rows where the lowering answers differently, or answers a query the translator rejects.
	Mismatches int `json:"mismatches"`
	// Rejected are the rows the proxy refuses before anything is translated (the request's validation:
	// logql.ValidateLogQLExpr, then queryplan.Validate), with Loki's parse error: neither the plan nor the string
	// translator ever sees them, so they are not rows of the shadow (TestShadowRejectedRowsAreLokiRejections).
	Rejected int `json:"rejected"`
	// DelegatedByConstruct counts, by the construct at the top of the plan, the rows answered by the string
	// translator because the lowering does not cover them yet (reported, not gated: it grows with the corpus).
	DelegatedByConstruct map[string]int `json:"delegated_by_construct"`
	// Flipped are the constructs the proxy serves from the lowering instead of the string translator
	// (proxy.QueryPlanFlippedConstructs): each is native on the corpus with 0 mismatches, text included.
	Flipped []string `json:"flipped"`
	// FlippedLogStages are the stage families of log queries the proxy serves from the lowering on top of line and label
	// filters (proxy.QueryPlanFlippedLogStages): their LogSelect rows are native with 0 mismatches, text included.
	FlippedLogStages []string `json:"flipped_log_stages,omitempty"`
}

// shadowSlice is one profile × construct of a shadow run: the slice a failure names.
type shadowSlice struct {
	rows, native, mismatches, delegated int
	problems                            []string
}

// lowering is what the harness holds to the string translator: the native lowering, or a test's stand-in.
type lowering func(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error)

// It screens the canonical text (logql.CanonicalizeLogRanges), as the proxy does when it serves a query from the
// lowering (lowerMetric): the shadow holds what the proxy sends.
func nativeLowering(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error) {
	return lower.PlanQuery(logql.CanonicalizeLogRanges(q), p, prof.lowerProfile())
}

func (p planProfile) lowerProfile() lower.Profile {
	return lower.Profile{Caps: p.caps, Label: p.labelFn, StreamFields: p.stream, KeepLine: p.keepLine}
}

// runShadow compares lowerFn with the string translator on the first limit corpus rows (all when limit is 0) and
// returns the counts and the mismatching rows. No row is held to what the proxy serves: a stand-in lowering is held
// to the translator, or delegated as the recorded divergence its row is.
func runShadow(t *testing.T, lowerFn lowering, limit int) (shadowSection, []string) {
	sec, problems, _ := runShadowSlices(t, lowerFn, nil, limit)
	return sec, problems
}

// servedText is the text the proxy serves from the plan for a query under a profile (proxy.PlanServedText), and
// false when the string translator translates it.
type servedText func(q string, prof planProfile) (string, bool)

// proxyServed is servedText of a proxy built with the defaults the proxy runs with (-query-plan-lowering on, the
// constructs and stage families of QueryPlanFlippedConstructs and QueryPlanFlippedLogStages).
func proxyServed(t *testing.T) servedText {
	t.Helper()
	p, err := proxy.New(proxy.Config{BackendURL: "http://unused", LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return func(q string, prof planProfile) (string, bool) { return p.PlanServedText(q, prof.lowerProfile()) }
}

// servedDivergences are the recorded known divergences a row of which the proxy may serve from the plan, each with
// the differential that holds the plan's answers to Loki (recorded Loki answers, the Loki oracle and VictoriaLogs
// v1.49 and v1.53). A served row of another divergence stays delegated: no differential holds its answer.
var servedDivergences = map[string]string{
	knownKeepIdentity:    "TestKeepUnwrapIdentityMatchLoki (internal/querycorpus)",
	knownDynamicIdentity: "TestKeepUnwrapIdentityMatchLoki, TestStddevStdvarMatchLoki (internal/querycorpus)",
	knownCountVariance:   "TestStddevStdvarMatchLoki, TestExtractionListIdentityMatchLoki (internal/querycorpus)",
	"semantics/translator-writes-reserved-field-names": "TestReservedFieldLabelsMatchLoki (internal/queryplan/lower)",
}

// spellingDifferential holds the rows translatorspelling.Case names (a spelling the translator reads otherwise than
// the plan, which reads the tree) to Loki: the proxy serves the plan's text for them. The other rows of those cases
// (semantics/translator-reads-quoted-text-as-syntax: a log query's label filter value) have no differential yet and
// stay delegated.
const spellingDifferential = "TestPlanSpellingsMatchLoki (internal/proxy)"

// encodeOrEmpty is the text of op, or "" for none.
func encodeOrEmpty(op physical.Op) string {
	if op == nil {
		return ""
	}
	return translator.EncodePhysical(op)
}

// rejectedByValidation reports a query the request's validation refuses before anything is translated: the parser
// and its semantic pass (logql.ValidateLogQLExpr), then Loki's checks on the typed tree (queryplan.Validate), as the
// proxy's validateQueryTree reads it.
func rejectedByValidation(q string) bool {
	expr, msg := logql.ValidateLogQLExpr(q)
	if msg != "" {
		return true
	}
	return expr != nil && queryplan.Validate(expr) != nil
}

// runShadowSlices is runShadow with the counts of each profile × construct (the construct at the top of the plan); a
// row of servedDivergences that served names is held to the text the proxy serves for it.
func runShadowSlices(t *testing.T, lowerFn lowering, served servedText, limit int) (shadowSection, []string, map[[2]string]*shadowSlice) {
	t.Helper()
	corpus := planCorpus(t)
	if limit > 0 && limit < len(corpus) {
		corpus = corpus[:limit]
	}
	profiles := planProfiles()
	sec := shadowSection{Profiles: len(profiles), DelegatedByConstruct: map[string]int{}}
	var problems []string
	slices := map[[2]string]*shadowSlice{}
	for _, row := range corpus {
		// The handlers read the query as the request's rewrite leaves it (proxy.RequestQueryText): the metric calls and
		// parentheses in one spelling, keywords in lower case, no comments; then they clamp a quantile above 1
		// (proxy.RewriteQuantilePhiGT1) before they plan and translate it.
		q := proxy.RewriteQuantilePhiGT1(proxy.RequestQueryText(row))
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
		// A query the request's validation refuses is answered with Loki's parse error before any translation.
		rejected := rejectedByValidation(q)
		// A `by` clause of only blanks is `by ()` to Loki and to the plan: the reference is the string translator's
		// text for that spelling, which reads the clause as Loki does (semantics/translator-reads-blank-grouping-as-none).
		ref := blankgrouping.Cut(q)
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
			legacy, ok := prof.translate(ref)
			legacy = lower.QuoteKeywordFieldText(legacy) // the translator writes a keyword field name bare, the plan quotes it
			if ok && rejected {
				sec.Rejected++
				continue
			}
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
			routed, divergence := routedRoot(plan)
			caseID, ok := knownTypedDivergences()[queryID("logql", row)]
			if !ok && translatorMisreadsQuotedText(q) {
				caseID, ok = "semantics/translator-reads-quoted-text-as-syntax", true
			}
			differential := ""
			if !ok && !routed && translatorMisreadsParenthesizedMetric(q) {
				caseID, ok = "semantics/translator-reads-parenthesized-metric-as-text", true
			}
			if !ok && translatorReadsStageMarker(q) {
				caseID, ok = "semantics/translator-reads-marker-bytes-in-quoted-value", true
			}
			if !ok && translatorCopiesUnspacedParser(q) {
				caseID, ok = "semantics/translator-copies-unspaced-parser", true
			}
			if !ok && emptyWithoutDiffers(q, plan, prof) {
				// A `without ()` operand: Loki keeps every series of it, the translator writes it ungrouped, and the
				// route's reference is that text (TestEmptyWithoutOperandsKeepEverySeries holds the lowering).
				caseID, ok = knownEmptyWithoutOperand, true
			}
			if !ok && lower.ReadsReservedField(plan, prof.lowerProfile()) {
				// A label called _time, _stream, _stream_id or _msg: the plan filters the stream tag, the
				// translator a field VictoriaLogs rejects or reads as its own filter.
				caseID, ok = "semantics/translator-writes-reserved-field-names", true
			}
			if !ok && lower.WritesKeepUnwrapIdentity(plan) && !sameAsTranslator(got, legacy) {
				// A keep before the unwrap decides the series: the plan groups by the labels it leaves, the translator
				// by the stream (and each line), and the proxy serves the plan's text (lowerMetric).
				caseID, ok = knownKeepIdentity, true
			}
			if !ok && lower.WritesDynamicUnwrapIdentity(plan) && !sameAsTranslator(got, legacy) {
				// The series are the stream and every parsed label: the plan groups by the stream and a hash of the
				// parsed labels, the translator by the stream (and each line), and the proxy serves the plan's text
				// (lowerDynamicIdentity).
				caseID, ok = knownDynamicIdentity, true
			}
			if !ok && lower.WritesCountVarianceSeries(plan) && !sameAsTranslator(got, legacy) {
				// A count by labels over stdvar_over_time: the plan counts the series, the translator answers the
				// variance of the group's pooled samples, and the proxy serves the plan's text (lowerMetric).
				caseID, ok = knownCountVariance, true
			}
			if !ok && lower.WritesNumericEquality(plan) {
				// A numeric, duration or bytes equality after a json or logfmt parser: the plan compares the value as
				// a number as Loki does, the translator writes the exact text of the literal.
				caseID, ok = "semantics/label-filter-numeric-equality", true
			}
			if c := translatorspelling.Case(q, plan); !ok && !routed && c != "" && !sameAsTranslator(got, legacy) {
				// A spelling the translator reads otherwise than the plan, which reads the tree: the proxy serves the
				// plan's text, held to Loki by the spelling's differential.
				caseID, ok, differential = c, true, spellingDifferential
			}
			if differential == "" {
				differential = servedDivergences[caseID]
			}
			if ok && served != nil && differential != "" {
				// The proxy serves the plan's text for the row: it must be the lowering's, byte for byte, and the
				// case's differential holds its answers to Loki.
				switch text, isServed := served(q, prof); {
				case !isServed || text == legacy:
					// The string translator's text, or the plan's where it writes the translator's reading (a
					// parser's series identity under a binary operation, which lowerMetric writes with the stream's
					// series): the row stays the divergence.
				case text == translator.EncodePhysical(got):
					sec.Native++
					sec.NativeLokiHeld++
					sl.native++
					continue
				default:
					fail(fmt.Sprintf("%s: %q: the proxy serves %q, the lowering writes %q", prof.name, q, text, encodeOrEmpty(got)))
					continue
				}
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

// spellingDivergences are the rows of sec delegated as a spelling the translator reads otherwise than the plan
// (translatorspelling.Case): like a keep's series identity, a lowering that differs from the translator there is that
// divergence, held to Loki by its differential, not a mismatch.
func spellingDivergences(sec shadowSection) int {
	n := 0
	for _, c := range []string{translatorspelling.QuotedText, translatorspelling.UnwrapConversionBlank, translatorspelling.QuantileLiteral, translatorspelling.BinaryText} {
		n += sec.DelegatedByConstruct["known divergence "+c]
	}
	return n
}

// knownKeepIdentity is the registry case of the plan's series identity behind a keep, which the proxy serves from
// the lowering where it differs from the string translator's.
const knownKeepIdentity = "semantics/keep-before-unwrap-series-identity"

// knownDynamicIdentity is the registry case of the plan's series identity after a json or logfmt parser with no keep
// (the stream and every parsed label), which the proxy serves from the lowering where it differs from the string
// translator's.
const knownDynamicIdentity = "semantics/unwrap-per-series-identity-raw-rows"

// knownCountVariance is the registry case of a count by labels over stdvar_over_time, which the proxy serves from the
// lowering (the count of the series) where the string translator writes the group's variance.
const knownCountVariance = "semantics/count-over-stdvar-answers-variance"

// sameAsTranslator reports a lowering that writes the string translator's operators and text: a keep whose
// identity the translator happens to write too (an outer by that holds every kept label) is native.
func sameAsTranslator(got physical.Op, legacy string) bool {
	want, err := translator.DecodePhysical(legacy)
	return err == nil && physical.Equal(want, got) && translator.EncodePhysical(got) == legacy
}

// TestPlanShadow holds the native lowering to the string translator on the whole corpus under every profile.
// conformance: architecture-syntax-package-layering
func TestPlanShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	got, problems, slices := runShadowSlices(t, nativeLowering, proxyServed(t), 0)
	got.Flipped = proxy.QueryPlanFlippedConstructs
	got.FlippedLogStages = proxy.QueryPlanFlippedLogStages
	// A flipped construct is served from the lowering with the corpus as its proof: it must be native under every
	// profile that translates it, and its rows must all equal the string translator's text (a mismatch fails the run
	// above). The keepLine profiles translate log queries only (a metric query never keeps its line), so a metric
	// construct has no row there.
	for _, c := range got.Flipped {
		for _, prof := range planProfiles() {
			sl := slices[[2]string{prof.name, c}]
			if sl != nil && sl.rows == 0 && prof.keepLine && c != "LogSelect" {
				continue
			}
			if sl == nil || sl.native == 0 || sl.mismatches != 0 {
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
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && !reflect.DeepEqual(got.FlippedLogStages, want.FlippedLogStages) {
		t.Fatalf("the log stage families served from the lowering are %v, %s records %v: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestPlanShadow and commit it", got.FlippedLogStages, planPath, want.FlippedLogStages)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && !reflect.DeepEqual(got.Flipped, want.Flipped) {
		t.Fatalf("the constructs served from the lowering are %v, %s records %v: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestPlanShadow and commit it", got.Flipped, planPath, want.Flipped)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && got.Native > want.Native {
		t.Fatalf("native rose from %d to %d: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestPlanShadow and commit %s", want.Native, got.Native, planPath)
	}
}

// The proxy asks cheap screens before it plans a query to serve from the lowering (a stream selector first,
// logql.MayHaveNonFilterStage, and for a filter on detected_level the log stage screen lower.ScreenLogStages): none
// may turn away a filter-only query the lowering writes natively, or the flip would silently leave it to the string
// translator.
// conformance: architecture-syntax-package-layering
func TestFlipScreenAdmitsEveryNativeRow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	admitted, turned, level := 0, 0, 0
	for _, q := range append(planCorpus(t), labelFilterGrid()...) {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
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
		for _, prof := range planProfiles() {
			if _, err := lower.Plan(plan, prof.lowerProfile()); err != nil {
				continue
			}
			if msg := screenProblem(plan, q); msg != "" {
				t.Errorf("%s: %q: %s", prof.name, q, msg)
				turned++
				continue
			}
			if filtersDetectedLevel(plan) {
				level++
			}
			admitted++
		}
	}
	t.Logf("native rows admitted by the screens: %d (detected_level filters %d), turned away: %d", admitted, level, turned)
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
		if lower.RoutedBinary(p.Root) {
			op, _, routed, err := proxy.BinaryOperandsPhysical(routedQuery(q), prof.operandTranslate)
			if err != nil || !routed {
				return nil, &lower.NotLoweredError{Construct: "rejected"}
			}
			return op, nil
		}
		if routed, _ := evaluatedRoot(p); routed {
			op, _, routed, err := proxyReference(q, prof)
			if err != nil || !routed {
				return nil, &lower.NotLoweredError{Construct: "rejected"}
			}
			return op, nil
		}
		s, ok := prof.translate(blankgrouping.Cut(q)) // the reference the shadow holds a blank grouping to
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
		// A row whose series a keep before the unwrap, or a parser's labels, decide is held to Loki
		// (TestKeepUnwrapIdentityMatchLoki), not to the translator: a lowering that differs there is that divergence,
		// not a mismatch.
		if bad, _ := runShadow(t, wrong, rows); bad.Native != 0 || bad.Mismatches < sec.Rows-known-bad.DelegatedByConstruct["known divergence "+knownKeepIdentity]-bad.DelegatedByConstruct["known divergence "+knownDynamicIdentity]-bad.DelegatedByConstruct["known divergence "+knownCountVariance]-spellingDivergences(bad) {
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
		if bad, _ := runShadow(t, subtle, rows); bad.Native != 0 || bad.Mismatches != sec.Rows-known-bad.DelegatedByConstruct["known divergence "+knownKeepIdentity]-bad.DelegatedByConstruct["known divergence "+knownDynamicIdentity]-bad.DelegatedByConstruct["known divergence "+knownCountVariance]-spellingDivergences(bad) {
			t.Fatalf("a lowering that adds a label_replace to each answer: %+v", bad)
		}
	})
}

func joinFirst(lines []string, n int) string {
	if os.Getenv("ZZALL") == "" && len(lines) > n {
		lines = append(lines[:n:n], fmt.Sprintf("... and %d more", len(lines)-n))
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}
