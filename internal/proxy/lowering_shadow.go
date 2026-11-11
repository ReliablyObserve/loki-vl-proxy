package proxy

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The lowering shadow (-translate-shadow-sample) is an observe-only check of the typed query-plan lowering
// against live traffic: on a sampled share of the queries the string translator translates (translation cache
// misses), a background goroutine runs the typed pipeline (logql.Parse, queryplan.Build and Resolve, lower.Plan)
// under the options the translator received and compares its operators with the translator's output, exactly as
// the corpus shadow (internal/querycorpus TestPlanShadow) does. It counts the outcome in
// loki_vl_proxy_lowering_shadow_total and never touches the answer, the caches or a VictoriaLogs request, so a
// construct can be switched to the lowering once it matches live traffic as well as the corpus.
const (
	// loweringShadowSlots bounds the comparisons running at once; with both slots busy a sampled query is skipped.
	loweringShadowSlots = 2
	// loweringShadowBudget is how long one comparison may take, checked between its stages.
	loweringShadowBudget = 100 * time.Millisecond
	// loweringShadowLogEvery is the shortest time between two mismatch log lines.
	loweringShadowLogEvery = time.Second

	// The results of loki_vl_proxy_lowering_shadow_total.
	shadowMatch      = "match"
	shadowMismatch   = "mismatch"
	shadowNewError   = "new_error"
	shadowOldError   = "old_error"
	shadowNotLowered = "not_lowered"
	shadowSkipped    = "skipped"
	shadowPanic      = "panic"
	shadowBudget     = "budget"
	// shadowFlipped counts a query the lowering answered instead of the string translator (-query-plan-lowering):
	// nothing is compared for it.
	shadowFlipped = "flipped"
	// shadowNativeProxyEvaluated counts a query the proxy evaluates in part itself (a ranking, stddev or stdvar,
	// and the layers above a ranking) whose lowering equals the proxy's route: the operand's request byte for
	// byte and the levels the proxy applies to its answer (shadowProxyEvaluated); the corpus shadow's
	// native_proxy_evaluated.
	shadowNativeProxyEvaluated = "native_proxy_evaluated"
	// shadowKnownDivergence counts such a query whose plan holds a level the route does not evaluate (a stddev,
	// stdvar or ranking inside the operand the proxy translates as text, which drops it): the plan is right, the
	// route is the known difference (registry case semantics/stddev-stdvar-nested-or-binary), nothing is compared.
	shadowKnownDivergence = "known_divergence"

	// shadowNoConstruct is the construct of an outcome with no plan: a query that does not parse, or a
	// comparison that never ran.
	shadowNoConstruct = "none"
)

// shadowConstructKinds are the kinds of node at the top of a plan (lower.Construct).
var shadowConstructKinds = map[string]bool{
	"LogSelect": true, "RangeAgg": true, "VectorAgg": true, "Rank": true, "BinOp": true,
	"LabelReplace": true, "VectorLit": true, "Scalar": true, "Opaque": true,
}

// shadowConstruct bounds the construct label: a node kind, or a name lower.NotLoweredError gives to a part of it
// ("LogSelect pipeline"), which is a kind followed by what the lowering does not cover yet. Anything else,
// whatever its origin, is "other"; no label value is ever taken from a query.
func shadowConstruct(c string) string {
	if c == shadowNoConstruct {
		return c
	}
	kind, _, _ := strings.Cut(c, " ")
	if !shadowConstructKinds[kind] || len(c) > 48 {
		return "other"
	}
	for _, r := range c {
		if r != ' ' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return "other"
		}
	}
	return c
}

// loweringShadow is the state of the shadow; the Proxy holds nil when it is off.
type loweringShadow struct {
	sample float64
	budget time.Duration
	slots  chan struct{}
	stop   chan struct{}

	mu           sync.Mutex // guards closed against wg.Add
	closed       bool
	wg           sync.WaitGroup
	lastLog      atomic.Int64 // unix nanoseconds of the last mismatch log line
	lastPanicLog atomic.Int64 // and of the last panic log line

	// Seams for the tests.
	lower func(*queryplan.Plan, lower.Profile) (physical.Op, error)
	rand  func() float64
}

// newLoweringShadow returns the shadow for a sample share, nil when it is off (0, negative or not a number).
func newLoweringShadow(sample float64) *loweringShadow {
	if !(sample > 0) {
		return nil
	}
	if sample > 1 {
		sample = 1
	}
	return &loweringShadow{
		sample: sample,
		budget: loweringShadowBudget,
		slots:  make(chan struct{}, loweringShadowSlots),
		stop:   make(chan struct{}),
		lower:  lower.Plan,
		rand:   rand.Float64,
	}
}

// close stops accepting comparisons and waits, until ctx ends, for the running ones: each stops at its next stage.
func (s *loweringShadow) close(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.stop)
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// shadowInput is what the string translator was given and answered for one query.
type shadowInput struct {
	query        string // the normalized client query, before the log ranges are canonicalized
	keepLine     bool
	label        translator.LabelTranslateFunc
	streamFields map[string]bool
	caps         logsql.Capabilities
	legacy       string // the translator's output, without the proxy's own "* " prefix
	legacyErr    error
}

func (in shadowInput) profile() lower.Profile {
	return loweringProfile(in.label, in.streamFields, in.caps, in.keepLine)
}

// shadowLowering is called on a translation cache miss once the translator's answer is computed and in use. It
// never blocks: it samples, takes a free slot and hands the comparison to a goroutine.
func (p *Proxy) shadowLowering(in shadowInput) {
	s := p.loweringShadow
	if !s.sampled() {
		return
	}
	if p.takeShadowSlot(s) {
		go func() {
			defer s.done()
			p.runLoweringShadow(s, in)
		}()
	}
}

// sampled reports whether this query is compared: the shadow is on and the sample draws it.
func (s *loweringShadow) sampled() bool {
	return s != nil && (s.sample >= 1 || s.rand() < s.sample)
}

// takeShadowSlot reserves a comparison: false when the proxy is closing, or when every slot is busy (counted as
// skipped). The caller runs the comparison on a goroutine that calls s.done.
func (p *Proxy) takeShadowSlot(s *loweringShadow) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.slots <- struct{}{}:
	default:
		p.recordLoweringShadow(shadowSkipped, shadowNoConstruct)
		return false
	}
	s.wg.Add(1)
	return true
}

// done frees the slot of a finished comparison.
func (s *loweringShadow) done() {
	<-s.slots
	s.wg.Done()
}

func (p *Proxy) recordLoweringShadow(result, construct string) {
	if p.metrics != nil {
		p.metrics.RecordLoweringShadow(result, shadowConstruct(construct))
	}
}

// runLoweringShadow compares the lowering with the string translator's answer and counts the outcome.
func (p *Proxy) runLoweringShadow(s *loweringShadow, in shadowInput) {
	run := p.newShadowRun(s)
	defer run.recoverPanic(in.query)

	var lowered physical.Op
	var newErr error
	plan, err := loweringPlan(noFlowCtx, logqlpkg.CanonicalizeLogRanges(in.query))
	if err == nil {
		run.construct = lower.Construct(plan.Root)
		if run.out() {
			run.result(shadowBudget)
			return
		}
		// The plan holds no text: a spelling the translator reads unlike the plan is left to it.
		if err = lower.ScreenText(in.query, plan); err == nil {
			lowered, err = s.lower(plan, in.profile())
		}
		if err == nil && physical.Evaluated(lowered) {
			// The proxy evaluates a ranking, stddev or stdvar itself, over the answer of its operand, and translates
			// the operand alone: the text the translator wrote for the whole query is not what it sends, so it is no
			// reference for these operators. The corpus shadow holds them to the proxy's route
			// (proxy.PostAggregationPhysical), and so does the post-aggregation handlers' live shadow
			// (shadowProxyEvaluated); a query that reaches the translator whole is one the route does not take.
			run.result(shadowNotLowered)
			return
		}
		if err != nil {
			var nl *lower.NotLoweredError
			switch {
			case in.legacyErr != nil && (errors.As(err, &nl) || errors.Is(err, lower.ErrNotLowered)):
				// The translator rejects the query and the lowering does not cover it: not compared.
				return
			case errors.As(err, &nl):
				run.construct = nl.Construct
				run.result(shadowNotLowered)
				return
			case errors.Is(err, lower.ErrNotLowered):
				run.result(shadowNotLowered)
				return
			}
		}
	}
	newErr = err
	if run.out() {
		run.result(shadowBudget)
		return
	}
	switch {
	case in.legacyErr != nil && newErr == nil:
		run.result(shadowOldError)
		return
	case in.legacyErr == nil && newErr != nil:
		run.result(shadowNewError)
		return
	case in.legacyErr != nil:
		// Both reject the query: nothing to compare (the error texts are the error layer's), as in the corpus
		// shadow, which skips a row the translator rejects.
		return
	}
	// The translator writes a label called not or in bare, which VictoriaLogs rejects; the lowering quotes it. The
	// translator's text is held to the lowering with that name quoted.
	want, err := translator.DecodePhysical(lower.QuoteKeywordFieldText(in.legacy))
	if err == nil && lower.UnwrapsReservedFieldOtherCase(plan) {
		// The lowering writes the unwrap of _TIME and the like in the outbound guard's form, which is what the proxy
		// sends for the translator's text too (guardOutboundQuery).
		want = lower.GuardOutbound(want)
	}
	if err != nil {
		run.result(shadowOldError)
		return
	}
	if run.out() {
		run.result(shadowBudget)
		return
	}
	if physical.Equal(want, lowered) {
		run.result(shadowMatch)
		return
	}
	run.result(shadowMismatch)
	p.logLoweringShadowMismatch(s, in, run.construct, lowered)
}

// shadowRun is one comparison in progress: its budget and the construct it has reached.
type shadowRun struct {
	p         *Proxy
	s         *loweringShadow
	deadline  time.Time
	construct string
}

func (p *Proxy) newShadowRun(s *loweringShadow) shadowRun {
	return shadowRun{p: p, s: s, deadline: time.Now().Add(s.budget), construct: shadowNoConstruct}
}

// out reports that the comparison has used its budget.
func (r *shadowRun) out() bool { return time.Now().After(r.deadline) }

// result counts an outcome under the construct reached, unless the proxy is closing, which ends the comparison at
// its next stage without counting it.
func (r *shadowRun) result(res string) {
	select {
	case <-r.s.stop:
	default:
		r.p.recordLoweringShadow(res, r.construct)
	}
}

// recoverPanic, deferred by a comparison, counts and logs a panic of it under the construct it had reached.
func (r *shadowRun) recoverPanic(query string) {
	if v := recover(); v != nil {
		r.p.recordLoweringShadow(shadowPanic, r.construct)
		// The panic value can hold query text, so it is not logged; the query is redacted like any other.
		if r.s.logAllowed(&r.s.lastPanicLog) {
			r.p.log.Error("lowering shadow panicked", "construct", shadowConstruct(r.construct), "logql", redactQuery(query, r.p.debugLogRawQueries))
		}
	}
}

// logLoweringShadowMismatch logs a mismatch, at most once per loweringShadowLogEvery (logAllowed), with the queries redacted
// like every other query log line (-debug-log-raw-queries shows them).
func (p *Proxy) logLoweringShadowMismatch(s *loweringShadow, in shadowInput, construct string, lowered physical.Op) {
	if !s.logAllowed(&s.lastLog) {
		return
	}
	p.log.Warn("lowering shadow: the lowering differs from the string translator",
		"construct", shadowConstruct(construct),
		"profile", fmt.Sprintf("%+v keepLine=%t streamFields=%d", in.caps, in.keepLine, len(in.streamFields)),
		"logql", redactQuery(in.query, p.debugLogRawQueries),
		"translator", redactQuery(in.legacy, p.debugLogRawQueries),
		"lowering", redactQuery(translator.EncodePhysical(lowered), p.debugLogRawQueries))
}

// logAllowed reports whether a log line may be written now, and if so takes the slot: one line per
// loweringShadowLogEvery for each last.
func (s *loweringShadow) logAllowed(last *atomic.Int64) bool {
	now := time.Now().UnixNano()
	prev := last.Load()
	return now-prev >= int64(loweringShadowLogEvery) && last.CompareAndSwap(prev, now)
}
