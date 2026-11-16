package proxy

import (
	"context"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankgrouping"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Every metric query of the plan corpus is translated to the same text with the flag on and off, under every
// translator profile of the route parity test, with the metric constructs of metricFlipCandidates served from the
// lowering: they write the string translator's text, byte for byte, through the proxy's own translation path, so a
// flip of them moves neither the translation cache, nor the requests VictoriaLogs sees, nor the keys of its answers. The one
// metric shape served with another text is the keep identity (keepIdentityConstruct, semantics/
// keep-before-unwrap-series-identity), which the handlers read too (keepUnwrapIdentity). Each of them is served
// on the corpus under every profile. (The log queries the lowering serves are held to the
// translator by the corpus shadow and the label-filter grids, with their recorded divergences.)
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_CorpusTextParity(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	total := map[string]int{}
	for _, prof := range loweringParityProfiles {
		t.Run(prof.name, func(t *testing.T) {
			start := func(disable bool) *Proxy {
				cfg := Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", DisableQueryPlanLowering: disable}
				prof.config(&cfg)
				p, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				if prof.version != "" {
					p.backendVersionMu.Lock()
					p.backendVersionSemver = prof.version
					p.backendVersionMu.Unlock()
				}
				return p
			}
			on, off := start(false), start(true)
			withFlipped(on, metricFlipCandidates...)
			// No translation cache: every row is translated, not read back.
			on.translationCache, off.translationCache = nil, nil
			ctx := context.Background()
			differs, keep, dynamic := 0, 0, 0
			for _, q := range corpus {
				if strings.HasPrefix(strings.TrimSpace(q), "{") {
					continue
				}
				got, gotErr := on.translateQueryOpts(ctx, q, false)
				// A query with an unspaced parser is held to the translator's text of its spaced spelling (respaced), one
				// with a `by` clause of blanks to its text of `by ()` (semantics/translator-reads-blank-grouping-as-none) and
				// one with a tab, a line feed or a carriage return between tokens to its text with a space
				// (semantics/translator-reads-only-space-between-tokens).
				want, wantErr := off.translateQueryOpts(ctx, spelledAsTranslatorReads(q), false)
				if _, kept := on.keepUnwrapIdentity(q); kept {
					keep++
					continue
				}
				if got != want && blankgrouping.Cut(q) != q {
					// A blank grouping on a construct the plan leaves to the translator (a ranking) is the translator's own
					// text of the query as written.
					if own, _ := off.translateQueryOpts(ctx, respaced(q), false); got == own {
						want = got
					}
				}
				// A parser's series identity is the plan's, held to Loki by the differentials (lower.DynamicUnwrapIdentity);
				// a request the handlers keep on the stream's series (streamIdentityKey) is served from the plan with the
				// translator's text, as the flip of its construct requires.
				if on.dynamicUnwrapIdentity(q) {
					dynamic++
					marked, markedErr := on.translateQueryOpts(context.WithValue(ctx, streamIdentityKey{}, true), q, false)
					if marked != want || (markedErr == nil) != (wantErr == nil) {
						if differs++; differs <= 10 {
							t.Errorf("%q on the stream's series:\n flag on  %q %v\n flag off %q %v", q, marked, markedErr, want, wantErr)
						}
					}
					continue
				}
				if got != want || (gotErr == nil) != (wantErr == nil) {
					if differs++; differs <= 10 {
						t.Errorf("%q:\n flag on  %q %v\n flag off %q %v", q, got, gotErr, want, wantErr)
					}
				}
			}
			if keep == 0 {
				t.Error("no keep identity row: the case tests nothing")
			}
			counts := shadowCounts(t, on)
			total["keep identity rows"] += keep
			total["dynamic identity rows"] += dynamic
			for _, c := range metricFlipCandidates {
				n := counts["flipped "+c]
				total[c] += n
				if n == 0 {
					t.Errorf("%s is flipped, but no corpus row is served from the lowering", c)
				}
			}
			if n := shadowTotal(shadowCounts(t, off), "flipped"); n != 0 {
				t.Fatalf("flag off: %d flipped translations", n)
			}
		})
	}
	constructs := make([]string, 0, len(total))
	for c := range total {
		constructs = append(constructs, c)
	}
	sort.Strings(constructs)
	for _, c := range constructs {
		t.Logf("served from the lowering: %-18s %d translations", c, total[c])
	}
}

// FuzzMetricFlipGate holds the metric flip to the string translator on any query: with the candidate metric
// constructs flipped, a query the lowering serves (writesTranslatorText passed) is the translator's text, or the keep
// identity; the gate and the lowering never panic out of lowerMetric. The translator's text is also taken with its
// keyword label names quoted (lower.QuoteKeywordFieldText), as the corpus shadow compares it: VictoriaLogs rejects
// them bare (semantics/translator-writes-keyword-field-names-bare). A `by` clause of only blanks is held to the
// translator's text of `by ()` (semantics/translator-reads-blank-grouping-as-none), and one with a tab, a line feed or
// a carriage return between tokens (after json, logfmt, regexp, pattern, unwrap, ...) to its text with a space
// (semantics/translator-reads-only-space-between-tokens).
//
// conformance: architecture-syntax-package-layering, semantics/translator-reads-blank-grouping-as-none, semantics/translator-reads-only-space-between-tokens
func FuzzMetricFlipGate(f *testing.F) {
	for _, q := range []string{
		`sum by (app) (rate({app="a"} |= "x" [5m]))`, `count_over_time({app="a"} | json | status="500" [5m])`,
		`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`, `label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`,
		`topk(2, sum by (app) (rate({app="a"}[5m])))`, `stddev(count_over_time({app="a"}[5m]))`,
		`sum without (pod) (rate({app="a"}[5m])) / 2`, `sum(count_over_time({app="a"} | json | n == 5 [5m]))`,
		`sum(count_over_time({app="a"} | _time="x" [5m]))`, `max_over_time({app="a"} | logfmt | unwrap d [5m]) by (app)`,
		`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`, `sum by (app) (bytes_over_time(({app="a"} |~ "x")[5m]))`,
		// The recorded divergences of log queries, inside a metric operand.
		"count_over_time({app=\"x\"} | s=`a detected_level b` [5m])",
		`sum(count_over_time({app="x"} | o="\x02\x00\x02" | json | K_extracted!~"p|q" [5m]))`,
		`sum(count_over_time({service_name="api-gateway"} | status > - .5 [5m]))`,
		`count_over_time({app="nginx"} | pattern "<_> <a><b" [5m])`,
		`count_over_time({app="a"} | json | not="x" [5m])`, `sum(count_over_time({app="a"} | n > +2 [5m]))`,
		// Spellings the fuzz found, as valid queries: a parser with no blank before its expression (the translator
		// copies it), a keyword in another case, a grouping with no labels.
		`count_over_time({app="a"} |pattern"<_> <m>" [5m])`, "count_over_time({app=\"a\"} |regexp`(?P<a>x)` [5m])",
		`first_over_time({app="a"} | json | keep app, n | unwrAp n [5m])`, `max_over_time({app="a"} | logfmt | unwrap A [5m]) by ()`,
		// A grouping of blanks, which the translator reads as no grouping and the plan as `by ()`.
		`sum by( ) (rate({A="0"}[1y]))`, "sum by(\t) (count_over_time({A=\"0\"}[5m]))", `sum(rate({A="0"}[5m])) by(  )`,
		// A tab, a line feed or a carriage return where the translator reads a space: after a parser keyword, a stage name
		// and a function name.
		"first_over_time({A00=\"0\"} | json\rA0| unwrap A0[0y])", "count_over_time({A=\"0\"} | json\tA0 [5m])",
		"count_over_time({A=\"0\"} | logfmt\r\na [5m])", "count_over_time({A=\"0\"} | regexp\n`(?P<a>x)` [5m])",
		"count_over_time({A=\"0\"} | pattern\r\"<a> x\" [5m])", "sum(count_over_time({A=\"0\"} | json | label_format\ta=b [5m]))",
		"max_over_time({A=\"0\"}| unwrap\rA[0])", "sum_over_time({A=\"0\"} | json |\tunwrap\nduration(a) [5m])",
		// An unwrap with no parser before it after another blank, and keywords written with the Kelvin sign or U+0130,
		// which the request path writes in lower case.
		"max_over_time({A=\"0\"} | unwrap\tA [5m])", "sum(max_over_time({A=\"0\"} |\nunwrap\r\nA [5m]))",
		"top\u212A(2, sum by (app) (rate({app=\"a\"}[5m])))", "avg_over_t\u0130me({app=\"a\"} | unwrap n [5m])",
		"sum w\u0130thout (pod) (rate({app=\"a\"}[5m]))",
	} {
		f.Add(q)
	}
	newProxy := func(disable bool) *Proxy {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error",
			LabelStyle: LabelStyleUnderscores, DisableQueryPlanLowering: disable})
		if err != nil {
			f.Fatal(err)
		}
		f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		p.translationCache = nil
		return p
	}
	on, off := newProxy(false), newProxy(true)
	withFlipped(on, metricFlipCandidates...)
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 || strings.HasPrefix(strings.TrimSpace(q), "{") {
			return
		}
		// The query as the handlers and the translation read it (lowercaseQueryKeywords): keywords in lower case,
		// without comments and parser flags.
		q = logqlpkg.LowercaseKeywords(logqlpkg.StripParserFlags(logqlpkg.StripComments(q)))
		// A query Loki rejects is rejected by the request's validation before any translation (an empty selector, an
		// empty pattern): neither path translates it.
		if logqlpkg.ParseAndValidate(q) != nil {
			return
		}
		got, gotErr := on.translateQueryOpts(context.Background(), q, false)
		// The translator reads a grouping of blanks (`by( )`) as no grouping, Loki and the plan as `by ()`
		// (semantics/translator-reads-blank-grouping-as-none), and a tab, a line feed or a carriage return after a
		// parser keyword as part of the text (semantics/translator-reads-only-space-between-tokens): it is held to the
		// spelling it reads as Loki does.
		want, wantErr := off.translateQueryOpts(context.Background(), spelledAsTranslatorReads(q), false)
		if _, kept := on.keepUnwrapIdentity(q); kept {
			return
		}
		// A query with an unspaced parser the screen leaves to the translator gets the translator's own text, as with
		// nothing flipped; one the plan serves may escape a byte the spaced spelling's text copies raw.
		respelled, delegated := false, false
		if respaced(blankspace.Collapse(q)) != q {
			unflipped, unflippedErr := off.translateQueryOpts(context.Background(), q, false)
			// A construct the plan leaves to the translator (its text and its failure are the translator's own) is not
			// served from this text: the handlers read it through their own path, which TestBlankAfterKeywordRequests
			// holds to the spaced spelling's requests.
			delegated = got == unflipped && (gotErr == nil) == (unflippedErr == nil)
			respelled = delegated || sameLogsQL(got, want)
		}
		// A blank grouping on a construct the plan leaves to the translator (a ranking) is the translator's own text.
		if cut := blankgrouping.Cut(q); cut != q && !respelled {
			unflipped, _ := off.translateQueryOpts(context.Background(), respaced(q), false)
			respelled = got == unflipped
		}
		// A control byte in a quoted string: the plan writes it as an escape (`\x1d`), the translator copies the byte.
		// VictoriaLogs reads both as the same query.
		if !respelled && strings.IndexFunc(q, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
			respelled = sameLogsQL(got, want)
		}
		if (got != want && got != lower.QuoteKeywordFieldText(want) && !respelled) || ((gotErr == nil) != (wantErr == nil) && !delegated) {
			t.Fatalf("%q:\n flag on  %q %v\n flag off %q %v", q, got, gotErr, want, wantErr)
		}
	})
}

// spelledAsTranslatorReads is q in the spelling the string translator reads as Loki does: each run of the blanks Loki
// skips written as one space, a parser expression with a blank before it, and a `by` clause of only blanks as `by ()`.
// Loki's tree holds the same query for every spelling of it (Loki oracle, TestBlankAfterKeywordMatchesLoki).
func spelledAsTranslatorReads(q string) string {
	return respaced(blankgrouping.Cut(blankspace.Collapse(q)))
}

// The metric screen (lower.ScreenMetric) holds to the plan on every metric query of the corpus under every translator
// profile, with the metric constructs flipped: it names the plan's root for every query the plan serves; the queries
// it passes that the plan then leaves to the translator (a parse, build and resolve on top of the translation) stay
// under 0.5% of those it passes; and the served queries it turns away (translated by the string translator at its own
// cost) under 2% of the served ones. The keep identity has its own path and is not counted.
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_MetricScreenCorpus(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var served, passed, wasted, lost int
	for _, prof := range loweringParityProfiles {
		cfg := Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error"}
		prof.config(&cfg)
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		if prof.version != "" {
			p.backendVersionMu.Lock()
			p.backendVersionSemver = prof.version
			p.backendVersionMu.Unlock()
		}
		withFlipped(p, metricFlipCandidates...)
		ctx := context.Background()
		for _, q := range corpus {
			normalized := strings.TrimSpace(q)
			if normalized == "" || normalized[0] == '{' {
				continue
			}
			canon := logqlpkg.CanonicalizeLogRanges(normalized)
			plan, err := loweringPlan(ctx, canon)
			if err != nil || lower.KeepDecidesUnwrapIdentity(plan) {
				continue
			}
			caps := p.logsqlCapabilities()
			caps.NoDetectedLevelBodyScan = !p.detectedLevelBodyScan
			prof := loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, p.plainStreamLabelNames(ctx, normalized)), p.streamFieldsMap, caps, false)
			// What the typed gate serves, without the screen; and what the proxy serves, with it.
			construct := lower.Construct(plan.Root)
			ok := slices.Contains(p.flippedConstructs, construct) && writesTranslatorText(plan, prof)
			if ok {
				_, err := lower.PlanQuery(canon, plan, prof)
				ok = err == nil
			}
			screen := lower.ScreenMetric(canon)
			if _, servedNow := p.lowerMetric(ctx, canon, prof); servedNow != (ok && screen == construct) && !strings.Contains(canon, "keep") {
				t.Errorf("%q: the proxy serves it: %v; the gate %v, the screen %q", canon, servedNow, ok, screen)
			}
			if ok {
				served++
			}
			if screen != "" {
				passed++
			}
			switch {
			case ok && screen != "" && screen != construct:
				t.Errorf("%q: the screen names %s, the plan's root is %s", canon, screen, construct)
			case ok && screen == "":
				lost++
			case !ok && screen != "":
				if wasted++; wasted <= 5 {
					t.Logf("passed by the screen, left to the translator: %q", canon)
				}
			}
		}
	}
	t.Logf("metric rows served %d, passed by the screen %d, passed and left to the translator %d, served and turned away %d", served, passed, wasted, lost)
	if served == 0 || wasted*200 > passed || lost*50 > served {
		t.Fatalf("the screen passes %d rows the plan leaves to the translator (of %d passed) and turns away %d served rows (of %d)", wasted, passed, lost, served)
	}
}
