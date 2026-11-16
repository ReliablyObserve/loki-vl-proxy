package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// logStageFlipCandidates are the stage families of log queries the tests serve from the lowering on a proxy of their
// own (withFlippedLogStages), to prove a flip of them ready.
var logStageFlipCandidates = []string{"LevelFilter", "UnspacedParser", "JSON", "Logfmt", "DropKeep", "LabelFormat", "LineFormat", "RegexpPattern"}

// unspacedParserRE finds a regexp or pattern parser written with no blank before its quoted expression.
var unspacedParserRE = regexp.MustCompile("(\\|\\s*(?:regexp|pattern))([\"`])")

// respaced is q with a blank between each regexp or pattern parser and its expression: the spelling the string
// translator writes as the plan writes both (semantics/translator-copies-unspaced-parser), where it copies the
// unspaced one into a pipe VictoriaLogs rejects. The tests hold a query with an unspaced parser to the translator's
// text of its spaced spelling.
func respaced(q string) string { return unspacedParserRE.ReplaceAllString(q, "$1 $2") }

// withFlippedLogStages makes p serve the stage families it serves today and the given ones from the lowering.
func withFlippedLogStages(p *Proxy, names ...string) {
	p.flippedLogStages = lower.LogStagesOf(append(append([]string(nil), QueryPlanFlippedLogStages...), names...))
}

// newLoweringProfileProxy is a proxy of one of loweringParityProfiles, without a translation cache.
func newLoweringProfileProxy(t testing.TB, name string, config func(*Config), version string) *Proxy {
	t.Helper()
	cfg := Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error"}
	config(&cfg)
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if version != "" {
		p.backendVersionMu.Lock()
		p.backendVersionSemver = version
		p.backendVersionMu.Unlock()
	}
	p.translationCache = nil
	return p
}

// logLoweringProfile is the lowering profile the proxy's translation gives a log query (after backendLogQuery).
func logLoweringProfile(ctx context.Context, p *Proxy, normalized string, keepLine bool) lower.Profile {
	caps := p.logsqlCapabilities()
	caps.NoDetectedLevelBodyScan = !p.detectedLevelBodyScan
	return loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, p.plainStreamLabelNames(ctx, normalized)), p.streamFieldsMap, caps, keepLine)
}

// Every log query of the plan corpus is translated to the same text, through the request's own translation
// (translateLogResponseQuery, which keeps the line aside for a line_format the Loki-compatible profile evaluates), with
// the candidate stage families served from the lowering and with none: the plan writes the string translator's text,
// byte for byte, so the flip moves neither the translation cache, nor the requests VictoriaLogs sees, nor the keys of
// its answers. Each candidate family is served on the corpus under every profile.
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_LogCorpusTextParity(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	candidates := lower.LogStagesOf(logStageFlipCandidates)
	total := map[string]int{}
	for _, prof := range loweringParityProfiles {
		t.Run(prof.name, func(t *testing.T) {
			on := newLoweringProfileProxy(t, prof.name, prof.config, prof.version)
			off := newLoweringProfileProxy(t, prof.name, prof.config, prof.version)
			withFlippedLogStages(on, logStageFlipCandidates...)
			off.flippedLogStages = 0
			ctx := context.Background()
			differs := 0
			served := map[lower.LogStages]int{}
			for _, q := range corpus {
				if !strings.HasPrefix(strings.TrimSpace(q), "{") {
					continue
				}
				got, gotErr := on.translateLogResponseQuery(ctx, q)
				want, wantErr := off.translateLogResponseQuery(ctx, respaced(q))
				if got != want || (gotErr == nil) != (wantErr == nil) {
					if differs++; differs <= 10 {
						t.Errorf("%q:\n flip on  %q %v\n flip off %q %v", q, got, gotErr, want, wantErr)
					}
				}
				query, keepLine := on.backendLogQuery(q)
				normalized := strings.TrimSpace(query)
				canon := logqlpkg.CanonicalizeLogRanges(normalized)
				if !logqlpkg.MayHaveNonFilterStage(canon) {
					continue
				}
				if _, ok := on.lowerLogSelect(ctx, canon, logLoweringProfile(ctx, on, normalized, keepLine)); !ok {
					continue
				}
				plan, err := loweringPlan(ctx, canon)
				if err != nil {
					t.Fatalf("%q: served, but does not plan: %v", q, err)
				}
				// A query with an unspaced parser is served with its regexp and pattern parsers (UnspacedParser).
				stages, _ := lower.PlanLogStages(plan)
				allowed := candidates
				if respaced(q) != q {
					allowed |= lower.LogRegexpPattern
					stages |= lower.LogUnspacedParser
				}
				if stages == 0 || stages&^allowed != 0 {
					t.Errorf("%q: served with stage families %b, the candidates are %b", q, stages, allowed)
				}
				served[stages]++
			}
			for _, name := range logStageFlipCandidates {
				st := lower.LogStagesOf([]string{name})
				n := 0
				for set, c := range served {
					if set&st != 0 {
						n += c
					}
				}
				total[name] += n
				if n == 0 {
					t.Errorf("%s is flipped, but no corpus row is served from the lowering", name)
				}
			}
		})
	}
	names := make([]string, 0, len(total))
	for n := range total {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t.Logf("served from the lowering: %-14s %d translations", n, total[n])
	}
}

// The log screen (lower.ScreenLogStages) holds to the typed gate on every log query of the corpus with more than line
// and label filters, under every translator profile, with the candidate families flipped: it names the plan's
// families for every query the plan serves; the queries it passes that the plan then leaves to the translator (a
// parse, build and resolve on top of the translation) stay under 0.5% of those it passes; and the served queries it
// turns away (translated by the string translator at its own cost) under 2% of the served ones.
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_LogScreenCorpus(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var served, passed, wasted, lost int
	for _, prof := range loweringParityProfiles {
		p := newLoweringProfileProxy(t, prof.name, prof.config, prof.version)
		withFlippedLogStages(p, logStageFlipCandidates...)
		ctx := context.Background()
		for _, q := range corpus {
			if !strings.HasPrefix(strings.TrimSpace(q), "{") {
				continue
			}
			query, keepLine := p.backendLogQuery(q)
			normalized := strings.TrimSpace(query)
			canon := logqlpkg.CanonicalizeLogRanges(normalized)
			if !logqlpkg.MayHaveNonFilterStage(canon) {
				continue
			}
			plan, err := loweringPlan(ctx, canon)
			if err != nil {
				continue
			}
			lp := logLoweringProfile(ctx, p, normalized, keepLine)
			// What the typed gate serves, without the screen; and what the proxy serves, with it.
			allowed := p.flippedLogStages
			screen := lower.ScreenLogStagesIn(canon, allowed)
			if screen&allowed&lower.LogUnspacedParser != 0 {
				allowed |= lower.LogRegexpPattern
			}
			ok := servesLogStages(plan, lp, allowed)
			if ok {
				_, err := lower.Plan(plan, lp)
				ok = err == nil
			}
			planned, _ := lower.PlanLogStages(plan)
			screenOK := screen != 0 && screen&^allowed == 0
			if _, servedNow := p.lowerLogSelect(ctx, canon, lp); servedNow != (ok && screenOK) {
				t.Errorf("%q: the proxy serves it: %v; the gate %v, the screen %b", canon, servedNow, ok, screen)
			}
			if ok {
				served++
			}
			if screenOK {
				passed++
			}
			switch {
			case ok && screenOK && screen&^lower.LogUnspacedParser != planned:
				t.Errorf("%q: the screen names families %b, the plan holds %b", canon, screen, planned)
			case ok && !screenOK:
				if lost++; lost <= 5 {
					t.Logf("served by the gate, turned away by the screen: %q", canon)
				}
			case !ok && screenOK:
				if wasted++; wasted <= 5 {
					t.Logf("passed by the screen, left to the translator: %q", canon)
				}
			}
		}
	}
	t.Logf("log rows served %d, passed by the screen %d, passed and left to the translator %d, served and turned away %d", served, passed, wasted, lost)
	if served == 0 || wasted*200 > passed || lost*50 > served {
		t.Fatalf("the screen passes %d rows the plan leaves to the translator (of %d passed) and turns away %d served rows (of %d)", wasted, passed, lost, served)
	}
}

// FuzzLogFlipGate holds the log stage flip to the string translator on any log query: with the candidate stage
// families flipped, a query is translated to the same text as with none flipped (the translator's, or the plan's for a
// filter-only query, which the filter flip already serves), or to the translator's text with its keyword label names
// quoted (lower.QuoteKeywordFieldText: VictoriaLogs rejects them bare, semantics/translator-writes-keyword-field-names-bare);
// the screen, the gate and the lowering never panic out of lowerLogSelect. The translator is held to the spelling it
// reads as Loki does (spelledAsTranslatorReads), as in FuzzMetricFlipGate: a tab, a line feed or a carriage return
// after a parser keyword is Loki's blank, which the translator copies into a pipe VictoriaLogs rejects
// (semantics/translator-reads-only-space-between-tokens).
//
// conformance: architecture-syntax-package-layering, semantics/translator-reads-only-space-between-tokens
func FuzzLogFlipGate(f *testing.F) {
	for _, q := range []string{
		`{app="a"} | detected_level="error"`, `{service_name="a"} |= "x" | detected_level=~"error|warn" | status >= 500`,
		`{app="a"} | detected_level="error" | s="a detected_level b"`, `{app="a"} | detected_level="error" | n > -5`,
		`{app="a"} | detected_level="error" | _time="x"`, `{app="a"} | detected_level="error" | o="\x01"`,
		`{app="a"} | (detected_level="error" or detected_level="warn") and not="x"`, `{app="a"} | json | detected_level="x"`,
		`{app="a"} | detected_level="x" | json | status == 500`, `{app="a"} | service_name="x" | detected_level="y"`,
		`{app="a"} | detected_level`, `{app="a"} | json | status="500"`, `{app="a"} |pattern"<_> <m>"`,
		`{app="a"} |= "x" | json m="a.b", n="c" | m="x"`, `{app="a"} | logfmt --strict | status >= 500 | path = "/a"`,
		`{app="a"} | json | drop a, b | keep c, d`, `{app="a"} | keep a="x", b="y"`, `{app="a"} | drop detected_level`,
		`{app="a"} | json | label_format x="{{.a}}", y=z`, `{app="a"} | json | line_format "{{.a}} {{.b}}"`,
		`{app="a"} | pattern "<_> <m> <_>" | m="GET"`, "{app=\"a\"} | regexp `(?P<m>[A-Z]+) ` | m=\"GET\"",
		`{app="a"} | logfmt | line_format "{{.msg}}" | json | level="error"`, `{app="a"} | json | _time="x"`,
		// A blank Loki skips other than a space before a parser's expression (the translator copies the parser as written,
		// semantics/translator-reads-only-space-between-tokens), capture names with a leading digit, and the characters
		// Loki rejects there.
		"{A=\"0\"}|regexp\t`(?<0>0)`", "{A=\"0\"} |regexp\n`(?P<0a>x)`", "{A=\"0\"}|pattern\r\n\"<a> x\"",
		"{A=\"0\"} | regexp \t\"(?P<9>x)(?P<b>y)\"", "{A=\"0\"} |= \"x\" |regexp\r`(?<a>0)`", "{A=\"0\"}|regexp\v`(?<a>0)`",
		"{A=\"0\"}|regexp\f`(?<a>0)`", "{A=\"0\"}|pattern\u0085\"<a> x\"", "{A=\"0\"}|regexp\u00a0`(?<a>0)`",
	} {
		f.Add(q)
	}
	newProxy := func() *Proxy {
		p := newLoweringProfileProxy(f, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
		return p
	}
	on, off := newProxy(), newProxy()
	withFlippedLogStages(on, logStageFlipCandidates...)
	off.flippedLogStages = 0
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 || !strings.HasPrefix(strings.TrimSpace(q), "{") {
			return
		}
		// The query as the handlers and the translation read it (lowercaseQueryKeywords), and only one the request's
		// validation accepts.
		q = logqlpkg.LowercaseKeywords(logqlpkg.StripParserFlags(logqlpkg.StripComments(q)))
		if logqlpkg.ParseAndValidate(q) != nil {
			return
		}
		if err := logFlipGateHolds(on, off, q); err != "" {
			t.Fatal(err)
		}
	})
}

// logFlipGateHolds is FuzzLogFlipGate's check of one valid log query on the flipped (on) and unflipped (off) proxies:
// empty when the flipped text is the translator's of the spelling it reads as Loki does, else the difference.
func logFlipGateHolds(on, off *Proxy, q string) string {
	ctx := context.Background()
	reads := spelledAsTranslatorReads(q)
	got, gotErr := on.translateLogResponseQuery(ctx, q)
	want, wantErr := off.translateLogResponseQuery(ctx, reads)
	// A respelled parser's expression is quoted by the plan, where the translator copies the spaced spelling's as it
	// is written: a control byte is escaped in one and raw in the other, the same string to LogsQL.
	respelled := reads != q && sameLogsQL(got, want)
	// A query with an unspaced parser the screen leaves to the translator gets the translator's own text, as with
	// nothing flipped.
	if !respelled && reads != q {
		unflipped, _ := off.translateLogResponseQuery(ctx, q)
		respelled = got == unflipped
	}
	if (got != want && got != lower.QuoteKeywordFieldText(want) && !respelled) || (gotErr == nil) != (wantErr == nil) {
		return fmt.Sprintf("%q:\n flip on  %q %v\n flip off %q %v", q, got, gotErr, want, wantErr)
	}
	return ""
}

// The log flip gate's edge table: a regexp or pattern parser with each spelling of the blank before its expression,
// each quote and capture names Loki reads (a leading digit included: Loki accepts `(?<0>0)`, Loki oracle, pipeline
// command). A blank Loki skips (none, a space, a tab, a line feed, a carriage return, a run of them) is the spaced
// query: the proxy serves the plan's extract pipe, the gate holds it to the translator's text of the spaced spelling,
// and the translator's own text of a spelling other than a space is the parser copied as written (the recorded
// divergence semantics/translator-reads-only-space-between-tokens, or translator-copies-unspaced-parser with no
// blank). A character Loki does not skip (a vertical tab, a form feed, U+0085, U+00A0, a control byte) is rejected by
// the request's validation (Loki's text: internal/logql TestBlankAfterKeywordMatchesLoki), so nothing is translated.
//
// conformance: semantics/translator-reads-only-space-between-tokens, semantics/translator-copies-unspaced-parser, architecture-syntax-package-layering
func TestLogFlipGate_BlankBeforeParserExpression(t *testing.T) {
	newProxy := func() *Proxy {
		return newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	}
	on, off, shipped := newProxy(), newProxy(), newProxy()
	withFlippedLogStages(on, logStageFlipCandidates...)
	off.flippedLogStages = 0
	ctx := context.Background()
	parsers := []string{
		"regexp`(?<0>0)`", "regexp`(?<a>0)`", "regexp\"(?P<0a>x)(?P<b>y)\"", "regexp`(?P<9>x)(?P<_0>y)`", "regexp`(?<1_>x)`",
		"pattern\"<a> <_>\"", "pattern`<_> <m> <_>`",
	}
	var served, rejected int
	for _, lead := range []string{`{A="0"}|`, `{A="0"} | `, `{A="0"} |= "x" |`} {
		for _, parser := range parsers {
			cut := strings.IndexAny(parser, "\"`")
			for _, blank := range append([]string{"", " "}, blankspace.Skipped...) {
				q := lead + parser[:cut] + blank + parser[cut:]
				if err := logqlpkg.ParseAndValidate(q); err != nil {
					t.Errorf("%q: Loki reads a blank here, the validation rejects it: %v", q, err)
					continue
				}
				if err := logFlipGateHolds(on, off, q); err != "" {
					t.Error(err)
				}
				got, err := shipped.translateLogResponseQuery(ctx, q)
				spaced, _ := shipped.translateLogResponseQuery(ctx, lead+parser[:cut]+" "+parser[cut:])
				if err != nil || got != spaced || !strings.Contains(got, "| extract") {
					t.Errorf("%q: served %q (%v), the spaced spelling %q", q, got, err, spaced)
				}
				if own, _ := off.translateLogResponseQuery(ctx, q); !strings.HasPrefix(blank, " ") && own == got {
					t.Errorf("%q: the string translator writes the plan's text %q: the recorded divergence is gone", q, own)
				}
				served++
			}
			for _, blank := range blankspace.Rejected {
				q := lead + parser[:cut] + blank + parser[cut:]
				if logqlpkg.ParseAndValidate(q) == nil {
					t.Errorf("%q: Loki rejects the character, the validation accepts it", q)
				}
				rejected++
			}
		}
	}
	t.Logf("%d spellings served from the plan, %d rejected", served, rejected)
}

// sameLogsQL reports two LogsQL texts the LogsQL parser reads as the same query.
func sameLogsQL(a, b string) bool {
	qa, err := logsql.Parse(a)
	if err != nil {
		return false
	}
	qb, err := logsql.Parse(b)
	return err == nil && qa.String() == qb.String()
}

// A regexp or pattern parser written with no blank before its quoted expression is served from the plan with the
// text the string translator writes for the spelling with the blank (semantics/translator-copies-unspaced-parser): a
// log query, with its other regexp and pattern parsers, and a metric construct served from the plan over one, also as
// the operand of a ranking or stddev the proxy evaluates itself. The translator copies the unspaced parser into a pipe
// VictoriaLogs rejects. A query with another stage the plan does not serve, and a stddev nested under an aggregation
// (translated whole by the string translator), keep the translator's text.
//
// conformance: semantics/translator-copies-unspaced-parser, architecture-syntax-package-layering
func TestQueryPlanLowering_ServesUnspacedParsers(t *testing.T) {
	p := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	off := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores; c.DisableQueryPlanLowering = true }, "")
	ctx := context.Background()
	for _, q := range []string{
		`{app="a"} |pattern"<_> <m> <_>"`, `{app="a"} |pattern"<_> <m> <_>" | m="GET"`, "{app=\"a\"} |regexp`(?P<m>[A-Z]+) ` | m!=\"x\"",
		`{app="a"} |= "x" |pattern"<_> <m> <_>" | pattern "<n> <_>" | n="1"`, `{app="a"} | detected_level="error" |pattern"<_> <m>"`,
		`count_over_time({app="a"} |pattern"<_> <m> <_>" [5m])`, `sum by (m) (rate({app="a"} |regexp"(?P<m>[A-Z]+) " [5m]))`,
	} {
		got, err := p.translateLogResponseQuery(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		want, err := off.translateLogResponseQuery(ctx, respaced(q))
		if err != nil {
			t.Fatal(err)
		}
		if got != want || strings.Contains(got, "pattern\"") || strings.Contains(got, "regexp") && !strings.Contains(got, "extract_regexp") {
			t.Errorf("%q: served %q, want the spaced spelling's %q", q, got, want)
		}
		legacy, err := off.translateLogResponseQuery(ctx, q)
		if err != nil || legacy == got {
			t.Errorf("%q: the string translator writes %q (%v): the divergence is gone", q, legacy, err)
		}
	}
	// Left to the string translator: a stage the plan does not serve.
	if q := `{app="a"} |pattern"<_> <m>" | decolorize`; true {
		got, _ := p.translateLogResponseQuery(ctx, q)
		want, _ := off.translateLogResponseQuery(ctx, q)
		if got != want {
			t.Errorf("%q: served %q, the translator writes %q", q, got, want)
		}
	}
	// Through a request: a ranking and a stddev the proxy evaluates send their operand's extract text; a stddev nested
	// under an aggregation is translated whole by the string translator and still sends the parser as written.
	up := &upstreamRecorder{}
	vl := httptest.NewServer(http.HandlerFunc(up.handler))
	t.Cleanup(vl.Close)
	rp := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle, c.BackendURL = LabelStyleUnderscores, vl.URL }, "")
	for _, tc := range []struct {
		query   string
		extract bool
	}{
		{`topk(2, sum by (m) (count_over_time({app="a"} |pattern"<_> <m> <_>" [5m])))`, true},
		{`stddev by (m) (count_over_time({app="a"} |pattern"<_> <m> <_>" [5m]))`, true},
		{`{app="a"} |pattern"<_> <m> <_>" | m="GET"`, true},
		{`sum(stddev by (m) (count_over_time({app="a"} |pattern"<_> <m> <_>" [5m])))`, false},
	} {
		up.take()
		target := "/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&limit=100&query=" + url.QueryEscape(tc.query)
		if rec := doCompatProxyRequest(rp, target, map[string]string{"X-Scope-OrgID": "0"}); rec.Code != http.StatusOK {
			t.Fatalf("%q: %d %s", tc.query, rec.Code, rec.Body)
		}
		reqs := strings.Join(up.take(), "\n")
		if got := strings.Contains(reqs, "extract") && !strings.Contains(reqs, "pattern%22") && !strings.Contains(reqs, `pattern"`); got != tc.extract {
			t.Errorf("%q: VictoriaLogs gets the extract text: %v, want %v\n%s", tc.query, got, tc.extract, reqs)
		}
	}
	// With the flag off, and with the family not flipped, the translator answers.
	none := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	none.flippedLogStages = lower.LogStagesOf([]string{"LevelFilter"})
	q := `{app="a"} |pattern"<_> <m> <_>" | m="GET"`
	if got, _ := none.translateLogResponseQuery(ctx, q); !strings.Contains(got, `pattern"`) {
		t.Errorf("not flipped: %q served %q", q, got)
	}
}

// parserFormatFamilies are the parser and formatting stage families, each with log queries the plan serves once the
// family is flipped and ones it leaves to the string translator whatever is flipped.
var parserFormatFamilies = []struct {
	family     string
	served     []string
	turnedAway []string
}{
	{"JSON", []string{
		`{app="a"} | json`, `{app="a"} |= "x" | json | status="500"`, `{app="a"} | json | status >= 500 | path=~"/api.*"`,
		`{app="a"} | json m="a.b", n="c" | m="x"`, `{app="a"} | json | (status="500" or level!="info")`,
		`{app="a"} | json | k8s.pod.name="p"`,
	}, []string{
		`{app="a"} | json | status == 500`, `{app="a"} | json | detected_level="error"`, `{app="a"} | json | service_name="x"`,
		`{app="a"} | json | unpack`,
	}},
	{"Logfmt", []string{
		`{app="a"} | logfmt`, `{app="a"} | logfmt | status >= 500 | path = "/api"`, `{app="a"} | logfmt --strict | level="error"`,
		`{app="a"} | logfmt a="b" | a!="x"`,
	}, []string{`{app="a"} | logfmt | status = 500`, `{app="a"} | logfmt | detected_level="x"`}},
	{"DropKeep", []string{
		`{app="a"} | drop a, b`, `{app="a"} | keep c, d`, `{app="a"} | drop a="x", b`, `{app="a"} | keep a="x", b="y"`,
		`{app="a"} |= "x" | drop detected_level`,
	}, []string{`{app="a"} | keep a="x", b`, `{app="a"} | drop service_name`}},
	{"LabelFormat", []string{
		`{app="a"} | label_format x="{{.a}}"`, `{app="a"} | label_format x="{{ .a }} {{.b}}", y=z`,
	}, []string{`{app="a"} | label_format x="{{.a | upper}}"`, `{app="a"} | label_format x="<a>"`}},
	{"LineFormat", []string{
		`{app="a"} | line_format "{{.a}} {{.b}}"`, `{app="a"} |= "x" | line_format "plain"`,
	}, []string{`{app="a"} | line_format "{{.a.b}}"`, `{app="a"} | line_format "{{ printf \"%s\" .a }}"`, `{app="a"} | line_format"{{.a}}"`}},
	{"RegexpPattern", []string{
		`{app="a"} | pattern "<_> <m> <_>" | m="GET"`, "{app=\"a\"} | regexp `(?P<m>[A-Z]+) ` | m=\"GET\"",
		`{app="a"} | regexp "(?P<n>\\d+)" | n > 5`,
	}, []string{`{app="a"} | pattern "\x1a<m>"`, `{app="a"} | regexp "(?P<n>\x41)"`}},
}

// A log query of a flipped parser or formatting stage family is served from the plan with the string translator's
// text, and one of a family not flipped, or one the family leaves to the translator (a numeric equality after a
// parser, a level filter after a stage, a template that is more than fields, a keep of names and matchers), is
// translated by the string translator. With -query-plan-lowering=false nothing is served.
//
// conformance: architecture-syntax-package-layering
func TestQueryPlanLowering_ServesParserAndFormatStages(t *testing.T) {
	ctx := context.Background()
	p := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	off := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores; c.DisableQueryPlanLowering = true }, "")
	flipped := lower.LogStagesOf(QueryPlanFlippedLogStages)
	for _, fam := range parserFormatFamilies {
		on := flipped&lower.LogStagesOf([]string{fam.family}) != 0
		for _, q := range fam.served {
			want, err := off.translateLogResponseQuery(ctx, q)
			if err != nil {
				t.Fatalf("%s %q: %v", fam.family, q, err)
			}
			got, err := p.translateLogResponseQuery(ctx, q)
			if err != nil || got != want {
				t.Errorf("%s %q: %q %v, want the translator's text %q", fam.family, q, got, err, want)
			}
			if _, served := p.lowerLogSelect(ctx, q, logLoweringProfile(ctx, p, q, false)); served != on {
				t.Errorf("%s %q: served from the plan %v, the family is flipped %v", fam.family, q, served, on)
			}
		}
		for _, q := range fam.turnedAway {
			if text, served := p.lowerLogSelect(ctx, q, logLoweringProfile(ctx, p, q, false)); served {
				t.Errorf("%s %q: served %q, want the translator to answer", fam.family, q, text)
			}
		}
	}
	// With the flag off, no translation is served from the plan, every family flipped.
	withFlippedLogStages(off, logStageFlipCandidates...)
	for _, fam := range parserFormatFamilies {
		for _, q := range fam.served {
			if _, err := off.translateLogResponseQuery(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := shadowCounts(t, off)["flipped LogSelect"]; n != 0 {
		t.Errorf("flag off: %d translations served from the plan", n)
	}
}
