package proxy

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// A fence screen decides from the query text, before any parse, whether a response key carries a version constant.
// The parser accepts spellings the screens must read as well: every keyword in any case (TOPK(, sum WITHOUT,
// | UNWRAP, | KEEP) and any character Go calls white space before the parenthesis of a function keyword
// (sort\v(). These tests parse a query and read the construct a fence guards from the tree, then require the
// fence to fire: a query whose tree holds the construct and whose key lacks the fence would serve a reply cached
// before the behaviour change.

// fenceFacts are the constructs the response key fences guard, read from the parsed tree.
type fenceFacts struct {
	keepBeforeUnwrap  bool // a keep stage before an unwrap stage of one log query
	keep              bool
	ip                bool // an ip() line filter or label filter
	labelFilterStages int
	reservedCase      bool // a matcher or label filter on _time, _stream or _stream_id in another case
	reservedLabel     bool // a matcher or label filter on _time, _stream, _stream_id in any case, or _msg
	keywordSpelling   bool // a label spelled Ip, IP or Label_replace
	presence          bool // a label filter or line filter `!= ""`
	logfmt            bool
	json              bool
	// perStreamSeries is a construct whose series the proxy reads per stream: a without fold, an unwrap, or topk,
	// bottomk, sort, sort_desc or label_replace.
	perStreamSeries bool
	// rankOverBinary is a ranking, stddev or stdvar whose operand is a binary operation.
	rankOverBinary bool
	// rankOverBinaryLabelReplace is such an operand that holds a label_replace call.
	rankOverBinaryLabelReplace bool
	// levelChain is a read that may carry the detected_level chain: a range aggregation, or a stage that names
	// detected_level (a label filter on it, keep or drop of it).
	levelChain bool
	// emptyWithoutOperand is a binary operation with an operand that is a sum, avg, min or max `without ()`.
	emptyWithoutOperand bool
}

// keepsEveryOperandSeries reports a sum, avg, min or max with a `without` clause of no label, read from the tree.
func keepsEveryOperandSeries(e logqlpkg.Expr) bool {
	v, ok := e.(*logqlpkg.VectorAggregation)
	if !ok || v.Grouping == nil || !v.Grouping.Without || len(v.Grouping.Labels) != 0 {
		return false
	}
	switch v.Op {
	case logqlpkg.VectorSum, logqlpkg.VectorAvg, logqlpkg.VectorMin, logqlpkg.VectorMax:
		return true
	}
	return false
}

func (f *fenceFacts) name(name string, comparison bool) {
	if name == detectedLevelLabel {
		f.levelChain = true
	}
	if !comparison {
		return
	}
	if logsql.ReservedFieldNameOtherCase(name) {
		f.reservedCase = true
	}
	if reservedLabelWord(name) {
		f.reservedLabel = true
	}
}

func (f *fenceFacts) labelName(name string) {
	if name == detectedLevelLabel {
		f.levelChain = true
	}
	// The scanner lower cases a word with strings.ToLower before it looks the word up.
	if lower := strings.ToLower(name); (lower == "ip" && name != "ip") || (lower == "label_replace" && name != "label_replace") {
		f.keywordSpelling = true
	}
}

// comparesLabel reports a label filter expression with a comparison in it.
func comparesLabel(e *logqlpkg.LabelFilterExpr) bool {
	if e == nil {
		return false
	}
	if e.Op == logqlpkg.LabelFilterLeaf {
		return e.Cmp != ""
	}
	return comparesLabel(e.Left) || comparesLabel(e.Right)
}

func (f *fenceFacts) labelFilter(e *logqlpkg.LabelFilterExpr) {
	if e == nil {
		return
	}
	if e.Op == logqlpkg.LabelFilterLeaf {
		f.name(e.Name, true)
		f.labelName(e.Name)
		// A string literal the parser cannot read leaves the leaf's kind empty (the stage is still accepted): read its text.
		if e.Kind == logqlpkg.LabelFilterValueIP || e.Kind == logqlpkg.LabelFilterValueNone && containsFold(e.Leaf, "ip") {
			f.ip = true
		}
		if e.Cmp == "!=" && e.Kind == logqlpkg.LabelFilterValueString && e.Value == "" {
			f.presence = true
		}
		return
	}
	f.labelFilter(e.Left)
	f.labelFilter(e.Right)
}

func (f *fenceFacts) logQuery(lq *logqlpkg.LogQuery) {
	for _, m := range lq.Selector.Matchers {
		f.name(m.Name, true)
		f.labelName(m.Name)
	}
	keepAt := -1
	for i, st := range lq.Pipeline {
		switch s := st.(type) {
		case *logqlpkg.KeepStage:
			f.keep = true
			if keepAt < 0 {
				keepAt = i
			}
			for _, m := range s.Matchers {
				f.name(m.Name, true)
			}
			for _, l := range s.Labels {
				f.labelName(l)
			}
		case *logqlpkg.DropStage:
			for _, m := range s.Matchers {
				f.name(m.Name, true)
			}
			for _, l := range s.Labels {
				f.labelName(l)
			}
		case *logqlpkg.UnwrapStage:
			f.perStreamSeries = true
			if keepAt >= 0 {
				f.keepBeforeUnwrap = true
			}
		case *logqlpkg.LabelFilterStage:
			// A stage with no comparison (`| a`) filters nothing the fence is about.
			if comparesLabel(s.Expr) {
				f.labelFilterStages++
			}
			f.labelFilter(s.Expr)
		case *logqlpkg.LineFilterStage:
			if s.IP {
				f.ip = true
			}
			for _, a := range s.Or {
				if a.IP {
					f.ip = true
				}
			}
			if s.Op == logqlpkg.LineFilterExcludes && s.Value == "" {
				f.presence = true
			}
		case *logqlpkg.ParserStage:
			switch s.Type {
			case logqlpkg.ParserLogfmt:
				f.logfmt = true
			case logqlpkg.ParserJSON:
				f.json = true
			}
		}
	}
}

func (f *fenceFacts) expr(e logqlpkg.Expr) {
	switch e := e.(type) {
	case *logqlpkg.LogQuery:
		f.logQuery(e)
	case *logqlpkg.RangeAggregation:
		f.levelChain = true
		f.expr(e.Inner)
	case *logqlpkg.VectorAggregation:
		switch e.Op {
		case logqlpkg.VectorTopK, logqlpkg.VectorBottomK, logqlpkg.VectorSort, logqlpkg.VectorSortDesc:
			f.perStreamSeries = true
		}
		if e.Grouping != nil && e.Grouping.Without {
			f.perStreamSeries = true
		}
		if holdsBinaryOperand(e.Inner) {
			switch e.Op {
			case logqlpkg.VectorTopK, logqlpkg.VectorBottomK, logqlpkg.VectorSort, logqlpkg.VectorSortDesc, logqlpkg.VectorStddev, logqlpkg.VectorStdvar:
				f.rankOverBinary = true
				f.rankOverBinaryLabelReplace = f.rankOverBinaryLabelReplace || exprHoldsLabelReplace(e.Inner)
			}
		}
		f.expr(e.Inner)
	case *logqlpkg.BinOpExpr:
		if keepsEveryOperandSeries(e.Left) || keepsEveryOperandSeries(e.Right) {
			f.emptyWithoutOperand = true
		}
		f.expr(e.Left)
		f.expr(e.Right)
	case *logqlpkg.OpaqueMetricExpr:
		if e.Func == "label_replace" {
			f.perStreamSeries = true
		}
		if e.Inner != nil {
			f.expr(e.Inner)
		}
	}
}

// factsOf parses query as the proxy does and reads the facts; ok is false for a query the parser or the validation
// rejects (it has no tree).
func factsOf(query string) (facts fenceFacts, expr logqlpkg.Expr, ok bool) {
	expr, msg := logqlpkg.ValidateLogQLExpr(query)
	if msg != "" || expr == nil {
		return facts, nil, false
	}
	facts.expr(expr)
	return facts, expr, true
}

// fenceComments are `#` comments to the end of a line, with quote characters a scanner that skips quoted text must
// not read as one.
var fenceComments = []string{"#c\n", "# \"\n", "# `\n", "#'\n", "#\n", "# a=\"b\n"}

var fenceBlanks = []string{" ", "\t", "\n", "\r", "\v", "\f", " ", " ", "\u0085", "　", " ", " "}

// respell rewrites query outside quoted text: random case for words, random white space for blanks and between
// tokens. Every result is a spelling the parser may accept or reject; the tests read the tree of the ones it
// accepts.
func respell(query string, rng *rand.Rand) string {
	var b strings.Builder
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '`':
			j := skipQuoted(query, i)
			b.WriteString(query[i : j+1])
			i = j + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(query) && (query[j] == '_' || query[j] == '.' || query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9') {
				j++
			}
			w := query[i:j]
			switch rng.Intn(10) {
			case 0:
				w = strings.ToUpper(w)
			case 3:
				// The letters that lower case to an ASCII k or i read as those letters in a keyword.
				w = strings.NewReplacer("k", "\u212a", "i", "\u0130", "s", "\u017f").Replace(w)
			case 1:
				w = strings.ToTitle(w[:1]) + w[1:]
			case 2:
				bs := []byte(w)
				for k := range bs {
					if rng.Intn(2) == 0 {
						bs[k] ^= 0x20 * byte(boolInt(bs[k] >= 'a' && bs[k] <= 'z' || bs[k] >= 'A' && bs[k] <= 'Z'))
					}
				}
				w = string(bs)
			}
			b.WriteString(w)
			i = j
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			b.WriteString(fenceBlank(rng, 12))
			if rng.Intn(8) == 0 {
				b.WriteString(fenceBlank(rng, 12))
			}
			i++
		case c == '(' || c == '|' || c == '=' || c == '!' || c == ',' || c == ')' || c == '[' || c == '{' || c == '~':
			if rng.Intn(10) == 0 {
				b.WriteString(fenceBlank(rng, 12))
			}
			b.WriteByte(c)
			if rng.Intn(10) == 0 {
				b.WriteString(fenceBlank(rng, 12))
			}
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// fenceBlank is a blank: mostly one of the four the lexer skips, and with a 1 in unicodeOdds chance any character Go
// calls white space.
func fenceBlank(rng *rand.Rand, unicodeOdds int) string {
	if rng.Intn(8) == 0 {
		return fenceComments[rng.Intn(len(fenceComments))]
	}
	if rng.Intn(unicodeOdds) == 0 {
		return fenceBlanks[rng.Intn(len(fenceBlanks))]
	}
	return fenceBlanks[rng.Intn(4)]
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// fenceShapeRequest is a query_range request with a window and a step, so Grafana range tokens resolve.
func fenceShapeRequest(query string) *http.Request {
	v := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}
	return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
}

// fenceWants is, per fence, whether the parsed tree of query holds the construct the fence guards; nil for a query
// the parser rejects (it has no tree).
func fenceWants(query string) map[string]bool {
	f, expr, ok := factsOf(query)
	if !ok {
		return nil
	}
	r := fenceShapeRequest(query)
	want := map[string]bool{
		"keep-metadata-projection": f.keep,
		"keep-unwrap-identity":     f.keepBeforeUnwrap,
		"ip-label-filter":          f.ip,
		"repeated-label-filters":   f.labelFilterStages >= 2,
		"reserved-field-case":      f.reservedCase,
		"reserved-field-labels":    f.reservedLabel,
		"keyword-label-names":      f.keywordSpelling,
		"stream-level-series":      f.perStreamSeries || f.levelChain,
		"rank-over-binary":         f.rankOverBinary,
		"rank-over-binary-lr":      f.rankOverBinaryLabelReplace,
		"binary-empty-without":     f.emptyWithoutOperand,
		// An unwrap stage of the tree that the text does not write `| unwrap ` (outside quotes and comments): the
		// printed tree writes each stage so.
		"unwrap-blank-spelling": strings.Count(textOutsideQuotes(expr.String()), "| unwrap ") >
			strings.Count(strings.ToLower(textOutsideQuotes(query)), "| unwrap "),
		// Read by fenceGains: the text screen of repeated filters also counts a quoted text that looks like a stage.
		"a-label-filter-stage": f.labelFilterStages >= 1,
	}
	// The dynamic unwrap identity is a plan attribute: the screen in front of the plan must let every query through
	// whose plan holds it. The plan is built here from the tree of the query as written, not read from the memo the
	// fence reads (which holds the plan of the keyword lower cased text).
	if plan, err := queryplan.Build(expr, queryplan.Inputs{}); err == nil {
		queryplan.Resolve(plan)
		want["unwrap-dynamic-identity"] = lower.WritesDynamicUnwrapIdentity(plan)
		// The request has a step of 15s: an extraction list's identity at the root over a longer range is fenced.
		if va, ok := plan.Root.(*queryplan.VectorAgg); ok && listedWindowsPlan(plan) {
			if ra, ok := va.In.(*queryplan.RangeAgg); ok && ra.Range > 15*time.Second {
				want["extraction-list-windows"] = true
			}
		}
	}
	resolved := resolveGrafanaRangeTemplateTokens(query, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if rexpr, err := logqlpkg.Parse(resolved); err == nil {
		want["ordered-json-vector"] = containsOrderedJSONVectorExpr(rexpr)
	}
	if spec, ok := parseBareParserMetricCompatSpec(resolved); ok && strings.Contains(spec.baseQuery, "| json") {
		want["bare-json-metric"] = true
	}
	if ra, ok := ungroupedSumRange(resolved); ok && ra.Op == logqlpkg.RangeCountOverTime && f.logfmt && f.presence {
		if lq, ok := ra.Inner.(*logqlpkg.LogQuery); ok {
			for _, st := range lq.Pipeline {
				if p, ok := st.(*logqlpkg.ParserStage); ok && p.Type == logqlpkg.ParserLogfmt {
					want["logfmt-presence"] = true
				}
			}
		}
	}
	return want
}

// fenceMisses lists the fences a query's tree requires and its key lacks.
func fenceMisses(query string) []string {
	var miss []string
	for name, want := range fenceWants(query) {
		if want && name != "a-label-filter-stage" && !fenceFires(name, query) {
			miss = append(miss, name)
		}
	}
	sort.Strings(miss)
	return miss
}

var fenceSpellingBases = []string{
	"rate({app=\"a\"} |\tunwrap n [5m])", "sum(max_over_time({app=\"a\"} | json |\r\nunwrap  bytes(n) [5m]))",
	`sum_over_time({app="a"} |  unwrap n [5m]) / sum_over_time({app="a"} | unwrap n [5m])`,
	`{app="a"} | json | unwrap n | keep a`,
	`{app="a"} | decolorize | keep a, b | json | line_format "{{.a}}"`,
	`{app="a"} | json | _time > 5s | _msg = "x" | _stream != "y"`,
	`sum by (a) (count_over_time({app="a"} | json | _TIME = "x" [5m]))`,
	`{app="a"} | json | drop level="x" | keep _stream_id = "z"`,
	`{app="a"} != ip("1.2.3.4") or ip("5.6.7.8")`,
	`{app="a"} | json | a=~"x" | b!~"y" | c>=1 | d<2 | e==3 | f!=4`,
	`sum by (x) (avg by (y) (max by (z) (count_over_time({app="a"} | json [5m]))))`,
	`bottomk(2, sum by (x) (count_over_time({app="a"} | json [5m])))`,
	`topk(2, count_over_time({app="a"} | json [5m]))`,
	`count_over_time({app="a"} | json | drop __error__ [5m])`,
	`sum(count_over_time({app="a"} | logfmt | a="b" | msg != "" [5m]))`,
	`{app="a"} | json | line_format "{{ . }}" | label_format a=b | unpack | pattern "<x>" | regexp "(?P<x>.)"`,

	`{app="a"} | keep level`,
	`{app="a"} | json | keep level, msg | unwrap n`,
	`sum by (x) (sum_over_time({app="a"} | json | keep x | unwrap n [5m]))`,
	`{app="a"} | status = ip("10.0.0.0/8")`,
	`{app="a"} |= ip("10.0.0.1")`,
	`{app="a"} | json | a="b" | c="d"`,
	`{app="a"} | json | (a="b") | (c="d")`,
	`{_TIME="a"}`,
	`{app="a"} | _TIME >= 5s`,
	`{app="a"} | _Stream_Id = "x"`,
	`{app="a"} | _msg = "x"`,
	`{Ip="a"}`,
	`{app="a"} | drop Label_replace`,
	`topk(3, sum by (x) (count_over_time({app="a"} | json x="x" [5m])))`,
	`sum by (x) (sum by (y) (count_over_time({app="a"} | json [5m])))`,
	`sum without (x) (count_over_time({app="a"} | json [5m])) / sum(count_over_time({app="a"} | json [5m]))`,
	`bottomk(2, max by (x) (count_over_time({app="a"} | json | drop __error__ [5m])))`,
	`sum(count_over_time({app="a"} | json [5m] offset 1h))`,
	`sum by (level) (count_over_time({app="a"} | json [$__auto]))`,
	`quantile_over_time(0.9, {app="a"} | json | unwrap d [5m])`,
	`sum(count_over_time({app="a"} | logfmt | msg != "" [5m]))`,
	`sum(count_over_time({app="a"} | logfmt | msg != "" [$__auto]))`,
	`sum(count_over_time({app="a"} | logfmt | msg != ` + "``" + ` [5m]))`,
	`sum(rate({app="a"} | json | keep level | status = ip("1.2.3.4") | b = "x" [5m]))`,
	`sum without (pod) (rate({app="a"} | json [1m]))`,
	`sort(rate({app="a"} | json [1m]))`,
	`sort_desc (sum by (x) (rate({app="a"}[1m])))`,
	`label_replace(rate({app="a"}[1m]), "x", "$1", "pod", "(.*)")`,
	`sum by (x) (avg_over_time({app="a"} | json | unwrap n [5m]))`,
	`max(stddev_over_time({app="a"} | logfmt | unwrap n [5m]))`,
	`min by (x) (min_over_time({app="a"} | json | unwrap n [5m])) / sum(max_over_time({app="a"} | json | unwrap n [5m]))`,
	`topk(2, sum by (x) (rate({app="a"}[1m])) / sum by (x) (rate({app="b"}[1m])))`,
	`sort_desc(sum by (x) (count_over_time({app="a"}[1m])) > bool 3)`,
	`sum(bottomk(1, sum by (x) (rate({app="a"}[1m])) and sum by (x) (rate({app="b"}[1m]))))`,
	`stddev by (x) (sum by (x, y) (rate({app="a"}[1m])) * 2) + 1`,
	`topk(2, label_replace(sum by (x) (rate({app="a"}[1m])), "y", "x-$1", "x", "(.*)") * 2)`,
	`sum without () (count_over_time({app="a"} | json [5m])) / 2`,
	`max without () (rate({app="a"}[5m])) > on(app) group_left sum by (app) (rate({app="a"}[5m]))`,
}

// TestResponseKeyFencesFireOnEverySpelling re-spells each base query many ways and requires every fence whose
// construct the parsed tree holds to fire.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesFireOnEverySpelling(t *testing.T) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	t.Cleanup(querycorpus.PauseRecording())
	rng := rand.New(rand.NewSource(1))
	seen := map[string]string{}
	accepted, withFacts := 0, 0
	for _, base := range fenceSpellingBases {
		for i := 0; i < 4000; i++ {
			q := base
			if i > 0 {
				q = respell(base, rng)
			}
			if f, _, ok := factsOf(q); ok {
				accepted++
				if f.keep || f.ip || f.json {
					withFacts++
				}
			}
			for _, m := range fenceMisses(q) {
				if _, dup := seen[m+"|"+base]; !dup {
					seen[m+"|"+base] = q
				}
			}
		}
	}
	t.Logf("%d accepted spellings, %d with a construct", accepted, withFacts)
	for k, q := range seen {
		t.Errorf("fence %s did not fire for %q (%q)", k, q, fmt.Sprintf("%+q", q))
	}
}

// hasCommentQuote reports a `#` followed, on its line, by a quote character. The older screens read such a quote as the
// start of quoted text and fenced (or missed) what follows it by accident; the screens now skip the comment's quotes,
// so those queries may change key once. It is a text test: a `#` inside a string counts too.
func hasCommentQuote(query string) bool {
	for i := 0; i < len(query); i++ {
		if query[i] != '#' {
			continue
		}
		line := query[i:]
		if end := strings.IndexByte(line, '\n'); end >= 0 {
			line = line[:end]
		}
		if strings.ContainsAny(line, "\"`") {
			return true
		}
	}
	return false
}

// fenceScreens is the pure text screens, by fence name, with the frozen older screen of each.
var fenceScreens = []struct {
	name string
	old  func(string) bool
	now  func(string) string
}{
	{"keep-metadata-projection", refKeepMetadata, keepMetadataProjectionResponseKeyVersion},
	{"keep-unwrap-identity", refKeepUnwrap, keepUnwrapIdentityResponseKeyVersion},
	{"ip-label-filter", refIP, ipLabelFilterResponseKeyVersion},
	{"repeated-label-filters", refRepeatedLabelFilter, repeatedLabelFilterResponseKeyVersion},
	{"reserved-field-case", refReservedCase, reservedFieldCaseResponseKeyVersion},
	{"reserved-field-labels", refReservedLabel, reservedFieldLabelResponseKeyVersion},
	{"keyword-label-names", refKeywordLabel, keywordLabelResponseKeyVersion},
	{"stream-level-series", refStreamLevelSeries, streamLevelSeriesResponseKeyVersion},
	// New with the plan serving an unwrap written with other blanks: no older screen fired it.
	{"unwrap-blank-spelling", func(string) bool { return false }, unwrapBlankSpellingResponseKeyVersion},
}

// fenceLosses lists the fences an older binary's screen fired for that the screen now lacks: a response key it wrote
// would no longer be reachable. It reads every query, the ones the parser rejects too (the compat edge looks its cache
// up before it validates), except a query with a quote in a comment: a one-time cache miss, see hasCommentQuote.
func fenceLosses(query string) []string {
	if hasCommentQuote(query) {
		return nil
	}
	var lost []string
	for _, sc := range fenceScreens {
		if sc.old(query) && sc.now(query) == "" {
			lost = append(lost, sc.name)
		}
	}
	// The three screens that parse after a text prefilter: the prefilter alone must keep every query it let through.
	check := func(name string, old bool, now bool) {
		if old && !now {
			lost = append(lost, name)
		}
	}
	check("ordered-json-vector prefilter", refOrderedJSONCandidate(query), orderedJSONVectorFenceCandidate(query, spelledOtherwise(query)))
	check("bare-json-metric prefilter", refBareJSONText(query), containsFold(query, "json"))
	check("logfmt-presence prefilter", refLogfmtPresenceText(query), containsFold(query, "logfmt") && emptyStringComparisonIn(query))
	q := dynamicQuery(query)
	check("unwrap-dynamic-identity prefilter", refDynamicIdentityScreen(q), dynamicIdentityFenceScreen(q, strings.Contains(q, "\u0130")))
	return lost
}

// fenceGains lists the fences the screens fire for now that their older form did not, and that the parsed tree of the
// query does not justify. A query the parser rejects has no tree and holds no gain to justify. The screens are text
// screens, a superset of the construct (`_time!0` fires the reserved field label fence), so this reads the
// corpus of real queries, not the fuzzer's.
func fenceGains(query string) []string {
	want := fenceWants(query)
	if want == nil || strings.Contains(query, "#") {
		// The words of a comment are read like any other (as the older screens read them), so a screen can fire on
		// a word in a comment that holds no construct of the tree.
		return nil
	}
	var unjustified []string
	for _, sc := range fenceScreens {
		// The keep and unwrap fence is a text screen by design: a query whose keep decides nothing is fenced too, so
		// a keep stage is the construct it answers to.
		justified := want[sc.name] || sc.name == "keep-unwrap-identity" && want["keep-metadata-projection"] ||
			sc.name == "repeated-label-filters" && want["a-label-filter-stage"]
		if !sc.old(query) && sc.now(query) != "" && !justified {
			unjustified = append(unjustified, sc.name)
		}
	}
	return unjustified
}

// TestResponseKeyFencesKeepTheirOldKeys requires every screen to fire on every query its older form fired on, over
// the corpus the key tests use: the queries clients send in the spelling they send them.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesKeepTheirOldKeys(t *testing.T) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	t.Cleanup(querycorpus.PauseRecording())
	seen := map[string]bool{}
	var corpus []string
	add := func(qs ...string) {
		for _, q := range qs {
			if !seen[q] {
				seen[q] = true
				corpus = append(corpus, q)
			}
		}
	}
	add(fenceSyntheticQueries...)
	add(fenceSpellingBases...)
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			if strings.Contains(s, "{") {
				add(s)
			}
		}
	}
	add(fenceCorpusYAMLQueries(t)...)
	if len(corpus) < 300 {
		t.Fatalf("query corpus has %d entries", len(corpus))
	}
	changed := 0
	for _, q := range corpus {
		if lost := fenceLosses(q); len(lost) > 0 {
			t.Errorf("%q: fences %v fired before and do not now", q, lost)
		}
		if gain := fenceGains(q); len(gain) > 0 {
			t.Errorf("%q: fences %v fire now and the parsed tree does not hold their construct", q, gain)
		}
		for _, sc := range fenceScreens {
			if !sc.old(q) && sc.now(q) != "" {
				changed++
				break
			}
		}
		if miss := fenceMisses(q); len(miss) > 0 {
			t.Errorf("%q: fences %v do not fire for the construct in the tree", q, miss)
		}
		if emptyWithoutOverFires(q) {
			t.Errorf("%q: the binary empty without fence fires and the parsed tree holds no such operand", q)
		}
	}
	t.Logf("%d of %d corpus queries carry a fence their older screens did not fire, each justified by its parsed tree", changed, len(corpus))
}

// FuzzResponseKeyFencesMatchParsedTree checks every response key fence against the parsed tree: a query whose tree
// holds the construct a fence guards must fire it, whatever the case of its keywords, the white space and comments
// between its tokens, the letters of its names. The seeds are the fence corpus queries re-spelled (seed 0 is the
// query as written); the fuzzer mutates the text and the spelling seed. A query must also keep every fence its older
// screen fired.
//
// conformance: cache-response-key-versions
func FuzzResponseKeyFencesMatchParsedTree(f *testing.F) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	f.Cleanup(querycorpus.PauseRecording())
	for i, q := range fenceSpellingBases {
		f.Add(q, uint64(0))
		f.Add(q, uint64(i+1))
	}
	for _, q := range fenceSyntheticQueries {
		f.Add(q, uint64(7))
	}
	f.Fuzz(func(t *testing.T, query string, seed uint64) {
		if len(query) > 2048 {
			t.Skip()
		}
		if seed != 0 {
			query = respell(query, rand.New(rand.NewSource(int64(seed))))
		}
		if miss := fenceMisses(query); len(miss) > 0 {
			t.Fatalf("%+q: fences %v do not fire for the construct in the parsed tree", query, miss)
		}
		if lost := fenceLosses(query); len(lost) > 0 {
			t.Fatalf("%+q: fences %v fired before and do not now", query, lost)
		}
		if emptyWithoutOverFires(query) {
			t.Fatalf("%+q: the binary empty without fence fires and the parsed tree holds no such operand", query)
		}
	})
}

// emptyWithoutOverFires reports a query whose key carries the binary empty without fence while its parsed tree holds
// no binary operation with a sum, avg, min or max `without ()` operand: the fence is new, so no older screen states
// where it fired, and it must fire nowhere else (no other query's key changes).
func emptyWithoutOverFires(query string) bool {
	return emptyWithoutOperandResponseKeyVersion(query) != "" && !fenceWants(query)["binary-empty-without"]
}

// fenceFires reports whether the named fence is in the response key chain of query (a query_range request).
func fenceFires(name, query string) bool {
	r := fenceShapeRequest(query)
	return map[string]func() string{
		"keep-metadata-projection": func() string { return keepMetadataProjectionResponseKeyVersion(query) },
		"keep-unwrap-identity":     func() string { return keepUnwrapIdentityResponseKeyVersion(query) },
		"ip-label-filter":          func() string { return ipLabelFilterResponseKeyVersion(query) },
		"repeated-label-filters":   func() string { return repeatedLabelFilterResponseKeyVersion(query) },
		"reserved-field-case":      func() string { return reservedFieldCaseResponseKeyVersion(query) },
		"reserved-field-labels":    func() string { return reservedFieldLabelResponseKeyVersion(query) },
		"keyword-label-names":      func() string { return keywordLabelResponseKeyVersion(query) },
		"ordered-json-vector":      func() string { return orderedJSONVectorResponseKeyVersion(r, query) },
		"bare-json-metric":         func() string { return bareJSONMetricResponseKeyVersion(r, query) },
		"logfmt-presence":          func() string { return logfmtPresenceResponseKeyVersion(r, query) },
		"stream-level-series":      func() string { return streamLevelSeriesResponseKeyVersion(query) },
		"unwrap-dynamic-identity":  func() string { return dynamicUnwrapIdentityResponseKeyVersion(query) },
		"rank-over-binary": func() string {
			return rankOverBinaryFence(query)
		},
		"rank-over-binary-lr": func() string {
			return rankOverBinaryLabelReplaceFence(query)
		},
		"binary-empty-without":    func() string { return emptyWithoutOperandResponseKeyVersion(query) },
		"unwrap-blank-spelling":   func() string { return unwrapBlankSpellingResponseKeyVersion(query) },
		"extraction-list-windows": func() string { return listedWindowsResponseKeyVersion(r, query) },
	}[name]() != ""
}

// TestResponseKeyFencesFireOnTheSpellingsTheParserReads pins one query per spelling class a fence screen missed:
// keywords in any case, a letter that lower cases to an ASCII one (U+212A, U+0130), `#` comments between tokens (with
// quote characters in them), a grouped label filter, an empty back-quoted string, names with non-ASCII letters. Each
// row parses to a tree that holds the construct (checked by the row's facts) and must carry the fence; the rows after
// them are queries that must not.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesFireOnTheSpellingsTheParserReads(t *testing.T) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	t.Cleanup(querycorpus.PauseRecording())
	for _, tc := range []struct {
		fence, query string
		want         bool
	}{
		{"keep-metadata-projection", `{a="b"} | keep level`, true},
		{"keep-metadata-projection", `{a="b"} | KEEP level`, true},
		{"keep-metadata-projection", `{a="b"} | Keep level`, true},
		{"keep-metadata-projection", `{a="b"} |keep level`, true},
		{"keep-metadata-projection", "{a=\"b\"} |\n\t# comment\n keep level", true},
		{"keep-metadata-projection", "{a=\"b\"} | keep#c\nlevel", true},
		{"keep-metadata-projection", "{a=\"b\"} | Keep level", true},
		{"keep-metadata-projection", `{a="b"} | keeper="x"`, false},
		{"keep-metadata-projection", `{a="b"} | json | drop keep`, false},
		{"keep-metadata-projection", `{a="b"} |= "keep"`, false},
		{"keep-unwrap-identity", `sum(sum_over_time({a="b"} | json | KEEP x | UNWRAP n [5m]))`, true},
		{"keep-unwrap-identity", `sum(sum_over_time({a="b"} | json | Keep x | Unwrap n [5m]))`, true},
		{"keep-unwrap-identity", `sum(sum_over_time({a="b"} | json | keep x | unwrap n [5m]))`, true},
		{"keep-unwrap-identity", `sum(sum_over_time({a="b"} | json | unwrap n [5m]))`, false},
		{"keep-unwrap-identity", `{a="b"} | json | keep x`, false},
		{"ip-label-filter", `{a="b"} | x = ip("10.0.0.0/8")`, true},
		{"ip-label-filter", `{a="b"} | x = IP("10.0.0.0/8")`, true},
		{"ip-label-filter", `{a="b"} | x != Ip ("10.0.0.0/8")`, true},
		{"ip-label-filter", `{a="b"} |= iP("10.0.0.1")`, true},
		{"ip-label-filter", `{a="b"} != IP("10.0.0.1") or ip("10.0.0.2")`, true},
		{"ip-label-filter", "{a=\"b\"} | x = #c\nip(\"10.0.0.1\")", true},
		{"ip-label-filter", "{a=\"b\"} | x #c\n= ip(\"10.0.0.1\")", true},
		{"ip-label-filter", "{a=\"b\"} |= İp(\"10.0.0.1\")", true},
		{"ip-label-filter", "{a=\"b\"} | é = IP(\"10.0.0.1\")", true},
		{"ip-label-filter", `{a="b"} | x = zip("10.0.0.1")`, false},
		{"ip-label-filter", `{a="b"} |= "IP(10.0.0.1)"`, false},
		{"ip-label-filter", `sum by (IP) (count_over_time({a="b"}[5m]))`, false},
		{"repeated-label-filters", `{a="b"} | json | x="1" | y="2"`, true},
		{"repeated-label-filters", `{a="b"} | json | (x="1") | (y="2")`, true},
		{"repeated-label-filters", "{a=\"b\"} | json |#c\nx=\"1\" |# \"\n y>2", true},
		{"repeated-label-filters", "{a=\"b\"} | json | é=\"1\" | ü!=\"2\"", true},
		{"repeated-label-filters", `{a="b"} | json | x="1" | y`, false},
		{"repeated-label-filters", `{a="b"} | json | x="1" and y="2"`, false},
		{"repeated-label-filters", `{a="b"} | json | x="1"`, false},
		{"reserved-field-case", `{_TIME="x"}`, true},
		{"reserved-field-case", "{a=\"b\"} | _TIME #c\n>= 5s", true},
		{"reserved-field-case", "{a=\"b\"} # \"\n | _STREAM_ID = \"x\"", true},
		{"reserved-field-case", "{a=\"b\"} | _TİME > 5s", true},
		{"reserved-field-case", `{a="b"} | _time > 5s`, false},
		{"reserved-field-labels", `{_msg="x"}`, true},
		{"reserved-field-labels", "{a=\"b\"} # `\n | _Stream != \"x\"", true},
		{"reserved-field-labels", "{a=\"b\"} | _TİME > 5s", true},
		{"reserved-field-labels", `{a="b"} |= "_msg = x"`, false},
		{"keyword-label-names", `{Ip="a"}`, true},
		{"keyword-label-names", "{a=\"b\"} # \"\n | drop Json", true},
		{"keyword-label-names", "{a=\"b\"} | drop Keep", true},
		{"keyword-label-names", "{a=\"b\"} | drop éKeep", false},
		{"keyword-label-names", `{a="b"} | json | unwrap n`, false},
		{"keyword-label-names", `{a="b"} | JSON | UNWRAP n`, false},
		{"ordered-json-vector", `bottomk(2, sum by (x) (count_over_time({a="b"} | json [5m])))`, true},
		{"ordered-json-vector", `BOTTOMK(2, SUM BY (x) (COUNT_OVER_TIME({a="b"} | JSON [5m])))`, true},
		{"ordered-json-vector", `sum by (x) (sum by (y) (count_over_time({a="b"} | json [5m])))`, true},
		{"ordered-json-vector", `SUM BY (x) (Sum By (y) (Count_Over_Time({a="b"} | JSON [5m])))`, true},
		{"ordered-json-vector", `sum by (x) (count_over_time({a="b"} | json [5m]))`, false},
		{"bare-json-metric", `count_over_time({a="b"} | json | drop __error__ [5m])`, true},
		{"bare-json-metric", `COUNT_OVER_TIME({a="b"} | JSON | drop __error__ [5m])`, true},
		{"bare-json-metric", `count_over_time({a="b"} |JSON| drop __error__ [5m])`, true},
		{"bare-json-metric", `count_over_time({a="b"} | logfmt [5m])`, false},
		{"logfmt-presence", `sum(count_over_time({a="b"} | logfmt | msg != "" [5m]))`, true},
		{"logfmt-presence", "sum(count_over_time({a=\"b\"} | logfmt | msg != `` [5m]))", true},
		{"logfmt-presence", `SUM(COUNT_OVER_TIME({a="b"} | LOGFMT | msg != "" [5m]))`, true},
		{"logfmt-presence", "sum(count_over_time({a=\"b\"} | logfmt | msg !=#c\n \"\" [5m]))", true},
		{"logfmt-presence", `sum(count_over_time({a="b"} | logfmt | msg != "x" [5m]))`, false},
		{"stream-level-series", `TOPK(2, count_over_time({a="b"} | json [5m]))`, true},
		{"stream-level-series", "top\u212a(2, count_over_time({a=\"b\"} | json [5m]))", true},
		{"stream-level-series", "bottom\u212a(2, count_over_time({a=\"b\"} | json [5m]))", true},
		{"stream-level-series", "Sort_Desc\t(rate({a=\"b\"} | json [1m]))", true},
		// Every range aggregation may read the detected_level chain, and so may a stage naming detected_level.
		{"stream-level-series", `sum by (pod) (rate({a="b"}[1m]))`, true},
		{"stream-level-series", "sum by (pod) (rate({a=\"b\"} #sort\n[1m]))", true},
		{"stream-level-series", `{a="b"} | detected_level="error"`, true},
		{"stream-level-series", `{a="b"} | json | keep detected_level`, true},
		{"stream-level-series", `{a="b"} | json | level="error"`, false},
		{"stream-level-series", `{a="b"} | regexp "(?P<n>[0-9]+)"`, false},
		{"stream-level-series", `{a="b"} | s=` + "`a detected_level b`", false},
		{"stream-level-series", `{a="b"} | detected_level_extracted="error"`, false},
		{"unwrap-dynamic-identity", `sum by (x) (avg_over_time({a="b"} | json | unwrap n [5m]))`, true},
		{"unwrap-dynamic-identity", `SUM BY (x) (AVG_OVER_TIME({a="b"} | JSON | UNWRAP n [5m]))`, true},
		{"unwrap-dynamic-identity", "sum by (x) (avg_over_t\u0130me({a=\"b\"} | json | unwrap n [5m]))", true},
		{"unwrap-dynamic-identity", "MAX(MAX_OVER_T\u0130ME({a=\"b\"} | LOGFMT | UNWRAP n [5m]))", true},
		{"unwrap-dynamic-identity", `sum by (x) (avg_over_time({a="b"} | json | keep x | unwrap n [5m]))`, false},
		{"unwrap-dynamic-identity", `sum by (x) (avg_over_time({a="b"} | unwrap n [5m]))`, false},
		{"binary-empty-without", `sum without () (count_over_time({a="b"}[5m])) / 2`, true},
		{"binary-empty-without", `SUM WITHOUT () (count_over_time({a="b"}[5m])) / 2`, true},
		// U+0130 lower cases to the i of without: Loki reads the word as without (recorded Loki answers,
		// internal/logql/testdata/keyword-fold-runes-loki.jsonl.gz).
		{"binary-empty-without", "sum w\u0130thout () (count_over_time({a=\"b\"}[5m])) / 2", true},
		{"binary-empty-without", "2 * Avg(rate({a=\"b\"}[5m])) Without#c\n()", true},
		{"binary-empty-without", `sum(sum without () (count_over_time({a="b"}[5m])) / 2)`, true},
		{"binary-empty-without", `label_replace(max without () (rate({a="b"}[5m])) > 1, "x", "$1", "a", "(.*)")`, true},
		{"binary-empty-without", `sum without () (count_over_time({a="b"}[5m]))`, false},
		{"binary-empty-without", `count without () (count_over_time({a="b"}[5m])) / 2`, false},
		{"binary-empty-without", `sum without (pod) (count_over_time({a="b"}[5m])) / 2`, false},
		{"binary-empty-without", `sum by (a) (sum without () (count_over_time({a="b"}[5m]))) / 2`, false},
		{"extraction-list-windows", `sum(stdvar_over_time({a="b"} | logfmt n | unwrap n [5m]))`, true},
		{"extraction-list-windows", `SUM BY (x) (MAX_OVER_TIME({a="b"} | JSON n, m | UNWRAP n [5m]))`, true},
		{"extraction-list-windows", "avg(avg_over_t\u0130me({a=\"b\"} | logfmt n, m | unwrap n [1m]))", true},
		{"extraction-list-windows", `sum(stdvar_over_time({a="b"} | logfmt n | unwrap n [10s]))`, false},
		{"extraction-list-windows", `sum(stdvar_over_time({a="b"} | logfmt | unwrap n [5m]))`, false},
		{"extraction-list-windows", `sum(stdvar_over_time({a="b"} | json a="n" | unwrap a [5m]))`, false},
		{"extraction-list-windows", `count by (x) (max_over_time({a="b"} | logfmt n | unwrap n [5m]))`, false},
		{"extraction-list-windows", `sum(stdvar_over_time({a="b"} | logfmt n | unwrap n [5m])) * 2`, false},
		// Longer than the plan memo holds: the plan is built for the fence, uncached.
		{"unwrap-dynamic-identity", `SUM BY (x) (AVG_OVER_TIME({a="b"} |= "` + strings.Repeat("x", validationCacheMaxQueryBytes) + `" | JSON | UNWRAP n [5m]))`, true},
		{"rank-over-binary", `topk(2, sum by (x) (rate({a="b"}[1m])) / sum by (x) (rate({a="c"}[1m])))`, true},
		{"rank-over-binary", `TOPK(2, SUM BY (x) (RATE({a="b"}[1m])) / SUM BY (x) (RATE({a="c"}[1m])))`, true},
		{"rank-over-binary", "top\u212a(2, sum by (x) (rate({a=\"b\"}[1m])) > bool 1)", true},
		{"rank-over-binary", "Sort_Desc\t(sum by (x) (rate({a=\"b\"}[1m])) and #c\n sum by (x) (rate({a=\"c\"}[1m])))", true},
		{"rank-over-binary", `sum(bottomk(1, (sum by (x) (rate({a="b"}[1m])) - 1)))`, true},
		{"rank-over-binary", `stdvar(sum by (x) (rate({a="b"}[1m])) * 2) / 3`, true},
		{"rank-over-binary", `STDDEV BY (x) (SUM BY (x, y) (RATE({a="b"}[1m])) / 2)`, true},
		{"rank-over-binary", "StdVar\t(sum by (x) (rate({a=\"b\"}[1m])) * 2)", true},
		{"rank-over-binary", `stddev(stddev_over_time({a="b"} | unwrap n [1m]))`, false},
		{"rank-over-binary", `label_replace(sort(sum by (x) (rate({a="b"}[1m])) unless sum by (x) (rate({a="c"}[1m]))), "y", "$1", "x", "(.*)")`, true},
		{"rank-over-binary", `topk(2, sum by (x) (rate({a="b"}[1m]) / rate({a="c"}[1m])))`, true},
		{"rank-over-binary", `sort(label_replace(sum by (x) (rate({a="b"}[1m])) > 1, "y", "$1", "x", "(.*)"))`, true},
		{"rank-over-binary", `topk(2, sum by (x) (rate({a="b"}[1m]))) / sum by (x) (rate({a="c"}[1m]))`, false},
		{"rank-over-binary", `sort(rate({a="b"}[1m]))`, false},
		{"rank-over-binary", `sum by (x) (rate({a="b"}[1m])) / sum by (x) (rate({a="c"}[1m]))`, false},
		{"rank-over-binary", `stddev(rate({a="b"}[1m]))`, false},
		{"rank-over-binary", `sum(rate({a="b"}[1m]) / 2)`, false},
		{"rank-over-binary-lr", `topk(1, label_replace(rate({a="b"}[1m]), "p", "x-$1", "p", "(.*)") * 2)`, true},
		{"rank-over-binary-lr", `TOPK(1, LABEL_REPLACE(rate({a="b"}[1m]) * 2, "p", "x-$1", "p", "(.*)"))`, true},
		{"rank-over-binary-lr", "StdDev\t(label_replace(rate({a=\"b\"}[1m]), \"p\", \"$1\", \"p\", \"(.*)\") / #c\n 2)", true},
		{"rank-over-binary-lr", `sort(sum by (p) (label_replace(rate({a="b"}[1m]), "p", "$1", "p", "(.*)")) > 1)`, true},
		{"rank-over-binary-lr", `label_replace(topk(1, rate({a="b"}[1m]) * 2), "p", "$1", "p", "(.*)")`, false},
		{"rank-over-binary-lr", `topk(1, label_replace(rate({a="b"}[1m]), "p", "$1", "p", "(.*)"))`, false},
		{"rank-over-binary-lr", `topk(1, rate({a="b"}[1m]) * 2)`, false},
	} {
		if got := fenceFires(tc.fence, tc.query); got != tc.want {
			t.Errorf("%s: %+q fires %v, want %v", tc.fence, tc.query, got, tc.want)
		}
		// A row that must fire also parses to a tree that holds the construct (the fence rows the parser rejects,
		// keyword-label-names and the like, are the exception: they have no tree).
		if tc.want && tc.fence != "keyword-label-names" {
			if _, _, ok := factsOf(tc.query); !ok {
				t.Errorf("%s: %+q does not parse; a row must read as the parser reads it", tc.fence, tc.query)
			}
		}
	}
}

// TestResponseKeyFenceScreensDoNotAllocate keeps the screens that never allocated allocation free, on a firing and a
// clean query each.
//
// conformance: cache-response-key-versions
func TestResponseKeyFenceScreensDoNotAllocate(t *testing.T) {
	// These tests check the fences against the parser, not the translation: their spellings stay out of the query corpus.
	t.Cleanup(querycorpus.PauseRecording())
	for _, query := range []string{
		`{app="a",env=~"p.*"} | json | status="500" |= "error"`,
		`{app="a"} | json | KEEP level | UNWRAP n | x = IP("1.2.3.4") | y = "z" | _TIME > 5s`,
		"{app=\"a\"} # \"\n | json | (x=\"1\") | (y=\"2\") | Keep a",
		`sum by (x) (sum_over_time({app="a"} | json | keep x | unwrap n [5m]))`,
	} {
		for name, screen := range map[string]func(string) string{
			"keep":     keepMetadataProjectionResponseKeyVersion,
			"unwrap":   keepUnwrapIdentityResponseKeyVersion,
			"ip":       ipLabelFilterResponseKeyVersion,
			"repeated": repeatedLabelFilterResponseKeyVersion,
			"case":     reservedFieldCaseResponseKeyVersion,
			"label":    reservedFieldLabelResponseKeyVersion,
			"keyword":  keywordLabelResponseKeyVersion,
			"empty-without": func(q string) string {
				if emptyWithoutClauseIn(q) {
					return emptyWithoutOperandResponseVersion
				}
				return ""
			},
		} {
			if allocs := testing.AllocsPerRun(50, func() { _ = screen(query) }); allocs != 0 {
				t.Errorf("%s screen of %q allocates %v times", name, query, allocs)
			}
		}
	}
}

// textOutsideQuotes is s with the content of its quoted and backquoted strings and its comments left out: the text a
// response key fence reads as words.
func textOutsideQuotes(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"', '`':
			i = skipQuoted(s, i)
			b.WriteString(`""`)
		case '#':
			i = skipComment(s, i) - 1
			b.WriteByte('\n')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// An unwrap written with other blanks than one space on each side of `| unwrap` is served from the plan with the
// spaced spelling's answer, where the string translator failed it or read no unwrap, so its response key carries the
// unwrap blank spelling fence; a query whose every unwrap is written `| unwrap ` keeps its key.
//
// conformance: cache-response-key-versions, semantics/translator-reads-only-space-between-tokens
func TestUnwrapBlankSpellingFence(t *testing.T) {
	for q, want := range map[string]bool{
		"rate({app=\"a\"} |\tunwrap n [5m])":                                           true,
		"max_over_time({A=\"0\"} | unwrap\rA [1m])":                                    true,
		"max_over_time({A=\"0\"}| unwrap\nA [1m])":                                     true,
		`sum_over_time({app="a"} |  unwrap n [5m])`:                                    true,
		`sum_over_time({app="a"} |unwrap n [5m])`:                                      true,
		"sum_over_time({app=\"a\"} | # c\n unwrap n [5m])":                             true,
		`sum(rate({app="a"} | unwrap n [5m])) / sum(rate({app="b"} |  unwrap n [5m]))`: true,
		`sum_over_time({app="a"} | UNWRAP n [5m])`:                                     false,
		`sum_over_time({app="a"} | unwrap n [5m])`:                                     false,
		`sum_over_time({app="a"} | unwrap  n [5m])`:                                    false,
		`quantile_over_time(0.9, {app="a"} | json | unwrap duration(d) [5m]) by (a)`:   false,
		`sum(rate({app="a"} | unwrap n [5m])) / sum(rate({app="b"} | unwrap m [5m]))`:  false,
		`count_over_time({app="a"} |= "x\tunwrap" [5m])`:                               false,
		"count_over_time({app=\"a\"} |= `|\tunwrap` [5m])":                             false,
		`count_over_time({app="a", unwrapped="1"} [5m])`:                               false,
		`rate({app="a"}[5m])`:                                                          false,
	} {
		if got := unwrapBlankSpellingResponseKeyVersion(q) != ""; got != want {
			t.Errorf("%q: fence %v, want %v", q, got, want)
		}
		if keys := responseKeyFences(fenceShapeRequest(q), q, responseKeyCompatQueryRange); strings.Contains(keys, unwrapBlankSpellingResponseVersion) != want {
			t.Errorf("%q: compat key fences %q", q, keys)
		}
	}
}

// exprHoldsLabelReplace reports a label_replace call anywhere in e.
func exprHoldsLabelReplace(e logqlpkg.Expr) bool {
	switch e := e.(type) {
	case *logqlpkg.VectorAggregation:
		return exprHoldsLabelReplace(e.Inner)
	case *logqlpkg.BinOpExpr:
		return exprHoldsLabelReplace(e.Left) || exprHoldsLabelReplace(e.Right)
	case *logqlpkg.OpaqueMetricExpr:
		return e.Func == "label_replace" || e.Inner != nil && exprHoldsLabelReplace(e.Inner)
	}
	return false
}
