package proxy

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
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
}

func (f *fenceFacts) name(name string, comparison bool) {
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
	if (strings.EqualFold(name, "ip") && name != "ip") || (strings.EqualFold(name, "label_replace") && name != "label_replace") {
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
		if e.Kind == logqlpkg.LabelFilterValueIP {
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
		f.expr(e.Inner)
	case *logqlpkg.VectorAggregation:
		f.expr(e.Inner)
	case *logqlpkg.BinOpExpr:
		f.expr(e.Left)
		f.expr(e.Right)
	case *logqlpkg.OpaqueMetricExpr:
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
				w = strings.NewReplacer("k", "\u212a", "i", "\u0130").Replace(w)
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

// fenceMisses lists the fences a query's tree requires and its key lacks.
func fenceMisses(query string) []string {
	f, expr, ok := factsOf(query)
	if !ok {
		return nil
	}
	r := fenceShapeRequest(query)
	var miss []string
	need := func(name string, want bool, got string) {
		if want && got == "" {
			miss = append(miss, name)
		}
	}
	need("keep-metadata-projection", f.keep, keepMetadataProjectionResponseKeyVersion(query))
	need("keep-unwrap-identity", f.keepBeforeUnwrap, keepUnwrapIdentityResponseKeyVersion(query))
	need("ip-label-filter", f.ip, ipLabelFilterResponseKeyVersion(query))
	need("repeated-label-filters", f.labelFilterStages >= 2, repeatedLabelFilterResponseKeyVersion(query))
	need("reserved-field-case", f.reservedCase, reservedFieldCaseResponseKeyVersion(query))
	need("reserved-field-labels", f.reservedLabel, reservedFieldLabelResponseKeyVersion(query))
	need("keyword-label-names", f.keywordSpelling, keywordLabelResponseKeyVersion(query))
	resolved := resolveGrafanaRangeTemplateTokens(query, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if rexpr, err := logqlpkg.Parse(resolved); err == nil {
		need("ordered-json-vector", containsOrderedJSONVectorExpr(rexpr), orderedJSONVectorResponseKeyVersion(r, query))
	}
	if spec, ok := parseBareParserMetricCompatSpec(resolved); ok && strings.Contains(spec.baseQuery, "| json") {
		need("bare-json-metric", true, bareJSONMetricResponseKeyVersion(r, query))
	}
	if ra, ok := ungroupedSumRange(resolved); ok && ra.Op == logqlpkg.RangeCountOverTime && f.logfmt && f.presence {
		if lq, ok := ra.Inner.(*logqlpkg.LogQuery); ok {
			for _, st := range lq.Pipeline {
				if p, ok := st.(*logqlpkg.ParserStage); ok && p.Type == logqlpkg.ParserLogfmt {
					need("logfmt-presence", true, logfmtPresenceResponseKeyVersion(r, query))
				}
			}
		}
	}
	_ = expr
	return miss
}

var fenceSpellingBases = []string{
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
}

// TestResponseKeyFencesFireOnEverySpelling re-spells each base query many ways and requires every fence whose
// construct the parsed tree holds to fire.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesFireOnEverySpelling(t *testing.T) {
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

// fenceLosses lists the fences an older binary's screen fired for that the screen now lacks: a response key it wrote
// would no longer be reachable. It reads queries the parser accepts and that hold no comment: the older screens read
// a quote inside a comment as the start of quoted text, so a rejected or odd query could fire them by accident, and
// no 200 is cached for a rejected query.
func fenceLosses(query string) []string {
	if _, _, ok := factsOf(query); !ok || strings.Contains(query, "#") {
		return nil
	}
	var lost []string
	check := func(name string, old bool, now string) {
		if old && now == "" {
			lost = append(lost, name)
		}
	}
	check("keep-metadata-projection", refKeepMetadata(query), keepMetadataProjectionResponseKeyVersion(query))
	check("keep-unwrap-identity", refKeepUnwrap(query), keepUnwrapIdentityResponseKeyVersion(query))
	check("ip-label-filter", refIP(query), ipLabelFilterResponseKeyVersion(query))
	check("repeated-label-filters", refRepeatedLabelFilter(query), repeatedLabelFilterResponseKeyVersion(query))
	check("reserved-field-case", refReservedCase(query), reservedFieldCaseResponseKeyVersion(query))
	check("reserved-field-labels", refReservedLabel(query), reservedFieldLabelResponseKeyVersion(query))
	check("keyword-label-names", refKeywordLabel(query), keywordLabelResponseKeyVersion(query))
	// The three screens that parse after a text prefilter: the prefilter alone must keep every query it let through.
	check("ordered-json-vector prefilter", refOrderedJSONCandidate(query), boolKey(orderedJSONVectorFenceCandidate(query)))
	check("bare-json-metric prefilter", refBareJSONText(query), boolKey(containsFold(query, "json")))
	check("logfmt-presence prefilter", refLogfmtPresenceText(query), boolKey(containsFold(query, "logfmt") && emptyStringComparisonIn(query)))
	return lost
}

func boolKey(b bool) string {
	if b {
		return "x"
	}
	return ""
}

// TestResponseKeyFencesKeepTheirOldKeys requires every screen to fire on every query its older form fired on, over
// the corpus the key tests use: the queries clients send in the spelling they send them.
//
// conformance: cache-response-key-versions
func TestResponseKeyFencesKeepTheirOldKeys(t *testing.T) {
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
	for _, q := range corpus {
		if lost := fenceLosses(q); len(lost) > 0 {
			t.Errorf("%q: fences %v fired before and do not now", q, lost)
		}
		if miss := fenceMisses(q); len(miss) > 0 {
			t.Errorf("%q: fences %v do not fire for the construct in the tree", q, miss)
		}
	}
}

// FuzzResponseKeyFencesMatchParsedTree checks every response key fence against the parsed tree: a query whose tree
// holds the construct a fence guards must fire it, whatever the case of its keywords, the white space and comments
// between its tokens, the letters of its names. The seeds are the fence corpus queries re-spelled (seed 0 is the
// query as written); the fuzzer mutates the text and the spelling seed. A query must also keep every fence its older
// screen fired.
//
// conformance: cache-response-key-versions
func FuzzResponseKeyFencesMatchParsedTree(f *testing.F) {
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
	})
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
		} {
			if allocs := testing.AllocsPerRun(50, func() { _ = screen(query) }); allocs != 0 {
				t.Errorf("%s screen of %q allocates %v times", name, query, allocs)
			}
		}
	}
}
