package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// skippedParsersText is q, a sum SkipsParsers reports at the root, as the query with no stage from its first parser
// on: the query whose text the lowering writes for it (the translator's text of that query). "" when q is not such a
// sum.
func skippedParsersText(q string) string {
	expr, err := logql.Parse(q)
	if err != nil {
		return ""
	}
	va, ok := expr.(*logql.VectorAggregation)
	if !ok {
		return ""
	}
	ra, ok := va.Inner.(*logql.RangeAggregation)
	if !ok {
		return ""
	}
	lq, ok := ra.Inner.(*logql.LogQuery)
	if !ok {
		return ""
	}
	for i, st := range lq.Pipeline {
		if _, ok := st.(*logql.ParserStage); ok {
			lq.Pipeline = lq.Pipeline[:i]
			return logql.CanonicalSpelling(expr.String())
		}
	}
	return ""
}

// skippedParsersWant is the text the lowering writes for q, a sum SkipsParsers reports at the root (bare json and
// logfmt parsers), under prof: for a count the translator's text of the query with no stage from the first parser on;
// for a rate the text the unpack rule writes for the same sum over an unpack (SkipsUnpack): the rate keeps the
// series of a parsed query (by the stream), which the query with no parser would not (its series carry the level).
func skippedParsersWant(q string, prof rangeProfile) (string, error) {
	cut := skippedParsersText(q)
	if !strings.HasPrefix(q, "sum(rate(") && !strings.HasPrefix(q, "sum by () (rate(") {
		want, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(cut), prof.label, prof.stream, prof.caps)
		if strings.HasPrefix(want, "|") {
			want = "* " + want
		}
		return want, err
	}
	// The cut query ends with its range: the unpack goes before the range's bracket.
	at := strings.LastIndex(cut, "[")
	unpacked := strings.TrimRight(cut[:at], " ") + " | unpack " + cut[at:]
	p := rangeAggPlan(unpacked)
	if p == nil || !SkipsUnpack(p) {
		return "", errNotUnpackSkip
	}
	op, err := PlanQuery(unpacked, p, prof.lowerProfile())
	if err != nil {
		return "", err
	}
	return op.(physical.VLQuery).Text, nil
}

var errNotUnpackSkip = errors.New("the rate over an unpack is not skipped")

// A sum that needs no label of a count or a rate over bare json and logfmt parsers is written with no stage from
// the first parser on: the translator's text of the query without them, under every profile. Every other shape is
// written as the translator writes it, the parsers included (semantics/ungrouped-sum-skips-parsers).
//
// conformance: semantics/ungrouped-sum-skips-parsers
func TestParserSkipLowering(t *testing.T) {
	skipped := []string{
		`sum(count_over_time({app="x"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum(count_over_time({app="x"} | json | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | logfmt | json [5m]))`,
		`sum(count_over_time({app="x"} | json [5m]))`,
		`sum(count_over_time({app="x"} | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | json | json | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | json | drop a | logfmt | drop b [5m] offset 1h))`,
		`sum(count_over_time({app="x"} |= "k" != "j" | json | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | json | logfmt | keep a [5m]))`,
		`sum(rate({app="x"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum(rate({app="x"} | logfmt [1h]))`,
		`sum by () (count_over_time({app="x"} | json | logfmt [5m]))`,
		`sum by () (rate({app="x"} | json [5m]))`,
	}
	kept := []string{
		`sum by (a) (count_over_time({app="x"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`sum without () (count_over_time({app="x"} | json | logfmt [5m]))`,
		`sum without (a) (count_over_time({app="x"} | json [5m]))`,
		`count(count_over_time({app="x"} | json | logfmt [5m]))`,
		`max(count_over_time({app="x"} | logfmt [5m]))`,
		`count_over_time({app="x"} | json | logfmt [5m])`,
		`sum(rate({app="x"} | json | logfmt | keep a [5m]))`,
		`sum(count_over_time({app="x"} | json | logfmt | a!="" [5m]))`,
		`sum(count_over_time({app="x"} | json | __error__="" [5m]))`,
		`sum(count_over_time({app="x"} | json |= "k" [5m]))`,
		`sum(count_over_time({app="x"} | json a="a" [5m]))`,
		`sum(count_over_time({app="x"} | logfmt a [5m]))`,
		`sum(count_over_time({app="x"} | logfmt --strict [5m]))`,
		`sum(count_over_time({app="x"} | logfmt --keep-empty [5m]))`,
		`sum(count_over_time({app="x"} | regexp "(?P<a>x)" [5m]))`,
		`sum(count_over_time({app="x"} | pattern "<a> <_>" [5m]))`,
		`sum(count_over_time({app="x"} | json | line_format "{{.a}}" [5m]))`,
		`sum(count_over_time({app="x"} | json | label_format b=a [5m]))`,
		`sum(count_over_time({app="x"} | decolorize | json [5m]))`,
		`sum(count_over_time({app="x"} | level="error" | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | drop a | logfmt [5m]))`,
		`sum(count_over_time({app="x"} | json | unpack [5m]))`,
		`sum(bytes_over_time({app="x"} | json | logfmt [5m]))`,
		`sum(bytes_rate({app="x"} | json [5m]))`,
		`sum(absent_over_time({app="x"} | json | logfmt [5m]))`,
		`sum(sum_over_time({app="x"} | json | unwrap a [5m]))`,
	}
	for _, prof := range rangeProfiles() {
		for _, q := range skipped {
			p := rangeAggPlan(q)
			if p == nil || !SkipsParsers(p) || !WritesSkippedParsers(p) || SkipsUnpack(p) {
				t.Fatalf("%s: SkipsParsers %v", q, p != nil && SkipsParsers(p))
			}
			op, err := PlanQuery(q, p, prof.lowerProfile())
			if err != nil {
				t.Fatalf("%s: %s: not lowered: %v", prof.name, q, err)
			}
			got := op.(physical.VLQuery).Text
			want, err := skippedParsersWant(q, prof)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if got != want {
				t.Errorf("%s: %s:\n lowered %s\n    want %s", prof.name, q, got, want)
			}
			if strings.Contains(got, "unpack_") {
				t.Errorf("%s: %s: lowered %s, want no unpack pipe", prof.name, q, got)
			}
		}
		for _, q := range kept {
			p := rangeAggPlan(q)
			if p == nil {
				t.Fatalf("%s: no plan", q)
			}
			if SkipsParsers(p) || WritesSkippedParsers(p) {
				t.Errorf("%s: skipped", q)
			}
		}
	}
	// Under a binary operation and a label_replace call the operand is skipped (SkipsParsersWrapped); under a
	// ranking or another aggregation it is written wherever the plan writes it (WritesSkippedParsers).
	sum := `sum(count_over_time({app="x"} | json | logfmt [5m]))`
	for _, tc := range []struct {
		q                      string
		root, wrapped, written bool
	}{
		{sum + ` * 2`, false, true, true},
		{`label_replace(` + sum + `, "a", "b", "", "")`, false, true, true},
		{sum + ` / sum(count_over_time({app="x"} [5m]))`, false, true, true},
		{sum + ` + sum by (a) (count_over_time({app="x"} | json [5m]))`, false, true, true},
		{`sort(` + sum + `)`, false, false, true},
		{`max(` + sum + `)`, false, false, true},
		{`sum by (a) (count_over_time({app="x"} | json [5m])) * 2`, false, false, false},
	} {
		p := rangeAggPlan(tc.q)
		if p == nil {
			t.Fatalf("%s: no plan", tc.q)
		}
		if SkipsParsers(p) != tc.root || SkipsParsersWrapped(p) != tc.wrapped || WritesSkippedParsers(p) != tc.written {
			t.Errorf("%s: SkipsParsers %v, SkipsParsersWrapped %v, WritesSkippedParsers %v", tc.q, SkipsParsers(p), SkipsParsersWrapped(p), WritesSkippedParsers(p))
		}
	}
}

// The unpack keeps its own rule: SkipsParsers reports SkipsUnpack's shape, and the parser skip reads no unpack.
//
// conformance: semantics/ungrouped-sum-skips-unpack, semantics/ungrouped-sum-skips-parsers
func TestParserSkipKeepsTheUnpackRule(t *testing.T) {
	for _, q := range []string{
		`sum(count_over_time({app="x"} | unpack [5m]))`,
		`sum(rate({app="x"} | unpack | drop a [5m]))`,
		`sum(count_over_time({app="x"} | unpack | keep a [5m]))`,
	} {
		p := rangeAggPlan(q)
		if p == nil || !SkipsUnpack(p) || !SkipsParsers(p) || !WritesSkippedParsers(p) {
			t.Errorf("%s: SkipsUnpack and SkipsParsers must both hold", q)
		}
	}
	for _, q := range []string{
		`sum(count_over_time({app="x"} | unpack | json [5m]))`,
		`sum(rate({app="x"} | unpack | keep a [5m]))`,
	} {
		p := rangeAggPlan(q)
		if p == nil || SkipsParsers(p) {
			t.Errorf("%s: skipped", q)
		}
	}
}

// FuzzParserSkipLowering builds sums over json and logfmt parsers with stages around them: a skipped sum is always
// the translator's text of the query with no stage from its first parser on, and holds no unpack pipe; a query the
// plan does not skip is never reported as skipped at the root and lowered without the parsers.
func FuzzParserSkipLowering(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add(uint8(3), uint8(7), uint8(2), uint8(1), uint8(9))
	f.Add(uint8(255), uint8(13), uint8(77), uint8(5), uint8(31))
	f.Fuzz(func(t *testing.T, fn, pre, mid, post, clause uint8) {
		fns := []string{"count_over_time", "rate", "bytes_over_time", "absent_over_time", "bytes_rate"}
		pres := []string{``, ` |= "k"`, ` != "k" |~ "a.*"`, ` | level="x"`, ` | drop a`, ` | decolorize`}
		mids := []string{` | json`, ` | logfmt`, ` | json | logfmt`, ` | logfmt | json`, ` | json a="b"`, ` | logfmt --strict`, ` | regexp "(?P<a>x)"`, ` | unpack`, ` | json | unpack`}
		posts := []string{``, ` | drop __error__, __error_details__`, ` | keep a`, ` | drop a | keep b`, ` | a!=""`, ` | line_format "{{.a}}"`, ` |= "z"`, ` | json`}
		clauses := []string{`sum(%s)`, `sum by () (%s)`, `sum by (a) (%s)`, `sum without () (%s)`, `count(%s)`, `%s`}
		body := `{app="x"}` + pres[pre%uint8(len(pres))] + mids[mid%uint8(len(mids))] + posts[post%uint8(len(posts))]
		inner := fns[fn%uint8(len(fns))] + `(` + body + ` [5m])`
		q := strings.Replace(clauses[clause%uint8(len(clauses))], "%s", inner, 1)
		p := rangeAggPlan(q)
		if p == nil || !SkipsParsers(p) || SkipsUnpack(p) {
			return
		}
		for _, prof := range rangeProfiles() {
			op, err := PlanQuery(q, p, prof.lowerProfile())
			if err != nil {
				t.Fatalf("%s: skipped but not lowered: %v", q, err)
			}
			got := op.(physical.VLQuery).Text
			want, err := skippedParsersWant(q, prof)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if got != want || strings.Contains(got, "unpack_") {
				t.Fatalf("%s: %s:\n lowered %s\n    want %s", prof.name, q, got, want)
			}
		}
	})
}
