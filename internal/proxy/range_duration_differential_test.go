package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The AST carries a range selector's window and offset as durations; the
// proxy used to parse the strings with parseLokiDuration and
// parsePositiveStepDuration. These tests hold the typed fields to what those
// helpers return for every range and offset literal of the query corpora.

var (
	rangeLiteralRE  = regexp.MustCompile(`\[([0-9][0-9.]*[A-Za-zµ]+)\]`)
	offsetLiteralRE = regexp.MustCompile(`offset ([0-9][0-9.]*[A-Za-zµ]+)`)
)

func rangeDurationCorpusStrings(t *testing.T) []string {
	t.Helper()
	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			out = append(out, x)
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		walk(doc)
	}
	return out
}

func rangeDurationLiterals(t *testing.T) (ranges, offsets []string, corpus []string) {
	t.Helper()
	rangeSet := map[string]bool{}
	offsetSet := map[string]bool{}
	for _, s := range []string{
		"1ms", "500ms", "1s", "30s", "1m", "5m", "1h", "1h30m", "2h45m30s", "1d", "7d", "1w", "1y", "0s", "0d", "00s",
		"90s", "1.5h", "0.5s", "1us", "1µs", "1ns", "5M", "1H", "5x", "1h1d", "1m30", "1e", "1.5.5h", "9999999999h",
		"99999999999999d", "99999999999999999y", "2562047h", "2562048h",
	} {
		rangeSet[s], offsetSet[s] = true, true
	}
	for _, s := range rangeDurationCorpusStrings(t) {
		corpus = append(corpus, s)
		for _, m := range rangeLiteralRE.FindAllStringSubmatch(s, -1) {
			rangeSet[m[1]] = true
		}
		for _, m := range offsetLiteralRE.FindAllStringSubmatch(s, -1) {
			offsetSet[m[1]] = true
		}
	}
	for s := range rangeSet {
		ranges = append(ranges, s)
	}
	for s := range offsetSet {
		offsets = append(offsets, s)
	}
	sort.Strings(ranges)
	sort.Strings(offsets)
	return ranges, offsets, corpus
}

// oldLokiDuration is parseLokiDuration with its one disagreement with
// parsePositiveStepDuration set aside: a whole number of days too large for a
// time.Duration (over 106751991, about 292 years) overflows the multiplication
// in its "d" fallback and returns a wrapped, meaningless value, where
// parsePositiveStepDuration and the typed field give 0 (not usable).
func oldLokiDuration(s string) (d time.Duration, overflowed bool) {
	d = parseLokiDuration(s)
	if d != oldPositive(s) && strings.HasSuffix(s, "d") && oldPositive(s) == 0 {
		var days int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(s, "d"), "%d", &days); err == nil && days > 106751991 {
			return 0, true
		}
	}
	return d, false
}

// oldPositive is what parsePositiveStepDuration made of a literal, 0 when it
// reported not ok.
func oldPositive(s string) time.Duration {
	d, ok := parsePositiveStepDuration(s)
	if !ok {
		return 0
	}
	return d
}

func TestRangeAggregationDurationsMatchOldHelpers(t *testing.T) {
	ranges, offsets, _ := rangeDurationLiterals(t)
	parsed, rejected := 0, 0
	for _, lit := range ranges {
		expr, err := logqlpkg.Parse(`rate({app="api"}[` + lit + `])`)
		if err != nil {
			rejected++
			continue
		}
		ra := expr.(*logqlpkg.RangeAggregation)
		parsed++
		if ra.Range != lit || ra.HasOffset() || ra.OffsetDuration != 0 {
			t.Errorf("[%s]: Range=%q HasOffset=%v OffsetDuration=%v", lit, ra.Range, ra.HasOffset(), ra.OffsetDuration)
		}
		if want := oldPositive(lit); ra.RangeDuration != want {
			t.Errorf("[%s]: RangeDuration=%v, parsePositiveStepDuration gives %v", lit, ra.RangeDuration, want)
		}
		// parseLokiDuration is the other helper the consumers used: it also
		// reports 0 for a literal it cannot read.
		if want, _ := oldLokiDuration(lit); ra.RangeDuration != want {
			t.Errorf("[%s]: RangeDuration=%v, parseLokiDuration gives %v", lit, ra.RangeDuration, want)
		}
	}
	for _, lit := range offsets {
		expr, err := logqlpkg.Parse(`rate({app="api"}[5m] offset ` + lit + `)`)
		if err != nil {
			rejected++
			continue
		}
		ra := expr.(*logqlpkg.RangeAggregation)
		parsed++
		if ra.Offset != lit || !ra.HasOffset() || ra.RangeDuration != 5*time.Minute {
			t.Errorf("offset %s: Offset=%q HasOffset=%v RangeDuration=%v", lit, ra.Offset, ra.HasOffset(), ra.RangeDuration)
		}
		if want := oldPositive(lit); ra.OffsetDuration != want {
			t.Errorf("offset %s: OffsetDuration=%v, parsePositiveStepDuration gives %v", lit, ra.OffsetDuration, want)
		}
		if want, _ := oldLokiDuration(lit); ra.OffsetDuration != want {
			t.Errorf("offset %s: OffsetDuration=%v, parseLokiDuration gives %v", lit, ra.OffsetDuration, want)
		}
	}
	t.Logf("%d range literals, %d offset literals: %d parsed, %d rejected by the parser", len(ranges), len(offsets), parsed, rejected)
	if parsed < 40 {
		t.Fatalf("only %d literals parsed; the corpus did not load", parsed)
	}
}

func TestOldLokiDurationOverflowIsTheOnlyDisagreement(t *testing.T) {
	if _, overflowed := oldLokiDuration("99999999999999d"); !overflowed {
		t.Fatal("99999999999999d should overflow parseLokiDuration")
	}
	if parseLokiDuration("99999999999999d") >= 0 {
		t.Skip("the overflow no longer wraps negative")
	}
	for _, lit := range []string{"106751991d", "5d", "0d", "1w", "1y", "5x"} {
		if _, overflowed := oldLokiDuration(lit); overflowed {
			t.Errorf("%s reported as overflowing", lit)
		}
	}
}

func TestRangeAggregationDurationsKnownValues(t *testing.T) {
	for lit, want := range map[string]time.Duration{
		"1ms": time.Millisecond, "500ms": 500 * time.Millisecond, "30s": 30 * time.Second, "90s": 90 * time.Second,
		"5m": 5 * time.Minute, "1h": time.Hour, "1.5h": 90 * time.Minute, "1d": 24 * time.Hour,
		"7d": 7 * 24 * time.Hour, "1w": 7 * 24 * time.Hour, "1y": 365 * 24 * time.Hour, "0s": 0,
	} {
		expr, err := logqlpkg.Parse(`count_over_time({app="api"}[` + lit + `])`)
		if err != nil {
			t.Fatalf("[%s]: %v", lit, err)
		}
		if got := expr.(*logqlpkg.RangeAggregation).RangeDuration; got != want {
			t.Errorf("[%s]: RangeDuration=%v, want %v", lit, got, want)
		}
	}
	// Loki's units are case-sensitive and closed: these are parse errors (Loki v3.7.9, recorded with the test oracle).
	for lit, want := range map[string]string{
		"5M": `parse error at line 0, col 28: unknown unit "M" in duration "5M"`,
		"5x": `parse error at line 0, col 28: unknown unit "x" in duration "5x"`,
	} {
		if _, msg := logqlpkg.ValidateLogQLExpr(`count_over_time({app="api"}[` + lit + `])`); msg != want {
			t.Errorf("[%s]: %q, Loki %q", lit, msg, want)
		}
	}
	expr, err := logqlpkg.Parse(`rate({app="api"}[5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if ra := expr.(*logqlpkg.RangeAggregation); ra.HasOffset() || ra.OffsetDuration != 0 {
		t.Errorf("no offset: HasOffset=%v OffsetDuration=%v", ra.HasOffset(), ra.OffsetDuration)
	}
}

// TestCorpusRangeAggregationDurationsMatchOldHelpers parses every corpus string
// that is a LogQL expression and holds each range aggregation in it, at any
// depth, to the old helpers.
func TestCorpusRangeAggregationDurationsMatchOldHelpers(t *testing.T) {
	_, _, corpus := rangeDurationLiterals(t)
	var visit func(e logqlpkg.Expr, q string)
	checked := 0
	visit = func(e logqlpkg.Expr, q string) {
		switch n := e.(type) {
		case *logqlpkg.RangeAggregation:
			checked++
			if want := oldPositive(n.Range); n.RangeDuration != want {
				t.Errorf("%s: RangeDuration=%v, want %v", q, n.RangeDuration, want)
			}
			if want, _ := oldLokiDuration(n.Range); n.RangeDuration != want {
				t.Errorf("%s: RangeDuration=%v, parseLokiDuration gives %v", q, n.RangeDuration, want)
			}
			if n.HasOffset() != (n.Offset != "") || n.OffsetDuration != oldPositive(n.Offset) {
				t.Errorf("%s: Offset=%q HasOffset=%v OffsetDuration=%v", q, n.Offset, n.HasOffset(), n.OffsetDuration)
			}
		case *logqlpkg.VectorAggregation:
			visit(n.Inner, q)
		case *logqlpkg.BinOpExpr:
			visit(n.Left, q)
			visit(n.Right, q)
		}
	}
	for _, q := range corpus {
		if !strings.Contains(q, "[") {
			continue
		}
		if expr, err := logqlpkg.Parse(q); err == nil {
			visit(expr, q)
		}
	}
	if checked < 100 {
		t.Fatalf("only %d range aggregations found in the corpus", checked)
	}
	t.Logf("%d range aggregations from the corpus match the old helpers", checked)
}

// labelFormatRawMentionsLine is the check loki_profile.go made on the stage's
// raw text before it read the parsed Formats.
func labelFormatRawMentionsLine(s *logqlpkg.LabelFormatStage) bool {
	return strings.Contains(s.Raw, "__line__")
}

func TestLabelFormatMentionsLineMatchesRawCheck(t *testing.T) {
	for _, stage := range []string{
		`a="{{ __line__ }}"`,
		`a="{{__line__}}"`,
		`a=__line__`,
		`__line__=a`,
		`__line__x="v"`,
		`a="v", b=__line__`,
		`a="v", b="{{ .c }} {{ __line__ | trunc 5 }}"`,
		`a="x__line__y"`,
		"a=`{{ __line__ }}`",
		`a="say \"__line__\""`,
		`a="__line__"`,
		`a="__line_"`,
		`a="__LINE__"`,
		`a="{{ .line }}"`,
		`a=b`,
		`a="v", b="w", c=d`,
		`a="é{{ __line__ }}"`,
	} {
		lq, err := logqlpkg.ParseLogQuery(`{app="api"} | label_format ` + stage)
		if err != nil {
			t.Errorf("%s: %v", stage, err)
			continue
		}
		s := lq.Pipeline[0].(*logqlpkg.LabelFormatStage)
		if got, want := labelFormatMentionsLine(s), labelFormatRawMentionsLine(s); got != want {
			t.Errorf("%s: labelFormatMentionsLine=%v, raw check=%v (raw %q)", stage, got, want, s.Raw)
		}
	}
	// The cases above reach both outcomes.
	for stage, want := range map[string]bool{`a="{{ __line__ }}"`: true, `a=__line__`: true, `a="x__line__y"`: true, `a="{{ .line }}"`: false, `a=b`: false} {
		lq, err := logqlpkg.ParseLogQuery(`{app="api"} | label_format ` + stage)
		if err != nil {
			t.Fatal(err)
		}
		if got := labelFormatMentionsLine(lq.Pipeline[0].(*logqlpkg.LabelFormatStage)); got != want {
			t.Errorf("%s: got %v, want %v", stage, got, want)
		}
	}
}
