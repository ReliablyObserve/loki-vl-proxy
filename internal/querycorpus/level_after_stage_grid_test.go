package querycorpus_test

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// hoistedSpelling is q with every filter on detected_level after a stage written ahead of the first stage, which
// Loki answers the same. There is none for a query with a stage that names the label, a filter that compares "", a
// shape other than a log query under range and vector aggregations, or no such filter.
func hoistedSpelling(q string) (string, bool) {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return "", false
	}
	e := expr
	for {
		switch x := e.(type) {
		case *logql.VectorAggregation:
			e = x.Inner
			continue
		case *logql.RangeAggregation:
			e = x.Inner
			continue
		}
		break
	}
	log, ok := e.(*logql.LogQuery)
	if !ok {
		return "", false
	}
	var ahead, rest []logql.Stage
	first := -1
	for _, st := range log.Pipeline {
		switch s := st.(type) {
		case *logql.LineFilterStage:
		case *logql.LabelFilterStage:
			if s.Expr != nil && s.Expr.IsLeaf() && s.Expr.Name == "detected_level" {
				if s.Expr.Value == "" {
					return "", false
				}
				if first >= 0 {
					ahead = append(ahead, st)
					continue
				}
			}
		default:
			text := st.String()
			if strings.HasPrefix(text, "| keep") || strings.HasPrefix(text, "| drop") && strings.Contains(text, "detected_level") {
				return "", false
			}
			if first < 0 && text != "| drop __error__" && text != "| drop __error__, __error_details__" {
				first = len(rest)
			}
		}
		rest = append(rest, st)
	}
	if len(ahead) == 0 {
		return "", false
	}
	log.Pipeline = slices.Concat(rest[:first], ahead, rest[first:])
	return expr.String(), true
}

// hoistedSpellingHeld holds the lowering of a filter on detected_level after a stage to the string translator's text
// of the same query with every such filter written ahead of the first stage (hoistedSpelling): byte for byte, the
// pipes a metric row holds the level across a parser with left out. It answers "" for a query with no such spelling:
// those rows are held to Loki alone.
func hoistedSpellingHeld(q string, prof planProfile, got physical.Op) string {
	spelled, ok := hoistedSpelling(q)
	if !ok {
		return ""
	}
	want, ok := prof.translate(spelled)
	if !ok || strings.HasPrefix(want, `"`) {
		// The translator writes a ranking spelled with a blank as a phrase; the proxy evaluates it.
		return ""
	}
	if strings.HasPrefix(want, "|") {
		want = "* " + want
	}
	text := physical.Encode(got)
	if withoutKeptOriginals(text) != withoutKeptOriginals(lower.QuoteKeywordFieldText(want)) {
		return fmt.Sprintf("%s: %q:\n lowered %s\n  string %s (of %q)", prof.name, q, text, want, spelled)
	}
	return ""
}

// withoutKeptOriginals is the text without the keep_original_fields flag a metric row's parser behind the level
// derivation takes (the derivation's own format pipes write the flag too, in both texts alike).
func withoutKeptOriginals(text string) string {
	return keptOriginalParser.ReplaceAllString(text, "$1")
}

var keptOriginalParser = regexp.MustCompile(`(\| (?:unpack_json|unpack_logfmt|extract|extract_regexp)(?: [^|]*?)?) keep_original_fields`)

// TestLevelAfterStageTypedPair holds the VictoriaLogs oracle's pair of a filter on detected_level after a stage to
// the translator's text of the hoisted spelling, which VictoriaLogs must read as the plan's: for a log query the two
// texts are equal, and a query with no such spelling has no pair.
func TestLevelAfterStageTypedPair(t *testing.T) {
	for _, q := range []string{
		`{app="a"} | json | detected_level="error"`,
		`{app="a"} | logfmt | detected_level!="info"`,
		`{app="a"} | decolorize | detected_level="warn"`,
		`count_over_time({app="a"} | logfmt | detected_level="error" [1m])`,
	} {
		typed, built, ok := typedQueryPair(q, "v1.54.0")
		if !ok || withoutKeptOriginals(typed) != withoutKeptOriginals(built) || !strings.Contains(typed, "__dl") {
			t.Errorf("%q: pair ok=%v\n plan %s\n held %s", q, ok, typed, built)
		}
	}
	if _, _, ok := typedQueryPair(`{app="a"} | drop detected_level | detected_level="error"`, "v1.54.0"); ok {
		t.Error("a filter on a dropped label has no hoisted spelling, so no pair")
	}
}
