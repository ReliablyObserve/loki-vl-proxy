package querycorpus_test

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// hoistedSpellingHeld holds the lowering of a filter on detected_level after a stage to the string translator's text
// of the same query with every such filter written ahead of the first stage, which Loki answers the same: byte for
// byte, the pipes a metric row holds the level across a parser with left out. It answers "" for a query with no such
// spelling (a stage that names the label, a filter that compares "" or a shape other than a log query under range and
// vector aggregations): those rows are held to Loki alone.
func hoistedSpellingHeld(q string, prof planProfile, got physical.Op) string {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return ""
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
		return ""
	}
	var ahead, rest []logql.Stage
	first := -1
	for _, st := range log.Pipeline {
		switch s := st.(type) {
		case *logql.LineFilterStage:
		case *logql.LabelFilterStage:
			if s.Expr != nil && s.Expr.IsLeaf() && s.Expr.Name == "detected_level" {
				if s.Expr.Value == "" {
					return ""
				}
				if first >= 0 {
					ahead = append(ahead, st)
					continue
				}
			}
		default:
			text := st.String()
			if strings.HasPrefix(text, "| keep") || strings.HasPrefix(text, "| drop") && strings.Contains(text, "detected_level") {
				return ""
			}
			if first < 0 && text != "| drop __error__" && text != "| drop __error__, __error_details__" {
				first = len(rest)
			}
		}
		rest = append(rest, st)
	}
	if len(ahead) == 0 {
		return ""
	}
	log.Pipeline = slices.Concat(rest[:first], ahead, rest[first:])
	spelled := expr.String()
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
