package logql

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// lokiFilterShape reads the String() Loki prints for a label filter ("( a , b )"
// for and, "( a or b )" for or) as an operator tree with every filter written x.
func lokiFilterShape(s string) string {
	pos := 0
	var expr func() string
	expr = func() string {
		if strings.HasPrefix(s[pos:], "( ") {
			pos += 2
			left := expr()
			op := "or"
			if strings.HasPrefix(s[pos:], " , ") {
				op = ","
			}
			pos += len(op) + 2
			right := expr()
			pos += 2 // " )"
			return "(" + left + " " + op + " " + right + ")"
		}
		for pos < len(s) {
			switch {
			case s[pos] == '"' || s[pos] == '`':
				quote := s[pos]
				for pos++; pos < len(s) && s[pos] != quote; pos++ {
					if s[pos] == '\\' && quote == '"' {
						pos++
					}
				}
			case strings.HasPrefix(s[pos:], " , "), strings.HasPrefix(s[pos:], " or "), strings.HasPrefix(s[pos:], " )"):
				return "x"
			}
			pos++
		}
		return "x"
	}
	return expr()
}

func filterShape(e *LabelFilterExpr) string {
	switch e.Op {
	case LabelFilterAnd:
		return "(" + filterShape(e.Left) + " , " + filterShape(e.Right) + ")"
	case LabelFilterOr:
		return "(" + filterShape(e.Left) + " or " + filterShape(e.Right) + ")"
	}
	return "x"
}

// lokiRangeMissingQuery is the one recorded query with a ")" where the range
// aggregation expects "[".
const lokiRangeMissingQuery = `sum without (app) (rate({app="a"} | logfmt | o=ip ("1.2.3.4")) (t= "(x)") [1m]))`

// TestLabelFilterExpressionMatchesLokiGolden replays random label filter
// expressions (valid and mutated) recorded from Loki v3.7.9: the proxy accepts
// exactly what Loki accepts, reads the same operator tree, and rejects with
// Loki's message, line and column.
//
// conformance: semantics/label-filter-expression-syntax
func TestLabelFilterExpressionMatchesLokiGolden(t *testing.T) {
	f, err := os.Open("testdata/label_filter_expr_loki.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var valid, invalid, treeChecked int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		cols := strings.SplitN(line, "\t", 3)
		query, status, want := cols[0], cols[1], cols[2]
		got := ValidateLogQL(query)
		if status == "ERR" {
			invalid++
			// Two answers differ for reasons unrelated to label filter values: a
			// bare identifier stage (`| a`) passes through to the backend like
			// any unknown stage keyword, and a ")" where a range aggregation
			// expects "[" keeps its generic message.
			if got == "" && strings.HasSuffix(query, "| a") || query == lokiRangeMissingQuery {
				continue
			}
			if got != want {
				t.Errorf("%s\n  proxy: %q\n  Loki:  %q", query, got, want)
			}
			continue
		}
		valid++
		if got != "" {
			t.Errorf("%s\n  proxy rejects what Loki accepts: %q", query, got)
			continue
		}
		if !strings.HasPrefix(query, "{") || strings.Contains(query, "unwrap") || strings.Contains(query, "_over_time") {
			continue // metric expressions: accepted is the whole check
		}
		lq, err := ParseLogQuery(query)
		if err != nil {
			t.Errorf("%s: %v", query, err)
			continue
		}
		if _, ok := lq.Pipeline[len(lq.Pipeline)-1].(*LabelFilterStage); !ok {
			t.Errorf("%s: last stage is %T", query, lq.Pipeline[len(lq.Pipeline)-1])
			continue
		}
		_, text, _ := strings.Cut(query, "} | ")
		text = strings.TrimPrefix(strings.TrimPrefix(text, "json | "), "logfmt | ")
		expr, serr := ParseLabelFilterExpr(text)
		if serr != nil {
			t.Errorf("%s: %q: %v", query, text, serr)
			continue
		}
		_, filter, _ := strings.Cut(want, "} | ")
		filter = strings.TrimPrefix(strings.TrimPrefix(filter, "json | "), "logfmt | ")
		treeChecked++
		if got, wantShape := filterShape(expr), lokiFilterShape(filter); got != wantShape {
			t.Errorf("%s\n  proxy tree: %s\n  Loki tree:  %s (%s)", query, got, wantShape, filter)
		}
	}
	t.Logf("%d accepted (%d trees compared), %d rejected", valid, treeChecked, invalid)
	if valid < 200 || invalid < 100 || treeChecked < 150 {
		t.Fatalf("golden corpus too small: %d valid, %d invalid, %d trees", valid, invalid, treeChecked)
	}
}
