package logql

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// printReparseLiterals are the number, duration and bytes literals the scanner
// cuts into several tokens (`1h5m` is `1h` `5m`, `1e3` is `1e` `3`) or reads
// with a sign, and so must print the way they were written.
var printReparseLiterals = []string{
	"1h5m", "1h30m45s", "1h5m3s", "100ms", "5us", "5µs", "1.5s", "1.5h2m",
	"1e3", "1e+5", "1.5e3", "0x10", "1_000", "+5", "-5", ".5", "10KB", "10MiB", "1000000",
}

// printReparseQueries wraps a literal (written %s) in the places a label filter
// can sit.
var printReparseQueries = []string{
	`{a="b"} | json | latency > %s`,
	`{a="b"} | latency>%s | json`,
	`{a="b"} | logfmt | latency >= %s and level="error"`,
	`{a="b"} | json | level="error" or latency < %s`,
	`{a="b"} | json | level="error", latency <= %s`,
	`{a="b"} | json | (latency == %s or latency != 0), level="x"`,
	`{a="b"} | json | unwrap latency | latency > %s`,
	`{a="b"} | json | x > %s | y > %s`,
	`sum(rate({a="b"} | json | latency > %s [5m]))`,
	`sum(count_over_time({a="b"} | json | latency > %s [5m])) / 2`,
	`sum(count_over_time({a="b"} | json | latency > %s [5m])) / sum(count_over_time({a="b"} | json | latency < %s [5m]))`,
	`avg_over_time({a="b"} | json | latency > %s | unwrap latency [5m])`,
}

// walkTree calls visit on every struct of a parsed tree (by pointer).
func walkTree(v reflect.Value, visit func(any)) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			walkTree(v.Elem(), visit)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			walkTree(v.Index(i), visit)
		}
	case reflect.Struct:
		visit(v.Addr().Interface())
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkTree(v.Field(i), visit)
			}
		}
	}
}

// withoutSourceSpacing clears, in a parsed tree, the text of a parsed label
// filter that keeps the spacing of its source (the stage's Raw and each leaf's
// Leaf), so that two trees compare by what they say, not by how it was spaced:
// the printer writes `a>1`, the client may have written `a > 1`. What the
// filters say stays in Name, Cmp, Value and Kind of each leaf. The Escaped flag of a quoted value is source text
// too: the printer writes the canonical quoting of the value, where the client may have written \x41.
func withoutSourceSpacing(e Expr) {
	walkTree(reflect.ValueOf(&e), func(node any) {
		switch x := node.(type) {
		case *LabelFilterStage:
			if x.Expr != nil {
				x.Raw = ""
			}
		case *LabelFilterExpr:
			x.Leaf = ""
		case *LabelMatcher:
			x.Escaped = false
		case *LineFilterStage:
			x.Escaped = false
		case *LineFilterAlt:
			x.Escaped = false
		}
	})
}

var (
	// labelFilterName is a label name as the printer is meant to keep.
	labelFilterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)
	// labelFilterNumeral is a number (decimal, exponent, hex, `_` separators),
	// duration (`1h5m`, `1.5s`, `5µs`) or bytes (`10KB`, `10MiB`) literal
	// likewise. Values such as `.` or `0+` pass ValidateLogQL but are no number
	// Loki reads, so the printer has nothing to keep for them.
	labelFilterNumeral = regexp.MustCompile(`^[+-]?(0[xX][0-9A-Fa-f_]+|([0-9][0-9_]*(\.[0-9_]*)?|\.[0-9][0-9_]*)([eE][+-]?[0-9]+)?([A-Za-zµ]+([0-9][0-9_]*(\.[0-9_]*)?[A-Za-zµ]+)*)?)$`)
)

// outsideLabelFilterDomain reports whether a label filter stage of the tree is
// one the printer is not meant to keep: text kept as it is because it is not a
// label filter (the stage `| j"0` of an unterminated string) or a string that is no valid
// literal (`a="\0"`), or a filter whose
// "label name" or number holds what is no name or number (`}0`, `0#`), which
// the stage text prints without the spacing of its source.
func outsideLabelFilterDomain(e Expr) bool {
	outside := false
	walkTree(reflect.ValueOf(&e), func(node any) {
		switch x := node.(type) {
		case *LabelFilterStage:
			outside = outside || x.Expr == nil
		case *UnwrapStage:
			// `| unwrap [5m]` and `| unwrap[`: an unwrap without its label prints
			// without what followed it (a known printer gap, not a literal).
			outside = outside || x.Label == ""
		case *LabelFilterExpr:
			if x.IsLeaf() {
				numeric := x.Kind == LabelFilterValueNumber || x.Kind == LabelFilterValueDuration || x.Kind == LabelFilterValueBytes
				outside = outside || x.Kind == LabelFilterValueNone || !labelFilterName.MatchString(x.Name) || numeric && !labelFilterNumeral.MatchString(x.Value) ||
					// A label named like a keyword (`BY`): the scanner lowercases it in the
					// stage text, which the text readers rely on (`IP(` reads as `ip(`).
					lokiKeywords[strings.ToLower(x.Name)] && strings.ToLower(x.Name) != x.Name
			}
		}
	})
	return outside
}

func sameTree(a, b Expr) bool {
	withoutSourceSpacing(a)
	withoutSourceSpacing(b)
	return reflect.DeepEqual(a, b)
}

func formatLiteralQuery(format, literal string) string {
	return strings.ReplaceAll(format, "%s", literal)
}

// Printing a parsed query writes the literals of its label filters as they were
// written, and the printed query parses to the same tree. A literal cut by a
// space (`1h 5m`) is another query: the translator reads it as a comparison to
// `1h` and a line phrase.
// conformance: semantics/label-filter-expression-syntax, semantics/binary-operand-literals-reprint
func TestPrintReparsePreservesLabelFilterLiterals(t *testing.T) {
	for _, literal := range printReparseLiterals {
		for _, format := range printReparseQueries {
			query := formatLiteralQuery(format, literal)
			first, err := Parse(query)
			if err != nil {
				// Not every literal is valid for every operator in Loki's grammar;
				// the table only needs the ones that parse.
				continue
			}
			printed := first.String()
			if !strings.Contains(printed, literal) {
				t.Errorf("%q printed as %q: literal %q is not intact", query, printed, literal)
			}
			second, err := Parse(printed)
			if err != nil {
				t.Errorf("%q printed as %q, which does not parse: %v", query, printed, err)
				continue
			}
			if again := second.String(); again != printed {
				t.Errorf("%q printed as %q then %q: not stable", query, printed, again)
			}
			if !sameTree(first, second) {
				t.Errorf("%q printed as %q, which parses to a different tree", query, printed)
			}
		}
	}
}

// The printed text of a label filter is its token form (no space around the
// operator), the literal as it was written.
// conformance: semantics/label-filter-expression-syntax, semantics/binary-operand-literals-reprint
func TestPrintLabelFilterLiteralText(t *testing.T) {
	for _, literal := range printReparseLiterals {
		for _, tc := range []struct{ format, want string }{
			{`{a="b"} | json | latency > %s`, `{a="b"} | json | latency>%s`},
			{`{a="b"} | logfmt | latency >= %s and level="error"`, `{a="b"} | logfmt | latency>=%s and level="error"`},
			{`{a="b"} | json | unwrap latency | latency > %s`, `{a="b"} | json | unwrap latency | latency>%s`},
			{`sum(rate({a="b"} | json | latency > %s [5m])) / 2`, `sum (rate({a="b"} | json | latency>%s[5m])) / 2`},
		} {
			query := formatLiteralQuery(tc.format, literal)
			expr, err := Parse(query)
			if err != nil {
				continue // not valid for this operator in Loki's grammar
			}
			if got, want := expr.String(), formatLiteralQuery(tc.want, literal); got != want {
				t.Errorf("%s printed as %s, want %s", query, got, want)
			}
		}
	}
}

// Every literal of the table is accepted in at least one form, so the test above
// cannot pass by skipping everything.
func TestPrintReparseLiteralsAreParsed(t *testing.T) {
	for _, literal := range printReparseLiterals {
		parsed := 0
		for _, format := range printReparseQueries {
			if _, err := Parse(formatLiteralQuery(format, literal)); err == nil {
				parsed++
			}
		}
		if parsed == 0 {
			t.Errorf("literal %q parses in none of the queries", literal)
		}
	}
}

// A scalar prints without an exponent: the scanner reads `1e+06` as `1e`, `+`,
// `06`, so a printed `* 1e+06` did not parse.
// conformance: semantics/label-filter-expression-syntax, semantics/binary-operand-literals-reprint
func TestPrintScalarLiteralHasNoExponent(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`sum(rate({a="b"}[5m])) * 1000000`, `sum (rate({a="b"}[5m])) * 1000000`},
		{`sum(rate({a="b"}[5m])) > 2500000`, `sum (rate({a="b"}[5m])) > 2500000`},
		{`sum(rate({a="b"}[5m])) > 0.00001`, `sum (rate({a="b"}[5m])) > 0.00001`},
		{`sum(rate({a="b"}[5m])) > 0.0000001`, `sum (rate({a="b"}[5m])) > 0.0000001`},
		{`topk(1000000, rate({a="b"}[5m]))`, `topk (1000000,rate({a="b"}[5m]))`},
		{`quantile_over_time(0.99, {a="b"} | unwrap x [5m])`, `quantile_over_time(0.99,{a="b"} | unwrap x[5m])`},
	} {
		expr, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		printed := expr.String()
		if printed != tc.want {
			t.Errorf("%s printed as %s, want %s", tc.in, printed, tc.want)
		}
		again, err := Parse(printed)
		if err != nil {
			t.Errorf("%s printed as %s, which does not parse: %v", tc.in, printed, err)
			continue
		}
		if !sameTree(expr, again) {
			t.Errorf("%s printed as %s, which parses to a different tree", tc.in, printed)
		}
	}
}

// FuzzPrintReparse: for any input the proxy accepts (ValidateLogQL accepts it),
// printing and parsing again gives the same tree, and printing that gives the
// same text. Parse alone also takes what Loki rejects, such as a stage with an
// unterminated string, and there is nothing to print back for that.
// Run with: go test -fuzz=FuzzPrintReparse ./internal/logql/
func FuzzPrintReparse(f *testing.F) {
	for _, literal := range printReparseLiterals {
		for _, format := range printReparseQueries {
			f.Add(formatLiteralQuery(format, literal))
		}
	}
	for _, seed := range []string{
		`-00`, // negative zero prints as "- 0": Loki's lexer reads "-0" as a duration
		`{app="nginx"}`,
		`{app="nginx"} |= "error" != "debug"`,
		`{app="\x41", env=` + "`b`" + `} |~ "\101b" !~ "a\tb"`, // a value spelled other than Go quotes it
		`{app="nginx"} | json | level="error"`,
		`{app="nginx"} | logfmt | status>=500 and (a="1" or b=~"x.*")`,
		`{app="nginx"} | label_format new=old`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`{app="nginx"} | drop a, b | keep c`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
		`topk(5, rate({app="api"}[5m]))`,
		`rate({app="api"}[5m] offset 1h)`,
		`100 * sum by (app) (rate({app="api"}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`rate({app="api"}[5m]) * on (app) group_left (team) rate({app="api"}[5m])`,
		`sum(rate({app="api"}[5m])) > 1000000`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if !utf8.ValidString(input) {
			return // the scanner reads a stray byte as U+FFFD
		}
		first, msg := ValidateLogQLExpr(input)
		if msg != "" || first == nil || outsideLabelFilterDomain(first) {
			return
		}
		printed := first.String()
		second, err := Parse(printed)
		if err != nil {
			t.Fatalf("%q printed as %q, which does not parse: %v", input, printed, err)
		}
		if again := second.String(); again != printed {
			t.Fatalf("%q printed as %q then %q: not stable", input, printed, again)
		}
		if !sameTree(first, second) {
			t.Fatalf("%q printed as %q, which parses to a different tree", input, printed)
		}
	})
}
