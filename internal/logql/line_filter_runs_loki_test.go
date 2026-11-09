package logql

import (
	"testing"
)

// lineFilterRunGrid is the grid the run-aware line filter validation is held to Loki on: two filters next to each
// other for every pair of invalid ones (regexp, pattern, ip, negated and positive `or` chains), invalid filters next
// to valid ones, runs across each stage kind that keeps a run going or ends it, invalid stages of other kinds before
// and after a run, and the quoting of every operand form.
func lineFilterRunGrid() []string {
	bad := []string{`|~ "("`, `|~ "["`, `!~ "a)"`, `|> "<a>"`, `!> "<_><_>"`, `|= ip("z")`, `!= ip("y")`,
		`|~ ip("1.2.3.4")`, `!~ "(" or "["`, `|~ "a" or "["`, `!= "x" or ip("q")`, `!> "<_>" or ip("1.2.3.4")`}
	good := []string{`|= "x"`, "!= `y`", `|~ "\\d"`, `|> "<_> a"`, `|= ip("1.2.3.4")`, `|= "a" or "b"`,
		`!= "a" or "b"`, `|= ip("z") or "w"`, `|= "a" or ip("z") or "c"`}
	keeps := []string{`| json`, `| logfmt`, `| logfmt --strict --keep-empty`, `| json a="b"`, `| logfmt a="b"`,
		`| regexp "(?P<a>x)"`, `| pattern "<a> x"`, `| label_format a="b"`, `| label_format a=b`, `| drop a`,
		`| keep a`, `| decolorize`, `| json | logfmt`}
	ends := []string{`| unpack`, `| x="1"`, `| __error__=""`, `| x > 1`, `| x="1" or y="2"`, `| x=ip("1.2.3.4")`,
		`| line_format "x"`, `| json | line_format "{{.a}}"`}
	invalid := []string{`| json x="a["`, `| logfmt x="a.b"`, `| label_format x="{{.y}"`, `| label_format __error__="x"`,
		`| label_format x="a",x="b"`, `| line_format "{{.x}"`, `| regexp "x"`, `| pattern "<a><b>"`}
	const sel = `{app="a"} `
	var out []string
	for _, a := range bad {
		for _, b := range bad {
			out = append(out, sel+a+" "+b)
		}
		for _, g := range good {
			out = append(out, sel+g+" "+a, sel+a+" "+g)
		}
	}
	for _, s := range append(append([]string{}, keeps...), ends...) {
		out = append(out,
			sel+`|~ "(" `+s+` |~ "["`,
			sel+`|> "<a>" `+s+` |= "x" != ip("z")`,
			sel+`|= "x" `+s+` |~ "("`,
			sel+`|~ "(" `+s+` |= "x"`,
			sel+s+` |~ "(" |~ "["`,
			sel+`|= "x" `+s+` |~ "(" `+s+` |~ "["`,
		)
	}
	for _, n := range invalid {
		for _, s := range []string{"", `| x="1" `, `| line_format "x" `, `| unpack `, `| json `} {
			for _, b := range []string{`|~ "("`, `|> "<a>"`, `!= ip("z")`} {
				out = append(out, sel+n+" "+s+b, sel+b+" "+s+n)
			}
		}
	}
	return append(out,
		// The runs the registry case names.
		`{app="x"} |= "x" |~ "("`, `{app="x"} |~ "(" |~ "["`, `{app="a"} |~ "(" | json |~ "["`,
		`{app="a"} |> "<a>" | json |~ "("`, `{app="a"} | logfmt | x="1" |~ "(" |= "k"`,
		`{app="a"} |> "<a>" | x="1" |> "<_><_>"`, `{app="a"} | line_format "{{.x}" |> "<a>"`,
		`{app="a"} |> "<a>" | line_format "{{.x}"`, `{app="a"} |~ "(" | label_format x="{{.y}"`,
		`{app="a"} |= "x" != "y" or ip("z")`, `{app="a"} != ip("z") |~ "("`, `{app="a"} |~ "(" != ip("z")`,
		`{app="a"} != "x" or "y" |~ "("`,
		// Three and more filters, several runs, several segments.
		`{app="x"} |~ "(" |~ "[" |~ "a)"`, `{app="x"} |~ "(" |= "y" |~ "["`, `{app="a"} |= "a" | json |= "b" | json |~ "("`,
		`{app="a"} | json |> "<a>" | logfmt |> "<b>" | x="1" |~ "(" | regexp "(?P<a>x)" |~ "["`,
		`{app="a"} | json | drop x | keep y | decolorize | label_format a="b" |~ "(" |= "x" | logfmt != "y" or "z"`,
		`{app="a"} |= "a" | logfmt | label_format x="y" | drop z | keep w | decolorize |~ "(" | x="1" |= "q" | json |~ "["`,
		`{app="a"} | line_format "x" |~ "(" | line_format "y" |~ "["`, `{app="a"} |~ "(" | json | unpack |~ "["`,
		`{app="a"} | json |~ "(" | unpack | logfmt |~ "[" | x="1" |~ "a)"`,
		`{app="a"} | json |~ "a" | unpack | logfmt |~ "[" | x="1" |~ "a)"`,
		// Other stages before a run in its segment, and the order of their own errors.
		`{app="a"} | json x="a[" |~ "("`, `{app="a"} | json x="a[" | x="1" |~ "("`, `{app="a"} | x="1" | json x="a[" |~ "("`,
		`{app="a"} | json x="a[" | logfmt y="b[" |~ "("`, `{app="a"} | label_format x="{{.y}" | x="1" |~ "("`,
		`{app="a"} | label_format x="{{.y}" | line_format "x" |~ "("`, `{app="a"} | line_format "x" | label_format x="{{.y}" |~ "("`,
		`{app="a"} | json x="a[" | line_format "{{.x}"`, `{app="a"} | line_format "{{.x}" | json x="a["`,
		`{app="a"} | json x="a[" | unpack | label_format x="{{.y}"`, `{app="a"} | unpack | json x="a[" |~ "("`,
		`{app="a"} | json x="a[" | x="1" |~ ip("z")`, `{app="a"} | regexp "x" |= ip("z")`, `{app="a"} |= ip("z") | pattern "<a><b>"`,
		`{app="a"} | regexp "x" | pattern "<a><b>"`, `{app="a"} | pattern "<a><b>" | regexp "x"`,
		`{app="a"} |~ "(" | b=ip("b")`, `{app="a"} | b=ip("b") |~ "("`, `{app="a"} | label_format x="{{.y}" | b=ip("b")`,
		`{app="a"} | b=ip("b") | label_format x="{{.y}"`, `{app="a"} | b=ip("1.2.3.4") | c=ip("c") |~ "("`,
		`{app="a"} | json x="a[" | b=ip("b")`, `{app="a"} | b=ip("b") | line_format "{{.x}"`,
		// ip() operands: the operators, positive chains (text) and negated chains (one filter each).
		`{app="a"} != ip("z")`, `{app="a"} |= ip("z")`, `{app="a"} |~ ip("z")`, `{app="a"} !~ ip("z")`, `{app="a"} |> ip("z")`,
		`{app="a"} !> ip("z")`, `{app="a"} !~ ip("1.2.3.4")`, `{app="a"} != ip("1.2.3.4")`, `{app="a"} |= ip("z") or "x"`,
		`{app="a"} |= ip("z") or ip("y")`, `{app="a"} |~ ip("(") or "a"`, `{app="a"} |~ ip("1.2.3.4") or "a"`,
		`{app="a"} |> ip("<a>") or "x"`, `{app="a"} |> "x" or ip("<a>")`, `{app="a"} != ip("z") or "x"`,
		`{app="a"} != ip("z") or ip("y")`, `{app="a"} != ip("z") != ip("y")`, `{app="a"} !~ "(" or ip("z")`,
		`{app="a"} !~ ip("z") or "("`, `{app="a"} !> ip("z") or "<a>"`, `{app="a"} !> "<a>" or ip("z")`,
		`{app="a"} != "x" or ip("1.2.3.4") or ip("z")`, `{app="a"} |= ip("y") != "x" or ip("z")`,
		`{app="a"} != "x" or ip("z") |= ip("y")`, `{app="a"} |~ ip("(") |= "x"`, `{app="a"} |~ ip("1.2.3.4") |~ "("`,
		"{app=\"a\"} |= ip(`z`) |~ \"(\"", `{app="a"} |= ip "x"`, `{app="a"} |= ip("x"`, `{app="a"} |~ ip`,
		`{app="a"} |= "a" or ip "x"`, `{app="a"} |= "a" or ip`, `{app="a"} |= ip ("1.2.3.4") |~ "("`, `{app="a"} |= ip(1)`,
		`{app="a"} |= IP("1.2.3.4") |~ "("`,
		// Positive chains: one filter, its alternatives read in order, those before an ip() dropped.
		`{app="a"} |~ "(" or "["`, `{app="a"} |~ "a" or "(" or "["`, `{app="a"} !~ "a" or "(" or "["`,
		`{app="a"} |= "a" or ip("z") or "c" |~ "("`, `{app="a"} |~ "(" or ip("z") or "a"`, `{app="a"} |~ "[" or ip("z") or "("`,
		`{app="a"} |> "<a>" or ip("z") or "x"`, `{app="a"} != "a" or ip("z") or "c" |~ "("`,
		`{app="a"} |> "<a>" or "<_><_>" |~ "(" | json`, `{app="a"} |~ "(" !> "<a>" or "<_><_>"`,
		// How each operand is printed.
		"{app=\"a\"} |= `x\\y` |~ \"(\"", "{app=\"a\"} |~ `\\d` |~ \"(\"", `{app="a"} |= "a\"b" |~ "("`,
		`{app="a"} |= "\t" |~ "("`, `{app="a"} |= "éÿ\x01" |~ "("`, `{app="a"} |= "a'b" |~ "("`,
		`{app="a"} |= "" |~ "("`, "{app=\"a\"} |= `a\"b` |~ \"(\"", `{app="a"} |= "x" OR "y" |~ "("`,
	)
}

const lineFilterRunLokiAnswers = "testdata/line-filter-runs-loki.jsonl"

// An invalid line filter is rejected like Loki rejects it: the error quotes the whole run of line filters Loki builds
// together and names the filter Loki names, and it comes in Loki's order next to the errors of other stages. Loki's
// answers are recorded with the Loki oracle (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/line-filter-error-quotes-the-run
func TestLineFilterRunValidationMatchesLoki(t *testing.T) {
	requireLokiPipelineAnswers(t, lineFilterRunGrid(), lineFilterRunLokiAnswers, map[string]bool{
		// semantics/regexp-chain-led-by-ip: Loki reads the ip() head of a positive |~ chain as text.
		`{app="a"} |~ ip("1.2.3.4") or "a"`: true,
	})
}

// lineFilterRunMetricGrid is the run grid's metric queries: runs across parsers and in several segments under
// range aggregations, and a line_format Loki drops because nothing reads it next to one a later filter keeps. The
// bytes aggregations, which keep every line_format (TestValidateLogQL_LineFormatInRangeAggregation, recorded on the
// stack), are left out: the oracle drops an unread line_format under every range aggregation.
func lineFilterRunMetricGrid() []string {
	return []string{
		`sum(count_over_time({app="a"} |= "x" | logfmt |~ "(" [5m]))`,
		`sum(count_over_time({app="a"} != ip("z") |~ "(" [5m]))`,
		`count_over_time({app="a"} |~ "(" | json |~ "[" [5m])`,
		`rate({app="a"} |~ "(" | x="1" |~ "[" [1m])`,
		`count_over_time({app="a"} | json x="a[" |~ "(" [5m])`,
		`count_over_time({app="a"} |~ "(" | line_format "{{.x}" [5m])`,
		`count_over_time({app="a"} | line_format "{{.x}" [5m])`,
		`count_over_time({app="a"} | line_format "{{.x}" |~ "(" [5m])`,
		`count_over_time({app="a"} | line_format "{{.x}" | json [5m])`,
		`sum_over_time({app="a"} |~ "(" | logfmt | unwrap y [5m])`,
		`sum_over_time({app="a"} |= "x" | logfmt |~ "(" | x="1" |> "<a>" | unwrap y [5m])`,
		`sum by (a) (rate({app="a"} |> "<a>" | json !> "<_><_>" [1m])) / sum(rate({app="b"} |~ "(" [1m]))`,
		`label_replace(rate({app="a"} |= "x" |~ "(" [1m]), "a", "$1", "b", "(.*)")`,
		`count_over_time({app="a"} |= ip("z") or "x" [5m])`,
		`count_over_time({app="a"} |= "a" or ip("z") or "c" |~ "(" [5m])`,
	}
}

const lineFilterRunMetricLokiAnswers = "testdata/line-filter-runs-metric-loki.jsonl"

// Metric queries check their line filter runs as log queries do, after Loki drops a line_format nothing reads.
// Loki's answers are recorded with the Loki oracle check command, which builds the pipelines of metric queries.
//
// conformance: semantics/line-filter-error-quotes-the-run
func TestLineFilterRunMetricQueriesMatchLoki(t *testing.T) {
	requireLokiAnswers(t, "check", lineFilterRunMetricGrid(), lineFilterRunMetricLokiAnswers, nil)
}
