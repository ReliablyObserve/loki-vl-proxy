package querycorpus_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
)

// The grammar sweep (grammar_sweep_test.go) checks the proxy against Loki and VictoriaLogs on queries enumerated from
// the LogQL grammar: this file is the enumeration. The grammar is a table of short parts (a selector, a stage, a range
// function, a range, an offset, a grouping, an aggregation, a binary operator and its modifier), joined by templates
// (a log query, a range aggregation, a vector aggregation, a binary operation, a label_replace, a literal, the white
// space and comments between tokens). The parts of one template are combined by a covering design, not the full
// product: every value of every part appears at least once (a row with the baseline in every other part), and every
// pair of classes of two parts appears in some row (a class is a kind of stage or function; a value of a class is
// drawn with a seeded generator, in several rounds). The same seed gives the same queries, so a recording of the
// answers of Loki and VictoriaLogs can be held to the query set by a hash.
//
// The parts are short on purpose: the queries are composed, they are not a copy of any other project's test list.

// sweepSeed is the seed of the generator. LVP_GRAMMAR_SWEEP_SEED replaces it to explore other combinations; a
// recording belongs to the default.
const sweepSeed = 20261112

// sweepRounds is how many times the class pairs are drawn with other values of the classes: the size knob. The
// default size runs with the recorded answers in the unit tests; LVP_GRAMMAR_SWEEP_SIZE=full is the nightly size.
func sweepRounds() (pairs, triples int) {
	if os.Getenv("LVP_GRAMMAR_SWEEP_SIZE") == "full" {
		return 24, 8
	}
	return 6, 2
}

// sweepValue is one value of a part and its class: values of one class are alike (a kind of stage, a family of
// functions), and the covering design pairs classes, so every pair of kinds meets.
//
// A value is odd when it is wrong or unusual (a stage Loki rejects, a range with a typo, a selector with a dot): a row
// holds at most one odd value, so the answer for it is the answer for that value, not for whichever of two defects an
// answer names first. Every odd value is tried in the context of other values, in each round.
type sweepValue struct {
	v, cls string
	odd    bool
}

// vals makes the values of one class.
func vals(cls string, vs ...string) []sweepValue {
	out := make([]sweepValue, len(vs))
	for i, v := range vs {
		out[i] = sweepValue{v: v, cls: cls}
	}
	return out
}

// oddv makes odd values of one class.
func oddv(cls string, vs ...string) []sweepValue {
	out := vals(cls, vs...)
	for i := range out {
		out[i].odd = true
	}
	return out
}

func join(groups ...[]sweepValue) []sweepValue {
	var out []sweepValue
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// sweepPart is one part of a template: the values it takes and the one it takes in the baseline row.
type sweepPart struct {
	name     string
	vals     []sweepValue
	baseline int
}

// sweepTemplate joins the parts of one query.
type sweepTemplate struct {
	name   string
	parts  []sweepPart
	render func(v []string) string
}

// sweepQuery is one generated query and the values of the parts it holds, which attribute a difference to them.
type sweepQuery struct {
	Q    string
	Tags []string
}

func bt(s string) string { return "`" + s + "`" }

// ---- parts ----

func selectorValues() []sweepValue {
	return join(
		vals("eq", `{a="b"}`, `{a="b",}`, `{ a = "b" }`, `{a="b", c="d"}`, `{a="é"}`, `{a="b\"c"}`,
			`{a="\x41"}`, `{a="é"}`, `{a=`+bt(`b\d`)+`}`, `{app="x", level="error"}`, `{a_b="c"}`),
		oddv("eq", `{a="b", a="c"}`, `{a=""}`, `{__name__="x"}`, `{a.b="c"}`),
		vals("neq", `{a!="b"}`, `{a="b", c!="d"}`),
		oddv("neq", `{a!=""}`),
		vals("re", `{a=~"b.*"}`, `{a=~".+"}`, `{a=~"(?i)b"}`, `{a=~"b|c", d="e"}`),
		oddv("re", `{a=~".*"}`, `{a=~""}`, `{a=~"a|"}`, `{a=~"["}`, `{a=~"(b"}`, `{a=~"b{1001}"}`),
		vals("nre", `{a!~"b"}`, `{a!~"b", c="d"}`),
		oddv("nre", `{a!~".*"}`),
		oddv("bad", `{}`, `{a}`, `{a=}`, `{="b"}`, `{a="b"`, `a="b"`, `{a="b"}}`, `{a="b" c="d"}`, `{"a"="b"}`, `{a=1}`, `{a=~~"b"}`, `{a=="b"}`),
	)
}

func lineFilterValues() []sweepValue {
	return join(
		vals("lf", `|= "x"`, `!= "x"`, `|~ "x.*"`, `!~ "x"`, `|= ""`, `|~ ""`, `|= "é"`, `|= "\\d"`, `|= `+bt("x"), `|~ "(?i)x"`, `|= "a" |= "b"`, `!= "x" != "y"`),
		vals("lfor", `|= "a" or "b"`, `|~ "a" or "b" or "c"`, `!= "a" or "b"`, `!~ "a" or "b"`, `|= "a" or ip("1.1.1.1")`),
		oddv("lfor", `|= "a" or`, `|= or "a"`),
		vals("lfpat", `|> "<_>x"`, `!> "<_>x"`, `|> "<_> x <_>"`, `!> "<_> <_>"`, `|> ""`, `|> "x"`),
		oddv("lfpat", `|> "<a>"`, `|> "<_><_>"`),
		vals("lfip", `|= ip("1.2.3.4")`, `!= ip("10.0.0.0/8")`, `|= ip("1.2.3.4-1.2.3.9")`, `|= ip("::1")`),
		oddv("lfip", `|= ip("bad")`, `|~ ip("1.2.3.4")`, `|= ip()`),
		oddv("lfbad", `|=`, `|= x`, `|= 1`, `|~ "("`, `|= "x`),
	)
}

func parserValues() []sweepValue {
	return join(
		vals("json", `| json`, `| json a="b"`, `| json a="b.c", d="e[0]"`, `| json --strict`, `| json --keep-empty`, `| json --strict --keep-empty a="b"`,
			`| json a`, `| json a, b`, `| json a="b" | json c="d"`, `| json a="b[\"c\"]"`),
		oddv("json", `| json "a"="b"`, `| json a=b`, `| json a="b",`, `| json a="b" c="d"`, `| json --foo`),
		vals("logfmt", `| logfmt`, `| logfmt --strict`, `| logfmt --keep-empty`, `| logfmt a, b="c"`, `| logfmt --strict --keep-empty a`, `| logfmt a="b"`),
		oddv("logfmt", `| logfmt a=b`, `| logfmt --foo`, `| logfmt a,`),
		vals("regexp", `| regexp "(?P<a>x)"`, `| regexp "(?P<a>x)(?P<b>y)"`, `| regexp `+bt(`(?P<a>\d+)`)),
		oddv("regexp", `| regexp "x"`, `| regexp "("`, `| regexp ""`, `| regexp "(?P<a>x)(?P<a>y)"`, `| regexp "(?P<1>x)"`),
		vals("pattern", `| pattern "<a> <_> <b>"`, `| pattern "<_>"`, `| pattern `+bt("<ip> - <_>")),
		oddv("pattern", `| pattern "x"`, `| pattern "<a><b>"`, `| pattern ""`, `| pattern "<a> <a>"`, `| pattern "<1a>"`),
		vals("unpack", `| unpack`),
		oddv("unpack", `| unpack a`),
	)
}

// labelFilterValues are the label filters of every kind of value: string, number, duration, bytes, ip(), and the
// connectives and parentheses between them.
func labelFilterValues() []sweepValue {
	var num, dur, byt []string
	for _, op := range []string{"==", "!=", ">", ">=", "<", "<="} {
		num = append(num, "| a "+op+" 1")
		dur = append(dur, "| a "+op+" 1s")
		byt = append(byt, "| a "+op+" 1KB")
	}
	return join(
		vals("lbstr", `| a="b"`, `| a!="b"`, `| a=~"b"`, `| a!~"b"`, `| a = "b"`, `| a =~ "(?i)x"`, `| a=""`, `| a=~""`, `| a=`+bt("b"), `| a=="b"`, `| __error__=""`, `| __error__!=""`, `| detected_level="error"`),
		oddv("lbstr", `| a=~"("`, `| a>"x"`, `| a=b`),
		vals("lbnum", append(num, `| a==1.5`, `| a>=-1`, `| a> +1`, `| a<1e3`, `| a<=0x10`, `| a>.5`)...),
		oddv("lbnum", `| a>1_000`, `| a>1.`, `| a>NaN`, `| a>Inf`),
		vals("lbdur", append(dur, `| a>1h30m`, `| a<1ms`, `| a>=0s`, `| a>1d`, `| a>1.5h`, `| a>-1s`, `| a>1us`)...),
		vals("lbbyte", append(byt, `| a<1MiB`, `| a>=1.5GB`, `| a>1B`, `| a>1PB`)...),
		oddv("lbbyte", `| a>1Kib`, `| a>1k`),
		vals("lbip", `| a=ip("1.2.3.4")`, `| a!=ip("10.0.0.0/8")`, `| a=ip("1.2.3.4-1.2.3.9")`),
		oddv("lbip", `| a=ip("bad")`, `| a>ip("1.2.3.4")`, `| a=~ip("1.2.3.4")`),
		vals("lbtree", `| a="b" and c="d"`, `| a="b" or c="d"`, `| a="b", c="d"`, `| (a="b" or c="d") and e>1`, `| a="b" and (c="d" or e="f")`, `| a=1 or b>2s and c="x"`,
			`| (a="b")`, `| ((a="b"))`),
		oddv("lbtree", `| a="b" and`, `| ( a="b"`, `| a="b" c="d"`, `| a="b" , or c="d"`, `| a="b" AND c="d"`, `| a="b" OR c="d"`, `| not a="b"`),
	)
}

func formatValues() []sweepValue {
	fns := []string{"upper", "lower", "title", "trim", "trimAll \"x\"", "trimPrefix \"x\"", "trimSuffix \"x\"", "trunc 3", "substr 0 3", "repeat 2", "replace \"a\" \"b\"",
		"indent 2", "contains \"x\"", "hasPrefix \"x\"", "hasSuffix \"x\"", "add 1", "sub 1", "mul 2", "div 2", "mod 2", "ceil", "floor", "round 2", "int", "float64", "toString",
		"date \"2006\"", "toDate \"2006\"", "unixEpoch", "duration", "duration_seconds", "bytes", "count \"x\"", "regexReplaceAll \"a\" \"b\"", "urlencode", "urldecode",
		"b64enc", "b64dec", "fromJson", "default \"x\"", "alignLeft 5", "alignRight 5"}
	var lf []string
	for _, f := range fns {
		lf = append(lf, `| line_format "{{ .a | `+f+` }}"`)
	}
	lf = append(lf, `| line_format "{{ now }}"`, `| line_format "{{ __line__ }}"`, `| line_format "{{ __timestamp__ }}"`, `| line_format "{{.a}}"`, `| line_format `+bt("{{.a}}"),
		`| line_format ""`, `| line_format "x"`, `| line_format "{{ if .a }}x{{ else }}y{{ end }}"`,
		`| line_format "{{ range .a }}x{{ end }}"`, `| line_format "{{ printf \"%s\" .a }}"`, `| line_format "{{ .a }}" | line_format "{{ .b }}"`, `| line_format "{{ eq .a \"x\" }}"`)
	var fl []string
	for _, f := range fns[:12] {
		fl = append(fl, `| label_format a="{{ .b | `+f+` }}"`)
	}
	fl = append(fl, `| label_format a=b`, `| label_format a="{{.b}}"`, `| label_format a=b, c="{{.d}}"`, `| label_format a=a`, `| label_format a=`+bt("{{.b}}"),
		`| label_format a=b, c=d, e=f`, `| label_format a="x"`)
	return join(vals("linefmt", lf...), vals("labelfmt", fl...),
		oddv("linefmt", `| line_format "{{ .a | nope }}"`, `| line_format "{{.a"`, `| line_format "{{ .a | }}"`, `| line_format x`, `| line_format`),
		oddv("labelfmt", `| label_format a=b, a=c`, `| label_format __error__="x"`, `| label_format a="{{"`, `| label_format a=`, `| label_format`, `| label_format a=1`, `| label_format a="x" b="y"`,
			`| label_format a=b,`, `| label_format a=__line__`, `| label_format a.b=c`, `| label_format "a"=b`))
}

func dropKeepValues() []sweepValue {
	return join(
		vals("drop", `| drop a`, `| drop a, b`, `| drop a="b"`, `| drop a=~"b", c`, `| drop __error__`, `| drop __error__, __error_details__`, `| drop a!="b"`, `| drop a!~"b", c!="d"`),
		oddv("drop", `| drop a>5`, `| drop a=~"("`, `| drop`, `| drop a,`, `| drop a b`, `| drop a="b" and c="d"`, `| drop a.b`, `| drop "a"`),
		vals("keep", `| keep a`, `| keep a, b`, `| keep a="b"`, `| keep a=~"x"`, `| keep a, b="c"`),
		oddv("keep", `| keep`, `| keep __error__`, `| keep a,`, `| keep a=~"("`, `| keep a>1`),
		vals("deco", `| decolorize`),
		oddv("deco", `| decolorize x`),
		oddv("stagebad", `|`, `| |= "x"`, `| foo`, `|=`, `| and`, `| (`, `| json |`),
	)
}

func unwrapValues() []sweepValue {
	return join(
		oddv("unwrap", `| unwrap a`, `| unwrap duration(a)`, `| unwrap duration_seconds(a)`, `| unwrap bytes(a)`, `| unwrap a | a>1`, `| unwrap foo(a)`, `| unwrap`,
			`| unwrap a | unwrap b`, `| unwrap a, b`, `| unwrap a="b"`, `| unwrap duration(a`, `| unwrap a | json`, `| unwrap a | line_format "x"`, `| unwrap a | drop b`, `| unwrap a | label_format c=d`,
			`| unwrap Duration(a)`, `| unwrap "a"`, `| unwrap __error__`, `| unwrap a | __error__=""`))
}

func stageValues() []sweepValue {
	return join(lineFilterValues(), parserValues(), labelFilterValues(), formatValues(), dropKeepValues(), unwrapValues(), []sweepValue{{v: "", cls: "none"}})
}

// logQueryPool are the log queries inside a range aggregation: a selector with the pipelines the functions read.
func logQueryPool() []sweepValue { return join(plainLogQueries(), unwrapLogQueries()) }

func plainLogQueries() []sweepValue {
	return join(
		vals("sel", `{a="b"}`, `{a=~".*"}`, `{a="b", c=~"d.*"}`),
		vals("filter", `{a="b"} |= "x"`, `{a="b"} != "x" |~ "y"`, `{a="b"} |> "<_>x"`, `{a="b"} |= ip("1.2.3.4")`, `{a="b"} |= "x" or "y"`),
		vals("parse", `{a="b"} | json`, `{a="b"} | logfmt`, `{a="b"} | logfmt | c="d"`, `{a="b"} | json | __error__=""`, `{a="b"} | logfmt | drop __error__`, `{a="b"} | regexp "(?P<x>.)"`, `{a="b"} | pattern "<x>" | x>5`, `{a="b"} | unpack`, `{a="b"} | json | c>1s`),
		vals("fmt", `{a="b"} | line_format "x"`, `{a="b"} | label_format x=y`, `{a="b"} | json | line_format "{{.c}}" | c="d"`),
		oddv("fmt", `{a="b"} | line_format "{{.x"`),
		vals("dk", `{a="b"} | drop x`, `{a="b"} | keep x`, `{a="b"} | decolorize`, `{a="b"} | keep x | drop y`),
	)
}

func unwrapLogQueries() []sweepValue {
	return join(
		vals("unwrap", `{a="b"} | json | unwrap c`, `{a="b"} | unwrap c`, `{a="b"} | logfmt | unwrap duration(c)`, `{a="b"} | unwrap bytes(c) | c>1`, `{a="b"} | regexp "(?P<x>.)" | unwrap x`,
			`{a="b"} | unwrap duration_seconds(x) | __error__=""`, `{a="b"} | json | unwrap c | drop __error__`),
		oddv("unwrap", `{a="b"} | unwrap c | unwrap d`, `{a="b"} | unwrap foo(c)`),
	)
}

func rangeValues() []sweepValue {
	return join(
		vals("dur", "5m", "1h", "1h30m", "90s", "1d", "1w", "100ms", "1h5m3s"),
		oddv("durodd", "1.5m", "0s", "5", "5x", "1y", "1us", "1µs", "1ns", "5M", "-5m", "", " 5m ", "5m:1m", "5m 1m", "5m5m", "1h1h", "0", "00m", "9999999999h", "1E3s"),
	)
}

func offsetValues() []sweepValue {
	return join(
		vals("off", "", " offset 5m", " offset 0s", " offset 1h30m", " offset 1d", " offset 1ms"),
		oddv("offodd", " offset -5m", " offset 5", " offset 1.5h", " offset 5m offset 5m", " offset", " OFFSET 5m", " offset 5m ", " offset 5x", " offset 1y", " offset 0", " offset +5m"),
	)
}

func groupingValues() []sweepValue {
	return join(
		vals("grp", "", " by (a)", " by (a, b)", " without (a)", " by ()", " without ()"),
		groupingOdd())
}

func groupingOdd() []sweepValue {
	return join(
		oddv("grpodd", " by (a, a)", " by (a,)", " by a", " by (__error__)", " by (a.b)", " by (\"a\")", " by(a)", " BY (a)", " without (a, b, c)", " by (a) without (b)", " by (1)", " by (", " without a", " by (a b)", " by (é)"),
	)
}

// The range functions are formats of the log query with its range and offset (%s) and, when the function takes one, its
// other argument.
func plainFnValues() []sweepValue {
	return vals("rate", "rate(%s)", "bytes_rate(%s)", "count_over_time(%s)", "bytes_over_time(%s)", "absent_over_time(%s)")
}

func unwrapFnValues() []sweepValue {
	return join(
		vals("unwrapfn", "rate_counter(%s)", "sum_over_time(%s)", "avg_over_time(%s)", "max_over_time(%s)", "min_over_time(%s)", "stddev_over_time(%s)", "stdvar_over_time(%s)", "first_over_time(%s)", "last_over_time(%s)"),
		vals("quantile", "quantile_over_time(0.5, %s)", "quantile_over_time(0, %s)", "quantile_over_time(1, %s)", "quantile_over_time(.5, %s)"),
		oddv("quantile", "quantile_over_time(1.5, %s)", "quantile_over_time(-1, %s)",
			"quantile_over_time(%s)", "quantile_over_time(0.5 %s)", "quantile_over_time(\"x\", %s)", "quantile_over_time(0.5, 0.6, %s)"))
}

func oddFnValues() []sweepValue {
	return oddv("fnbad", "ratee(%s)", "RATE(%s)", "rate (%s)", "rate()", "rate(%s,)", "count_over_time(%s, 1)", "sum_over_time(%s) by (a)", "rate{%s}", "avg(%s)", "rate_over_time(%s)", "increase(%s)", "irate(%s)", "present_over_time(%s)", "mad_over_time(%s)")
}

func aggValues() []sweepValue {
	return join(
		vals("agg", "sum|", "avg|", "min|", "max|", "count|", "stddev|", "stdvar|"),
		oddv("agg", "group|"),
		vals("rank", "topk|2, ", "bottomk|2, ", "topk|1, "),
		oddv("rank", "sort|", "sort_desc|", "topk|0, ", "topk|-1, ", "topk|1.5, ", "bottomk|0, ", "topk|a, ", "topk|", "sort|1, ", "approx_topk|2, ", "topk|9999999999, "),
		oddv("aggbad", "count_values|\"x\", ", "quantile|0.5, ", "limitk|2, ", "SUM|", "sum|1, ", "avg|2, ", "stddev|2, ", "max|\"x\", ", "last|", "sum_over_time|"),
	)
}

func metricInnerPool() []sweepValue {
	return join(
		vals("rangeagg", `rate({a="b"}[5m])`, `count_over_time({a="b"} | json [1h])`, `sum_over_time({a="b"} | unwrap c [5m])`, `bytes_over_time({a="b"}[5m] offset 1h)`,
			`quantile_over_time(0.9, {a="b"} | logfmt | unwrap d [5m]) by (e)`, `absent_over_time({a="b"}[5m])`, `rate_counter({a="b"} | unwrap c [5m])`, `count_over_time({a="b"} |= "x" | json | c="d" [1m])`),
		vals("nested", `sum(rate({a="b"}[5m]))`, `sum by (a) (count_over_time({a="b"}[5m]))`, `topk(1, rate({a="b"}[5m]))`, `rate({a="b"}[5m]) * 2`, `(rate({a="b"}[5m]))`, `-rate({a="b"}[5m])`,
			`label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)")`, `rate({a="b"}[5m]) / rate({a="b"}[5m] offset 1h)`, `max(sum by (a, b) (rate({a="b"}[5m])))`),
		vals("scalar", `1`, `1.5`, `vector(1)`, `vector(0)`, `1 + 1`),
		oddv("notmetric", `{a="b"}`, `{a="b"} | json`, ``, `a`, `"x"`, `{a="b"}[5m]`),
	)
}

// ---- covering design ----

// coverClasses returns rows of value indexes: every pair (t = 2) or triple (t = 3) of classes of two or three parts
// is in at least one row, with the greedy construction of a covering array. A part with one class takes no part.
func coverClasses(classes [][]int, t int, rng *rand.Rand) [][]int {
	n := len(classes)
	if n < t {
		t = n
	}
	if t < 1 {
		return nil
	}
	count := make([]int, n)
	radix := 1
	for i, c := range classes {
		for _, k := range c {
			if k+1 > count[i] {
				count[i] = k + 1
			}
		}
		if count[i]+1 > radix {
			radix = count[i] + 1
		}
	}
	combos := combinations(n, t)
	byPart := make([][]int, n)
	for ci, c := range combos {
		for _, p := range c {
			byPart[p] = append(byPart[p], ci)
		}
	}
	covered := map[uint64]bool{}
	key := func(ci int, row []int) uint64 {
		k := uint64(ci)
		for _, p := range combos[ci] {
			k = k*uint64(radix) + uint64(row[p])
		}
		return k
	}
	// The classes present at each part.
	present := make([][]int, n)
	for i, c := range classes {
		seen := map[int]bool{}
		for _, k := range c {
			if !seen[k] {
				seen[k] = true
				present[i] = append(present[i], k)
			}
		}
		sort.Ints(present[i])
	}
	var rows [][]int
	row := make([]int, n)
	for ci, combo := range combos {
		idx := make([]int, len(combo))
		for {
			for i := range row {
				row[i] = -1
			}
			for i, p := range combo {
				row[p] = present[p][idx[i]]
			}
			if !covered[key(ci, row)] {
				order := rng.Perm(n)
				for _, p := range order {
					if row[p] >= 0 {
						continue
					}
					best, bestScore := present[p][rng.Intn(len(present[p]))], -1
					for _, k := range present[p] {
						row[p] = k
						score := 0
						for _, cj := range byPart[p] {
							ok := true
							for _, q := range combos[cj] {
								if row[q] < 0 {
									ok = false
									break
								}
							}
							if ok && !covered[key(cj, row)] {
								score++
							}
						}
						if score > bestScore {
							best, bestScore = k, score
						}
					}
					row[p] = best
				}
				for cj := range combos {
					covered[key(cj, row)] = true
				}
				rows = append(rows, append([]int(nil), row...))
			}
			// Next tuple of classes of this combination.
			i := len(combo) - 1
			for i >= 0 {
				idx[i]++
				if idx[i] < len(present[combo[i]]) {
					break
				}
				idx[i] = 0
				i--
			}
			if i < 0 {
				break
			}
		}
	}
	return rows
}

func combinations(n, t int) [][]int {
	var out [][]int
	var rec func(start int, cur []int)
	rec = func(start int, cur []int) {
		if len(cur) == t {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := start; i < n; i++ {
			rec(i+1, append(cur, i))
		}
	}
	rec(0, nil)
	return out
}

// expand turns the rows of a covering design into the queries of a template: a class takes a value drawn from the
// non-odd values of that class, so the rounds with other draws meet other values; the baseline row for each value of
// each part holds every other part at its baseline; each odd value is put in the place of one value of a drawn row.
func (tpl sweepTemplate) expand(rng *rand.Rand, pairRounds, tripleRounds int) []sweepQuery {
	var out []sweepQuery
	add := func(idx []int) {
		v := make([]string, len(idx))
		for i, k := range idx {
			v[i] = tpl.parts[i].vals[k].v
		}
		var tags []string
		for i, s := range v {
			if s != "" {
				tags = append(tags, tpl.name+" "+tpl.parts[i].name+" "+s, tpl.name+" "+tpl.parts[i].name+":"+tpl.parts[i].vals[idx[i]].cls)
			}
		}
		out = append(out, sweepQuery{Q: tpl.render(v), Tags: tags})
	}
	base := make([]int, len(tpl.parts))
	for i, p := range tpl.parts {
		base[i] = p.baseline
	}
	// One part moves, the others stay at the baseline.
	for i, p := range tpl.parts {
		for k := range p.vals {
			idx := append([]int(nil), base...)
			idx[i] = k
			add(idx)
		}
	}
	// The class pairs (and triples) of the usual values, drawn in rounds.
	classOf := make([][]int, len(tpl.parts))
	members := make([]map[int][]int, len(tpl.parts))
	for i, p := range tpl.parts {
		names := map[string]int{}
		members[i] = map[int][]int{}
		for k, v := range p.vals {
			if v.odd {
				continue
			}
			c, ok := names[v.cls]
			if !ok {
				c = len(names)
				names[v.cls] = c
			}
			classOf[i] = append(classOf[i], c)
			members[i][c] = append(members[i][c], k)
		}
	}
	var odds [][2]int
	for i, p := range tpl.parts {
		for k, v := range p.vals {
			if v.odd {
				odds = append(odds, [2]int{i, k})
			}
		}
	}
	draw := func(t, rounds int, withOdd bool) {
		for r := 0; r < rounds; r++ {
			var rows [][]int
			for _, row := range coverClasses(classOf, t, rng) {
				idx := make([]int, len(row))
				for i, c := range row {
					m := members[i][c]
					idx[i] = m[rng.Intn(len(m))]
				}
				rows = append(rows, idx)
				add(idx)
			}
			if !withOdd || len(rows) == 0 {
				continue
			}
			// An odd value goes into the baseline row, with one other part moved to a usual value: the defect is
			// the odd value, in a context of its own.
			for _, o := range odds {
				idx := append([]int(nil), base...)
				if j := rng.Intn(len(tpl.parts)); j != o[0] {
					idx[j] = rows[rng.Intn(len(rows))][j]
				}
				idx[o[0]] = o[1]
				add(idx)
			}
		}
	}
	draw(2, pairRounds, true)
	if tripleRounds > 0 {
		draw(3, tripleRounds, false)
	}
	return out
}

// ---- templates ----

func part(name string, vs []sweepValue, baseline int) sweepPart {
	return sweepPart{name: name, vals: vs, baseline: baseline}
}

func sweepTemplates() []sweepTemplate {
	sels := selectorValues()
	stages := stageValues()
	none := len(stages) - 1
	logParts := func() []sweepPart {
		return []sweepPart{part("selector", sels, 0), part("stage", stages, none), part("stage", stages, none), part("stage", stages, none)}
	}
	renderLog := func(v []string) string {
		var b strings.Builder
		b.WriteString(v[0])
		for _, s := range v[1:] {
			if s != "" {
				b.WriteString(" " + s)
			}
		}
		return b.String()
	}
	rangesV := rangeValues()
	offs := offsetValues()
	grps := groupingValues()
	aggs := aggValues()
	inners := metricInnerPool()
	ops := join(
		vals("arith", "+", "-", "*", "/", "%", "^"),
		vals("cmp", "==", "!=", ">", "<", ">=", "<="),
		vals("set", "and", "or", "unless"),
		oddv("opbad", "&&", "||", "=", "<>", "**", "//", "===", "!", "xor", "AND", "+-", "- -"),
	)
	mods := join(
		vals("mod", "", " bool", " on (a)", " on ()", " ignoring (a)", " ignoring ()", " bool on (a)", " on (a, b)", " ignoring (a, b) bool"),
		vals("group", " on (a) group_left", " on (a) group_left (b)", " on (a) group_right (b)", " ignoring (a) group_left", " group_left", " on (a, b) group_left (c, d)", " on (a) group_left ()", " on(a)group_left(b)", " on (a) group_right"),
		oddv("modbad", " on (a) bool", " on", " on a", " group_left (b)", " bool bool", " on (a) on (b)", " on (a) group_left group_right", " BOOL", " ignoring on (a)", " on (a) group_left (b) group_right (c)"),
	)
	operands := join(
		vals("rangeagg", `rate({a="b"}[5m])`, `count_over_time({a="b"} | json [1h])`, `sum_over_time({a="b"} | unwrap c [5m])`, `rate({a="b"}[5m] offset 1h)`),
		vals("vecagg", `sum(rate({a="b"}[5m]))`, `sum by (a) (rate({a="b"}[5m]))`, `sum without (a) (rate({a="b"}[5m]))`, `topk(2, rate({a="b"}[5m]))`, `avg by (a, b) (count_over_time({a="b"}[1m]))`),
		vals("scalar", `1`, `2.5`, `0`, `-1`, `vector(1)`, `1 + 2`, `.5`),
		vals("nest", `(rate({a="b"}[5m]))`, `-rate({a="b"}[5m])`, `rate({a="b"}[5m]) * 2`, `(rate({a="b"}[5m]) + rate({a="c"}[5m]))`, `label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)")`),
		oddv("notmetric", `{a="b"}`, `{a="b"} | json`, ``, `"x"`),
	)
	rangeAggRender := func(v []string) string {
		fn, logq, rng, off, ord, grp := v[0], v[1], v[2], v[3], v[4], v[5]
		var in string
		if ord == "swap" {
			in = logq + off + " [" + rng + "]"
		} else {
			in = logq + "[" + rng + "]" + off
		}
		out := strings.Replace(fn, "%s", in, 1)
		return out + grp
	}
	tpls := []sweepTemplate{
		{name: "log", parts: logParts(), render: renderLog},
		// A function that needs no unwrap over a log query without one, an unwrap function over one with it, and every
		// function over every log query.
		{name: "rangeagg", parts: []sweepPart{
			part("fn", join(plainFnValues(), oddFnValues()), 0), part("logquery", plainLogQueries(), 0), part("range", rangesV, 0), part("offset", offs, 0),
			part("layout", join(vals("l", "std"), oddv("l", "swap")), 0), part("grouping", join(vals("grp", ""), groupingOdd(), oddv("grp", " by (a)", " without (a)")), 0)}, render: rangeAggRender},
		{name: "rangeagg unwrap", parts: []sweepPart{
			part("fn", unwrapFnValues(), 0), part("logquery", unwrapLogQueries(), 0), part("range", rangesV, 0), part("offset", offs, 0),
			part("layout", join(vals("l", "std"), oddv("l", "swap")), 0), part("grouping", grps, 0)}, render: rangeAggRender},
		{name: "rangeagg pairing", parts: []sweepPart{
			part("fn", join(plainFnValues(), unwrapFnValues()), 0), part("logquery", logQueryPool(), 0),
			part("range", vals("dur", "5m"), 0), part("offset", vals("off", ""), 0),
			part("layout", vals("l", "std"), 0), part("grouping", vals("grp", ""), 0)}, render: rangeAggRender},
		{name: "vecagg", parts: []sweepPart{
			part("agg", aggs, 0), part("grouping", grps, 0), part("inner", inners, 0), part("pos", vals("p", "pre", "post"), 0)},
			render: func(v []string) string {
				name, params, _ := strings.Cut(v[0], "|")
				if v[3] == "post" {
					return name + "(" + params + v[2] + ")" + v[1]
				}
				return name + v[1] + " (" + params + v[2] + ")"
			}},
		{name: "binary", parts: []sweepPart{part("op", ops, 0), part("modifier", mods, 0), part("left", operands, 0), part("right", operands, 0)},
			render: func(v []string) string { return v[2] + " " + v[0] + v[1] + " " + v[3] }},
		{name: "label_replace", parts: []sweepPart{
			part("inner", inners, 0),
			part("dst", join(vals("d", `"d"`, `"a"`), oddv("d", `""`, `"a.b"`, `"__name__"`, `d`)), 0),
			part("replacement", join(vals("r", `"$1"`, `"x"`, `"${1}y"`), oddv("r", `""`, `"$"`, `"$$"`)), 0),
			part("src", join(vals("s", `"a"`, `"b"`), oddv("s", `""`, `"__name__"`, `a`)), 0),
			part("regex", join(vals("re", `"(.*)"`, `".+"`, `"^(a)(b)$"`, `"(?P<n>x)"`), oddv("re", `""`, `"("`, `"a|"`, `"[a-"`)), 0),
			part("shape", join(vals("shape", "full"), oddv("shape", "short", "long", "nocomma")), 0)},
			render: func(v []string) string {
				args := []string{v[0], v[1], v[2], v[3], v[4]}
				switch v[5] {
				case "short":
					args = args[:4]
				case "long":
					args = append(args, `"x"`)
				case "nocomma":
					return "label_replace(" + strings.Join(args, " ") + ")"
				}
				return "label_replace(" + strings.Join(args, ", ") + ")"
			}},
		{name: "vector", parts: []sweepPart{
			part("arg", join(vals("n", "1", "0", "-1", "1.5", "1e3", ".5", "0x10"), oddv("n", "", `"a"`, "a", "1, 2", "NaN", "Inf", "1_000", "+1", "-Inf", "(1)", "1 + 1", "rate({a=\"b\"}[5m])")), 0),
			part("wrap", join(vals("w", "%s", "sum(%s)", "%s + 1", "1 + %s", "sum by (a) (%s)", "-%s", "(%s)", "%s > bool 0", "topk(1, %s)", "label_replace(%s, \"a\", \"b\", \"c\", \"d\")"), oddv("w", "rate(%s)", "%s by (a)")), 0)},
			render: func(v []string) string { return strings.Replace(v[1], "%s", "vector("+v[0]+")", 1) }},
		{name: "literal", parts: []sweepPart{
			part("number", join(vals("n", "1", "1.5", ".5", "1e3", "0x1F", "1e+3", "1E3", "-1", "+1", "1 + 2", "1 + 2 * 3", "(1)", "-(1)", "1 == bool 2", "2 ^ 3 ^ 2", "1 - -1", "(((1)))"),
				oddv("n", "1_000", "0b1", "Inf", "+Inf", "-Inf", "NaN", "1 and 2", "1 / 0", "0 / 0", "1 % 0", "1s", "1.", "00", "01", "1e", "0x", "1.5.5", "1__0", "0o17", "9223372036854775808", "1e999", "- 1", "+ 1", "--1", "1 1", "")), 0),
			part("wrap", join(vals("w", "%s", "%s + rate({a=\"b\"}[5m])", "rate({a=\"b\"}[5m]) + %s", "sum(%s)", "rate({a=\"b\"}[5m]) > bool %s", "topk(%s, rate({a=\"b\"}[5m]))", "{a=\"b\"} | a > %s", "{a=\"b\"} | a == %s"), oddv("w", "quantile_over_time(%s, {a=\"b\"} | unwrap c [5m])")), 0)},
			render: func(v []string) string { return strings.Replace(v[1], "%s", v[0], 1) }},
	}
	return tpls
}

// spacingQueries are queries with other white space and comments between their tokens: for each base query, every
// separator in place of every single space outside a string, and in place of all of them.
func spacingQueries() []sweepQuery {
	bases := []string{
		`{a="b"} |= "x" | json | c="d"`, `rate({a="b"}[5m])`, `sum by (a) (rate({a="b"}[5m]))`, `sum (rate({a="b"}[5m])) by (a)`,
		`rate({a="b"}[5m]) / on (a) group_left (b) rate({a="c"}[5m] offset 1h)`, `quantile_over_time(0.5, {a="b"} | unwrap c [5m]) by (d)`,
		`{a="b"} | line_format "{{.a}}" | label_format x=y | drop z`, `topk(2, sum by (a) (rate({a="b"}[5m])))`, `label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)")`,
		`count_over_time({a="b"} | logfmt --strict | unwrap duration(c) [1m] offset 5m)`, `vector(1) + 1`, `{a="b"} | a > 1 and (b = "c" or d =~ "e")`,
	}
	seps := []string{"\t", "\n", "  ", "\r\n", " # c\n", "#c\n", " ", " ", "\u0085", "\v", "\f"}
	var out []sweepQuery
	for _, b := range bases {
		out = append(out, sweepQuery{Q: " " + b + " ", Tags: []string{"spacing lead and trail"}}, sweepQuery{Q: "\n" + b + "\n", Tags: []string{"spacing newlines"}},
			sweepQuery{Q: b + " # trailing", Tags: []string{"spacing trailing comment"}}, sweepQuery{Q: "# lead\n" + b, Tags: []string{"spacing leading comment"}},
			sweepQuery{Q: b + "\n# c", Tags: []string{"spacing comment line"}})
		gaps := blankGaps(b)
		for _, sep := range seps {
			tag := fmt.Sprintf("spacing separator %q", sep)
			out = append(out, sweepQuery{Q: strings.ReplaceAll(b, " ", sep), Tags: []string{tag, "spacing every blank"}})
			for _, g := range gaps {
				out = append(out, sweepQuery{Q: b[:g] + sep + b[g+1:], Tags: []string{tag, "spacing one blank"}})
			}
		}
		for _, g := range gaps {
			out = append(out, sweepQuery{Q: b[:g] + b[g+1:], Tags: []string{"spacing removed"}}, sweepQuery{Q: b[:g] + "   " + b[g+1:], Tags: []string{"spacing wide"}})
		}
	}
	return out
}

// blankGaps are the offsets of the single blanks of q outside its quoted strings.
func blankGaps(q string) []int {
	var out []int
	in := byte(0)
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case in != 0:
			if c == '\\' && in == '"' {
				i++
			} else if c == in {
				in = 0
			}
		case c == '"' || c == '`':
			in = c
		case c == ' ':
			out = append(out, i)
		}
	}
	return out
}

// sweepQueries is every query of the sweep, sorted and without duplicates, with the parts each one holds (the union
// over its duplicates).
func sweepQueries() []sweepQuery {
	seed := int64(sweepSeed)
	if s := os.Getenv("LVP_GRAMMAR_SWEEP_SEED"); s != "" {
		_, _ = fmt.Sscan(s, &seed)
	}
	pairs, triples := sweepRounds()
	var all []sweepQuery
	for i, tpl := range sweepTemplates() {
		rng := rand.New(rand.NewSource(seed + int64(i)))
		rounds := pairs
		all = append(all, tpl.expand(rng, rounds, triples)...)
	}
	all = append(all, spacingQueries()...)
	byQuery := map[string]*sweepQuery{}
	for _, q := range all {
		if q.Q == "" {
			continue
		}
		if e, ok := byQuery[q.Q]; ok {
			e.Tags = append(e.Tags, q.Tags...)
			continue
		}
		c := q
		byQuery[q.Q] = &c
	}
	out := make([]sweepQuery, 0, len(byQuery))
	for _, q := range byQuery {
		seen := map[string]bool{}
		var tags []string
		for _, t := range q.Tags {
			if !seen[t] {
				seen[t] = true
				tags = append(tags, t)
			}
		}
		sort.Strings(tags)
		out = append(out, sweepQuery{Q: q.Q, Tags: tags})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Q < out[j].Q })
	return out
}

// sweepHash is the hash of the query set: a recording made for another set is stale.
func sweepHash(qs []sweepQuery) string {
	h := sha256.New()
	for _, q := range qs {
		h.Write([]byte(q.Q))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:12])
}
