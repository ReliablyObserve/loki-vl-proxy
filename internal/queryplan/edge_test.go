package queryplan

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func keyOfText(t testing.TB, q string, in Inputs) *Plan {
	t.Helper()
	e, err := parseCanon(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	p, err := Build(e, in)
	if err != nil {
		t.Fatalf("build %q: %v", q, err)
	}
	checkKey(t, p, e, q)
	return p
}

const r5 = `rate({app="a"}[5m])`

// pair is two queries and whether Loki reads them as one: their keys are equal exactly then. Where the key keeps
// what Loki merges (the order of matchers, the spelling of a zero duration), the pair is listed as different, with
// the reason.
type pair struct {
	a, b  string
	equal bool
	why   string
}

func TestKeyEdgePairs(t *testing.T) {
	long127, long128 := strings.Repeat("x", 127), strings.Repeat("x", 128)
	pairs := []pair{
		// ---- spellings Loki reads as one query
		{`{a="x"}`, `{ a = "x" }`, true, "spacing in a selector"},
		{`{a="x"}`, "{a=`x`}", true, "backquoted value"},
		{`{a="x"}`, `{a="x"}`, true, "escaped value"},
		{`{a="x"} |= "e"`, `{a="x"}   |=   "e"`, true, "spacing in a line filter"},
		{`{a="x"} | json`, `{a="x"}|json`, true, "no spacing around the pipe"},
		{`{a="x"} | line_format "{{.m}}"`, "{a=\"x\"} | line_format `{{.m}}`", true, "backquoted template"},
		{`{a="x"} | label_format b="v"`, "{a=\"x\"} | label_format b=`v`", true, "backquoted label_format value"},
		{r5, `rate({app="a"}[300s])`, true, "300s is 5m"},
		{`rate({app="a"}[1h])`, `rate({app="a"}[60m])`, true, "60m is 1h"},
		{`rate({app="a"}[1d])`, `rate({app="a"}[24h])`, true, "24h is 1d"},
		{`rate({app="a"}[5m] offset 1h)`, `rate({app="a"}[5m] offset 60m)`, true, "equal offsets"},
		{r5, `(` + r5 + `)`, true, "parentheses"},
		{r5, `((` + r5 + `))`, true, "nested parentheses"},
		{`sum by (a) (` + r5 + `)`, `sum(` + r5 + `) by (a)`, true, "trailing grouping"},
		{`sum by (a,b) (` + r5 + `)`, `sum by ( a , b ) (` + r5 + `)`, true, "spacing in a grouping"},
		{`rate({app="a"} |= "e" [5m])`, `rate(({app="a"} |= "e")[5m])`, true, "parenthesised log range"},
		{`{a="x"} | s>1 and t<2`, `{a="x"} | s>1, t<2`, true, "and and comma"},
		{`{a="x"} | s>1 and t<2`, `{a="x"} | s>1 t<2`, true, "and and juxtaposition"},
		{`{a="x"} | s>1 or t<2`, `{a="x"} | (s>1 or t<2)`, true, "parenthesised or"},
		{`{a="x"} | json x="y"`, `{a="x"} | json   x = "y"`, true, "spacing in an extraction list"},
		{`topk(2, ` + r5 + `)`, `topk (2, ` + r5 + `)`, true, "spacing after topk"},
		{r5 + ` > 1`, r5 + ` >  1`, true, "spacing around a comparison"},

		// ---- queries Loki reads differently
		{`{a="x"}`, `{a="y"}`, false, "value"},
		{`{a="x"}`, `{b="x"}`, false, "name"},
		{`{a="x"}`, `{A="x"}`, false, "case of a name"},
		{`{a="x"}`, `{a="X"}`, false, "case of a value"},
		{`{a="x"}`, `{a=~"x"}`, false, "operator"},
		{`{a="x"}`, `{a!="x"}`, false, "negated operator"},
		{`{a=~"x"}`, `{a!~"x"}`, false, "negated regexp"},
		{`{a=""}`, `{a="", b=""}`, false, "one matcher or two"},
		{`{a="x", b="y"}`, `{b="y", a="x"}`, false, "matcher order is kept as written"},
		{`{ab="c"}`, `{a="bc"}`, false, "name and value share bytes"},
		{`{a="b", c="d"}`, `{a="b\x00c\x00d"}`, false, "values with NUL"},
		{`{a="` + long127 + `"}`, `{a="` + long128 + `"}`, false, "length prefix of one and two bytes"},
		{"{a=\"\u00e9\"}", "{a=\"e\u0301\"}", false, "composed and decomposed e acute: label values are bytes"},
		{`{a="x"}`, "{a=\"x\u200b\"}", false, "zero width space"},
		{"{a=\"\uff41\"}", `{a="a"}`, false, "full width a"},
		{"{a=\"\u212b\"}", "{a=\"\u00c5\"}", false, "angstrom sign and A ring: canonically equivalent, different bytes"},
		{`{a="x"} |= "e"`, `{a="x"} |= "E"`, false, "line filter case"},
		{`{a="x"} |= "e"`, `{a="x"} != "e"`, false, "line filter negation"},
		{`{a="x"} |= "e"`, `{a="x"} |> "e"`, false, "line filter pattern"},
		{`{a="x"} !~ "e"`, `{a="x"} !> "e"`, false, "negated regexp and pattern"},
		{`{a="x"} |= "a" or "b"`, `{a="x"} |= "b" or "a"`, false, "alternative order is kept"},
		{`{a="x"} |= "a" or "b"`, `{a="x"} |= "a" |= "b"`, false, "or and chain"},
		{`{a="x"} |= ip("1.2.3.4")`, `{a="x"} |= "1.2.3.4"`, false, "ip filter and literal"},
		{`{a="x"} |= ip("1.2.3.4")`, `{a="x"} |= "ip(1.2.3.4)"`, false, "ip filter and its text"},
		{`{a="x"} | json`, `{a="x"} | json a`, false, "extraction list or none"},
		{`{a="x"} | json a, b`, `{a="x"} | json b, a`, false, "extraction order"},
		{`{a="x"} | json a="b"`, `{a="x"} | json a="c"`, false, "extraction expression"},
		{`{a="x"} | json a`, `{a="x"} | json a="a"`, true, "a bare field is its own expression"},
		{`{a="x"} | json`, `{a="x"} | logfmt`, false, "parser kind"},
		{`{a="x"} | logfmt`, `{a="x"} | unpack`, false, "logfmt and unpack"},
		{`{a="x"} | regexp "(?P<q>.)"`, `{a="x"} | pattern "(?P<q>.)"`, false, "regexp and pattern"},
		{`{a="x"} | json | logfmt`, `{a="x"} | logfmt | json`, false, "parser order"},
		{`{a="x"} | drop a`, `{a="x"} | keep a`, false, "drop and keep"},
		{`{a="x"} | drop a, b`, `{a="x"} | drop b, a`, false, "drop order"},
		{`{a="x"} | drop a`, `{a="x"} | drop a=""`, false, "name or condition"},
		{`{a="x"} | drop a="1"`, `{a="x"} | drop a!="1"`, false, "condition operator"},
		{`{a="x"} | drop a="1"`, `{a="x"} | drop a=~"1"`, false, "condition regexp"},
		{`{a="x"} | s="1"`, `{a="x"} | s=1`, false, "string and number"},
		{`{a="x"} | s>5`, `{a="x"} | s>=5`, false, "comparator"},
		{`{a="x"} | s>5`, `{a="x"} | s<5`, false, "direction"},
		{`{a="x"} | s>5`, `{a="x"} | s>5s`, false, "number and duration"},
		{`{a="x"} | s>5s`, `{a="x"} | s>5KB`, false, "duration and bytes"},
		{`{a="x"} | s=ip("1.2.3.4")`, `{a="x"} | s="1.2.3.4"`, false, "ip and string"},
		{`{a="x"} | s=ip("1.2.3.4")`, `{a="x"} | s!=ip("1.2.3.4")`, false, "ip negation"},
		{`{a="x"} | s=~"x"`, `{a="x"} | s!~"x"`, false, "label filter regexp"},
		{`{a="x"} | s>1 or t<2`, `{a="x"} | s>1 and t<2`, false, "or and and"},
		{`{a="x"} | s>1 and (t<2 or u<3)`, `{a="x"} | (s>1 and t<2) or u<3`, false, "grouping of a filter"},
		{`{a="x"} | s="\q"`, `{a="x"} | s="\z"`, false, "two literals logql cannot read keep their text"},
		{`{a="x"} | s="\q"`, `{a="x"} | s=""`, false, "an unreadable literal and an empty one"},
		{`{a="x"} | line_format "{{.a}}"`, `{a="x"} | line_format "{{.b}}"`, false, "template"},
		{`{a="x"} | label_format b=c`, `{a="x"} | label_format b="c"`, false, "rename and template"},
		{`{a="x"} | label_format b=c, d=e`, `{a="x"} | label_format d=e, b=c`, false, "label_format order"},
		{`{a="x"} | decolorize`, `{a="x"}`, false, "decolorize"},
		{`{a="x"} | unwrap n`, `{a="x"} | unwrap bytes(n)`, false, "bare and converted unwrap"},
		{`{a="x"} | unwrap bytes(n)`, `{a="x"} | unwrap duration(n)`, false, "converters"},
		{`{a="x"} | unwrap duration(n)`, `{a="x"} | unwrap duration_seconds(n)`, false, "duration converters"},

		{r5, `rate({app="a"}[10m])`, false, "range"},
		{`rate({app="a"}[5m] offset 0s)`, r5, false, "offset 0s is an offset"},
		{`rate({app="a"}[5m] offset 1h)`, `rate({app="a"}[5m] offset 2h)`, false, "offset value"},
		{`rate({app="a"}[5m] offset 1h)`, `rate({app="a"}[5m] offset 1m)`, false, "offset unit"},
		{r5, `count_over_time({app="a"}[5m])`, false, "range function"},
		{`bytes_rate({app="a"}[5m])`, `bytes_over_time({app="a"}[5m])`, false, "bytes functions"},
		{`first_over_time({app="a"} | unwrap n [5m])`, `last_over_time({app="a"} | unwrap n [5m])`, false, "first and last"},
		{`absent_over_time({app="a"}[5m])`, `count_over_time({app="a"}[5m])`, false, "absent"},
		{`rate_counter({app="a"} | unwrap n [5m])`, `rate({app="a"} | unwrap n [5m])`, false, "rate_counter"},
		{`avg_over_time({app="a"} | unwrap n [5m])`, `sum_over_time({app="a"} | unwrap n [5m])`, false, "avg and sum"},
		{`stddev_over_time({app="a"} | unwrap n [5m])`, `stdvar_over_time({app="a"} | unwrap n [5m])`, false, "stddev and stdvar"},
		{`min_over_time({app="a"} | unwrap n [5m])`, `max_over_time({app="a"} | unwrap n [5m])`, false, "min and max"},
		{`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`, `quantile_over_time(0.95, {app="a"} | unwrap n [5m])`, false, "quantile"},
		{`quantile_over_time(0.5, {app="a"} | unwrap n [5m]) by (x)`, `quantile_over_time(0.5, {app="a"} | unwrap n [5m])`, false, "range grouping"},
		{`sum by (a) (` + r5 + `)`, `sum without (a) (` + r5 + `)`, false, "by and without"},
		{`sum by () (` + r5 + `)`, `sum(` + r5 + `)`, false, "by () and none"},
		{`sum without () (` + r5 + `)`, `sum(` + r5 + `)`, false, "without () and none"},
		{`sum by () (` + r5 + `)`, `sum without () (` + r5 + `)`, false, "by () and without ()"},
		{`sum by (a, b) (` + r5 + `)`, `sum by (b, a) (` + r5 + `)`, false, "grouping order is kept"},
		{`sum(` + r5 + `)`, `avg(` + r5 + `)`, false, "sum and avg"},
		{`min(` + r5 + `)`, `max(` + r5 + `)`, false, "min and max"},
		{`count(` + r5 + `)`, `stddev(` + r5 + `)`, false, "count and stddev"},
		{`stddev(` + r5 + `)`, `stdvar(` + r5 + `)`, false, "stddev and stdvar"},
		{`topk(1, ` + r5 + `)`, `topk(2, ` + r5 + `)`, false, "k"},
		{`topk(1, ` + r5 + `)`, `bottomk(1, ` + r5 + `)`, false, "topk and bottomk"},
		{`topk(1, ` + r5 + `)`, `topk by (a) (1, ` + r5 + `)`, false, "rank grouping"},
		{`sort(` + r5 + `)`, `sort_desc(` + r5 + `)`, false, "sort direction"},
		{`sort(` + r5 + `)`, `sum(` + r5 + `)`, false, "sort and sum"},
		{`sum(sum(` + r5 + `))`, `sum(` + r5 + `)`, false, "nesting"},

		{r5 + ` + 1`, r5 + ` - 1`, false, "arithmetic operator"},
		{r5 + ` > 1`, r5 + ` > bool 1`, false, "bool modifier"},
		{r5 + ` / ` + r5, r5 + ` / on (a) ` + r5, false, "on"},
		{r5 + ` / on (a) ` + r5, r5 + ` / ignoring (a) ` + r5, false, "on and ignoring"},
		{r5 + ` / on () ` + r5, r5 + ` / ` + r5, false, "on () and none"},
		{r5 + ` / on (a) ` + r5, r5 + ` / on (b) ` + r5, false, "on labels"},
		{r5 + ` / on (a) group_left ` + r5, r5 + ` / on (a) group_right ` + r5, false, "group side"},
		{r5 + ` / on (a) group_left ` + r5, r5 + ` / on (a) group_left (b) ` + r5, false, "include labels"},
		{r5 + ` / on (a) group_left (b) ` + r5, r5 + ` / on (a) group_left (c) ` + r5, false, "include label"},
		{r5 + ` and ` + r5, r5 + ` or ` + r5, false, "set operators"},
		{r5 + ` or ` + r5, r5 + ` unless ` + r5, false, "or and unless"},
		{r5 + ` + 1`, `1 + ` + r5, false, "scalar side"},
		{`1 + 2`, `2 + 1`, false, "scalar order"},
		{`1 + 2`, `1 + 3`, false, "scalar value"},
		{`2 ^ 3`, `2 * 3`, false, "power and product"},
		{`7 % 3`, `7 / 3`, false, "modulo and division"},
		{`1 == bool 1`, `1 != bool 1`, false, "equality"},
		{`1 + 2 + 3`, `1 + (2 + 3)`, false, "association"},
		{`1 + 2 * 3`, `(1 + 2) * 3`, false, "precedence"},

		{`vector(1)`, `vector(2)`, false, "vector argument"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, `label_replace(` + r5 + `, "a", "b", "c", "e")`, false, "label_replace regexp"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, `label_replace(` + r5 + `, "x", "b", "c", "d")`, false, "label_replace destination"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, `label_replace( ` + r5 + ` , "a" , "b", "c", "d" )`, true, "label_replace is typed: spacing is not part of it"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, "label_replace(" + r5 + ", `a`, \"b\", \"c\", \"d\")", true, "label_replace strings: a backquoted and a quoted string with one value"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, `label_replace(` + r5 + `, "a", "b", "d", "c")`, false, "label_replace source and regexp swapped"},
		{`label_replace(` + r5 + `, "a", "b", "c", "d")`, `label_replace(sum(` + r5 + `), "a", "b", "c", "d")`, false, "label_replace input"},
		{`vector(1)`, `vector( 1 )`, true, "vector is typed: spacing is not part of it"},
		{`vector(1)`, `vector(1.0)`, true, "vector value: the same number"},
		{`approx_topk(3, sum by (a) (` + r5 + `))`, `approx_topk(3,sum by (a) (` + r5 + `))`, false, "an opaque call keeps its text, spacing included"},
	}
	equalKeys, distinctKeys := 0, 0
	for _, c := range pairs {
		a := keyOfText(t, c.a, Inputs{Kind: KindRange})
		b := keyOfText(t, c.b, Inputs{Kind: KindRange})
		if got := bytes.Equal(a.Key, b.Key); got != c.equal {
			t.Errorf("%s: keys equal = %v, want %v\n  %q\n  %q", c.why, got, c.equal, c.a, c.b)
		}
		if c.equal {
			equalKeys++
		} else {
			distinctKeys++
		}
	}
	t.Logf("%d pairs: %d with equal keys, %d with different keys", len(pairs), equalKeys, distinctKeys)
}

// A zero range or offset keeps its text, so spellings the parser reads as zero are not merged by the key.
func TestKeyZeroDurationsKeepText(t *testing.T) {
	for _, c := range []pair{
		{`rate({a="b"}[0s])`, `rate({a="b"}[0m])`, false, "zero ranges"},
		{`rate({a="b"}[5m] offset 0s)`, `rate({a="b"}[5m] offset 0m)`, false, "zero offsets"},
		{`rate({a="b"}[5m] offset 0s)`, `rate({a="b"}[5m] offset 0s)`, true, "same text"},
	} {
		a := keyOfText(t, c.a, Inputs{})
		b := keyOfText(t, c.b, Inputs{})
		if bytes.Equal(a.Key, b.Key) != c.equal {
			t.Errorf("%s: %q and %q", c.why, c.a, c.b)
		}
	}
}

// Inputs are length-prefixed lists of lengths-prefixed names: crafted names cannot shift a boundary.
func TestKeyInputsEdgePairs(t *testing.T) {
	q := `{a="x"}`
	base := Inputs{Kind: KindRange, StreamLabels: []string{"a", "b"}}
	for _, in := range []Inputs{
		{Kind: KindRange, StreamLabels: []string{"ab"}},
		{Kind: KindRange, StreamLabels: []string{"a\x00b"}},
		{Kind: KindRange, StreamLabels: []string{"a", "b", ""}},
		{Kind: KindRange, StreamLabels: []string{"a"}},
		{Kind: KindRange, StreamLabels: []string{"b", "a"}},
		{Kind: KindRange, StreamLabels: nil},
		{Kind: KindRange, StreamLabels: []string{"a", "b"}, Exposure: Exposure{LokiProfile: true}},
		{Kind: KindRange, StreamLabels: []string{"a", "b"}, Exposure: Exposure{DottedNames: true}},
		{Kind: KindInstant, StreamLabels: []string{"a", "b"}},
		{Kind: KindLogs, StreamLabels: []string{"a", "b"}},
	} {
		if bytes.Equal(keyOfText(t, q, base).Key, keyOfText(t, q, in).Key) {
			t.Errorf("inputs %+v and %+v share a key", base, in)
		}
	}
	if !bytes.Equal(keyOfText(t, q, Inputs{StreamLabels: []string{}}).Key, keyOfText(t, q, Inputs{StreamLabels: nil}).Key) {
		t.Errorf("an empty and a nil label list differ")
	}
	// A nil and an empty list are one input; the same query under each of two sets never collides.
	seen := map[string]Inputs{}
	for kind := KindLogs; kind <= KindInstant; kind++ {
		for _, labels := range [][]string{nil, {""}, {"a"}, {"a", "b"}, {"ab"}} {
			for _, ex := range []Exposure{{}, {LokiProfile: true}, {DottedNames: true}, {true, true}} {
				in := Inputs{Kind: kind, StreamLabels: labels, Exposure: ex}
				k := string(keyOfText(t, q, in).Key)
				if prev, ok := seen[k]; ok {
					t.Errorf("inputs %+v and %+v share a key", prev, in)
				}
				seen[k] = in
			}
		}
	}
}

// Crafted values: a value that holds the bytes of a tag, a length prefix, a boolean or the encoding of another
// plan's key changes nothing about where the fields end, so it never makes two different queries share a key.
func TestKeyCraftedValues(t *testing.T) {
	_, exprs := corpus(t)
	// The encoding of a few plans, to hide inside values.
	var payloads []string
	for i := 0; i < len(exprs); i += len(exprs) / 40 {
		p, _ := Build(exprs[i], Inputs{})
		payloads = append(payloads, string(p.Key), string(p.Key[:len(p.Key)/2]))
	}
	payloads = append(payloads,
		"\x01", "\x00\x01", "\x05\x00", "\x01\x01\x01\x01", "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", "\xff\xff\xff\xff",
		string([]byte{tagLogSelect, 0, 0}), string([]byte{tagInputs, 0, 0, 0, 0, 0}), strings.Repeat("\x7f", 300),
	)
	// Raw strings take any byte but a backquote.
	quote := func(s string) (string, bool) {
		if strings.ContainsRune(s, '`') {
			return "", false
		}
		return "`" + s + "`", true
	}
	templates := []func(v string) string{
		func(v string) string { return `{a=` + v + `}` },
		func(v string) string { return `{a="x"} |= ` + v },
		func(v string) string { return `{a="x"} | json f=` + v },
		func(v string) string { return `{a="x"} | line_format ` + v },
		func(v string) string { return `{a="x"} | label_format b=` + v },
		func(v string) string { return `{a="x"} | regexp ` + v },
		func(v string) string { return `{a="x"} | s=` + v },
		func(v string) string { return `{a="x"} | drop s=` + v },
		func(v string) string { return `sum(count_over_time({a="x"} |= ` + v + ` [5m]))` },
	}
	seen := map[string]string{}
	parsed := 0
	for ti, tpl := range templates {
		for _, payload := range payloads {
			v, ok := quote(payload)
			if !ok {
				continue
			}
			q := tpl(v)
			e, err := parseCanon(q)
			if err != nil {
				continue // a template or a byte the parser refuses
			}
			parsed++
			p, err := Build(e, Inputs{})
			if err != nil {
				t.Fatal(err)
			}
			checkKey(t, p, e, q)
			if prev, dup := seen[string(p.Key)]; dup {
				t.Errorf("template %d: %q and %q share a key", ti, prev, q)
			}
			seen[string(p.Key)] = q
		}
	}
	if parsed < 100 {
		t.Fatalf("only %d crafted queries parsed", parsed)
	}
	t.Logf("%d crafted queries, %d keys, no collision", parsed, len(seen))
}

// No two corpus queries with different fields share a key, and queries with the same fields share one.
func TestKeyInjectiveOnCorpus(t *testing.T) {
	texts, exprs := corpus(t)
	byKey := map[string]string{}
	shared := 0
	for i, e := range exprs {
		p, err := Build(e, Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		toks, _ := describePlan(p, e)
		desc := strings.Join(toks, "\x00")
		if prev, ok := byKey[string(p.Key)]; ok {
			shared++
			if prev != desc {
				t.Fatalf("%q: a key shared with a plan of different fields", texts[i])
			}
		}
		byKey[string(p.Key)] = desc
	}
	t.Logf("%d queries, %d distinct keys, %d repeats with the same fields", len(exprs), len(byKey), shared)
}

// ---- sizes and shapes

func TestBuildLargeAndOddShapes(t *testing.T) {
	long := strings.Repeat("v", 1<<20)
	manyMatchers := make([]string, 1000)
	for i := range manyMatchers {
		manyMatchers[i] = `l` + strings.Repeat("a", i%7) + string(rune('a'+i%26)) + `="` + strings.Repeat("x", i%5) + `"`
	}
	// The race detector makes a build ten times slower: the large cases shrink with it.
	big := func(n int) int {
		if raceBuild() {
			return n / 10
		}
		return n
	}
	nested := func(n int) string { return strings.Repeat("sum(", n) + r5 + strings.Repeat(")", n) }
	cases := []struct {
		name, q  string
		minNodes int
	}{
		{"empty-string value", `{a=""}`, 1},
		{"unicode names and values", `{é="日本語", ключ=~"значение.*"} |= "🙂" | json 名前="値"`, 1},
		{"one megabyte value", `{a="` + long + `"}`, 1},
		{"one megabyte line filter", `{a="x"} |= "` + long + `"`, 1},
		{"1000 matchers", `{` + strings.Join(manyMatchers, ",") + `}`, 1},
		{"1000 stages", `{a="x"}` + strings.Repeat(` | json | s>1 |= "e" | drop q`, 250), 1},
		{"50 nested aggregations", nested(50), 51},
		{"1000 nested aggregations", nested(1000), 1001},
		{"100000 nested aggregations", nested(big(100000)), big(100000) + 1},
		{"100000 nested parentheses", strings.Repeat("(", big(100000)) + r5 + strings.Repeat(")", big(100000)), 2},
		{"20000 binary operands", strings.Repeat(r5+" + ", big(20000)) + "1", 2*big(20000) + 1},
		{"20000 right nested operands", strings.Repeat("1 + (", big(20000)) + "1" + strings.Repeat(")", big(20000)), 2*big(20000) + 1},
		{"100000 matchers", `{` + strings.Repeat(`a="b",`, big(100000)) + `a="b"}`, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, err := logql.Parse(c.q)
			if err != nil {
				t.Skipf("the parser refuses it: %v", err)
			}
			p, err := Build(e, Inputs{Kind: KindRange})
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Key) < len(c.q)/16 && len(c.q) > 1000 && !strings.Contains(c.name, "parentheses") {
				t.Errorf("key of %d bytes for a query of %d", len(p.Key), len(c.q))
			}
			// Linear: the key is at most the query's own bytes plus a small constant per node.
			if limit := 4*len(c.q) + 64*c.minNodes + 1024; len(p.Key) > limit {
				t.Errorf("key of %d bytes, limit %d", len(p.Key), limit)
			}
			if n := countNodes(p.Root); n < c.minNodes {
				t.Errorf("%d nodes, want at least %d", n, c.minNodes)
			}
			if c.minNodes < 1000 {
				checkKey(t, p, e, c.name)
			} else if _, spans, err := decodeKey(p.Key); err != nil || len(spans) != countNodes(p.Root) {
				t.Errorf("the key does not decode to %d nodes: %v", countNodes(p.Root), err)
			}
			if got := p.AppendKey(make([]byte, 0, len(p.Key))); !bytes.Equal(got, p.Key) {
				t.Errorf("AppendKey differs from Key")
			}
		})
	}
}

func countNodes(n Node) int {
	switch x := n.(type) {
	case *RangeAgg:
		return 1 + countNodes(x.In)
	case *VectorAgg:
		return 1 + countNodes(x.In)
	case *Rank:
		return 1 + countNodes(x.In)
	case *BinOp:
		return 1 + countNodes(x.L) + countNodes(x.R)
	}
	return 1
}

// A tree deeper than maxDepth is refused instead of recursed into: a Go stack that overflows ends the process. The
// trees here are built by hand, deeper than the parser allows a request to be.
func TestBuildRefusesTooDeepTrees(t *testing.T) {
	chain := func(n int, wrap func(logql.Expr) logql.Expr) logql.Expr {
		var e logql.Expr = &logql.LiteralExpr{Value: 1}
		for i := 0; i < n; i++ {
			e = wrap(e)
		}
		return e
	}
	sum := func(e logql.Expr) logql.Expr { return &logql.VectorAggregation{Op: logql.VectorSum, Inner: e} }
	topk := func(e logql.Expr) logql.Expr {
		return &logql.VectorAggregation{Op: logql.VectorTopK, Param: 1, HasParam: true, Inner: e}
	}
	binLeft := func(e logql.Expr) logql.Expr {
		return &logql.BinOpExpr{Op: "+", Left: e, Right: &logql.LiteralExpr{Value: 1}}
	}
	binRight := func(e logql.Expr) logql.Expr {
		return &logql.BinOpExpr{Op: "+", Left: &logql.LiteralExpr{Value: 1}, Right: e}
	}
	for name, wrap := range map[string]func(logql.Expr) logql.Expr{"aggregations": sum, "ranks": topk, "left operands": binLeft, "right operands": binRight} {
		p, err := Build(chain(maxDepth-1, wrap), Inputs{})
		if err != nil || p == nil {
			t.Errorf("%s: a tree of the deepest allowed depth is refused: %v", name, err)
		}
		var u *UnsupportedError
		if p, err := Build(chain(maxDepth, wrap), Inputs{}); p != nil || !errors.As(err, &u) {
			t.Errorf("%s: a tree one level too deep builds (%v)", name, err)
		}
	}

	leaf := &logql.LabelFilterExpr{Op: logql.LabelFilterLeaf, Name: "s", Cmp: "=", Value: "1", Kind: logql.LabelFilterValueNumber}
	filter := func(n int) logql.Expr {
		f := leaf
		for i := 0; i < n; i++ {
			f = &logql.LabelFilterExpr{Op: logql.LabelFilterAnd, Left: f, Right: leaf}
		}
		return &logql.LogQuery{Selector: &logql.StreamSelector{}, Pipeline: []logql.Stage{&logql.LabelFilterStage{Expr: f}}}
	}
	if _, err := Build(filter(maxDepth-1), Inputs{}); err != nil {
		t.Errorf("a filter of the deepest allowed depth is refused: %v", err)
	}
	var u *UnsupportedError
	if p, err := Build(filter(maxDepth+1), Inputs{}); p != nil || !errors.As(err, &u) {
		t.Errorf("a filter one level too deep builds (%v)", err)
	}
	// A refused build leaves nothing behind: the next one is as before.
	if _, err := Build(chain(3, sum), Inputs{}); err != nil {
		t.Errorf("a build after a refused one: %v", err)
	}
}
