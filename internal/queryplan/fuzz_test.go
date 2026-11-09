package queryplan

import (
	"bytes"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// seedQueries are the corpus texts and a table of edge shapes, for every fuzz target.
func seedQueries(f *testing.F) {
	texts, _ := corpus(f)
	for _, q := range texts {
		f.Add(q)
	}
	for _, q := range []string{
		`{app="nginx"}`, `{}`, `{a=""}`, `*`, ``, `{`, `not logql`,
		`{app="nginx"} |= "a" or "b" != ip("1.2.3.4") | json a="b" | logfmt | status>=500 or (a="1" and b=~"x")`,
		`{app="nginx"} | drop a, b="c" | keep d | line_format "{{.m}}" | label_format x=y | decolorize | regexp "(?P<a>.)" | pattern "<a>"`,
		`sum by (app) (rate({app="api"} | json | unwrap duration(d) [5m] offset 1h))`,
		`sum without () (count_over_time({app="api"}[1m]))`, `sum by () (count_over_time({app="api"}[1m]))`,
		`quantile_over_time(0.99, {a="b"} | unwrap n [5m]) by (x)`,
		`topk(5, bytes_rate({a="b"}[1m])) / on (a) group_left (b) sort(rate({a="b"}[1m]))`,
		`label_replace(rate({a="b"}[1m]), "x", "y", "z", "w") + vector(1) + 3`,
		`absent_over_time({a="b"}[5m]) and rate_counter({a="b"} | unwrap n [1m])`,
		`rate({a="b"}[0s] offset 0s)`, `rate({a="b"}[5m] offset -1s)`, `rate({a="b"}[$__auto])`,
		"{a=`\x00\x01\x02`} |= `\xff\xfe`", "{a=`\x01\x01\x01`} | json f=`\x80\x80`",
		`{é="日本語"} |= "🙂"`, `approx_topk(3, rate({a="b"}[1m]))`,
		strings.Repeat("(", 200) + `rate({a="b"}[1m])` + strings.Repeat(")", 200),
		strings.Repeat("sum(", 100) + `rate({a="b"}[1m])` + strings.Repeat(")", 100),
	} {
		f.Add(q)
	}
}

// FuzzBuild: for any query the parser accepts, Build does not panic, builds the same key twice, builds the same
// key from a copy of the tree, keeps the Expr as it was, and gives every node a key that is a part of the plan's.
// The key of a query equals the key of its printed form whenever the printed form parses to the same tree.
func FuzzBuild(f *testing.F) {
	seedQueries(f)
	f.Fuzz(func(t *testing.T, q string) {
		expr, err := parseCanon(q)
		if err != nil {
			return
		}
		copyOf, err := parseCanon(q)
		if err != nil {
			t.Fatalf("a query parses once and not twice: %q", q)
		}
		in := Inputs{Kind: KindRange, StreamLabels: []string{"app"}}
		a, err := Build(expr, in)
		if err != nil {
			t.Fatalf("Build(%q): %v", q, err)
		}
		if !reflect.DeepEqual(expr, copyOf) {
			t.Fatalf("Build changed the Expr of %q", q)
		}
		b, err := Build(expr, in)
		c, err2 := Build(copyOf, in)
		if err != nil || err2 != nil || !bytes.Equal(a.Key, b.Key) || !bytes.Equal(a.Key, c.Key) {
			t.Fatalf("Build(%q) is not deterministic: %v %v", q, err, err2)
		}
		checkKey(t, a, expr, q)
		if got := a.AppendKey(nil); !bytes.Equal(got, a.Key) {
			t.Fatalf("AppendKey differs from Key for %q", q)
		}
		if printed, err := logql.Parse(expr.String()); err == nil && reflect.DeepEqual(printed, expr) {
			p, err := Build(printed, in)
			if err != nil || !bytes.Equal(p.Key, a.Key) {
				t.Fatalf("%q: the printed form %q has another key", q, expr.String())
			}
		}
	})
}

// FuzzKeyDecode: the key of any query the parser accepts decodes, to the last byte, into the fields of its plan, so
// the encoding is unambiguous for any value a query can hold; and no cut of it, and no byte after it, decodes.
func FuzzKeyDecode(f *testing.F) {
	seedQueries(f)
	f.Fuzz(func(t *testing.T, q string) {
		expr, err := parseCanon(q)
		if err != nil {
			return
		}
		p, err := Build(expr, Inputs{Kind: KindLogs, StreamLabels: []string{q, "b"}})
		if err != nil {
			t.Fatal(err)
		}
		checkKey(t, p, expr, q)
		if len(p.Key) <= 600 {
			for n := 0; n < len(p.Key); n++ {
				if _, _, err := decodeKey(p.Key[:n]); err == nil {
					t.Fatalf("%q: a %d byte prefix of its %d byte key decodes", q, n, len(p.Key))
				}
			}
		}
		if _, _, err := decodeKey(append(append([]byte(nil), p.Key...), 0)); err == nil {
			t.Fatalf("%q: a key with a trailing byte decodes", q)
		}
	})
}

var (
	mutBy       = regexp.MustCompile(`\bby\b`)
	mutWithout  = regexp.MustCompile(`\bwithout\b`)
	mutName     = regexp.MustCompile(`\{\s*([A-Za-z_][A-Za-z0-9_]*)`)
	mutOffset   = regexp.MustCompile(`offset\s+(\d+)`)
	mutDigit    = regexp.MustCompile(`[0-9]`)
	mutRange    = regexp.MustCompile(`\[(\d+)m\]`)
	mutMatchers = regexp.MustCompile(`\{([^,{}"` + "`" + `]*"[^"]*")\s*,\s*([^,{}"` + "`" + `]*"[^"]*")`)
)

// semanticMutations change what a query means, or the order the key keeps: each changes a field the key encodes.
var semanticMutations = []struct {
	name string
	do   func(q string) string
}{
	{"by to without", func(q string) string { return mutBy.ReplaceAllString(q, "without") }},
	{"without to by", func(q string) string { return mutWithout.ReplaceAllString(q, "by") }},
	{"a label name", func(q string) string { return mutName.ReplaceAllString(q, "{${1}x") }},
	{"an offset", func(q string) string {
		return mutOffset.ReplaceAllStringFunc(q, func(m string) string {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(m, "offset")))
			return "offset " + strconv.Itoa(n+1)
		})
	}},
	{"a line filter operator", func(q string) string { return strings.Replace(q, "|=", "|~", 1) }},
	{"a line filter negation", func(q string) string { return strings.Replace(q, "|=", "!=", 1) }},
	{"the first digit", func(q string) string {
		loc := mutDigit.FindStringIndex(q)
		if loc == nil {
			return q
		}
		return q[:loc[0]] + string('0'+(q[loc[0]]-'0'+1)%10) + q[loc[1]:]
	}},
	{"a range", func(q string) string { return mutRange.ReplaceAllString(q, "[7${1}m]") }},
	{"the order of two matchers", func(q string) string { return mutMatchers.ReplaceAllString(q, "{$2, $1") }},
	// On a new line: a query may end in a comment, which runs to the end of its line.
	{"a stage added", func(q string) string { return q + "\n| decolorize" }},
}

// nonSemanticMutations change the text only: the tree, and so the key, stay.
var nonSemanticMutations = []struct {
	name string
	do   func(q string) string
}{
	{"surrounding whitespace", func(q string) string { return " \t" + q + "\n " }},
	{"parentheses", func(q string) string { return "(" + q + "\n)" }},
	{"a range in seconds", func(q string) string {
		return mutRange.ReplaceAllStringFunc(q, func(m string) string {
			n, _ := strconv.Atoi(m[1 : len(m)-2])
			return "[" + strconv.Itoa(n*60) + "s]"
		})
	}},
	{"a range in minutes", func(q string) string { return strings.Replace(q, "[1h]", "[60m]", 1) }},
	{"spaces after commas", func(q string) string { return strings.Replace(q, ",", " , ", 1) }},
}

// checkMutations applies each mutation to q: a semantic one that still parses to another tree must change the key,
// and a spelling one that still parses must keep it. applied counts the mutations that took effect, by name.
func checkMutations(t testing.TB, q string, applied map[string]int) {
	base, err := parseCanon(q)
	if err != nil {
		return
	}
	a, err := Build(base, Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range semanticMutations {
		mq := m.do(q)
		if mq == q {
			continue
		}
		mut, err := parseCanon(mq)
		if err != nil || reflect.DeepEqual(base, mut) {
			continue
		}
		b, err := Build(mut, Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		applied[m.name]++
		if bytes.Equal(a.Key, b.Key) {
			t.Fatalf("%s: %q and %q read as different trees and have one key", m.name, q, mq)
		}
	}
	for _, m := range nonSemanticMutations {
		mq := m.do(q)
		mut, err := parseCanon(mq)
		if err != nil {
			continue
		}
		b, err := Build(mut, Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		if mq != q {
			applied[m.name]++
		}
		if !bytes.Equal(a.Key, b.Key) {
			// A spelling change that landed inside a literal changed a value: the fields then differ.
			ta, _ := describePlan(a, base)
			tb, _ := describePlan(b, mut)
			if reflect.DeepEqual(ta, tb) {
				t.Fatalf("%s: %q and %q have the same fields and different keys", m.name, q, mq)
			}
			applied[m.name+" (inside a literal)"]++
		}
	}
}

// FuzzKeyDistinctMutations: see checkMutations. The corpus is the seed.
func FuzzKeyDistinctMutations(f *testing.F) {
	seedQueries(f)
	f.Fuzz(func(t *testing.T, q string) { checkMutations(t, q, map[string]int{}) })
}

// Every mutation takes effect on enough of the corpus for the fuzz target to mean something.
func TestKeyMutationsOnCorpus(t *testing.T) {
	texts, _ := corpus(t)
	applied := map[string]int{}
	for _, q := range texts {
		checkMutations(t, q, applied)
	}
	for _, m := range semanticMutations {
		if applied[m.name] < 5 {
			t.Errorf("semantic mutation %q took effect on %d corpus queries (want 5)", m.name, applied[m.name])
		}
	}
	for _, m := range nonSemanticMutations {
		if applied[m.name] < 5 {
			t.Errorf("spelling mutation %q took effect on %d corpus queries (want 5)", m.name, applied[m.name])
		}
	}
	t.Logf("mutations applied: %v", applied)
}

// Resolve is total on every plan Build makes, deterministic, idempotent, and leaves the key as it was.
func FuzzResolve(f *testing.F) {
	seedQueries(f)
	f.Fuzz(func(t *testing.T, q string) {
		expr, err := parseCanon(q)
		if err != nil {
			return
		}
		a, err := Build(expr, Inputs{})
		if err != nil {
			return
		}
		b, _ := Build(expr, Inputs{})
		key := string(a.Key)
		Resolve(a)
		Resolve(b)
		if !reflect.DeepEqual(a.Sem, b.Sem) {
			t.Fatalf("%q: two resolves differ: %+v %+v", q, a.Sem, b.Sem)
		}
		again := a.Sem
		Resolve(a)
		if !reflect.DeepEqual(again, a.Sem) || string(a.Key) != key {
			t.Fatalf("%q: Resolve is not idempotent or changed the key", q)
		}
		if s := firstSelect(a.Root); s != nil {
			if s.Sem == nil {
				t.Fatalf("%q: a pipeline is not resolved", q)
			}
			if (s.Sem.Parsers == 0) != (s.Sem.ErrorPolicy == ErrorNoParser) {
				t.Fatalf("%q: %d parsers and error policy %d", q, s.Sem.Parsers, s.Sem.ErrorPolicy)
			}
			if (s.Sem.ErrorPolicy == ErrorDroppedLabel) != (s.Sem.ErrorAt >= 0) {
				t.Fatalf("%q: error policy %d at %d", q, s.Sem.ErrorPolicy, s.Sem.ErrorAt)
			}
		}
	})
}
