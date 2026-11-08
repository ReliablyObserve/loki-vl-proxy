//go:build !corpuscapture

package querycorpus_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The tests of translator.SplitPost and the codec that hold them to the proxy's two-step strip as it ran before the
// codec (legacySplit), on the corpus, on hand-written edges and under fuzzing.

// lrMarker is the marker a translated query carries for a label_replace call, written here from the format: the
// base64 of the four arguments joined by NUL, between `__lvp_lr:` and `__`.
func lrMarker(dst, repl, src, regex string) string {
	return "__lvp_lr:" + base64.StdEncoding.EncodeToString([]byte(dst+"\x00"+repl+"\x00"+src+"\x00"+regex)) + "__"
}

// assertSplitPost holds SplitPost to the old strip, field by field, and to the codec's own reading: the clean text,
// the without fold (operator and labels) and the label_replace specs equal what the old helpers return, equal what
// physical.Split(DecodePhysical(s)) gives, and the decoded operators write s back.
func assertSplitPost(t testing.TB, s string) {
	t.Helper()
	wantClean, wantOp, wantLabels, wantSpecs := referenceSplit(s)
	clean, post := translator.SplitPost(s)
	if clean != wantClean {
		t.Fatalf("SplitPost(%q) clean = %q, reference %q", s, clean, wantClean)
	}
	if wantFolds := wantOp != "" || len(wantLabels) > 0; post.Folds() != wantFolds {
		t.Fatalf("SplitPost(%q) folds = %v, legacy op %q labels %#v", s, post.Folds(), wantOp, wantLabels)
	}
	if post.Folds() {
		if post.Without.Op != wantOp || !reflect.DeepEqual(post.Without.Labels, wantLabels) {
			t.Fatalf("SplitPost(%q) without = %q %#v, legacy %q %#v", s, post.Without.Op, post.Without.Labels, wantOp, wantLabels)
		}
	} else if !reflect.DeepEqual(post.Without, physical.FoldLevel{}) {
		t.Fatalf("SplitPost(%q) has no fold but %#v", s, post.Without)
	}
	if !reflect.DeepEqual(post.LabelReplace, wantSpecs) {
		t.Fatalf("SplitPost(%q) label_replace = %#v, legacy %#v", s, post.LabelReplace, wantSpecs)
	}

	op, err := translator.DecodePhysical(s)
	if err != nil {
		t.Fatalf("DecodePhysical(%q): %v", s, err)
	}
	core, decoded := physical.Split(op)
	if got := translator.EncodePhysical(core); got != clean {
		t.Fatalf("DecodePhysical(%q) clean = %q, SplitPost %q", s, got, clean)
	}
	if !reflect.DeepEqual(decoded, post) {
		t.Fatalf("DecodePhysical(%q) post = %#v, SplitPost %#v", s, decoded, post)
	}
}

// splitEdgeInputs are texts at the boundaries of the marker grammar and of the helpers that read it.
func splitEdgeInputs() []string {
	const q = `app:=x | stats by (pod, node) count() as c`
	long := strings.Repeat("a", 1<<20)
	var thousand []string
	for i := 0; i < 1000; i++ {
		thousand = append(thousand, "l"+strconv.Itoa(i))
	}
	var hundred string
	for i := 0; i < 100; i++ {
		hundred += lrMarker("d"+strconv.Itoa(i), "$1", "app", "(.*)")
	}
	inputs := []string{
		// no marker, or nothing at all
		"", " ", "\n", "\x00", q, "*", `error "connection reset"`, "_", "__", "___", "____",
		// a marker and nothing else
		"__without__:", "__without__:a", "__without__:sum:a", "__lvp_lr:__", lrMarker("", "", "", ""),
		// the markers at the start, in the middle and at the end
		"__without__:a" + q, q + "__without__:a", "x__without__:a y", lrMarker("a", "b", "c", "d") + q, q + lrMarker("a", "b", "c", "d"),
		"x" + lrMarker("a", "b", "c", "d") + "y",
		// without: empty, padded, duplicate and odd label lists
		q + "__without__:,", q + "__without__:,,", q + "__without__: ", q + "__without__:a,,b", q + "__without__:  a , b  ",
		q + "__without__:a,a,a", q + "__without__:sum:", q + "__without__::a", q + "__without__:sum:a:b", q + "__without__:sum:,",
		q + "__without__:a\n", q + "__without__:a\r\n,b\r\n", q + "__without__:a\x00,b", q + "__without__:é,日本, x", q + "__without__:a b,c d",
		q + "__without__:" + strings.Repeat("x", 100000), q + "__without__:" + strings.Join(thousand, ","),
		q + "__without__:sum:" + strings.Join(thousand, ","),
		// more than one without marker: the last one is read, the earlier ones stay in the query
		"a__without__:x b__without__:y", "__without__:a__without__:b", "__without____without__:a", "___without__:a", "__without__:__without__:",
		"__without__:a__without__:b__lvp_lr:__",
		// a label_replace marker after, before and between without markers
		q + "__without__:pod" + lrMarker("a", "$1", "app", "(.*)"), q + lrMarker("a", "$1", "app", "(.*)") + "__without__:pod",
		q + "__without__:pod" + lrMarker("a", "b", "c", "d") + lrMarker("e", "f", "g", "h") + lrMarker("i", "j", "k", "l"),
		q + lrMarker("a", "b", "c", "d") + "__without__:sum:pod" + lrMarker("e", "f", "g", "h"),
		q + "__without__:pod" + "__lvp_lr:", q + "__without__:a__lvp_lr:YQ==__b,c",
		// label_replace with empty and special arguments, and every base64 padding length
		lrMarker("", "$1", "app", "(.*)"), lrMarker("a", "", "app", "(.*)"), lrMarker("a", "$1", "", "(.*)"), lrMarker("a", "$1", "app", ""),
		lrMarker("a", "${1}", "app", "(?P<x>.*)"), lrMarker("a", "$$", "app", ".*"), lrMarker("a", "$name", "app", "(.*)"),
		lrMarker("a", "b", "c", "("), lrMarker("a", "b", "c", "[a-"), lrMarker("a", "b", "c", "\x00"), lrMarker("a", "b", "c", "d\x00e\x00f"),
		lrMarker("x", "", "", ""), lrMarker("xy", "", "", ""), lrMarker("xyz", "", "", ""), lrMarker("é", "日本", "ü", "(.*)"),
		lrMarker(strings.Repeat("d", 50000), "r", "s", "(.*)"), lrMarker("a", "b", "c", strings.Repeat("(a|b)", 5000)),
		// invalid, truncated and near-miss label_replace markers
		"__lvp_lr:", "__lvp_lr:__", "__lvp_lr:!!!!__", "__lvp_lr:YQ__", "__lvp_lr:YQ=__", "__lvp_lr:YQ==", "__lvp_lr:YQ==__x", "__lvp_lr",
		"__lvp_lr:" + base64.StdEncoding.EncodeToString([]byte("a\x00b")) + "__", "__lvp_lr:" + base64.StdEncoding.EncodeToString([]byte("a\x00b\x00c")) + "__",
		"__lvp_lr:" + base64.StdEncoding.EncodeToString([]byte("a\x00b\x00c\x00d\x00e")) + "__", "__lvp_lr::__", "__lvp_lr:__lvp_lr:__", "__lvp_lr:__lvp_lr:YQ==__",
		"__lvp_l:YQ==__", "__LVP_LR:YQ==__", "_lvp_lr:YQ==__", "q__lvp_lr:%%%__" + lrMarker("a", "b", "c", "d"), lrMarker("a", "b", "c", "d") + "__lvp_lr:%%%__",
		// near-miss without markers
		"__without_:a", "__without__", "_without__:a", "__WITHOUT__:a", "__without:a", "__without_ _:a", "without__:a", "__withou__:a",
		// markers inside quoted LogsQL strings and mid-query, which the old helpers read like any other text
		`_msg:"x __without__:a" | stats count()`, `_msg:"__lvp_lr:YQ==__" | stats count()`, `_msg:~"a__without__:b,c" | filter x:y`,
		`{app="x"} | stats by (a) count() as "__without__:p"`, "a | format \"__without__:a\" as b | stats count()",
		// binary text with markers on one side or after
		"__binary__:/:A__without__:p|||B", "__binary__:/:A|||B__without__:p", "__binary__:/:A|||B@@@on:a__without__:p" + lrMarker("a", "b", "c", "d"),
		"__binary__:/:A" + lrMarker("a", "b", "c", "d") + "|||B", "__binary__:/:A|||B" + lrMarker("a", "b", "c", "d"),
		"__binary__:/:A__without__:p" + lrMarker("a", "b", "c", "d") + "|||B", "__binary__:*:__binary__:+:A|||B|||C", "__binary__:*:A|||__binary__:+:B|||C",
		"__binary__:", "__binary__::", "__binary__:/:", "__binary__:/:|||", "__binary__:/:A|||", "__binary__:/:|||B", "__binary__:/:A|||B@@@", "__binary__:/:A|||B@@@on",
		"__binary__:/:A|||B@@@on:a@@@on:b@@@group_left", "__binary__:/:A|||B@@@x:y@@@ignoring:p, q@@@group_right:", "__binary__:/:A@@@on:a|||B",
		"__binary__:/:A|||B|||C", "__binary__:/:A|||B@@@on:a@@@@@@", "x__binary__:/:A|||B",
		// scalars
		"1", "1e3", "-0", "+0", "NaN", "nan", "+Inf", "-inf", "Infinity", "0x1p3", "0X1.8P-2", "1_0", "0b1", ".5", "5.", "1e", " 1 ", "1\n", "\t1", "1 2",
		"__binary__:*:A|||1e3", "__binary__:*:-0|||B", "__binary__:*:NaN|||+Inf", "__binary__:*: 5 |||B", "__binary__:*:0x10|||0x1p3",
		// separators and whitespace
		"a|||b", "a@@@b", "a:b:c", ":", "::", ",", ",,", "\r\n", "\r", "a\r\n__without__:b\r\n", " __without__:a ",
		// non UTF-8 and large
		"\xff\xfe__without__:\xc3\x28", "\xff__lvp_lr:\xff\xfe__", "__without__:\xff,\xfe", "\xc0\xaf", "__binary__:/:\xffA|||\xfeB",
		long, long + "__without__:a", "__without__:a" + long, long + lrMarker("a", "b", "c", "d"), hundred, q + "__without__:pod" + hundred,
		strings.Repeat("__without__:a", 200), strings.Repeat(lrMarker("a", "b", "c", "d")+"__without__:a", 50),
	}
	return inputs
}

// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestSplitPostEdgeTable(t *testing.T) {
	for i, s := range splitEdgeInputs() {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assertSplitPost(t, s)
			checkPhysicalCodec(t, s)
		})
	}
}

// The shapes SplitPost must read to the value, not just to the old helpers' value.
func TestSplitPostValues(t *testing.T) {
	const q = `app:=x | stats count() as c`
	for _, tc := range []struct {
		name, in, clean string
		op              string
		labels          []string
		specs           []physical.LabelReplace
	}{
		{name: "none", in: q, clean: q},
		{name: "without", in: q + "__without__:pod,node", clean: q, labels: []string{"pod", "node"}},
		{name: "without with an op", in: q + "__without__:avg:pod", clean: q, op: "avg", labels: []string{"pod"}},
		{name: "padded and doubled", in: q + "__without__: a ,, b ", clean: q, labels: []string{"a", "b"}},
		{name: "last without wins", in: "x__without__:a y__without__:b", clean: "x__without__:a y", labels: []string{"b"}},
		{name: "label_replace", in: q + lrMarker("d", "$1", "s", "(.*)"), clean: q, specs: []physical.LabelReplace{{DstLabel: "d", Replacement: "$1", SrcLabel: "s", Regex: "(.*)"}}},
		{name: "empty arguments", in: q + lrMarker("", "", "", ""), clean: q, specs: []physical.LabelReplace{{}}},
		{name: "innermost first", in: q + lrMarker("a", "", "", "") + lrMarker("b", "", "", ""), clean: q, specs: []physical.LabelReplace{{DstLabel: "a"}, {DstLabel: "b"}}},
		{name: "without then label_replace", in: q + "__without__:p" + lrMarker("a", "", "", ""), clean: q, labels: []string{"p"}, specs: []physical.LabelReplace{{DstLabel: "a"}}},
		{name: "invalid payload stays", in: q + "__lvp_lr:!!__", clean: q + "__lvp_lr:!!__"},
		{name: "regexp that does not compile is read, not judged", in: q + lrMarker("a", "b", "c", "("), clean: q, specs: []physical.LabelReplace{{DstLabel: "a", Replacement: "b", SrcLabel: "c", Regex: "("}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clean, post := translator.SplitPost(tc.in)
			if clean != tc.clean || post.Folds() != (tc.op != "" || tc.labels != nil) {
				t.Fatalf("SplitPost = %q, folds %v; want %q", clean, post.Folds(), tc.clean)
			}
			if post.Folds() && (post.Without.Op != tc.op || !reflect.DeepEqual(post.Without.Labels, tc.labels) || !post.Without.Without) {
				t.Errorf("without = %#v, want op %q labels %#v", post.Without, tc.op, tc.labels)
			}
			if !reflect.DeepEqual(post.LabelReplace, tc.specs) {
				t.Errorf("label_replace = %#v, want %#v", post.LabelReplace, tc.specs)
			}
			assertSplitPost(t, tc.in)
		})
	}
}

// Numbers on their own are scalars, and what only looks like one stays a query.
// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestDecodePhysicalScalarText(t *testing.T) {
	for _, s := range []string{"1", "1e3", "-0", "+0", "+Inf", "-inf", "Infinity", "0x1p3", "0X1.8P-2", ".5", "5.", "1e-9", "1_0", "123456789012345678901234567890"} {
		op, err := translator.DecodePhysical(s)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := strconv.ParseFloat(s, 64)
		if got, ok := op.(physical.ScalarOp); !ok || got.Text != s || got.V != want || math.Signbit(got.V) != math.Signbit(want) {
			t.Errorf("DecodePhysical(%q) = %#v, want a scalar %v", s, op, want)
		}
		checkPhysicalCodec(t, s)
	}
	if op, _ := translator.DecodePhysical("NaN"); !math.IsNaN(op.(physical.ScalarOp).V) {
		t.Errorf("NaN is %v", op)
	}
	for _, s := range []string{"", " ", "1e", "e1", "nan1", "0x", "0b1", "- 1", "1 2", "1,5", "--1", "+-1", "app:=x", "inf inf", "NaNx"} {
		op, _ := translator.DecodePhysical(s)
		if _, ok := op.(physical.VLQuery); !ok {
			t.Errorf("DecodePhysical(%q) = %#v, want a plain query", s, op)
		}
		checkPhysicalCodec(t, s)
	}
	// a padded number is a number; the text is kept as written
	if op, _ := translator.DecodePhysical(" 5 "); op != (physical.ScalarOp{V: 5, Text: " 5 "}) {
		t.Errorf("DecodePhysical(\" 5 \") = %#v", op)
	}
}

// SplitPost must not cost more than the helpers it replaced: nothing on a query without markers, and no more
// allocations than the old strip for each marker shape.
func TestSplitPostAllocations(t *testing.T) {
	if raceDetectorEnabled || testing.CoverMode() != "" {
		t.Skip("allocation counts are not stable under the race detector or coverage")
	}
	const q = `app:=x | format if (level:*) "<level>" as __dl_s | stats by (pod, node) count() as c`
	long := strings.Repeat(q+" | ", 200)
	shapes := map[string]string{
		"none":                  q,
		"none, long":            long,
		"empty":                 "",
		"scalar":                "100",
		"binary":                "__binary__:/:A|||B",
		"binary with matching":  "__binary__:/:A|||B@@@on:app",
		"without":               q + "__without__:pod,node",
		"without with an op":    q + "__without__:sum:pod",
		"label_replace":         q + lrMarker("a", "$1", "app", "(.*)"),
		"label_replace three":   q + lrMarker("a", "$1", "app", "(.*)") + lrMarker("b", "x", "a", ".*") + lrMarker("c", "y", "b", "(.*)"),
		"without and replace":   q + "__without__:pod" + lrMarker("a", "$1", "app", "(.*)"),
		"binary over without":   "__binary__:/:A|||B__without__:pod",
		"invalid label_replace": q + "__lvp_lr:%%%__",
	}
	for name, s := range shapes {
		old := testing.AllocsPerRun(200, func() { _, _, _, _ = legacySplit(s) })
		got := testing.AllocsPerRun(200, func() { _, _ = translator.SplitPost(s) })
		if got > old {
			t.Errorf("%s: SplitPost allocates %v times, the old strip %v", name, got, old)
		}
		if !strings.Contains(s, "__without__:") && !strings.Contains(s, "__lvp_lr:") && got != 0 {
			t.Errorf("%s: SplitPost allocates %v times on a query without markers, want 0", name, got)
		}
	}
}

// sampleTranslations are the translations of the capture sample under the 12 profiles that carry a marker or a
// binary, and one in twenty of the others: the seeds of the fuzz targets. The seeds are not read in a corpus capture
// run.
func sampleTranslations(tb testing.TB) []string {
	tb.Helper()
	b, err := os.ReadFile(captureSamplePath)
	if err != nil {
		tb.Fatal(err)
	}
	var sample captureSample
	if err := json.Unmarshal(b, &sample); err != nil {
		tb.Fatal(err)
	}
	seen := map[string]bool{}
	var out []string
	other := 0
	for _, r := range sample.Rows {
		for _, prof := range planProfiles() {
			s, ok := prof.translate(r.Query)
			if !ok || seen[s] {
				continue
			}
			seen[s] = true
			if strings.Contains(s, "__") && (strings.Contains(s, "__without__:") || strings.Contains(s, "__lvp_lr:") || strings.HasPrefix(s, "__binary__:")) {
				out = append(out, s)
			} else if other++; other%20 == 0 {
				out = append(out, s)
			}
		}
	}
	return out
}

func addSplitSeeds(f *testing.F) {
	for _, s := range splitEdgeInputs() {
		if len(s) <= 1<<16 {
			f.Add(s)
		}
	}
	for _, s := range sampleTranslations(f) {
		f.Add(s)
	}
}

// FuzzSplitPostMatchesLegacy: for any text, SplitPost reads what the old two-step strip read.
func FuzzSplitPostMatchesLegacy(f *testing.F) {
	addSplitSeeds(f)
	f.Fuzz(func(t *testing.T, s string) { assertSplitPost(t, s) })
}

// FuzzPhysicalCodec: for any text, the decoded operators write it back, the operators of a structural decode write it
// back from their parts too, and SplitPost is the post-processing of the decode.
func FuzzPhysicalCodec(f *testing.F) {
	addSplitSeeds(f)
	f.Fuzz(func(t *testing.T, s string) { checkPhysicalCodec(t, s) })
}

// FuzzSplitPostOnTranslatorOutput: for any LogQL text the translator accepts, SplitPost reads its output like the
// reference, the codec writes it back, and a without fold or a label_replace is read only when the query, outside its
// strings, has a without or a label_replace: text inside a client's string never becomes one.
func FuzzSplitPostOnTranslatorOutput(f *testing.F) {
	b, err := os.ReadFile(captureSamplePath)
	if err != nil {
		f.Fatal(err)
	}
	var sample captureSample
	if err := json.Unmarshal(b, &sample); err != nil {
		f.Fatal(err)
	}
	for i, r := range sample.Rows {
		if i%3 == 0 || strings.Contains(r.Stratum, "without") || strings.Contains(r.Stratum, "label_replace") || strings.Contains(r.Stratum, "binary") {
			f.Add(r.Query)
		}
	}
	for _, q := range []string{
		`sum without (pod) (rate({app="x"}[5m]))`, `label_replace(rate({app="x"}[5m]), "a", "$1", "app", "(.*)")`,
		`rate({app="x"}[5m]) / on (app) group_left rate({app="y"}[5m])`, `{app="x"} |= "__without__:a"`, `{app="x"} |= "__lvp_lr:YQ==__"`,
		`label_replace(sum without (pod) (rate({app="x"}[5m])), "", "", "", "")`, `sum without () (rate({app="x"}[5m]))`,
		`sum(count_over_time({app="x"} |= "__without__:pod" [1m]))`, "{app=\"x\"} |= `__without__:pod`",
		`sum by (a) (count_over_time({app="__lvp_lr:YQ==__"} | json | x="__without__:a" [1m]))`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		s, err := translator.TranslateLogQL(q)
		if err != nil {
			t.Skip()
		}
		checkPhysicalCodec(t, s)
		code := strings.ToLower(withoutStrings(q))
		_, post := translator.SplitPost(s)
		if post.Folds() && !strings.Contains(code, "without") {
			t.Fatalf("%q: a without fold is read, and the query has no without outside its strings\n%q", q, s)
		}
		if len(post.LabelReplace) > strings.Count(code, "label_replace") {
			t.Fatalf("%q: %d label_replace calls are read, the query has %d outside its strings\n%q", q,
				len(post.LabelReplace), strings.Count(code, "label_replace"), s)
		}
	})
}

// withoutStrings is LogQL text with its quoted strings ("…", `…`) removed, escapes included.
func withoutStrings(q string) string {
	var b strings.Builder
	for i := 0; i < len(q); i++ {
		switch c := q[i]; c {
		case '"':
			for i++; i < len(q) && q[i] != '"'; i++ {
				if q[i] == '\\' {
					i++
				}
			}
		case '`':
			for i++; i < len(q) && q[i] != '`'; i++ {
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// opGen builds physical operators from fuzzer bytes: a byte stream picks the shape, the strings and the depth, and an
// exhausted stream picks zeros.
type opGen struct {
	b []byte
	i int
	// canonical keeps to the text the translator can write (no commas in labels, no markers in queries, no nested
	// binary on the left), where the decoded operators must equal the generated ones. Otherwise the strings are
	// hostile and only the text is checked.
	canonical bool
}

func (g *opGen) next() int {
	if g.i >= len(g.b) {
		return 0
	}
	g.i++
	return int(g.b[g.i-1])
}

var (
	safeText    = []string{"app:=x", `{app="x"} | stats count() as c`, "*", "level:error", "a b c", "é日本", `_msg:"x y"`, "x:y", "q_1"}
	hostileText = []string{"", "a", "é", "日本", "a,b", " a ", "x|||y", "@@@on:a", "__without__:", "__lvp_lr:", ":", "\x00", "_", "pod", "__binary__:", "\n", "\xff", "1e3", "__lvp_lr:YQ==__"}
	safeLabels  = []string{"pod", "node", "app", "é", "日本", "a_b", "x.y", "l1", "long" + strings.Repeat("x", 300)}
	scalarTexts = []string{"1", "100", "1e3", "-0", "0.5", "+Inf", "-Inf", ".5", "0x1p4", "2.5e-3"}
	binaryOps   = []string{"/", "*", "+", "-", "%", "^", "==", "!=", ">", "<", ">=", "<="}
	foldOps     = []string{"", "sum", "min", "max", "avg", "count"}
)

func (g *opGen) text() string {
	if g.canonical {
		return safeText[g.next()%len(safeText)]
	}
	if g.next()%4 == 0 {
		n := g.next() % 12
		raw := make([]byte, 0, n)
		for k := 0; k < n; k++ {
			raw = append(raw, byte(g.next()))
		}
		return string(raw)
	}
	return hostileText[g.next()%len(hostileText)]
}

func (g *opGen) label() string {
	if g.canonical {
		return safeLabels[g.next()%len(safeLabels)]
	}
	return g.text()
}

func (g *opGen) operand(depth int, rightmost bool) physical.Op {
	switch k := g.next() % 4; {
	case k == 0:
		t := scalarTexts[g.next()%len(scalarTexts)]
		v, _ := strconv.ParseFloat(t, 64)
		return physical.ScalarOp{V: v, Text: t}
	case k == 1 && depth > 0 && (rightmost || !g.canonical):
		return g.binary(depth-1, g.canonical)
	}
	return physical.VLQuery{Text: g.operandText()}
}

// operandText is a query that can be one side of a binary: no separator, and not a number.
func (g *opGen) operandText() string {
	if g.canonical {
		return safeText[g.next()%len(safeText)]
	}
	return g.text()
}

func (g *opGen) binary(depth int, bare bool) physical.Binary {
	b := physical.Binary{Op: binaryOps[g.next()%len(binaryOps)]}
	b.L, b.R = g.operand(depth, false), g.operand(depth, true)
	if !bare || !g.canonical {
		switch g.next() % 4 {
		case 1:
			b.Tail = "@@@on:" + g.label() + "," + g.label()
		case 2:
			b.Tail = "@@@ignoring:" + g.label() + "@@@group_left:" + g.label()
		case 3:
			if !g.canonical {
				b.Tail = "@@@" + g.text()
			}
		}
	}
	return b
}

func (g *opGen) op() physical.Op {
	var out physical.Op
	switch k := g.next() % 6; {
	case k == 0:
		t := scalarTexts[g.next()%len(scalarTexts)]
		v, _ := strconv.ParseFloat(t, 64)
		out = physical.ScalarOp{V: v, Text: t}
	case k <= 2:
		out = g.binary(1+g.next()%2, false)
	default:
		out = physical.VLQuery{Text: g.text()}
	}
	if g.next()%2 == 0 {
		level := physical.FoldLevel{Op: foldOps[g.next()%len(foldOps)], Without: true}
		for n := 1 + g.next()%4; n > 0; n-- {
			level.Labels = append(level.Labels, g.label())
		}
		out = physical.Fold{Levels: []physical.FoldLevel{level}, In: out}
	}
	if n := g.next() % 5; n > 0 {
		var specs []physical.LabelReplace
		for ; n > 0; n-- {
			spec := physical.LabelReplace{DstLabel: g.label(), Replacement: g.text(), SrcLabel: g.label(), Regex: g.text()}
			specs = append(specs, spec)
		}
		out = physical.LabelMap{Replace: specs, In: out}
	}
	return out
}

// plain drops what a decode adds to an operator: the text of a binary and its parsed matching (the generated one has
// the modifier text only), and the value of a number (NaN is not equal to itself).
func plain(op physical.Op) physical.Op {
	switch v := op.(type) {
	case physical.Binary:
		v.Text, v.Match, v.L, v.R = "", nil, plain(v.L), plain(v.R)
		return v
	case physical.ScalarOp:
		return physical.ScalarOp{Text: v.Text}
	case physical.Fold:
		v.In = plain(v.In)
		return v
	case physical.LabelMap:
		v.In = plain(v.In)
		return v
	}
	return op
}

// FuzzEncodeDecodeOps: operators built from the fuzzer's bytes encode to text that decodes to operators writing the
// same text. Built from text the translator could write, they decode to the operators they were built from; built
// from hostile strings (commas, separators, markers, NUL, invalid UTF-8) the text still round-trips and SplitPost
// still reads it like the old strip.
func FuzzEncodeDecodeOps(f *testing.F) {
	for _, seed := range [][]byte{
		nil, {0}, {1, 1, 0, 0, 1, 2, 1, 3, 4}, {2, 3, 1, 5, 0, 0, 2, 1, 1, 7, 7, 2}, {3, 0, 0, 1, 1, 1, 1, 9, 9, 9, 9},
		[]byte("\x01\x05\x02\x00\x03\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d"), {255, 255, 255, 255, 255, 255, 255, 255, 255},
	} {
		f.Add(seed, true)
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, data []byte, canonical bool) {
		g := &opGen{b: data, canonical: canonical}
		op := g.op()
		text := translator.EncodePhysical(op)
		got, err := translator.DecodePhysical(text)
		if err != nil {
			t.Fatal(err)
		}
		if again := translator.EncodePhysical(got); again != text {
			t.Fatalf("decode then encode of %q = %q", text, again)
		}
		checkPhysicalCodec(t, text)
		if !canonical {
			return
		}
		if _, v := got.(physical.Verbatim); v {
			t.Fatalf("%q, written from %#v, is kept as text", text, op)
		}
		if want, have := plain(op), plain(got); !reflect.DeepEqual(want, have) {
			t.Fatalf("%q decodes to\n%#v\nwant\n%#v", text, have, want)
		}
	})
}

// The generator reaches every kind of operator, so the fuzz target is not blind to one.
func TestOpGeneratorCoversTheOperators(t *testing.T) {
	kinds := map[string]bool{}
	var walk func(physical.Op)
	walk = func(op physical.Op) {
		kinds[fmt.Sprintf("%T", op)] = true
		switch v := op.(type) {
		case physical.Binary:
			walk(v.L)
			walk(v.R)
		case physical.Fold:
			walk(v.In)
		case physical.LabelMap:
			walk(v.In)
		}
	}
	for seed := 0; seed < 4000; seed++ {
		data := make([]byte, 40)
		for i := range data {
			data[i] = byte(seed*31 + i*7 + seed/3)
		}
		walk((&opGen{b: data, canonical: seed%2 == 0}).op())
	}
	for _, want := range []string{"physical.Binary", "physical.ScalarOp", "physical.VLQuery", "physical.Fold", "physical.LabelMap"} {
		if !kinds[want] {
			t.Errorf("the generator never builds a %s", want)
		}
	}
}
