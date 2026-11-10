package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// extractGrid is the generated grid of log queries with the regexp and pattern parsers and decolorize: regexps
// and patterns written every way (named and unnamed groups, names the label translation maps, names of stream
// labels and of derived labels, alternations, quotes, backticks, escapes, non-ASCII, wildcard-only patterns),
// alone, in pairs, among line filters, pattern filters, label filters on the captured names and on others,
// json and logfmt parsers, drops and keeps, under several selectors. Deterministic; the lowering and the string
// translator are held to each other, text included, on every row under every profile (TestExtractGridShadow).
func extractGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	regexps := []string{
		"| regexp `(?P<a>\\d+)`", "| regexp `(?P<a>\\d+) (?P<b>\\w+)`", "| regexp `(\\d+)`", "| regexp `(?P<a>x)|(?P<b>y)`",
		"| regexp `(?P<k8s_pod_name>\\S+)`", "| regexp `(?P<status_code>\\d+)`", "| regexp `(?P<app>\\S+)`", "| regexp `(?P<pod>\\S+) (?P<level>\\w+)`",
		"| regexp `(?P<detected_level>\\w+)`", "| regexp `(?P<service_name>\\S+)`", "| regexp `(?P<a_extracted>\\S+)`", "| regexp `(?P<a>\\S+)(?P<a_extracted>\\S+)`",
		"| regexp `(?P<a>\\S+)\"x\"`", "| regexp `(?P<a>\\S+)'x'`", "| regexp `(?P<a>[^\"]+)`", `| regexp "(?P<a>\\d+)"`, `| regexp "(?P<a>\\S+)\"q\""`,
		`| regexp "(?P<a>\\S+)\t"`, `| regexp "(?P<a>\\S+)\n"`, `| regexp "(?P<a>é+)"`, "| regexp `(?P<a>é+)`",
		`| regexp "(?P<a>\\S+)é"`, "| regexp `(?P<a>.*)`", "| regexp `.*`", "| regexp `(?P<a>a|b|c)`", "| regexp `(?P<a>\\S+) | (?P<b>\\S+)`",
		"| regexp `(?P<a>{x})`", "| regexp `(?P<a>\\S+)#x`", "| regexp `(?P<a>\\S+) != x`", "| regexp `(?P<a>\\S+) |= x`", "| regexp `(?P<a>a\\\\b)`",
		"| regexp `(?P<a>\\S+) `", "| regexp ` (?P<a>\\S+)`", "| regexp `(?P<A>\\S+)`", "| regexp `(?P<a1>\\S+)`", "| regexp `(?P<_a>\\S+)`", "| regexp `(?i)(?P<a>x)`",
		"| regexp `(?<a>\\S+)`", "| regexp `^(?P<ip>\\S+) \\S+ \\S+ \\[(?P<ts>[^\\]]+)\\] \"(?P<method>\\S+) (?P<path>\\S+)[^\"]*\" (?P<code>\\d+)`",
	}
	patterns := []string{
		"| pattern `<a> <b>`", "| pattern `<a> <_> <b>`", "| pattern `<_>`", "| pattern `<_> <_>`", "| pattern `<a>`", "| pattern `<a>.<b>`", "| pattern `(.*)`",
		"| pattern `.*`", "| pattern `^.*$`", "| pattern `(?s).*`", "| pattern `(?s:.*)`", "| pattern ` (.*) `", "| pattern `<a>(.*)`", "| pattern `<k8s_pod_name> <x>`",
		"| pattern `<app> <pod>`", "| pattern `<level> <detected_level>`", "| pattern `<service_name>`", "| pattern `<a_extracted>`", "| pattern `<a> <a>`", "| pattern `<_a>`",
		"| pattern `<1a>`", "| pattern `<a.b>`", "| pattern `<a-b>`", "| pattern `<A>`", "| pattern `<a1_b>`", `| pattern "<a> <b>"`, `| pattern "<a> \"q\" <b>"`,
		"| pattern `<a> \"q\" <b>`", "| pattern `<a> 'q' <b>`", `| pattern "<a>\t<b>"`, `| pattern "<a>\n"`, `| pattern "<a>é<b>"`, "| pattern `<a>é<b>`",
		`| pattern "<a>é<b>"`, "| pattern `<a>|<b>`", "| pattern `<a> | <b>`", "| pattern `<a> != <b>`", "| pattern `<a> |= <b>`", "| pattern `<ip> - - [<_>] \"<method> <path> <_>\" <status>`",
		"| pattern `{<a>}`", "| pattern `<a>#<b>`", "| pattern `<a> `", "| pattern ` <a>`", "| pattern `a`", "| pattern ``",
	}
	others := []string{`| decolorize`}
	stages := append(append(append([]string{}, regexps...), patterns...), others...)
	selectors := []string{`{app="a"}`, `{app="a", env=~"prod|stg"}`, `{detected_level="error"}`, `{service_name="x", pod!="y"}`}

	for _, sel := range selectors {
		for _, s := range stages {
			add(sel + " " + s)
		}
	}
	sel := selectors[0]
	lines := []string{`|= "x"`, `!= "y"`, `|~ "z.+"`, `!~ "w"`, `|> "<_> x <_> y"`, `!> "<_> a <_> b"`, `|= "k" or "j"`}
	labels := []string{
		`| a="5"`, `| b!="x"`, `| a=~"x|y" and b="2"`, `| k8s_pod_name="p"`, `| k8s.pod.name="p"`, `| status_code>=500`, `| status_code="5"`, `| app="a"`, `| pod="p"`,
		`| level="e"`, `| detected_level="e"`, `| service_name="x"`, `| a_extracted="x"`, `| a=""`, `| a!=""`, `| code>=400`, `| a=5`, `| a_b="1"`, `| x_y="1"`,
	}
	around := append(append(append([]string{}, lines...), labels...), `| drop x`, `| drop a`, `| drop x="y"`, `| keep k`, `| keep a, k8s_pod_name`, `| keep k="v"`, `| decolorize`,
		`| json`, `| logfmt --strict`, `| json a="b.c"`, "| regexp `(?P<z>\\d+)`", "| pattern `<z> <_>`", "| pattern `(.*)`")
	for _, s := range stages {
		for _, a := range around {
			add(sel + " " + s + " " + a)
			add(sel + " " + a + " " + s)
			add(sel + " " + a + " " + s + " " + a)
		}
	}
	// pairs and triples of the parsers.
	pool := []string{regexps[0], regexps[1], regexps[5], regexps[6], patterns[0], patterns[2], patterns[6], patterns[13], patterns[14], `| decolorize`, `| json`, `| logfmt`, `|= "x"`, `!= "y"`, `| a="5"`, `| k8s_pod_name="p"`, `| drop a`, `| keep a`, `| status_code>=500`}
	for _, x := range pool {
		for _, y := range pool {
			add(sel + " " + x + " " + y)
			for _, z := range pool {
				add(sel + " " + x + " " + y + " " + z)
			}
		}
	}
	return out
}

// extractSpellingGrid are the regexps and patterns written with escapes strconv.Quote spells otherwise (`\x41`,
// `\u00e9`, octal): the string translator keeps a double-quoted argument as written, the typed stage holds it
// unquoted, so the lowering writes the canonical spelling. VictoriaLogs reads the two alike (TestExtractSpellingClass).
func extractSpellingGrid() []string {
	var out []string
	for _, s := range []string{
		`| regexp "(?P<a>\\S+)\x41"`, `| regexp "(?P<a>\\S+)\u00e9"`, `| regexp "(?P<a>\\S+)\101"`, `| regexp "\x28?P<a>\x5cd+\x29"`,
		`| pattern "<a>\x41<b>"`, `| pattern "<a>\u00e9<b>"`, `| pattern "<a>\101<b>"`, `| pattern "<a>\x7c<b>"`, `| pattern "\x3ca\x3e <b>"`,
	} {
		for _, a := range []string{``, `| a="5"`, `|= "x"`, `| keep a`} {
			out = append(out, `{app="a"} `+s+` `+a)
		}
	}
	return out
}

// TestExtractSpellingClass holds the lowering to the string translator, by meaning and not by bytes, on the
// spellings the typed stage cannot tell from the canonical one.
// conformance: architecture-syntax-package-layering
func TestExtractSpellingClass(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	rows, native, _, problems := gridShadow(t, validQueries(extractSpellingGrid()), false)
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 20))
	}
	if native == 0 || native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}

// TestExtractGridShadow holds the lowering of the regexp and pattern parsers and decolorize to the string
// translator on the generated grid under every profile: 0 mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestExtractGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := validQueries(extractGrid())
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 20))
	}
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}

// validQueries are the queries of the grid that LogQL validation accepts: every route answers Loki's rejection of
// the others before it translates anything (a pattern without a named capture, a regexp that does not compile).
func validQueries(grid []string) []string {
	out := grid[:0:0]
	for _, q := range grid {
		if logql.ParseAndValidate(logql.CanonicalizeLogRanges(q)) == nil {
			out = append(out, q)
		}
	}
	return out
}
