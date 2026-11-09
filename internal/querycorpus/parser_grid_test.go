package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// parserGrid is the generated grid of log queries with json and logfmt parsers (their flags and field lists:
// aliases, bracket paths, raw strings, escapes, commas, repeated names) and drop and keep stages (bare names,
// matchers and both, the level fields, the _extracted names, dotted names, the names a stream-labels profile
// reads), alone, in pairs and in threes, among line filters, pattern filters and label filters before the
// parser, under several selectors. Deterministic; the lowering and the string translator are held to each other
// on every row under every profile (TestParserGridShadow).
func parserGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	parsers := []string{
		`| json`, `| json a`, `| json a, b`, `| json a="b"`, `| json a="b.c"`, `| json a="b", c="d"`, `| json a="a"`, `| json a="x", a="y"`,
		`| json a="x", a`, `| json a, a="x"`, `| json a="a", a="x"`, "| json a=`raw`", `| json a="b\"c"`, `| json a="b,c"`, `| json a=" b"`,
		`| json a="b "`, `| json a="[0]"`, `| json a="[\"x\"]"`, `| json a="[\"x\"][\"y\"]"`, `| json a=""`, `| json a="k8s.pod.name"`,
		`| json a-b="x"`, `| json a_b="x.y"`, `| json k8s.pod.name="x"`, `| json z="1", a="2", m="3"`, `| json a="b\\c"`, `| json  a = "b" ,  c = "d"`,
		`| json app="x"`, `| json pod="a.b"`, `| json status="http.status"`, `| json level="severity"`, `| json detected_level="x"`,
		`| logfmt`, `| logfmt --strict`, `| logfmt --keep-empty`, `| logfmt --strict --keep-empty`, `| logfmt --keep-empty --strict`,
		`| logfmt a`, `| logfmt a, b`, `| logfmt a="b"`, `| logfmt --strict a="b.c", d`, `| logfmt --keep-empty a="b"`, "| logfmt a=`b`",
		`| logfmt app="x"`, `| logfmt pod`, `| logfmt level="lvl"`,
		`| unpack`, "| regexp `(?P<a>\\d+)`", "| pattern `<a> <b>`", `| decolorize`, `| line_format "{{.a}}"`, `| label_format a=b`,
	}
	drops := []string{
		`| drop a`, `| drop a, b`, `| drop level`, `| drop level, detected_level`, `| drop detected_level`, `| drop LEVEL, x`, `| drop Level`,
		`| drop log.level`, `| drop severity_number, a`, `| drop __error__`, `| drop __error__, __error_details__`, `| drop a="x"`,
		`| drop a=~"x.*"`, `| drop a!="x"`, `| drop a!~"y"`, `| drop a, b="x"`, `| drop b="x", a`, `| drop a="x,y"`, `| drop a="x\"y"`,
		"| drop a=`x\"y`", "| drop a=`x,y`", `| drop a="x y", b`, `| drop k8s.pod.name`, `| drop k8s_pod_name`, `| drop pod`, `| drop app`,
		`| drop app="x"`, `| drop level="debug"`, `| drop level="debug", level`, `| drop a, a`, `| drop a_extracted`, `| drop service_name`,
		`| drop pod, namespace, env`, `| drop a="x" , b="y"`, `| drop a=~"5\\d\\d"`, `| drop a=""`, `| drop a="é"`,
	}
	keeps := []string{
		`| keep a`, `| keep a, b`, `| keep level`, `| keep detected_level`, `| keep detected_level, level`, `| keep level, detected_level`,
		`| keep a_extracted`, `| keep a_extracted, a`, `| keep a, a_extracted`, `| keep _extracted`, `| keep a_extracted_extracted`,
		`| keep a="x"`, `| keep a=~"x"`, `| keep a!="x"`, `| keep a!~"x"`, `| keep a="x", b="y"`, `| keep a, b="x"`, `| keep b="x", a`,
		`| keep a="x=~y"`, `| keep a="x!y"`, `| keep a="x~y"`, `| keep a="x,y"`, `| keep a="x\"y"`, "| keep a=`x\"y`", `| keep k8s_pod_name`,
		`| keep k8s.pod.name`, `| keep service_name`, `| keep deployment_environment`, `| keep app`, `| keep pod_extracted`, `| keep detected_level="x"`,
		`| keep service_name, detected_level`, `| keep a, a`, `| keep k8s_pod_name, k8s.pod.name`, `| keep a=""`, `| keep a="é"`,
	}
	lines := []string{`|= "x"`, `!= "y"`, `|~ "z.+"`, `!~ "w"`, `|> "<_> x <_> y"`, `!> "<_> a <_> b"`, `|= "k" or "j"`}
	labels := []string{`| lvl="e"`, `| status>=500`, `| a=~"x|y" and b!="2"`, `| detected_level="e"`, `| k8s_pod_name="p"`, `| a=""`, `| pod="p"`, `| app!="q"`}
	selectors := []string{`{app="a"}`, `{app="a", env=~"prod|stg"}`, `{detected_level="error"}`, `{service_name="x", pod!="y"}`}

	// each stage alone, under every selector.
	var all []string
	all = append(all, parsers...)
	all = append(all, drops...)
	all = append(all, keeps...)
	for _, sel := range selectors {
		for _, s := range all {
			add(sel + " " + s)
		}
	}
	sel := selectors[0]
	// every parser among each kind of stage, before and after.
	around := append(append(append([]string{}, lines...), labels...), `| drop x`, `| drop x="y"`, `| keep k`, `| keep k="v"`, `| json`, `| logfmt --strict`, `| json a="b.c"`)
	for _, p := range parsers {
		for _, a := range around {
			add(sel + " " + p + " " + a)
			add(sel + " " + a + " " + p)
			add(sel + " " + a + " " + p + " " + a)
		}
	}
	// drop and keep among a parser and the stages around it.
	for _, d := range append(append([]string{}, drops...), keeps...) {
		for _, a := range around {
			add(sel + " " + d + " " + a)
			add(sel + " " + a + " " + d)
			add(sel + " | json " + d + " " + a)
			add(sel + " " + a + " | logfmt " + d)
			add(sel + " " + d + " | json a=\"b.c\" " + a)
		}
	}
	// ordered triples over a pool.
	pool := []string{`| json`, `| json a="b.c"`, `| logfmt --strict`, `| drop a`, `| drop level, x`, `| drop q="r"`, `| keep k`, `| keep detected_level`, `|= "x"`, `!= "y"`, `|> "<_> x <_> y"`, `| lvl="e"`, `| status>=500`}
	for _, x := range pool {
		for _, y := range pool {
			for _, z := range pool {
				add(sel + " " + x + " " + y + " " + z)
			}
		}
	}
	// the labels after a parser stay with the string translator; before one they are lowered.
	for _, l := range labels {
		add(sel + " | json " + l)
		add(sel + " | logfmt " + l)
		add(sel + " | drop a " + l + " | json")
		add(sel + " " + l + " | keep a | logfmt")
	}
	return out
}

// TestParserGridShadow holds the lowering of json, logfmt, drop and keep to the string translator on the
// generated grid under every profile: 0 mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestParserGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := parserGrid()
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
	if native < rows/3 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
