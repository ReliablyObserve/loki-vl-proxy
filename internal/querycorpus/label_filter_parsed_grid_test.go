package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// labelFilterParsedGrid is the generated grid of log queries with label filters after a json or logfmt parser:
// every kind of value against names that are plain, hold underscores (the keys a parser can have stored them
// under), are hyphenated, dotted, underscore-leading, error labels, collision names and names the
// extraction list defines; and/or/comma trees; the filters among line filters, pattern filters, drops, keeps and
// a second parser; before and after the parser; repeated. Deterministic; the lowering and the string
// translator are held to each other, text included, on every row under every profile
// (TestLabelFilterParsedGridShadow).
func labelFilterParsedGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	parsers := []string{
		`| json`, `| logfmt`, `| logfmt --strict`, `| logfmt --keep-empty`, `| json a`, `| json a, b`, `| json a_b`, `| json x="y.z"`,
		`| json a_b="c"`, `| json a_b="a_b"`, `| logfmt a="b"`, `| logfmt http_status`, `| json status="http.status"`,
		`| json x="y.z", http_status="http.status"`, `| json st="status" | json x="a_b"`, `| json http_status="a.b", k8s_pod_name="pod"`, `| json x="service_name"`,
		`| json x="a b"`, `| json x="a-b", y="a.b.c"`, `| json x="1a"`, `| json x="k8s.pod.name"`, `| json x="status" | logfmt x="q"`,
	}
	names := []string{
		"status", "st", "level", "http_status", "a_b_c", "k8s_pod_name", "k8s.pod.name", "a-b", "_x", "__error__", "__error_details__",
		"detected_level", "service_name", "a_extracted", "pod_extracted", "service_name_extracted", "x_y_extracted", "_extracted", "a_extracted_extracted", "Foo_Bar", "a_", "a_b_c_d_e_f_g_h", "a__b", "x", "y_z", "app", "pod", "env", "namespace", "cluster", "extract", "drop", "keep", "json", "logfmt", "pattern", "regexp",
	}
	reps := []string{
		`="error"`, `!="error"`, `==""`, `=""`, `!=""`, `=="x"`, `=~"a|b"`, `!~"a|b"`, `=~".*"`, `!~".*"`, `=~"a.*"`, `=~"(?i)x"`, `=~"x*"`, `!~"x*"`,
		`=~"a?"`, `=~"(a|)"`, `!~"(a|)"`, `=~""`, `!~""`, `=~".+"`, `>5`, `>=1KB`, `<5s`, `<=1.5`, `>"abc"`, `=5`, `==5`, `!=5`, `=5s`, `!=1KB`, `="5"`,
		`!="5"`, "=`raw`", `="a\"b"`, `="é"`, ` = "error"`, ` >= 5`, ` !~ "a|b"`, `=ip("10.0.0.0/8")`, `!=ip("10.0.0.1-10.0.0.5")`, `=ip("::1")`, `=~"a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s"`, `=~"("`,
	}
	for _, p := range parsers {
		for _, n := range names {
			for _, r := range reps {
				add(`{app="a"} ` + p + ` | ` + n + r)
			}
		}
	}
	// the stage forms: spaces, parentheses, juxtaposition.
	for _, q := range []string{
		`| status = "x"`, `| status   >=   500`, `| (status="x")`, `| ((a_b="x"))`, `| ( http_status >= 500 )`, `|status="x"`,
		`| a_b = "x"`, `| a_b="x" a_c="y"`, `| a_b="x", a_c="y"`,
	} {
		for _, p := range []string{`| json`, `| logfmt`} {
			add(`{app="a"} ` + p + ` ` + q)
		}
	}
	// and/or/comma trees over a leaf set.
	leaves := []string{`level="error"`, `status>=500`, `a_b=~"x|y"`, `b!="2"`, `ip_a=ip("10.0.0.0/8")`, `c_d!~"p.*"`, `d_e=""`, `level=""`, `e_f!=""`, `k8s.pod.name="p"`, `f=1m30s`, `g_h=~".*"`, `__error__=""`, `x_y=~"x*"`}
	for _, x := range leaves {
		for _, p := range []string{`| json`, `| logfmt`} {
			add(`{app="a"} ` + p + ` | (` + x + `)`)
		}
		for _, y := range leaves {
			for _, c := range []string{" and ", " or ", ", ", " "} {
				add(`{app="a"} | json | ` + x + c + y)
			}
			add(`{app="a"} | logfmt | (` + x + ` or ` + y + `) and ` + leaves[0])
			add(`{app="a"} | json | ` + x + ` and (` + y + ` or ` + leaves[1] + `)`)
			add(`{app="a"} | logfmt | (` + x + ` and ` + y + `) or (` + leaves[2] + ` and ` + leaves[3] + `)`)
			add(`{app="a"} | json | ` + x + ` or ` + y + ` or ` + leaves[4])
			add(`{app="a"} | json | ` + x + `, ` + y + ` or ` + leaves[6])
		}
	}
	// the filters among other stages, before and after the parser, and repeated.
	reps2 := []string{`level="error"`, `status>=500`, `a_b=~"x|y"`, `a_b="1" and b_c!="2"`, `d=""`, `e_f=""`, `ip_a=ip("10.0.0.0/8")`, `a_b!=""`, `x_y=~"p.*" or y="2"`, `m_n=1KB`, `__error__=""`, `a_b=~"x*"`}
	lines := []string{`|= "x"`, `!= "y"`, `|~ "z.+"`, `!~ "w"`, `|> "<_> x <_> y"`, `!> "<_> a <_> b"`, `|= "k" or "j"`}
	others := []string{`| drop q`, `| drop q="r"`, `| drop __error__, __error_details__`, `| keep a_b, k`, `| keep k="v"`, `| drop level`}
	for _, f := range reps2 {
		for _, p := range []string{`| json`, `| logfmt`, `| json a="b.c"`, `| logfmt --strict`} {
			add(`{app="a"} ` + p + ` | ` + f)
			add(`{app="a", env=~"prod|stg"} ` + p + ` | ` + f)
			add(`{app!="a", env="x"} ` + p + ` | ` + f)
			add(`{app="a"} | ` + f + ` ` + p)
			add(`{app="a"} | ` + f + ` ` + p + ` | ` + f)
			for _, l := range lines {
				add(`{app="a"} ` + p + ` | ` + f + ` ` + l)
				add(`{app="a"} ` + p + ` ` + l + ` | ` + f)
				add(`{app="a"} ` + l + ` ` + p + ` | ` + f + ` ` + l)
				add(`{app="a"} ` + p + ` | ` + f + ` ` + l + ` | ` + reps2[0] + ` ` + l)
			}
			for _, o := range others {
				add(`{app="a"} ` + p + ` ` + o + ` | ` + f)
				add(`{app="a"} ` + p + ` | ` + f + ` ` + o)
				add(`{app="a"} ` + p + ` | ` + f + ` ` + o + ` | ` + reps2[1])
			}
			for _, g := range reps2 {
				add(`{app="a"} ` + p + ` | ` + f + ` | ` + g)
				add(`{app="a"} ` + p + ` | ` + f + ` | ` + g + ` | ` + f)
				add(`{app="a"} | ` + f + ` ` + p + ` | ` + g)
				add(`{app="a"} ` + p + ` |> "<_> x <_> y" | ` + f + ` |= "k" | ` + g + ` != "j"`)
			}
		}
		// two parsers.
		add(`{app="a"} | json | ` + f + ` | logfmt | ` + reps2[1])
		add(`{app="a"} | json | logfmt | ` + f)
		add(`{app="a"} | json a="b" | ` + f + ` | json c="d.e" | ` + reps2[2])
	}
	// the name_extracted labels with several parsers, among other stages.
	ex := []string{`a_extracted="1"`, `pod_extracted=~"x|y"`, `service_name_extracted!=""`, `a_extracted="1" and b_extracted!~"p.*"`, `x_y_extracted=""`, `a_extracted>5`, `(a_extracted="1" or env="e")`}
	for _, e := range ex {
		for _, ps := range []string{`| json`, `| logfmt`, `| json | logfmt`, `| logfmt | json`, `| json | json`, `| json | logfmt --strict`, `| logfmt --strict | json`, `| json a`, `| json | json a="b.c"`} {
			add(`{app="a"} ` + ps + ` | ` + e)
			add(`{app="a"} ` + ps + ` | ` + e + ` | ` + ex[0])
			add(`{app="a"} ` + ps + ` | ` + e + ` |= "k" | ` + ex[3] + ` != "j"`)
			add(`{app="a"} ` + ps + ` | drop q | ` + e + ` | status>=5`)
			add(`{app="a"} | ` + e + ` ` + ps)
			add(`{app="a"} ` + ps + ` | ` + e + ` | keep a`)
		}
		add(`{app="a"} | json | ` + e + ` | logfmt | ` + ex[1] + ` | json | ` + ex[2])
	}
	// the stream labels read after a parser, among the other readings of the pipeline text.
	for _, q := range []string{
		`{app="a"} |= "{{.pod}}" | json`, `{app="a"} | json | x="{{ .pod }}"`, `{app="a"} | json |= "q" | x="{{.pod}}"`,
		`{app="a"} | json | pod="p" and x="y"`, `{app="a"} | json | pod="p" | app="q" | env="z" | x_y="r"`, `{app="a"} | json | (pod="p")`,
		`{app="a"} | json | pod ="p"`, `{app="a"} | json | pod="p" | logfmt | env!="e"`, `{app="a"} | json x="pod" | pod="p"`,
		`{app="a"} | json pod="x" | pod="p"`, `{app="a"} | json pod="x" | x="p" | env="e"`, `{app="a"} | json x="pod" | x="p" | env="e"`,
		`{app="a"} | pod="p" | json | pod!="q"`, `{app="a"} | pod="p" | json`, `{app="a"} | json | pod=~"p.*" | pod_extracted="1"`,
		`{app="a"} | json | pod="p" |= "k" | pod!="q" != "j"`, `{app="a"} | json | drop pod | pod="p"`, `{app="a"} | json | keep pod | pod="p"`,
		`{app="a"} | json | pod="p" or env="e"`, `{app="a"} | json | env="e", pod="p"`, `{app="a"} | json | (env="e" and (pod="p" or app="q"))`,
		`{app="a"} | logfmt | pod=ip("10.0.0.0/8")`, `{app="a"} | logfmt | pod!=""`, `{app="a"} | logfmt | pod=""`, `{app="a"} | json | service_name="s"`,
	} {
		add(q)
	}
	return out
}

// TestLabelFilterParsedGridShadow holds the lowering of label filters after a parser to the string translator on
// the generated grid under every profile: 0 mismatches (text included), and the rows each side answers are
// counted.
// conformance: architecture-syntax-package-layering
func TestLabelFilterParsedGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := labelFilterParsedGrid()
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
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}

// TestPlanCorpusLowersParsedLabelFiltersByteForByte holds the text of the lowering to the string translator's, byte
// for byte, on every corpus row with a label filter after a parser that the lowering writes natively under every
// profile (the text a flipped query serves; the shadow compares structure).
// conformance: architecture-syntax-package-layering
func TestPlanCorpusLowersParsedLabelFiltersByteForByte(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	rows := 0
	for _, q := range planCorpus(t) {
		if !strings.Contains(q, "| json") && !strings.Contains(q, "| logfmt") {
			continue
		}
		r, _, _, problems := labelFilterShadow(t, []string{q})
		rows += r
		if len(problems) > 0 {
			t.Fatalf("%s", joinFirst(problems, 5))
		}
	}
	if rows == 0 {
		t.Fatal("no corpus row with a parser")
	}
}
