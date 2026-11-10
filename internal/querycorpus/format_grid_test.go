package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// formatGrid is the generated grid of log queries with line_format: templates of text and fields written every
// way (spaces in the action, raw strings, escapes, non-ASCII, repeated and adjacent fields, the names of stream
// labels and of the labels the profile translates, JSON text, percent signs, and the templates the lowering
// leaves to the translator: functions, pipes, conditions, dotted fields, trim markers, angle brackets), alone, in
// pairs, among line filters, label filters on the names it reads and on others, and every parser, drop, keep and
// decolorize, under several selectors. Deterministic; the lowering and the string translator are held to each
// other, text included, on every row under every profile (TestFormatGridShadow).
func formatGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	formats := []string{
		`| line_format "{{.a}}"`, `| line_format "{{.a}} {{.b}}"`, `| line_format "{{ .a }}"`, `| line_format "{{.a }}"`, `| line_format "{{  .a  }} x"`,
		"| line_format `{{.a}}`", "| line_format `{{.a}} \"q\" {{.b}}`", `| line_format "{{.a}}{{.b}}"`, `| line_format "{{.a}} {{.a}}"`, `| line_format "plain"`,
		`| line_format ""`, `| line_format " "`, `| line_format "{{.k8s_pod_name}}"`, `| line_format "{{.k8s.pod.name}}"`, `| line_format "{{.app}} {{.pod}}"`,
		`| line_format "{{.service_name}}"`, `| line_format "{{.level}} {{.detected_level}}"`, `| line_format "{{._x}}"`, `| line_format "{{.a_extracted}}"`,
		`| line_format "{{.A1}}"`, `| line_format "{{.a}}\n{{.b}}"`, `| line_format "{{.a}}\t"`, `| line_format "\"{{.a}}\""`, `| line_format "{{.a}}\\"`,
		`| line_format "{{.a}} é {{.b}}"`, "| line_format `{{.a}}é`", `| line_format "{{.a}}\x41"`, `| line_format "{{.a}}é"`, `| line_format "{\"a\":\"{{.a}}\"}"`,
		`| line_format "{{.a}}%s%d"`, `| line_format "{{.a}} | {{.b}}"`, `| line_format "{{.a}} != x"`, `| line_format "{{.a}} |= x"`, `| line_format "{{.a}} # x"`,
		`| line_format "{{.a | upper}}"`, `| line_format "{{upper .a}}"`, `| line_format "{{if .a}}x{{end}}"`, `| line_format "{{.}}"`, `| line_format "{{__line__}}"`,
		`| line_format "{{ __timestamp__ }}"`, `| line_format "{{- .a -}}"`, `| line_format "{{.a -}}"`, `| line_format "{{/* c */}}{{.a}}"`, `| line_format "{{\"x\"}}"`,
		`| line_format "{{.a}} <b>"`, `| line_format "<a>"`, `| line_format "{{.a}} >"`, `| line_format "{{$.a}}"`, `| line_format "{{ index . \"a\" }}"`,
		`| line_format "{{.a}} {{.b.c}}"`, `| line_format "{{.a}} {{range .b}}x{{end}}"`, `| line_format "{{define \"t\"}}x{{end}}{{.a}}"`,
	}
	// label_format: renames and templates written every way, the destinations the profile translates, stream
	// labels, labels read by the filters after it, repeated and mixed assignments.
	formats = append(formats,
		`| label_format a=b`, `| label_format a=b, c=d`, `| label_format a="{{.b}}"`, `| label_format a="{{.b}}-x", c=d`, "| label_format a=`{{.b}}`", `| label_format a="x"`,
		`| label_format a=""`, `| label_format a=a`, `| label_format a=b, b=c`, `| label_format a=b, a=c`, `| label_format a="{{.a}}"`, `| label_format a = b`,
		`| label_format  a="{{ .b }}" ,  c = d `, `| label_format k8s_pod_name=pod`, `| label_format k8s_pod_name="{{.pod}}"`, `| label_format pod=k8s_pod_name`,
		`| label_format app=x`, `| label_format x=app`, `| label_format x="{{.app}}"`, `| label_format service_name=x`, `| label_format level="{{.lvl}}"`,
		`| label_format detected_level="x"`, `| label_format a="{{.b}}", a="{{.c}}"`, `| label_format a="{{.b | upper}}"`, `| label_format a="{{.b}} <c>"`,
		`| label_format a="{{- .b -}}"`, `| label_format a="x,y"`, "| label_format a=`x,\"y`", `| label_format a="x=y"`, `| label_format a="{{.b}}=1"`,
		`| label_format a="é"`, `| label_format a="\x41{{.b}}"`, `| label_format a_b=c_d`, `| label_format a.b=c`, `| label_format a_extracted=b`,
		`| label_format of=x`, `| label_format Of=x`, `| label_format OF="{{.b}}"`, `| label_format x=Json`, `| label_format Keep=x`, `| label_format Ip=x`, `| label_format Rate=a`,
		`| label_format __error__=""`, `| label_format a="{{.b}}", c=d, e="{{.f}} {{.g}}"`)
	selectors := []string{`{app="a"}`, `{app="a", env=~"prod|stg"}`, `{detected_level="error"}`, `{service_name="x", pod!="y"}`}
	for _, sel := range selectors {
		for _, f := range formats {
			add(sel + " " + f)
		}
	}
	sel := selectors[0]
	lines := []string{`|= "x"`, `!= "y"`, `|~ "z.+"`, `!~ "w"`, `|> "<_> x <_> y"`, `!> "<_> a <_> b"`, `|= "k" or "j"`}
	labels := []string{
		`| a="5"`, `| b!="x"`, `| a=~"x|y" and b="2"`, `| k8s_pod_name="p"`, `| status_code>=500`, `| app="a"`, `| pod="p"`, `| level="e"`, `| detected_level="e"`,
		`| service_name="x"`, `| a_extracted="x"`, `| a=""`, `| a!=""`, `| a=5`, `| a_b="1"`,
	}
	others := []string{
		`| json`, `| logfmt --strict`, `| json a="b.c"`, `| unpack`, "| regexp `(?P<z>\\d+)`", "| pattern `<z> <_>`", `| decolorize`, `| drop x`, `| drop x="y"`, `| keep k`,
		`| keep a, k8s_pod_name`, `| drop level`, `| line_format "{{.z}}"`, `| line_format "{{.z | upper}}"`, `| label_format z=y`, `| label_format z="{{.y}}"`,
	}
	labels = append(labels, `| z="1"`, `| b="1"`, `| c="1"`, `| k8s_pod_name="p" | pod="q"`, `| a_b="1"`, `| a_b=~"x" or c_d="y"`)
	around := append(append(append([]string{}, lines...), labels...), others...)
	for _, f := range formats {
		for _, a := range around {
			add(sel + " " + f + " " + a)
			add(sel + " " + a + " " + f)
			add(sel + " " + a + " " + f + " " + a)
		}
	}
	// a parser and a filter on the names the formatting defines or reads, in both orders.
	parsers := []string{`| json`, `| logfmt`, `| json x="y.z"`, "| regexp `(?P<q>\\d+)`", "| pattern `<q> <_>`"}
	for _, f := range formats {
		for _, p := range parsers {
			for _, l := range labels {
				add(sel + " " + f + " " + p + " " + l)
				add(sel + " " + p + " " + f + " " + l)
			}
		}
	}
	pool := []string{formats[0], formats[1], formats[7], formats[12], formats[14], formats[34], `| json`, `| logfmt`, `| decolorize`, "| regexp `(?P<z>\\d+)`", `|= "x"`, `!= "y"`, `| a="5"`, `| k8s_pod_name="p"`, `| drop a`, `| keep a`}
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

// TestFormatGridShadow holds the lowering of line_format to the string translator on the generated grid under
// every profile: 0 mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestFormatGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := validQueries(formatGrid())
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
