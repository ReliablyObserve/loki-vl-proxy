package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// line_format and label_format lower to the query the string translator writes, under every profile (the keepLine
// profiles copy the stored line aside before the first line_format): templates of text and fields, renames, the
// labels they define read by the filters after them, and the stages among them.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersFormatStages(t *testing.T) {
	queries := []string{
		`{app="a"} | line_format "{{.a}}"`,
		`{app="a"} | line_format "{{ .a }} x {{.b}}"`,
		"{app=\"a\"} | line_format `{{.a}}`",
		`{app="a"} | line_format "plain"`,
		`{app="a"} | line_format ""`,
		`{app="a"} | line_format "{{.a}}" | line_format "{{.b}}"`,
		`{app="a"} | json | line_format "{{.a}}" |= "x"`,
		`{app="a"} |= "x" | line_format "{{.a}}" |~ "y" | a="1"`,
		`{app="a"} | logfmt | line_format "{{.k8s_pod_name}}"`,
		`{app="a"} | label_format a=b`,
		`{app="a"} | label_format a=b, c=d`,
		`{app="a"} | label_format a="{{.b}}"`,
		`{app="a"} | label_format a="{{.b}}-x", c=d`,
		"{app=\"a\"} | label_format a=`{{.b}}`",
		`{app="a"} | label_format a="x"`,
		`{app="a"} | label_format a=b | a="5"`,
		`{app="a"} | label_format a_b=c_d | json | a_b="1"`,
		`{app="a"} | json | label_format k8s_pod_name=pod | k8s_pod_name="p"`,
		`{app="a"} | label_format a="{{.b}}" | line_format "{{.a}}" | drop x`,
		`{app="a"} | regexp ` + "`(?P<z>\\d+)`" + ` | label_format a=z | keep a`,
	}
	for _, prof := range parserProfiles() {
		for _, q := range queries {
			lowered, legacy, construct, same, delegated, ok := parserLowering(t, q, prof)
			if !ok {
				continue
			}
			if delegated {
				if strings.HasPrefix(construct, "LogSelect stream label") {
					continue
				}
				t.Errorf("%s: %s: not lowered (%s)", prof.name, q, construct)
				continue
			}
			if !same || lowered != legacy {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	}
}

// What the lowering leaves to the string translator: the templates it copies into the query as they are (a
// function, a pipe, a condition, a dotted field, a trim marker, text with an angle bracket, which VictoriaLogs
// reads as a placeholder), a label_format rename onto itself (the translator deletes the label), a negative line
// filter right after a formatting stage, and the formatting of a stream label read after a parser.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesFormatShapes(t *testing.T) {
	streamLabels := parserProfiles()[5].label
	for _, c := range []struct {
		q, construct string
		label        func(string) string
	}{
		{`{app="a"} | line_format "{{.a | upper}}"`, "LogSelect line format function", nil},
		{`{app="a"} | line_format "{{upper .a}}"`, "LogSelect line format function", nil},
		{`{app="a"} | line_format "{{.a.b}}"`, "LogSelect line format function", nil},
		{`{app="a"} | line_format "{{if .a}}x{{end}}"`, "LogSelect line format function", nil},
		{`{app="a"} | line_format "{{- .a -}}"`, "LogSelect line format trim marker", nil},
		{`{app="a"} | line_format "x <b> {{.a}}"`, "LogSelect line format angle bracket", nil},
		{`{app="a"} | line_format "{{.a}}" != "y"`, "LogSelect line filter after parser arguments", nil},
		{`{app="a"} | label_format a="{{.b}}" !~ "y"`, "LogSelect line filter after parser arguments", nil},
		{`{app="a"} | label_format a="{{.b | upper}}"`, "LogSelect label format function", nil},
		{`{app="a"} | label_format a="{{.b}} <c>"`, "LogSelect label format angle bracket", nil},
		{`{app="a"} | label_format a="{{- .b -}}"`, "LogSelect label format trim marker", nil},
		{`{app="a"} | label_format a=a`, "LogSelect label format rename onto itself", nil},
		{`{app="a"} | json | line_format "{{.app}}"`, "LogSelect stream label after parser", streamLabels},
		{`{app="a"} | json | label_format x="{{.pod}}"`, "LogSelect stream label after parser", streamLabels},
		{`{app="a"} | line_format "{{.app}}" | json`, "LogSelect stream label after parser", streamLabels},
		{`{app="a"} | json | label_format x=pod`, "LogSelect stream label after parser", streamLabels},
		{`{app="a"} | json | label_format pod="x"`, "LogSelect stream label after parser", streamLabels},
	} {
		_, err := Plan(planOf(t, c.q), Profile{Label: c.label})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != c.construct {
			t.Errorf("%s: got %v, want not lowered: %s", c.q, err, c.construct)
		}
	}
	// Without a parser no stream label is copied aside, so the same formatting is lowered.
	for _, q := range []string{`{app="a"} | line_format "{{.app}}"`, `{app="a"} | label_format x=pod`} {
		if _, err := Plan(planOf(t, q), Profile{Label: streamLabels}); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// FuzzFormatLowering holds the lowering of line_format and label_format to the string translator on any
// templates, names and orders of stages, under profiles with and without stream labels and keepLine: both write
// the same query, or the lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzFormatLowering(f *testing.F) {
	f.Add("a", "b", "{{.a}} x", uint8(0), uint8(0), uint8(0))
	f.Add("app", "pod", "<b>{{.pod}}", uint8(77), uint8(200), uint8(9))
	f.Add("k8s_pod_name", "x", "{{ .a | upper }}", uint8(255), uint8(3), uint8(31))
	profiles := parserProfiles()
	f.Fuzz(func(t *testing.T, n1, n2, tmpl string, stages, shape, which uint8) {
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 || b.String() == "bool" || b.String() == "without" {
				return "x"
			}
			return b.String()
		}
		quote := func(v string, bit uint8) string {
			if bit != 0 && !strings.Contains(v, "`") {
				return "`" + v + "`"
			}
			return strconv.Quote(v)
		}
		stage := func(sel uint8, n, m string, bit uint8) string {
			n, m = ident(n), ident(m)
			switch sel % 11 {
			case 0:
				return "| line_format " + quote(tmpl, bit)
			case 1:
				return `| line_format "{{.` + n + `}} {{.` + m + `}}"`
			case 2:
				return "| label_format " + n + "=" + m
			case 3:
				return "| label_format " + n + "=" + quote(tmpl, bit)
			case 4:
				return "| label_format " + n + `="{{.` + m + `}}", ` + m + "=" + n
			case 5:
				return "| json"
			case 6:
				return "| " + n + `="1"`
			case 7:
				return "| " + n + `="1" | ` + m + `="2"`
			case 8:
				return "| keep k" // a keep of a name the scanner folds to lower case is a finding of the parser step
			case 9:
				return "| decolorize"
			}
			return "| regexp `(?P<" + n + ">x)`"
		}
		lines := []string{``, ` |= "k"`, ` != "j"`, ` !~ ` + quote(tmpl, shape), ` |> "<_> x <_> y"`, ` | ` + ident(n2) + `="1"`}
		q := `{app="a"} ` + stage(stages, n1, n2, shape>>7) + lines[int(shape)%len(lines)] + ` ` + stage(stages>>4, n2, n1, shape>>6&1) + lines[int(which)%len(lines)]
		if which%3 == 0 {
			q += ` ` + stage(which>>2, n1, n2, shape>>5&1)
		}
		if logql.ParseAndValidate(q) != nil {
			return
		}
		for _, prof := range profiles {
			if lowered, legacy, _, same, delegated, ok := parserLowering(t, q, prof); ok && !delegated && (!same || lowered != legacy) {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
