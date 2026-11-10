package lower

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// parserProfile is a profile the lowering and the string translator are both run under.
type parserProfile struct {
	name   string
	label  func(string) string
	stream map[string]bool
	caps   logsql.Capabilities
	keep   bool
}

func parserProfiles() []parserProfile {
	under := func(s string) string { return strings.ReplaceAll(s, ".", "_") }
	dots := func(s string) string { return strings.ReplaceAll(s, "_", ".") }
	streamLabels := translator.WithStreamLabels(under, []string{"app", "namespace", "pod", "service_name"})
	fields := map[string]bool{"app": true, "namespace": true, "env": true, "level": true}
	v53, v49 := logsql.CapabilitiesFor("v1.53.0"), logsql.CapabilitiesFor("v1.49.0")
	noScan := v53
	noScan.NoDetectedLevelBodyScan = true
	return []parserProfile{
		{name: "none", caps: v53},
		{name: "underscores", label: under, caps: v53},
		{name: "underscores v1.49", label: under, caps: v49},
		{name: "dots", label: dots, caps: v53},
		{name: "keepLine", label: under, caps: v53, keep: true},
		{name: "stream labels", label: streamLabels, caps: v53},
		{name: "stream fields", label: under, stream: fields, caps: v53},
		{name: "stream fields and labels v1.49", label: streamLabels, stream: fields, caps: v49},
		{name: "no body scan", label: under, caps: noScan, stream: fields, keep: true},
	}
}

// parserLowering lowers q under the profile and compares it with the string translator, both read by the LogsQL
// parser. delegated is true when the lowering answers ErrNotLowered (construct names it); ok is false when q does
// not parse or neither side writes it.
func parserLowering(t testing.TB, q string, prof parserProfile) (lowered, legacy, construct string, same, delegated, ok bool) {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		return "", "", "", false, false, false
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return "", "", "", false, false, false
	}
	queryplan.Resolve(p)
	var terr error
	if prof.keep {
		legacy, terr = translator.TranslateLogQueryKeepingLine(q, prof.label, prof.stream, prof.caps)
	} else {
		legacy, terr = translator.TranslateLogQLWithCapabilities(q, prof.label, prof.stream, prof.caps)
	}
	op, lerr := Plan(p, Profile{Caps: prof.caps, Label: prof.label, StreamFields: prof.stream, KeepLine: prof.keep})
	var nl *NotLoweredError
	if errors.As(lerr, &nl) {
		return "", legacy, nl.Construct, false, true, terr == nil
	}
	if terr != nil || lerr != nil {
		if (terr == nil) != (lerr == nil) {
			t.Fatalf("%s: translator error %v, lowering error %v", q, terr, lerr)
		}
		return "", "", "", false, false, false
	}
	lowered = op.(physical.VLQuery).Text
	want, err := logsql.Parse(legacy)
	if err != nil {
		t.Fatalf("%s: the translator's %s does not parse: %v", q, legacy, err)
	}
	got, err := logsql.Parse(lowered)
	if err != nil {
		t.Fatalf("%s: the lowered %s does not parse: %v", q, lowered, err)
	}
	return lowered, legacy, "", reflect.DeepEqual(want, got), false, true
}

// The json and logfmt parsers, drop and keep lower to the query the string translator writes, under every
// profile: bare parsers, the flags and extraction lists, the labels a json expression defines, drop and keep of
// names and of matchers, the stored level fields, the _extracted names, and the filters among them.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersParsers(t *testing.T) {
	queries := []string{
		`{app="a"} | json`,
		`{app="a"} | logfmt`,
		`{app="a"} | json a`,
		`{app="a"} | json a, b`,
		`{app="a"} | json a="b"`,
		`{app="a"} | json a="b.c"`,
		`{app="a"} | json a="b.c", d="e"`,
		`{app="a"} | json z="1", a="2", m="3"`,
		`{app="a"} | json a="a"`,
		`{app="a"} | json a=""`,
		`{app="a"} | json k8s.pod.name="x"`,
		`{app="a"} | json a-b="x"`,
		`{app="a"} | json | json a="b.c"`,
		`{app="a"} | logfmt --strict`,
		`{app="a"} | logfmt --keep-empty`,
		`{app="a"} | logfmt --strict --keep-empty`,
		`{app="a"} | logfmt --keep-empty --strict`,
		`{app="a"} | logfmt a, b`,
		`{app="a"} | logfmt a="b"`,
		`{app="a"} | logfmt --strict a="b.c", d`,
		`{app="a"} | json | logfmt`,
		`{app="a"} | drop a`,
		`{app="a"} | drop a, b`,
		`{app="a"} | drop level`,
		`{app="a"} | drop level, detected_level`,
		`{app="a"} | drop detected_level`,
		`{app="a"} | drop LEVEL, x`,
		`{app="a"} | drop log.level`,
		`{app="a"} | drop __error__, __error_details__`,
		`{app="a"} | drop a="x"`,
		`{app="a"} | drop a=~"x.*"`,
		`{app="a"} | drop a!="x"`,
		`{app="a"} | drop a!~"y"`,
		`{app="a"} | drop a, b="x"`,
		`{app="a"} | drop b="x", a`,
		`{app="a"} | drop a="x,y"`,
		`{app="a"} | drop k8s_pod_name`,
		`{app="a"} | json | drop a`,
		`{app="a"} | json | drop a="x" | logfmt`,
		`{app="a"} | keep a`,
		`{app="a"} | keep a, b`,
		`{app="a"} | keep level`,
		`{app="a"} | keep detected_level`,
		`{app="a"} | keep detected_level, level`,
		`{app="a"} | keep a_extracted`,
		`{app="a"} | keep a_extracted, a`,
		`{app="a"} | keep _extracted`,
		`{app="a"} | keep a="x"`,
		`{app="a"} | keep a="x", b=~"y"`,
		`{app="a"} | keep k8s_pod_name`,
		`{app="a"} | keep service_name`,
		`{app="a"} | keep deployment_environment`,
		`{app="a"} | keep a, a`,
		`{app="a"} | json | keep a, b`,
		`{app="a"} | json a="b.c" | drop level, x | keep k, level`,
		`{app="a"} |= "x" | json`,
		`{app="a"} | json |= "x"`,
		`{app="a"} | json != "x" |~ "y.+"`,
		`{app="a"} |= "x" | json |= "y" | drop a |~ "z"`,
		`{app="a"} | logfmt != "x"`,
		`{app="a"} | drop a != "x"`,
		`{app="a"} | keep a !~ "x"`,
		`{app="a"} |> "<_> x <_> y" | json`,
		`{app="a"} | json |> "<_> x <_> y" |= "k"`,
		`{app="a"} |> "<_> x <_> y" | logfmt |> "<_> m <_> n" | drop a`,
		`{app="a"} | status="5" | json`,
		`{app="a"} | status>=500 | logfmt --strict | drop a`,
		`{app="a"} | a="1" and b!="2" | json |= "x"`,
		`{app="a"} | drop x | status="5"`,
		`{app="a"} | keep x | status="5" | a=""`,
		`{app="a"} | json a="b.c" | drop x |= "k"`,
		`{app="a"} | drop x | level="e" | json | keep y`,
		`{app="a", env=~"prod|stg"} | json | drop a`,
		`{detected_level="error"} | logfmt`,
		`{service_name="x", pod!="y"} | json a="b" | keep c`,
	}
	for _, prof := range parserProfiles() {
		for _, q := range queries {
			lowered, legacy, construct, same, delegated, ok := parserLowering(t, q, prof)
			if !ok {
				continue
			}
			if delegated {
				// A stream-labels profile leaves the queries that name a stream label to the string translator.
				if prof.label != nil && translate.StreamLabelNames(prof.label) != nil && construct == "LogSelect stream label after parser" {
					continue
				}
				t.Errorf("%s: %s: not lowered (%s)", prof.name, q, construct)
				continue
			}
			if !same {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	}
}

// What the lowering leaves to the string translator, and why: the stages it does not write, a label filter after a
// parser, and the spellings the translator reads from the text differently from the typed stage.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesParserShapes(t *testing.T) {
	for q, construct := range map[string]string{
		`{app="a"} | json a_b="a_b" | a_b="1"`:        "LogSelect label filter json extraction label",
		`{app="a"} | json a="service_name" | a="1"`:   "LogSelect label filter json extraction label",
		`{app="a"} | json | a_b=5`:                    "LogSelect label filter numeric equality after parser",
		`{app="a"} | json | extract ="x"`:             "LogSelect label filter extract name",
		"{app=\"a\"} | regexp `(?P<a>\\d+)` != \"y\"": "LogSelect line filter after parser arguments",
		"{app=\"a\"} | pattern `<a> <b>` !~ \"y\"":    "LogSelect line filter after parser arguments",
		`{app="a"} | unpack`:                          "LogSelect unpack parser",
		`{app="a"} | json | line_format "{{.a}}"`:     "LogSelect pipeline",
		`{app="a"} | label_format a=b`:                "LogSelect pipeline",
		`{app="a"} | json a="b\"c"`:                   "LogSelect json extraction",
		"{app=\"a\"} | json a=`raw`":                  "LogSelect json extraction",
		`{app="a"} | json a="b,c"`:                    "LogSelect json extraction",
		`{app="a"} | json a=" b"`:                     "LogSelect json extraction",
		`{app="a"} | json a="[0]"`:                    "LogSelect json extraction",
		`{app="a"} | json a="x", a="y"`:               "LogSelect json extraction",
		`{app="a"} | json a="b\\c"`:                   "LogSelect json extraction",
		`{app="a"} | json a="b" != "y"`:               "LogSelect line filter after parser arguments",
		`{app="a"} | logfmt --strict !~ "y"`:          "LogSelect line filter after parser arguments",
		`{app="a"} | drop a="x" != "y"`:               "LogSelect line filter after parser arguments",
		`{app="a"} | keep a="x" !~ "y"`:               "LogSelect line filter after parser arguments",
		`{app="a"} | drop a="x\"y"`:                   "LogSelect drop matcher value",
		`{app="a"} | keep a="x=~y"`:                   "LogSelect keep matcher value",
		`{app="a"} | keep a="x!y"`:                    "LogSelect keep matcher value",
		`{app="a"} | keep a, b="x"`:                   "LogSelect keep order",
		`{app="a"} | keep b="x", a`:                   "LogSelect keep order",
		`{app="a"} | detected_level="e" | json`:       "LogSelect label filter detected_level",
		`{app="a"} | json | detected_level="e"`:       "LogSelect label filter detected_level",
	} {
		expr, err := logql.Parse(q)
		if err != nil {
			continue // a spelling logql rejects
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queryplan.Resolve(p)
		_, err = Plan(p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}

// A stream-labels profile leaves a query with a parser to the string translator when a stage other than a filter
// on one label names a stream label (the translator copies its stored value aside, and the lowering writes that
// for a label filter alone), or when a string holds a template read of one. A stage that names none is lowered.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesStreamLabelsAfterParser(t *testing.T) {
	under := func(s string) string { return strings.ReplaceAll(s, ".", "_") }
	label := translator.WithStreamLabels(under, []string{"app", "pod"})
	for q, want := range map[string]string{
		`{app="a"} | json`:                        "",
		`{app="a"} | json | drop x`:               "",
		`{app="a"} | drop pod`:                    "",
		`{app="a"} | json pod="x"`:                "LogSelect stream label after parser",
		`{app="a"} | logfmt app`:                  "LogSelect stream label after parser",
		`{app="a"} | pod="x" | json`:              "",
		`{app="a"} | json | pod="x"`:              "",
		`{app="a"} | app="x" or b="y" | logfmt`:   "LogSelect stream label in label filter tree",
		`{app="a"} |= "{{.pod}}" | json`:          "LogSelect stream label after parser",
		`{app="a"} | json | x="{{ .app }}"`:       "LogSelect stream label after parser",
		`{app="a"} | drop pod="x" | json`:         "LogSelect stream label after parser",
		`{app="a"} | keep app="x" | logfmt`:       "LogSelect stream label after parser",
		`{app="a"} | other="x" | json | keep app`: "",
	} {
		expr, err := logql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		p, _ := queryplan.Build(expr, queryplan.Inputs{})
		queryplan.Resolve(p)
		_, err = Plan(p, Profile{Label: label})
		var nl *NotLoweredError
		switch {
		case want == "" && err != nil:
			t.Errorf("%s: %v, want lowered", q, err)
		case want != "" && (!errors.As(err, &nl) || nl.Construct != want):
			t.Errorf("%s: got %v, want not lowered: %s", q, err, want)
		}
	}
}

// The values the builders hold are the string translator's: the stored level fields a log query's drop keeps, and
// the probe through which a profile carries the stream label names.
//
// conformance: architecture-syntax-package-layering
func TestParserBuildersMatchTranslator(t *testing.T) {
	if !slices.Equal(translate.LevelSourceFields(), translator.DetectedLevelFieldNames[1:]) {
		t.Errorf("level source fields %v, translator %v", translate.LevelSourceFields(), translator.DetectedLevelFieldNames[1:])
	}
	if translate.DetectedLevelLabel != translator.DetectedLevelFieldNames[0] {
		t.Errorf("detected level label %q", translate.DetectedLevelLabel)
	}
	names := []string{"app", "namespace", "service_name"}
	if got := translate.StreamLabelNames(translator.WithStreamLabels(nil, names)); !slices.Equal(got, names) {
		t.Errorf("stream label names %v, want %v", got, names)
	}
	if got := translate.StreamLabelNames(translator.WithStreamLabels(func(s string) string { return s }, nil)); got != nil {
		t.Errorf("a function carrying none gave %v", got)
	}
	if translate.StreamLabelNames(nil) != nil || translate.StreamLabelNames(func(s string) string { return s }) != nil {
		t.Error("a plain function carries stream labels")
	}
}

// The builders themselves: the copies a json expression defines, the delete a drop writes and the fields a keep
// projects.
//
// conformance: architecture-syntax-package-layering
func TestParserBuilders(t *testing.T) {
	fields := func(kv ...string) []logql.ExtractionField {
		var out []logql.ExtractionField
		for i := 0; i < len(kv); i += 2 {
			out = append(out, logql.ExtractionField{Name: kv[i], Expression: kv[i+1]})
		}
		return out
	}
	text := func(ps []logsql.Pipe) string {
		var b []string
		for _, p := range ps {
			b = append(b, p.String())
		}
		return strings.Join(b, " ")
	}
	for _, c := range []struct {
		fields []logql.ExtractionField
		param  string
		want   string
		ok     bool
	}{
		{nil, "", "", true},
		{fields("a", "a"), "a", "", true},
		{fields("a", "b.c"), `a="b.c"`, `| copy "b.c" as a`, true},
		{fields("z", "1", "a", "2"), `z="1", a="2"`, `| copy "2" as a | copy "1" as z`, true},
		{fields("k8s.pod.name", "x"), `k8s.pod.name="x"`, `| copy "x" as "k8s.pod.name"`, true},
		{fields("a-b", "x"), `a-b="x"`, `| copy "x" as "a-b"`, true},
		{fields("a", ""), `a=""`, "", true},
		{fields("a", "b,c"), `a="b,c"`, "", false},
		{fields("a", " b"), `a=" b"`, "", false},
		{fields("a", "b "), `a="b "`, "", false},
		{fields("a", "[0]"), `a="[0]"`, "", false},
		{fields("a", "b"), "a=`b`", "", false},
		{fields("a", "b\"c"), `a="b\"c"`, "", false},
		{fields("a", "x", "a", "y"), `a="x", a="y"`, "", false},
		{fields("a", "x"), "a=\"x\"\n", "", false},
	} {
		got, ok := translate.JSONAliasCopies(c.fields, c.param)
		if ok != c.ok || (ok && text(got) != c.want) {
			t.Errorf("JSONAliasCopies(%v, %q) = %q, %v; want %q, %v", c.fields, c.param, text(got), ok, c.want, c.ok)
		}
	}
	if p, ok := translate.DropDelete([]string{"a", "level", "b", "detected_level"}); !ok || p.String() != "| delete a, b, detected_level" {
		t.Errorf("DropDelete = %v, %v", p, ok)
	}
	if p, ok := translate.DropDelete([]string{"level", "severity_number"}); ok {
		t.Errorf("DropDelete of the level fields wrote %v", p)
	}
	if _, ok := translate.DropDelete(nil); ok {
		t.Error("DropDelete of nothing wrote a pipe")
	}
	label := func(s string) string { return strings.ReplaceAll(s, "_", ".") }
	for _, c := range []struct {
		names []string
		label func(string) string
		want  string
	}{
		{[]string{"a"}, nil, "| fields _time, _msg, _stream, a"},
		{[]string{"a_extracted"}, nil, "| fields _time, _msg, _stream, a_extracted, a"},
		{[]string{"a_extracted", "a"}, nil, "| fields _time, _msg, _stream, a_extracted, a"},
		{[]string{"_extracted"}, nil, "| fields _time, _msg, _stream, _extracted"},
		{[]string{"detected_level"}, nil, "| fields _time, _msg, _stream, detected_level, level"},
		{[]string{"detected_level", "level"}, nil, "| fields _time, _msg, _stream, detected_level, level"},
		{[]string{"k8s_pod", "service_name"}, label, "| fields _time, _msg, _stream, k8s_pod, service_name, k8s.pod"},
		{nil, nil, "| fields _time, _msg, _stream"},
	} {
		if got := translate.KeepFields(c.names, c.label).String(); got != c.want {
			t.Errorf("KeepFields(%v) = %s, want %s", c.names, got, c.want)
		}
	}
}

// FuzzParserLowering holds the lowering of json, logfmt, drop and keep to the string translator on any names,
// values and orders of stages, under profiles with and without stream labels: both write the same query, or the
// lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzParserLowering(f *testing.F) {
	f.Add("a", "b.c", "level", "x", uint8(0), uint8(0), uint8(0))
	f.Add("app", "k8s.pod", "detected_level", "é\n", uint8(77), uint8(200), uint8(9))
	f.Add("x_extracted", "[0]", "pod", "a,b", uint8(255), uint8(3), uint8(31))
	profiles := parserProfiles()
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string, stages, shape, which uint8) {
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r == '.' && b.Len() > 0 || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 || b.String() == "bool" || b.String() == "without" {
				return "x"
			}
			return strings.TrimRight(b.String(), ".")
		}
		quote := func(v string, bit uint8) string {
			if bit != 0 && !strings.Contains(v, "`") {
				return "`" + v + "`"
			}
			return strconv.Quote(v)
		}
		stage := func(sel uint8, n, v string, bit uint8) string {
			n = ident(n)
			switch sel % 14 {
			case 0:
				return "| json"
			case 1:
				return "| json " + n
			case 2:
				return "| json " + n + "=" + quote(v, bit)
			case 3:
				return "| json " + n + "=" + quote(v, bit) + ", " + ident(v) + "=" + quote(n, bit)
			case 4:
				return "| logfmt"
			case 5:
				return "| logfmt --strict " + n
			case 6:
				return "| logfmt --keep-empty --strict " + n + "=" + quote(v, bit)
			case 7:
				return "| drop " + n
			case 8:
				return "| drop " + n + ", " + ident(v)
			case 9:
				return "| drop " + n + "=" + quote(v, bit)
			case 10:
				return "| drop " + ident(v) + ", " + n + "=~" + quote(v, bit)
			case 11:
				return "| keep " + n + ", " + ident(v)
			case 12:
				return "| keep " + n + "=" + quote(v, bit)
			}
			return "| keep " + n + "_extracted"
		}
		lines := []string{``, ` |= "k"`, ` != "j"`, ` !~ ` + quote(v2, shape), ` |> "<_> x <_> y"`, ` | ` + ident(n2) + `="1"`}
		q := `{app="a"} ` + stage(stages, n1, v1, shape>>7) + lines[int(shape)%len(lines)] + ` ` + stage(stages>>4, n2, v2, shape>>6&1) + lines[int(which)%len(lines)]
		if which%3 == 0 {
			q += ` ` + stage(which>>2, n1, v2, shape>>5&1)
		}
		for _, prof := range profiles {
			if lowered, legacy, _, same, delegated, ok := parserLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
