package lower

import (
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// parsedLowering is parserLowering held to the string translator byte for byte, the text a flipped query serves.
func parsedLowering(t testing.TB, q string, prof parserProfile) (lowered, legacy, construct string, delegated, ok bool) {
	t.Helper()
	lowered, legacy, construct, _, delegated, ok = parserLowering(t, q, prof)
	return lowered, legacy, construct, delegated, ok
}

// A label filter after a json or logfmt parser lowers to the query the string translator writes, byte for byte,
// under every profile: the filter pipe of its own, the keys a sanitized label can be stored under, an alias a
// json stage defines rewritten to its key, the stream labels read from their scratch copies, and the name_extracted
// labels read from the second unpack of the keys.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersLabelFiltersAfterParser(t *testing.T) {
	queries := []string{
		`{app="a"} | json | status >= 500`,
		`{app="a"} | logfmt | level="error"`,
		`{app="a"} | json | a=~"x|y" or b!="z"`,
		`{app="a"} | json | addr=ip("10.0.0.0/8")`,
		`{app="a"} | json | http_status="5"`,
		`{app="a"} | json | http_status!=""`,
		`{app="a"} | json | http_status=""`,
		`{app="a"} | json | http_status=~"x*"`,
		`{app="a"} | json | http_status!~"x|y|"`,
		`{app="a"} | json | a_b_c_d_e_f_g_h="x"`,
		`{app="a"} | json | k8s_pod_name="p"`,
		`{app="a"} | json | k8s.pod.name="p"`,
		`{app="a"} | json | _x_y="p"`,
		`{app="a"} | json | __error__=""`,
		`{app="a"} | json | __error__!="" and __error_details__=~"x.*"`,
		`{app="a"} | logfmt --strict | status>=500 | level="e"`,
		`{app="a"} | json | status>=500 | status<600`,
		`{app="a"} | json | a="1" | a="2"`,
		`{app="a"} | json | a_b="1" and (c_d="2" or e_f!="3")`,
		`{app="a"} | json | a_b="1", c_d="2"`,
		`{app="a"} | json | a_b="1" c_d="2"`,
		`{app="a"} | json | (a_b="1")`,
		`{app="a"} | json |= "x" | a="1"`,
		`{app="a"} | json | a="1" |= "x" | b="2" != "y"`,
		`{app="a"} | json | a="1" |> "<_> x <_> y" | b_c="2"`,
		`{app="a"} | json | drop q | a_b="1"`,
		`{app="a"} | json | a_b="1" | keep a_b`,
		`{app="a"} | json | logfmt | a_b="1"`,
		`{app="a"} | a_b="1" | json | a_b="2"`,
		`{app="a"} | json | level="" | x_y!=""`,
		`{app="a"} | json | a=~".*"`,
		`{app="a"} | json | a=~"a.*"`,
		`{app="a"} | json | d>5s and b<=1KB`,
		`{app="a"} | json x="y.z" | x="e"`,
		`{app="a"} | json x="y.z" | x ="e"`,
		`{app="a"} | json x="y.z" | x="e" and y_z="r"`,
		`{app="a"} | json x="y.z", st="status" | st>=500`,
		`{app="a"} | json http_status="a.b" | http_status="1"`,
		`{app="a"} | json | pod_extracted="1"`,
		`{app="a"} | json | service_name_extracted!=""`,
		`{app="a"} | json | logfmt | a_extracted="1" |= "k" | b_extracted=~"p|q"`,
		`{app="a"} | json | a_extracted="1" | logfmt | a_extracted="2"`,
		`{app="a"} | json | pod="p"`,
		`{app="a"} | pod="p" | json | pod!="q"`,
		`{app="a"} | json | logfmt | env=~"p|q" |= "k"`,
		`{app="a"} | json | drop pod | pod="p"`,
		`{app="a"} | json | pod=ip("10.0.0.0/8")`,
		`{app="a", env=~"prod|stg"} | json | status>=500`,
		`{service_name="x", pod!="y"} | logfmt | a_b="1"`,
	}
	for _, prof := range parserProfiles() {
		for _, q := range queries {
			lowered, legacy, construct, delegated, ok := parsedLowering(t, q, prof)
			if !ok {
				continue
			}
			if delegated {
				// A stream-labels profile leaves a service_name filter (derived) to the string translator.
				if strings.Contains(q, "service_name") && (construct == "LogSelect label filter service_name" || strings.HasPrefix(construct, "LogSelect level filter")) {
					continue
				}
				t.Errorf("%s: %s: not lowered (%s)", prof.name, q, construct)
				continue
			}
			if lowered != legacy {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	}
}

// What the lowering leaves to the string translator after a parser, and why.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesLabelFiltersAfterParser(t *testing.T) {
	for q, construct := range map[string]string{
		// translator defects: numeric equality is written as the string form.
		`{app="a"} | json | status=5`:      "LogSelect label filter numeric equality after parser",
		`{app="a"} | json | status==5`:     "LogSelect label filter numeric equality after parser",
		`{app="a"} | json | d!=5s`:         "LogSelect label filter numeric equality after parser",
		`{app="a"} | logfmt | b=1KB`:       "LogSelect label filter numeric equality after parser",
		`{app="a"} | json | a="1" and b=2`: "LogSelect label filter numeric equality after parser",
		// the string translator reads the stage text.
		`{app="a"} | json a_b="a_b" | a_b="1"`:      "LogSelect label filter json extraction label",
		`{app="a"} | json a_b | a_b="1"`:            "LogSelect label filter json extraction label",
		`{app="a"} | json x="service_name" | x="1"`: "LogSelect label filter json extraction label",
		`{app="a"} | json x="a b" | x="1"`:          "LogSelect label filter json extraction label",
		`{app="a"} | json | extract ="x"`:           "LogSelect label filter extract name",
		`{app="a"} | extract >5`:                    "LogSelect label filter extract name",
		// the parsers' text is read as naming the labels they extract, flags included.
		`{app="a"} | logfmt --strict | a_extracted="1"`:            "LogSelect label filter _extracted with parser arguments",
		`{app="a"} | json a | a_extracted="1"`:                     "LogSelect label filter _extracted with parser arguments",
		`{app="a"} | json | logfmt --keep-empty | a_extracted="1"`: "LogSelect label filter _extracted with parser arguments",
		// derived labels.
		`{app="a"} | json | detected_level="e"`: levelAfterStageConstruct,
		`{app="a"} | json | service_name="s"`:   "LogSelect label filter service_name",
	} {
		lowered, _, construct2, delegated, ok := parsedLowering(t, q, parserProfiles()[0])
		if !ok {
			if construct != "" {
				t.Errorf("%s: no parse", q)
			}
			continue
		}
		switch {
		case construct == "" && delegated:
			t.Errorf("%s: not lowered (%s), want lowered: %s", q, construct2, lowered)
		case construct != "" && (!delegated || construct2 != construct):
			t.Errorf("%s: got %q (delegated %v), want not lowered: %s", q, construct2, delegated, construct)
		}
	}
}

// The builders of the keys a label filter after a parser names are the string translator's.
//
// conformance: architecture-syntax-package-layering
func TestParsedKeyBuilders(t *testing.T) {
	for label, want := range map[string]string{
		"plain":           "plain",
		"a_b":             "a_b a.b a-b",
		"a_b_c":           "a_b_c a.b_c a_b.c a.b.c a-b-c",
		"a_":              "a_ a-",
		"a_b_c_d_e_f_g_h": "a_b_c_d_e_f_g_h a.b.c.d.e.f.g.h a-b-c-d-e-f-g-h",
	} {
		if got := strings.Join(translate.ParsedKeyVariants(label), " "); got != want {
			t.Errorf("ParsedKeyVariants(%q) = %q, want %q", label, got, want)
		}
	}
	for _, c := range []struct {
		label, translated string
		keys              bool
	}{
		{"a_b", "a_b", true}, {"a_b", "a.b", false}, {"ab", "ab", false}, {"_a_b", "_a_b", false}, {"a.b_c", "a.b_c", false},
		{"a-b_c", "a-b_c", false}, {"", "", false}, {"a_b\x00c", "a_b\x00c", false}, {"a_b\x01c", "a_b\x01c", false},
	} {
		if got := translate.ParsedKeysOf(c.label, c.translated); (got != nil) != c.keys {
			t.Errorf("ParsedKeysOf(%q, %q) = %v, want keys %v", c.label, c.translated, got, c.keys)
		}
	}
	re, _ := translate.LookupLabelFilterOp("=~")
	nre, _ := translate.LookupLabelFilterOp("!~")
	eq, _ := translate.LookupLabelFilterOp("=")
	ne, _ := translate.LookupLabelFilterOp("!=")
	gt, _ := translate.LookupLabelFilterOp(">")
	for _, c := range []struct {
		value string
		op    translate.LabelFilterOp
		ip    bool
		want  bool
	}{
		{"", eq, false, true}, {"x", eq, false, false}, {"", ne, false, false}, {"x", ne, false, true},
		{"x*", re, false, true}, {"x+", re, false, false}, {"x*", nre, false, false}, {"x+", nre, false, true},
		{"(", re, false, false}, {"(", nre, false, true}, {"5", gt, false, false}, {"10.0.0.0/8", eq, true, false}, {"10.0.0.0/8", ne, true, false},
	} {
		if got := translate.ParsedKeyMatchesEmpty(c.value, c.op, c.ip); got != c.want {
			t.Errorf("ParsedKeyMatchesEmpty(%q, %+v, %v) = %v, want %v", c.value, c.op, c.ip, got, c.want)
		}
	}
	if op, ok := translate.LookupLabelFilterOp("<>"); ok {
		t.Errorf("LookupLabelFilterOp of an unknown operator = %+v", op)
	}
	if base, ok := translate.ExtractedBase("a_extracted"); !ok || base != "a" {
		t.Errorf("ExtractedBase(a_extracted) = %q, %v", base, ok)
	}
	for _, label := range []string{"_extracted", "extracted", "a.b_extracted", "a_extracted_", "a-b_extracted", ""} {
		if base, ok := translate.ExtractedBase(label); ok && label != "a_extracted_" {
			t.Errorf("ExtractedBase(%q) = %q, want none", label, base)
		}
	}
}

// FuzzLabelFilterAfterParserLowering holds the lowering of label filters after a json or logfmt parser to the string
// translator, byte for byte, on any names, operators, values, trees, parsers and surrounding stages, under profiles
// with and without stream labels: both write the same query, or the lowering leaves the query to the translator. A
// value the translator cuts (translatorReadsStageMarker) is left out; the seed control-chars-in-label-value is one.
//
// conformance: architecture-syntax-package-layering
func FuzzLabelFilterAfterParserLowering(f *testing.F) {
	f.Add("status", "error", "http_status", "x|y", uint8(0), uint8(0), uint8(0))
	f.Add("a_extracted", "", "pod", ".*", uint8(7), uint8(3), uint8(5))
	f.Add("k8s_pod_name", "10.0.0.0/8", "a.b", "é\n\\d", uint8(200), uint8(9), uint8(255))
	profiles := parserProfiles()
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string, ops, shape, which uint8) {
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 {
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
		leaf := func(name, v string, op, bit uint8) string {
			name = ident(name)
			switch op % 14 {
			case 0:
				return name + "=" + quote(v, bit)
			case 1:
				return name + "!=" + quote(v, bit)
			case 2:
				return name + "=~" + quote(v, bit)
			case 3:
				return name + "!~" + quote(v, bit)
			case 4:
				return name + ">=" + strconv.Itoa(len(v))
			case 5:
				return name + " < " + strconv.Itoa(len(v)) + "s"
			case 6:
				return name + ">" + strconv.Itoa(len(v)) + "KB"
			case 7:
				return name + "==" + strconv.Itoa(len(v))
			case 8:
				return name + "=ip(" + strconv.Quote(v) + ")"
			case 9:
				return name + `!=ip("10.0.0.0/8")`
			case 10:
				return name + `=""`
			case 11:
				return name + `!=""`
			case 12:
				return name + "_extracted" + `="x"`
			}
			return name + "_extracted" + `!~"p|q"`
		}
		parsers := []string{`| json`, `| logfmt`, `| json | logfmt`, `| json ` + ident(n2) + `="` + ident(v2) + `"`, `| logfmt --strict`, `| json a`}
		l1, l2 := leaf(n1, v1, ops, shape>>4), leaf(n2, v2, ops>>4, shape>>5)
		var stage string
		switch shape % 6 {
		case 0:
			stage = l1
		case 1:
			stage = l1 + " and " + l2
		case 2:
			stage = l1 + " or " + l2
		case 3:
			stage = "(" + l1 + ", " + l2 + ") or " + l1
		case 4:
			stage = l1 + " | " + l2
		default:
			stage = l1 + ` |= "k" | ` + l2
		}
		pre := ""
		if which%4 == 0 {
			pre = ` | ` + leaf(n2, v2, ops>>2, 0)
		}
		q := `{app="a"}` + pre + ` ` + parsers[int(which>>2)%len(parsers)] + ` | ` + stage
		if which%5 == 0 {
			q += ` | drop q`
		}
		if translatorReadsStageMarker(q) {
			return // the translator cuts the value (semantics/translator-reads-marker-bytes-in-quoted-value)
		}
		for _, prof := range profiles {
			if lowered, legacy, _, delegated, ok := parsedLowering(t, q, prof); ok && !delegated && lowered != legacy {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
