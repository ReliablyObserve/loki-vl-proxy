package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The regexp and pattern parsers and decolorize lower to the query the string translator writes, under every
// profile: the captured names, the wildcard-only patterns that write no pipe, the labels read after them (a name
// a regexp captured is not translated, a label a parser defines is not a key to look up), and the filters among
// them. (A pattern without a named capture and a regexp without a named group are rejected by LogQL validation
// before any route translates them, so a wildcard-only pattern only reaches the lowering from a test.)
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersExtractStages(t *testing.T) {
	queries := []string{
		"{app=\"a\"} | regexp `(?P<a>\\d+)`",
		"{app=\"a\"} | regexp `(?P<a>\\d+) (?P<b>\\w+)`",
		`{app="a"} | regexp "(?P<a>\\d+)"`,
		"{app=\"a\"} | regexp `(?P<k8s_pod_name>\\S+)` | k8s_pod_name=\"p\"",
		"{app=\"a\"} | regexp `(?P<k8s_pod_name>\\S+)` | keep k8s_pod_name",
		"{app=\"a\"} | pattern `<a> <_> <b>`",
		"{app=\"a\"} | pattern `<a> <b>` | a=\"5\" |= \"x\"",
		"{app=\"a\"} | pattern `<k8s_pod_name> <_>` | json | k8s_pod_name=\"p\"",
		"{app=\"a\"} | json | pattern `<status_code> <_>` | status_code=\"5\"",
		"{app=\"a\"} | regexp `(?P<a>\\d+)` | logfmt | a_b=\"1\" | drop a",
		"{app=\"a\"} | decolorize",
		"{app=\"a\"} |= \"x\" | decolorize |= \"y\" | a=\"1\"",
		"{app=\"a\"} | decolorize | json | status>=500",
		"{app=\"a\"} | regexp `(?P<a>\\d+)` |~ \"x\" |> \"<_> y\"",
		"{app=\"a\"} | pattern `<a> <b>` | pattern `<c>` | regexp `(?P<d>x)` | decolorize",
	}
	for _, prof := range parserProfiles() {
		for _, q := range queries {
			if logql.ParseAndValidate(q) != nil {
				t.Fatalf("%s: the query is rejected", q)
			}
			lowered, legacy, construct, same, delegated, ok := parserLowering(t, q, prof)
			if !ok {
				continue
			}
			if delegated {
				// A stream-labels profile leaves the queries that name a stream label to the string translator.
				if strings.HasPrefix(construct, "LogSelect stream label") || construct == "LogSelect no-op pattern with stream label" {
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

// What the lowering leaves to the string translator: a negative line filter right after a regexp or pattern (the
// translator reads the operator as part of the stage's argument and writes no filter), and a wildcard-only
// pattern in a query that reads a stream label (the translator copies the label aside without the fill that
// follows a parser's pipe).
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesExtractShapes(t *testing.T) {
	label := parserProfiles()[5].label
	for _, c := range []struct {
		q, construct string
		label        func(string) string
	}{
		{"{app=\"a\"} | regexp `(?P<a>x)` != \"y\"", "LogSelect line filter after parser arguments", nil},
		{"{app=\"a\"} | regexp `(?P<a>x)` !~ \"y\"", "LogSelect line filter after parser arguments", nil},
		{"{app=\"a\"} | pattern `<a>` != \"y\"", "LogSelect line filter after parser arguments", nil},
		{"{app=\"a\"} | pattern `<a>` !> \"<_> y\"", "LogSelect line filter after parser arguments", nil},
		{"{app=\"a\"} | pattern `(.*)` | app=\"x\"", "LogSelect no-op pattern with stream label", label},
	} {
		p := planOf(t, c.q)
		_, err := Plan(p, Profile{Label: c.label})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != c.construct {
			t.Errorf("%s: got %v, want not lowered: %s", c.q, err, c.construct)
		}
	}
	// A stream label named by a regexp group is read as the scratch copy, like any other parser.
	if _, err := Plan(planOf(t, "{app=\"a\"} | regexp `(?P<a>x)` | app=\"x\""), Profile{Label: label}); err != nil {
		t.Errorf("stream label after a regexp: %v", err)
	}
}

// FuzzExtractLowering holds the lowering of the regexp and pattern parsers and decolorize to the string
// translator on any group names, arguments and orders of stages, under profiles with and without stream labels:
// both write the same query, or the lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzExtractLowering(f *testing.F) {
	f.Add("a", "b", "k8s_pod_name", "x", uint8(0), uint8(0), uint8(0))
	f.Add("app", "(.*)", "pod", "é\n", uint8(77), uint8(200), uint8(9))
	f.Add("status_code", "<_>", "level", "a|b", uint8(255), uint8(3), uint8(31))
	profiles := parserProfiles()
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string, stages, shape, which uint8) {
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
		stage := func(sel uint8, n, v string, bit uint8) string {
			n = ident(n)
			switch sel % 9 {
			case 0:
				return "| regexp " + quote("(?P<"+n+">"+v+")", bit)
			case 1:
				return "| regexp " + quote(v, bit)
			case 2:
				return "| pattern " + quote("<"+n+"> "+v+" <_>", bit)
			case 3:
				return "| pattern " + quote(v, bit)
			case 4:
				return "| pattern " + quote("(.*)", bit)
			case 5:
				return "| decolorize"
			case 6:
				return "| json"
			case 7:
				return "| " + n + "=" + quote(v, bit)
			}
			return "| keep " + n
		}
		lines := []string{``, ` |= "k"`, ` != "j"`, ` !~ ` + quote(v2, shape), ` |> "<_> x <_> y"`, ` | ` + ident(n2) + `="1"`}
		q := `{app="a"} ` + stage(stages, n1, v1, shape>>7) + lines[int(shape)%len(lines)] + ` ` + stage(stages>>4, n2, v2, shape>>6&1) + lines[int(which)%len(lines)]
		if which%3 == 0 {
			q += ` ` + stage(which>>2, n1, v2, shape>>5&1)
		}
		if logql.ParseAndValidate(q) != nil {
			return // a query every route rejects before it translates anything
		}
		for _, prof := range profiles {
			if lowered, legacy, _, same, delegated, ok := parserLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
