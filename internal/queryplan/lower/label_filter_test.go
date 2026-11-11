package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// labelFilterProfiles are the label functions the lowering is held to the string translator under: none, the
// dots of a name written as underscores (what the client of an underscore-style proxy sees), and the other way
// round (a stored dotted name, which LogsQL needs quoted).
func labelFilterProfiles() map[string]func(string) string {
	return map[string]func(string) string{
		"identity":    nil,
		"underscores": func(s string) string { return strings.ReplaceAll(s, ".", "_") },
		"dots":        func(s string) string { return strings.ReplaceAll(s, "_", ".") },
	}
}

// labelFilterLowering lowers q under label function fn and compares it with the string translator, byte for
// byte (the text a flipped query serves); both must also parse. delegated is true when the lowering answers ErrNotLowered; ok is false when q does not parse.
func labelFilterLowering(t testing.TB, q string, fn func(string) string) (lowered, legacy string, same, delegated, ok bool) {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		return "", "", false, false, false
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return "", "", false, false, false
	}
	queryplan.Resolve(p)
	legacy, terr := translator.TranslateLogQLWithLabels(q, fn)
	op, lerr := Plan(p, Profile{Label: fn})
	// A label called _time, _stream, _stream_id or _msg is the stream tag in the lowering and a field filter VictoriaLogs
	// rejects or misreads in the translator (semantics/translator-writes-reserved-field-names): not compared.
	if ReadsReservedField(p, Profile{Label: fn}) {
		return "", "", false, false, false
	}
	if errors.Is(lerr, ErrNotLowered) {
		return "", legacy, false, true, terr == nil
	}
	if terr != nil || lerr != nil {
		if (terr == nil) != (lerr == nil) {
			t.Fatalf("%s: translator error %v, lowering error %v", q, terr, lerr)
		}
		return "", "", false, false, false
	}
	lowered = op.(physical.VLQuery).Text
	legacy = QuoteKeywordFieldText(legacy)
	if _, err := logsql.Parse(legacy); err != nil {
		t.Fatalf("%s: the translator's %s does not parse: %v", q, legacy, err)
	}
	if _, err := logsql.Parse(lowered); err != nil {
		t.Fatalf("%s: the lowered %s does not parse: %v", q, lowered, err)
	}
	return lowered, legacy, lowered == legacy, false, true
}

// A label filter without a parser lowers to the query the string translator writes, for every shape its
// builder has: each operator and kind of value, the regexps Loki simplifies, empty values, ip() patterns, quoted
// names, and/or/comma trees, and filters among line filters, after the pipes of a pattern filter and in a row.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersLabelFilters(t *testing.T) {
	queries := []string{
		`{app="a"} | level="error"`,
		`{app="a"} | level!="error"`,
		`{app="a"} | status >= 500`,
		`{app="a"} | status<5`,
		`{app="a"} | d>5s`,
		`{app="a"} | d<=1m30s`,
		`{app="a"} | b>1KB`,
		`{app="a"} | b<20MiB`,
		`{app="a"} | n=5`,
		`{app="a"} | n==5`,
		`{app="a"} | n!=5.0`,
		`{app="a"} | a=~"x|y"`,
		`{app="a"} | a!~"x|y"`,
		`{app="a"} | a=~"error|warn|fatal"`,
		`{app="a"} | a=~".*"`,
		`{app="a"} | a=~"a.*"`,
		`{app="a"} | a=~"(?i)x"`,
		`{app="a"} | a=~"^a$"`,
		`{app="a"} | a=~"x{2}"`,
		`{app="a"} | a=""`,
		`{app="a"} | a!=""`,
		`{app="a"} | level=""`,
		`{app="a"} | level!=""`,
		`{app="a"} | a=~""`,
		`{app="a"} | a="1" and b!="2"`,
		`{app="a"} | a="1", b!="2"`,
		`{app="a"} | a="1" b!="2"`,
		`{app="a"} | a="1" or b="2" or c="3"`,
		`{app="a"} | (a="1" or b="2") and c="3"`,
		`{app="a"} | a="1" and (b="2" or c="3")`,
		`{app="a"} | (a="1" and b="2") or (c="3" and d="4")`,
		`{app="a"} | a="1", b="2" or c="3"`,
		`{app="a"} | (a="1")`,
		`{app="a"} | ip_a=ip("10.0.0.0/8")`,
		`{app="a"} | ip_a!=ip("10.0.0.1-10.0.0.5")`,
		`{app="a"} | ip_a=ip("::1")`,
		`{app="a"} | ip_a!=ip("fe80::/10")`,
		`{app="a"} | ip_a=ip("10.0.0.0/8") and b=~"x|y"`,
		`{app="a"} | k8s.pod.name="p"`,
		`{app="a"} | k8s_pod_name="p"`,
		`{app="a"} | k8s_pod_name=~"p|q"`,
		`{app="a"} | k8s_pod_name!=""`,
		`{app="a"} | a="a\"b"`,
		`{app="a"} | a="é\n"`,
		"{app=\"a\"} | a=`raw\\d`",
		`{app="a"} | a="x" | b="y" | c>1`,
		`{app="a"} |= "x" | a="1"`,
		`{app="a"} | a="1" |= "x"`,
		`{app="a"} != "x" | a="1" |~ "y.+" | b="2"`,
		`{app="a"} |> "<_> x <_> y" | a="1"`,
		`{app="a"} |> "<_> x <_> y" | a="1" | b>2`,
		`{app="a"} |> "<_> x <_> y" | a="1" |= "k" | b>2 != "j" | c=""`,
		`{app="a"} |> "<_> x <_> y" | level="" | b="2"`,
		`{app="a"} |> "<_> x <_> y" | a="1" and b="2" | c="3"`,
		`{app="a"} !> "<_> x <_> y" | a="1" | b="2"`,
		`{app="a"} |> "<_> x <_> y" |> "<_> m <_> n" | a="1" |= "k" | b="2"`,
		`{app="a"} | a="1" |> "<_> x <_> y" | b="2"`,
		`{app="a"} | a="1" | a="2"`,
		`{app="a"} | a="1" | a!="2"`,
		`{app="a"} | a>5 | a>5`,
		`{app="a"} | a=~"x" |= "k" | a=~"y"`,
		`{app="a"} |> "<_> x <_> y" | a="1" | a="2"`,
		`{app="a"} | ip_a=ip("10.0.0.0/8") | ip_a=ip("10.0.0.0/8")`,
		`{app="a"} | a="1" or b="2" | a="3"`,
		`{app="a"} | a!="x" | a!="x"`,
		`{app="a", env=~"prod|stg"} | a="1"`,
	}
	for name, fn := range labelFilterProfiles() {
		for _, q := range queries {
			lowered, legacy, same, delegated, ok := labelFilterLowering(t, q, fn)
			if !ok || delegated {
				t.Errorf("%s: %s: not lowered (ok %v)", name, q, ok)
				continue
			}
			if !same {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", name, q, lowered, legacy)
			}
		}
	}
}

// What the lowering leaves to the string translator: the labels it derives (detected_level, service_name), a
// value that is not a valid literal, and a label that names several parsed keys.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesLabelFilters(t *testing.T) {
	for q, construct := range map[string]string{
		`{app="a"} | detected_level="error"`:         "LogSelect label filter detected_level",
		`{app="a"} | a="1" and service_name="s"`:     "LogSelect label filter service_name",
		`{app="a"} | a=~"x\d"`:                       "LogSelect label filter value",
		`{app="a"} | level="x" | detected_level="y"`: "LogSelect label filter detected_level",
	} {
		expr, err := logql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
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
	// A label function that names several parsed keys is the string translator's.
	expr, _ := logql.Parse(`{app="a"} | a="1"`)
	p, _ := queryplan.Build(expr, queryplan.Inputs{})
	queryplan.Resolve(p)
	if _, err := Plan(p, Profile{Label: func(s string) string { return s + "\x00" + s + "_x" }}); !errors.Is(err, ErrNotLowered) {
		t.Errorf("parsed keys: got %v", err)
	}
}

// FuzzLabelFilterLowering holds the lowering of label filters to the string translator on any names, operators,
// values and trees: both write the same query, or the lowering leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzLabelFilterLowering(f *testing.F) {
	f.Add("status", "error", "level", "x|y", uint8(0), uint8(0))
	f.Add("k8s_pod_name", "", "a.b", ".*", uint8(7), uint8(3))
	f.Add("a", "10.0.0.0/8", "b", "é\n\\d", uint8(200), uint8(9))
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string, ops, shape uint8) {
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
		leaf := func(name, v string, op uint8, bit uint8) string {
			name = ident(name)
			switch op % 12 {
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
				return name + "<" + strconv.Itoa(len(v)) + "s"
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
			}
			return name + `!=""`
		}
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
		q := `{app="a"} | ` + stage
		for name, fn := range labelFilterProfiles() {
			if lowered, legacy, same, delegated, ok := labelFilterLowering(t, q, fn); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", name, q, lowered, legacy)
			}
		}
	})
}
