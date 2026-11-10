package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// Until a construct's lowering lands, Plan delegates it to the string translator with ErrNotLowered naming the
// construct at the top of the plan, under every profile.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesEveryConstruct(t *testing.T) {
	for construct, q := range map[string]string{
		"LogSelect pipeline": `{app="a"} | line_format "{{.a}}"`,
		"LogSelect label filter numeric equality after parser": `{app="a"} | json | status=5`,
		"LogSelect unpack parser":                              `{app="a"} | unpack`,
		"RangeAgg":                                             `rate({app="a"}[5m])`,
		"VectorAgg":                                            `sum by (app) (count_over_time({app="a"}[5m]))`,
		"Rank":                                                 `topk(3, sum by (app) (rate({app="a"}[5m])))`,
		"BinOp":                                                `sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`,
		"LabelReplace":                                         `label_replace(rate({app="a"}[5m]), "b", "$1", "app", "(.*)")`,
		"VectorLit":                                            `vector(1)`,
		"Scalar":                                               `1`,
	} {
		t.Run(construct, func(t *testing.T) {
			expr, err := logql.Parse(q)
			if err != nil {
				t.Fatal(err)
			}
			p, err := queryplan.Build(expr, queryplan.Inputs{})
			if err != nil {
				t.Fatal(err)
			}
			queryplan.Resolve(p)
			for _, prof := range []Profile{{}, {KeepLine: true}, {StreamFields: map[string]bool{"app": true}}} {
				op, err := Plan(p, prof)
				var nl *NotLoweredError
				if op != nil || !errors.Is(err, ErrNotLowered) || !errors.As(err, &nl) || nl.Construct != construct {
					t.Fatalf("Plan = %v, %v; want ErrNotLowered naming %s", op, err, construct)
				}
			}
		})
	}
}

func planOf(t *testing.T, q string) *queryplan.Plan {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	queryplan.Resolve(p)
	return p
}

// A bare selector lowers to its stream filter, with Loki's selector semantics: regexps anchored to the whole value
// with '.' matching newlines, an empty value matching a missing label, service_name matched on the value the proxy
// derives, detected_level (not a stream field) matching every stream or none, exact matchers on declared stream
// fields in one stream filter, and labels renamed to their VictoriaLogs fields. The shadow holds the same lowering
// to the string translator on the whole corpus; this table pins the answers.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersBareSelectors(t *testing.T) {
	dots := func(s string) string { return strings.ReplaceAll(s, "_", ".") }
	for _, c := range []struct {
		name, q string
		prof    Profile
		want    string
	}{
		{"exact", `{app="a"}`, Profile{}, `app:="a"`},
		{"not equal", `{app="a", env!="b"}`, Profile{}, `app:="a" -env:="b"`},
		{"regexp anchored", `{app=~"a|b"}`, Profile{}, `app:~"^(?s:a|b)$"`},
		{"negated regexp", `{app!~"a.*"}`, Profile{}, `-app:~"^(?s:a.*)$"`},
		{"empty value", `{app=""}`, Profile{}, `app:=""`},
		{"empty level", `{level=""}`, Profile{}, `-level:*`},
		{"not empty", `{app!=""}`, Profile{}, `app:!""`},
		{"stream field", `{app="a", env="b"}`, Profile{StreamFields: map[string]bool{"app": true}}, `{app="a"} env:="b"`},
		{"literal regexp is exact, not a stream filter", `{app=~"a"}`, Profile{StreamFields: map[string]bool{"app": true}}, `app:="a"`},
		{"renamed label", `{k8s_pod_name="p"}`, Profile{Label: dots}, `"k8s.pod.name":="p"`},
		{"detected_level matches none", `{detected_level="error"}`, Profile{}, `_stream_id:in()`},
		{"detected_level matches all", `{detected_level=~".*"}`, Profile{}, `*`},
		{"keep line", `{app="a"}`, Profile{KeepLine: true}, `app:="a"`},
		{"substring line filter", `{app="a"} |= "x.y"`, Profile{}, `app:="a" ~"x\\.y"`},
		{"negated regexp line filter", `{app="a"} !~ "e.+r"`, Profile{}, `app:="a" NOT ~"e.+r"`},
		{"or chain", `{app="a"} |= "a" or "b"`, Profile{}, `app:="a" ~"a|b"`},
		{"negated or chain", `{app="a"} != "a" or "b"`, Profile{}, `app:="a" NOT ~"a|b"`},
		{"ip line filter", `{app="a"} != ip("10.0.0.1")`, Profile{}, `app:="a" ` + translate.IPLineFilter("10.0.0.1", true).String()},
		{"line filters in order", `{app="a"} |= "x" != "y"`, Profile{}, `app:="a" ~"x" NOT ~"y"`},
		{"text that is not UTF-8 matches no line", `{app="a"} |= "\xff\xfe"`, Profile{}, `app:="a" _stream_id:in()`},
		{"!= of text that is not UTF-8 keeps every line", `{app="a"} != "\xff"`, Profile{}, `app:="a"`},
		{"or chain without the text that is not UTF-8", `{app="a"} |= "\xff" or "b"`, Profile{}, `app:="a" ~"b"`},
		{"!= chain without the text that is not UTF-8", `{app="a"} != "\xff" or "b"`, Profile{}, `app:="a" NOT ~"b"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			op, err := Plan(planOf(t, c.q), c.prof)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := op.(physical.VLQuery); !ok || got.Text != c.want {
				t.Fatalf("Plan(%s) = %#v, want %s", c.q, op, c.want)
			}
		})
	}
	t.Run("service_name", func(t *testing.T) {
		op, err := Plan(planOf(t, `{service_name="api"}`), Profile{})
		got, _ := op.(physical.VLQuery)
		if err != nil || !strings.HasPrefix(got.Text, `(service_name:="api" OR (service_name:="" "service.name":="api")`) {
			t.Fatalf("Plan = %#v, %v", op, err)
		}
	})
}
