package translator

import (
	"strings"
	"testing"
)

func TestPatternAndTranslatorHelperBehaviors(t *testing.T) {
	if !isNoopPatternExpression(`"^.*$"`) {
		t.Fatal("expected quoted catch-all regex to be treated as noop pattern")
	}
	if isNoopPatternExpression(`"error"`) {
		t.Fatal("expected concrete pattern not to be treated as noop")
	}

	if got := planPatternLineFilter(`foo<_>`, 0); got.re != `(?s)foo.+` || got.extract != "" {
		t.Fatalf("unexpected exact pattern translation %+v", got)
	}
	if got := planPatternLineFilter(`literal.*value`, 0); got.re != `literal\.\*value$` || got.extract != "" || got.exact != `_msg:~"literal\\.\\*value" _msg:~"(?s)literal\\.\\*value$" NOT _msg:~"(?s)literal\\.\\*value."` {
		t.Fatalf("unexpected literal pattern translation %+v", got)
	}
	if got := planPatternLineFilter(``, 0); got.re != `^$` || got.extract != "" {
		t.Fatalf("unexpected empty pattern translation %+v", got)
	}
	seq := 0
	if got := strings.Join(translatePatternLineFilter([]string{`foo<_>bar`, `baz<_>`}, false, &seq), " "); got != `((_msg:~"foo" _msg:~"bar") or (_msg:~"baz")) ~"(?:(?s)foo.+bar$)|(?:(?s)baz.+)" | extract "foo<plain:__lvp_pf0_1>bar<plain:__lvp_pf0_r>" from _msg | filter (__lvp_pf0_1:* __lvp_pf0_r:"") or (_msg:~"(?s)baz.+") | delete __lvp_pf*` || seq != 1 {
		t.Fatalf("unexpected translated pattern line filter %q (seq %d)", got, seq)
	}
	if got := strings.Join(translatePatternLineFilter([]string{`foo<_>`}, true, &seq), " "); got != `NOT ~"(?s)foo.+"` || seq != 1 {
		t.Fatalf("unexpected translated negative pattern line filter %q (seq %d)", got, seq)
	}
	if values, rest, err := takePatternValues(`"foo<_>bar" or "baz" != "x"`); err != nil || len(values) != 2 || values[0] != "foo<_>bar" || values[1] != "baz" || rest != `!= "x"` {
		t.Fatalf("unexpected extracted pattern filter values %#v rest=%q err=%v", values, rest, err)
	}
	if got := normalizeQuotedStageExpr("`(?P<field>.*)`"); got != `"(?P<field>.*)"` {
		t.Fatalf("unexpected normalized raw stage expression %q", got)
	}

	if field, op, ok := translatedFilterFieldOp(`-"service.name":~"api.*"`); !ok || field != `"service.name"` || op != ":~" {
		t.Fatalf("unexpected translated filter field/op: %q %q %v", field, op, ok)
	}
	if _, _, ok := translatedFilterFieldOp(`bogus`); ok {
		t.Fatal("expected malformed translated filter to fail parsing")
	}

	if query, duration := extractQueryAndDuration(`{app="api"} | unwrap latency [5m]`); query != `{app="api"} | unwrap latency` || duration != "5m" {
		t.Fatalf("unexpected query/duration split query=%q duration=%q", query, duration)
	}
	if query, duration := extractQueryAndDuration(`{app="api"}`); query != `{app="api"}` || duration != "" {
		t.Fatalf("unexpected query/duration split without window query=%q duration=%q", query, duration)
	}

	labelFn := func(label string) string {
		if label == "detected_level" {
			return "level"
		}
		if label == "drop" {
			return ""
		}
		return label
	}
	// detected_level is derived, not translated to the stored level field.
	if got := normalizeByLabels(`detected_level, cluster, detected_level, drop`, labelFn); got != "detected_level, cluster" {
		t.Fatalf("unexpected normalized by-labels %q", got)
	}

	if got, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95, {app="api"} | unwrap duration(latency) [5m])`, "", "detected_level", labelFn); !ok || got != `app:="api"`+UnwrapGateFor("latency", "duration")+` | stats by (detected_level) quantile(0.95, __lvp_v)` {
		t.Fatalf("unexpected quantile translation %q ok=%v", got, ok)
	}
	if _, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95 {app="api"})`, "", "", nil); ok {
		t.Fatal("expected malformed quantile_over_time expression to fail translation")
	}

	if got := findLastMatchingParen(`inner(")") ) tail`); got != 11 {
		t.Fatalf("unexpected last matching paren index got=%d want=%d", got, 11)
	}
}
