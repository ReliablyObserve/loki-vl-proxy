package translator

import "testing"

func TestCoverage_MoreTranslatorHelpers(t *testing.T) {
	if !isNoopPatternExpression(`"^.*$"`) {
		t.Fatal("expected quoted catch-all regex to be treated as noop pattern")
	}
	if isNoopPatternExpression(`"error"`) {
		t.Fatal("expected concrete pattern not to be treated as noop")
	}

	if got := patternFilterValueToRegex(`foo<_>baz`); got != `foo.*baz` {
		t.Fatalf("unexpected placeholder translation %q", got)
	}
	if got := patternFilterValueToRegex(`literal.*value`); got != `literal\.\*value` {
		t.Fatalf("unexpected literal pattern translation %q", got)
	}
	if got := patternFilterValueToRegex(``); got != `.*` {
		t.Fatalf("unexpected empty pattern translation %q", got)
	}
	if got := translatePatternLineFilter(`"foo<_>bar" or "baz"`, false); got != `~"(?:foo.*bar)|(?:baz)"` {
		t.Fatalf("unexpected translated pattern line filter %q", got)
	}
	if got := translatePatternLineFilter(`"foo<_>bar"`, true); got != `NOT ~"foo.*bar"` {
		t.Fatalf("unexpected translated negative pattern line filter %q", got)
	}
	if values, ok := extractPatternFilterValues(`"foo<_>bar" or "baz"`); !ok || len(values) != 2 || values[0] != "foo<_>bar" || values[1] != "baz" {
		t.Fatalf("unexpected extracted pattern filter values %#v ok=%v", values, ok)
	}
	if _, ok := extractPatternFilterValues(`"foo<_>bar" and "baz"`); ok {
		t.Fatal("expected malformed pattern alternation to fail parsing")
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
	if got := normalizeByLabels(`detected_level, cluster, detected_level, drop`, labelFn); got != "level, cluster" {
		t.Fatalf("unexpected normalized by-labels %q", got)
	}

	if got, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95, {app="api"} | unwrap duration(latency) [5m])`, "", "detected_level", labelFn); !ok || got != `app:=api | stats by (level) quantile(0.95, latency)` {
		t.Fatalf("unexpected quantile translation %q ok=%v", got, ok)
	}
	if _, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95 {app="api"})`, "", "", nil); ok {
		t.Fatal("expected malformed quantile_over_time expression to fail translation")
	}

	if got := findLastMatchingParen(`inner(")") ) tail`); got < 0 {
		t.Fatalf("expected last matching paren finder to locate closing paren")
	}
}
