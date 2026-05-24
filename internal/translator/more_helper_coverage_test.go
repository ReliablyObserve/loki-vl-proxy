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

	if got, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95, {app="api"} | unwrap duration(latency) [5m])`, "", "detected_level", labelFn); !ok || got != `app:="api" | stats by (level) quantile(0.95, latency)` {
		t.Fatalf("unexpected quantile translation %q ok=%v", got, ok)
	}
	if _, ok := tryTranslateQuantileOverTime(`quantile_over_time(0.95 {app="api"})`, "", "", nil); ok {
		t.Fatal("expected malformed quantile_over_time expression to fail translation")
	}

	if got := findLastMatchingParen(`inner(")") ) tail`); got != 11 {
		t.Fatalf("unexpected last matching paren index got=%d want=%d", got, 11)
	}
}

func TestServiceNameMatcherFilter(t *testing.T) {
	// Positive exact match: OR of all synthetic fields.
	got := serviceNameMatcherFilter(":=", "auth", false, false)
	if !strings.HasPrefix(got, "(") || !strings.HasSuffix(got, ")") {
		t.Fatalf("expected positive match to be OR-grouped: %q", got)
	}
	if !strings.Contains(got, `service_name:="auth"`) {
		t.Fatalf("expected service_name field in positive match: %q", got)
	}
	if !strings.Contains(got, `"service.name":="auth"`) {
		t.Fatalf("expected quoted service.name field in positive match: %q", got)
	}
	if !strings.Contains(got, " OR ") {
		t.Fatalf("expected OR join in positive match: %q", got)
	}

	// Negative exact match: AND of all negated synthetic fields (no parens, space join).
	got = serviceNameMatcherFilter(":=", "auth", true, false)
	if strings.HasPrefix(got, "(") {
		t.Fatalf("expected negated match NOT to be OR-grouped: %q", got)
	}
	if !strings.Contains(got, `-service_name:="auth"`) {
		t.Fatalf("expected negated service_name field: %q", got)
	}
	if !strings.Contains(got, `-"service.name":="auth"`) {
		t.Fatalf("expected negated quoted service.name field: %q", got)
	}

	// Positive regex match: OR of all fields with :~ op.
	got = serviceNameMatcherFilter(":~", "auth.*", false, true)
	if !strings.Contains(got, `service_name:~"auth.*"`) {
		t.Fatalf("expected regex service_name field: %q", got)
	}
	if !strings.Contains(got, ` OR `) {
		t.Fatalf("expected OR join in regex positive match: %q", got)
	}

	// Negative regex match: AND join.
	got = serviceNameMatcherFilter(":~", "auth.*", true, true)
	if strings.Contains(got, " OR ") {
		t.Fatalf("expected AND (space) join in regex negative match: %q", got)
	}
	if !strings.Contains(got, `-service_name:~"auth.*"`) {
		t.Fatalf("expected negated regex service_name field: %q", got)
	}

	// Empty value positive: space join (all must be empty).
	got = serviceNameMatcherFilter(":=", "", false, false)
	if strings.Contains(got, " OR ") {
		t.Fatalf("expected space (AND) join for empty positive match: %q", got)
	}
	if !strings.Contains(got, `service_name:=""`) {
		t.Fatalf("expected empty exact service_name field: %q", got)
	}

	// Empty value negative: OR join ("not empty" = any field is non-empty).
	got = serviceNameMatcherFilter(":=", "", true, false)
	if !strings.Contains(got, " OR ") {
		t.Fatalf("expected OR join for empty negative (not-empty) match: %q", got)
	}
	if !strings.Contains(got, `service_name:!""`) {
		t.Fatalf("expected not-empty service_name field: %q", got)
	}
	if !strings.Contains(got, `"service.name":!""`) {
		t.Fatalf("expected not-empty quoted service.name field: %q", got)
	}
}
