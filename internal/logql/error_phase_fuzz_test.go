package logql

import (
	"strings"
	"testing"
)

// actionCarriers are log queries that end in a matcher or stage Loki builds while it parses and refuses; each
// answers its action error alone.
var actionCarriers = []string{
	`{a="b"} | pattern "<_>"`,
	`{a="b"} | regexp "x"`,
	`{a="b"} | drop c=~"("`,
	`{a="b"} | keep c=~"("`,
	`{a=~"(b"}`,
	`{a=~"b{1001}"} |= "x"`,
}

// An action error (a selector, drop or keep regex, a pattern or regexp parser Loki refuses as it reads the stage)
// answers ahead of whatever follows the stage: a later syntax error, a lexer error, a tree check or a build error,
// and the same error stays inside every wrapper Loki keeps errors through. The recorded grid (TestErrorPhaseGridMatchesLoki in internal/proxy)
// holds the answers to Loki's; this target holds the order on any text after the stage, and that the validation
// returns.
//
// conformance: semantics/loki-error-phase-order
func FuzzActionErrorAheadOfLaterErrors(f *testing.F) {
	for _, s := range []string{"", "unpack a", `a=="b"`, `c > 1PB`, `|= "\q"`, `line_format "{{.a"`, "json", ")", "[5m]",
		`c=~"("`, "logfmt --strict", `label_format __error__="x"`, "(((((", `"`, "\x00", "unwrap x"} {
		for i := range actionCarriers {
			f.Add(uint8(i), s)
		}
	}
	f.Fuzz(func(t *testing.T, which uint8, rest string) {
		carrier := actionCarriers[int(which)%len(actionCarriers)]
		want := ValidateLogQL(carrier)
		if want == "" {
			t.Fatalf("%q is accepted", carrier)
		}
		q := carrier + " | " + rest
		if len(q) >= MaxInputSize {
			return
		}
		if got := ValidateLogQL(q); got != want {
			t.Fatalf("%q answers %q, not the action error %q of the stage before", q, got, want)
		}
		if strings.HasPrefix(carrier, "{a=~") {
			return
		}
		metric := "sum(count_over_time(" + carrier + ` |= "x" [5m])) ` + rest
		if len(metric) < MaxInputSize {
			if got := ValidateLogQL(metric); got != want {
				t.Fatalf("%q answers %q, not the action error %q inside its range aggregation", metric, got, want)
			}
		}
	})
}
