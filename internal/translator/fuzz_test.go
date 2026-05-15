package translator

import (
	"testing"
)

// FuzzTranslateLogQL tests the translator with arbitrary inputs.
// Run: go test -fuzz=FuzzTranslateLogQL -fuzztime=30s ./internal/translator/
func FuzzTranslateLogQL(f *testing.F) {
	// Seed corpus: valid queries
	seeds := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} != "debug" |~ "err.*"`,
		`{app="nginx"} | json`,
		`{app="nginx"} | logfmt`,
		`{app="nginx"} | pattern "<ip> - - [<_>] \"<method> <path> <_>\" <status> <size>"`,
		`{app="nginx"} | regexp "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)"`,
		`{app="nginx"} | json | status >= 500`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`{app="nginx"} | label_format new_label="{{.old_label}}"`,
		`{app="nginx"} | drop temp_label`,
		`{app="nginx"} | keep status, method`,
		`{app="nginx"} | decolorize`,
		`rate({app="nginx"}[5m])`,
		`count_over_time({app="nginx"}[5m])`,
		`sum(rate({app="nginx"}[5m])) by (status)`,
		`topk(10, rate({app="nginx" |= "error"}[5m]))`,
		`bytes_over_time({app="nginx"}[5m])`,
		`avg_over_time({app="nginx"} | unwrap duration [5m])`,
		// Edge cases
		``,
		`*`,
		`{}`,
		`{app=~".*"}`,
		`{app!=""}`,
		`{app="nginx",host="host-42"}`,
		// Malformed inputs that should not panic
		`{`,
		`{app="nginx"`,
		`{app="nginx"} |`,
		`{app="nginx"} |=`,
		`rate(`,
		`rate({app="nginx"}`,
		`rate({app="nginx"}[`,
		`{app="nginx"} | json | status == `,
		`}}}}`,
		`{{{`,
		`|||`,
		"😀🔥",
		"\x00\x01\x02",
		`{app="nginx"} |= "` + string(make([]byte, 1000)) + `"`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		// The translator must never panic, regardless of input.
		// Errors are acceptable for malformed input.
		_, _ = TranslateLogQL(query)
	})
}

// FuzzTranslateSubquery tests subquery parsing with random inputs.
func FuzzTranslateSubquery(f *testing.F) {
	seeds := []string{
		`max_over_time(rate({app="nginx"}[5m])[1h:5m])`,
		`min_over_time(count_over_time({job="x"}[5m])[30m:1m])`,
		`avg_over_time(rate({app="api"}[1m])[6h:30m])`,
		`sum_over_time(rate({app="api"}[1m])[30m:5m])`,
		`stddev_over_time(rate({app="api"}[5m])[1h:5m])`,
		`rate({app="nginx"}[5m])[1h:5m]`,              // no outer func
		`max_over_time(rate({app="nginx"}[5m]))`,      // no subquery range
		`max_over_time(rate({app="nginx"}[5m])[])`,    // empty brackets
		`max_over_time(rate({app="nginx"}[5m])[1h:])`, // missing step
		`max_over_time(rate({app="nginx"}[5m])[:5m])`, // missing range
		`max_over_time(`,
		`sum_over_time(rate({app="x"}[5m])[1h:5m]) / rate({app="y"}[5m])`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		// Must never panic
		_, _ = TranslateLogQL(query)
	})
}

// FuzzParseBinaryMetricExpr tests binary expression parsing with random inputs.
func FuzzParseBinaryMetricExpr(f *testing.F) {
	seeds := []string{
		"__binary__:/:left|||right",
		"__binary__:*:a|||b",
		"__binary__:+:x|||100",
		"__binary__:>:a|||0",
		"__binary__:==:a|||b",
		"not a binary expr",
		"",
		"__binary__:",
		"__binary__:op:",
		"__binary__:op:query",
		"__binary__:op:left|||",
		"|||",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_, _, _, _ = ParseBinaryMetricExpr(input)
	})
}

// FuzzParseSubqueryExpr tests subquery expression parsing with random inputs.
func FuzzParseSubqueryExpr(f *testing.F) {
	seeds := []string{
		"__subquery__:max_over_time:app:=nginx | stats rate():1h:5m",
		"__subquery__:min_over_time:query:30m:1m",
		"__subquery__:",
		"__subquery__:func:",
		"__subquery__:func:query:range",
		"",
		"regular query",
		"__binary__:/:left|||right",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_, _, _, _, _ = ParseSubqueryExpr(input)
	})
}

// FuzzIsScalar tests scalar detection with random inputs.
func FuzzIsScalar(f *testing.F) {
	seeds := []string{
		"42", "3.14", "-1", "1e5", "1.5e-3", "+42",
		"", "abc", "1.2.3", `{app="nginx"}`,
		"NaN", "Inf", "-Inf", "0x1F", "0b101",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_ = IsScalar(input)
	})
}

// FuzzTranslateWithStreamFields tests stream field optimization with random inputs.
func FuzzTranslateWithStreamFields(f *testing.F) {
	seeds := []string{
		`{app="nginx"}`,
		`{app="nginx", level="error"}`,
		`{app=~"nginx.*"}`,
		`{app!="nginx"}`,
		`{}`,
		`{app="nginx"} |= "error"`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	streamFields := map[string]bool{"app": true, "env": true}
	f.Fuzz(func(t *testing.T, query string) {
		_, _ = TranslateLogQLWithStreamFields(query, nil, streamFields)
	})
}

// FuzzBinaryExpressions tests binary expression translation with random inputs.
func FuzzBinaryExpressions(f *testing.F) {
	seeds := []string{
		`rate({app="a"}[5m]) / rate({app="b"}[5m])`,
		`rate({app="nginx"}[5m]) * 100`,
		`rate({app="nginx"}[5m]) > 0`,
		`rate({app="nginx"}[5m]) + rate({app="nginx"}[5m])`,
		`rate({app="nginx"}[5m]) % 2`,
		`rate({app="nginx"}[5m]) ^ 2`,
		`rate({app="nginx"}[5m]) == 0`,
		`rate({app="nginx"}[5m]) != 0`,
		`rate({app="nginx"}[5m]) >= 100`,
		`rate({app="nginx"}[5m]) <= 100`,
		`rate({app="a"}[5m]) / on(app) rate({app="b"}[5m])`,
		`rate({app="a"}[5m]) * on(app) group_left(team) rate({app="b"}[5m])`,
		`100 / rate({app="nginx"}[5m])`,
		`rate({app="nginx"}[5m]) > bool 0`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		_, _ = TranslateLogQL(query)
	})
}

// FuzzSanitizeLabelName tests label sanitization with arbitrary inputs.
func FuzzSanitizeLabelName(f *testing.F) {
	seeds := []string{
		"service.name",
		"k8s.pod.name",
		"already_underscore",
		"123numeric",
		"",
		"a.b.c.d.e.f.g",
		"with-dashes-and.dots",
		"UPPER.case.Field",
		"\x00null",
		"emoji😀field",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, name string) {
		// Must never panic
		// The import of proxy.SanitizeLabelName isn't available here,
		// so we test the translator's label handling instead
		_, _ = TranslateLogQLWithLabels(`{`+name+`="value"}`, nil)
	})
}
