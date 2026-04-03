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
