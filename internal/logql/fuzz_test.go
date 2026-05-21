package logql

import (
	"testing"
)

// FuzzParse ensures that Parse + String + Translate never panic on arbitrary input.
// Run with: go test -fuzz=FuzzParse ./internal/logql/
func FuzzParse(f *testing.F) {
	seeds := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`{}`,
		``,
		`{`,
		`{app=`,
		`| json`,
		`not logql at all`,
		`{app="nginx"} | drop a, b | keep c | decolorize`,
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		expr, err := Parse(input)
		if err != nil {
			return
		}
		// If Parse succeeded, String() and Translate() must not panic.
		_ = expr.String()
		_, _ = Translate(expr, TranslateOptions{})
	})
}
