package translator

import (
	"fmt"
	"testing"
)

func TestAuditEscaping(t *testing.T) {
	tests := []string{
		`{app="x"} |= "error("`,
		`{app="x"} |= "a.b"`,
		`{app="x"} |= "C:\\"`,
		`{app="x"} |= "C:\\" |= "must-also-match"`,
		`{app="x", ns="prod\\"} |= "y"`,
		`{app="evil\"} | stats count() | {x=\""}`,
		`{app="x"} | level="err\\"`,
	}
	for _, q := range tests {
		result, err := TranslateLogQL(q)
		fmt.Printf("IN:  %s\nOUT: %s (err: %v)\n\n", q, result, err)
	}
}
