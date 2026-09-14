package logql

import (
	"reflect"
	"strings"
	"testing"
)

func TestBinaryExplicitParserExtractionRoundTrip(t *testing.T) {
	for _, stage := range []string{
		`json status="http.status", agent="request.headers[\"User-Agent\"]"`,
		"json status=`http.status`, method",
		`logfmt status="http_status", method`,
		"logfmt status=`http_status`, method",
	} {
		t.Run(stage, func(t *testing.T) {
			operand := `sum by(status)(count_over_time({app="a"} | ` + stage + ` | status="200" [5m]))`
			query := `(` + operand + ` + on(status) ` + operand + `) / on(status) ` + operand
			expr, err := Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(expr.String(), "| "+stage) != 3 {
				t.Fatalf("extraction lost: %s", expr.String())
			}
			again, err := Parse(expr.String())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expr, again) {
				t.Fatalf("AST changed on round trip: %s", expr.String())
			}
		})
	}
}
