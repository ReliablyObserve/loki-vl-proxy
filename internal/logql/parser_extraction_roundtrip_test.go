package logql

import (
	"reflect"
	"strconv"
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

func TestBinaryOperandStringValuesRoundTrip(t *testing.T) {
	values := []string{"plain", "a\"b", `a\b`, "line\nnext\r\t\x00", "a`b"}
	for _, value := range values {
		quoted := strconv.Quote(value)
		for _, pipeline := range []string{
			"|= " + quoted,
			"!= " + quoted,
			"|~ " + quoted,
			"!~ " + quoted,
			"|> " + quoted,
			"!> " + quoted,
			"| regexp " + quoted,
			"| pattern " + quoted,
			"| line_format " + quoted,
			"| label_format value=" + quoted,
			"| value=" + quoted,
			"| drop value=" + quoted,
			"| keep value=" + quoted,
		} {
			query := `sum without()(count_over_time({app="a",value=` + quoted + `} ` + pipeline + `[5m])) + on() vector(0)`
			t.Run(pipeline, func(t *testing.T) {
				expr, err := Parse(query)
				if err != nil {
					t.Fatal(err)
				}
				again, err := Parse(expr.String())
				if err != nil || !reflect.DeepEqual(expr, again) {
					t.Fatalf("changed executable AST: original=%s rendered=%s err=%v", query, expr.String(), err)
				}
			})
		}
	}
}

func TestBinaryOperandEmptyGroupingRoundTrip(t *testing.T) {
	for _, grouping := range []string{"by()", "without()"} {
		query := `sum ` + grouping + `(count_over_time({app="a"}[5m])) + on() vector(0)`
		expr, err := Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Parse(expr.String())
		if err != nil || !reflect.DeepEqual(expr, again) {
			t.Fatalf("lost grouping: %s err=%v", expr.String(), err)
		}
	}
}

func TestIPCallAndLiteralRemainDistinct(t *testing.T) {
	call, err := ParseLogQuery(`{app="a"} |= ip("10.0.0.1")`)
	if err != nil {
		t.Fatal(err)
	}
	literal, err := ParseLogQuery(`{app="a"} |= "ip(10.0.0.1)"`)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(call, literal) || !call.Pipeline[0].(*LineFilterStage).IP || literal.Pipeline[0].(*LineFilterStage).IP {
		t.Fatal("function call and literal lost their distinction")
	}
	for _, expr := range []*LogQuery{call, literal} {
		again, err := ParseLogQuery(expr.String())
		if err != nil || !reflect.DeepEqual(expr, again) {
			t.Fatalf("lost IP kind: %s err=%v", expr.String(), err)
		}
	}
	callQL, callErr := Translate(call, TranslateOptions{})
	literalQL, literalErr := Translate(literal, TranslateOptions{})
	if callErr != nil || literalErr != nil || callQL == literalQL {
		t.Fatalf("IP call translated as literal: call=%s literal=%s errors=%v/%v", callQL, literalQL, callErr, literalErr)
	}
}

func TestRawAndEscapedStringValuesPreserveDecodedBytes(t *testing.T) {
	for _, value := range []string{"carriage\rreturn", "separator\u2028paragraph\u2029", "bell\aform\fvertical\v", "nul\x00", "Łódź 日本語"} {
		for _, literal := range []string{"`" + value + "`", strconv.Quote(value)} {
			query := "{app=" + literal + "} |= " + literal
			expr, err := ParseLogQuery(query)
			if err != nil {
				t.Fatalf("query=%q err=%v", query, err)
			}
			if expr.Selector.Matchers[0].Value != value || expr.Pipeline[0].(*LineFilterStage).Value != value {
				t.Fatalf("decoded bytes changed: input=%q selector=%q line=%q", value, expr.Selector.Matchers[0].Value, expr.Pipeline[0].(*LineFilterStage).Value)
			}
			again, err := ParseLogQuery(expr.String())
			if err != nil || !reflect.DeepEqual(expr, again) {
				t.Fatalf("roundtrip=%q err=%v", expr.String(), err)
			}
		}
	}
}
