package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestTranslate_StreamSelector(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`{app="nginx"}`,
			`app:=nginx`,
		},
		{
			`{app="nginx", env="prod"}`,
			`app:=nginx env:=prod`,
		},
		{
			`{app=~"api.*"}`,
			`app:~"api.*"`,
		},
		{
			`{app!="debug"}`,
			`-app:=debug`,
		},
		{
			`{app!~"kube-.*"}`,
			`-app:~"kube-.*"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := logql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			got, err := logql.Translate(expr, logql.TranslateOptions{})
			if err != nil {
				t.Fatalf("Translate error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslate_LineFilter(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`{app="nginx"} |= "error"`,
			`app:=nginx ~"error"`,
		},
		{
			`{app="nginx"} != "debug"`,
			`app:=nginx NOT ~"debug"`,
		},
		{
			`{app="nginx"} |~ "err.*"`,
			`app:=nginx ~"err.*"`,
		},
		{
			`{app="nginx"} !~ "ok.*"`,
			`app:=nginx NOT ~"ok.*"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := logql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			got, err := logql.Translate(expr, logql.TranslateOptions{})
			if err != nil {
				t.Fatalf("Translate error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslate_WithLabelFn(t *testing.T) {
	labelFn := func(label string) string {
		if label == "kubernetes_namespace_name" {
			return "k8s.namespace.name"
		}
		return label
	}
	input := `{kubernetes_namespace_name="default"}`
	expr, err := logql.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{LabelFn: labelFn})
	if err != nil {
		t.Fatalf("Translate error: %v", err)
	}
	want := `"k8s.namespace.name":=default`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTranslate_ParserStages(t *testing.T) {
	tests := []struct {
		input string
	}{
		{`{app="nginx"} | json`},
		{`{app="nginx"} | logfmt`},
		{`{app="nginx"} | json | level="error"`},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := logql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			_, err = logql.Translate(expr, logql.TranslateOptions{})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestTranslate_RoundTrip(t *testing.T) {
	// Parse → String() → re-Parse → Translate must equal Parse → Translate
	// (ensures AST serialisation is canonical and stable).
	input := `{app="api", env="prod"} |= "error" | json | level="error"`
	expr1, err := logql.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	out1, err := logql.Translate(expr1, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("Translate error: %v", err)
	}

	expr2, err := logql.Parse(expr1.String())
	if err != nil {
		t.Fatalf("re-Parse error: %v", err)
	}
	out2, err := logql.Translate(expr2, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("re-Translate error: %v", err)
	}

	if out1 != out2 {
		t.Errorf("round-trip mismatch:\n  first:  %q\n  second: %q", out1, out2)
	}
}
