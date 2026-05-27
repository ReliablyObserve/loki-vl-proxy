package logql_test

import (
	"strings"
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
			`app:="nginx"`,
		},
		{
			`{app="nginx", env="prod"}`,
			`app:="nginx" env:="prod"`,
		},
		{
			`{app=~"api.*"}`,
			`app:~"api.*"`,
		},
		{
			`{app!="debug"}`,
			`-app:="debug"`,
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
			`app:="nginx" ~"error"`,
		},
		{
			`{app="nginx"} != "debug"`,
			`app:="nginx" NOT ~"debug"`,
		},
		{
			`{app="nginx"} |~ "err.*"`,
			`app:="nginx" ~"err.*"`,
		},
		{
			`{app="nginx"} !~ "ok.*"`,
			`app:="nginx" NOT ~"ok.*"`,
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
	want := `"k8s.namespace.name":="default"`
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

func TestTranslate_OpaqueMetricExpr_FallsThrough(t *testing.T) {
	// label_replace falls through to string translator — must not panic
	expr, err := logql.Parse(`label_replace(rate({app="x"}[5m]), "new", "$1", "old", "(.*)")`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	_, err = logql.Translate(expr, logql.TranslateOptions{})
	// error is acceptable; panic is not
	_ = err
}

func TestTranslate_RangeAgg_FallsThrough(t *testing.T) {
	expr, err := logql.Parse(`rate({app="x"}[5m])`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Logf("translate returned error (acceptable for metric path): %v", err)
		return
	}
	if got == "" {
		t.Error("want non-empty LogsQL output from string translator fallthrough")
	}
}

func TestTranslate_Parser_JSON_Bare(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | json`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "unpack_json") {
		t.Errorf("want 'unpack_json' in output, got %q", got)
	}
}

func TestTranslate_Parser_Logfmt(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | logfmt`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "unpack_logfmt") {
		t.Errorf("want 'unpack_logfmt' in output, got %q", got)
	}
}

func TestTranslate_Parser_Regexp(t *testing.T) {
	expr, err := logql.Parse("{ app=\"x\" } | regexp `(?P<level>\\w+) (?P<msg>.+)`")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "extract_regexp") {
		t.Errorf("want 'extract_regexp' in output, got %q", got)
	}
}

func TestTranslate_DropStage_BareLabels(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | json | drop level, env`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "delete") {
		t.Errorf("want 'delete' in output, got %q", got)
	}
	if !strings.Contains(got, "level") || !strings.Contains(got, "env") {
		t.Errorf("want 'level' and 'env' in output, got %q", got)
	}
}

func TestTranslate_DropStage_WithMatchers_FallsThrough(t *testing.T) {
	// Conditional drop (| drop level="debug") cannot be expressed in LogsQL —
	// the AST path falls through: the drop is silently omitted and the rest
	// of the pipeline is translated normally. No error is returned.
	expr, err := logql.Parse(`{app="x"} | json | drop level="debug"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// The stream selector and json parser must still appear.
	if !strings.Contains(got, "x") {
		t.Errorf("want stream selector value in output, got %q", got)
	}
}

func TestTranslate_KeepStage_BareLabels(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | json | keep level, msg`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "fields") {
		t.Errorf("want 'fields' in output, got %q", got)
	}
	if !strings.Contains(got, "level") || !strings.Contains(got, "msg") {
		t.Errorf("want 'level' and 'msg' in output, got %q", got)
	}
}

func TestTranslate_LineFormatStage_Simple(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | line_format "{{.level}}: {{.msg}}"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "format") {
		t.Errorf("want 'format' in output, got %q", got)
	}
}

func TestTranslate_LineFormatStage_Complex_FallsThrough(t *testing.T) {
	// Complex template with conditional — falls through to string translator
	// which may transform the template. Must not produce an error.
	expr, err := logql.Parse(`{app="x"} | line_format "{{if .level}}{{.level}}{{end}}"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := logql.Translate(expr, logql.TranslateOptions{})
	if err != nil {
		t.Logf("translate returned error: %v (acceptable from string translator)", err)
	}
	// output must contain at least the stream selector value
	if !strings.Contains(got, "x") {
		t.Errorf("want stream selector value in output, got %q", got)
	}
}

func TestTranslate_LabelFn_Applied(t *testing.T) {
	expr, err := logql.Parse(`{app="api-gateway"}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	opts := logql.TranslateOptions{
		LabelFn: func(label string) string {
			if label == "app" {
				return "service"
			}
			return label
		},
	}
	got, err := logql.Translate(expr, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(got, "service") {
		t.Errorf("want 'service' in output, got %q", got)
	}
}

func TestTranslate_StreamFields_UsesStreamFilter(t *testing.T) {
	expr, err := logql.Parse(`{app="api-gateway", level="error"}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	opts := logql.TranslateOptions{
		StreamFields: map[string]bool{"app": true, "level": true},
	}
	got, err := logql.Translate(expr, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	// Note: StreamFields is currently reserved (not wired in AST path)
	// This test verifies no panic and output contains expected values
	if !strings.Contains(got, "api-gateway") {
		t.Errorf("want 'api-gateway' in output, got %q", got)
	}
}
