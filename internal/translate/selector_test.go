package translate

import (
	"errors"
	"strings"
	"testing"
)

// The stream-selector filter, matcher by matcher, with Loki's selector semantics: a regexp matches the whole
// value with '.' matching newlines (and simplifies to a substring or an equality when it can), an empty value
// matches a missing label, service_name is matched on the value the proxy derives (every row has one, so
// `service_name=""` matches none), detected_level (not a stream field) matches every stream or none, a renamed
// label is matched on its VictoriaLogs field, and a matcher that is not LogQL is a ParseError. The plan shadow
// holds the same builder to the string translator on the whole corpus; this table pins the answers.
//
// conformance: architecture-syntax-package-layering
func TestSelectorFilterMatchers(t *testing.T) {
	dots := func(s string) string { return strings.ReplaceAll(s, "_", ".") }
	for _, c := range []struct {
		name, label, op, value string
		want                   string
	}{
		{"exact", "app", "=", "a", `app:="a"`},
		{"not equal", "app", "!=", "a", `-app:="a"`},
		{"empty value", "app", "=", "", `app:=""`},
		{"empty level", "level", "=", "", `-level:*`},
		{"not empty", "app", "!=", "", `app:!""`},
		{"regexp anchored", "app", "=~", "a.*", `app:~"^(?s:a.*)$"`},
		{"regexp alternatives", "app", "=~", "a|b|c", `app:~"^(?s:a|b|c)$"`},
		{"regexp keeps all", "app", "=~", ".*", `app:~".*"`},
		{"regexp non-empty", "app", "=~", ".+", `app:~".+"`},
		{"regexp substring", "app", "=~", ".*a.*", `app:~"a"`},
		{"regexp flags", "app", "=~", "(?i)a", `app:~"^(?s:(?i)a)$"`},
		{"negated empty regexp", "app", "!~", "", `-app:~"^(?s:)$"`},
		{"renamed label", "k8s_pod_name", "=~", "p.*", `"k8s.pod.name":~"^(?s:p.*)$"`},
		{"dotted field quoted", "a.b", "=", "x", `"a.b":="x"`},
		{"service_name empty matches none", "service_name", "=", "", `-*`},
		{"service_name not empty matches all", "service_name", "!=", "", `*`},
		{"detected_level matches none", "detected_level", "=", "error", `_stream_id:in()`},
		{"detected_level negated matches all", "detected_level", "!=", "info", `*`},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, err := SelectorFilter([]SelectorMatcher{{Name: c.label, Op: c.op, Value: c.value}}, dots, nil)
			if err != nil || f.String() != c.want {
				t.Fatalf("SelectorFilter(%s%s%q) = %v, %v; want %s", c.label, c.op, c.value, f, err, c.want)
			}
		})
	}
	t.Run("service_name derived", func(t *testing.T) {
		for _, c := range []struct{ op, value, prefix, suffix string }{
			{"=", "api", `(service_name:="api" OR (service_name:="" "service.name":="api")`, ""},
			{"=~", "api.*", `(service_name:~"^(?s:api.*)$" OR (service_name:="" "service.name":~"^(?s:api.*)$")`, ""},
			// unknown_service also matches the rows whose every source field is empty: the last branch.
			{"=", UnknownServiceNameValue, `(service_name:="unknown_service" OR`, `"k8s.job.name":="" k8s_job_name:=""))`},
		} {
			f, err := SelectorFilter([]SelectorMatcher{{Name: "service_name", Op: c.op, Value: c.value}}, nil, nil)
			if err != nil || !strings.HasPrefix(f.String(), c.prefix) || !strings.HasSuffix(f.String(), c.suffix) {
				t.Fatalf("service_name%s%q = %v, %v", c.op, c.value, f, err)
			}
		}
	})
	t.Run("stream fields", func(t *testing.T) {
		f, err := SelectorFilter([]SelectorMatcher{
			{Name: "env", Op: "=", Value: "b"}, {Name: "app", Op: "=", Value: "a"}, {Name: "pod", Op: "=", Value: "p"},
			{Name: "app", Op: "!=", Value: "c"}, {Name: "pod", Op: "=~", Value: "p.*"},
		}, nil, map[string]bool{"app": true, "pod": true})
		// Exact matchers on stream fields form one stream filter, first; the others keep the selector's order.
		if want := `{app="a", pod="p"} env:="b" -app:="c" pod:~"^(?s:p.*)$"`; err != nil || f.String() != want {
			t.Fatalf("SelectorFilter = %v, %v; want %s", f, err, want)
		}
	})
	t.Run("errors", func(t *testing.T) {
		for _, m := range []SelectorMatcher{{Name: "app", Op: "~", Value: "x"}, {Name: "detected_level", Op: "=~", Value: "("}} {
			var pe *ParseError
			if _, err := SelectorFilter([]SelectorMatcher{m}, nil, nil); !errors.As(err, &pe) || pe.Pos != -1 {
				t.Fatalf("SelectorFilter(%v) = %v, want a ParseError", m, err)
			}
		}
	})
	t.Run("no matchers keep every row", func(t *testing.T) {
		if f, err := SelectorFilter(nil, nil, nil); err != nil || f.String() != "*" {
			t.Fatalf("SelectorFilter() = %v, %v", f, err)
		}
	})
}

// The field-name normaliser drops the white space around a dot and skips its regexp for a name that has no dot or
// no white space; both ways give the name the regexp alone would.
//
// conformance: architecture-syntax-package-layering
func TestNormalizeFieldIdentifier(t *testing.T) {
	for in, want := range map[string]string{
		"status":         "status",
		"k8s.pod.name":   "k8s.pod.name",
		"k8s . pod.name": "k8s.pod.name",
		"a .b":           "a.b",
		"a. b":           "a.b",
		"a\t.\nb":        "a.b",
		" a b ":          "a b",
		"`a`":            "a",
		`"a . b"`:        "a.b",
		"":               "",
		"  ":             "",
	} {
		if got := NormalizeFieldIdentifier(in); got != want {
			t.Errorf("NormalizeFieldIdentifier(%q) = %q, want %q", in, got, want)
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = NormalizeFieldIdentifier("k8s_pod_name") }); n != 0 {
		t.Errorf("a plain name allocates %v times", n)
	}
}
