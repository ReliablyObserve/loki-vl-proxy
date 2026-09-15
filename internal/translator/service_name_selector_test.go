package translator

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// deriveServiceNameForTest is the read-path service_name derivation:
// the first non-empty field in priority order, else unknown_service.
func deriveServiceNameForTest(stream map[string]string) string {
	for _, field := range syntheticServiceNameFields {
		if v := strings.TrimSpace(stream[field]); v != "" {
			return v
		}
	}
	return unknownServiceNameValue
}

// splitTopLevelForTest splits s on sep outside double-quoted strings and
// parentheses.
func splitTopLevelForTest(s, sep string) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			for i++; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' {
					i++
				}
			}
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && strings.HasPrefix(s[i:], sep) {
				parts = append(parts, s[start:i])
				start = i + len(sep)
				i += len(sep) - 1
			}
		}
	}
	return append(parts, s[start:])
}

// evalServiceNameFilterForTest evaluates a service_name selector filter against
// one row with VictoriaLogs semantics: a missing field reads as empty, `:=` is
// exact, `:~` is an (unanchored) RE2 search, `:!""` means non-empty.
func evalServiceNameFilterForTest(t *testing.T, filter string, row map[string]string) bool {
	t.Helper()
	switch filter {
	case "*":
		return true
	case matchNoStreamsFilter:
		return false
	}
	negate := strings.HasPrefix(filter, "-")
	body := strings.TrimPrefix(filter, "-")
	if !strings.HasPrefix(body, "(") || !strings.HasSuffix(body, ")") {
		t.Fatalf("service_name matcher must be one parenthesised filter, got %q", filter)
	}
	matched := false
	for _, branch := range splitTopLevelForTest(body[1:len(body)-1], " OR ") {
		branch = strings.TrimSuffix(strings.TrimPrefix(branch, "("), ")")
		ok := true
		for _, term := range splitTopLevelForTest(branch, " ") {
			var name, op, raw string
			negate := strings.HasPrefix(term, "-")
			term = strings.TrimPrefix(term, "-")
			for _, candidate := range []string{":=", ":~", ":!"} {
				if idx := strings.LastIndex(term, candidate); idx > 0 {
					name, op, raw = term[:idx], candidate, term[idx+len(candidate):]
					break
				}
			}
			if unquoted, err := strconv.Unquote(name); err == nil {
				name = unquoted
			}
			value, err := strconv.Unquote(raw)
			if err != nil || strings.Contains(name, "(") {
				t.Fatalf("unexpected term %q in %q", term, filter)
			}
			got := row[name]
			switch op {
			case ":=":
				ok = got == value
			case ":~":
				ok = regexp.MustCompile(value).MatchString(got)
			case ":!":
				ok = got != value
			}
			if negate {
				ok = !ok
			}
			if !ok {
				break
			}
		}
		if ok {
			matched = true
			break
		}
	}
	return matched != negate
}

func lokiMatcherMatchesForTest(op, value, derived string) bool {
	switch op {
	case "=":
		return derived == value
	case "!=":
		return derived != value
	case "=~":
		return regexp.MustCompile("^(?s:" + value + ")$").MatchString(derived)
	case "!~":
		return !regexp.MustCompile("^(?s:" + value + ")$").MatchString(derived)
	}
	panic(op)
}

// TestServiceNameSelectorMatchesDerivedLabel locks the contract that a
// `{service_name<op>"v"}` selector returns a stream if and only if the proxy
// derives a service_name for that row satisfying the matcher.
func TestServiceNameSelectorMatchesDerivedLabel(t *testing.T) {
	streams := []map[string]string{
		{"service_name": "other"},
		{"service_name": "other", "service.name": "checkout"},
		{"service.name": "checkout"},
		{"service.name": "checkout", "app": "web"},
		{"app": "checkout", "container": "web"},
		{"app": "web", "container": "checkout"},
		{"container": "checkout"},
		{"k8s.container.name": "checkout"},
		{"k8s_job_name": "checkout"},
		{"job": "ns/checkout"},
		{"namespace": "prod"},
		{},
		{"app": `a"b\c`},
	}
	matchers := []struct{ op, value string }{
		{"=", "checkout"},
		{"=", "other"},
		{"=", "web"},
		{"=", "unknown_service"},
		{"=", ""},
		{"=", `a"b\c`},
		{"!=", "checkout"},
		{"!=", ""},
		{"=~", "check.*"},
		{"=~", ".+"},
		{"=~", ".*"},
		{"=~", "checkout|"},
		{"=~", "unknown.*"},
		{"!~", "check.*"},
		{"!~", ".*"},
	}
	for _, m := range matchers {
		selector := `service_name` + m.op + strconv.Quote(m.value)
		filter := streamMatcherToFieldFilter(selector, func(label string) string {
			if label == "service_name" {
				return "service.name"
			}
			return label
		})
		for _, stream := range streams {
			want := lokiMatcherMatchesForTest(m.op, m.value, deriveServiceNameForTest(stream))
			if got := evalServiceNameFilterForTest(t, filter, stream); got != want {
				t.Errorf("%s on stream %v: got match=%v want %v (derived=%q)\nfilter: %s", selector, stream, got, want, deriveServiceNameForTest(stream), filter)
			}
		}
	}
}

func TestServiceNameSelectorBacktickRegex(t *testing.T) {
	filter := streamMatcherToFieldFilter("service_name=~`check\\w+`", nil)
	for stream, want := range map[string]bool{"checkout": true, "check": false} {
		if got := evalServiceNameFilterForTest(t, filter, map[string]string{"app": stream}); got != want {
			t.Errorf("stream app=%q: got %v want %v (filter %s)", stream, got, want, filter)
		}
	}
}

func TestServiceNameLabelFilterStageUsesDerivedMatcher(t *testing.T) {
	for _, tc := range []struct{ logql, want string }{
		{`{app="api"} | service_name="checkout"`, `app:="api" ` + serviceNameMatcherFilter(`"checkout"`, false, false)},
		{`{app="api"} | logfmt | service_name!="checkout"`, `app:="api" | unpack_logfmt | filter ` + serviceNameMatcherFilter(`"checkout"`, true, false)},
	} {
		got, err := TranslateLogQLWithLabels(tc.logql, nil)
		if err != nil || got != tc.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", tc.logql, got, err, tc.want)
		}
	}
}
