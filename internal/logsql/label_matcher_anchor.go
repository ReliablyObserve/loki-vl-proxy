package logsql

import "regexp"

// AnchorLabelMatcherRegex wraps a Loki LABEL-matcher regexp so it has to match
// the WHOLE label value.
//
// This is LogQL semantics rather than an option. Loki builds its label matchers
// with Prometheus' labels.NewMatcher, and MatchRegexp compiles the pattern as
// "^(?:" + v + ")$" (prometheus/prometheus, model/labels/regexp.go), so
// {namespace=~"nch"} selects exactly the value "nch" and never "anch" or
// "nch-b". VictoriaLogs' field:~"re" is an UNANCHORED match — measured against
// v1.52.0, app:~"anchor" returns the row whose app is "anchor-test-xyz" —
// so passing the pattern through verbatim silently widens every =~ matcher.
// A widened matcher inflates counts instead of erroring, so nothing surfaces
// it.
//
// The non-capturing group is load-bearing: `^a|b$` parses as `(^a)|(b$)`, so an
// alternation has to be grouped before it is anchored.
//
// Leading inline flags are SCOPED to the body — `(?i)abc` becomes
// `^(?:(?i:abc))$` — never hoisted in front of the anchors: a hoisted `(?m)`
// makes ^ and $ match at line boundaries, which would let a multi-line value
// satisfy a matcher written for one of its lines.
//
// An empty pattern is anchored like any other: Loki's =~"" matches only the
// empty value, so it becomes `^(?:)$` rather than an unanchored match-all.
//
// Line filters (|~) must NOT go through this. Loki's |~ is a substring regexp
// over the log line, which is what VictoriaLogs already does.
func AnchorLabelMatcherRegex(pattern string) string {
	flags, body := splitLeadingRegexFlags(pattern)

	inner := body
	for i := len(flags) - 1; i >= 0; i-- {
		inner = "(?" + flags[i] + ":" + inner + ")"
	}
	return "^(?:" + inner + ")$"
}

// splitLeadingRegexFlags peels the leading inline-flag groups off a pattern and
// returns their flag strings in order plus the remaining body. `(?i)(?s)foo`
// yields (["i", "s"], "foo"). A scoped group such as `(?i:…)` carries its own
// body and is left in place.
func splitLeadingRegexFlags(pattern string) ([]string, string) {
	var flags []string
	rest := pattern
	for {
		m := leadingRegexFlagsRE.FindStringSubmatch(rest)
		if m == nil {
			return flags, rest
		}
		flags = append(flags, m[1])
		rest = rest[len(m[0]):]
	}
}

// leadingRegexFlagsRE matches a leading inline-flag group such as `(?i)`,
// `(?is)`, `(?-s)` or `(?i-s)` and captures the flag string. It deliberately
// does not match `(?i:...)`, which is a scoped group carrying its own body.
var leadingRegexFlagsRE = regexp.MustCompile(`^\(\?([imsU]+(?:-[imsU]+)?|-[imsU]+)\)`)
