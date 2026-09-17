package logsql

// AnchorLabelMatcherRegex wraps a Loki LABEL-matcher regexp so it has to match
// the WHOLE label value.
//
// This is LogQL semantics rather than an option. Loki builds its label matchers
// with Prometheus' labels.NewMatcher, which compiles the pattern as
// "^(?s:" + v + ")$" (prometheus/prometheus, model/labels/regexp.go), so
// {namespace=~"nch"} selects exactly the value "nch" and never "anch" or
// "nch-b". VictoriaLogs' field:~"re" is an UNANCHORED match — measured against
// v1.52.0, app:~"anchor" returns the row whose app is "anchor-test-xyz" — so
// passing the pattern through verbatim silently widens every =~ matcher. A
// widened matcher inflates counts instead of erroring, so nothing surfaces it.
//
// The group is written "(?:" rather than Prometheus' "(?s:" because
// VictoriaLogs compiles with DotNL already on: measured on v1.52.0,
// field:~"a.b" matches "a\nb" and only an explicit field:~"(?-s)a.b" does not.
// The two forms are therefore identical on this backend. Should that default
// ever change, this is the line that has to follow Prometheus.
//
// The non-capturing group is load-bearing: `^a|b$` parses as `(^a)|(b$)`, so an
// alternation has to be grouped before it is anchored. An inline flag in the
// pattern stays inside that group and so cannot reach the anchors: `(?m)foo`
// becomes `^(?:(?m)foo)$`, where the outer `^`/`$` still mean the whole value.
//
// An empty pattern is anchored like any other: Loki's =~"" matches only the
// empty value, so it becomes `^(?:)$` rather than an unanchored match-all.
//
// Line filters (|~) must NOT go through this. Loki's |~ is a substring regexp
// over the log line, which is what VictoriaLogs already does.
func AnchorLabelMatcherRegex(pattern string) string {
	return "^(?:" + pattern + ")$"
}
