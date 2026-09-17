package logsql

import (
	"regexp"
	"testing"
)

// TestAnchorLabelMatcherRegex locks the Loki label-matcher contract: a label
// regex must match the WHOLE label value. VictoriaLogs' `field:~"re"` is an
// unanchored match, so an un-anchored pattern silently widens every `=~`
// matcher — and a widened matcher inflates counts instead of erroring.
func TestAnchorLabelMatcherRegex(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			// The reported case: `nch` must not match `anch` or `nch-b`.
			name: "plain literal",
			in:   "nch",
			want: "^(?:nch)$",
		},
		{
			// The group is load-bearing: "^a|b$" parses as "(^a)|(b$)", which
			// would match any value merely CONTAINING a or ending in b.
			name: "alternation is grouped, not split by the anchors",
			in:   "a|b",
			want: "^(?:a|b)$",
		},
		{
			name: "multi-branch alternation",
			in:   "prod|staging|dev",
			want: "^(?:prod|staging|dev)$",
		},
		{
			name: "match-all",
			in:   ".*",
			want: "^(?:.*)$",
		},
		{
			name: "prefix wildcard",
			in:   "api-.*",
			want: "^(?:api-.*)$",
		},
		{
			// Flags are SCOPED to the body, never hoisted in front of the
			// anchors — see the multiline case below for why.
			name: "case-insensitive flag is scoped to the body",
			in:   "(?i)prod",
			want: "^(?:(?i:prod))$",
		},
		{
			name: "multiple flags in one group stay together",
			in:   "(?is)prod",
			want: "^(?:(?is:prod))$",
		},
		{
			// The reason flags must not be hoisted: a hoisted (?m) makes ^ and $
			// match at line boundaries, so "foo\nbar" would pass a matcher for
			// "foo". Scoped, the anchors keep meaning "the whole value".
			name: "multiline flag is scoped so the anchors stay value-anchored",
			in:   "(?m)foo",
			want: "^(?:(?m:foo))$",
		},
		{
			name: "successive flag groups nest in order",
			in:   "(?i)(?s)foo",
			want: "^(?:(?i:(?s:foo)))$",
		},
		{
			name: "negated flag group",
			in:   "(?-s)foo",
			want: "^(?:(?-s:foo))$",
		},
		{
			// A scoped group carries its own body and must stay inside.
			name: "scoped flag group is not hoisted",
			in:   "(?i:prod)",
			want: "^(?:(?i:prod))$",
		},
		{
			name: "already anchored by the author is anchored again, harmlessly",
			in:   "^prod$",
			want: "^(?:^prod$)$",
		},
		{
			name: "character class",
			in:   "kafka-[23]",
			want: "^(?:kafka-[23])$",
		},
		{
			// An empty pattern is still a matcher: Loki's `=~""` matches only
			// the empty value. Returning it unanchored made it match anything.
			name: "empty pattern anchors to the empty value",
			in:   "",
			want: "^(?:)$",
		},
		{
			name: "flags with no body anchor to the empty value",
			in:   "(?i)",
			want: "^(?:(?i:))$",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AnchorLabelMatcherRegex(tc.in)
			if got != tc.want {
				t.Errorf("AnchorLabelMatcherRegex(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if got != "" {
				if _, err := regexp.Compile(got); err != nil {
					t.Errorf("AnchorLabelMatcherRegex(%q) = %q, which does not compile: %v", tc.in, got, err)
				}
			}
		})
	}
}

// TestAnchorLabelMatcherRegexMatchesLokiSemantics is the behavioural half: the
// anchored pattern must accept exactly the values Loki's fully-anchored matcher
// accepts. Table-driven over (pattern, value, shouldMatch) so a future
// "optimisation" of the wrapper cannot quietly change the matched set.
func TestAnchorLabelMatcherRegexMatchesLokiSemantics(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		// The exact reported defect.
		{"nch", "nch", true},
		{"nch", "anch", false},
		{"nch", "nch-b", false},
		{"anch", "anch", true},
		{"anch", "anch-b", false},

		// Alternation must not leak into a prefix/suffix match.
		{"a|b", "a", true},
		{"a|b", "b", true},
		{"a|b", "ab", false},
		{"a|b", "xa", false},
		{"a|b", "bx", false},
		{"prod|staging", "prod", true},
		{"prod|staging", "production", false},

		// Wildcards.
		{".*", "", true},
		{".*", "anything", true},
		{"api-.*", "api-gw", true},
		{"api-.*", "my-api-gw", false},

		// Flags still apply once scoped to the body.
		{"(?i)prod", "PROD", true},
		{"(?i)prod", "prod", true},
		{"(?i)prod", "production", false},

		// The multiline hole: a value with a newline must never satisfy a
		// matcher for one of its lines, whatever flags the author set.
		{"(?m)foo", "foo", true},
		{"(?m)foo", "foo\nbar", false},
		{"(?m)foo", "bar\nfoo", false},
		{"(?s)foo", "foo\nbar", false},
		{"foo", "foo\nbar", false},

		// Empty patterns match only the empty value.
		{"", "", true},
		{"", "anything", false},
		{"(?i)", "", true},
		{"(?i)", "x", false},

		// Character class.
		{"kafka-[23]", "kafka-2", true},
		{"kafka-[23]", "kafka-4", false},
		{"kafka-[23]", "my-kafka-2", false},
	}

	for _, tc := range tests {
		anchored := AnchorLabelMatcherRegex(tc.pattern)
		re, err := regexp.Compile(anchored)
		if err != nil {
			t.Fatalf("pattern %q -> %q does not compile: %v", tc.pattern, anchored, err)
		}
		if got := re.MatchString(tc.value); got != tc.want {
			t.Errorf("pattern %q (anchored %q) against %q = %v, want %v",
				tc.pattern, anchored, tc.value, got, tc.want)
		}
	}
}
