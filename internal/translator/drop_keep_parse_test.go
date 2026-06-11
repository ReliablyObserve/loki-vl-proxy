package translator

import (
	"fmt"
	"testing"
)

// dkKey renders a DropCondition as "field op value" for order-independent
// comparison without touching the unexported compiled-regexp field.
func dkKey(c DropCondition) string { return fmt.Sprintf("%s %s %s", c.Field, c.Op, c.Value) }

func dkKeys(cs []DropCondition) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, dkKey(c))
	}
	return out
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			return false
		}
		seen[w]--
	}
	return true
}

// TestParseDropKeepConditions_Table exercises the matcher/bare-field split for
// `| drop` and `| keep` stages, including the defense-in-depth behavior that a
// malformed matcher (one the upstream validator would already have rejected) is
// silently skipped while valid items in the same stage are retained — rather
// than panicking or corrupting the surrounding conditions.
func TestParseDropKeepConditions_Table(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantDrop  []string // matcher-form drop conditions
		wantKeep  []string // matcher-form keep conditions
		wantBareD []string
		wantBareK []string
	}{
		{
			name:      "drop bare fields only",
			query:     `{app="api"} | drop trace_id, span_id`,
			wantBareD: []string{"trace_id", "span_id"},
		},
		{
			name:     "drop single matcher",
			query:    `{app="api"} | drop level="debug"`,
			wantDrop: []string{"level = debug"},
		},
		{
			name:     "drop all operators",
			query:    `{app="api"} | drop a="1" | drop b!="2" | drop c=~"3" | drop d!~"4"`,
			wantDrop: []string{"a = 1", "b != 2", "c =~ 3", "d !~ 4"},
		},
		{
			name:      "keep mixed bare and matcher",
			query:     `{app="api"} | json | keep app, status="200"`,
			wantKeep:  []string{"status = 200"},
			wantBareK: []string{"app"},
		},
		{
			name:      "malformed drop matcher skipped, valid items kept",
			query:     `{app="api"} | drop a, level="x", bad!=~"y", b`,
			wantDrop:  []string{"level = x"},
			wantBareD: []string{"a", "b"},
		},
		{
			name:      "malformed keep matcher skipped, valid items kept",
			query:     `{app="api"} | keep app, status="200", bad!=~"z"`,
			wantKeep:  []string{"status = 200"},
			wantBareK: []string{"app"},
		},
		{
			name:  "metric query has no drop/keep",
			query: `sum by (app) (rate({app="api"}[5m]))`,
		},
		{
			name:  "no pipeline",
			query: `{app="api"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dkKeys(ParseDropConditions(tc.query)); !sameSet(got, tc.wantDrop) {
				t.Errorf("ParseDropConditions = %v, want %v", got, tc.wantDrop)
			}
			if got := dkKeys(ParseKeepConditions(tc.query)); !sameSet(got, tc.wantKeep) {
				t.Errorf("ParseKeepConditions = %v, want %v", got, tc.wantKeep)
			}
			if got := ParseBareDropFields(tc.query); !sameSet(got, tc.wantBareD) {
				t.Errorf("ParseBareDropFields = %v, want %v", got, tc.wantBareD)
			}
			if got := ParseBareKeepFields(tc.query); !sameSet(got, tc.wantBareK) {
				t.Errorf("ParseBareKeepFields = %v, want %v", got, tc.wantBareK)
			}
		})
	}
}

// TestDropConditionMatches verifies the matcher predicate semantics used when the
// proxy post-processes drop/keep stages, across all four operators.
func TestDropConditionMatches(t *testing.T) {
	mk := func(field, op, val string) DropCondition {
		c, err := NewDropCondition(field, op, val)
		if err != nil {
			t.Fatalf("NewDropCondition(%q,%q,%q): %v", field, op, val, err)
		}
		return c
	}
	cases := []struct {
		name  string
		cond  DropCondition
		value string
		want  bool
	}{
		{"eq match", mk("level", "=", "debug"), "debug", true},
		{"eq no match", mk("level", "=", "debug"), "info", false},
		{"neq match", mk("level", "!=", "debug"), "info", true},
		{"neq no match", mk("level", "!=", "debug"), "debug", false},
		{"regex match", mk("level", "=~", "deb.*"), "debug", true},
		{"regex no match", mk("level", "=~", "deb.*"), "info", false},
		{"neg regex match", mk("level", "!~", "deb.*"), "info", true},
		{"neg regex no match", mk("level", "!~", "deb.*"), "debug", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cond.Matches(tc.value); got != tc.want {
				t.Errorf("%s.Matches(%q) = %v, want %v", dkKey(tc.cond), tc.value, got, tc.want)
			}
		})
	}
}

// FuzzParseDropKeepConsistency feeds arbitrary queries through all four
// drop/keep parsers and asserts they never panic and never return an internally
// inconsistent result (a condition with an empty field or unknown operator, or
// an empty bare-field name).
func FuzzParseDropKeepConsistency(f *testing.F) {
	seeds := []string{
		`{app="api"} | drop a, level="x", bad!=~"y", b`,
		`{app="api"} | keep app, status="200", bad!=~"z"`,
		`{app="api"} | drop level=~"a|b" | keep c!="d"`,
		`{app="api"} | json | drop __error__, level="debug" | logfmt | keep msg`,
		`{app="api"} | drop`,
		`{app="api"} | keep =`,
		`{app="api"} | drop ="value"`,
		`| drop x`,
		``,
		`{app="api"}`,
		`rate({app="api"}[5m])`,
		`{app="api"} | drop "quoted", weird===, a=b=c`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		drops := ParseDropConditions(query)
		keeps := ParseKeepConditions(query)
		bareD := ParseBareDropFields(query)
		bareK := ParseBareKeepFields(query)

		for _, set := range [][]DropCondition{drops, keeps} {
			for _, c := range set {
				if c.Field == "" {
					t.Fatalf("condition with empty field from %q", query)
				}
				switch c.Op {
				case "=", "!=", "=~", "!~":
				default:
					t.Fatalf("unknown op %q from %q", c.Op, query)
				}
				// Matches must never panic on arbitrary input.
				_ = c.Matches("")
				_ = c.Matches(c.Value)
				_ = c.Matches("arbitrary_xyz")
			}
		}
		// Bare field names are never empty. (The same field name may legitimately
		// appear as a bare field in one stage and a matcher in another, so the two
		// outputs are not required to be disjoint.)
		for _, b := range append(append([]string{}, bareD...), bareK...) {
			if b == "" {
				t.Fatalf("empty bare field from %q", query)
			}
		}
	})
}
