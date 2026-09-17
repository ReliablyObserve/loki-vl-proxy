package translator

import "testing"

// TestLabelMatchersAreAnchoredLineFiltersAreNot locks the boundary between the
// two kinds of regex in LogQL:
//
//   - a LABEL matcher (`{label=~"re"}`, `| label =~ "re"`) is fully anchored in
//     Loki — it must match the whole label value;
//   - a LINE filter (`|~ "re"`, `!~ "re"`) is a substring regex over the log
//     line, which is what VictoriaLogs already does.
//
// Getting this backwards is silent in both directions: an unanchored label
// matcher inflates results, an anchored line filter drops them.
func TestLabelMatchersAreAnchoredLineFiltersAreNot(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "stream selector =~ is anchored",
			in:   `{namespace=~"nch"}`,
			want: `namespace:~"^(?:nch)$"`,
		},
		{
			name: "stream selector !~ is anchored and negated",
			in:   `{namespace!~"kube-.*"}`,
			want: `-namespace:~"^(?:kube-.*)$"`,
		},
		{
			name: "alternation is grouped inside the anchors",
			in:   `{namespace=~"prod|staging"}`,
			want: `namespace:~"^(?:prod|staging)$"`,
		},
		{
			name: "match-all",
			in:   `{namespace=~".*"}`,
			want: `namespace:~"^(?:.*)$"`,
		},
		{
			// An inline flag stays inside the group: hoisted, a (?m) would
			// make the anchors match at line boundaries.
			name: "inline flag scoped inside the anchors",
			in:   `{namespace=~"(?i)prod"}`,
			want: `namespace:~"^(?:(?i)prod)$"`,
		},
		{
			name: "multiline flag cannot widen the anchors",
			in:   `{namespace=~"(?m)prod"}`,
			want: `namespace:~"^(?:(?m)prod)$"`,
		},
		{
			name: "exact matcher is untouched",
			in:   `{namespace="prod"}`,
			want: `namespace:="prod"`,
		},
		{
			// The discriminating case: both kinds in one query.
			name: "label matcher anchored, line filter left unanchored",
			in:   `{app=~"a|b"} |~ "substring"`,
			want: `app:~"^(?:a|b)$" ~"substring"`,
		},
		{
			name: "negative line filter stays unanchored",
			in:   `{app="x"} !~ "noise"`,
			want: `app:="x" NOT ~"noise"`,
		},
		{
			name: "pipeline label filter =~ is anchored",
			in:   `{app="x"} | json | status =~ "4|5"`,
			want: `app:="x" | unpack_json | filter status:~"^(?:4|5)$"`,
		},
		{
			name: "pipeline label filter !~ is anchored",
			in:   `{app="x"} | json | status !~ "2.."`,
			want: `app:="x" | unpack_json | filter -status:~"^(?:2..)$"`,
		},
		{
			name: "anchoring survives a metric wrapper",
			in:   `sum(count_over_time({namespace=~"a|b"}[5m]))`,
			want: `namespace:~"^(?:a|b)$" | stats count()`,
		},
		{
			name: "backtick-quoted label regex is anchored too",
			in:   "{namespace=~`api-.*`}",
			want: `namespace:~"^(?:api-.*)$"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.in)
			if err != nil {
				t.Fatalf("TranslateLogQL(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("TranslateLogQL(%q)\n got: %s\nwant: %s", tc.in, got, tc.want)
			}
		})
	}
}
