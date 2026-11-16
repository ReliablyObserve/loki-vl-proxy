package logsql

import "testing"

// SelectionRowCount keeps the filter of a query and drops its pipes; a query the parser does not read is counted
// whole.
func TestSelectionRowCount(t *testing.T) {
	for q, want := range map[string]string{
		`app:="a" | unpack_json | stats by (x) count()`:        `app:="a" | stats count() as n`,
		`app:="a" "err" -"skip" | unpack_logfmt | math x as y`: `app:="a" "err" -"skip" | stats count() as n`,
		`app:="a" | (`: `app:="a" | ( | stats count() as n`,
	} {
		if got := SelectionRowCount(q, "n"); got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
}
