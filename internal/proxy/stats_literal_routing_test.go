package proxy

import (
	"reflect"
	"strconv"
	"testing"
)

func TestStatsRoutingIgnoresEscapedQuotesAndLiteralPipes(t *testing.T) {
	for _, tc := range []struct {
		query string
		stats bool
	}{
		{`path:="quote\"slash\\" | stats count()`, true},
		{`path:="quote\" | stats count()"`, false},
		{`path:="quote\" | stats count()" | stats count()`, true},
	} {
		if got := isStatsQueryHeuristic(tc.query); got != tc.stats {
			t.Fatalf("query=%s got=%v", tc.query, got)
		}
		if got := isStatsQuery(tc.query); got != tc.stats {
			t.Fatalf("typed query=%s got=%v", tc.query, got)
		}
	}
}

func TestStreamIdentityPreservesEscapedLabelValues(t *testing.T) {
	for _, value := range []string{`quote"slash\`, `comma",next=still_value`, "raw\rseparator\u2028", "Łódź 日本語"} {
		stream := `{kind="first",path=` + strconv.Quote(value) + `,service_name="after"}`
		want := map[string]string{"kind": "first", "path": value, "service_name": "after"}
		if got := parseStreamLabelsUncached(stream); !reflect.DeepEqual(got, want) {
			t.Fatalf("stream=%s labels=%v want=%v", stream, got, want)
		}
	}
}
