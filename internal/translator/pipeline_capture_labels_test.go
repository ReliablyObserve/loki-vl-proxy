package translator

import (
	"strings"
	"testing"
)

func TestRegexpCaptureFiltersPreserveQueryLocalNames(t *testing.T) {
	mapLabel := func(label string) string { return strings.ReplaceAll(label, "_", ".") }
	for _, tc := range []struct {
		name, query, want string
	}{
		{
			"named_capture",
			`{app="a"} | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`,
			`app:="a" | extract_regexp "(?P<http_method>[A-Z]+)" | filter http_method:="GET"`,
		},
		{
			"raw_capture_and_backend_field",
			"{app=\"a\"} | regexp `(?<http_method>[A-Z]+)` | http_method=\"GET\" and http_status=\"200\"",
			`app:="a" | extract_regexp "(?<http_method>[A-Z]+)" | filter (http_method:="GET" and "http.status":="200")`,
		},
		{
			"selector_keeps_backend_mapping",
			`{http_status="200"} | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`,
			`"http.status":="200" | extract_regexp "(?P<http_method>[A-Z]+)" | filter http_method:="GET"`,
		},
		{
			"earlier_filter_keeps_backend_mapping",
			`{app="a"} | http_method="POST" | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`,
			`app:="a" "http.method":="POST" | extract_regexp "(?P<http_method>[A-Z]+)" | filter http_method:="GET"`,
		},
		{
			"multiple_captures",
			`{app="a"} | regexp "(?P<http_method>[A-Z]+) (?P<http_status>[0-9]+)" | http_method="GET" | http_status="200"`,
			`app:="a" | extract_regexp "(?P<http_method>[A-Z]+) (?P<http_status>[0-9]+)" | filter http_method:="GET" | filter http_status:="200"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQLWithLabels(tc.query, mapLabel)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestRegexpCaptureAliasesDoNotLeakToOtherQueries(t *testing.T) {
	mapLabel := func(label string) string { return strings.ReplaceAll(label, "_", ".") }
	for _, query := range []string{
		`{app="a"} | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`,
		`{app="a"} | http_method="GET"`,
	} {
		got, err := TranslateLogQLWithLabels(query, mapLabel)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(query, "regexp") && !strings.Contains(got, `"http.method":="GET"`) {
			t.Fatalf("capture alias leaked: %s", got)
		}
	}
}
