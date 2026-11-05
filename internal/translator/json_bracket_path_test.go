package translator

import (
	"strconv"
	"strings"
	"testing"
)

// A Loki JSON path in bracket notation (pkg/logql/log/jsonexpr, as Logs
// Drilldown writes a nested field: ["service"]["name"]) names the field
// VictoriaLogs' unpack_json writes, the nested keys joined with dots; a path
// VictoriaLogs has no field for resolves to "" and is skipped.
// conformance: drilldown/fields-pill-json-path-extracted-empty
func TestResolveJSONBracketPath(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`["api"]`, "api"},
		{`[\"api\"]`, "api"},
		{`["service"]["name"]`, "service.name"},
		{`[\"service\"][\"name\"]`, "service.name"},
		{`["k8s"]["pod"]["name"]`, "k8s.pod.name"},
		{`["service"].name`, "service.name"},
		{`service.name`, "service.name"}, // not bracket notation
		{`plain`, "plain"},
		{`["items"][0]`, ""},             // an array index: unpack_json keeps arrays as JSON text
		{`[0]`, ""},                      // an array at the top
		{`["probe.verb"]`, "probe.verb"}, // unpack_json keeps a dotted key as it is
		{`["a b"]`, ""},                  // a space
		{`[""]`, ""},                     // an empty key
		{`["a"`, ""},                     // unclosed
		{`["a"]["b"`, ""},                // unclosed segment
		{`[\"a\"`, ""},                   // unclosed, escaped
		{`["a\"b"]`, ""},                 // a quote inside the key
	} {
		if got := resolveJSONBracketPath(tc.in); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The Logs Drilldown Fields breakdown over a nested JSON field, with and
// without a level pill: the extraction list gives the label its value as a
// field, and the filter reads that field.
// conformance: drilldown/fields-pill-json-path-extracted-empty
func TestTranslate_DrilldownFieldsJSONPathExtraction(t *testing.T) {
	for _, q := range []string{
		`sum by (service_name_extracted) (count_over_time({app="api"} | detected_level="error" | json service_name_extracted="[\"service\"][\"name\"]" | drop __error__, __error_details__ | service_name_extracted!="" [1m]))`,
		`sum by (service_name_extracted) (count_over_time({app="api"} | json service_name_extracted="[\"service\"][\"name\"]" | drop __error__, __error_details__ | service_name_extracted!="" [1m]))`,
	} {
		got, err := TranslateLogQL(q)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, `| unpack_json | copy "service.name" as service_name_extracted |`) ||
			!strings.Contains(got, `filter "service.name":!""`) || !strings.HasSuffix(got, "| stats by (service_name_extracted) count()") {
			t.Errorf("%s\n=> %s", q, got)
		}
	}
}

// FuzzResolveJSONBracketPath: a bracket path of plain keys, quoted or with
// escaped quotes, resolves to the keys joined with dots; any bracket path
// resolves without panicking to "" or a dotted name without brackets or
// quotes (another input is a plain name, returned as is).
func FuzzResolveJSONBracketPath(f *testing.F) {
	f.Add("service", "name", "", `["a"][0]`)
	f.Add("k8s", "pod", "name", `[\"a\"`)
	f.Fuzz(func(t *testing.T, a, b, c, raw string) {
		if got := resolveJSONBracketPath(raw); strings.HasPrefix(strings.TrimSpace(raw), "[") && strings.ContainsAny(got, `[]"\`) {
			t.Fatalf("%q resolved to %q", raw, got)
		}
		var keys []string
		for _, k := range []string{a, b, c} {
			if k == "" {
				continue
			}
			if strings.ContainsAny(k, "[]\"'\\ ") || !strconv.CanBackquote(k) {
				return
			}
			keys = append(keys, k)
		}
		if len(keys) == 0 {
			return
		}
		var plain, escaped strings.Builder
		for _, k := range keys {
			plain.WriteString("[" + strconv.Quote(k) + "]")
			escaped.WriteString(`[\"` + k + `\"]`)
		}
		want := strings.Join(keys, ".")
		if got := resolveJSONBracketPath(plain.String()); got != want {
			t.Fatalf("%s: %q, want %q", plain.String(), got, want)
		}
		if got := resolveJSONBracketPath(escaped.String()); got != want {
			t.Fatalf("%s: %q, want %q", escaped.String(), got, want)
		}
	})
}
