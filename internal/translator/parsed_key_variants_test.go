package translator

import (
	"strings"
	"testing"
)

// conformance: profiles/sanitized-json-key-filter
func TestParsedKeyFilterMatchesEverySpellingOfASanitizedLabel(t *testing.T) {
	identity := func(s string) string { return s }
	const prefix = `app:="a" | unpack_json | filter `
	for _, tc := range []struct {
		name, query string
		want        []string // substrings the translation must contain
		notWant     []string
	}{
		{
			name:  "equality holds when any spelling matches",
			query: `{app="a"} | json | http_method="GET"`,
			want:  []string{prefix + `(http_method:="GET" OR "http.method":="GET" OR "http-method":="GET")`},
		},
		{
			name:  "inequality accepts an absent label, so every spelling must differ",
			query: `{app="a"} | json | http_method!="GET"`,
			want:  []string{prefix + `(-http_method:="GET" -"http.method":="GET" -"http-method":="GET")`},
		},
		{
			name:  "empty equality holds only when every spelling is empty",
			query: `{app="a"} | json | http_method=""`,
			want:  []string{prefix + `(http_method:="" "http.method":="" "http-method":="")`},
		},
		{
			name:    "an existence check keeps its single key for the Drilldown fast paths",
			query:   `{app="a"} | json | http_method!=""`,
			want:    []string{prefix + `http_method:!""`},
			notWant: []string{" OR "},
		},
		{
			name:  "a regexp that accepts the empty value needs every spelling to accept it",
			query: `{app="a"} | json | http_method=~"GET|"`,
			want:  []string{prefix + `(http_method:~"GET|" "http.method":~"GET|" "http-method":~"GET|")`},
		},
		{
			name:  "comparisons reject an absent label, any spelling may satisfy them",
			query: `{app="a"} | json | http_status_code>=400`,
			want:  []string{`"http.status_code":>=400`, `"http_status.code":>=400`, `"http.status.code":>=400`, ` OR `},
		},
		{
			name:  "logfmt keys are sanitized the same way",
			query: `{app="a"} | logfmt | http_method="GET"`,
			want:  []string{`| unpack_logfmt | filter (http_method:="GET" OR "http.method":="GET"`},
		},
		{
			name:  "a chain of filters expands each operand",
			query: `{app="a"} | json | http_method="GET" or msg="x"`,
			want:  []string{`(http_method:="GET" OR "http.method":="GET" OR "http-method":="GET") or msg:="x"`},
		},
		{
			name:    "a name without an underscore is one key",
			query:   `{app="a"} | json | method="GET"`,
			want:    []string{prefix + `method:="GET"`},
			notWant: []string{" OR "},
		},
		{
			name:    "no parser, no sanitized keys",
			query:   `{app="a"} | http_method="GET"`,
			notWant: []string{`"http.method"`},
		},
		{
			name:    "a json expression names the original key",
			query:   `{app="a"} | json http_method="http.method" | http_method="GET"`,
			notWant: []string{`"http-method"`, `"http_method"`},
		},
		{
			name:    "regexp captures are query-local names",
			query:   `{app="a"} | json | regexp "(?P<http_method>[A-Z]+)" | http_method="GET"`,
			notWant: []string{`"http.method"`},
		},
		{
			name:    "pattern captures are query-local names",
			query:   `{app="a"} | json | pattern "<dst_ip> <_>" | dst_ip="x"`,
			notWant: []string{`"dst.ip"`, `"dst-ip"`},
		},
		{
			name:    "label_format destinations are query-local names",
			query:   `{app="a"} | json | label_format dst_ip="{{.a}}" | dst_ip="x"`,
			notWant: []string{`"dst.ip"`, `"dst-ip"`},
		},
		{
			name:    "the error labels are not keys",
			query:   `{app="a"} | json | __error__=""`,
			notWant: []string{`"_.error__"`, ` OR `},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQLWithLabels(tc.query, identity)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("%s\n got: %s\nwant substring: %s", tc.query, got, w)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(got, w) {
					t.Errorf("%s\n got: %s\nunwanted substring: %s", tc.query, got, w)
				}
			}
		})
	}
}

func TestParsedKeyFilterKeepsMappedLabels(t *testing.T) {
	// A label the translation maps names a known VictoriaLogs field.
	got, err := TranslateLogQLWithLabels(`{app="a"} | json | k8s_pod_name="p"`, func(s string) string {
		return strings.ReplaceAll(s, "_", ".")
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := `app:="a" | unpack_json | filter "k8s.pod.name":="p"`; got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestParsedKeyFilterQuotesFieldNamesInIPFilters(t *testing.T) {
	got, err := TranslateLogQLWithLabels(`{app="a"} | json | client_addr=ip("10.0.0.0/8")`, func(s string) string { return s })
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{` client.addr:`, `(client.addr:`, ` client-addr:`, `""client`} {
		if strings.Contains(got, bad) {
			t.Errorf("unquoted or double-quoted field name %q in %s", bad, got)
		}
	}
	if !strings.Contains(got, `"client.addr":`) || !strings.Contains(got, `"client-addr":`) {
		t.Errorf("missing quoted spellings in %s", got)
	}
}

// Grouping by or unwrapping a sanitized key reads the value as a field: a pipe
// after the parser copies it from the original spelling; a json expression
// copies its path.
//
// conformance: profiles/sanitized-json-key-grouping
func TestParsedKeyGroupingAndUnwrapReadOriginalKeys(t *testing.T) {
	identity := func(s string) string { return s }
	for _, tc := range []struct {
		name, query string
		want        []string
		notWant     []string
	}{
		{
			name:  "by() of a sanitized key",
			query: `sum by (http_method) (count_over_time({app="a"} | json | http_method!="" [1m]))`,
			want: []string{
				`| format if ("http.method":*) "<http.method>" as http_method keep_original_fields | format if ("http-method":*) "<http-method>" as http_method keep_original_fields | filter http_method:!"" | stats by (http_method) count()`,
				`stats by (http_method) count()`,
			},
		},
		{
			name:  "unwrap of a sanitized key",
			query: `sum(sum_over_time({app="a"} | json | unwrap http_code [1m]))`,
			want:  []string{`format if ("http.code":*) "<http.code>" as http_code`, `| filter http_code:*`, `sum(http_code)`},
		},
		{
			name:  "logfmt keys resolve too",
			query: `sum by (http_method) (count_over_time({app="a"} | logfmt [1m]))`,
			want:  []string{`| unpack_logfmt | format if ("http.method":*) "<http.method>" as http_method keep_original_fields`},
		},
		{
			name:  "json expression copies its path",
			query: `sum by (a) (count_over_time({app="a"} | json a="[\"probe.verb\"]" | a!="" [1m]))`,
			want:  []string{`| unpack_json | copy "probe.verb" as a |`, `stats by (a) count()`},
		},
		{
			name:  "json expression feeding unwrap",
			query: `sum(sum_over_time({app="a"} | json c="[\"probe.code\"]" | unwrap c [1m]))`,
			want:  []string{`| copy "probe.code" as c | filter c:*`, `sum(c)`},
		},
		{
			name:    "a name without an underscore is one key",
			query:   `sum by (method) (count_over_time({app="a"} | json [1m]))`,
			notWant: []string{`format if`},
		},
		{
			name:    "pattern captures are not keys",
			query:   `sum by (dst_ip) (count_over_time({app="a"} | json | pattern "<dst_ip> x" [1m]))`,
			notWant: []string{`format if`},
		},
		{
			name:  "a binary expression resolves each operand on its own",
			query: `sum(sum_over_time({app="a"} | json | unwrap resp_time [1m])) / sum(count_over_time({app="a"} | json [1m]))`,
			want:  []string{`| filter resp_time:* | stats by (_stream, _msg) sum(resp_time) as __lvp_inner | stats sum(__lvp_inner)|||app:="a" | unpack_json | stats count()`},
		},
		{
			name:    "a label a later stage defines is left to that stage",
			query:   `sum(sum_over_time({app="a"} | json | label_format http_code="{{.status}}" | unwrap http_code [1m]))`,
			notWant: []string{`format if`, `filter http_code:*`},
		},
		{
			name:  "a quoted parser name in a line filter does not split a pipe",
			query: `sum by (a_b) (count_over_time({app="a"} |= "| unpack_json" | json [1m]))`,
			want:  []string{`~"\\| unpack_json" | unpack_json | format if ("a.b":*) "<a.b>" as a_b keep_original_fields`},
		},
		{
			name:  "a detected_level grouping keeps the derived-level pipes intact",
			query: `sum by (detected_level, http_method) (count_over_time({app="a"} | json [1m]))`,
			want:  []string{`| unpack_json | format if ("http.method":*) "<http.method>" as http_method keep_original_fields`, `stats by (detected_level, http_method) count()`},
		},
		{
			name:    "a json expression named like the grouping label is not overwritten",
			query:   `sum by (a_b) (count_over_time({app="a"} | json a_b="x" [1m]))`,
			want:    []string{`| copy "x" as a_b | stats by (a_b) count()`},
			notWant: []string{`format if`},
		},
		{
			name:    "no parser, nothing to resolve",
			query:   `sum by (http_method) (count_over_time({app="a"} [1m]))`,
			notWant: []string{`format if`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQLWithLabels(tc.query, identity)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("%s\n got: %s\nwant substring: %s", tc.query, got, w)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(got, w) {
					t.Errorf("%s\n got: %s\nunwanted substring: %s", tc.query, got, w)
				}
			}
		})
	}
}

func TestParsedKeyVariantsAreBounded(t *testing.T) {
	if got := parsedKeyVariants("a_b_c"); len(got) != 1<<2+1 { // 4 dot/underscore spellings + hyphens
		t.Errorf("a_b_c: %d variants %v", len(got), got)
	}
	long := "a_b_c_d_e_f_g_h"
	got := parsedKeyVariants(long)
	if len(got) != 3 || got[0] != long || got[1] != "a.b.c.d.e.f.g.h" || got[2] != "a-b-c-d-e-f-g-h" {
		t.Errorf("long name: %v", got)
	}
	// A candidate with adjacent, leading or trailing dots would sanitize to a
	// different label, so it is not offered.
	for _, label := range []string{"a__b", "status_", "a_b_"} {
		for _, v := range parsedKeyVariants(label) {
			if strings.Contains(v, "..") || strings.HasPrefix(v, ".") || strings.HasSuffix(v, ".") {
				t.Errorf("%s: candidate %q", label, v)
			}
		}
	}
	if got := parsedKeyVariants("plain"); len(got) != 1 {
		t.Errorf("plain: %v", got)
	}
}
