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
			name:  "non-empty check holds when any spelling has a value",
			query: `{app="a"} | json | http_method!=""`,
			want:  []string{prefix + `(http_method:!"" OR "http.method":!"" OR "http-method":!"")`},
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

func TestParsedKeyVariantsAreBounded(t *testing.T) {
	if got := parsedKeyVariants("a_b_c"); len(got) != 1<<2+1 { // 4 dot/underscore spellings + hyphens
		t.Errorf("a_b_c: %d variants %v", len(got), got)
	}
	long := "a_b_c_d_e_f_g_h"
	got := parsedKeyVariants(long)
	if len(got) != 3 || got[0] != long || got[1] != "a.b.c.d.e.f.g.h" || got[2] != "a-b-c-d-e-f-g-h" {
		t.Errorf("long name: %v", got)
	}
	if got := parsedKeyVariants("plain"); len(got) != 1 {
		t.Errorf("plain: %v", got)
	}
}
