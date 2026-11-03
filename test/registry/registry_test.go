package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const repoRegistry = "../../conformance/registry"

func TestRegistryCasesLoadStrictly(t *testing.T) {
	cases, problems, err := LoadAll(repoRegistry)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
	if len(cases) < 200 {
		t.Fatalf("loaded %d cases, the registry holds more than 200", len(cases))
	}
}

func TestParseIsStrict(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		want string
	}{
		"colon in an unquoted title":   {"title: a: b\n", "mapping values are not allowed"},
		"backtick starts a token":      {"title: `x`\n", "cannot start any token"},
		"duplicate key":                {"a: 1\na: 2\n", "already defined"},
		"unquoted flow item with [":    {"fields: [a, b[].c]\n", "expected ',' or ']'"},
		"two documents":                {"a: 1\n---\nb: 2\n", "more than one"},
		"not a mapping":                {"- a\n- b\n", "not a mapping"},
		"unquoted timestamp":           {"since: 2026-10-27\n", "unquoted timestamp"},
		"non-string key":               {"1: a\n", "key that is not a string"},
		"unquoted timestamp in a list": {"x:\n  - 2026-10-27\n", "unquoted timestamp"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.in))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Parse(%q) error = %v, want one containing %q", tc.in, err, tc.want)
			}
		})
	}
	doc, err := Parse([]byte("title: 'a: `b` [c]'\nfields: ['x[].y']\nn: 3\n"))
	if err != nil || doc["title"] != "a: `b` [c]" {
		t.Fatalf("quoted values must load: %v %v", doc, err)
	}
}

func TestLoadSchemaRejectsUnsupportedKeyword(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	for _, body := range []string{
		`{"type":"object","properties":{"a":{"type":"string","format":"date"}}}`,
		`{"type":"object","patternProperties":{"^a":{"type":"string"}}}`,
		`{"allOf":[{"type":"string"}]}`,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSchema(path); err == nil || !strings.Contains(err.Error(), "unsupported keyword") {
			t.Fatalf("schema %s: error = %v, want unsupported keyword", body, err)
		}
	}
}

func TestSchemaValidate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	schema := `{
	  "type":"object","required":["id"],"additionalProperties":false,
	  "properties":{
	    "id":{"type":"string","pattern":"^[a-z]+$"},
	    "n":{"type":"integer","minimum":1,"maximum":3},
	    "tags":{"type":"array","minItems":1,"items":{"$ref":"#/definitions/tag"}},
	    "kind":{"type":"string","enum":["a","b"]},
	    "either":{"type":["string","null"]}
	  },
	  "anyOf":[{"required":["n"]},{"required":["tags"]}],
	  "definitions":{"tag":{"type":"string","minLength":2}}
	}`
	if err := os.WriteFile(path, []byte(schema), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadSchema(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		doc  string
		want string // empty: valid
	}{
		"valid":                 {"id: abc\nn: 2\n", ""},
		"valid by the other":    {"id: abc\ntags: [xy]\neither: null\n", ""},
		"missing required":      {"n: 1\n", `missing required key "id"`},
		"unknown key":           {"id: abc\nn: 1\nzzz: 1\n", `unknown key "zzz"`},
		"wrong type":            {"id: 5\nn: 1\n", "$.id: is integer, want string"},
		"pattern":               {"id: ABC\nn: 1\n", "does not match"},
		"below minimum":         {"id: abc\nn: 0\n", "is below 1"},
		"above maximum":         {"id: abc\nn: 4\n", "is above 3"},
		"enum":                  {"id: abc\nn: 1\nkind: c\n", "is not one of"},
		"empty array":           {"id: abc\ntags: []\n", "want at least 1"},
		"ref and item":          {"id: abc\ntags: [x]\n", "$.tags[0]: is shorter than 2"},
		"anyOf matches none":    {"id: abc\n", "anyOf alternatives"},
		"a float is no int":     {"id: abc\nn: 1.5\n", "is number, want integer"},
		"type list rejects int": {"id: abc\nn: 1\neither: 3\n", "want string or null"},
	} {
		t.Run(name, func(t *testing.T) {
			doc, err := Parse([]byte(tc.doc))
			if err != nil {
				t.Fatal(err)
			}
			got := strings.Join(s.Validate(doc), "\n")
			if tc.want == "" && got != "" {
				t.Fatalf("want valid, got %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// A copy of the registry with one case broken must be reported, per file, without stopping the others.
func TestLoadAllReportsEachBrokenFile(t *testing.T) {
	root := t.TempDir()
	schema, err := os.ReadFile(filepath.Join(repoRegistry, SchemaFile))
	if err != nil {
		t.Fatal(err)
	}
	write := func(rel, body string) {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(SchemaFile, string(schema))
	good := "id: g/ok\ntitle: ok\nproves: [x]\nconsumer: api\nwhy: w\nrequest: {method: GET, path: /a}\ncompare: {mode: exact}\n"
	write("cases/g/ok.yaml", good)
	write("cases/g/colon.yaml", "id: g/colon\ntitle: a: b\n")
	write("cases/g/wrongid.yaml", strings.Replace(good, "g/ok", "g/other", 1))
	write("cases/g/badcluster.yaml", strings.Replace(good, "g/ok", "g/badcluster", 1)+
		"gap: {status: open, impact: edge, effort: S, area: a, loki: l, proxy: p, clusters: [05582624, 0e676869]}\n")
	cases, problems, err := LoadAll(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 3 {
		t.Fatalf("loaded %d parseable cases, want 3: %v", len(cases), problems)
	}
	joined := strings.Join(problems, "\n")
	for _, want := range []string{"colon.yaml", `id "g/other" is not the path "g/wrongid"`, "clusters[0]: is", "clusters[1]: is"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in:\n%s", want, joined)
		}
	}
}
