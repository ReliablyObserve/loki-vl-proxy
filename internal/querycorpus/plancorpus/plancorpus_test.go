package plancorpus

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// write puts a file under root, creating its directory.
func write(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// tree is a repository root holding one source of each kind.
func tree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write(t, root, ShapesPath, `{"sets": {
		"a": {"shapes": [{"query": "{app=\"b\"}"}, {"query": "{app=\"a\"}"}, {"query": "  "}]},
		"b": {"shapes": [{"query": "{app=\"a\"}"}]}}}`)
	write(t, root, SemanticsPath, `{"cases": [
		{"query": "rate({app=\"c\"}[1m])"},
		{"queries": ["{app=\"d\"} |= \"x\"", "{app=\"b\"}"]},
		{"query": ""}],
		"performance_cases": [{"query": "{app=\"p\"}", "queries": ["{app=\"q\"}"]}]}`)
	write(t, root, RegistryCasesDir+"/semantics/one.yaml", "id: one\nrequest:\n  params:\n    query: '{app=\"r\"} | json'\n    start: now-1h\nchecks:\n  - expr: 'sum(rate({app=\"e\"}[5m]))'\n    note: ignored\n")
	write(t, root, RegistryCasesDir+"/other/two.yaml", "id: two\nquery: '{app=\"c\"}'\nnested:\n  deeper:\n    - query: '{app=\"n\"}'\n")
	write(t, root, RegistryCasesDir+"/other/notes.txt", "query: ignored")
	write(t, root, CaptureSamplePath, `{"description": "sample", "rows": [
		{"q": "{app=\"s1\"}", "stratum": "x"}, {"q": "{app=\"a\"}", "stratum": "y"}, {"q": "{app=\"s2\"}", "stratum": "x"}, {"q": "{app=\"s1\"}", "stratum": "z"}]}`)
	return root
}

// The curated rows are the shapes, the semantics matrix and the registry cases, without duplicates and blanks, sorted.
// conformance: architecture-syntax-package-layering
func TestCurated(t *testing.T) {
	got, err := Curated(tree(t))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`rate({app="c"}[1m])`,
		`sum(rate({app="e"}[5m]))`,
		`{app="a"}`,
		`{app="b"}`,
		`{app="c"}`,
		`{app="d"} |= "x"`,
		`{app="n"}`,
		`{app="p"}`,
		`{app="q"}`,
		`{app="r"} | json`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("Curated:\n got  %q\n want %q", got, want)
	}
}

// A case that compares only a client error (values.status 4xx and an error facet, no data facet) carries a query
// Loki refuses: its request is not a corpus row. Every other case keeps its row, and so does a query the case lists
// outside its request.
// conformance: architecture-syntax-package-layering
func TestRejectionOnlyCasesLeaveOutTheirRequestQuery(t *testing.T) {
	const query = `rate({app="x"}[5m]) + on 1`
	for _, c := range []struct {
		name    string
		compare string
		want    bool // the request's query is left out
	}{
		{"status 400, error text", "mode: observed\n  fields: [status, error_text]\n  values: {status: 400}", true},
		{"status 400, errorType and error", "mode: structural\n  fields: [status, errorType, error]\n  values: {status: 400}", true},
		{"status 499, message", "mode: observed\n  fields: [message]\n  values: {status: 499}", true},
		{"no status stated", "mode: observed\n  fields: [status, error_text]", false},
		{"status 200", "mode: observed\n  fields: [status, error_text]\n  values: {status: 200}", false},
		{"status 500", "mode: observed\n  fields: [status, error_text]\n  values: {status: 500}", false},
		{"status as text", "mode: observed\n  fields: [status, error_text]\n  values: {status: \"400\"}", false},
		{"a data facet beside the error", "mode: observed\n  fields: [status, error_text, values]\n  values: {status: 400}", false},
		{"status only", "mode: observed\n  fields: [status]\n  values: {status: 400}", false},
		{"no fields", "mode: observed\n  values: {status: 400}", false},
		{"no values block", "mode: status", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := tree(t)
			write(t, root, RegistryCasesDir+"/semantics/reject.yaml", "id: reject\nrequest:\n  params:\n    query: '"+query+"'\ncompare:\n  "+c.compare+"\n")
			got, err := Curated(root)
			if err != nil {
				t.Fatal(err)
			}
			if has := slices.Contains(got, query); has == c.want {
				t.Errorf("query in the corpus = %v, want %v", has, !c.want)
			}
		})
	}
	t.Run("a query outside the request stays", func(t *testing.T) {
		root := tree(t)
		write(t, root, RegistryCasesDir+"/semantics/reject.yaml", "id: reject\nrequest:\n  params:\n    query: '"+query+"'\ncorpus:\n  dimensions:\n    shapes:\n      - query: '{app=\"kept\"}'\ncompare:\n  mode: observed\n  fields: [status, error_text]\n  values: {status: 400}\n")
		got, err := Curated(root)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(got, query) || !slices.Contains(got, `{app="kept"}`) {
			t.Errorf("Curated = %q, want the shape kept and the request query left out", got)
		}
	})
}

// The queries are the curated rows, then the sample's rows in file order, each once.
// conformance: architecture-syntax-package-layering
func TestQueries(t *testing.T) {
	root := tree(t)
	curated, err := Curated(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Queries(root)
	if err != nil {
		t.Fatal(err)
	}
	want := append(slices.Clone(curated), `{app="s1"}`, `{app="s2"}`)
	if !slices.Equal(got, want) {
		t.Errorf("Queries:\n got  %q\n want %q", got, want)
	}
}

// A read failure names its source: a missing file, a malformed file, and a root that is not there.
// conformance: architecture-syntax-package-layering
func TestCorpusReadErrors(t *testing.T) {
	for _, c := range []struct {
		name    string
		damage  func(t *testing.T, root string)
		wantErr string
	}{
		{"missing shapes", func(t *testing.T, root string) { remove(t, root, ShapesPath) }, "shapes.json"},
		{"missing matrix", func(t *testing.T, root string) { remove(t, root, SemanticsPath) }, "query-semantics-matrix.json"},
		{"malformed shapes", func(t *testing.T, root string) { write(t, root, ShapesPath, "{") }, ShapesPath},
		{"malformed matrix", func(t *testing.T, root string) { write(t, root, SemanticsPath, "[") }, SemanticsPath},
		{"malformed case", func(t *testing.T, root string) { write(t, root, RegistryCasesDir+"/bad.yaml", "a: [") }, "bad.yaml"},
		{"missing cases directory", func(t *testing.T, root string) {
			if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(RegistryCasesDir))); err != nil {
				t.Fatal(err)
			}
		}, "cases"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := tree(t)
			c.damage(t, root)
			for name, read := range map[string]func(string) ([]string, error){"Curated": Curated, "Queries": Queries} {
				if _, err := read(root); err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("%s: err = %v, want one naming %s", name, err, c.wantErr)
				}
			}
		})
	}
	t.Run("missing and malformed capture sample", func(t *testing.T) {
		root := tree(t)
		if _, err := Curated(root); err != nil {
			t.Fatalf("the sample is not read by Curated: %v", err)
		}
		remove(t, root, CaptureSamplePath)
		if _, err := Queries(root); err == nil || !strings.Contains(err.Error(), "plan-capture-sample.json") {
			t.Errorf("missing sample: %v", err)
		}
		write(t, root, CaptureSamplePath, "not json")
		if _, err := Queries(root); err == nil || !strings.Contains(err.Error(), CaptureSamplePath) {
			t.Errorf("malformed sample: %v", err)
		}
	})
	missing := filepath.Join(t.TempDir(), "none")
	if _, err := Curated(missing); err == nil {
		t.Error("a root that does not exist must fail")
	}
	if _, err := Queries(missing); err == nil {
		t.Error("a root that does not exist must fail")
	}
}

// A symlink in the tree cannot lead a read outside the root.
// conformance: architecture-syntax-package-layering
func TestCorpusReadsStayInsideTheRoot(t *testing.T) {
	root := tree(t)
	outside := t.TempDir()
	write(t, outside, "shapes.json", `{"sets": {"x": {"shapes": [{"query": "{app=\"outside\"}"}]}}}`)
	shapes := filepath.Join(root, filepath.FromSlash(ShapesPath))
	if err := os.Remove(shapes); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "shapes.json"), shapes); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	if got, err := Curated(root); err == nil {
		t.Errorf("a symlink out of the root was followed: %q", got)
	}
}

// The committed corpus reads: the shapes, the matrix, the registry cases and the capture sample.
// conformance: architecture-syntax-package-layering
func TestCommittedCorpus(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	curated, err := Curated(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(curated) < 100 || !slices.IsSorted(curated) {
		t.Errorf("curated rows: %d, sorted %v", len(curated), slices.IsSorted(curated))
	}
	all, err := Queries(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) <= len(curated) {
		t.Errorf("the capture sample adds rows: %d queries, %d curated", len(all), len(curated))
	}
	seen := map[string]bool{}
	for _, q := range all {
		if strings.TrimSpace(q) == "" {
			t.Error("a blank query")
		}
		if seen[q] {
			t.Errorf("duplicate query %q", q)
		}
		seen[q] = true
	}
}

func remove(t *testing.T, root, rel string) {
	t.Helper()
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
		t.Fatal(err)
	}
}
