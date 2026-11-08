package queryplan

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// The corpus the package tests and the fuzz seeds run over: every query string of the benchmark shapes and the
// semantics matrix, the query and expr values of the registry cases, and the capture sample of client queries.
// The querycorpus tests read the same sources, so this is the same set of rows the plan ratchets count.
const (
	shapesPath       = "../../bench/ab/shapes.json"
	matrixPath       = "../../test/e2e-compat/query-semantics-matrix.json"
	registryCasesDir = "../../conformance/registry/cases"
	capturePath      = "../querycorpus/testdata/plan-capture-sample.json"
)

var (
	corpusOnce   sync.Once
	corpusTexts  []string
	corpusParsed []logql.Expr
)

// corpus is the corpus texts that parse, canonicalised as the proxy does before it parses, and their trees.
// The trees are shared by the tests, which must not change them.
func corpus(tb testing.TB) (texts []string, exprs []logql.Expr) {
	tb.Helper()
	corpusOnce.Do(func() {
		seen := map[string]bool{}
		add := func(q string) {
			if strings.TrimSpace(q) != "" {
				seen[q] = true
			}
		}
		var walkJSON func(v any)
		walkJSON = func(v any) {
			switch v := v.(type) {
			case string:
				if strings.Contains(v, "{") {
					add(v)
				}
			case []any:
				for _, e := range v {
					walkJSON(e)
				}
			case map[string]any:
				for _, e := range v {
					walkJSON(e)
				}
			}
		}
		for _, path := range []string{shapesPath, matrixPath} {
			raw, err := os.ReadFile(path)
			if err != nil {
				tb.Fatal(err)
			}
			var doc any
			if err := json.Unmarshal(raw, &doc); err != nil {
				tb.Fatalf("%s: %v", path, err)
			}
			walkJSON(doc)
		}
		var walkYAML func(v any, key string)
		walkYAML = func(v any, key string) {
			switch v := v.(type) {
			case string:
				if key == "query" || key == "expr" {
					add(v)
				}
			case []any:
				for _, e := range v {
					walkYAML(e, key)
				}
			case map[string]any:
				for k, e := range v {
					walkYAML(e, k)
				}
			}
		}
		err := filepath.WalkDir(registryCasesDir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var doc any
			if yaml.Unmarshal(b, &doc) == nil {
				walkYAML(doc, "")
			}
			return nil
		})
		if err != nil {
			tb.Fatal(err)
		}
		var sample struct {
			Rows []struct {
				Query string `json:"q"`
			} `json:"rows"`
		}
		if raw, err := os.ReadFile(capturePath); err != nil {
			tb.Fatal(err)
		} else if err := json.Unmarshal(raw, &sample); err != nil {
			tb.Fatal(err)
		}
		for _, r := range sample.Rows {
			add(r.Query)
		}
		all := make([]string, 0, len(seen))
		for q := range seen {
			all = append(all, q)
		}
		sort.Strings(all)
		for _, q := range all {
			if e, err := logql.Parse(logql.CanonicalizeLogRanges(q)); err == nil {
				corpusTexts = append(corpusTexts, q)
				corpusParsed = append(corpusParsed, e)
			}
		}
	})
	if len(corpusParsed) < 2000 {
		tb.Fatalf("the corpus has %d parsed queries, expected the curated sources and the capture sample", len(corpusParsed))
	}
	return corpusTexts, corpusParsed
}

// parseCanon parses a query as the proxy does.
func parseCanon(q string) (logql.Expr, error) { return logql.Parse(logql.CanonicalizeLogRanges(q)) }
