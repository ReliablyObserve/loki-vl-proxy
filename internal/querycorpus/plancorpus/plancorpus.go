// Package plancorpus reads the corpus the query plan tests run over: the curated rows every test reads from its own
// source (the A/B shapes, the semantics matrix and the registry cases) and a committed sample of the captured client
// queries. It is test support: tests in several packages (querycorpus, proxy) read the same rows through it.
package plancorpus

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The sources, relative to the repository root.
const (
	ShapesPath        = "bench/ab/shapes.json"
	SemanticsPath     = "test/e2e-compat/query-semantics-matrix.json"
	RegistryCasesDir  = "conformance/registry/cases"
	CaptureSamplePath = "internal/querycorpus/testdata/plan-capture-sample.json"
	// PlanPath holds the plan ratchets, one section per gate.
	PlanPath = "conformance/registry/query-plan.json"
)

// CaptureSample is the committed sample of the captured corpus. The full captured corpus is a CI artifact, not a
// committed file, so the sample is what a local run has of it: about 2,000 client LogQL rows, stratified by the
// markers their translation carries and by their construct, so every shape is present.
type CaptureSample struct {
	Description string `json:"description"`
	Rows        []struct {
		Query   string `json:"q"`
		Stratum string `json:"stratum"`
	} `json:"rows"`
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Curated is the query texts of the shapes, the semantics matrix and the registry cases (`query` and `expr`
// parameters), without duplicates, sorted. root is the repository root.
func Curated(root string) ([]string, error) {
	seen := map[string]bool{}
	add := func(q string) {
		if strings.TrimSpace(q) != "" {
			seen[q] = true
		}
	}
	var shapes struct {
		Sets map[string]struct {
			Shapes []struct {
				Query string `json:"query"`
			} `json:"shapes"`
		} `json:"sets"`
	}
	if err := readJSON(filepath.Join(root, ShapesPath), &shapes); err != nil {
		return nil, err
	}
	for _, set := range shapes.Sets {
		for _, s := range set.Shapes {
			add(s.Query)
		}
	}
	var matrix struct {
		Cases []struct {
			Query   string   `json:"query"`
			Queries []string `json:"queries"`
		} `json:"cases"`
		Performance []struct {
			Query   string   `json:"query"`
			Queries []string `json:"queries"`
		} `json:"performance_cases"`
	}
	if err := readJSON(filepath.Join(root, SemanticsPath), &matrix); err != nil {
		return nil, err
	}
	for _, c := range matrix.Cases {
		add(c.Query)
		for _, q := range c.Queries {
			add(q)
		}
	}
	for _, c := range matrix.Performance {
		add(c.Query)
		for _, q := range c.Queries {
			add(q)
		}
	}
	err := filepath.WalkDir(filepath.Join(root, RegistryCasesDir), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var doc any
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		var walk func(any)
		walk = func(n any) {
			switch v := n.(type) {
			case map[string]any:
				for k, val := range v {
					if s, ok := val.(string); ok && (k == "query" || k == "expr") {
						add(s)
					} else {
						walk(val)
					}
				}
			case []any:
				for _, e := range v {
					walk(e)
				}
			}
		}
		walk(doc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for q := range seen {
		out = append(out, q)
	}
	sort.Strings(out)
	return out, nil
}

// Queries is the curated rows and the capture sample, without duplicates: the curated rows first, in sorted order,
// then the sample's rows in file order.
func Queries(root string) ([]string, error) {
	curated, err := Curated(root)
	if err != nil {
		return nil, err
	}
	var sample CaptureSample
	if err := readJSON(filepath.Join(root, CaptureSamplePath), &sample); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(curated)+len(sample.Rows))
	out := make([]string, 0, len(curated)+len(sample.Rows))
	for _, q := range curated {
		seen[q] = true
		out = append(out, q)
	}
	for _, r := range sample.Rows {
		if !seen[r.Query] {
			seen[r.Query] = true
			out = append(out, r.Query)
		}
	}
	return out, nil
}
