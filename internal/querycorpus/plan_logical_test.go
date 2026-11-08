package querycorpus_test

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// planPath holds the plan ratchets, one section each. This file reads and writes only its `logical` section and
// leaves the others as they are: LVP_QUERY_PLAN_UPDATE=1 rewrites it.
const planPath = "../../conformance/registry/query-plan.json"

const planDescription = "Ratchets of the query plan (internal/queryplan), one section per gate; a count may only fall, and LVP_QUERY_PLAN_UPDATE=1 rewrites the section of the test that ran. `logical` is the logical plan built from every query of the in-repo corpus: how many rows parse and build (all of them), and the spots the plan still holds as text, per construct (typing them lowers the count)."

type logicalSection struct {
	Rows    int            `json:"rows"`
	Parsed  int            `json:"parsed"`
	Built   int            `json:"built"`
	Untyped map[string]int `json:"untyped_by_construct"`
}

// planCorpus is the in-repo query corpus: every string of the benchmark shapes and the semantics matrix that
// holds a selector, and the query and expr values of the registry rows.
func planCorpus(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	add := func(s string) {
		if strings.Contains(s, "{") {
			seen[s] = true
		}
	}
	var walkJSON func(v any)
	walkJSON = func(v any) {
		switch v := v.(type) {
		case string:
			add(v)
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
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		var doc any
		readJSON(t, path, &doc)
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
	err := filepath.WalkDir("../../conformance/registry", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var doc any
		if yaml.Unmarshal(data, &doc) == nil { // a few prose-only rows are not strict YAML and hold no query
			walkYAML(doc, "")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// planInputs is the logical input set a corpus row is built with: a metric query is a range query, the rest a log query.
func planInputs(e logql.Expr) queryplan.Inputs {
	if _, ok := e.(*logql.LogQuery); ok {
		return queryplan.Inputs{Kind: queryplan.KindLogs, StreamLabels: []string{"app", "namespace"}}
	}
	return queryplan.Inputs{Kind: queryplan.KindRange, StreamLabels: []string{"app", "namespace"}}
}

// walkPlan visits n and every node below it.
func walkPlan(n queryplan.Node, f func(queryplan.Node)) {
	f(n)
	switch x := n.(type) {
	case *queryplan.RangeAgg:
		f(x.In)
	case *queryplan.VectorAgg:
		walkPlan(x.In, f)
	case *queryplan.Rank:
		walkPlan(x.In, f)
	case *queryplan.BinOp:
		walkPlan(x.L, f)
		walkPlan(x.R, f)
	}
}

// countUntyped adds to c the spots of the plan that still hold text.
func countUntyped(root queryplan.Node, c map[string]int) {
	walkPlan(root, func(n queryplan.Node) {
		switch x := n.(type) {
		case *queryplan.Opaque:
			switch {
			case strings.HasPrefix(x.Raw, "label_replace"):
				c["opaque:label_replace"]++
			case strings.HasPrefix(x.Raw, "vector"):
				c["opaque:vector"]++
			case strings.HasPrefix(x.Raw, "approx_topk"):
				c["opaque:approx_topk"]++
			default:
				c["opaque:other"]++
			}
		case *queryplan.LogSelect:
			for _, s := range x.Stages {
				switch st := s.(type) {
				case *queryplan.LabelFilter:
					if st.Expr == nil {
						c["labelfilter_untyped"]++
					}
				case *queryplan.LineFormat:
					c["line_format_raw"]++
				case *queryplan.Parse:
					if st.Param != "" && len(st.Fields) == 0 {
						c["parser_param_raw"]++
					}
				}
			}
		}
	})
}

// TestLogicalBuildTotal: every corpus query logql.Parse accepts builds into a logical plan. The spots the plan holds
// as text are counted per construct in the `logical` section of query-plan.json; the counts may only fall.
func TestLogicalBuildTotal(t *testing.T) {
	got := logicalSection{Untyped: map[string]int{
		"opaque:label_replace": 0, "opaque:vector": 0, "opaque:approx_topk": 0, "opaque:other": 0,
		"labelfilter_untyped": 0, "line_format_raw": 0, "parser_param_raw": 0,
	}}
	for _, q := range planCorpus(t) {
		got.Rows++
		expr, err := logql.Parse(q)
		if err != nil {
			continue
		}
		got.Parsed++
		plan, err := queryplan.Build(expr, planInputs(expr))
		if err != nil {
			t.Errorf("Build(%q): %v", q, err)
			continue
		}
		got.Built++
		countUntyped(plan.Root, got.Untyped)
	}
	if got.Rows < 300 {
		t.Fatalf("the corpus has %d rows, expected the shapes, the matrix and the registry", got.Rows)
	}
	if got.Built != got.Parsed {
		t.Errorf("built %d of %d parsed rows", got.Built, got.Parsed)
	}

	file := map[string]json.RawMessage{}
	b, err := os.ReadFile(planPath)
	if err == nil {
		err = json.Unmarshal(b, &file)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		if _, ok := file["description"]; !ok {
			file["description"], _ = json.Marshal(planDescription)
		}
		file["logical"], _ = json.Marshal(got)
		out, err := json.MarshalIndent(file, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(planPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s: %+v", planPath, got)
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", planPath, err)
	}
	var want logicalSection
	if err := json.Unmarshal(file["logical"], &want); err != nil {
		t.Fatalf("%s has no readable logical section: %v", planPath, err)
	}
	for name, have := range got.Untyped {
		switch limit := want.Untyped[name]; {
		case have > limit:
			t.Errorf("%s grew from %d to %d", name, limit, have)
		case have < limit:
			t.Errorf("%s fell from %d to %d: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestLogicalBuildTotal and commit %s",
				name, limit, have, strings.TrimPrefix(planPath, "../../"))
		}
	}
	t.Logf("rows %d, parsed %d, built %d, untyped %v", got.Rows, got.Parsed, got.Built, got.Untyped)
}

// TestLogicalKeyDistinct: queries Loki reads differently never share a key.
func TestLogicalKeyDistinct(t *testing.T) {
	r := `rate({app="a"}[5m])`
	pairs := [][2]string{
		{`sum by (a) (` + r + `)`, `sum without (a) (` + r + `)`},
		{`sum by () (` + r + `)`, `sum (` + r + `)`},
		{`sum without () (` + r + `)`, `sum (` + r + `)`},
		{`sum by () (` + r + `)`, `sum without () (` + r + `)`},
		{`sum by (a, b) (` + r + `)`, `sum by (b, a) (` + r + `)`},
		{`rate({app="a"}[5m] offset 0s)`, r},
		{`rate({app="a"}[5m] offset 1h)`, `rate({app="a"}[5m] offset 2h)`},
		{`rate({app="a"}[5m])`, `rate({app="a"}[10m])`},
		{`rate({app="a"}[5m])`, `count_over_time({app="a"}[5m])`},
		{`{app="a"} |= "x"`, `{app="a"} |~ "x"`},
		{`{app="a"} |= "x"`, `{app="a"} != "x"`},
		{`{app="a"} |= "x" or "y"`, `{app="a"} |= "y" or "x"`},
		{`{app="a"} |= "x" or "y"`, `{app="a"} |= "x" |= "y"`},
		{`{app="a"} |= ip("1.2.3.4")`, `{app="a"} |= "ip(1.2.3.4)"`},
		{`{app="a"} | json a, b`, `{app="a"} | json b, a`},
		{`{app="a"} | json a="x"`, `{app="a"} | json a="y"`},
		{`{app="a"} | json`, `{app="a"} | logfmt`},
		{`{app="a"} | json | logfmt`, `{app="a"} | logfmt | json`},
		{`{app="a"} | regexp "(?P<x>.)"`, `{app="a"} | pattern "(?P<x>.)"`},
		{`{app="a", b="c"}`, `{b="c", app="a"}`},
		{`{app="a"}`, `{app=~"a"}`},
		{`{app="a"} | drop x`, `{app="a"} | keep x`},
		{`{app="a"} | drop x, y`, `{app="a"} | drop y, x`},
		{`{app="a"} | drop x="y"`, `{app="a"} | drop x!="y"`},
		{`{app="a"} | x="1" or y="2"`, `{app="a"} | x="1" and y="2"`},
		{`{app="a"} | x>5`, `{app="a"} | x>=5`},
		{`{app="a"} | x=5`, `{app="a"} | x="5"`},
		{`{app="a"} | line_format "{{.a}}"`, `{app="a"} | line_format "{{.b}}"`},
		{`{app="a"} | label_format x=y`, `{app="a"} | label_format x="y"`},
		{`{app="a"} | decolorize`, `{app="a"}`},
		{`avg_over_time({app="a"} | unwrap n [5m])`, `avg_over_time({app="a"} | unwrap bytes(n) [5m])`},
		{`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap n [5m])`},
		{`sum by (x) (avg_over_time({app="a"} | unwrap n [5m]))`, `avg_over_time({app="a"} | unwrap n [5m]) by (x)`},
		{`topk(2, ` + r + `)`, `topk(3, ` + r + `)`},
		{`topk(2, ` + r + `)`, `bottomk(2, ` + r + `)`},
		{`topk(2, ` + r + `)`, `topk by (a) (2, ` + r + `)`},
		{`sort(` + r + `)`, `sort_desc(` + r + `)`},
		{r + ` > 1`, r + ` > bool 1`},
		{r + ` > 1`, r + ` < 1`},
		{r + ` / ` + r, r + ` / on (a) ` + r},
		{r + ` / on (a) ` + r, r + ` / ignoring (a) ` + r},
		{r + ` / on (a) group_left ` + r, r + ` / on (a) group_right ` + r},
		{r + ` / on (a) group_left (b) ` + r, r + ` / on (a) group_left (c) ` + r},
		{r + ` + 1`, `1 + ` + r},
		{r + ` + 1`, r + ` + 2`},
		{`label_replace(` + r + `, "a", "b", "c", "d")`, `label_replace(` + r + `, "a", "b", "c", "e")`},
		{`vector(1)`, `vector(2)`},
	}
	for _, p := range pairs {
		a, b := keyOf(t, p[0], queryplan.Inputs{}), keyOf(t, p[1], queryplan.Inputs{})
		if bytes.Equal(a, b) {
			t.Errorf("one key for %q and %q", p[0], p[1])
		}
	}

	// The same query under different logical inputs has different keys.
	base := queryplan.Inputs{Kind: queryplan.KindRange, StreamLabels: []string{"app"}}
	for _, in := range []queryplan.Inputs{
		{Kind: queryplan.KindInstant, StreamLabels: []string{"app"}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app", "env"}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app"}, Exposure: queryplan.Exposure{LokiProfile: true}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app"}, Exposure: queryplan.Exposure{DottedNames: true}},
	} {
		if bytes.Equal(keyOf(t, r, base), keyOf(t, r, in)) {
			t.Errorf("one key for inputs %+v and %+v", base, in)
		}
	}
}

func keyOf(t *testing.T, q string, in queryplan.Inputs) []byte {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	plan, err := queryplan.Build(expr, in)
	if err != nil {
		t.Fatalf("build %q: %v", q, err)
	}
	return plan.Key
}
