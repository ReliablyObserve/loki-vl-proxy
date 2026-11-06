package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// targetsPath is the committed targets: the corpus counts at the last update, shown beside a PR's counts.
const targetsPath = "../../conformance/registry/query-corpus-targets.json"

// Targets is the committed file. They are not a gate: the counts grow with new tests. Max counts are to fall and
// min counts to rise as the AST work goes on.
type Targets struct {
	Description string         `json:"description"`
	Max         map[string]int `json:"max"`
	Min         map[string]int `json:"min"`
}

// TargetCheck is one count against its target.
type TargetCheck struct {
	Name string `json:"name"`
	Rule string `json:"rule"` // "max" or "min"
	Want int    `json:"want"`
	Got  int    `json:"got"`
	// State is "held" (equal), "worse" or "better".
	State string `json:"state"`
}

// Regressions are the queries on both the base and the PR that got worse: the gate.
type Regressions struct {
	// Untyped are queries with an untyped node kind the base did not have.
	Untyped []string `json:"untyped"`
	// GoldenUnread are queries whose newest golden our LogsQL parser read on the base and does not read now.
	GoldenUnread []string `json:"golden_unread"`
}

// GoldenDiff compares the PR's goldens with the base's, per VictoriaLogs version, over the queries both have.
type GoldenDiff struct {
	Version  string   `json:"version"`
	Compared int      `json:"compared"`
	Changed  int      `json:"changed"`
	Added    int      `json:"added"`
	Removed  int      `json:"removed"`
	Examples []string `json:"examples"`
}

// Report is what the PR quality report shows of the corpus.
type Report struct {
	Summary     Summary       `json:"summary"`
	Base        *Summary      `json:"base,omitempty"`
	Targets     []TargetCheck `json:"targets"`
	GoldenDiffs []GoldenDiff  `json:"golden_diffs,omitempty"`
	// Regressions is set only when there is a base corpus to compare with.
	Regressions *Regressions `json:"regressions,omitempty"`
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// checkTargets compares every count the targets name with the summary.
func checkTargets(t *testing.T, r Targets, s Summary) []TargetCheck {
	t.Helper()
	b, _ := json.Marshal(s)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	var checks []TargetCheck
	add := func(rule string, limits map[string]int) {
		names := make([]string, 0, len(limits))
		for n := range limits {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			v, present := got[n].(float64)
			if !present {
				t.Fatalf("the targets name %q, which the corpus summary does not have", n)
			}
			c := TargetCheck{Name: n, Rule: rule, Want: limits[n], Got: int(v), State: "held"}
			switch {
			case c.Got == c.Want:
			case (rule == "max") == (c.Got > c.Want):
				c.State = "worse"
			default:
				c.State = "better"
			}
			checks = append(checks, c)
		}
	}
	add("max", r.Max)
	add("min", r.Min)
	return checks
}

// clientRows reads the LogQL client rows with goldens of a corpus file, by query id.
func clientRows(t *testing.T, path string) map[string]Row {
	t.Helper()
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Row{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r Row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Golden != nil {
			out[r.ID] = r
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// at is the LogsQL of version v: its own golden, or the newest one where v's is the same.
func at(g map[string]string, v, newest string) string {
	if s, ok := g[v]; ok {
		return s
	}
	return g[newest]
}

func diffGoldens(base, head map[string]Row, versions []string) []GoldenDiff {
	var out []GoldenDiff
	for _, v := range versions {
		d := GoldenDiff{Version: v}
		for id, h := range head {
			b, ok := base[id]
			if !ok {
				d.Added++
				continue
			}
			d.Compared++
			if at(b.Golden, v, versions[0]) != at(h.Golden, v, versions[0]) {
				d.Changed++
				d.Examples = append(d.Examples, id)
			}
		}
		for id := range base {
			if _, ok := head[id]; !ok {
				d.Removed++
			}
		}
		sort.Strings(d.Examples)
		if len(d.Examples) > 20 {
			d.Examples = d.Examples[:20]
		}
		out = append(out, d)
	}
	return out
}

// regressions lists the queries on both the base and the PR that got worse. New and removed queries are not
// compared: a new test never fails the gate.
func regressions(base, head map[string]Row) Regressions {
	reg := Regressions{Untyped: []string{}, GoldenUnread: []string{}}
	for id, h := range head {
		b, ok := base[id]
		if !ok {
			continue
		}
		had := map[string]bool{}
		for _, k := range b.Untyped {
			had[k] = true
		}
		for _, k := range h.Untyped {
			if !had[k] {
				reg.Untyped = append(reg.Untyped, id)
				break
			}
		}
		if b.GoldenReads && !h.GoldenReads {
			reg.GoldenUnread = append(reg.GoldenUnread, id)
		}
	}
	sort.Strings(reg.Untyped)
	sort.Strings(reg.GoldenUnread)
	return reg
}

// TestQueryCorpusReport writes what the PR quality report shows of the corpus at LVP_QUERY_CORPUS_OUT to
// LVP_QUERY_CORPUS_REPORT: its summary against the committed targets and, with LVP_QUERY_CORPUS_BASE set to the
// base commit's corpus, the base summary, the golden changes and the regressions. It fails on a regression: a
// query on both sides that turned untyped, or whose golden our LogsQL parser no longer reads.
func TestQueryCorpusReport(t *testing.T) {
	corpus, reportPath := os.Getenv("LVP_QUERY_CORPUS_OUT"), os.Getenv("LVP_QUERY_CORPUS_REPORT")
	if corpus == "" || reportPath == "" {
		t.Skip("LVP_QUERY_CORPUS_OUT and LVP_QUERY_CORPUS_REPORT are not set")
	}
	var rep Report
	readJSON(t, summaryPath(corpus), &rep.Summary)
	var targets Targets
	readJSON(t, targetsPath, &targets)
	rep.Targets = checkTargets(t, targets, rep.Summary)
	// A base corpus built before goldens existed has no summary: then there is nothing to compare.
	if base := os.Getenv("LVP_QUERY_CORPUS_BASE"); base != "" {
		if _, err := os.Stat(summaryPath(base)); err == nil {
			var bs Summary
			readJSON(t, summaryPath(base), &bs)
			rep.Base = &bs
			baseRows, headRows := clientRows(t, base), clientRows(t, corpus)
			rep.GoldenDiffs = diffGoldens(baseRows, headRows, rep.Summary.GoldenVersions)
			reg := regressions(baseRows, headRows)
			rep.Regressions = &reg
		}
	}
	b, err := json.MarshalIndent(rep, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reportPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := rep.Regressions; r != nil {
		if len(r.Untyped) > 0 {
			t.Errorf("%d queries on both the base and the PR turned untyped: %v", len(r.Untyped), first(r.Untyped))
		}
		if len(r.GoldenUnread) > 0 {
			t.Errorf("%d goldens our LogsQL parser read on the base and no longer reads: %v", len(r.GoldenUnread), first(r.GoldenUnread))
		}
	}
}

func first(ids []string) []string {
	if len(ids) > 20 {
		return ids[:20]
	}
	return ids
}

func TestTargetChecksHoldWorsenAndImprove(t *testing.T) {
	r := Targets{Max: map[string]int{"internal_parse_call_sites": 70}, Min: map[string]int{"golden_read": 600}}
	for _, c := range []struct {
		sites, read int
		want        []string
	}{
		{70, 600, []string{"held", "held"}},
		{71, 600, []string{"worse", "held"}},
		{70, 599, []string{"held", "worse"}},
		{69, 601, []string{"better", "better"}},
	} {
		checks := checkTargets(t, r, Summary{InternalParseCallSites: c.sites, GoldenRead: c.read})
		if got := []string{checks[0].State, checks[1].State}; got[0] != c.want[0] || got[1] != c.want[1] {
			t.Errorf("sites=%d read=%d: states %v, want %v", c.sites, c.read, got, c.want)
		}
	}
}

// Only queries on both sides are compared: a new query, typed or not, never fails the gate.
func TestRegressionsAreQueriesOnBothSidesThatGotWorse(t *testing.T) {
	g := map[string]string{"v1.53.0": "x"}
	base := map[string]Row{
		"typed":     {ID: "typed", Golden: g, GoldenReads: true},
		"kept":      {ID: "kept", Golden: g, Untyped: []string{"raw_label_filter"}},
		"read-lost": {ID: "read-lost", Golden: g, GoldenReads: true},
		"gone":      {ID: "gone", Golden: g, GoldenReads: true},
	}
	head := map[string]Row{
		"typed":     {ID: "typed", Golden: g, GoldenReads: true, Untyped: []string{"opaque_metric"}},
		"kept":      {ID: "kept", Golden: g, Untyped: []string{"raw_label_filter"}},
		"read-lost": {ID: "read-lost", Golden: g},
		"new":       {ID: "new", Golden: g, Untyped: []string{"raw_label_filter"}},
	}
	got := regressions(base, head)
	if len(got.Untyped) != 1 || got.Untyped[0] != "typed" {
		t.Errorf("untyped regressions = %v, want [typed]", got.Untyped)
	}
	if len(got.GoldenUnread) != 1 || got.GoldenUnread[0] != "read-lost" {
		t.Errorf("golden regressions = %v, want [read-lost]", got.GoldenUnread)
	}
}

func TestGoldenDiffComparesEachVersionAndFallsBackToTheNewest(t *testing.T) {
	versions := []string{"v1.53.0", "v1.49.0"}
	row := func(g map[string]string) Row { return Row{Golden: g} }
	base := map[string]Row{
		"a": row(map[string]string{"v1.53.0": "x"}),
		"b": row(map[string]string{"v1.53.0": "y", "v1.49.0": "y-old"}),
		"c": row(map[string]string{"v1.53.0": "z"}),
	}
	head := map[string]Row{
		"a": row(map[string]string{"v1.53.0": "x"}),                     // unchanged in both
		"b": row(map[string]string{"v1.53.0": "y"}),                     // v1.49.0 now the same as v1.53.0: changed for v1.49.0 only
		"d": row(map[string]string{"v1.53.0": "w", "v1.49.0": "w-old"}), // added
	}
	got := diffGoldens(base, head, versions)
	want := []GoldenDiff{
		{Version: "v1.53.0", Compared: 2, Added: 1, Removed: 1},
		{Version: "v1.49.0", Compared: 2, Changed: 1, Added: 1, Removed: 1, Examples: []string{"b"}},
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Version != w.Version || g.Compared != w.Compared || g.Changed != w.Changed || g.Added != w.Added ||
			g.Removed != w.Removed || len(g.Examples) != len(w.Examples) {
			t.Errorf("version %s: got %+v, want %+v", w.Version, g, w)
		}
	}
}
