package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The predicate parity gate: what the resolver (queryplan.Resolve) derives from the typed plan, against the text
// predicates the handlers call today, on every plan-corpus row that validates. Each predicate is given the text its
// call site reads (handlerQueryText: the client text after the request rewrites that run before the handler). A difference is allowed only when the predicate_parity section of query-plan.json lists it with
// a reason (the predicate misreads the text, the resolver reads Loki's meaning); LVP_QUERY_PLAN_UPDATE=1 rewrites
// the section, keeping the reasons, dropping the rows that agree again and adding new differences unexplained, which
// fail until someone writes their reason.

const unexplained = "UNEXPLAINED"

type parityDiff struct {
	Query  string `json:"query"`
	Old    string `json:"old"`
	New    string `json:"new"`
	Reason string `json:"reason"`
}

type parityAttribute struct {
	Predicate string       `json:"predicate"`
	Compared  int          `json:"compared"`
	Diffs     []parityDiff `json:"explained"`
}

type paritySection struct {
	Rows       int                         `json:"rows"`
	Validated  int                         `json:"validated"`
	Attributes map[string]*parityAttribute `json:"attributes"`
}

// handlerQueryText is the query a query_range handler reads from the request: keywords lowercased (the
// lowercaseQueryKeywords middleware), validated, then the keep-series rewrite (applyKeepSeriesGrouping on a range
// request). ok is false when the query does not validate: no handler sees it.
func handlerQueryText(q string) (string, bool) {
	q = logqlpkg.LowercaseKeywords(q)
	if _, msg := logqlpkg.ValidateLogQLExpr(q); msg != "" {
		return "", false
	}
	if rewritten, changed := keepSeriesGrouping(q); changed && (!affectedOrderedJSONVectorQuery(rewritten) || affectedOrderedJSONVectorQuery(q)) {
		q = rewritten
	}
	return q, true
}

func offsetText(kind queryplan.OffsetKind, d string) string {
	switch kind {
	case queryplan.OffsetNone:
		return "none"
	case queryplan.OffsetUniform:
		return "uniform " + d
	case queryplan.OffsetMixed:
		return "mixed"
	}
	return "distinct"
}

// conformance: architecture-syntax-package-layering
func TestPlanPredicateParity(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	root := filepath.Join("..", "..")
	rows, err := plancorpus.Queries(root)
	if err != nil {
		t.Fatal(err)
	}
	got := paritySection{Rows: len(rows), Attributes: map[string]*parityAttribute{
		"offset": {Predicate: "extractLogQLOffset(handler query text)", Diffs: []parityDiff{}},
	}}
	diff := func(attr, q string, old, new any) {
		a := got.Attributes[attr]
		a.Compared++
		if o, n := fmt.Sprint(old), fmt.Sprint(new); o != n {
			a.Diffs = append(a.Diffs, parityDiff{Query: q, Old: o, New: n})
		}
	}
	for _, client := range rows {
		q, ok := handlerQueryText(client)
		if !ok {
			continue
		}
		expr, msg := logqlpkg.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		got.Validated++
		queryplan.Resolve(p)

		old := "none"
		if d, _, err := extractLogQLOffset(q); err != nil {
			old = "mixed"
			if strings.Contains(err.Error(), "distinct offsets") {
				old = "distinct"
			}
		} else if logqlOffsetRE.MatchString(q) {
			old = "uniform " + d.String()
		}
		diff("offset", q, old, offsetText(p.Sem.OffsetKind, p.Sem.Offset.String()))
	}

	path := filepath.Join(root, plancorpus.PlanPath)
	file := map[string]json.RawMessage{}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &file); err != nil {
			t.Fatal(err)
		}
	}
	var want paritySection
	if raw, ok := file["predicate_parity"]; ok {
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
	}
	reasons := map[string]string{}
	for name, a := range want.Attributes {
		for _, d := range a.Diffs {
			reasons[name+"\x00"+d.Query+"\x00"+d.Old+"\x00"+d.New] = d.Reason
		}
	}
	names := make([]string, 0, len(got.Attributes))
	for name := range got.Attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	update := os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1"
	var problems []string
	for _, name := range names {
		a := got.Attributes[name]
		sort.Slice(a.Diffs, func(i, j int) bool { return a.Diffs[i].Query < a.Diffs[j].Query })
		seen := map[string]bool{}
		for i := range a.Diffs {
			d := &a.Diffs[i]
			k := name + "\x00" + d.Query + "\x00" + d.Old + "\x00" + d.New
			seen[k] = true
			d.Reason = reasons[k]
			if d.Reason == "" {
				d.Reason = unexplained
			}
			if d.Reason == unexplained {
				problems = append(problems, fmt.Sprintf("%s: %q: old %s, resolver %s, no reason", name, d.Query, d.Old, d.New))
			}
		}
		if w := want.Attributes[name]; w != nil {
			for _, d := range w.Diffs {
				if !seen[name+"\x00"+d.Query+"\x00"+d.Old+"\x00"+d.New] {
					problems = append(problems, fmt.Sprintf("%s: %q is listed as a difference and now agrees (or differs another way)", name, d.Query))
				}
			}
		}
		t.Logf("%s: %d compared, %d differences", name, a.Compared, len(a.Diffs))
	}
	if update {
		file["predicate_parity"], _ = json.Marshal(got)
		out, err := json.MarshalIndent(file, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if len(problems) > 0 {
		t.Fatalf("%d predicate differences are not explained in the predicate_parity section of %s (LVP_QUERY_PLAN_UPDATE=1 lists them; write each reason):\n%s",
			len(problems), plancorpus.PlanPath, strings.Join(problems, "\n"))
	}
}
