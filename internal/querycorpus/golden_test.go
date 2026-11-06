package querycorpus_test

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// matrixPath is the compatibility matrix: its VictoriaLogs matrix_versions are
// the versions every corpus comparison uses (the latest v1.4x release and the
// newest v1.5x release, the pin).
const matrixPath = "../../test/e2e-compat/compatibility-matrix.json"

// goldenVersions returns the matrix's VictoriaLogs versions, newest (the pin) first.
func goldenVersions(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(matrixPath)
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Stack struct {
			VictoriaLogs struct {
				Pinned   string   `json:"pinned_version"`
				Versions []string `json:"matrix_versions"`
			} `json:"victorialogs"`
		} `json:"stack"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	vl := m.Stack.VictoriaLogs
	out := []string{vl.Pinned}
	for _, v := range vl.Versions {
		if v != vl.Pinned {
			out = append(out, v)
		}
	}
	return out
}

// translate is the LogsQL the translator produces for a LogQL query against a
// VictoriaLogs version, with label names kept as they are, so the golden is a
// function of the query and the version only.
func translate(q, version string) string {
	out, err := translator.TranslateLogQLWithCapabilities(q, func(s string) string { return s }, nil, logsql.CapabilitiesFor(version))
	if err != nil {
		return "error: " + err.Error()
	}
	return out
}

// annotate adds the goldens, whether our LogsQL parser reads the newest one,
// and the untyped nodes of a valid LogQL client row. Golden holds the newest
// version's LogsQL, and an older version's only where it differs.
func annotate(r *Row, versions []string) {
	if r.Lang != "logql" || r.Path != "" || !r.Valid {
		return
	}
	expr, err := logql.Parse(r.Query)
	if err != nil {
		return
	}
	r.Untyped = untypedNodes(expr)
	newest := translate(r.Query, versions[0])
	r.Golden = map[string]string{versions[0]: newest}
	for _, v := range versions[1:] {
		if out := translate(r.Query, v); out != newest {
			r.Golden[v] = out
		}
	}
	_, err = logsql.Parse(newest)
	r.GoldenReads = err == nil
}

// untypedNodes lists the kinds of node in expr that hold query text instead of
// typed fields: a label filter or label_format kept as raw text, and a metric
// function the parser passes through whole.
func untypedNodes(expr logql.Expr) []string {
	seen := map[string]bool{}
	var walk func(e logql.Expr)
	walk = func(e logql.Expr) {
		switch v := e.(type) {
		case *logql.OpaqueMetricExpr:
			seen["opaque_metric"] = true
		case *logql.LogQuery:
			for _, s := range v.Pipeline {
				switch s.(type) {
				case *logql.LabelFilterStage:
					seen["raw_label_filter"] = true
				case *logql.LabelFormatStage:
					seen["raw_label_format"] = true
				}
			}
		case *logql.RangeAggregation:
			walk(v.Inner)
		case *logql.VectorAggregation:
			walk(v.Inner)
		case *logql.BinOpExpr:
			walk(v.Left)
			walk(v.Right)
		}
	}
	walk(expr)
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Summary is the corpus's counts: the PR quality report shows them beside the
// committed targets.
type Summary struct {
	Rows                    int            `json:"rows"`
	ClientRows              int            `json:"client_rows"`
	DuplicatedClientQueries int            `json:"duplicated_client_queries"`
	InternalParseCallSites  int            `json:"internal_parse_call_sites"`
	LogQLRows               int            `json:"logql_rows"`
	UntypedLogQLRows        int            `json:"untyped_logql_rows"`
	UntypedByKind           map[string]int `json:"untyped_by_kind"`
	GoldenVersions          []string       `json:"golden_versions"`
	GoldenRows              int            `json:"golden_rows"`
	GoldenRead              int            `json:"golden_read"`
	GoldenDifferByVersion   int            `json:"golden_differ_by_version"`
	SentRows                int            `json:"sent_rows"`
	SentRead                int            `json:"sent_read"`
	SentRowsFromTests       int            `json:"sent_rows_from_tests"`
	SentReadFromTests       int            `json:"sent_read_from_tests"`
}

func summarize(rows []*Row, duplicated, internalSites int, versions []string) Summary {
	s := Summary{Rows: len(rows), DuplicatedClientQueries: duplicated, InternalParseCallSites: internalSites,
		UntypedByKind: map[string]int{}, GoldenVersions: versions}
	for _, r := range rows {
		if r.Path != "" {
			s.SentRows++
			if r.Valid {
				s.SentRead++
			}
			if len(r.Sources) > 0 {
				s.SentRowsFromTests++
				if r.Valid {
					s.SentReadFromTests++
				}
			}
			continue
		}
		s.ClientRows++
		if r.Lang == "logql" && r.Valid {
			s.LogQLRows++
		}
		if len(r.Untyped) > 0 {
			s.UntypedLogQLRows++
			for _, k := range r.Untyped {
				s.UntypedByKind[k]++
			}
		}
		if r.Golden != nil {
			s.GoldenRows++
			if r.GoldenReads {
				s.GoldenRead++
			}
			if len(r.Golden) > 1 {
				s.GoldenDifferByVersion++
			}
		}
	}
	return s
}
