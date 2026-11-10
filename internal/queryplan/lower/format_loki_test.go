package lower

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The formatting stages against Loki. The lowering writes `| format "<a> x <b>"`, a conditional format for a
// label_format rename and a delete of its source; a small model of VictoriaLogs' format, copy and delete pipes
// (below) is run over rows of every combination of present, missing and empty labels, and its line and labels
// must be what Loki's pipeline makes of the same stream and line. Loki's answers are recorded with the Loki
// oracle (test/oracle/loki process); LVP_RUN_ORACLES=1 records them again.

const formatLokiAnswers = "testdata/format-stages-loki.json"

type formatLokiCase struct {
	Query  string            `json:"q"`
	Labels map[string]string `json:"labels"`
	Line   string            `json:"line"`
	Keep   bool              `json:"keep"`
	Out    string            `json:"out"`
	OutLbl map[string]string `json:"out_labels"`
}

func formatLokiCases() []formatLokiCase {
	queries := []string{
		`{app="x"} | line_format "{{.a}}"`,
		`{app="x"} | line_format "{{.a}} x {{.b}}"`,
		`{app="x"} | line_format "{{ .a }}-{{.a}}"`,
		`{app="x"} | line_format "plain"`,
		`{app="x"} | line_format ""`,
		`{app="x"} | line_format "{{.app}}: {{.a}}"`,
		`{app="x"} | line_format "{{.a}}" | line_format "[{{.b}}]"`,
		`{app="x"} | label_format c=a`,
		`{app="x"} | label_format c=a, d=b`,
		`{app="x"} | label_format a=b`,
		`{app="x"} | label_format a="{{.b}}"`,
		`{app="x"} | label_format c="{{.a}}-{{.b}}"`,
		`{app="x"} | label_format c="plain"`,
		`{app="x"} | label_format c=""`,
		`{app="x"} | label_format c=a, a=b`,
		`{app="x"} | label_format c=a | label_format d=c`,
		`{app="x"} | label_format c="{{.a}}" | line_format "{{.c}}/{{.a}}"`,
		`{app="x"} | line_format "{{.a}}" | label_format c=b`,
		`{app="x"} | label_format app=a`,
		`{app="x"} | label_format c=app`,
		`{app="x"} | label_format c="{{.app}}{{.a}}"`,
		`{app="x"} | label_format a="{{.a}}{{.b}}"`,
	}
	var out []formatLokiCase
	vals := []map[string]string{
		{"a": "1", "b": "2"}, {"a": "1"}, {"b": "2"}, {}, {"a": "", "b": "2"}, {"a": "1", "b": ""}, {"a": "", "b": ""}, {"a": "x y", "b": "é"}, {"a": "1", "b": "2", "c": "old", "d": "older"}, {"a": "1", "c": "old"},
	}
	for _, q := range queries {
		for _, v := range vals {
			labels := map[string]string{"app": "x"}
			for k, s := range v {
				labels[k] = s
			}
			for _, line := range []string{"line one", ""} {
				out = append(out, formatLokiCase{Query: q, Labels: labels, Line: line})
			}
		}
	}
	return out
}

func recordFormatLokiAnswers(t *testing.T, cases []formatLokiCase) {
	t.Helper()
	pc := make([]oraclerun.ProcessCase, len(cases))
	for i, c := range cases {
		pc[i] = oraclerun.ProcessCase{Query: c.Query, Labels: c.Labels, Line: c.Line}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "..", "test", "oracle", "loki"), pc)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range answers {
		if a.Error != "" {
			t.Fatalf("Loki rejects %s: %s", cases[i].Query, a.Error)
		}
		cases[i].Keep, cases[i].Out, cases[i].OutLbl = a.Keep, a.Line, a.Labels
	}
	b, err := json.MarshalIndent(cases, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(formatLokiAnswers, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// vlRow runs the pipes of a lowered query over a row (VictoriaLogs treats an empty field as a missing one):
// `format` replaces each <field> of its template by the field's value (empty when missing), writes it to the
// result field (_msg when none) and, under `if`, only where the condition holds, and under skip_empty_results
// only when the result is not empty; `copy` and `delete` as named.
func vlRow(t *testing.T, pipes []logsql.Pipe, row map[string]string) {
	t.Helper()
	holds := func(f logsql.FilterExpr) bool {
		switch f := f.(type) {
		case nil:
			return true
		case logsql.FieldFilter:
			if f.Op == logsql.FieldOpAny && !f.Negate {
				return row[f.Field] != ""
			}
		}
		t.Fatalf("the model does not read the condition %v", f)
		return false
	}
	for _, p := range pipes {
		switch p := p.(type) {
		case logsql.PipeCopy:
			for _, pair := range p.Pairs {
				row[pair[1]] = row[pair[0]]
			}
		case logsql.PipeDelete:
			for _, l := range p.Labels {
				delete(row, l)
			}
		case logsql.PipeFormat:
			if !holds(p.If) {
				continue
			}
			res := p.Template
			for name := range row {
				res = strings.ReplaceAll(res, "<"+name+">", row[name])
			}
			// a placeholder of a missing field is empty
			for {
				i := strings.IndexByte(res, '<')
				j := strings.IndexByte(res, '>')
				if i < 0 || j < i {
					break
				}
				res = res[:i] + res[j+1:]
			}
			if res == "" && p.SkipEmptyResults {
				continue
			}
			dst := p.ResultField
			if dst == "" {
				dst = "_msg"
			}
			row[dst] = res
		case logsql.PipeFilter:
		default:
			t.Fatalf("the model does not run %T", p)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestFormatStagesMeanWhatLokiMeans(t *testing.T) {
	cases := formatLokiCases()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordFormatLokiAnswers(t, cases)
	}
	b, err := os.ReadFile(formatLokiAnswers)
	if err != nil {
		t.Fatal(err)
	}
	var recorded []formatLokiCase
	if err := json.Unmarshal(b, &recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != len(cases) {
		t.Fatalf("%d recorded answers for %d cases: LVP_RUN_ORACLES=1 records them again", len(recorded), len(cases))
	}
	lowered := 0
	for _, c := range recorded {
		op, err := Plan(planOf(t, c.Query), Profile{})
		if err != nil {
			continue // a query the lowering leaves to the string translator
		}
		q, err := logsql.Parse(op.(physical.VLQuery).Text)
		if err != nil {
			t.Fatalf("%s: %v", c.Query, err)
		}
		lowered++
		row := map[string]string{"_msg": c.Line}
		for k, v := range c.Labels {
			row[k] = v
		}
		vlRow(t, q.Pipes, row)
		if !c.Keep {
			t.Fatalf("%s: Loki drops the line", c.Query)
		}
		if got := row["_msg"]; got != c.Out {
			t.Errorf("%s labels %v line %q: lowered gives line %q, Loki %q", c.Query, c.Labels, c.Line, got, c.Out)
		}
		want := map[string]string{}
		for k, v := range c.OutLbl {
			if v != "" {
				want[k] = v
			}
		}
		got := map[string]string{}
		for k, v := range row {
			if k != "_msg" && v != "" {
				got[k] = v
			}
		}
		if len(got) != len(want) {
			t.Errorf("%s labels %v: lowered gives labels %v, Loki %v", c.Query, c.Labels, got, want)
			continue
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s labels %v: lowered gives labels %v, Loki %v", c.Query, c.Labels, got, want)
				break
			}
		}
	}
	if lowered < len(recorded)/2 {
		t.Fatalf("only %d of %d cases lowered", lowered, len(recorded))
	}
}
