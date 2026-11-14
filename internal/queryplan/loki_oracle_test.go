package queryplan

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The resolver against Loki's own tree: for every plan-corpus query both parsers accept, the test oracle under
// test/oracle/loki (a module of its own; Loki is AGPL-3.0 and only that module imports it) writes what Loki's
// syntax tree holds, and the resolved plan must say the same: whether the outermost vector aggregation is
// ungrouped, the offset of each range aggregation, and the parser and drop stages of the pipeline under the
// aggregations (every stage, in order), with whether the error label is dropped after the last parser and the
// handlers' narrower condition (an error-only drop right after the last parser, as the last stage). LVP_RUN_ORACLES=1 runs it (it
// builds the oracle); the PR quality report runs it with the other oracles.

const lokiOracleDir = "../../test/oracle/loki"

type lokiStage struct {
	Kind        string   `json:"kind"`
	Names       []string `json:"names"`
	Conditional bool     `json:"conditional"`
}

type lokiFacts struct {
	ID       string `json:"id"`
	Accept   bool   `json:"accept"`
	Error    string `json:"error"`
	Outer    string `json:"outer"`
	Grouping *struct {
		Groups  []string `json:"groups"`
		Without bool     `json:"without"`
	} `json:"grouping"`
	Offsets     []int64     `json:"offsets"`
	Pipeline    []lokiStage `json:"pipeline"`
	HasPipeline bool        `json:"has_pipeline"`
	Matchers    []struct {
		Name  string `json:"name"`
		Op    string `json:"op"`
		Value string `json:"value"`
	} `json:"matchers"`
	LineFilters  []lokiLineFilter  `json:"line_filters"`
	LabelFilters []lokiLabelFilter `json:"label_filters"`
	SeriesStages []lokiStage       `json:"series_stages"`
	Unwrap       string            `json:"unwrap"`
}

type lokiLabelFilter struct {
	Tree   bool `json:"tree"`
	Leaves []struct {
		Name  string `json:"name"`
		Op    string `json:"op"`
		Value string `json:"value"`
		Kind  string `json:"kind"`
	} `json:"leaves"`
}

// ourLevelFilters and lokiLevelFilters write the label filter stages that name detected_level the same way: a stage
// of one string comparison as the label, the operator and the unquoted value (what the lowering builds the level
// filter from), any other stage as the kind of each leaf and whether it is a tree (what the lowering delegates).
// Loki's tree holds `==` as `=`.
func ourLevelFilters(sel *LogSelect) []string {
	var out []string
	for _, st := range sel.Stages {
		lf, ok := st.(*LabelFilter)
		if !ok || lf.Expr == nil || !levelNamed(lf.Expr) {
			continue
		}
		if e := lf.Expr; e.IsLeaf() && e.Kind == logql.LabelFilterValueString && e.Cmp != "<" && e.Cmp != ">" && e.Cmp != "<=" && e.Cmp != ">=" {
			op := e.Cmp
			if op == "==" {
				op = "="
			}
			out = append(out, e.Name+op+strconv.Quote(string([]rune(e.Value))))
			continue
		}
		out = append(out, fmt.Sprintf("other tree=%v", !lf.Expr.IsLeaf()))
	}
	return out
}

// levelNamed reports whether a leaf of e names a label holding detected_level.
func levelNamed(e *logql.LabelFilterExpr) bool {
	if !e.IsLeaf() {
		return levelNamed(e.Left) || levelNamed(e.Right)
	}
	return strings.Contains(e.Name, "detected_level")
}

func lokiLevelFilters(fs []lokiLabelFilter) []string {
	var out []string
	for _, lf := range fs {
		named := false
		for _, l := range lf.Leaves {
			named = named || strings.Contains(l.Name, "detected_level")
		}
		if !named {
			continue
		}
		if l := lf.Leaves; !lf.Tree && len(l) == 1 && l[0].Kind == "string" {
			out = append(out, l[0].Name+l[0].Op+strconv.Quote(l[0].Value))
			continue
		}
		out = append(out, fmt.Sprintf("other tree=%v", lf.Tree))
	}
	return out
}

// lokiOracleLevelQueries are the label filters on detected_level held to Loki's tree on top of the plan corpus: every
// operator, the empty value in both quotings, escapes, a tree, and the stages before and after.
var lokiOracleLevelQueries = []string{
	`{a="b"} | detected_level="error"`, `{a="b"} | detected_level!="info"`, `{a="b"} | detected_level=~"error|warn"`,
	`{a="b"} | detected_level!~"debug|trace"`, `{a="b"} | detected_level=""`, `{a="b"} | detected_level!=""`,
	"{a=\"b\"} | detected_level=``", "{a=\"b\"} | detected_level=`ERROR`", `{a="b"} | detected_level="\x65rror"`,
	`{a="b"} | detected_level="a\"b"`, `{a="b"} | detected_level="é"`, `{a="b"} | (detected_level="warn")`,
	`{a="b"} | detected_level = "unknown"`, `{a="b"} | detected_level="error" or detected_level="warn"`,
	`{a="b"} | detected_level="error", a="x"`, `{a="b"} | detected_level>5`, `{a="b"} | detected_level=ip("10.0.0.1")`,
	`{a="b"} |= "x" | detected_level="error" | detected_level!="x"`, `{a="b"} | drop __error__ | detected_level="warn"`,
	`{a="b"} | json | detected_level="error"`, `{a="b"} | detected_level="info" | logfmt | drop __error__`,
	`sum by (detected_level) (count_over_time({a="b"} | detected_level!="info" [1m]))`,
	`sum by (service_name) (rate({a="b"} | detected_level="error" [5m]))`,
	`topk(5, sum by (service_name) (count_over_time({a="b"} | detected_level=~"error|warn" [5m])))`,
}

type lokiLineFilter struct {
	Ty    string           `json:"ty"`
	Match string           `json:"match"`
	Op    string           `json:"op"`
	Or    []lokiLineFilter `json:"or"`
}

var lineFilterOps = map[logql.LineFilterOp]string{
	logql.LineFilterContains: "|=", logql.LineFilterExcludes: "!=", logql.LineFilterMatchRe: "|~",
	logql.LineFilterExcludeRe: "!~", logql.LineFilterContainsPat: "|>", logql.LineFilterExcludePat: "!>",
}

// ourLineFilters and lokiLineFilters write the line filters of a pipeline the same way: the match type, the value,
// ip for ip(...), and the alternatives of an `or` chain. Loki's tree holds a negated chain (`!= "a" or "b"`) as one
// negated filter per alternative, the same lines (De Morgan), so a negated chain is written that way here too.
func ourLineFilters(sel *LogSelect) []string {
	var out []string
	one := func(op, value string, ip bool) string {
		// Loki's tree comes back as JSON, which writes each byte that is not UTF-8 as U+FFFD.
		f := op + " " + strconv.Quote(string([]rune(value)))
		if ip {
			f += " ip"
		}
		return f
	}
	for _, st := range sel.Stages {
		lf, ok := st.(*LineFilter)
		if !ok {
			continue
		}
		op := lineFilterOps[lf.Op]
		if negated := op[0] == '!'; negated {
			out = append(out, one(op, lf.Value, lf.IP))
			for _, alt := range lf.Or {
				out = append(out, one(op, alt.Value, alt.IP))
			}
			continue
		}
		f := one(op, lf.Value, lf.IP)
		for _, alt := range lf.Or {
			f += " or " + strings.TrimPrefix(one("", alt.Value, alt.IP), " ")
		}
		out = append(out, f)
	}
	return out
}

func lokiLineFilters(fs []lokiLineFilter) []string {
	var out []string
	for _, lf := range fs {
		f := lf.Ty + " " + strconv.Quote(lf.Match)
		if lf.Op == "ip" {
			f += " ip"
		}
		for _, alt := range lf.Or {
			f += " or " + strconv.Quote(alt.Match)
			if alt.Op == "ip" {
				f += " ip"
			}
		}
		out = append(out, f)
	}
	return out
}

var parserKinds = map[logql.ParserType]string{
	logql.ParserJSON: "json", logql.ParserLogfmt: "logfmt", logql.ParserRegexp: "regexp",
	logql.ParserPattern: "pattern", logql.ParserUnpack: "unpack",
}

// significant is the parser and drop stages of a pipeline, in order: what the error policy reads. A drop is
// summarised as Loki's tree exposes it: the names it lists and whether it carries a condition.
func significant(stages []lokiStage) []lokiStage {
	out := []lokiStage{}
	for _, s := range stages {
		if s.Kind != "other" {
			if s.Names == nil {
				s.Names = []string{}
			}
			out = append(out, s)
		}
	}
	return out
}

func ourStages(s *LogSelect) []lokiStage {
	out := []lokiStage{}
	for _, st := range s.Stages {
		switch x := st.(type) {
		case *Parse:
			out = append(out, lokiStage{Kind: parserKinds[x.Type], Names: []string{}})
		case *Drop:
			names := append([]string{}, x.Labels...)
			for _, m := range x.Matchers {
				names = append(names, m.Name)
			}
			out = append(out, lokiStage{Kind: "drop", Names: names, Conditional: len(x.Matchers) > 0})
		}
	}
	return out
}

// lokiDropsErrorLabel applies the resolver's definition to Loki's stages: a drop after the last parser lists
// __error__ unconditionally. A conditional drop that also lists __error__ cannot be told apart in Loki's tree
// (Names lists the condition's names too), so ok is false for it.
func lokiDropsErrorLabel(stages []lokiStage) (parsers int, dropped, ok bool) {
	last := -1
	for i, s := range stages {
		if s.Kind != "drop" {
			parsers++
			last = i
		}
	}
	for _, s := range stages[last+1:] {
		for _, n := range s.Names {
			if n == "__error__" {
				if s.Conditional {
					return parsers, false, false
				}
				dropped = true
			}
		}
	}
	return parsers, dropped && parsers > 0, true
}

// allStages is every stage of a pipeline as a kind, with a run of other stages (filters, formats, keep) held as
// one: Loki's tree can hold a chain of line filters as one stage. The unwrap, and the filters after it, are not
// stages in Loki's tree.
func allStages(stages []lokiStage) []lokiStage {
	out := []lokiStage{}
	for _, s := range stages {
		if s.Names == nil {
			s.Names = []string{}
		}
		if s.Kind == "other" && len(out) > 0 && out[len(out)-1].Kind == "other" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func ourAllStages(s *LogSelect) []lokiStage {
	var out []lokiStage
	for _, st := range s.Stages {
		switch x := st.(type) {
		case *Parse:
			out = append(out, lokiStage{Kind: parserKinds[x.Type]})
		case *Drop:
			names := append([]string{}, x.Labels...)
			for _, m := range x.Matchers {
				names = append(names, m.Name)
			}
			out = append(out, lokiStage{Kind: "drop", Names: names, Conditional: len(x.Matchers) > 0})
		case *Unwrap:
			// Loki's tree holds the unwrap and the filters after it outside the stages.
			return allStages(out)
		default:
			out = append(out, lokiStage{Kind: "other"})
		}
	}
	return allStages(out)
}

// dropOnlyErrorAfterParser is the call sites' narrow condition (internal/proxy queryDropsOnlyErrorAfterParser) on
// a stage list: the last stage is an unconditional drop of error labels only, right after a parser.
func dropOnlyErrorAfterParser(stages []lokiStage) bool {
	n := len(stages)
	if n < 2 || stages[n-1].Kind != "drop" || stages[n-1].Conditional || stages[n-2].Kind == "drop" || stages[n-2].Kind == "other" {
		return false
	}
	for _, name := range stages[n-1].Names {
		if name != "__error__" && name != "__error_details__" {
			return false
		}
	}
	return len(stages[n-1].Names) > 0
}

func ourOffsets(n Node, out *[]int64) {
	switch x := n.(type) {
	case *RangeAgg:
		*out = append(*out, int64(x.Offset))
	case *VectorAgg:
		ourOffsets(x.In, out)
	case *Rank:
		ourOffsets(x.In, out)
	case *LabelReplace:
		ourOffsets(x.In, out)
	case *BinOp:
		ourOffsets(x.L, out)
		ourOffsets(x.R, out)
	}
}

func ourPipeline(n Node) *LogSelect {
	for {
		switch x := n.(type) {
		case *LogSelect:
			return x
		case *LabelReplace:
			n = x.In
		case *VectorAgg:
			n = x.In
		case *Rank:
			n = x.In
		case *RangeAgg:
			return x.In
		default:
			return nil
		}
	}
}

var vectorOps = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true, "stddev": true, "stdvar": true}

// lokiOracleSeriesQueries are the keep and drop shapes of the series identity (SeriesLabels) besides the corpus's.
var lokiOracleSeriesQueries = []string{
	`first_over_time({app="a"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
	`sum by (app) (quantile_over_time(0.9, {app="a"} | json | keep app, duration_ms | unwrap duration_ms [5m]))`,
	`stddev_over_time({app="a"} | json | keep duration_ms | unwrap duration_ms [5m])`,
	`sum(rate_counter({app="a"} | json | keep app, region, __error__, duration_ms | unwrap duration_ms [5m]))`,
	`sum by (app) (stddev_over_time({app="a"} | keep app | json | unwrap duration_ms [5m]))`,
	`first_over_time({app="a"} | json duration_ms | drop pod | unwrap duration_ms [5m])`,
	`first_over_time({app="a"} | json | keep app, duration_ms | drop app | unwrap duration_ms [5m])`,
	`first_over_time({app="a"} | json | keep app | unwrap duration_ms [5m])`,
	`first_over_time({app="a"} | json | keep app, duration_ms | label_format x=app | unwrap duration_ms [5m])`,
	`rate({app="a"} | json | keep app, pod [5m])`,
	`count_over_time({app="a"} | logfmt | keep app | json [5m])`,
	`{app="a"} | json | keep app, pod`,
}

// seriesLabelsAgainstLoki is the series identity of the pipeline (SeriesLabels) and the same reading of Loki's tree
// (its stages that decide labels and its unwrapped label), for a pipeline with a keep or a drop and none with a
// matcher; ok is false for the others.
func seriesLabelsAgainstLoki(sel *LogSelect, lf lokiFacts) (ours, lokis OutLabels, ok bool) {
	named := false
	for _, st := range sel.Stages {
		switch x := st.(type) {
		case *Keep:
			named = true
			if len(x.Matchers) > 0 {
				return OutLabels{}, OutLabels{}, false
			}
		case *Drop:
			named = true
			if len(x.Matchers) > 0 {
				return OutLabels{}, OutLabels{}, false
			}
		}
	}
	if !named {
		return OutLabels{}, OutLabels{}, false
	}
	ours = SeriesLabels(sel.Stages)
	for _, st := range lf.SeriesStages {
		switch st.Kind {
		case "keep":
			lokis = OutLabels{Kept: true}
			for _, n := range st.Names {
				if n != "__error__" && n != "__error_details__" && n != "__preserve_error__" {
					lokis.Names = append(lokis.Names, n)
				}
			}
		case "drop":
			for _, n := range st.Names {
				lokis.Names = slices.DeleteFunc(lokis.Names, func(l string) bool { return l == n })
				if !slices.Contains(lokis.Dropped, n) {
					lokis.Dropped = append(lokis.Dropped, n)
				}
			}
		case "parser":
			lokis.Dynamic = true
		case "label_format":
			lokis.Opaque = true
		}
	}
	if u := lf.Unwrap; u != "" {
		if lokis.Kept && !lokis.Dynamic && !slices.Contains(lokis.Names, u) {
			lokis.Opaque = true
		}
		lokis.Names = slices.DeleteFunc(lokis.Names, func(l string) bool { return l == u })
		lokis.Unwrapped = u
	}
	// Loki's keep stage holds its names in an order of its own: both sides are compared sorted, and an empty set
	// as none.
	for _, o := range []*OutLabels{&ours, &lokis} {
		o.Names = slices.Sorted(slices.Values(o.Names))
		if len(o.Names) == 0 {
			o.Names = nil
		}
	}
	return ours, lokis, true
}

func TestPlanMatchesLokiOracle(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	rows, err := plancorpus.Queries("../..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiOracleDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		q string
		p *Plan
	}
	var ours []row
	in := filepath.Join(work, "facts-in.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for _, q := range append(append(rows, lokiOracleLevelQueries...), lokiOracleSeriesQueries...) {
		expr, msg := logql.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			continue
		}
		p, err := Build(expr, Inputs{})
		if err != nil {
			continue
		}
		Resolve(p)
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(len(ours)), "q": q})
		w.Write(append(b, '\n'))
		ours = append(ours, row{q, p})
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	out := filepath.Join(work, "facts.jsonl")
	if err := oraclerun.Run(bin, "facts", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var problems []string
	compared, lokiRejects, ambiguous, selectors, lineFilterPipelines, levelFilterPipelines, seriesPipelines := 0, 0, 0, 0, 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var lf lokiFacts
		if err := json.Unmarshal([]byte(line), &lf); err != nil {
			t.Fatal(err)
		}
		i, _ := strconv.Atoi(lf.ID)
		r := ours[i]
		if !lf.Accept {
			lokiRejects++
			continue
		}
		compared++
		// Loki's tree holds no clause and `by ()` alike: an empty grouping.
		wantUngrouped := vectorOps[lf.Outer] && (lf.Grouping == nil || (!lf.Grouping.Without && len(lf.Grouping.Groups) == 0))
		if r.p.Sem.Ungrouped != wantUngrouped {
			problems = append(problems, fmt.Sprintf("%q: ungrouped %v, Loki's tree: %s %+v", r.q, r.p.Sem.Ungrouped, lf.Outer, lf.Grouping))
		}
		offs := []int64{}
		ourOffsets(r.p.Root, &offs)
		if !reflect.DeepEqual(offs, lf.Offsets) {
			problems = append(problems, fmt.Sprintf("%q: offsets %v, Loki's tree %v", r.q, offs, lf.Offsets))
		}
		sel := ourPipeline(r.p.Root)
		if (sel != nil) != lf.HasPipeline {
			problems = append(problems, fmt.Sprintf("%q: pipeline found %v, Loki's tree %v", r.q, sel != nil, lf.HasPipeline))
			continue
		}
		if sel == nil {
			continue
		}
		// The stream selector the lowering turns into the VictoriaLogs filter: Loki's matchers, in Loki's order.
		ms, lms := make([]string, len(sel.Matchers)), make([]string, len(lf.Matchers))
		for i, m := range sel.Matchers {
			ms[i] = m.Name + m.Op.String() + strconv.Quote(m.Value)
		}
		for i, m := range lf.Matchers {
			lms[i] = m.Name + m.Op + strconv.Quote(m.Value)
		}
		selectors++
		if !reflect.DeepEqual(ms, lms) {
			problems = append(problems, fmt.Sprintf("%q: stream matchers %v, Loki's tree %v", r.q, ms, lms))
			continue
		}
		// The line filters the lowering turns into VictoriaLogs filters: Loki's, in Loki's order.
		if ours, lokis := ourLineFilters(sel), lokiLineFilters(lf.LineFilters); len(ours)+len(lokis) > 0 {
			lineFilterPipelines++
			if !reflect.DeepEqual(ours, lokis) {
				problems = append(problems, fmt.Sprintf("%q: line filters %v, Loki's tree %v", r.q, ours, lokis))
				continue
			}
		}
		// The label filters on detected_level the lowering reads: the label, the operator and the value of one
		// comparison, Loki's (the leaf the lowering builds the filter on Loki's ingest-time value from).
		if ours, lokis := ourLevelFilters(sel), lokiLevelFilters(lf.LabelFilters); len(ours)+len(lokis) > 0 {
			levelFilterPipelines++
			if !reflect.DeepEqual(ours, lokis) {
				problems = append(problems, fmt.Sprintf("%q: detected_level filters %v, Loki's tree %v", r.q, ours, lokis))
				continue
			}
		}
		// The labels a sample carries after the pipeline (SeriesLabels), against the same reading of the stages and
		// the unwrapped label in Loki's tree. A keep or a drop with a matcher is left out: Loki's tree lists the
		// matcher's name among the keep's labels.
		if ours, lokis, ok := seriesLabelsAgainstLoki(sel, lf); ok {
			seriesPipelines++
			if !reflect.DeepEqual(ours, lokis) {
				problems = append(problems, fmt.Sprintf("%q: series labels %+v, Loki's tree %+v", r.q, ours, lokis))
				continue
			}
		}
		mine, loki := ourStages(sel), significant(lf.Pipeline)
		if !reflect.DeepEqual(mine, loki) {
			problems = append(problems, fmt.Sprintf("%q: parser and drop stages %+v, Loki's tree %+v", r.q, mine, loki))
			continue
		}
		if mineAll, lokiAll := ourAllStages(sel), allStages(lf.Pipeline); !reflect.DeepEqual(mineAll, lokiAll) {
			problems = append(problems, fmt.Sprintf("%q: stages %+v, Loki's tree %+v", r.q, mineAll, lokiAll))
			continue
		} else if narrow, want := dropOnlyErrorAfterParser(mineAll), dropOnlyErrorAfterParser(lokiAll); narrow != want {
			problems = append(problems, fmt.Sprintf("%q: error-only drop after the parser %v, Loki's tree %v", r.q, narrow, want))
		}
		parsers, dropped, ok := lokiDropsErrorLabel(loki)
		if !ok {
			ambiguous++
			continue
		}
		if sel.Sem.Parsers != parsers || (sel.Sem.ErrorPolicy == ErrorDroppedLabel) != dropped {
			problems = append(problems, fmt.Sprintf("%q: %d parsers, error policy %d; Loki's tree: %d parsers, error label dropped %v", r.q, sel.Sem.Parsers, sel.Sem.ErrorPolicy, parsers, dropped))
		}
	}
	t.Logf("%d queries compared with Loki's tree (%d stream selectors, %d with line filters, %d with detected_level filters, %d with a keep or a drop), %d the proxy accepts and Loki rejects (the parse oracle reports them), %d conditional drops of __error__ left out", compared, selectors, lineFilterPipelines, levelFilterPipelines, seriesPipelines, lokiRejects, ambiguous)
	if seriesPipelines < len(lokiOracleSeriesQueries) {
		t.Fatalf("only %d pipelines with a keep or a drop compared", seriesPipelines)
	}
	if compared < 1000 {
		t.Fatalf("only %d queries compared", compared)
	}
	if len(problems) > 0 {
		t.Fatalf("%d differences from Loki's tree:\n%s", len(problems), strings.Join(problems, "\n"))
	}
}
