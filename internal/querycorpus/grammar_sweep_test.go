package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The grammar sweep holds the proxy to Loki and to VictoriaLogs on the queries of grammar_sweep_gen_test.go, which
// enumerate every node, stage, function, modifier and option of the LogQL grammar in combination.
//
//   - Loki's answers (the Loki oracle, test/oracle/loki: check for acceptance and error text, facts for the tree of
//     an accepted query) are recorded in testdata/grammar-sweep-loki.jsonl.gz, under the hash of the query set.
//   - VictoriaLogs' answers (the VictoriaLogs oracle, test/oracle/vl: roundtrip) are recorded in
//     testdata/grammar-sweep-vl.jsonl.gz for every LogsQL text the proxy writes for those queries (the string
//     translator's, and the query plan's), on v1.49.0 and v1.53.0, under the text.
//   - The proxy's answers are computed: the handlers' validation (sweepValidate: the request rewrite, Loki's dotted-name
//     error, logql.ValidateLogQLExpr), the query plan's tree, and the LogsQL of the plan and of the translator.
//
// Every query where the answers differ falls into a class: the kind of difference, Loki's and the proxy's answer
// without its numbers and names, and the grammar element all its queries share. A class is known when
// conformance/registry/grammar-sweep.json lists it with the registry case that holds it open; the test fails for a
// class not listed, for a class without a case, and when a class holds more queries than recorded. A class that holds
// fewer fails too until the file is rewritten (LVP_QUERY_PLAN_UPDATE=1), so the known differences only shrink.
// Rewriting keeps the case of each class; a class with a new id (a fix regrouped its queries) takes the one case its kind and answer had, or
// the first of the file's case_rules that matches, and nothing is written while a LogsQL text has no recorded
// VictoriaLogs answer. Up to 5 percent of the texts may lack one in a plain run (a changed translator or plan text),
// so a change to the text a construct writes does not fail this test before the answers are recorded again.
//
// LVP_RUN_ORACLES=1 records the answers again. The VictoriaLogs oracle downloads VictoriaLogs: set GOMODCACHE to a
// fresh directory (as for TestPlanTextsSurviveVictoriaLogsClone), because the shared module cache may hold a copy
// of VictoriaLogs whose checksum is not the repository's. LVP_GRAMMAR_SWEEP_SIZE=full sweeps the nightly size.

const (
	sweepLokiPath   = "testdata/grammar-sweep-loki.jsonl.gz"
	sweepVLPath     = "testdata/grammar-sweep-vl.jsonl.gz"
	sweepFile       = "../../conformance/registry/grammar-sweep.json"
	sweepFullLoki   = "testdata/grammar-sweep-full-loki.jsonl.gz"
	sweepFullVL     = "testdata/grammar-sweep-full-vl.jsonl.gz"
	sweepUnrecorded = 0.05
)

var sweepVersions = []string{"v1.49.0", "v1.53.0"}

// ---- the answers ----

// sweepFacts are the facts of an accepted query's tree both sides can say: the outermost aggregation, whether it is
// ungrouped, the offsets of the range aggregations, the stream selector's matchers, the kinds of the stages, and the
// binary operation at the top.
type sweepFacts struct {
	Outer     string   `json:"o,omitempty"`
	Ungrouped bool     `json:"u,omitempty"`
	Offsets   []int64  `json:"of"`
	Matchers  []string `json:"m,omitempty"`
	Stages    []string `json:"st,omitempty"`
	Pipeline  bool     `json:"hp,omitempty"`
	Binary    string   `json:"b,omitempty"`
}

type sweepLokiRow struct {
	Q      string      `json:"q,omitempty"`
	Accept bool        `json:"a"`
	Err    string      `json:"e,omitempty"`
	Facts  *sweepFacts `json:"f,omitempty"`
	// Header rows only.
	Hash  string `json:"hash,omitempty"`
	Count int    `json:"n,omitempty"`
}

type sweepVLVerdict struct {
	Accept bool   `json:"a"`
	OK     bool   `json:"ok"`
	Err    string `json:"e,omitempty"`
}

type sweepVLRow struct {
	Text string                    `json:"t"`
	V    map[string]sweepVLVerdict `json:"v"`
}

// sweepText is one LogsQL text the proxy writes, and whether it is a plan's or the translator's.
type sweepText struct {
	Text   string
	Source string // plan, translator
}

type sweepProxy struct {
	Msg          string // logql.ValidateLogQL
	Facts        *sweepFacts
	TranslateErr string
	Texts        map[string][]sweepText // by version
}

var sweepIdentity = func(s string) string { return s }

// sweepLogsQL collects the VictoriaLogs query texts an operator tree holds.
func sweepLogsQL(v reflect.Value, out *[]string) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			sweepLogsQL(v.Elem(), out)
		}
	case reflect.Struct:
		switch x := v.Interface().(type) {
		case physical.VLQuery:
			*out = append(*out, x.Text)
			return
		case physical.VLStats:
			*out = append(*out, x.Text)
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				sweepLogsQL(v.Field(i), out)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			sweepLogsQL(v.Index(i), out)
		}
	}
}

func sweepOpTexts(op physical.Op) []string {
	var texts []string
	sweepLogsQL(reflect.ValueOf(&op), &texts)
	return texts
}

var sweepParserKinds = map[logql.ParserType]string{
	logql.ParserJSON: "json", logql.ParserLogfmt: "logfmt", logql.ParserRegexp: "regexp",
	logql.ParserPattern: "pattern", logql.ParserUnpack: "unpack",
}

var sweepVectorOps = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true, "stddev": true, "stdvar": true}

func sweepOffsets(n queryplan.Node, out *[]int64) {
	switch x := n.(type) {
	case *queryplan.RangeAgg:
		*out = append(*out, int64(x.Offset))
	case *queryplan.VectorAgg:
		sweepOffsets(x.In, out)
	case *queryplan.Rank:
		sweepOffsets(x.In, out)
	case *queryplan.LabelReplace:
		sweepOffsets(x.In, out)
	case *queryplan.BinOp:
		sweepOffsets(x.L, out)
		sweepOffsets(x.R, out)
	}
}

func sweepPipeline(n queryplan.Node) *queryplan.LogSelect {
	for {
		switch x := n.(type) {
		case *queryplan.LogSelect:
			return x
		case *queryplan.LabelReplace:
			n = x.In
		case *queryplan.VectorAgg:
			n = x.In
		case *queryplan.Rank:
			n = x.In
		case *queryplan.RangeAgg:
			return x.In
		default:
			return nil
		}
	}
}

// sweepStageKinds writes the stages as Loki's tree holds them: a parser or a drop is a kind of its own, every other
// stage is "other" and a run of them is one (Loki's tree can hold a chain of line filters as one stage); the unwrap
// and the filters after it are not stages there.
func sweepStageKinds(sel *queryplan.LogSelect) []string {
	var out []string
	add := func(k string) {
		if k == "other" && len(out) > 0 && out[len(out)-1] == "other" {
			return
		}
		out = append(out, k)
	}
	for _, st := range sel.Stages {
		switch x := st.(type) {
		case *queryplan.Parse:
			add(sweepParserKinds[x.Type])
		case *queryplan.Drop:
			add("drop")
		case *queryplan.Unwrap:
			return out
		default:
			add("other")
		}
	}
	return out
}

// sweepProxyFacts reads the facts out of the proxy's plan.
func sweepProxyFacts(plan *queryplan.Plan) *sweepFacts {
	f := &sweepFacts{Offsets: []int64{}}
	top := plan.Root
	for {
		lr, ok := top.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		top = lr.In
	}
	switch x := top.(type) {
	case *queryplan.VectorAgg:
		f.Outer = string(x.Op)
	case *queryplan.Rank:
		f.Outer = string(x.Op)
	case *queryplan.RangeAgg:
		f.Outer = string(x.Op)
	case *queryplan.BinOp:
		f.Binary = sweepBinaryFact(x)
	}
	f.Ungrouped = plan.Sem.Ungrouped
	sweepOffsets(plan.Root, &f.Offsets)
	if sel := sweepPipeline(plan.Root); sel != nil {
		f.Pipeline = true
		for _, m := range sel.Matchers {
			f.Matchers = append(f.Matchers, m.Name+m.Op.String()+strconv.Quote(string([]rune(m.Value))))
		}
		f.Stages = sweepStageKinds(sel)
	}
	return f
}

// sweepRequestQuery is the query a handler reads: the middleware removes comments and parser flags and writes the
// keywords in lower case before any handler validates it (lowercaseQueryKeywords).
func sweepRequestQuery(q string) string {
	return logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(q)))
}

// sweepValidate is the handlers' validation (Proxy.validateQuery) of a client query in the Loki-compatible profile
// (the default): Loki's error for a dotted name, then the typed parser's validation.
func sweepValidate(q string) (logql.Expr, string) {
	rq := sweepRequestQuery(q)
	if msg := logql.InputSizeError(q); msg != "" {
		return nil, msg
	}
	if msg := logql.DottedNameError(rq); msg != "" {
		return nil, msg
	}
	return logql.ValidateLogQLExpr(rq)
}

// sweepEval is the proxy's answer for q: the validation the handlers run, the plan's facts, and the LogsQL of the
// plan and of the translator for each VictoriaLogs version.
func sweepEval(q string) sweepProxy {
	a := sweepProxy{Texts: map[string][]sweepText{}}
	expr, msg := sweepValidate(q)
	rq := sweepRequestQuery(q)
	if msg != "" {
		a.Msg = msg
		return a
	}
	if expr == nil {
		return a
	}
	// The handlers validate the client's query, then clamp a quantile above 1 (VictoriaLogs rejects it) and plan and
	// translate that text.
	rq = proxy.RewriteQuantilePhiGT1(rq)
	if rexpr, err := logql.Parse(rq); err == nil {
		expr = rexpr
	}
	plan, err := queryplan.Build(expr, queryplan.Inputs{})
	if err == nil {
		queryplan.Resolve(plan)
		a.Facts = sweepProxyFacts(plan)
	}
	rewritten := logql.CanonicalizeLogRanges(rq)
	for _, v := range sweepVersions {
		caps := logsql.CapabilitiesFor(strings.TrimPrefix(v, "v"))
		seen := map[string]bool{}
		add := func(source string, texts []string) {
			for _, t := range texts {
				// rate_counter's stats function is a marker the proxy answers itself: the text is never sent.
				if strings.Contains(t, rateCounterMarker) {
					continue
				}
				t = logsql.GuardReservedFieldCase(t)
				if !seen[source+t] {
					seen[source+t] = true
					a.Texts[v] = append(a.Texts[v], sweepText{Text: t, Source: source})
				}
			}
		}
		if plan != nil {
			if op, err := lower.PlanQuery(rq, plan, lower.Profile{Caps: caps, Label: sweepIdentity}); err == nil {
				add("plan", sweepOpTexts(op))
			}
		}
		translate := func(s string) (string, error) {
			out, err := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(s), sweepIdentity, nil, caps)
			if err == nil && strings.HasPrefix(strings.TrimSpace(out), "|") {
				out = "* " + strings.TrimSpace(out)
			}
			return out, err
		}
		// A ranking, stddev or stdvar over a range aggregation is evaluated by the proxy: it sends the operand's
		// request, the whole query is never translated.
		if op, _, routed, err := proxy.PostAggregationPhysical(rq, translate); routed {
			if err != nil {
				a.TranslateErr = err.Error()
				continue
			}
			add("translator", sweepOpTexts(op))
			continue
		}
		// A binary operation is answered operand by operand: each operand is a query of its own, a vector(n) operand
		// is answered from the constant, and a scalar is no query.
		for _, operand := range sweepOperands(expr, rewritten) {
			out, err := translator.TranslateLogQLWithCapabilities(operand, sweepIdentity, nil, caps)
			if err != nil {
				a.TranslateErr = err.Error()
				continue
			}
			if op, err := translator.DecodePhysical(out); err == nil {
				add("translator", sweepOpTexts(op))
			}
		}
	}
	return a
}

// sweepOperands are the queries the handlers translate for expr, whose text is whole: the operands of a binary
// operation (through the aggregations over it), each as a query of its own, and the whole text otherwise.
func sweepOperands(expr logql.Expr, whole string) []string {
	inner := expr
	for {
		agg, ok := inner.(*logql.VectorAggregation)
		if !ok {
			break
		}
		inner = agg.Inner
	}
	bin, ok := inner.(*logql.BinOpExpr)
	if !ok {
		return []string{whole}
	}
	var out []string
	var walk func(e logql.Expr)
	walk = func(e logql.Expr) {
		switch x := e.(type) {
		case *logql.BinOpExpr:
			walk(x.Left)
			walk(x.Right)
		case *logql.LiteralExpr:
		default:
			if text := e.String(); !strings.HasPrefix(text, "vector(") {
				out = append(out, logql.CanonicalizeLogRanges(text))
			}
		}
	}
	walk(bin)
	return out
}

// ---- recording ----

func sweepPaths() (loki, vl string) {
	if os.Getenv("LVP_GRAMMAR_SWEEP_SIZE") == "full" {
		return sweepFullLoki, sweepFullVL
	}
	return sweepLokiPath, sweepVLPath
}

func writeGzipLines[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	w := bufio.NewWriter(zw)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readGzipLines(t *testing.T, path string, each func(line []byte)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("%v: record the answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		each(sc.Bytes())
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

func writeJSONLines(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	var b strings.Builder
	for _, r := range rows {
		line, _ := json.Marshal(r)
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readJSONLines[T any](t *testing.T, path string) []T {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// lokiFactsRow is the part of the Loki oracle's facts answer the sweep compares.
type lokiFactsRow struct {
	ID       string `json:"id"`
	Accept   bool   `json:"accept"`
	Outer    string `json:"outer"`
	Grouping *struct {
		Groups  []string `json:"groups"`
		Without bool     `json:"without"`
	} `json:"grouping"`
	Offsets     []int64 `json:"offsets"`
	HasPipeline bool    `json:"has_pipeline"`
	Pipeline    []struct {
		Kind string `json:"kind"`
	} `json:"pipeline"`
	Matchers []struct {
		Name  string `json:"name"`
		Op    string `json:"op"`
		Value string `json:"value"`
	} `json:"matchers"`
	Binary *struct {
		Op         string   `json:"op"`
		ReturnBool bool     `json:"return_bool"`
		Matching   bool     `json:"matching"`
		On         bool     `json:"on"`
		Labels     []string `json:"labels"`
		Card       string   `json:"card"`
		Include    []string `json:"include"`
	} `json:"binary"`
}

func recordSweepLoki(t *testing.T, qs []sweepQuery, path string) {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(lokiFactsDir, "", work, "loki")
	if err != nil {
		t.Fatal(err)
	}
	// ours is true so the oracle compares no error text: the proxy's answer is computed when the recording is read,
	// so the recording does not depend on it.
	var in []map[string]any
	for i, q := range qs {
		in = append(in, map[string]any{"id": strconv.Itoa(i), "kind": "sweep", "ours": true, "q": q.Q})
	}
	inPath, outPath := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	writeJSONLines(t, inPath, in)
	if err := oraclerun.Run(bin, "check", "-in", inPath, "-out", outPath); err != nil {
		t.Fatal(err)
	}
	verdicts := readVerdictsWithError(t, outPath)
	var accepted []map[string]any
	for i, q := range qs {
		if verdicts[strconv.Itoa(i)].Accept {
			accepted = append(accepted, map[string]any{"id": strconv.Itoa(i), "q": q.Q})
		}
	}
	// The oracle's facts command cannot encode a NaN or an infinite literal (its JSON has no such number; Loki folds
	// `0 / 0` and `1 / 0` to them): a batch it fails on is halved until the queries it fails on stand alone, and
	// those have no facts.
	facts := map[string]lokiFactsRow{}
	var runFacts func(items []map[string]any, depth int)
	runFacts = func(items []map[string]any, depth int) {
		if len(items) == 0 {
			return
		}
		factsIn, factsOut := filepath.Join(work, fmt.Sprintf("facts-in-%d.jsonl", depth)), filepath.Join(work, fmt.Sprintf("facts-out-%d.jsonl", depth))
		writeJSONLines(t, factsIn, items)
		if err := oraclerun.Run(bin, "facts", "-in", factsIn, "-out", factsOut); err != nil {
			if len(items) == 1 {
				return
			}
			runFacts(items[:len(items)/2], depth+1)
			runFacts(items[len(items)/2:], depth+1)
			return
		}
		for _, r := range readJSONLines[lokiFactsRow](t, factsOut) {
			facts[r.ID] = r
		}
	}
	runFacts(accepted, 0)
	rows := []sweepLokiRow{{Hash: sweepHash(qs), Count: len(qs)}}
	for i, q := range qs {
		id := strconv.Itoa(i)
		v := verdicts[id]
		row := sweepLokiRow{Q: q.Q, Accept: v.Accept, Err: v.Error}
		if f, ok := facts[id]; ok && f.Accept {
			row.Facts = lokiFactsToSweep(f)
		}
		rows = append(rows, row)
	}
	writeGzipLines(t, path, rows)
}

type lokiVerdictWithError struct {
	ID     string `json:"id"`
	Accept bool   `json:"accept"`
	Error  string `json:"error"`
}

func readVerdictsWithError(t *testing.T, path string) map[string]lokiVerdictWithError {
	t.Helper()
	out := map[string]lokiVerdictWithError{}
	for _, v := range readJSONLines[lokiVerdictWithError](t, path) {
		out[v.ID] = v
	}
	return out
}

func lokiFactsToSweep(f lokiFactsRow) *sweepFacts {
	s := &sweepFacts{Outer: f.Outer, Offsets: f.Offsets, Pipeline: f.HasPipeline}
	if s.Offsets == nil {
		s.Offsets = []int64{}
	}
	s.Ungrouped = sweepVectorOps[f.Outer] && (f.Grouping == nil || (!f.Grouping.Without && len(f.Grouping.Groups) == 0))
	for _, m := range f.Matchers {
		s.Matchers = append(s.Matchers, m.Name+m.Op+strconv.Quote(m.Value))
	}
	var out []string
	for _, st := range f.Pipeline {
		if st.Kind == "other" && len(out) > 0 && out[len(out)-1] == "other" {
			continue
		}
		out = append(out, st.Kind)
	}
	s.Stages = out
	if b := f.Binary; b != nil {
		s.Binary = b.Op
		if b.ReturnBool {
			s.Binary += " bool"
		}
		if b.Matching && (b.On || len(b.Labels) > 0 || len(b.Include) > 0 || strings.Contains(b.Card, "ManyTo") || strings.Contains(b.Card, "OneToMany") || strings.Contains(strings.ToLower(b.Card), "many")) {
			card := "on"
			if !b.On {
				card = "ignoring"
			}
			side := ""
			switch {
			case strings.Contains(b.Card, "ManyToOne"), strings.Contains(strings.ToLower(b.Card), "many-to-one"):
				side = "group_left"
			case strings.Contains(b.Card, "OneToMany"), strings.Contains(strings.ToLower(b.Card), "one-to-many"):
				side = "group_right"
			}
			s.Binary += fmt.Sprintf(" %s%v %s%v", card, nonNil(b.Labels), side, nonNil(b.Include))
		}
	}
	return s
}

// sweepTexts is, for every query the proxy accepts (Loki's answer or not: a query Loki rejects can still reach
// VictoriaLogs through the proxy), the LogsQL texts the proxy writes for it.
func sweepTexts(qs []sweepQuery) map[string]bool {
	set := map[string]bool{}
	for _, q := range qs {
		for _, texts := range sweepEval(q.Q).Texts {
			for _, t := range texts {
				set[t.Text] = true
			}
		}
	}
	return set
}

func recordSweepVL(t *testing.T, texts map[string]bool, path string) {
	t.Helper()
	list := make([]string, 0, len(texts))
	for tx := range texts {
		list = append(list, tx)
	}
	sort.Strings(list)
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var rows []map[string]any
	for i, tx := range list {
		rows = append(rows, map[string]any{"id": strconv.Itoa(i), "q": tx})
	}
	writeJSONLines(t, in, rows)
	out := make([]sweepVLRow, len(list))
	for i, tx := range list {
		out[i] = sweepVLRow{Text: tx, V: map[string]sweepVLVerdict{}}
	}
	type roundtripRow struct {
		ID     string `json:"id"`
		Accept bool   `json:"accept"`
		Error  string `json:"error"`
		OK     bool   `json:"ok"`
		Panic  string `json:"panic"`
	}
	for _, v := range sweepVersions {
		modfile := ""
		if file := oracleModfile(v, goldenVersions(t)[0]); file != "go.mod" {
			modfile = file
		}
		bin, err := oraclerun.Build(oracleDir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		verdicts := filepath.Join(work, v+".jsonl")
		if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", verdicts); err != nil {
			t.Fatal(err)
		}
		for _, r := range readJSONLines[roundtripRow](t, verdicts) {
			i, _ := strconv.Atoi(r.ID)
			e := r.Error
			if r.Panic != "" {
				e = r.Panic
			}
			if len(e) > 160 {
				e = e[:160]
			}
			out[i].V[v] = sweepVLVerdict{Accept: r.Accept, OK: r.OK, Err: e}
		}
	}
	writeGzipLines(t, path, out)
}

func readSweepLoki(t *testing.T, path string, qs []sweepQuery) map[string]sweepLokiRow {
	t.Helper()
	out := map[string]sweepLokiRow{}
	var header sweepLokiRow
	readGzipLines(t, path, func(line []byte) {
		var r sweepLokiRow
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		if r.Hash != "" {
			header = r
			return
		}
		out[r.Q] = r
	})
	if want := sweepHash(qs); header.Hash != want {
		t.Fatalf("%s records the answers for another query set (%s, %d queries; the generator makes %s, %d queries): record them again with LVP_RUN_ORACLES=1", path, header.Hash, header.Count, want, len(qs))
	}
	return out
}

func readSweepVL(t *testing.T, path string) map[string]sweepVLRow {
	t.Helper()
	out := map[string]sweepVLRow{}
	readGzipLines(t, path, func(line []byte) {
		var r sweepVLRow
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		out[r.Text] = r
	})
	return out
}

// ---- classes ----

var (
	sweepDigits = regexp.MustCompile(`[0-9]+`)
	sweepQuoted = regexp.MustCompile("\"(?:[^\"\\\\]|\\\\.)*\"|`[^`]*`")
	sweepNames  = regexp.MustCompile(`'[^']*'`)
)

// sweepNorm is an error text without what is particular to its query: the numbers (positions, counts), the quoted
// strings and the names, and cut to its first 110 characters.
func sweepNorm(s string) string {
	s = sweepQuoted.ReplaceAllString(s, `"S"`)
	s = sweepNames.ReplaceAllString(s, `'S'`)
	s = sweepDigits.ReplaceAllString(s, "N")
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 110 {
		s = string(r[:110])
	}
	return s
}

var sweepPosition = regexp.MustCompile(`^parse error( at line [0-9]+, col [0-9]+)? ?: ?`)

// sweepCategory is what an error is about, for naming a class: the error without its position, and for a syntax error
// the token it stops at (not what it expected).
func sweepCategory(msg string) string {
	msg = sweepPosition.ReplaceAllString(msg, "")
	if rest, ok := strings.CutPrefix(msg, "syntax error: unexpected "); ok {
		tok, _, _ := strings.Cut(rest, ",")
		return "syntax error at " + tok
	}
	return sweepNorm(msg)
}

var sweepBrackets = regexp.MustCompile(`\[[^\]]*\]`)

// sweepNormVL is a VictoriaLogs error without the context it quotes from the query, what it expected, the hints after
// it and bracketed text.
func sweepNormVL(s string) string {
	for _, cut := range []string{"; context:", "; expecting", "; probably", "; see "} {
		if i := strings.Index(s, cut); i >= 0 {
			s = s[:i]
		}
	}
	return sweepNorm(sweepBrackets.ReplaceAllString(s, "[S]"))
}

// sweepDiff is one difference of one query.
type sweepDiff struct {
	Kind  string // the kind of difference
	Key   string // the answers, normalized
	Query string
	Loki  string // the answers as they are, for the examples
	Prox  string
	Tags  []string
}

// sweepResult is every difference of the queries against the recordings, and what was checked.
type sweepResult struct {
	diffs []sweepDiff
	// texts are the LogsQL texts checked (a text for each version), unrecorded those without a recorded VictoriaLogs
	// answer.
	texts, unrecorded int
	// vlRejectsOfLokiRejects counts the texts VictoriaLogs rejects that the proxy writes for a query Loki rejects: a
	// rejection there is a rejection of a query that should not get so far, so it is counted, not classified; a clone
	// failure (which restarts VictoriaLogs) is a difference of its own.
	vlRejectsOfLokiRejects int
}

func sweepDiffs(t *testing.T, qs []sweepQuery, loki map[string]sweepLokiRow, vl map[string]sweepVLRow) sweepResult {
	t.Helper()
	var res sweepResult
	for _, q := range qs {
		l, ok := loki[q.Q]
		if !ok {
			t.Fatalf("%q has no recorded Loki answer: record them again with LVP_RUN_ORACLES=1", q.Q)
		}
		p := sweepEval(q.Q)
		add := func(kind, key, lokiAnswer, proxyAnswer string) {
			res.diffs = append(res.diffs, sweepDiff{Kind: kind, Key: key, Query: q.Q, Loki: lokiAnswer, Prox: proxyAnswer, Tags: q.Tags})
		}
		// vlCheck holds the texts the proxy writes to VictoriaLogs' answer: one difference per query, source and kind of
		// failure, naming the versions it fails on.
		vlCheck := func(lokiRejects bool) {
			type vlFail struct{ kind, norm, text, err string }
			fails := map[vlFail][]string{}
			var order []vlFail
			for _, v := range sweepVersions {
				for _, tx := range p.Texts[v] {
					res.texts++
					r, ok := vl[tx.Text]
					if !ok {
						res.unrecorded++
						continue
					}
					x := r.V[v]
					var f vlFail
					switch {
					case !x.Accept && lokiRejects:
						res.vlRejectsOfLokiRejects++
						continue
					case !x.Accept:
						f = vlFail{"VictoriaLogs rejects the " + tx.Source + " text", sweepNormVL(x.Err), tx.Text, x.Err}
					case !x.OK && lokiRejects:
						f = vlFail{"VictoriaLogs fails on the clone of the " + tx.Source + " text of a query Loki rejects", sweepNormVL(x.Err), tx.Text, x.Err}
					case !x.OK:
						f = vlFail{"VictoriaLogs fails on the clone of the " + tx.Source + " text", sweepNormVL(x.Err), tx.Text, x.Err}
					default:
						continue
					}
					if _, seen := fails[f]; !seen {
						order = append(order, f)
					}
					fails[f] = append(fails[f], v)
				}
			}
			for _, f := range order {
				add(f.kind, f.norm+" ["+strings.Join(fails[f], "+")+"]", "accepted", "LogsQL "+f.text+" -> VictoriaLogs "+f.err)
			}
		}
		accepts := p.Msg == ""
		switch {
		case accepts && !l.Accept && p.TranslateErr != "":
			// The validation lets it through and the translation fails: the client gets an error, with another text.
			add("error text differs, rejected at translation", sweepCategory(l.Err)+" || "+sweepNorm(p.TranslateErr), l.Err, "translation: "+p.TranslateErr)
			vlCheck(true)
			continue
		case accepts && !l.Accept:
			add("proxy accepts, Loki rejects", sweepCategory(l.Err), l.Err, "accepted")
			vlCheck(true)
			continue
		case !accepts && l.Accept:
			add("proxy rejects, Loki accepts", sweepCategory(p.Msg), "accepted", p.Msg)
			continue
		case !accepts && !l.Accept:
			if p.Msg != l.Err {
				add("error text differs", sweepCategory(l.Err)+" || "+sweepCategory(p.Msg), l.Err, p.Msg)
			}
			continue
		}
		if p.TranslateErr != "" {
			add("accepted, translation fails", sweepNorm(p.TranslateErr), "accepted", p.TranslateErr)
		}
		if l.Facts != nil && p.Facts != nil {
			for _, d := range sweepFactDiffs(*l.Facts, *p.Facts) {
				add("tree fact differs: "+d[0], d[0], d[1], d[2])
			}
		}
		vlCheck(false)
	}
	return res
}

// sweepFactDiffs are the facts of Loki's tree and of the proxy's plan that differ: the fact, Loki's value, the proxy's.
func sweepFactDiffs(l, p sweepFacts) [][3]string {
	var out [][3]string
	diff := func(name string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			out = append(out, [3]string{name, fmt.Sprint(a), fmt.Sprint(b)})
		}
	}
	diff("outermost operation", l.Outer, p.Outer)
	diff("ungrouped", l.Ungrouped, p.Ungrouped)
	diff("offsets", l.Offsets, p.Offsets)
	diff("has a log pipeline", l.Pipeline, p.Pipeline)
	if l.Pipeline && p.Pipeline {
		diff("stream matchers", l.Matchers, p.Matchers)
		diff("stage kinds", l.Stages, p.Stages)
	}
	diff("binary operation", l.Binary, p.Binary)
	return out
}

// sweepClass is a class of differences: the kind, the answers and the grammar element every query of it holds.
type sweepClass struct {
	ID, Kind, Key, Element string
	Queries                []sweepDiff
	Examples               []sweepDiff
}

// sweepClasses groups the differences. Within a kind and an answer, the element held by most queries takes them, and
// again for the rest (a greedy cover), so a class names the part of the grammar that causes it. Elements held by many
// more queries without the difference come after the specific ones, and the shortest queries are the examples.
func sweepClasses(diffs []sweepDiff, qs []sweepQuery) []sweepClass {
	freq := map[string]int{}
	for _, q := range qs {
		for _, tg := range q.Tags {
			freq[tg]++
		}
	}
	buckets := map[string][]sweepDiff{}
	var keys []string
	for _, d := range diffs {
		k := d.Kind + " | " + d.Key
		if _, ok := buckets[k]; !ok {
			keys = append(keys, k)
		}
		buckets[k] = append(buckets[k], d)
	}
	sort.Strings(keys)
	var classes []sweepClass
	for _, k := range keys {
		rest := buckets[k]
		kind := rest[0].Kind
		for len(rest) > 0 {
			count := map[string]int{}
			for _, d := range rest {
				seen := map[string]bool{}
				for _, tg := range d.Tags {
					if !seen[tg] {
						seen[tg] = true
						count[tg]++
					}
				}
			}
			// The element that covers most of the differences and is rarest outside them: n*n/freq, the share of the
			// differences it holds times the share of its queries that differ.
			best, bestScore := "", 0.0
			for tg, n := range count {
				score := float64(n) * float64(n) / float64(freq[tg])
				if score > bestScore || score == bestScore && tg < best {
					best, bestScore = tg, score
				}
			}
			var in, out []sweepDiff
			for _, d := range rest {
				held := false
				for _, tg := range d.Tags {
					held = held || tg == best
				}
				if held {
					in = append(in, d)
				} else {
					out = append(out, d)
				}
			}
			if best == "" {
				in, out = rest, nil
			}
			sort.Slice(in, func(i, j int) bool {
				if len(in[i].Query) != len(in[j].Query) {
					return len(in[i].Query) < len(in[j].Query)
				}
				return in[i].Query < in[j].Query
			})
			c := sweepClass{ID: k + " | " + best, Kind: kind, Key: rest[0].Key, Element: best, Queries: in}
			for i := 0; i < len(in) && len(c.Examples) < 3; i++ {
				c.Examples = append(c.Examples, in[i])
			}
			classes = append(classes, c)
			rest = out
		}
	}
	return classes
}

// ---- the ratchet file ----

type sweepClassRecord struct {
	Count   int    `json:"count"`
	Case    string `json:"case"`
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Element string `json:"element"`
	// Examples are the shortest queries of the class with both answers.
	Examples []sweepExample `json:"examples"`
}

// sweepCaseRule gives a class the registry case that holds it when the class is new and no class of its kind and
// answer had a case: the first rule whose expression matches the class id wins.
type sweepCaseRule struct {
	Match string `json:"match"`
	Case  string `json:"case"`
}

type sweepExample struct {
	Query string `json:"query"`
	Loki  string `json:"loki"`
	Proxy string `json:"proxy"`
}

type sweepRecord struct {
	Description string                      `json:"description"`
	Queries     int                         `json:"queries"`
	Hash        string                      `json:"query_set_hash"`
	Accepted    map[string]int              `json:"loki_answers"`
	ByKind      map[string]int              `json:"differences_by_kind"`
	Texts       int                         `json:"logsql_texts_checked"`
	Unrecorded  int                         `json:"logsql_texts_unrecorded"`
	VLRejects   int                         `json:"victorialogs_rejections_for_queries_loki_rejects"`
	CaseRules   []sweepCaseRule             `json:"case_rules"`
	Classes     map[string]sweepClassRecord `json:"classes"`
}

const sweepDescription = "Counts of the grammar sweep (internal/querycorpus TestGrammarSweep): the queries enumerated from the LogQL grammar, Loki's answers to them, and every class of difference between the proxy and Loki or VictoriaLogs. A class is the kind of difference, the two answers without their numbers and names, and the grammar element all its queries hold. Each class is held open by a registry case (`case`); a class may only shrink, a new class fails the test, and LVP_QUERY_PLAN_UPDATE=1 rewrites this file keeping the cases of the classes that remain."

func sweepRecordOf(qs []sweepQuery, loki map[string]sweepLokiRow, classes []sweepClass, res sweepResult, old *sweepRecord) sweepRecord {
	r := sweepRecord{Description: sweepDescription, Queries: len(qs), Hash: sweepHash(qs), Accepted: map[string]int{"accepted": 0, "rejected": 0}, ByKind: map[string]int{},
		Texts: res.texts, Unrecorded: res.unrecorded, VLRejects: res.vlRejectsOfLokiRejects, Classes: map[string]sweepClassRecord{}}
	if old != nil {
		r.CaseRules = old.CaseRules
	}
	// A class keeps its registry case. A class with a new id (a fix regrouped the queries of its kind and answer) takes
	// the case of the one case its kind and answer had before, or else the first case rule that matches its id.
	bucketCases := map[string]map[string]bool{}
	if old != nil {
		for _, c := range old.Classes {
			k := c.Kind + " | " + c.Key
			if bucketCases[k] == nil {
				bucketCases[k] = map[string]bool{}
			}
			bucketCases[k][c.Case] = true
		}
	}
	for _, q := range qs {
		if loki[q.Q].Accept {
			r.Accepted["accepted"]++
		} else {
			r.Accepted["rejected"]++
		}
	}
	for _, c := range classes {
		r.ByKind[c.Kind] += len(c.Queries)
		rec := sweepClassRecord{Count: len(c.Queries), Kind: c.Kind, Key: c.Key, Element: c.Element}
		if old != nil {
			rec.Case = old.Classes[c.ID].Case
		}
		if cases := bucketCases[c.Kind+" | "+c.Key]; rec.Case == "" && len(cases) == 1 {
			for k := range cases {
				rec.Case = k
			}
		}
		for _, rule := range r.CaseRules {
			if rec.Case == "" && regexp.MustCompile(rule.Match).MatchString(c.ID) {
				rec.Case = rule.Case
			}
		}
		for i, e := range c.Examples {
			if i == 2 {
				break
			}
			rec.Examples = append(rec.Examples, sweepExample{Query: sweepShort(e.Query), Loki: sweepShort(e.Loki), Proxy: sweepShort(e.Prox)})
		}
		r.Classes[c.ID] = rec
	}
	return r
}

// TestGrammarSweep holds the proxy to Loki and VictoriaLogs on the queries of the grammar sweep (see the comment at
// the top of this file). It replays the recorded answers; LVP_RUN_ORACLES=1 records them again, LVP_QUERY_PLAN_UPDATE=1
// rewrites conformance/registry/grammar-sweep.json (keeping the case of each class that remains),
// LVP_GRAMMAR_SWEEP_REPORT=1 logs every class with examples and LVP_GRAMMAR_SWEEP_DUMP=<file> writes every query of
// every class. LVP_GRAMMAR_SWEEP_SIZE=full sweeps the nightly size and reports without the ratchet.
//
// conformance: semantics/approx-topk-opaque-plan, semantics/binary-literal-operation-folded-in-loki, semantics/binary-on-without-label-list-accepted, semantics/drop-keep-matcher-ip-value-accepted, semantics/go-style-comment-rejected, semantics/label-replace-over-log-selector-accepted, semantics/metric-unread-line-format-template-error, semantics/parse-error-text-parity, semantics/parser-reads-unicode-blank-as-white-space, semantics/quantile-over-time-parameter-required, semantics/range-duration-grammar-wider-than-loki, semantics/set-operation-folded-literal-operand, semantics/sort-grouping-accepted, semantics/stage-after-unwrap-accepted, semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-only-space-between-tokens, semantics/translator-reads-parenthesized-metric-as-text, semantics/translator-writes-json-path-with-escaped-quote, semantics/translator-writes-leading-dot-quantile, semantics/translator-writes-negated-line-filter-after-pipe, semantics/unary-plus-literal-rejected, semantics/unknown-pipeline-stage-error-text, semantics/unwrap-in-log-query, semantics/unwrap-unknown-conversion-function-accepted, semantics/vector-aggregation-over-scalar-rejected
func TestGrammarSweep(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	qs := sweepQueries()
	lokiPath, vlPath := sweepPaths()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordSweepLoki(t, qs, lokiPath)
		recordSweepVL(t, sweepTexts(qs), vlPath)
	}
	loki := readSweepLoki(t, lokiPath, qs)
	vl := readSweepVL(t, vlPath)
	res := sweepDiffs(t, qs, loki, vl)
	diffs, unrecorded, texts := res.diffs, res.unrecorded, res.texts
	classes := sweepClasses(diffs, qs)
	t.Logf("%d queries (%d accepted by Loki), %d differences in %d classes, %d LogsQL texts checked (%d without a recorded VictoriaLogs answer)",
		len(qs), len(loki)-countRejected(loki), len(diffs), len(classes), texts, unrecorded)
	if path := os.Getenv("LVP_GRAMMAR_SWEEP_DUMP"); path != "" {
		// Every query of every class with both answers, for reading a class through (one JSON object per line).
		var b strings.Builder
		for _, c := range classes {
			for _, d := range c.Queries {
				line, _ := json.Marshal(map[string]string{"class": c.ID, "q": d.Query, "loki": d.Loki, "proxy": d.Prox})
				b.Write(line)
				b.WriteByte('\n')
			}
		}
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("LVP_GRAMMAR_SWEEP_REPORT") == "1" {
		for _, c := range classes {
			t.Logf("%5d  %s", len(c.Queries), c.ID)
			for _, e := range c.Examples {
				t.Logf("         %q\n           Loki:  %s\n           proxy: %s", e.Query, e.Loki, e.Prox)
			}
		}
	}
	if len(qs) < 1000 {
		t.Fatalf("the sweep has only %d queries", len(qs))
	}
	if float64(unrecorded) > sweepUnrecorded*float64(texts) {
		t.Errorf("%d of %d LogsQL texts have no recorded VictoriaLogs answer (over %.0f%%): record them again with LVP_RUN_ORACLES=1 and a fresh GOMODCACHE", unrecorded, texts, 100*sweepUnrecorded)
	}
	if os.Getenv("LVP_GRAMMAR_SWEEP_SIZE") == "full" {
		return
	}

	b, readErr := os.ReadFile(sweepFile)
	var old *sweepRecord
	if readErr == nil {
		old = new(sweepRecord)
		if err := json.Unmarshal(b, old); err != nil {
			t.Fatalf("%s: %v", sweepFile, err)
		}
	}
	got := sweepRecordOf(qs, loki, classes, res, old)
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		// A text without a recorded VictoriaLogs answer would drop its difference from the record as if it were fixed.
		if unrecorded > 0 {
			t.Fatalf("%d LogsQL texts have no recorded VictoriaLogs answer: record them again with LVP_RUN_ORACLES=1 and a fresh GOMODCACHE before rewriting %s", unrecorded, sweepFile)
		}
		out, _ := json.MarshalIndent(got, "", "  ")
		if err := os.WriteFile(sweepFile, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", sweepFile)
		return
	}
	if old == nil {
		t.Fatalf("read %s: %v", sweepFile, readErr)
	}
	for id, c := range got.Classes {
		known, ok := old.Classes[id]
		switch {
		case !ok:
			t.Errorf("a new class of differences (%d queries): %s\n  e.g. %q\n    Loki:  %s\n    proxy: %s", c.Count, id, c.Examples[0].Query, c.Examples[0].Loki, c.Examples[0].Proxy)
		case known.Case == "":
			t.Errorf("the class has no registry case: %s", id)
		case c.Count > known.Count:
			t.Errorf("the class grew from %d to %d queries: %s", known.Count, c.Count, id)
		case c.Count < known.Count:
			t.Errorf("the class shrank from %d to %d queries: %s (run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestGrammarSweep and commit %s)", known.Count, c.Count, id, strings.TrimPrefix(sweepFile, "../../"))
		}
	}
	for id, c := range old.Classes {
		if _, ok := got.Classes[id]; !ok {
			t.Errorf("a class of %d queries is gone, remove it from %s (run LVP_QUERY_PLAN_UPDATE=1): %s", c.Count, strings.TrimPrefix(sweepFile, "../../"), id)
		}
	}
}

func countRejected(loki map[string]sweepLokiRow) int {
	n := 0
	for _, r := range loki {
		if !r.Accept {
			n++
		}
	}
	return n
}

// sweepShort cuts a long text (a LogsQL query, an error) to 200 characters for the record.
func sweepShort(s string) string {
	if r := []rune(s); len(r) > 200 {
		return string(r[:200]) + "..."
	}
	return s
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// sweepBinaryFact writes a binary operation as Loki's tree holds it: the operator, bool, and the vector matching.
// Loki's tree holds an empty ignoring clause like no clause.
func sweepBinaryFact(x *queryplan.BinOp) string {
	b := x.Op
	if x.Bool {
		b += " bool"
	}
	if m := x.Match; m != nil && (m.Card != "" || len(m.MatchLabels) > 0 || m.GroupSide != "" || len(m.Include) > 0) {
		card := m.Card
		if card == "" {
			card = "ignoring"
		}
		if card == "ignoring" && len(m.MatchLabels) == 0 && m.GroupSide == "" {
			return b
		}
		b += fmt.Sprintf(" %s%v %s%v", card, nonNil(m.MatchLabels), m.GroupSide, nonNil(m.Include))
	}
	return b
}
