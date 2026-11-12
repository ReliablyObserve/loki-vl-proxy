package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

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

// The Loki differential of log queries: every plan-corpus row whose plan is a log query (a LogSelect root) that the
// plan lowers, run by real Loki and by real VictoriaLogs over the same data, through the proxy's real handler.
//
//   - Data: buildDiffFixture, a few streams and lines made for the query (plan_loki_vl_diff_fixture_test.go).
//   - Loki: the Loki oracle's process command (test/oracle/loki) for every line of every stream: whether the
//     selector selects the stream, whether the pipeline keeps the line, the line and the labels after the stages,
//     in Loki's categories (stream, structured metadata, parsed).
//   - VictoriaLogs: the store oracle (test/oracle/vl, v1.53.0 and v1.49.0) holds the same lines as the Loki push
//     API hands them over (stream labels, then structured metadata, then _msg; each name spelled as the profile's
//     label translator writes it upstream) and runs the plan's LogsQL and the string translator's LogsQL.
//   - Proxy: the rows VictoriaLogs returned go through a fresh proxy's query_range handler (proxy.New, the profile's
//     label style and metadata mode, structured metadata on), plain and with categorized labels, and its answer is
//     compared with Loki's line by line.
//
// The plan's text is lowered under the options the handler itself uses for the request: a capture run records the
// text the handler sends, and the query (as written, or without its line_format stage, which the handler may
// evaluate itself) and keepLine are those under which the plan's or the translator's text equals it. A row where no
// options reproduce the handler's text is not compared and is counted.
//
// Comparison rules, per selected line: the same lines kept; the same line text; the same labels, name and value. A
// plain answer holds every label on the stream; it is compared with Loki's labels (stream, structured metadata and
// parsed together). A categorized answer is compared category by category. detected_level is left out on both
// sides: Loki adds it at ingest (the process command has no ingest), the proxy derives it on read; a query that
// reads detected_level is not compared (its own grid tests hold it). A query the fixture cannot discriminate (Loki
// keeps every selected line or none) is compared but counted apart as a weak fixture.
//
// The oracle answers are recorded (testdata/plan-loki-vl-diff.jsonl.gz), so CI compares without building the
// oracle modules; the plan's and the translator's texts, the fixtures and the handler run every time, so a change
// to the lowering, the fixtures or the handler's shaping is seen at once: a text or fixture the answers were not
// recorded for fails until LVP_RUN_ORACLES=1 records them again (with LVP_LOKI_DIFF_CHECK=1 as well, the live
// oracles' answers are compared with the committed ones instead of written: CI's oracle step). The counts are the loki_differential section of
// conformance/registry/query-plan.json (LVP_QUERY_PLAN_UPDATE=1 rewrites it): plan_matches_loki may only rise, and
// every query where the plan differs from Loki is listed with its registry case in
// testdata/loki-differential-known.json.

const (
	diffAnswersPath = "testdata/plan-loki-vl-diff.jsonl.gz"
	diffKnownPath   = "testdata/loki-differential-known.json"
)

// diffProfile is a proxy deployment the differential runs: the VictoriaLogs version (and the store oracle's
// modfile for it), and the label style and metadata mode of the proxy. The profiles are the proxy's defaults (the
// Loki-compatible profile: underscores and translated metadata) on the oldest and the newest VictoriaLogs the proxy
// supports; a passthrough or native-metadata deployment exposes VictoriaLogs' own names by design.
type diffProfile struct {
	name    string
	version string
	modfile string
	style   proxy.LabelStyle
	mode    proxy.MetadataFieldMode
}

func diffProfiles() []diffProfile {
	return []diffProfile{
		{"v1.53 loki profile", "v1.53.0", "", proxy.LabelStyleUnderscores, proxy.MetadataFieldModeTranslated},
		{"v1.49 loki profile", "v1.49.0", "go.v1.49.0.mod", proxy.LabelStyleUnderscores, proxy.MetadataFieldModeTranslated},
	}
}

// toVL is the profile's upstream spelling of a Loki label name.
func (p diffProfile) toVL() func(string) string { return proxy.NewLabelTranslator(p.style, nil).ToVL }

// vlKey names the recorded VictoriaLogs answers of the profile: its version and its data set's spelling.
func (p diffProfile) vlKey() string { return p.version + "/" + string(p.style) }

func (p diffProfile) caps() logsql.Capabilities { return logsql.CapabilitiesFor(p.version) }

// diffQuery is one corpus row of the differential.
type diffQuery struct {
	q    string
	id   string
	plan *queryplan.Plan
	sel  *queryplan.LogSelect
	fix  diffFixture
	// readsLevel is a query that reads detected_level, which Loki derives at ingest.
	readsLevel bool
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

// planOfText is the resolved plan of a canonical query.
func planOfText(canon string) (*queryplan.Plan, error) {
	expr, err := logql.Parse(canon)
	if err != nil {
		return nil, err
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil, err
	}
	queryplan.Resolve(p)
	return p, nil
}

// diffQueries are the plan-corpus rows whose plan is a log query the plan lowers under the first profile.
func diffQueries(t *testing.T) []diffQuery {
	t.Helper()
	prof := diffProfiles()[0]
	var out []diffQuery
	for _, q := range planCorpus(t) {
		canon := logql.CanonicalizeLogRanges(q)
		plan, err := planOfText(canon)
		if err != nil {
			continue
		}
		sel, ok := plan.Root.(*queryplan.LogSelect)
		if !ok {
			continue
		}
		if _, err := lower.PlanQuery(canon, plan, lower.Profile{Caps: prof.caps(), Label: prof.toVL()}); err != nil {
			continue
		}
		out = append(out, diffQuery{q: q, id: queryID("logql", q), plan: plan, sel: sel, fix: buildDiffFixture(sel), readsLevel: readsDetectedLevel(sel)})
	}
	return out
}

// readsDetectedLevel is whether a stage names detected_level.
func readsDetectedLevel(sel *queryplan.LogSelect) bool {
	for _, m := range sel.Matchers {
		if m.Name == "detected_level" {
			return true
		}
	}
	for _, st := range sel.Stages {
		switch s := st.(type) {
		case *queryplan.LabelFilter:
			for _, leaf := range labelFilterLeaves(s.Expr) {
				if leaf.Name == "detected_level" {
					return true
				}
			}
			if s.Expr == nil && strings.Contains(s.Raw, "detected_level") {
				return true
			}
		case *queryplan.Drop:
			if slicesContain(s.Labels, "detected_level") {
				return true
			}
			for _, m := range s.Matchers {
				if m.Name == "detected_level" {
					return true
				}
			}
		case *queryplan.Keep:
			if slicesContain(s.Labels, "detected_level") {
				return true
			}
			for _, m := range s.Matchers {
				if m.Name == "detected_level" {
					return true
				}
			}
		case *queryplan.LabelFormat:
			for _, f := range s.Formats {
				if f.Name == "detected_level" || strings.Contains(f.Value, "detected_level") {
					return true
				}
			}
		case *queryplan.LineFormat:
			if strings.Contains(s.Template, "detected_level") {
				return true
			}
		}
	}
	return false
}

func slicesContain(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// diffOptions are the options the handler translated a query with, found from the text it sent.
type diffOptions struct {
	// variant is the LogQL the handler translated: the query, or the query without its line_format stage.
	variant  string
	keepLine bool
	// plan and translator are the texts each writes under those options ("" when it cannot); planErr is the
	// lowering's error.
	plan, translator string
	planErr          error
	// served says which text the handler sent: plan, translator or both (the same text); sent is that text.
	served, sent string
}

// diffVariants are the LogQL texts the handler may translate for a query.
func diffVariants(dq diffQuery) []string {
	out := []string{logql.CanonicalizeLogRanges(dq.q)}
	formats := 0
	var kept []logql.Stage
	for _, st := range dq.sel.Stages {
		if _, ok := st.(*queryplan.LineFormat); ok {
			formats++
			continue
		}
		kept = append(kept, st)
	}
	if formats == 1 {
		out = append(out, (&logql.LogQuery{Selector: &logql.StreamSelector{Matchers: dq.sel.Matchers}, Pipeline: kept}).String())
	}
	return out
}

// sendable is a text as the handler sends it: a text that starts with a pipe gets "* " in front.
func sendable(text string) string {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "|") {
		return "* " + text
	}
	return text
}

func diffTranslate(variant string, prof diffProfile, keepLine bool) (string, error) {
	tr := translator.TranslateLogQLWithCapabilities
	if keepLine {
		tr = translator.TranslateLogQueryKeepingLine
	}
	text, err := tr(variant, prof.toVL(), nil, prof.caps())
	if err != nil {
		return "", err
	}
	if op, err := translator.DecodePhysical(text); err == nil {
		if v, ok := op.(physical.VLQuery); ok {
			text = v.Text
		}
	}
	return sendable(text), nil
}

func diffLower(variant string, prof diffProfile, keepLine bool) (string, error) {
	p, err := planOfText(variant)
	if err != nil {
		return "", err
	}
	op, err := lower.PlanQuery(variant, p, lower.Profile{Caps: prof.caps(), Label: prof.toVL(), KeepLine: keepLine})
	if err != nil {
		return "", err
	}
	return sendable(physical.Encode(op)), nil
}

// findDiffOptions are the options under which the plan's or the translator's text is the one the handler sent.
func findDiffOptions(dq diffQuery, prof diffProfile, sent string) (diffOptions, bool) {
	for _, v := range diffVariants(dq) {
		for _, keepLine := range []bool{false, true} {
			tr, _ := diffTranslate(v, prof, keepLine)
			pl, perr := diffLower(v, prof, keepLine)
			o := diffOptions{variant: v, keepLine: keepLine, plan: pl, translator: tr, planErr: perr}
			switch {
			case pl != "" && pl == sent && tr == sent:
				o.served = "both"
			case pl != "" && pl == sent:
				o.served = "plan"
			case tr != "" && tr == sent:
				o.served = "translator"
			default:
				continue
			}
			return o, true
		}
	}
	return diffOptions{}, false
}

// ---- recorded answers ----

// diffLokiEntry is Loki's answer for one line: selected (the selector selects its stream), kept, the line when the
// stages change it, and the labels after the stages by category.
type diffLokiEntry struct {
	Selected bool              `json:"s,omitempty"`
	Keep     bool              `json:"k,omitempty"`
	Line     *string           `json:"l,omitempty"`
	Stream   map[string]string `json:"st,omitempty"`
	Metadata map[string]string `json:"sm,omitempty"`
	Parsed   map[string]string `json:"p,omitempty"`
	// Labels are Loki's labels when they are not the union of the three categories.
	Labels map[string]string `json:"x,omitempty"`
}

// diffVLRow is one row VictoriaLogs returned: I is the row of the data set (its _time), -1 when the row has none;
// F the fields that differ from the stored row and D the stored fields it lacks.
type diffVLRow struct {
	I int               `json:"i"`
	F map[string]string `json:"f,omitempty"`
	D []string          `json:"d,omitempty"`
}

type diffVLAnswer struct {
	Error string      `json:"error,omitempty"`
	Rows  []diffVLRow `json:"rows,omitempty"`
}

// diffVLRecord is VictoriaLogs' data set of a query under one version and spelling: the rows as stored (each its
// name=value pairs) and the answer for each text, by its hash.
type diffVLRecord struct {
	Stored  [][]string              `json:"stored"`
	Answers map[string]diffVLAnswer `json:"answers"`
}

type diffRecord struct {
	Q        string                   `json:"q"`
	Fixture  string                   `json:"fixture"`
	LokiErr  string                   `json:"loki_error,omitempty"`
	Loki     []diffLokiEntry          `json:"loki,omitempty"`
	VL       map[string]*diffVLRecord `json:"vl"`
	Profiles map[string]string        `json:"profiles,omitempty"`
}

func readDiffAnswers(t *testing.T) map[string]*diffRecord {
	t.Helper()
	f, err := os.Open(diffAnswersPath)
	if err != nil {
		t.Fatalf("%s: %v: LVP_RUN_ORACLES=1 records it", diffAnswersPath, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*diffRecord{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r diffRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		out[r.Q] = &r
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeDiffAnswers(t *testing.T, recs []*diffRecord) {
	t.Helper()
	f, err := os.Create(diffAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	// The header carries no time, so the same answers write the same bytes.
	zw.ModTime = time.Time{}
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// storedRows are VictoriaLogs' rows of a data set as the handler receives them: each a field map, row i at its index.
func (r *diffVLRecord) storedRows() []map[string]string {
	out := make([]map[string]string, len(r.Stored))
	for i, pairs := range r.Stored {
		m := map[string]string{}
		for _, p := range pairs {
			k, v, _ := strings.Cut(p, "=")
			m[k] = v
		}
		out[i] = m
	}
	return out
}

// rows are the rows of an answer, rebuilt from the stored rows.
func (r *diffVLRecord) rows(a diffVLAnswer) []map[string]string {
	stored := r.storedRows()
	out := make([]map[string]string, 0, len(a.Rows))
	for _, row := range a.Rows {
		m := map[string]string{}
		if row.I >= 0 && row.I < len(stored) {
			for k, v := range stored[row.I] {
				m[k] = v
			}
		}
		for _, k := range row.D {
			delete(m, k)
		}
		for k, v := range row.F {
			m[k] = v
		}
		out = append(out, m)
	}
	return out
}

// ---- comparison ----

// diffSide is one text's run against Loki for a query under a profile.
type diffSide struct {
	// vlError is VictoriaLogs' rejection of the text; status the handler's status when it is not 200.
	vlError string
	status  int
	// plain and categorized are the differences of each answer, by class, with one example each.
	plain, categorized map[string]string
}

func (s diffSide) matches() bool {
	return s.vlError == "" && s.status == 0 && len(s.plain) == 0 && len(s.categorized) == 0
}

// classes are the difference classes of the side, sorted.
func (s diffSide) classes() []string {
	set := map[string]bool{}
	if s.vlError != "" {
		set["VictoriaLogs rejects the text"] = true
	}
	if s.status != 0 {
		set["the handler answers status "+strconv.Itoa(s.status)] = true
	}
	for c := range s.plain {
		set[c] = true
	}
	for c := range s.categorized {
		set["categorized: "+c] = true
	}
	var out []string
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// labelClass names the kind of a label for the difference classes: Loki's error labels, an _extracted name, or the
// category Loki puts it in.
func labelClass(name string, want diffLokiEntry) string {
	switch {
	case name == "__error__" || name == "__error_details__":
		return name
	case strings.HasSuffix(name, "_extracted"):
		return "an _extracted label"
	}
	if _, ok := want.Stream[name]; ok {
		return "a stream label"
	}
	if _, ok := want.Metadata[name]; ok {
		return "a structured metadata label"
	}
	if _, ok := want.Parsed[name]; ok {
		return "a parsed label"
	}
	return "a label Loki does not have"
}

// lokiLabels are Loki's labels of an entry without detected_level.
func (e diffLokiEntry) labels() map[string]string {
	out := map[string]string{}
	if e.Labels != nil {
		for k, v := range e.Labels {
			out[k] = v
		}
	} else {
		for _, m := range []map[string]string{e.Stream, e.Metadata, e.Parsed} {
			for k, v := range m {
				out[k] = v
			}
		}
	}
	delete(out, "detected_level")
	return out
}

func withoutLevel(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if k != "detected_level" {
			out[k] = v
		}
	}
	return out
}

// compareLabels adds the differences between got and want (one category) to diffs.
func compareLabels(diffs map[string]string, prefix string, got, want map[string]string, entry diffLokiEntry, example string) {
	for k, w := range want {
		g, ok := got[k]
		switch {
		case !ok && w == "":
			// Loki's pipeline holds a label with an empty value (a json key with ""), kept apart: whether Loki's HTTP
			// answer writes it is not something the process command shows.
			addDiff(diffs, prefix+"misses "+labelClass(k, entry)+" with an empty value", fmt.Sprintf("%s: %s=%q missing", example, k, w))
		case !ok:
			addDiff(diffs, prefix+"misses "+labelClass(k, entry), fmt.Sprintf("%s: %s=%q missing", example, k, w))
		case g != w:
			addDiff(diffs, prefix+"writes another value of "+labelClass(k, entry), fmt.Sprintf("%s: %s=%q, Loki %q", example, k, g, w))
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			addDiff(diffs, prefix+"adds "+labelClass(k, entry), fmt.Sprintf("%s: %s=%q extra", example, k, g))
		}
	}
}

func addDiff(diffs map[string]string, class, example string) {
	if _, ok := diffs[class]; !ok {
		diffs[class] = example
	}
}

// compareRun holds the handler's answer to Loki's, line by line.
func compareRun(dq diffQuery, loki []diffLokiEntry, run diffHandlerRun, categorized bool) map[string]string {
	diffs := map[string]string{}
	if run.unmapped > 0 {
		addDiff(diffs, "answers a line of no stored row", fmt.Sprintf("%d lines", run.unmapped))
	}
	if run.repeated > 0 {
		addDiff(diffs, "answers a line twice", fmt.Sprintf("%d lines", run.repeated))
	}
	for i, want := range loki {
		in := dq.fix.Entries[i]
		got, ok := run.entries[i]
		keep := want.Selected && want.Keep
		example := fmt.Sprintf("line %d %q", i, in.Line)
		switch {
		case keep && !ok:
			addDiff(diffs, "drops a line Loki keeps", example)
			continue
		case !keep && ok:
			addDiff(diffs, "keeps a line Loki drops", example)
			continue
		case !keep:
			continue
		}
		wantLine := in.Line
		if want.Line != nil {
			wantLine = *want.Line
		}
		if got.line != wantLine {
			addDiff(diffs, "writes another line", fmt.Sprintf("%s: %q, Loki %q", example, got.line, wantLine))
		}
		if !categorized {
			compareLabels(diffs, "", withoutLevel(got.stream), want.labels(), want, example)
			continue
		}
		compareLabels(diffs, "stream: ", withoutLevel(got.stream), withoutLevel(want.Stream), want, example)
		compareLabels(diffs, "structured metadata: ", withoutLevel(got.metadata), withoutLevel(want.Metadata), want, example)
		compareLabels(diffs, "parsed: ", withoutLevel(got.parsed), withoutLevel(want.Parsed), want, example)
	}
	for i := range run.entries {
		if i >= len(loki) {
			addDiff(diffs, "answers a line of no stored row", fmt.Sprintf("row %d", i))
		}
	}
	return diffs
}

// runDiffSide runs one text's recorded VictoriaLogs answer through the handler, plain and categorized.
func runDiffSide(t *testing.T, dq diffQuery, prof diffProfile, rec *diffVLRecord, text string, loki []diffLokiEntry) (diffSide, bool) {
	a, ok := rec.Answers[shortHash(text)]
	if !ok {
		return diffSide{}, false
	}
	if a.Error != "" {
		return diffSide{vlError: a.Error}, true
	}
	rows := rec.rows(a)
	var side diffSide
	for _, categorized := range []bool{false, true} {
		run := runDiffHandler(t, prof, dq.q, rows, categorized)
		if run.status != 200 {
			side.status = run.status
			return side, true
		}
		if categorized {
			side.categorized = compareRun(dq, loki, run, true)
		} else {
			side.plain = compareRun(dq, loki, run, false)
		}
	}
	return side, true
}

// ---- the section ----

// diffProfileCounts are the counts of one profile.
type diffProfileCounts struct {
	// Compared are the queries both sides ran; NotComparable the others, by reason.
	Compared      int            `json:"compared"`
	NotComparable map[string]int `json:"not_comparable"`
	// PlanMatchesLoki are the compared queries where the plan's text, through the handler, answers every line as
	// Loki does, plain and categorized; Plain and Categorized count each answer alone.
	PlanMatchesLoki            int `json:"plan_matches_loki"`
	PlanMatchesLokiPlain       int `json:"plan_matches_loki_plain"`
	PlanMatchesLokiCategorized int `json:"plan_matches_loki_categorized"`
	// TranslatorMatchesLoki is the same for the string translator's text (reported).
	TranslatorMatchesLoki int `json:"translator_matches_loki"`
	VLRejectsPlan         int `json:"vl_rejects_plan"`
	VLRejectsTranslator   int `json:"vl_rejects_translator"`
	// WeakFixture are compared queries where Loki keeps no selected line, or every one of a pipeline that filters.
	WeakFixture int `json:"weak_fixture"`
	// PlanDiffersByClass counts, by difference class, the queries where the plan differs from Loki.
	PlanDiffersByClass       map[string]int `json:"plan_differs_by_class"`
	TranslatorDiffersByClass map[string]int `json:"translator_differs_by_class"`
	// HandlerServes counts which text the handler sent: the plan's, the translator's, or both (the same text).
	HandlerServes map[string]int `json:"handler_serves"`
}

type diffSection struct {
	Queries  int                           `json:"queries"`
	Lines    int                           `json:"lines"`
	Profiles map[string]*diffProfileCounts `json:"profiles"`
}

// diffExample is one query of a difference class, for the report.
type diffExample struct {
	Profile, Query, Class, Example string
}

// TestPlanMatchesLokiOnLogQueries runs the differential over the recorded answers (LVP_RUN_ORACLES=1 records them
// first) and holds its counts to the loki_differential section of query-plan.json.
//
// conformance: architecture-syntax-package-layering, semantics/label-filter-after-regexp-parser-misses-metadata, semantics/numeric-label-filter-not-equal-missing-label, semantics/json-key-named-error-label, semantics/metadata-answered-as-parsed-after-extraction-list, semantics/otel-name-label-filter-after-parser
func TestPlanMatchesLokiOnLogQueries(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	queries := diffQueries(t)
	if len(queries) < 100 {
		t.Fatalf("%d log queries in the plan corpus, expected the curated sources and the capture sample", len(queries))
	}
	options := captureDiffOptions(t, queries)
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordDiffAnswers(t, queries, options)
	}
	answers := readDiffAnswers(t)
	known := readDiffKnown(t)

	sec := diffSection{Queries: len(queries), Profiles: map[string]*diffProfileCounts{}}
	var stale []string
	var examples []diffExample
	planDiffers := map[string]map[string]bool{} // query id -> classes
	for _, prof := range diffProfiles() {
		sec.Profiles[prof.name] = &diffProfileCounts{NotComparable: map[string]int{}, PlanDiffersByClass: map[string]int{},
			TranslatorDiffersByClass: map[string]int{}, HandlerServes: map[string]int{}}
	}
	for qi, dq := range queries {
		rec := answers[dq.q]
		if rec == nil || rec.Fixture != dq.fix.hash() {
			stale = append(stale, "fixture of "+dq.q)
			continue
		}
		sec.Lines += len(dq.fix.Entries)
		for _, prof := range diffProfiles() {
			c := sec.Profiles[prof.name]
			reason, o := diffNotComparable(dq, rec, options[qi][prof.name])
			if reason != "" {
				c.NotComparable[reason]++
				ex := rec.LokiErr
				if oo := options[qi][prof.name]; oo != nil && oo.served == "" {
					ex = "sent " + oo.sent
				}
				examples = append(examples, diffExample{prof.name, dq.q, "not compared: " + reason, ex})
				continue
			}
			vl := rec.VL[prof.vlKey()]
			if vl == nil {
				stale = append(stale, prof.vlKey()+" of "+dq.q)
				continue
			}
			plan, ok := runDiffSide(t, dq, prof, vl, o.plan, rec.Loki)
			if !ok {
				stale = append(stale, prof.name+" plan text of "+dq.q)
				continue
			}
			tr := plan
			if o.translator != o.plan {
				if o.translator == "" {
					tr = diffSide{vlError: "the translator does not translate it"}
				} else if tr, ok = runDiffSide(t, dq, prof, vl, o.translator, rec.Loki); !ok {
					stale = append(stale, prof.name+" translator text of "+dq.q)
					continue
				}
			}
			c.Compared++
			c.HandlerServes[o.served]++
			if weakDiffFixture(dq.sel, rec.Loki) {
				c.WeakFixture++
				examples = append(examples, diffExample{prof.name, dq.q, "weak fixture", ""})
			}
			if plan.vlError != "" {
				c.VLRejectsPlan++
			}
			if tr.vlError != "" {
				c.VLRejectsTranslator++
			}
			if len(plan.plain) == 0 && plan.vlError == "" && plan.status == 0 {
				c.PlanMatchesLokiPlain++
			}
			if len(plan.categorized) == 0 && plan.vlError == "" && plan.status == 0 {
				c.PlanMatchesLokiCategorized++
			}
			if plan.matches() {
				c.PlanMatchesLoki++
			} else {
				if planDiffers[dq.id] == nil {
					planDiffers[dq.id] = map[string]bool{}
				}
				for _, cl := range plan.classes() {
					c.PlanDiffersByClass[cl]++
					planDiffers[dq.id][cl] = true
					ex := plan.plain[cl]
					if ex == "" {
						ex = plan.categorized[strings.TrimPrefix(cl, "categorized: ")]
					}
					if ex == "" {
						ex = plan.vlError
					}
					examples = append(examples, diffExample{prof.name, dq.q, cl, ex})
				}
			}
			if tr.matches() {
				c.TranslatorMatchesLoki++
			} else {
				for _, cl := range tr.classes() {
					c.TranslatorDiffersByClass[cl]++
				}
			}
		}
	}
	if len(stale) > 0 {
		t.Fatalf("%d recorded answers are stale for the current corpus, fixtures or texts (first: %s): record them again with "+
			"LVP_RUN_ORACLES=1 go test ./internal/querycorpus -run '^TestPlanMatchesLokiOnLogQueries$' (a module cache whose "+
			"VictoriaLogs checksum matches, see test/oraclerun) and commit %s", len(stale), stale[0], diffAnswersPath)
	}
	if len(answers) != len(queries) {
		t.Errorf("%d recorded answers for %d queries: record them again", len(answers), len(queries))
	}
	reportDiff(t, sec, examples)
	checkDiffKnown(t, known, planDiffers)
	checkDiffSection(t, sec)
}

// diffNotComparable is why a query is not compared under a profile ("" when it is), and the handler's options.
func diffNotComparable(dq diffQuery, rec *diffRecord, o *diffOptions) (string, diffOptions) {
	switch {
	case rec.LokiErr != "":
		return "Loki rejects the query", diffOptions{}
	case dq.readsLevel:
		return "reads detected_level, which Loki derives at ingest", diffOptions{}
	case len(dq.fix.Entries) == 0:
		return "the fixture has no selected stream", diffOptions{}
	case o == nil:
		return "the handler sends no query to VictoriaLogs", diffOptions{}
	case o.served == "":
		return "the handler's options are not reproduced", diffOptions{}
	case o.plan == "":
		return "the plan does not lower it under the handler's options", diffOptions{}
	}
	return "", *o
}

// weakDiffFixture is whether Loki keeps no selected line, or every one of a pipeline that filters: the data then
// cannot tell a filter that works from one that does nothing.
func weakDiffFixture(sel *queryplan.LogSelect, loki []diffLokiEntry) bool {
	kept, selected := 0, 0
	for _, e := range loki {
		if e.Selected {
			selected++
			if e.Keep {
				kept++
			}
		}
	}
	if kept == 0 {
		return true
	}
	if kept < selected {
		return false
	}
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.LineFilter, *queryplan.LabelFilter:
			return true
		}
	}
	return false
}

// captureDiffOptions runs each query once through the handler of each profile with no data and finds the options
// it translated with (nil when it sent no query: the handler rejected it).
func captureDiffOptions(t *testing.T, queries []diffQuery) []map[string]*diffOptions {
	t.Helper()
	out := make([]map[string]*diffOptions, len(queries))
	for i, dq := range queries {
		out[i] = map[string]*diffOptions{}
		if len(dq.fix.Entries) == 0 {
			continue
		}
		for _, prof := range diffProfiles() {
			run := runDiffHandler(t, prof, dq.q, nil, false)
			sent := diffSentText(run.sent)
			if sent == "" {
				continue
			}
			o, ok := findDiffOptions(dq, prof, sent)
			if !ok {
				o = diffOptions{}
			}
			o.sent = sent
			out[i][prof.name] = &o
		}
	}
	return out
}

// ---- recording ----

// diffStoreRow is a row of the store oracle's input (test/oracle/vl store).
type diffStoreRow struct {
	Stream [][2]string `json:"stream"`
	Fields [][2]string `json:"fields"`
}

type diffStoreInput struct {
	ID     string         `json:"id"`
	Rows   []diffStoreRow `json:"rows,omitempty"`
	Query  string         `json:"q,omitempty"`
	Tenant uint32         `json:"tenant"`
	Time   bool           `json:"time"`
}

// storeRows are the fixture's lines as the Loki push API hands them to VictoriaLogs, names in the profile's
// upstream spelling.
func storeRows(f diffFixture, toVL func(string) string) []diffStoreRow {
	rows := make([]diffStoreRow, 0, len(f.Entries))
	for _, e := range f.Entries {
		var r diffStoreRow
		for _, k := range sortedKeys(f.Streams[e.Stream]) {
			r.Stream = append(r.Stream, [2]string{toVL(k), f.Streams[e.Stream][k]})
		}
		for _, k := range sortedKeys(e.Metadata) {
			r.Fields = append(r.Fields, [2]string{toVL(k), e.Metadata[k]})
		}
		r.Fields = append(r.Fields, [2]string{"_msg", e.Line})
		rows = append(rows, r)
	}
	return rows
}

func recordDiffAnswers(t *testing.T, queries []diffQuery, options []map[string]*diffOptions) {
	t.Helper()
	root := filepath.Join("..", "..")
	recs := make([]*diffRecord, len(queries))
	var cases []oraclerun.ProcessCase
	for i, dq := range queries {
		recs[i] = &diffRecord{Q: dq.q, Fixture: dq.fix.hash(), VL: map[string]*diffVLRecord{}, Profiles: map[string]string{}}
		for _, e := range dq.fix.Entries {
			cases = append(cases, oraclerun.ProcessCase{Query: dq.q, Labels: dq.fix.Streams[e.Stream], Line: e.Line, Metadata: e.Metadata, WithCategories: true})
		}
	}
	loki, err := oraclerun.LokiProcess(filepath.Join(root, "test", "oracle", "loki"), cases)
	if err != nil {
		t.Fatal(err)
	}
	k := 0
	for i, dq := range queries {
		for j := range dq.fix.Entries {
			a := loki[k]
			k++
			if a.Error != "" {
				if recs[i].LokiErr == "" {
					recs[i].LokiErr = a.Error
				}
				continue
			}
			e := diffLokiEntry{Selected: a.Selected, Keep: a.Keep}
			if a.Keep {
				if a.Line != dq.fix.Entries[j].Line {
					line := a.Line
					e.Line = &line
				}
				e.Stream, e.Metadata, e.Parsed = a.Stream, a.StructuredMetadata, a.Parsed
				union := map[string]string{}
				for _, m := range []map[string]string{a.Stream, a.StructuredMetadata, a.Parsed} {
					for k, v := range m {
						union[k] = v
					}
				}
				if !reflect.DeepEqual(union, a.Labels) && (len(union) > 0 || len(a.Labels) > 0) {
					e.Labels = a.Labels
				}
			}
			recs[i].Loki = append(recs[i].Loki, e)
		}
		if recs[i].LokiErr != "" {
			recs[i].Loki = nil
		}
	}
	// One store run per VictoriaLogs version, every data set of it under a tenant of its own.
	byVersion := map[string][]diffProfile{}
	var versions []string
	for _, prof := range diffProfiles() {
		if _, ok := byVersion[prof.version]; !ok {
			versions = append(versions, prof.version)
		}
		byVersion[prof.version] = append(byVersion[prof.version], prof)
	}
	for _, version := range versions {
		recordDiffVersion(t, root, queries, options, recs, byVersion[version])
	}
	if os.Getenv("LVP_LOKI_DIFF_CHECK") == "1" {
		// CI's live check: the oracles must answer what is committed.
		committed := readDiffAnswers(t)
		changed := 0
		for _, r := range recs {
			// Compared as written: an empty list and a missing one are the same answer.
			got, _ := json.Marshal(r)
			want, _ := json.Marshal(committed[r.Q])
			if string(got) != string(want) {
				if changed++; changed <= 5 {
					t.Errorf("the oracles answer %s otherwise than %s records", r.Q, diffAnswersPath)
				}
			}
		}
		if changed > 0 || len(committed) != len(recs) {
			t.Fatalf("%d of %d queries answered otherwise (%d recorded): record them again with LVP_RUN_ORACLES=1", changed, len(recs), len(committed))
		}
		t.Logf("the oracles answer the %d queries as %s records", len(recs), diffAnswersPath)
		return
	}
	writeDiffAnswers(t, recs)
	t.Logf("recorded %s: %d queries, %d Loki answers", diffAnswersPath, len(recs), len(loki))
}

// recordDiffVersion records VictoriaLogs' answers for the profiles of one version.
func recordDiffVersion(t *testing.T, root string, queries []diffQuery, options []map[string]*diffOptions, recs []*diffRecord, profs []diffProfile) {
	t.Helper()
	spellings := map[string]diffProfile{}
	var keys []string
	for _, p := range profs {
		if _, ok := spellings[p.vlKey()]; !ok {
			keys = append(keys, p.vlKey())
		}
		spellings[p.vlKey()] = p
	}
	var inputs []diffStoreInput
	type ref struct {
		qi   int
		key  string
		hash string
	}
	refs := map[string]ref{}
	for qi, dq := range queries {
		if len(dq.fix.Entries) == 0 {
			continue
		}
		for ki, key := range keys {
			tenant := uint32(qi*4 + ki + 1)
			id := fmt.Sprintf("rows/%d/%d", qi, ki)
			inputs = append(inputs, diffStoreInput{ID: id, Rows: storeRows(dq.fix, spellings[key].toVL()), Tenant: tenant, Time: true})
			refs[id] = ref{qi: qi, key: key}
			texts := map[string]bool{}
			for _, p := range profs {
				if p.vlKey() != key {
					continue
				}
				if o := options[qi][p.name]; o != nil && o.served != "" {
					for _, text := range []string{o.plan, o.translator} {
						if text != "" {
							texts[text] = true
						}
					}
				}
			}
			for text := range texts {
				qid := fmt.Sprintf("q/%d/%d/%s", qi, ki, shortHash(text))
				inputs = append(inputs, diffStoreInput{ID: qid, Query: text, Tenant: tenant, Time: true})
				refs[qid] = ref{qi: qi, key: key, hash: shortHash(text)}
			}
		}
	}
	verdicts := runDiffStore(t, filepath.Join(root, "test", "oracle", "vl"), profs[0].modfile, inputs)
	// The stored rows first, then the answers, which are kept as differences from them.
	for _, pass := range []bool{true, false} {
		for _, v := range verdicts {
			r, ok := refs[v.ID]
			if !ok || (r.hash == "") != pass {
				continue
			}
			rec := recs[r.qi].VL[r.key]
			if rec == nil {
				rec = &diffVLRecord{Answers: map[string]diffVLAnswer{}}
				recs[r.qi].VL[r.key] = rec
			}
			if pass {
				if v.Error != "" {
					t.Fatalf("%s: VictoriaLogs does not store the rows: %s", queries[r.qi].q, v.Error)
				}
				rec.Stored = make([][]string, len(queries[r.qi].fix.Entries))
				for _, row := range v.Rows {
					if i := diffRowIndex(row); i >= 0 && i < len(rec.Stored) {
						rec.Stored[i] = withoutEmptyFields(row)
					}
				}
				continue
			}
			rec.Answers[r.hash] = diffAnswerOf(v, rec)
		}
	}
}

// diffRowIndex is the row of the data set a returned row is (its _time), -1 when it has none.
func diffRowIndex(pairs []string) int {
	for _, p := range pairs {
		if v, ok := strings.CutPrefix(p, "_time="); ok {
			ts, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return -1
			}
			return int(ts.UnixNano() - diffStart.UnixNano())
		}
	}
	return -1
}

// diffAnswerOf keeps an answer as differences from the stored rows.
func diffAnswerOf(v diffStoreVerdict, rec *diffVLRecord) diffVLAnswer {
	if v.Error != "" {
		return diffVLAnswer{Error: v.Error}
	}
	a := diffVLAnswer{Rows: []diffVLRow{}}
	for _, pairs := range v.Rows {
		i := diffRowIndex(pairs)
		row := diffVLRow{I: i, F: map[string]string{}}
		stored := map[string]string{}
		if i >= 0 && i < len(rec.Stored) {
			for _, p := range rec.Stored[i] {
				k, val, _ := strings.Cut(p, "=")
				stored[k] = val
			}
		}
		seen := map[string]bool{}
		for _, p := range withoutEmptyFields(pairs) {
			k, val, _ := strings.Cut(p, "=")
			seen[k] = true
			if sv, ok := stored[k]; !ok || sv != val {
				row.F[k] = val
			}
		}
		for _, k := range sortedKeys(stored) {
			if !seen[k] {
				row.D = append(row.D, k)
			}
		}
		if len(row.F) == 0 {
			row.F = nil
		}
		a.Rows = append(a.Rows, row)
	}
	sort.SliceStable(a.Rows, func(i, j int) bool { return a.Rows[i].I < a.Rows[j].I })
	return a
}

// withoutEmptyFields drops the fields without a value: a block of rows holds every field of the block, and
// VictoriaLogs' HTTP answer leaves the empty ones out of each row (app/vlselect/logsql JSONRows).
func withoutEmptyFields(pairs []string) []string {
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if !strings.HasSuffix(p, "=") || strings.Count(p, "=") > 1 {
			out = append(out, p)
		}
	}
	return out
}

type diffStoreVerdict struct {
	ID    string     `json:"id"`
	Error string     `json:"error,omitempty"`
	Rows  [][]string `json:"rows,omitempty"`
}

// runDiffStore runs the store oracle over inputs.
func runDiffStore(t *testing.T, oracleDir, modfile string, inputs []diffStoreInput) []diffStoreVerdict {
	t.Helper()
	work := t.TempDir()
	bin, err := oraclerun.Build(oracleDir, modfile, work, "vl")
	if err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(work, "in.jsonl"), filepath.Join(work, "out.jsonl")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, x := range inputs {
		if err := enc.Encode(x); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(bin, "store", "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	r, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var verdicts []diffStoreVerdict
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var v diffStoreVerdict
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		verdicts = append(verdicts, v)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return verdicts
}

// ---- known differences, the section and the report ----

// diffKnown are the queries where the plan differs from Loki: per query id, its text and, per difference class, the
// registry case that holds it.
type diffKnown map[string]struct {
	Query   string            `json:"q"`
	Classes map[string]string `json:"classes"`
}

func readDiffKnown(t *testing.T) diffKnown {
	t.Helper()
	k := diffKnown{}
	b, err := os.ReadFile(diffKnownPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return k
		}
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &k); err != nil {
		t.Fatal(err)
	}
	return k
}

// checkDiffKnown holds the differences of the plan from Loki to the known list, both ways: a difference class of a
// query without a registry case fails, and so does a listed class the plan no longer shows.
func checkDiffKnown(t *testing.T, known diffKnown, differs map[string]map[string]bool) {
	t.Helper()
	for id, classes := range differs {
		for _, c := range sortedSet(classes) {
			if known[id].Classes[c] == "" {
				t.Errorf("the plan differs from Loki on %s: %q, and %s names no registry case for it", id, c, diffKnownPath)
			}
		}
	}
	for id, k := range known {
		for c, caseID := range k.Classes {
			if !differs[id][c] {
				t.Errorf("%s %q (%s) is listed as a difference, but the plan no longer shows it: drop it from %s", id, c, caseID, diffKnownPath)
			}
		}
	}
}

func sortedSet(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func checkDiffSection(t *testing.T, got diffSection) {
	t.Helper()
	file, readErr := readPlanFile()
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		writePlanSection(t, file, "loki_differential", got)
		return
	}
	if readErr != nil {
		t.Fatalf("read %s: %v", planPath, readErr)
	}
	var want diffSection
	if err := json.Unmarshal(file["loki_differential"], &want); err != nil {
		t.Fatalf("%s has no readable loki_differential section: %v", planPath, err)
	}
	for name, g := range got.Profiles {
		w := want.Profiles[name]
		if w == nil {
			t.Errorf("%s: no recorded counts", name)
			continue
		}
		if g.PlanMatchesLoki < w.PlanMatchesLoki {
			t.Errorf("%s: the plan matches Loki on %d queries, %d before", name, g.PlanMatchesLoki, w.PlanMatchesLoki)
		}
	}
	if !reflect.DeepEqual(got, want) {
		g, _ := json.MarshalIndent(got, "", "  ")
		t.Errorf("the loki_differential counts changed: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run '^TestPlanMatchesLokiOnLogQueries$' and commit %s\n%s",
			strings.TrimPrefix(planPath, "../../"), g)
	}
}

// reportDiff logs the headline per profile and the difference classes with examples; LVP_LOKI_DIFF_REPORT names a
// file to write them to as JSON.
func reportDiff(t *testing.T, sec diffSection, examples []diffExample) {
	t.Helper()
	for _, prof := range diffProfiles() {
		c := sec.Profiles[prof.name]
		pct := func(n int) string {
			if c.Compared == 0 {
				return "-"
			}
			return strconv.FormatFloat(100*float64(n)/float64(c.Compared), 'f', 1, 64) + "%"
		}
		t.Logf("%s: %d queries compared, plan matches Loki %d (%s; plain %d, categorized %d), translator %d (%s), VictoriaLogs rejects plan %d / translator %d, weak fixtures %d, not compared %v, served %v",
			prof.name, c.Compared, c.PlanMatchesLoki, pct(c.PlanMatchesLoki), c.PlanMatchesLokiPlain, c.PlanMatchesLokiCategorized,
			c.TranslatorMatchesLoki, pct(c.TranslatorMatchesLoki), c.VLRejectsPlan, c.VLRejectsTranslator, c.WeakFixture, c.NotComparable, c.HandlerServes)
		for _, cl := range sortedKeysInt(c.PlanDiffersByClass) {
			t.Logf("  plan differs: %s: %d", cl, c.PlanDiffersByClass[cl])
		}
	}
	if path := os.Getenv("LVP_LOKI_DIFF_REPORT"); path != "" {
		b, _ := json.MarshalIndent(map[string]any{"section": sec, "examples": examples}, "", "  ")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Error(err)
		}
	}
}

func sortedKeysInt(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
