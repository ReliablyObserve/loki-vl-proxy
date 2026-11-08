// The plan corpus translates thousands of queries the corpus capture would record as test-sent, so its tests pause
// recording (querycorpus.PauseRecording): the capture run still runs them and records only what other tests send.

package querycorpus_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The corpus the plan tests run over: the curated rows every test reads from its own source (the A/B shapes, the
// semantics matrix and the registry cases), and a committed sample of the captured client queries.
const (
	shapesPath        = "../../bench/ab/shapes.json"
	semanticsPath     = "../../test/e2e-compat/query-semantics-matrix.json"
	registryCasesDir  = "../../conformance/registry/cases"
	captureSamplePath = "testdata/plan-capture-sample.json"
)

// captureSample is testdata/plan-capture-sample.json. The full captured corpus is a CI artifact, not a committed
// file, so the sample is what a local run has of it: about 2,000 client LogQL rows, stratified by the markers their
// translation carries and by their construct, so every shape is present. TestBuildPlanCaptureSample rebuilds it.
type captureSample struct {
	Description string `json:"description"`
	Rows        []struct {
		Query   string `json:"q"`
		Stratum string `json:"stratum"`
	} `json:"rows"`
}

// curatedQueries are the query texts of the shapes, the semantics matrix and the registry cases (`query` and `expr`
// parameters), without duplicates, in a fixed order.
func curatedQueries(t *testing.T) []string {
	t.Helper()
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
	readJSON(t, shapesPath, &shapes)
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
	readJSON(t, semanticsPath, &matrix)
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
	err := filepath.WalkDir(registryCasesDir, func(path string, d os.DirEntry, err error) error {
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
		t.Fatal(err)
	}
	out := make([]string, 0, len(seen))
	for q := range seen {
		out = append(out, q)
	}
	sort.Strings(out)
	return out
}

// planCorpus is the curated rows and the capture sample, without duplicates.
func planCorpus(t *testing.T) []string {
	t.Helper()
	var sample captureSample
	readJSON(t, captureSamplePath, &sample)
	seen := map[string]bool{}
	var out []string
	for _, q := range curatedQueries(t) {
		seen[q] = true
		out = append(out, q)
	}
	for _, r := range sample.Rows {
		if !seen[r.Query] {
			seen[r.Query] = true
			out = append(out, r.Query)
		}
	}
	return out
}

// planProfile is one way the proxy translates a query: the options that change what the translator writes.
type planProfile struct {
	name     string
	labelFn  translator.LabelTranslateFunc
	stream   map[string]bool
	caps     logsql.Capabilities
	keepLine bool
}

// planProfiles are the 12 option combinations that change the translator's output, measured on the corpus: the
// VictoriaLogs floor (v1.49) against the pin (v1.53), the label style, the stream fields, the Loki-compatible
// stream labels, the detected_level body scan and the keepLine of log queries.
func planProfiles() []planProfile {
	identity := func(s string) string { return s }
	underscores := proxy.NewLabelTranslator(proxy.LabelStyleUnderscores, nil).ToVL
	streamLabels := translator.WithStreamLabels(underscores, []string{"app", "namespace", "service_name", "env", "cluster", "pod"})
	fields := map[string]bool{"app": true, "namespace": true, "env": true, "service_name": true, "cluster": true, "job": true, "level": true, "container": true, "pod": true}
	v53, v49 := logsql.CapabilitiesFor("v1.53.0"), logsql.CapabilitiesFor("v1.49.0")
	noScan := v53
	noScan.NoDetectedLevelBodyScan = true
	return []planProfile{
		{"v1.53 identity", identity, nil, v53, false},
		{"v1.49 identity", identity, nil, v49, false},
		{"v1.53 underscores", underscores, nil, v53, false},
		{"v1.49 underscores", underscores, nil, v49, false},
		{"v1.53 underscores keepLine", underscores, nil, v53, true},
		{"v1.53 stream labels", streamLabels, nil, v53, false},
		{"v1.53 no body scan", underscores, nil, noScan, false},
		{"v1.53 stream fields", underscores, fields, v53, false},
		{"v1.49 stream fields", underscores, fields, v49, false},
		{"v1.53 stream fields and labels", streamLabels, fields, v53, false},
		{"v1.53 stream labels keepLine", streamLabels, nil, v53, true},
		{"v1.53 no body scan, stream fields, keepLine", underscores, fields, noScan, true},
	}
}

// translate is the LogsQL (with markers) the proxy would hold for a client query under the profile, and false when
// the query is not translated (a parse error or an unsupported function) or, with keepLine, is not a log query.
func (p planProfile) translate(q string) (string, bool) {
	q = logql.CanonicalizeLogRanges(q)
	expr, err := logql.Parse(q)
	if err != nil {
		return "", false
	}
	var out string
	if p.keepLine {
		if _, isLog := expr.(*logql.LogQuery); !isLog {
			return "", false
		}
		out, err = translator.TranslateLogQueryKeepingLine(q, p.labelFn, p.stream, p.caps)
	} else {
		out, err = translator.TranslateLogQLWithCapabilities(q, p.labelFn, p.stream, p.caps)
	}
	return out, err == nil
}

// physicalShapes names the markers of a decoded query, for the sample's strata and the test's report.
func physicalShapes(op physical.Op) []string {
	var shapes []string
	if v, ok := op.(physical.Verbatim); ok {
		shapes = append(shapes, "verbatim")
		op = v.In
	}
	core, post := physical.Split(op)
	if post.Folds() {
		shapes = append(shapes, "without")
		if post.Without.Op != "" {
			shapes = append(shapes, "without-op:"+post.Without.Op)
		}
	}
	switch n := len(post.LabelReplace); {
	case n == 1:
		shapes = append(shapes, "label_replace")
	case n > 1:
		shapes = append(shapes, "label_replace-chain")
	}
	switch c := core.(type) {
	case physical.Binary:
		shapes = append(shapes, "binary")
		if c.Match != nil {
			shapes = append(shapes, "binary-matching")
		}
		for _, side := range []physical.Op{c.L, c.R} {
			sideCore, sidePost := physical.Split(side)
			switch sideCore.(type) {
			case physical.ScalarOp:
				shapes = append(shapes, "binary-scalar")
			case physical.Binary:
				shapes = append(shapes, "binary-nested")
			}
			if sidePost.Folds() || len(sidePost.LabelReplace) > 0 {
				shapes = append(shapes, "binary-side-markers")
			}
		}
	case physical.ScalarOp:
		shapes = append(shapes, "scalar")
	}
	return shapes
}

// TestPhysicalCodecRoundTrip decodes the translation of every corpus row under every profile and checks that the
// codec is the exact inverse the proxy relies on: the encoded operators are the translated text, the clean query is
// what the proxy's two-step strip left, and the post-processing is what ParseWithoutMarker and
// ParseAllLabelReplaceMarkers return.
// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestPhysicalCodecRoundTrip(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus := planCorpus(t)
	profiles := planProfiles()
	if len(profiles) != 12 {
		t.Fatalf("%d profiles, want 12", len(profiles))
	}
	shapes := map[string]int{}
	cases, marked, verbatim := 0, 0, 0
	for _, q := range corpus {
		for _, prof := range profiles {
			s, ok := prof.translate(q)
			if !ok {
				continue
			}
			cases++
			op, err := translator.DecodePhysical(s)
			if err != nil {
				t.Fatalf("%s: DecodePhysical(%q): %v", prof.name, s, err)
			}
			if got := translator.EncodePhysical(op); got != s {
				t.Fatalf("%s: query %q\nencode(decode(s)) differs:\n got %q\nwant %q", prof.name, q, got, s)
			}
			if got := translator.EncodePhysical(rebuilt(op)); got != s {
				t.Fatalf("%s: query %q\nthe operands write %q, want %q", prof.name, q, got, s)
			}
			if _, v := op.(physical.Verbatim); v {
				verbatim++
				t.Errorf("%s: query %q\ntranslated text %q is not written back by structure", prof.name, q, s)
			}

			assertSplitPost(t, s)

			// The proxy's two-step strip, as it ran before the codec, on the text after the last quote, where the
			// translator writes its markers (referenceSplit).
			wantClean, wantOp, wantLabels, wantSpecs := referenceSplit(s)
			// The label_replace markers were also read from the text before the without marker left it.
			after := strings.LastIndexAny(s, "\"'`") + 1
			if _, onRaw := translator.ParseAllLabelReplaceMarkers(s[after:]); fmt.Sprintf("%#v", onRaw) != fmt.Sprintf("%#v", wantSpecs) {
				t.Fatalf("%s: query %q\nlabel_replace of the whole text differs: got %#v, want %#v", prof.name, q, onRaw, wantSpecs)
			}
			core, post := physical.Split(op)
			if got := translator.EncodePhysical(core); got != wantClean {
				t.Fatalf("%s: query %q\nclean query differs:\n got %q\nwant %q", prof.name, q, got, wantClean)
			}
			var gotOp string
			var gotLabels []string
			if post.Folds() {
				gotOp, gotLabels = post.Without.Op, post.Without.Labels
			}
			if gotOp != wantOp || fmt.Sprintf("%#v", gotLabels) != fmt.Sprintf("%#v", wantLabels) {
				t.Fatalf("%s: query %q\nwithout differs: got %q %#v, want %q %#v", prof.name, q, gotOp, gotLabels, wantOp, wantLabels)
			}
			if fmt.Sprintf("%#v", post.LabelReplace) != fmt.Sprintf("%#v", wantSpecs) {
				t.Fatalf("%s: query %q\nlabel_replace differs: got %#v, want %#v", prof.name, q, post.LabelReplace, wantSpecs)
			}
			if sh := physicalShapes(op); len(sh) > 0 {
				marked++
				for _, name := range sh {
					shapes[name]++
				}
			}
		}
	}
	if cases < 10000 {
		t.Fatalf("only %d translated rows over %d queries and %d profiles: the corpus did not load", cases, len(corpus), len(profiles))
	}
	// Every marker shape the translator writes is in the corpus: a codec that never saw one proves nothing about it.
	for _, name := range []string{"without", "without-op:sum", "label_replace", "label_replace-chain", "binary", "binary-matching", "binary-scalar"} {
		if shapes[name] == 0 {
			t.Errorf("no corpus row carries a %q marker", name)
		}
	}
	t.Logf("%d queries x %d profiles: %d translated rows, %d with markers, %d kept as text; shapes %v", len(corpus), len(profiles), cases, marked, verbatim, shapes)
}

// TestBuildPlanCaptureSample rebuilds testdata/plan-capture-sample.json from the query corpus, a CI artifact:
//
//	LVP_QUERY_CORPUS=corpus.jsonl.gz LVP_PLAN_CAPTURE_SAMPLE_UPDATE=1 go test ./internal/querycorpus -run TestBuildPlanCaptureSample
//
// Candidates are the valid LogQL client rows that no curated source has. Each is given a stratum: the markers its
// translation carries under the 12 profiles (stream labels change them) and its construct. A stratum keeps its rows
// in the order of their hash, up to a cap; the cap is the largest one that keeps the sample within 2,000 rows, so a
// rare shape is kept whole and a common one is thinned evenly.
func TestBuildPlanCaptureSample(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpusPath := os.Getenv("LVP_QUERY_CORPUS")
	if corpusPath == "" || os.Getenv("LVP_PLAN_CAPTURE_SAMPLE_UPDATE") != "1" {
		t.Skip("LVP_QUERY_CORPUS and LVP_PLAN_CAPTURE_SAMPLE_UPDATE=1 are not set")
	}
	curated := map[string]bool{}
	for _, q := range curatedQueries(t) {
		curated[q] = true
	}
	profiles := planProfiles()
	strata := map[string][]string{}
	for _, r := range allRows(t, corpusPath) {
		if r.Lang != "logql" || r.Path != "" || !r.Valid || curated[r.Query] {
			continue
		}
		set := map[string]bool{}
		for _, prof := range profiles {
			s, ok := prof.translate(r.Query)
			if !ok {
				continue
			}
			op, _ := translator.DecodePhysical(s)
			for _, name := range physicalShapes(op) {
				set[name] = true
			}
		}
		shapes := make([]string, 0, len(set))
		for name := range set {
			shapes = append(shapes, name)
		}
		sort.Strings(shapes)
		key := strings.Join(shapes, "+") + " | " + construct(r.Query)
		strata[key] = append(strata[key], r.Query)
	}
	hash := func(q string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(q))) }
	for _, rows := range strata {
		sort.Slice(rows, func(i, j int) bool { return hash(rows[i]) < hash(rows[j]) })
	}
	total := func(limit int) int {
		n := 0
		for _, rows := range strata {
			n += min(len(rows), limit)
		}
		return n
	}
	limit := 1
	for total(limit+1) <= 2000 {
		limit++
	}
	var out captureSample
	out.Description = fmt.Sprintf("About 2,000 valid LogQL client rows of the query corpus (a CI artifact), stratified by the markers their translation carries under the 12 profiles of planProfiles and by construct (%d strata, at most %d rows each, in hash order). Rebuild: LVP_QUERY_CORPUS=corpus.jsonl.gz LVP_PLAN_CAPTURE_SAMPLE_UPDATE=1 go test ./internal/querycorpus -run TestBuildPlanCaptureSample", len(strata), limit)
	for key, rows := range strata {
		for _, q := range rows[:min(len(rows), limit)] {
			out.Rows = append(out.Rows, struct {
				Query   string `json:"q"`
				Stratum string `json:"stratum"`
			}{q, key})
		}
	}
	sort.Slice(out.Rows, func(i, j int) bool { return out.Rows[i].Query < out.Rows[j].Query })
	b, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(captureSamplePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(captureSamplePath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s: %d rows from %d strata, cap %d", captureSamplePath, len(out.Rows), len(strata), limit)
}

// construct names the outermost node of a LogQL query: its kind and, for an aggregation, its function.
func construct(q string) string {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return "invalid"
	}
	switch e := expr.(type) {
	case *logql.LogQuery:
		return "log"
	case *logql.RangeAggregation:
		return "range:" + string(e.Op)
	case *logql.VectorAggregation:
		return "vector:" + string(e.Op)
	case *logql.BinOpExpr:
		return "binop"
	case *logql.LiteralExpr:
		return "literal"
	}
	return fmt.Sprintf("%T", expr)
}
