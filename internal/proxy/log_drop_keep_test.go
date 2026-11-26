package proxy

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// translatorDropKeep is the reading the log post-processing had before it took
// the query's typed stages only: the parse's drop and keep stages, or, for a
// text the parser rejects, the string translator's walk of the text.
func translatorDropKeep(query string) (drop, keep []translate.DropCondition, bareDrop, bareKeep []string) {
	lq, err := parseLogQLQuery(noFlowCtx, query)
	if err != nil {
		return translator.ParseDropConditions(query), translator.ParseKeepConditions(query),
			translator.ParseBareDropFields(query), translator.ParseBareKeepFields(query)
	}
	drop, bareDrop, rules := dropKeepFromPipeline(lq.Pipeline)
	for _, r := range rules {
		keep = append(keep, r.conds...)
		bareKeep = append(bareKeep, r.bare...)
	}
	return drop, keep, bareDrop, bareKeep
}

// dropKeepProbeValues are the values both readings' conditions are evaluated on.
var dropKeepProbeValues = []string{"", "debug", "info", "error", "x", "500", "a|b", "de", "debugx", "10.0.0.1"}

func sameDropConditions(t *testing.T, q, what string, got, want []translate.DropCondition) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %s %v, translator reading %v", q, what, got, want)
		return
	}
	for i := range got {
		if got[i].Field != want[i].Field || got[i].Op != want[i].Op || got[i].Value != want[i].Value {
			t.Errorf("%s: %s[%d] %+v, translator reading %+v", q, what, i, got[i], want[i])
			continue
		}
		for _, v := range append(dropKeepProbeValues, want[i].Value) {
			if got[i].Matches(v) != want[i].Matches(v) {
				t.Errorf("%s: %s[%d] on %q: %v, translator reading %v", q, what, i, v, got[i].Matches(v), want[i].Matches(v))
			}
		}
	}
}

func sameNames(t *testing.T, q, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %s %q, translator reading %q", q, what, got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s: %s %q, translator reading %q", q, what, got, want)
			return
		}
	}
}

// The log post-processing reads a query's drop and keep stages from its typed
// stages only (extractDropKeepFromAST): on every query the request validation
// accepts, written as the handlers read it, it gives what the reading with the
// string translator's text walk as fallback gave, so no answer changes. The
// queries where the two readings differ are ones the validation answers with
// Loki's error before a log response is read.
//
// conformance: architecture-syntax-package-layering
func TestLogDropKeepReadsLikeTheTranslator(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	qs = append(qs,
		`{app="x"} | drop level="debug", trace_id`,
		`{app="x"} | keep level=~"de.*", app`,
		`{app="x"} | drop level!~"a|b"`,
		`{app="x"} | drop level!="debug" | keep app, level`,
		`{app="x"} |= "| drop y" | drop z`,
		`{app="x"} != "keep" |~ "a" | keep b`,
		`{app="x"} | json | drop __error__, __error_details__`,
		`{app="x"} | logfmt | keep status="500" | drop a`,
		`{app="x"} | json | keep level=~"(?i)DEBUG"`,
		`{app="x"} | drop level=~"("`,
		`{app="x"} | drop status!=~"5.."`,
		`{app="x"} | drop ٣`,
		`{app="x"} | DROP level="debug"`,
		`{app="x"} | drop level="debug" # | keep app`,
		`sum by (app) (count_over_time({app="x"} | drop y [5m]))`,
		`rate({app="x"} | keep level [1m])`,
		``,
	)
	var accepted, nonEmpty, rejectedDiffer int
	for _, raw := range qs {
		q := RequestQueryText(raw)
		gotDrop, gotKeep, gotBareDrop, gotBareKeep := flatDropKeep(q)
		wantDrop, wantKeep, wantBareDrop, wantBareKeep := translatorDropKeep(q)
		if cachedQueryValidation(q).msg != "" || q == "" {
			if len(wantDrop)+len(wantKeep)+len(wantBareDrop)+len(wantBareKeep) != len(gotDrop)+len(gotKeep)+len(gotBareDrop)+len(gotBareKeep) {
				rejectedDiffer++
			}
			continue
		}
		accepted++
		if len(gotDrop)+len(gotKeep)+len(gotBareDrop)+len(gotBareKeep) > 0 {
			nonEmpty++
		}
		sameDropConditions(t, q, "drop conditions", gotDrop, wantDrop)
		sameDropConditions(t, q, "keep conditions", gotKeep, wantKeep)
		sameNames(t, q, "drop names", gotBareDrop, wantBareDrop)
		sameNames(t, q, "keep names", gotBareKeep, wantBareKeep)
	}
	if accepted < 2000 || nonEmpty < 25 {
		t.Fatalf("compared %d accepted queries, %d with drop or keep stages: the corpus lost its rows", accepted, nonEmpty)
	}
	t.Logf("%d accepted queries compared (%d with drop or keep stages); %d rejected ones read otherwise", accepted, nonEmpty, rejectedDiffer)
}

// translatorLineFieldExposure and translatorBackendLogQuery are lineFieldExposure and backendLogQuery reading a parse
// of their own, as they did before they read the query's memoised parse.
func translatorLineFieldExposure(p *Proxy, query string) *lineFieldExposure {
	if !p.lokiCompatibleProfile() {
		return nil
	}
	lq, err := parseLogQLQuery(noFlowCtx, query)
	if err != nil {
		return nil
	}
	exposure := pipelineAddsLabels(lq.Pipeline)
	p.withPlaceholderLine(&exposure, lq.Pipeline)
	return &exposure
}

func translatorBackendLogQuery(p *Proxy, query string) (string, bool) {
	if !strings.Contains(query, "line_format") || !p.lokiCompatibleProfile() {
		return query, false
	}
	lq, err := parseLogQLQuery(noFlowCtx, query)
	if err != nil {
		return query, false
	}
	formats := 0
	for _, stage := range lq.Pipeline {
		if _, ok := stage.(*logqlpkg.LineFormatStage); ok {
			formats++
		}
	}
	if formats == 0 {
		return query, false
	}
	if stripped, ok := stripUnreadLineFormat(query, lq.Pipeline); ok {
		return stripped, false
	}
	return query, true
}

// The line-field exposure and the line_format decision of a log query (lineFieldExposure, backendLogQuery) read the
// query's memoised parse: on every plan-corpus log query the validation accepts, and on long queries whose parse the
// validation keeps in the plan memo, in the Loki-compatible profile with and without
// -json-line-fields-as-line-keys, they equal what a parse of their own gave.
//
// conformance: architecture-syntax-package-layering
func TestLogLineFieldExposureReadsLikeItsOwnParse(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	pad := `|= "` + strings.Repeat("x", validationCacheMaxQueryBytes) + `"`
	qs = append(qs,
		`{app="x"} | logfmt | line_format "{{.msg}}" | drop caller`,
		`{app="x"} | json | line_format "{{.level}} {{.msg}}" |= "error"`,
		`{app="x"} | line_format "{{.a}}" | line_format "{{.b}}"`,
		`{app="x"} | json | label_format lvl=level | line_format "{{.lvl}}" | keep lvl`,
		`{app="x"} `+pad+` | logfmt | line_format "{{.msg}}" | drop caller`,
		`{app="x"} `+pad+` | json | line_format "{{.level}}" |= "error" | level=~"warn|error"`,
	)
	for _, keys := range []bool{false, true} {
		cfg := Config{BackendURL: "http://unused", Cache: cache.NewDisabled(), LogLevel: "error",
			LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, JSONLineFieldsAsLineKeys: keys}
		p, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !p.lokiCompatibleProfile() {
			t.Fatal("not the Loki-compatible profile")
		}
		var compared, formats, long int
		for _, raw := range qs {
			q := RequestQueryText(raw)
			if cachedQueryValidation(q).msg != "" {
				continue
			}
			if _, err := parseLogQLQuery(noFlowCtx, q); err != nil {
				continue
			}
			compared++
			if got, want := p.lineFieldExposure(q), translatorLineFieldExposure(p, q); !reflect.DeepEqual(got, want) {
				t.Errorf("keys=%v %.200s: exposure %+v, own parse %+v", keys, q, got, want)
			}
			gotQuery, gotKeep := p.backendLogQuery(q)
			wantQuery, wantKeep := translatorBackendLogQuery(p, q)
			if gotQuery != wantQuery || gotKeep != wantKeep {
				t.Errorf("keys=%v %.200s: backend query %.200q %v, own parse %.200q %v", keys, q, gotQuery, gotKeep, wantQuery, wantKeep)
			}
			if strings.Contains(q, "line_format") {
				formats++
			}
			if longQuery(q) {
				long++
			}
		}
		if compared < 100 || formats < 6 || long < 2 {
			t.Fatalf("keys=%v: compared %d log queries, %d with line_format, %d long: the corpus lost its rows", keys, compared, formats, long)
		}
		t.Logf("keys=%v: %d log queries compared, %d with line_format", keys, compared, formats)
	}
}
