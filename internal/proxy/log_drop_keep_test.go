package proxy

import (
	"path/filepath"
	"testing"

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
	return dropKeepFromPipeline(lq.Pipeline)
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
		gotDrop, gotKeep, gotBareDrop, gotBareKeep := extractDropKeepFromAST(q)
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
