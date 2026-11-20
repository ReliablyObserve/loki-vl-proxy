package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// conformance: semantics/label-filter-numeric-error-label
func TestNumericParserHintsStayRequestLocal(t *testing.T) {
	p := newProfileProxy(t, "http://unused", LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	p.queryPlanLowering = true
	for _, grouped := range []bool{false, true, false} {
		query := `count_over_time({app="a"} | json | n=200 [1m])`
		if grouped {
			query = "sum(" + query + ")"
		}
		expr, ok := sharedLogQLExpr(query)
		if !ok {
			t.Fatal("query must parse")
		}
		check := &unwrapCheck{probes: unwrapErrorProbes(expr)}
		ctx := context.WithValue(t.Context(), unwrapCheckKey{}, check)
		translated, err := p.translateLogResponseQuery(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		dropsSource := strings.Contains(translated, "delete __error__, __error_details__, __preserve_error__")
		if dropsSource != grouped {
			t.Fatalf("grouped=%v: source-error hints not applied: %s", grouped, translated)
		}
		if !strings.Contains(translated, `"LabelFilterErr" as __error__`) {
			t.Fatal("source hints must retain generated conversion errors")
		}
	}
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericOperandBindingSurvivesWindowPhase(t *testing.T) {
	left := unwrapErrorProbe{numeric: true, numericSource: "left", window: 5 * time.Second}
	right := unwrapErrorProbe{numeric: true, numericSource: "right", window: 5 * time.Second}
	base := `app:="a" | unpack_json | filter n:*`
	check := &unwrapCheck{probes: []unwrapErrorProbe{left, right}, numericBases: map[string][]unwrapErrorProbe{base: {right}}}
	ctx := context.WithValue(t.Context(), unwrapCheckKey{}, check)
	derived := withWindowPhaseFilter(ctx, base, time.Unix(1700000000, 0), time.Minute, 5*time.Second)
	probes := check.numericProbesFor(derived)
	if len(probes) != 1 || probes[0].numericSource != "right" || check.numericFailure != nil {
		t.Fatalf("window rewrite lost its operand: %+v, failure=%v", probes, check.numericFailure)
	}
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericOperandBindingKeepsDistinctParserHints(t *testing.T) {
	bare := `count_over_time({app="a"} | json | n=200 [1m])`
	grouped := "sum(" + bare + ")"
	expr, ok := sharedLogQLExpr(grouped + " + (" + bare + ")")
	if !ok {
		t.Fatal("query must parse")
	}
	check := &unwrapCheck{probes: unwrapErrorProbes(expr)}
	physical := `app:="a" | unpack_json | format if (n:="abc") "LabelFilterErr" as __error__ | stats count()`
	for _, query := range []string{bare, grouped} {
		translated := check.numericQuery(query, physical)
		base := strings.Split(translated, " | stats ")[0]
		bound := check.numericProbesFor(base)
		if len(bound) != 1 || (bound[0].hints != nil) != (query == grouped) {
			t.Fatalf("operand %s bound to wrong parser hints: %+v", query, bound)
		}
	}
	// A marker for the whole binary is handled operand-by-operand, not parsed
	// as one pipeline (including when there is only one non-scalar operand).
	if got := check.numericQuery(grouped+" + ("+bare+")", physical); got != physical || check.numericFailure != nil {
		t.Fatalf("binary marker was rewritten: %s; %v", got, check.numericFailure)
	}
	if got := check.numericProbesFor("unrelated"); len(got) != 0 || check.numericFailure == nil {
		t.Fatal("an unbound error must fail closed")
	}
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericErrorMetadataRetainsConversionDetails(t *testing.T) {
	line := `{"n":"abc","__preserve_error__":"true"}`
	query := `{app="a"} | json | n=200`
	p := newProfileProxy(t, "http://unused", LabelStyleUnderscores, MetadataFieldModeTranslated, false)
	expr, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	// The replay is held to the recorded Loki pipeline in TestNumericLabelFilterErrorStageOrder.
	want, _, known := unwrapLokiLabels(unwrapErrorProbe{numeric: true, pipeline: expr.Pipeline}, map[string]string{"app": "a"}, line, line)
	if !known || want[errorDetailsLabel] == "" {
		t.Fatal("conversion replay must include details")
	}
	entry := map[string]interface{}{"_msg": line, "n": "abc", "__preserve_error__": "true", errorLabel: "LabelFilterErr"}
	stream := map[string]string{"app": "a"}
	_, parsed := p.classifyEntryMetadataFields(entry, stream, stream, stream, p.lineFieldExposure(query), true, map[string][]metadataFieldExposure{}, map[string]string{}, map[string]string{})
	for _, key := range []string{errorLabel, errorDetailsLabel, "__preserve_error__", "n"} {
		if parsed[key] != want[key] {
			t.Errorf("%s: got %q, want %q", key, parsed[key], want[key])
		}
	}
}

// conformance: semantics/label-filter-numeric-equality, semantics/unwrap-conversion-error
func TestNumericLabelFilterErrorStageOrder(t *testing.T) {
	var cases []oraclerun.ProcessCase
	for _, tc := range []struct {
		name, tail, line string
	}{
		{"invalid-number", `n = 200`, `{"n":"abc"}`},
		{"empty-present", `n != 200`, `{"n":""}`},
		{"missing", `n != 200`, `{}`},
		{"filter-after", `n = 200 | __error__=""`, `{"n":"abc"}`},
		{"filter-before", `__error__="" | n = 200`, `{"n":"abc"}`},
		{"filter-before-keeps-error", `__error__="LabelFilterErr" | n = 200`, `{"n":"abc"}`},
		{"details-before", `__error_details__!="" | n = 200`, `{"n":"abc"}`},
		{"filter-before-string", `__error__="" | n="abc" | n = 200`, `{"n":"abc"}`},
		{"drop-after", `n = 200 | drop __error__, __error_details__`, `{"n":"abc"}`},
		{"valid", `n = 200`, `{"n":"2e2"}`},
		{"reserved-source-error", `n = 200`, `{"n":"200","__error__":"LabelFilterErr"}`},
		{"reserved-source-detail", `n = 200`, `{"n":"200","__error__":"LabelFilterErr","__error_details__":"source"}`},
		{"reserved-custom-error", `n = 200`, `{"n":"200","__error__":"custom_source_error"}`},
		{"source-filter-empty", `n = 200 | __error__=""`, `{"n":"200","__error__":"custom_source_error"}`},
		{"source-filter-nonempty", `n = 200 | __error__!=""`, `{"n":"200","__error__":"custom_source_error"}`},
		{"partial-json-clean-filter", `n = 200 | __error__=""`, `{"n":"200"`},
		{"partial-json-error-filter", `n = 200 | __error__!=""`, `{"n":"200"`},
		{"source-internal-looking-key", `n = 200 | __error__=""`, `{"n":"200","__lvp_numeric_error":"user-value"}`},
		{"source-preserve-true", `n = 200`, `{"n":"abc","__preserve_error__":"true"}`},
		{"source-preserve-custom", `n = 200`, `{"n":"abc","__preserve_error__":"custom"}`},
		{"overflow", `n = 200`, `{"n":"1e309"}`},
		{"duration", `n = 5s`, `{"n":"abc"}`},
		{"size", `n = 1KB`, `{"n":"1_000"}`},
	} {
		cases = append(cases, oraclerun.ProcessCase{ID: tc.name, Query: `{app="a"} | json | ` + tc.tail, Labels: map[string]string{"app": "a"}, Line: tc.line})
	}
	answers := lokiProcessAnswers(t, "numeric_label_filter_error", cases)
	// The two exact adjacent-filter shapes remain open. Keep executing them:
	// an unknown mismatch or a fixed case left in this table fails the test.
	// conformance: semantics/label-filter-numeric-error-label
	knownGaps := map[string]map[string]string{
		`{app="a"} | json | __error__="" | n = 200`:               answers[0].Labels,
		`{app="a"} | json | __error__="LabelFilterErr" | n = 200`: nil,
	}
	mismatches := 0
	for i, tc := range cases {
		t.Run(tc.ID, func(t *testing.T) {
			want := answers[i]
			if want.Error != "" {
				t.Fatal(want.Error)
			}
			query, err := logqlpkg.ParseLogQuery(tc.Query)
			if err != nil {
				t.Fatal(err)
			}
			labels, _, known := unwrapLokiLabels(unwrapErrorProbe{pipeline: query.Pipeline}, tc.Labels, tc.Line, tc.Line)
			if !known {
				t.Fatalf("replay does not evaluate %s", tc.Query)
			}
			differs := (labels != nil) != want.Keep || (labels != nil && unwrapLabelsText(labels) != unwrapLabelsText(want.Labels))
			knownLabels, gap := knownGaps[tc.Query]
			if differs {
				mismatches++
				if !gap {
					t.Errorf("unknown difference: labels=%v, Loki keep=%v labels=%v", labels, want.Keep, want.Labels)
				} else if (labels == nil) != (knownLabels == nil) || unwrapLabelsText(labels) != unwrapLabelsText(knownLabels) {
					t.Errorf("known difference changed: labels=%v, recorded deviation=%v", labels, knownLabels)
				} else {
					t.Logf("open semantics/label-filter-numeric-error-label: labels=%v, Loki keep=%v labels=%v", labels, want.Keep, want.Labels)
				}
			} else if gap {
				t.Errorf("stale known difference: %s now matches Loki", tc.Query)
			}
		})
	}
	t.Logf("bounded numeric-filter replay: %d/%d differences from recorded Loki", mismatches, len(cases))
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericFilterProofFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"backend-error", "unavailable", http.StatusServiceUnavailable},
		{"malformed-body", "not json", http.StatusOK},
		{"empty-body", "", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer backend.Close()
			p := newTestProxy(t, backend.URL)
			at := time.Unix(1700000000, 0)
			probe := unwrapErrorProbe{numeric: true, label: errorLabel, conv: numericFilterConversion, window: time.Minute}
			check := &unwrapCheck{probes: []unwrapErrorProbe{probe}, start: at, end: at, instant: true}
			if got := check.lookupHit(t.Context(), p, probe, unwrapHit{base: `*`, conv: numericFilterConversion}, at.Add(-time.Minute), at); got != nil {
				t.Fatalf("failed proof invented a Loki pipeline error: %v", got)
			}
			if check.numericFailure == nil {
				t.Fatal("a failed proof must not turn a flagged metric into a successful answer")
			}
			out := httptest.NewRecorder()
			writer := &unwrapLookupWriter{p: p, ctx: t.Context(), parent: t.Context(), check: check, out: out, header: http.Header{}}
			writer.WriteHeader(http.StatusOK)
			if out.Code != http.StatusInternalServerError || strings.Contains(out.Body.String(), tc.body) && tc.body != "" {
				t.Fatalf("proof failure must use the Loki error layer: %d %s", out.Code, out.Body.String())
			}
		})
	}
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericFilterStatsCounterAndRawSharing(t *testing.T) {
	query := `app:="a" | unpack_json | format if (n:="abc") "LabelFilterErr" as __error__ | stats count()`
	probe := unwrapErrorProbe{numeric: true, label: errorLabel, conv: numericFilterConversion}
	check := &unwrapCheck{probes: []unwrapErrorProbe{probe}}
	params, strip, ok := check.countingStatsQuery("/select/logsql/stats_query", url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700000060"}})
	if !ok || !strings.Contains(params.Get("query"), "by ("+unwrapBadField+")") {
		t.Fatal("numeric errors must be counted inside the existing stats read")
	}
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__lvp_bad":"0","app":"a"},"value":[1700000060,"3"]},{"metric":{"__lvp_bad":"1","app":"a"},"value":[1700000060,"1"]}]}}`)
	out, err := strip(body)
	if err != nil || strings.Contains(string(out), unwrapBadField) || len(check.hits) != 1 || !strings.Contains(string(out), `"3"`) {
		t.Fatalf("counter leaked or lost a clean group: %s hits=%v err=%v", out, check.hits, err)
	}
	if !check.sharedNumericRawRead("/select/logsql/query") || check.sharedNumericRawRead("/select/logsql/stats_query") {
		t.Fatal("only raw reads report their request-local hits outside the transport")
	}
	check.json = &jsonErrorCheck{}
	if check.sharedNumericRawRead("/select/logsql/query") {
		t.Fatal("a different error check must retain its own coalescing isolation")
	}
}

// conformance: semantics/label-filter-numeric-error-label
func TestNumericFilterRawHitsIgnoreWindowGaps(t *testing.T) {
	start := time.Unix(1700000040, 0)
	probe := unwrapErrorProbe{numeric: true, label: errorLabel, conv: numericFilterConversion, window: 5 * time.Second}
	check := &unwrapCheck{probes: []unwrapErrorProbe{probe}, start: start, end: start.Add(time.Minute), step: time.Minute}
	for i := 0; i < unwrapMaxRawHits+1; i++ {
		check.numericRowHit("base", start.Add(20*time.Second).UnixNano())
	}
	if check.rawHits != 0 || len(check.hits) != 0 {
		t.Fatal("rows outside every evaluated window consumed the error budget")
	}
	check.numericRowHit("base", start.Add(59*time.Second).UnixNano())
	if check.rawHits != 1 || len(check.hits) != 1 {
		t.Fatal("a visible retained error was lost")
	}
}
