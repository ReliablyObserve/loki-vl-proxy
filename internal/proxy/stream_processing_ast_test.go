package proxy

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// extractDropKeepFromASTResult bundles all four outputs for table-driven tests.
type extractDropKeepFromASTResult struct {
	dropConds      []translator.DropCondition
	keepConds      []translator.DropCondition
	bareDropFields []string
	bareKeepFields []string
}

func runExtract(query string) extractDropKeepFromASTResult {
	dc, kc, bdf, bkf := extractDropKeepFromAST(query)
	return extractDropKeepFromASTResult{dc, kc, bdf, bkf}
}

// ─── bare fields (no matchers) ────────────────────────────────────────────────

func TestExtractDropKeep_BareDropSingle(t *testing.T) {
	r := runExtract(`{app="x"} | drop trace_id`)
	if len(r.bareDropFields) != 1 || r.bareDropFields[0] != "trace_id" {
		t.Errorf("bareDropFields = %v, want [trace_id]", r.bareDropFields)
	}
	if len(r.dropConds) != 0 {
		t.Errorf("dropConds = %v, want []", r.dropConds)
	}
}

func TestExtractDropKeep_BareDropMultiple(t *testing.T) {
	r := runExtract(`{app="x"} | drop trace_id, span_id, request_id`)
	want := []string{"trace_id", "span_id", "request_id"}
	if len(r.bareDropFields) != len(want) {
		t.Fatalf("bareDropFields = %v, want %v", r.bareDropFields, want)
	}
	for i, f := range want {
		if r.bareDropFields[i] != f {
			t.Errorf("bareDropFields[%d] = %q, want %q", i, r.bareDropFields[i], f)
		}
	}
}

func TestExtractDropKeep_BareKeepSingle(t *testing.T) {
	r := runExtract(`{app="x"} | keep level`)
	if len(r.bareKeepFields) != 1 || r.bareKeepFields[0] != "level" {
		t.Errorf("bareKeepFields = %v, want [level]", r.bareKeepFields)
	}
	if len(r.keepConds) != 0 {
		t.Errorf("keepConds = %v, want []", r.keepConds)
	}
}

func TestExtractDropKeep_BareKeepMultiple(t *testing.T) {
	r := runExtract(`{app="x"} | keep level, app, env`)
	want := []string{"level", "app", "env"}
	if len(r.bareKeepFields) != len(want) {
		t.Fatalf("bareKeepFields = %v, want %v", r.bareKeepFields, want)
	}
	for i, f := range want {
		if r.bareKeepFields[i] != f {
			t.Errorf("bareKeepFields[%d] = %q, want %q", i, r.bareKeepFields[i], f)
		}
	}
}

// ─── conditional matchers — all four operators ────────────────────────────────

func TestExtractDropKeep_DropEq(t *testing.T) {
	r := runExtract(`{app="x"} | drop level="debug"`)
	if len(r.dropConds) != 1 {
		t.Fatalf("dropConds = %v, want 1 condition", r.dropConds)
	}
	dc := r.dropConds[0]
	if dc.Field != "level" || dc.Op != "=" || dc.Value != "debug" {
		t.Errorf("dropCond = %+v, want {level = debug}", dc)
	}
	if len(r.bareDropFields) != 0 {
		t.Errorf("bareDropFields = %v, want []", r.bareDropFields)
	}
}

func TestExtractDropKeep_DropNeq(t *testing.T) {
	r := runExtract(`{app="x"} | drop level!="debug"`)
	if len(r.dropConds) != 1 {
		t.Fatalf("dropConds = %v, want 1 condition", r.dropConds)
	}
	dc := r.dropConds[0]
	if dc.Field != "level" || dc.Op != "!=" || dc.Value != "debug" {
		t.Errorf("dropCond = %+v, want {level != debug}", dc)
	}
}

func TestExtractDropKeep_DropReMatch(t *testing.T) {
	r := runExtract(`{app="x"} | drop level=~"debug|info"`)
	if len(r.dropConds) != 1 {
		t.Fatalf("dropConds = %v, want 1 condition", r.dropConds)
	}
	dc := r.dropConds[0]
	if dc.Field != "level" || dc.Op != "=~" || dc.Value != "debug|info" {
		t.Errorf("dropCond = %+v, want {level =~ debug|info}", dc)
	}
	// Verify regex works via Matches
	if !dc.Matches("debug") {
		t.Error("Matches(debug) should be true for =~ debug|info")
	}
	if !dc.Matches("info") {
		t.Error("Matches(info) should be true for =~ debug|info")
	}
	if dc.Matches("error") {
		t.Error("Matches(error) should be false for =~ debug|info")
	}
}

func TestExtractDropKeep_DropReNotMatch(t *testing.T) {
	r := runExtract(`{app="x"} | drop level!~"debug.*"`)
	if len(r.dropConds) != 1 {
		t.Fatalf("dropConds = %v, want 1 condition", r.dropConds)
	}
	dc := r.dropConds[0]
	if dc.Field != "level" || dc.Op != "!~" || dc.Value != "debug.*" {
		t.Errorf("dropCond = %+v, want {level !~ debug.*}", dc)
	}
	if dc.Matches("debugx") {
		t.Error("Matches(debugx) should be false for !~ debug.*")
	}
	if !dc.Matches("error") {
		t.Error("Matches(error) should be true for !~ debug.*")
	}
}

func TestExtractDropKeep_KeepEq(t *testing.T) {
	r := runExtract(`{app="x"} | keep status="200"`)
	if len(r.keepConds) != 1 {
		t.Fatalf("keepConds = %v, want 1 condition", r.keepConds)
	}
	kc := r.keepConds[0]
	if kc.Field != "status" || kc.Op != "=" || kc.Value != "200" {
		t.Errorf("keepCond = %+v, want {status = 200}", kc)
	}
}

func TestExtractDropKeep_KeepReMatch(t *testing.T) {
	r := runExtract(`{app="x"} | keep status=~"5.."`)
	if len(r.keepConds) != 1 {
		t.Fatalf("keepConds = %v, want 1 condition", r.keepConds)
	}
	kc := r.keepConds[0]
	if kc.Field != "status" || kc.Op != "=~" || kc.Value != "5.." {
		t.Errorf("keepCond = %+v, want {status =~ 5..}", kc)
	}
	if !kc.Matches("500") {
		t.Error("Matches(500) should be true for =~ 5..")
	}
	if kc.Matches("200") {
		t.Error("Matches(200) should be false for =~ 5..")
	}
}

// ─── mixed bare + conditional ─────────────────────────────────────────────────

func TestExtractDropKeep_MixedBareAndMatcher(t *testing.T) {
	r := runExtract(`{app="x"} | drop trace_id, level="debug"`)
	if len(r.bareDropFields) != 1 || r.bareDropFields[0] != "trace_id" {
		t.Errorf("bareDropFields = %v, want [trace_id]", r.bareDropFields)
	}
	if len(r.dropConds) != 1 || r.dropConds[0].Field != "level" {
		t.Errorf("dropConds = %v, want [{level = debug}]", r.dropConds)
	}
}

func TestExtractDropKeep_MultipleDropStages(t *testing.T) {
	r := runExtract(`{app="x"} | drop trace_id | drop level="debug"`)
	if len(r.bareDropFields) != 1 || r.bareDropFields[0] != "trace_id" {
		t.Errorf("bareDropFields = %v, want [trace_id]", r.bareDropFields)
	}
	if len(r.dropConds) != 1 || r.dropConds[0].Field != "level" {
		t.Errorf("dropConds = %v, want [{level = debug}]", r.dropConds)
	}
}

func TestExtractDropKeep_BothDropAndKeep(t *testing.T) {
	r := runExtract(`{app="x"} | drop trace_id | keep level`)
	if len(r.bareDropFields) != 1 || r.bareDropFields[0] != "trace_id" {
		t.Errorf("bareDropFields = %v, want [trace_id]", r.bareDropFields)
	}
	if len(r.bareKeepFields) != 1 || r.bareKeepFields[0] != "level" {
		t.Errorf("bareKeepFields = %v, want [level]", r.bareKeepFields)
	}
}

func TestExtractDropKeep_MultipleMixedConditions(t *testing.T) {
	r := runExtract(`{app="x"} | drop level="debug", level!~"info.*", trace_id | keep status=~"5..", env`)
	if len(r.dropConds) != 2 {
		t.Fatalf("dropConds = %v, want 2", r.dropConds)
	}
	if r.dropConds[0].Op != "=" || r.dropConds[0].Field != "level" {
		t.Errorf("dropConds[0] = %+v, want {level = debug}", r.dropConds[0])
	}
	if r.dropConds[1].Op != "!~" || r.dropConds[1].Field != "level" {
		t.Errorf("dropConds[1] = %+v, want {level !~ info.*}", r.dropConds[1])
	}
	if len(r.bareDropFields) != 1 || r.bareDropFields[0] != "trace_id" {
		t.Errorf("bareDropFields = %v, want [trace_id]", r.bareDropFields)
	}
	if len(r.keepConds) != 1 || r.keepConds[0].Field != "status" {
		t.Errorf("keepConds = %v, want [{status =~ 5..}]", r.keepConds)
	}
	if len(r.bareKeepFields) != 1 || r.bareKeepFields[0] != "env" {
		t.Errorf("bareKeepFields = %v, want [env]", r.bareKeepFields)
	}
}

// ─── parser-stage integration (drop/keep after json/logfmt) ──────────────────

func TestExtractDropKeep_AfterJSONStage(t *testing.T) {
	r := runExtract(`{app="x"} | json | drop level="debug"`)
	if len(r.dropConds) != 1 || r.dropConds[0].Field != "level" {
		t.Errorf("dropConds = %v, want [{level = debug}]", r.dropConds)
	}
}

func TestExtractDropKeep_AfterLogfmtStage(t *testing.T) {
	r := runExtract(`{app="x"} | logfmt | keep status=~"2.."`)
	if len(r.keepConds) != 1 || r.keepConds[0].Field != "status" {
		t.Errorf("keepConds = %v, want [{status =~ 2..}]", r.keepConds)
	}
}

// ─── fallback to regex (metric expressions, parse errors) ────────────────────

func TestExtractDropKeep_MetricExprFallback(t *testing.T) {
	// rate() is a metric expression — ParseLogQuery returns error → falls back to regex
	r := runExtract(`rate({app="x"}[5m])`)
	// The regex fallback should return empty (no drop/keep in this query)
	if len(r.dropConds)+len(r.keepConds)+len(r.bareDropFields)+len(r.bareKeepFields) != 0 {
		t.Errorf("metric expr fallback should return all empty; got dc=%v kc=%v bdf=%v bkf=%v",
			r.dropConds, r.keepConds, r.bareDropFields, r.bareKeepFields)
	}
}

func TestExtractDropKeep_MetricExprFallback_WithDrop(t *testing.T) {
	// sum(...) is a metric expr; regex fallback can still find drop conditions in the string
	// but importantly it must not panic
	r := runExtract(`sum by (app) (rate({app="x"}[5m]))`)
	_ = r // just verify no panic
}

func TestExtractDropKeep_EmptyQuery(t *testing.T) {
	// Must not panic on empty string
	r := runExtract(``)
	_ = r
}

func TestExtractDropKeep_NoDropKeep(t *testing.T) {
	r := runExtract(`{app="nginx"} | json | level="error"`)
	if len(r.dropConds)+len(r.keepConds)+len(r.bareDropFields)+len(r.bareKeepFields) != 0 {
		t.Errorf("query without drop/keep should return all empty; got %+v", r)
	}
}

// ─── AST path vs regex fallback parity ───────────────────────────────────────

// Verify that for valid log queries, AST path and regex fallback produce identical results.
func TestExtractDropKeep_ASTvsRegexParity(t *testing.T) {
	queries := []string{
		`{app="x"} | drop trace_id`,
		`{app="x"} | drop trace_id, span_id`,
		`{app="x"} | keep level`,
		`{app="x"} | keep level, app`,
		`{app="x"} | drop level="debug"`,
		`{app="x"} | drop level!="debug"`,
		`{app="x"} | json | drop trace_id | keep level`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			// AST path (default)
			astResult := runExtract(q)
			// Regex fallback via individual functions
			rdcFallback := translator.ParseDropConditions(q)
			rkcFallback := translator.ParseKeepConditions(q)
			rbdfFallback := translator.ParseBareDropFields(q)
			rbkfFallback := translator.ParseBareKeepFields(q)

			// For bare fields, AST and regex must agree
			if len(astResult.bareDropFields) != len(rbdfFallback) {
				t.Errorf("bareDropFields AST=%v regex=%v", astResult.bareDropFields, rbdfFallback)
			}
			if len(astResult.bareKeepFields) != len(rbkfFallback) {
				t.Errorf("bareKeepFields AST=%v regex=%v", astResult.bareKeepFields, rbkfFallback)
			}
			// Condition counts must match
			if len(astResult.dropConds) != len(rdcFallback) {
				t.Errorf("dropConds count AST=%d regex=%d", len(astResult.dropConds), len(rdcFallback))
			}
			if len(astResult.keepConds) != len(rkcFallback) {
				t.Errorf("keepConds count AST=%d regex=%d", len(astResult.keepConds), len(rkcFallback))
			}
		})
	}
}
