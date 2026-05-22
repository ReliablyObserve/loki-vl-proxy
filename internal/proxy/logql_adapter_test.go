package proxy

import (
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestBinOpExprToVMInfo_NilVectorMatching(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{Op: "/"}
	got := binOpExprToVMInfo(expr)
	if got != nil {
		t.Errorf("expected nil for nil VectorMatching, got %+v", got)
	}
}

func TestBinOpExprToVMInfo_On(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "/",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "on",
			MatchLabels: []string{"app", "env"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if len(got.On) != 2 || got.On[0] != "app" || got.On[1] != "env" {
		t.Errorf("On = %v, want [app env]", got.On)
	}
	if got.Ignoring != nil {
		t.Errorf("Ignoring should be nil, got %v", got.Ignoring)
	}
}

func TestBinOpExprToVMInfo_Ignoring(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "*",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "ignoring",
			MatchLabels: []string{"host"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if len(got.Ignoring) != 1 || got.Ignoring[0] != "host" {
		t.Errorf("Ignoring = %v, want [host]", got.Ignoring)
	}
	if got.On != nil {
		t.Errorf("On should be nil, got %v", got.On)
	}
}

func TestBinOpExprToVMInfo_GroupLeft(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "*",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "on",
			MatchLabels: []string{"app"},
			GroupSide:   "group_left",
			Include:     []string{"team", "owner"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if len(got.GroupLeft) != 2 || got.GroupLeft[0] != "team" || got.GroupLeft[1] != "owner" {
		t.Errorf("GroupLeft = %v, want [team owner]", got.GroupLeft)
	}
	if got.GroupRight != nil {
		t.Errorf("GroupRight should be nil, got %v", got.GroupRight)
	}
}

func TestBinOpExprToVMInfo_GroupRight(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "+",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "on",
			MatchLabels: []string{"app"},
			GroupSide:   "group_right",
			Include:     []string{"region"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if len(got.GroupRight) != 1 || got.GroupRight[0] != "region" {
		t.Errorf("GroupRight = %v, want [region]", got.GroupRight)
	}
	if got.GroupLeft != nil {
		t.Errorf("GroupLeft should be nil, got %v", got.GroupLeft)
	}
}

func TestBinOpExprToVMInfo_GroupLeftEmptyInclude(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "*",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "on",
			MatchLabels: []string{"app"},
			GroupSide:   "group_left",
			Include:     []string{},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if got.GroupLeft == nil {
		t.Error("GroupLeft should be non-nil empty slice (not nil) when group_left is present")
	}
}

func TestBinOpExprToVMInfo_EmptyMatchLabels(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "/",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "on",
			MatchLabels: []string{},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if got.On == nil {
		t.Error("On should be non-nil empty slice (not nil) for on()")
	}
}

func TestBinOpExprToVMInfo_UnknownCard(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "/",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:        "unknown",
			MatchLabels: []string{"app"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if got.On != nil || got.Ignoring != nil {
		t.Errorf("unknown Card should produce nil On/Ignoring; got On=%v Ignoring=%v", got.On, got.Ignoring)
	}
}

func TestBinOpExprToVMInfo_UnknownGroupSide(t *testing.T) {
	expr := &logqlpkg.BinOpExpr{
		Op: "*",
		VectorMatching: &logqlpkg.VectorMatching{
			Card:      "on",
			GroupSide: "unknown_side",
			Include:   []string{"label"},
		},
	}
	got := binOpExprToVMInfo(expr)
	if got == nil {
		t.Fatal("expected non-nil VectorMatchInfo")
	}
	if got.GroupLeft != nil || got.GroupRight != nil {
		t.Errorf("unknown GroupSide should produce nil GroupLeft/GroupRight")
	}
}

// TestBinOpExprToVMInfo_RoundTripFromParser verifies the bridge works with AST produced by
// the actual LogQL parser — the most important integration path.
func TestBinOpExprToVMInfo_RoundTripFromParser(t *testing.T) {
	tests := []struct {
		query     string
		wantOn    []string
		wantIgn   []string
		wantGL    []string
		wantGR    []string
	}{
		{
			query:  `rate({app="api"}[5m]) / on (app, env) rate({app="api"}[5m])`,
			wantOn: []string{"app", "env"},
		},
		{
			query:  `rate({app="api"}[5m]) / ignoring (host) rate({app="api"}[5m])`,
			wantIgn: []string{"host"},
		},
		{
			query: `rate({app="api"}[5m]) * on (app) group_left (team) rate({app="api"}[5m])`,
			wantOn: []string{"app"},
			wantGL: []string{"team"},
		},
		{
			query: `rate({app="api"}[5m]) + on (app) group_right (region) rate({app="api"}[5m])`,
			wantOn: []string{"app"},
			wantGR: []string{"region"},
		},
		{
			// on() with empty labels
			query:  `rate({app="api"}[5m]) / on () rate({app="api"}[5m])`,
			wantOn: []string{},
		},
		{
			// group_left with no include labels
			query:  `rate({app="api"}[5m]) * on (app) group_right () rate({app="api"}[5m])`,
			wantOn: []string{"app"},
			wantGR: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := logqlpkg.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.query, err)
			}
			binOp, ok := expr.(*logqlpkg.BinOpExpr)
			if !ok {
				t.Fatalf("expected *BinOpExpr, got %T", expr)
			}
			vm := binOpExprToVMInfo(binOp)
			if vm == nil {
				t.Fatal("binOpExprToVMInfo returned nil")
			}
			checkLabels(t, "On", vm.On, tt.wantOn)
			checkLabels(t, "Ignoring", vm.Ignoring, tt.wantIgn)
			checkLabels(t, "GroupLeft", vm.GroupLeft, tt.wantGL)
			checkLabels(t, "GroupRight", vm.GroupRight, tt.wantGR)
		})
	}
}

func checkLabels(t *testing.T, field string, got, want []string) {
	t.Helper()
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if len(got) != len(want) {
		t.Errorf("%s: got %v, want %v", field, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d]: got %q, want %q", field, i, got[i], want[i])
		}
	}
}
