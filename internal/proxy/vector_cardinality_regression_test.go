package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestVectorMatchCardinalityPerEvaluation(t *testing.T) {
	body := func(groups ...[]int) []byte {
		result := make([]any, 0, len(groups))
		for i, times := range groups {
			values := make([]any, 0, len(times))
			for _, ts := range times {
				values = append(values, []any{ts, "1"})
			}
			result = append(result, map[string]any{"metric": map[string]string{"app": "a", "level": string(rune('a' + i))}, "values": values})
		}
		b, _ := json.Marshal(map[string]any{"data": map[string]any{"result": result}})
		return b
	}
	for _, tc := range []struct {
		name                             string
		left, right                      []byte
		groupLeft, groupRight, wantError bool
	}{
		{"one_to_one", body([]int{1, 2}), body([]int{1, 2}), false, false, false},
		{"many_to_one", body([]int{1}, []int{1}), body([]int{1}), false, false, true},
		{"one_to_many", body([]int{1}), body([]int{1}, []int{1}), false, false, true},
		{"many_to_many", body([]int{1}, []int{1}), body([]int{1}, []int{1}), false, false, true},
		{"nonoverlapping_left", body([]int{1}, []int{2}), body([]int{1, 2}), false, false, false},
		{"nonoverlapping_right", body([]int{1, 2}), body([]int{1}, []int{2}), false, false, false},
		{"unmatched_left_duplicates", body([]int{2}, []int{2}), body([]int{1}), false, false, false},
		{"unmatched_right_duplicates", body([]int{2}), body([]int{1}, []int{1}), false, false, true},
		{"explicit_group_left", body([]int{1}, []int{1}), body([]int{1}), true, false, false},
		{"group_left_duplicate_one_side", body([]int{1}), body([]int{1}, []int{1}), true, false, true},
		{"explicit_group_right", body([]int{1}), body([]int{1}, []int{1}), false, true, false},
		{"group_right_duplicate_one_side", body([]int{1}, []int{1}), body([]int{1}), false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVectorMatchCardinality(tc.left, tc.right, nil, []string{"level"}, tc.groupLeft, tc.groupRight)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, want error %v", err, tc.wantError)
			}
		})
	}
}

func TestEmptyVectorMatchingModifiersSurviveAdapters(t *testing.T) {
	for _, side := range []string{"group_left", "group_right"} {
		expr, err := logqlpkg.Parse(`sum by(app)(rate({app="a"}[5m])) / on() ` + side + `() sum(rate({app="a"}[5m]))`)
		if err != nil {
			t.Fatal(err)
		}
		vm := binOpExprToVMInfo(expr.(*logqlpkg.BinOpExpr))
		if !vm.MatchOn || vm.GroupSide != side {
			t.Fatalf("lost modifiers: %+v", vm)
		}
		_, _, _, vm, ok := translator.ParseBinaryMetricExprFull("__binary__:/:left|||right@@@on:@@@" + side + ":")
		if !ok || !vm.MatchOn || vm.GroupSide != side {
			t.Fatalf("lost marker modifiers: %+v", vm)
		}
	}
}

func TestVectorCardinalityUsesUnambiguousLabels(t *testing.T) {
	body := []byte(`{"data":{"result":[
		{"metric":{"a":"x,b=y","b":"z"},"value":[1,"2"]},
		{"metric":{"a":"x","b":"y,b=z"},"value":[1,"3"]}
	]}}`)
	if err := validateVectorMatchCardinality(body, body, nil, nil, false, false); err != nil {
		t.Fatalf("distinct label maps collided: %v", err)
	}
	if err := validateVectorMatchCardinality(body, body, []string{}, nil, false, false); err == nil {
		t.Fatal("explicit on() must collapse all labels and reject duplicate matches")
	}
}

func TestNestedVectorCardinalityErrorStatus(t *testing.T) {
	err := fmt.Errorf("left query: %w", vectorMatchError("many-to-one matching must be explicit"))
	if got := statusFromUpstreamErr(err); got != http.StatusInternalServerError {
		t.Fatalf("nested cardinality status = %d, want 500", got)
	}
}
