package translator_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestParseKeepConditions(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []translator.DropCondition
	}{
		{
			name:  "empty_query_returns_nil",
			query: `{app="x"}`,
			want:  nil,
		},
		{
			name:  "bare_keep_returns_no_conditions",
			query: `{app="x"} | keep level`,
			want:  nil,
		},
		{
			name:  "single_matcher_keep",
			query: `{app="x"} | keep level="error"`,
			want: []translator.DropCondition{
				{Field: "level", Op: "=", Value: "error"},
			},
		},
		{
			name:  "mixed_bare_and_matcher_keeps",
			query: `{app="x"} | keep level="error", trace_id, status="500"`,
			want: []translator.DropCondition{
				{Field: "level", Op: "=", Value: "error"},
				{Field: "status", Op: "=", Value: "500"},
			},
		},
		{
			name:  "regex_matcher_keep",
			query: `{app="x"} | keep status=~"5.."`,
			want: []translator.DropCondition{
				{Field: "status", Op: "=~", Value: "5.."},
			},
		},
		{
			name:  "negated_matcher_keep",
			query: `{app="x"} | keep status!="200"`,
			want: []translator.DropCondition{
				{Field: "status", Op: "!=", Value: "200"},
			},
		},
		{
			name:  "drop_stage_is_not_keep",
			query: `{app="x"} | drop level="info"`,
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translator.ParseKeepConditions(tc.query)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d conditions, want %d: got=%+v want=%+v", len(got), len(tc.want), got, tc.want)
			}
			for i, w := range tc.want {
				if got[i].Field != w.Field || got[i].Op != w.Op || got[i].Value != w.Value {
					t.Errorf("[%d]: got {Field:%q Op:%q Value:%q}, want {Field:%q Op:%q Value:%q}",
						i, got[i].Field, got[i].Op, got[i].Value, w.Field, w.Op, w.Value)
				}
			}
		})
	}
}
