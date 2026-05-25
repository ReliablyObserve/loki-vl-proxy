package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestParse_IncompleteUnwrapStub(t *testing.T) {
	cases := []struct {
		input         string
		wantLabel     string
		wantConverter string
	}{
		{
			input:         `{app="api-gateway"} | json | unwrap [1s]`,
			wantLabel:     "",
			wantConverter: "",
		},
		{
			input:         `{app="api-gateway"} | json | unwrap [5m]`,
			wantLabel:     "",
			wantConverter: "",
		},
		{
			input:         `{app="api-gateway"} | json | unwrap duration_seconds() [1s]`,
			wantLabel:     "",
			wantConverter: "duration_seconds",
		},
		{
			input:         `{app="api-gateway"} | json | unwrap bytes() [1s]`,
			wantLabel:     "",
			wantConverter: "bytes",
		},
		{
			input:         `{app="api-gateway"} | json | unwrap duration() [1s]`,
			wantLabel:     "",
			wantConverter: "duration",
		},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := logql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			lq, ok := expr.(*logql.LogQuery)
			if !ok {
				t.Fatalf("expected *LogQuery, got %T", expr)
			}
			var found *logql.UnwrapStage
			for _, s := range lq.Pipeline {
				if u, ok := s.(*logql.UnwrapStage); ok {
					found = u
					break
				}
			}
			if found == nil {
				t.Fatalf("no UnwrapStage in pipeline: %v", lq.Pipeline)
			}
			if found.Label != tc.wantLabel {
				t.Errorf("Label: got %q, want %q", found.Label, tc.wantLabel)
			}
			if found.Converter != tc.wantConverter {
				t.Errorf("Converter: got %q, want %q", found.Converter, tc.wantConverter)
			}
		})
	}
}

func TestStripIncompleteUnwrapStubs(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{
			input: `{app="api-gateway"} | json | unwrap [1s]`,
			want:  `{app="api-gateway"} | json`,
		},
		{
			input: `{app="api-gateway"} | json | unwrap bytes() [1s]`,
			want:  `{app="api-gateway"} | json`,
		},
		{
			input: `{app="api-gateway"} | json | unwrap duration_seconds() [1s]`,
			want:  `{app="api-gateway"} | json`,
		},
		{
			// Valid complete unwrap — should NOT be stripped
			input: `{app="api-gateway"} | json | unwrap duration_ms`,
			want:  `{app="api-gateway"} | json | unwrap duration_ms`,
		},
		{
			// No unwrap at all — unchanged
			input: `{app="api-gateway"} | json`,
			want:  `{app="api-gateway"} | json`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := logql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			lq, ok := expr.(*logql.LogQuery)
			if !ok {
				t.Fatalf("expected *LogQuery, got %T", expr)
			}
			result := logql.StripIncompleteUnwrapStubs(lq)
			if got := result.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
