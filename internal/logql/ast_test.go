package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestStreamSelector_String(t *testing.T) {
	tests := []struct {
		name string
		s    *logql.StreamSelector
		want string
	}{
		{
			name: "single eq",
			s:    &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "app", Op: logql.MatchEq, Value: "nginx"}}},
			want: `{app="nginx"}`,
		},
		{
			name: "neq",
			s:    &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "env", Op: logql.MatchNeq, Value: "prod"}}},
			want: `{env!="prod"}`,
		},
		{
			name: "regex",
			s:    &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "app", Op: logql.MatchRe, Value: "api.*"}}},
			want: `{app=~"api.*"}`,
		},
		{
			name: "not-regex",
			s:    &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "app", Op: logql.MatchNotRe, Value: "debug.*"}}},
			want: `{app!~"debug.*"}`,
		},
		{
			name: "multiple",
			s: &logql.StreamSelector{Matchers: []logql.LabelMatcher{
				{Name: "app", Op: logql.MatchEq, Value: "nginx"},
				{Name: "env", Op: logql.MatchNeq, Value: "prod"},
			}},
			want: `{app="nginx", env!="prod"}`,
		},
		{name: "empty", s: &logql.StreamSelector{}, want: `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.s.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPipelineStage_String(t *testing.T) {
	tests := []struct {
		name  string
		stage logql.Stage
		want  string
	}{
		{"contains", &logql.LineFilterStage{Op: logql.LineFilterContains, Value: "error"}, `|= "error"`},
		{"excludes", &logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "debug"}, `!= "debug"`},
		{"matchRe", &logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: "err.*"}, `|~ "err.*"`},
		{"excludeRe", &logql.LineFilterStage{Op: logql.LineFilterExcludeRe, Value: "ok.*"}, `!~ "ok.*"`},
		{"pattern", &logql.LineFilterStage{Op: logql.LineFilterContainsPat, Value: "GET <_> HTTP"}, `|> "GET <_> HTTP"`},
		{"notPattern", &logql.LineFilterStage{Op: logql.LineFilterExcludePat, Value: "GET <_> HTTP"}, `!> "GET <_> HTTP"`},
		{"json", &logql.ParserStage{Type: logql.ParserJSON}, "| json"},
		{"logfmt", &logql.ParserStage{Type: logql.ParserLogfmt}, "| logfmt"},
		{"regexp", &logql.ParserStage{Type: logql.ParserRegexp, Param: `(?P<lvl>\w+)`}, "| regexp `(?P<lvl>\\w+)`"},
		{"labelFilter", &logql.LabelFilterStage{Raw: `level="error"`}, `| level="error"`},
		{"drop", &logql.DropStage{Labels: []string{"trace_id", "span_id"}}, "| drop trace_id, span_id"},
		{"keep", &logql.KeepStage{Labels: []string{"app", "level"}}, "| keep app, level"},
		{"decolorize", &logql.DecolorizeStage{}, "| decolorize"},
		{"unwrap bare", &logql.UnwrapStage{Label: "duration"}, "| unwrap duration"},
		{"unwrap bytes", &logql.UnwrapStage{Label: "size", Converter: "bytes"}, "| unwrap bytes(size)"},
		{"lineFormat", &logql.LineFormatStage{Template: "{{.msg}}"}, `| line_format "{{.msg}}"`},
		{"labelFormat", &logql.LabelFormatStage{Raw: "newlabel=oldlabel"}, "| label_format newlabel=oldlabel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.stage.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLogQuery_String(t *testing.T) {
	q := &logql.LogQuery{
		Selector: &logql.StreamSelector{Matchers: []logql.LabelMatcher{
			{Name: "app", Op: logql.MatchEq, Value: "nginx"},
		}},
		Pipeline: []logql.Stage{
			&logql.LineFilterStage{Op: logql.LineFilterContains, Value: "error"},
			&logql.ParserStage{Type: logql.ParserJSON},
			&logql.LabelFilterStage{Raw: `level="error"`},
		},
	}
	want := `{app="nginx"} |= "error" | json | level="error"`
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestGrouping_String(t *testing.T) {
	tests := []struct {
		g    *logql.Grouping
		want string
	}{
		{&logql.Grouping{Labels: []string{"app", "env"}}, "by (app, env)"},
		{&logql.Grouping{Without: true, Labels: []string{"host"}}, "without (host)"},
		{&logql.Grouping{}, ""},
	}
	for _, tc := range tests {
		if got := tc.g.String(); got != tc.want {
			t.Errorf("Grouping(%v).String() = %q, want %q", tc.g, got, tc.want)
		}
	}
}

func TestRangeAggregation_String(t *testing.T) {
	sel := &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "app", Op: logql.MatchEq, Value: "api"}}}
	ra := &logql.RangeAggregation{
		Op:    logql.RangeRate,
		Inner: &logql.LogQuery{Selector: sel},
		Range: "5m",
	}
	want := `rate({app="api"}[5m])`
	if got := ra.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestVectorAggregation_String(t *testing.T) {
	sel := &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "app", Op: logql.MatchEq, Value: "api"}}}
	va := &logql.VectorAggregation{
		Op:       logql.VectorSum,
		Grouping: &logql.Grouping{Labels: []string{"app", "env"}},
		Inner: &logql.RangeAggregation{
			Op: logql.RangeRate, Inner: &logql.LogQuery{Selector: sel}, Range: "5m",
		},
	}
	want := `sum by (app, env) (rate({app="api"}[5m]))`
	if got := va.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
