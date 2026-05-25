package logql_test

// Comprehensive edge-case coverage for Parse, ParseAndValidate, and ValidateLogQL.
// Organised by feature so gaps are easy to spot.

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustParse(t *testing.T, input string) {
	t.Helper()
	if _, err := logql.Parse(input); err != nil {
		t.Errorf("Parse(%q) unexpected error: %v", input, err)
	}
}

func mustFail(t *testing.T, input string) {
	t.Helper()
	if _, err := logql.Parse(input); err == nil {
		t.Errorf("Parse(%q) expected error, got nil", input)
	}
}

func mustValidate(t *testing.T, input string) {
	t.Helper()
	if err := logql.ParseAndValidate(input); err != nil {
		t.Errorf("ParseAndValidate(%q) unexpected error: %v", input, err)
	}
}

func mustReject(t *testing.T, input string) {
	t.Helper()
	if err := logql.ParseAndValidate(input); err == nil {
		t.Errorf("ParseAndValidate(%q) expected error, got nil", input)
	}
}

func mustValidateV(t *testing.T, input string) {
	t.Helper()
	if msg := logql.ValidateLogQL(input); msg != "" {
		t.Errorf("ValidateLogQL(%q) unexpected error: %s", input, msg)
	}
}

func mustRejectV(t *testing.T, input string) {
	t.Helper()
	if msg := logql.ValidateLogQL(input); msg == "" {
		t.Errorf("ValidateLogQL(%q) expected error, got empty", input)
	}
}

// ─── stream selectors ─────────────────────────────────────────────────────────

func TestEdge_StreamSelector(t *testing.T) {
	valid := []string{
		`{app="nginx"}`,
		`{app!="prod"}`,
		`{app=~"api.*"}`,
		`{app!~"debug.*"}`,
		`{app="nginx", env!="prod"}`,
		`{app="nginx", env=~"prod|staging", region="us-east"}`,
		`{_app="nginx"}`,
		`{_private="yes"}`,
		`{app="nginx", env="prod", host="host-01"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx", __error__=""}`,
		`{app="nginx", __error__!=""}`,
		`{}`,          // parses but fails semantic
		`{app=~".*"}`, // wildcard regex, parses OK
		`{a="1", b="2", c="3", d="4", e="5"}`,
		`{app="nginx", level=~"error|warn|info|debug"}`,
		`{namespace="default", pod=~"api-.*", container="app"}`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_StreamSelector_ParseFail(t *testing.T) {
	bad := []string{
		``,
		`{app=`,
		`{app`,
		`{=nginx}`,
		`{app="nginx"`,
		`not logql`,
		`| json`,
	}
	for _, q := range bad {
		t.Run(q, func(t *testing.T) { mustFail(t, q) })
	}
}

// ─── line filters ─────────────────────────────────────────────────────────────

func TestEdge_LineFilters(t *testing.T) {
	valid := []string{
		`{app="nginx"} |= "error"`,
		`{app="nginx"} != "debug"`,
		`{app="nginx"} |~ "err.*"`,
		`{app="nginx"} !~ "ok.*"`,
		`{app="nginx"} |> "<_> status <_>"`,
		`{app="nginx"} !> "<_> ok <_>"`,
		`{app="nginx"} |= ""`,
		`{app="nginx"} |= "error" != "debug"`,
		`{app="nginx"} |= "error" |~ "5[0-9]{2}"`,
		`{app="nginx"} != "info" != "debug"`,
		`{app="nginx"} |= "GET" |= "200"`,
		`{app="nginx"} |~ "err.*" !~ "expected"`,
		`{app="nginx"} |= "level=error"`,
		`{app="nginx"} |= "status=500"`,
		`{app="nginx"} |~ "(ERROR|WARN|FATAL)"`,
		`{app="nginx"} != "healthcheck"`,
		`{app="nginx"} |= "panic:"`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_LineFilters_RawStrings(t *testing.T) {
	valid := []string{
		"{app=\"nginx\"} |~ `err.*`",
		"{app=\"nginx\"} !~ `ok.*`",
		"{app=\"nginx\"} |> `<_> status <_>`",
		"{app=\"nginx\"} |= `error`",
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── parser stages ────────────────────────────────────────────────────────────

func TestEdge_ParserStages(t *testing.T) {
	valid := []string{
		`{app="nginx"} | json`,
		`{app="nginx"} | logfmt`,
		"{app=\"nginx\"} | regexp `(?P<level>\\w+) (?P<msg>.*)`",
		"{app=\"nginx\"} | pattern `<level> <msg>`",
		`{app="nginx"} | unpack`,
		`{app="nginx"} | json | logfmt`,
		`{app="nginx"} | json | json`,
		`{app="nginx"} | logfmt | json`,
		`{app="nginx"} |= "error" | json`,
		`{app="nginx"} | json |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`{app="nginx"} | logfmt | level="warn"`,
		"{app=\"nginx\"} | regexp `(?P<ts>[^ ]+)`",
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── label filters ────────────────────────────────────────────────────────────

func TestEdge_LabelFilters(t *testing.T) {
	valid := []string{
		`{app="nginx"} | json | level="error"`,
		`{app="nginx"} | json | status=404`,
		`{app="nginx"} | json | status!=200`,
		`{app="nginx"} | json | status>=400`,
		`{app="nginx"} | json | status<=500`,
		`{app="nginx"} | json | status>300`,
		`{app="nginx"} | json | status<500`,
		`{app="nginx"} | json | level=~"err.*"`,
		`{app="nginx"} | json | level!~"debug|info"`,
		`{app="nginx"} | json | level="error", status>=500`,
		`{app="nginx"} | json | level!="debug"`,
		`{app="nginx"} | logfmt | duration>100ms`,
		`{app="nginx"} | logfmt | bytes>1024`,
		`{app="nginx"} | json | status==200`,
		`{app="nginx"} | json | status==0`,
		`{app="nginx"} | json | status>=500, level="error"`,
		`{app="nginx"} | json | level="error" | status>=500`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── drop / keep / decolorize ────────────────────────────────────────────────

func TestEdge_DropKeepDecolorize(t *testing.T) {
	valid := []string{
		`{app="nginx"} | drop trace_id`,
		`{app="nginx"} | drop trace_id, span_id`,
		`{app="nginx"} | drop trace_id, span_id, request_id`,
		`{app="nginx"} | keep level, app`,
		`{app="nginx"} | keep level`,
		`{app="nginx"} | keep app, level, env, host`,
		`{app="nginx"} | decolorize`,
		`{app="nginx"} | json | decolorize`,
		`{app="nginx"} | json | drop __error__, __error_details__`,
		`{app="nginx"} | drop _stream`,
		`{app="nginx"} | json | keep level | drop trace_id`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── conditional drop / keep matchers ────────────────────────────────────────

func TestEdge_DropConditional_Parse(t *testing.T) {
	valid := []string{
		`{app="nginx"} | drop level="debug"`,
		`{app="nginx"} | drop level!="debug"`,
		`{app="nginx"} | drop level=~"debug|info"`,
		`{app="nginx"} | drop level!~"debug.*"`,
		`{app="nginx"} | drop level="debug", trace_id`,
		`{app="nginx"} | drop trace_id, level="debug"`,
		`{app="nginx"} | drop level="debug", env!="prod"`,
		`{app="nginx"} | json | drop level="debug"`,
		`{app="nginx"} | keep status=~"5.."`,
		`{app="nginx"} | keep status!~"2.."`,
		`{app="nginx"} | keep status="200"`,
		`{app="nginx"} | keep status!="404"`,
		`{app="nginx"} | keep level=~"error|warn", app`,
		`{app="nginx"} | drop level="debug" | keep app`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_DropConditional_AST(t *testing.T) {
	tests := []struct {
		query        string
		wantLabels   []string
		wantMatchers []struct{ name, op, val string }
		keep         bool
	}{
		{
			query:      `{app="nginx"} | drop level="debug"`,
			wantLabels: nil,
			wantMatchers: []struct{ name, op, val string }{
				{"level", "=", "debug"},
			},
		},
		{
			query: `{app="nginx"} | drop level!="debug"`,
			wantMatchers: []struct{ name, op, val string }{
				{"level", "!=", "debug"},
			},
		},
		{
			query: `{app="nginx"} | drop level=~"debug|info"`,
			wantMatchers: []struct{ name, op, val string }{
				{"level", "=~", "debug|info"},
			},
		},
		{
			query: `{app="nginx"} | drop level!~"debug.*"`,
			wantMatchers: []struct{ name, op, val string }{
				{"level", "!~", "debug.*"},
			},
		},
		{
			query:        `{app="nginx"} | drop trace_id, level="debug"`,
			wantLabels:   []string{"trace_id"},
			wantMatchers: []struct{ name, op, val string }{{"level", "=", "debug"}},
		},
		{
			query:        `{app="nginx"} | keep status=~"5.."`,
			wantMatchers: []struct{ name, op, val string }{{"status", "=~", "5.."}},
			keep:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			lq, err := logql.ParseLogQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseLogQuery(%q) error: %v", tt.query, err)
			}
			var foundLabels []string
			var foundMatchers []struct{ name, op, val string }
			for _, stage := range lq.Pipeline {
				if ds, ok := stage.(*logql.DropStage); ok && !tt.keep {
					foundLabels = append(foundLabels, ds.Labels...)
					for _, m := range ds.Matchers {
						foundMatchers = append(foundMatchers, struct{ name, op, val string }{m.Name, m.Op, m.Value})
					}
				}
				if ks, ok := stage.(*logql.KeepStage); ok && tt.keep {
					foundLabels = append(foundLabels, ks.Labels...)
					for _, m := range ks.Matchers {
						foundMatchers = append(foundMatchers, struct{ name, op, val string }{m.Name, m.Op, m.Value})
					}
				}
			}
			if len(foundLabels) != len(tt.wantLabels) {
				t.Errorf("labels: got %v, want %v", foundLabels, tt.wantLabels)
			}
			for i, wm := range tt.wantMatchers {
				if i >= len(foundMatchers) {
					t.Fatalf("matcher[%d] missing; got %d matchers", i, len(foundMatchers))
				}
				m := foundMatchers[i]
				if m.name != wm.name || m.op != wm.op || m.val != wm.val {
					t.Errorf("matcher[%d]: got {%s %s %s}, want {%s %s %s}",
						i, m.name, m.op, m.val, wm.name, wm.op, wm.val)
				}
			}
		})
	}
}

func TestEdge_DropConditional_RoundTrip(t *testing.T) {
	// String() output must re-parse identically (round-trip property).
	queries := []string{
		`{app="nginx"} | drop level="debug"`,
		`{app="nginx"} | drop level!="debug"`,
		`{app="nginx"} | drop level=~"debug|info"`,
		`{app="nginx"} | drop level!~"debug.*"`,
		`{app="nginx"} | drop trace_id, level="debug"`,
		`{app="nginx"} | keep status=~"5.."`,
		`{app="nginx"} | keep status!~"2.."`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			expr, err := logql.Parse(q)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", q, err)
			}
			s := expr.String()
			expr2, err2 := logql.Parse(s)
			if err2 != nil {
				t.Fatalf("Parse(String(%q)) = %q error: %v", q, s, err2)
			}
			if expr.String() != expr2.String() {
				t.Errorf("round-trip mismatch:\n  original: %q\n  reparsed: %q", expr.String(), expr2.String())
			}
		})
	}
}

// ─── unwrap ───────────────────────────────────────────────────────────────────

func TestEdge_Unwrap(t *testing.T) {
	valid := []string{
		`{app="nginx"} | unwrap duration`,
		`{app="nginx"} | unwrap bytes(size)`,
		`{app="nginx"} | unwrap duration(latency)`,
		`{app="nginx"} | unwrap rate(throughput)`,
		`{app="nginx"} | json | unwrap duration`,
		`{app="nginx"} | logfmt | unwrap bytes(response_size)`,
		`{app="nginx"} | json | level="error" | unwrap latency`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── line_format / label_format ───────────────────────────────────────────────

func TestEdge_LineFormat(t *testing.T) {
	valid := []string{
		`{app="nginx"} | line_format "{{.msg}}"`,
		`{app="nginx"} | line_format "{{ .level }}: {{ .msg }}"`,
		`{app="nginx"} | line_format "level={{.level}}"`,
		`{app="nginx"} | line_format "{{.ts}} {{.level}} {{.msg}}"`,
		`{app="nginx"} | line_format ""`,
		`{app="nginx"} | json | line_format "{{.message}}"`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_LabelFormat(t *testing.T) {
	valid := []string{
		`{app="nginx"} | label_format newlabel=oldlabel`,
		`{app="nginx"} | label_format level=severity`,
		`{app="nginx"} | label_format app="fixed"`,
		`{app="nginx"} | label_format a=b, c=d`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── range aggregations ───────────────────────────────────────────────────────

func TestEdge_RangeOps(t *testing.T) {
	valid := []string{
		`rate({app="api"}[5m])`,
		`count_over_time({app="api"}[1h])`,
		`bytes_rate({app="api"}[5m])`,
		`bytes_over_time({app="api"}[5m])`,
		`avg_over_time({app="api"} | unwrap latency [5m])`,
		`sum_over_time({app="api"} | unwrap latency [5m])`,
		`min_over_time({app="api"} | unwrap latency [5m])`,
		`max_over_time({app="api"} | unwrap latency [5m])`,
		`stdvar_over_time({app="api"} | unwrap latency [5m])`,
		`stddev_over_time({app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.5, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.95, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(1, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(1.5, {app="api"} | unwrap latency [5m])`,
		`first_over_time({app="api"} | unwrap latency [5m])`,
		`last_over_time({app="api"} | unwrap latency [5m])`,
		`absent_over_time({app="api"}[5m])`,
		`rate_counter({app="api"} | unwrap latency [5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_RangeDurations(t *testing.T) {
	durations := []string{"30s", "1m", "5m", "15m", "30m", "1h", "6h", "12h", "24h", "7d", "30d"}
	for _, d := range durations {
		q := `rate({app="api"}[` + d + `])`
		t.Run(d, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_RangeWithOffset(t *testing.T) {
	valid := []string{
		`rate({app="api"}[5m] offset 1h)`,
		`count_over_time({app="api"}[1h] offset 24h)`,
		`rate({app="api"}[5m] offset 5m)`,
		`rate({app="api"}[1m] offset 30s)`,
		`sum_over_time({app="api"} | unwrap latency [5m] offset 1h)`,
		`avg_over_time({app="api"} | unwrap latency [1h] offset 7d)`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_Subqueries(t *testing.T) {
	valid := []string{
		`max_over_time(rate({app="api"}[5m])[1h:5m])`,
		`avg_over_time(rate({app="api"}[1m])[1h:5m])`,
		`min_over_time(rate({app="api"}[1m])[30m:1m])`,
		`sum_over_time(rate({app="api"}[1m])[1h:])`,
		`max_over_time(sum by (app) (rate({app="api"}[5m]))[1h:5m])`,
		`max_over_time(rate({app="api"}[5m])[24h:1h])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── vector aggregations ──────────────────────────────────────────────────────

func TestEdge_VectorOps(t *testing.T) {
	valid := []string{
		`sum(rate({app="api"}[5m]))`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`sum(rate({app="api"}[5m])) by (app)`,
		`sum without (host) (rate({app="api"}[5m]))`,
		`avg by (app) (rate({app="api"}[5m]))`,
		`min by (app) (rate({app="api"}[5m]))`,
		`max by (app) (rate({app="api"}[5m]))`,
		`count(rate({app="api"}[5m]))`,
		`count by (app) (rate({app="api"}[5m]))`,
		`stddev by (app) (rate({app="api"}[5m]))`,
		`stdvar by (app) (rate({app="api"}[5m]))`,
		`topk(5, rate({app="api"}[5m]))`,
		`bottomk(3, count_over_time({app="api"}[1h]))`,
		`sort(rate({app="api"}[5m]))`,
		`sort_desc(rate({app="api"}[5m]))`,
		`sum by (app, env) (rate({app="api"}[5m]))`,
		`sum by (app, env, region) (rate({app="api"}[5m]))`,
		`sum by () (rate({app="api"}[5m]))`,
		`max by (app) (count_over_time({app="api"}[1h]))`,
		`min by (app) (max_over_time({app="api"} | unwrap lat [5m]))`,
		`sum(rate({app="api"}[5m])) by (app, env)`,
		`avg(count_over_time({app="api"}[5m])) by (app)`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── binary operations ────────────────────────────────────────────────────────

func TestEdge_BinaryOps_Arithmetic(t *testing.T) {
	valid := []string{
		`rate({app="api"}[5m]) / rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) * 100`,
		`100 * rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) + rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) - rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) % 2`,
		`rate({app="api"}[5m]) ^ 2`,
		`sum(rate({app="api"}[5m])) / sum(rate({app="api"}[5m]))`,
		`100 * sum(rate({app="api", status=~"5.."}[5m])) / sum(rate({app="api"}[5m]))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_BinaryOps_Comparison(t *testing.T) {
	valid := []string{
		`count_over_time({app="api"}[5m]) > 100`,
		`count_over_time({app="api"}[5m]) >= 100`,
		`count_over_time({app="api"}[5m]) < 100`,
		`count_over_time({app="api"}[5m]) <= 100`,
		`count_over_time({app="api"}[5m]) == 0`,
		`count_over_time({app="api"}[5m]) != 0`,
		`rate({app="api"}[5m]) > 0.1`,
		`sum(rate({app="api"}[5m])) > 10`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_BinaryOps_SetOps(t *testing.T) {
	valid := []string{
		`rate({app="api"}[5m]) and rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) or rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) unless rate({app="api"}[5m])`,
		`sum(rate({app="api"}[5m])) and sum(rate({app="api"}[5m]))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_BinaryOps_VectorMatching(t *testing.T) {
	valid := []string{
		`rate({app="api"}[5m]) / on (app) rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) / ignoring (host) rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) * on (app) group_left (team) rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) * on (app) group_right () rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) / on () rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) / on (app, env) rate({app="api"}[5m])`,
		`rate({app="api"}[5m]) + on (app) group_left () rate({app="api"}[5m])`,
		`sum(rate({app="api"}[5m])) / on (app) sum(rate({app="api"}[5m]))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── complex nested queries ───────────────────────────────────────────────────

func TestEdge_ComplexNested(t *testing.T) {
	valid := []string{
		`sum by (app) (rate({app="api"}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`100 * sum by (app) (rate({app="api", status=~"5.."}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`sum(count_over_time({service_name="argocd", service_name != ""}[5s])) by (service_name)`,
		`max by (app) (sum_over_time({app="api"} | unwrap latency [5m]))`,
		`topk(10, sum by (app) (rate({app="api"}[5m])))`,
		`bottomk(5, max by (env) (count_over_time({app="api"}[1h])))`,
		`sum by (app) (rate({app="api"} | json | status>=500 [5m]))`,
		`sum by (app) (rate({app="api"}[5m])) > 1`,
		`max_over_time(rate({app="api"}[1m])[5m:]) / sum by (app) (rate({app="api"}[5m]))`,
		`sum without (instance) (rate({job="integrations/node_exporter"}[5m]))`,
		`sort_desc(sum by (app) (rate({app="api"}[5m])))`,
		`topk(5, sum by (namespace, pod) (rate({namespace="default"}[5m])))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── wildcard ─────────────────────────────────────────────────────────────────

func TestEdge_Wildcard(t *testing.T) {
	if msg := logql.ValidateLogQL("*"); msg != "" {
		t.Errorf("ValidateLogQL(*) expected empty, got %q", msg)
	}
}

// ─── unknown pipeline stages (pass-through) ───────────────────────────────────

func TestEdge_UnknownPipelineStages(t *testing.T) {
	valid := []string{
		`{app="nginx"} | unpack_json`,
		`{app="nginx"} | custom_stage`,
		`{app="nginx"} | json | custom_filter`,
		`{app="nginx"} | noop`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── semantic valid ───────────────────────────────────────────────────────────

func TestEdge_Semantic_Valid(t *testing.T) {
	valid := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`count_over_time({app="api"}[1h])`,
		`bytes_rate({app="api"}[5m])`,
		`bytes_over_time({app="api"}[5m])`,
		`avg_over_time({app="api"} | unwrap latency [5m])`,
		`sum_over_time({app="api"} | unwrap latency [5m])`,
		`min_over_time({app="api"} | unwrap latency [5m])`,
		`max_over_time({app="api"} | unwrap latency [5m])`,
		`stdvar_over_time({app="api"} | unwrap latency [5m])`,
		`stddev_over_time({app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.5, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.95, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(1, {app="api"} | unwrap latency [5m])`,
		`quantile_over_time(1.5, {app="api"} | unwrap latency [5m])`, // >1 allowed by proxy
		`first_over_time({app="api"} | unwrap latency [5m])`,
		`last_over_time({app="api"} | unwrap latency [5m])`,
		`absent_over_time({app="api"}[5m])`,
		`rate_counter({app="api"} | unwrap latency [5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`sum(rate({app="api"}[5m])) by (app)`,
		`max by (app) (count_over_time({app="api"}[1h]))`,
		`rate({app="api"}[5m]) / rate({app="api"}[5m])`,
		`100 * sum by (app) (rate({app="api", status=~"5.."}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`sum(count_over_time({service_name="argocd", service_name != ""}[5s])) by (service_name)`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`{app="nginx"} | line_format "{{ .level }}: {{ .msg }}"`,
		`{app="nginx"} | unwrap duration`,
		`{app="nginx"} | json | unwrap latency`,
		`rate({app="api"}[5m] offset 1h)`,
		`max_over_time(rate({app="api"}[5m])[1h:5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustValidate(t, q) })
	}
}

// ─── semantic invalid ─────────────────────────────────────────────────────────

func TestEdge_Semantic_Invalid(t *testing.T) {
	invalid := []string{
		`{}`,                            // no non-wildcard matchers
		`rate_counter({app="api"}[5m])`, // rate_counter without unwrap
		`quantile_over_time(-0.1, {app="api"} | unwrap lat [5m])`,    // negative phi
		`quantile_over_time(-1, {app="api"} | unwrap lat [5m])`,      // negative phi
		`{app="nginx"} | unwrap a | unwrap b`,                        // double unwrap
		`avg_over_time({app="api"} | unwrap a [5m] | unwrap b [5m])`, // double unwrap in range
	}
	for _, q := range invalid {
		t.Run(q, func(t *testing.T) { mustReject(t, q) })
	}
	// Loki 3.7.1 accepts __error__/__error_details__ inside rate()/bytes_rate()
	// and does not validate line_format template syntax at parse time.
	acceptedByLoki := []string{
		`rate({__error__=""}[5m])`,
		`rate({__error_details__=""}[5m])`,
		`bytes_rate({__error__=""}[5m])`,
		`{app="nginx"} | line_format "{{.msg"`,
		`{app="nginx"} | line_format "{{.level"`,
	}
	for _, q := range acceptedByLoki {
		t.Run("loki_accepts:"+q, func(t *testing.T) { mustValidate(t, q) })
	}
}

// ─── ValidateLogQL (proxy entry point) ───────────────────────────────────────

func TestEdge_ValidateLogQL_Valid(t *testing.T) {
	valid := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`sum(rate({app="api"}[5m])) by (app)`,
		`rate({app="api"}[5m]) / rate({app="api"}[5m])`,
		`100 * sum by (app) (rate({app="api", status=~"5.."}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`max_over_time(rate({app="api"}[5m])[1h:5m])`,
		`rate({app="api"}[5m] offset 1h)`,
		`rate_counter({app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
		`{app="nginx"} | drop trace_id, span_id`,
		`{app="nginx"} | keep app, level`,
		`{app="nginx"} | decolorize`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`*`,
		`sum(count_over_time({service_name="argocd", service_name != ""}[5s])) by (service_name)`,
		`{app="nginx", env!="prod"} |~ "err.*" | logfmt | level="error"`,
		`topk(10, sum by (app) (rate({app="api"}[5m])))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustValidateV(t, q) })
	}
}

func TestEdge_ValidateLogQL_Invalid(t *testing.T) {
	invalid := []string{
		``,
		`{app=`,
		`not logql`,
		`rate({app="api"})`,
		`{}`,
		`| json`,
		`rate_counter({app="api"}[5m])`,
		`quantile_over_time(-0.5, {app="api"} | unwrap lat [5m])`,
		`{app="nginx"} | unwrap a | unwrap b`,
	}
	for _, q := range invalid {
		t.Run(q, func(t *testing.T) { mustRejectV(t, q) })
	}
	// Loki 3.7.1 accepts these at parse time.
	lokiAccepts := []string{
		`rate({__error__=""}[5m])`,
		`{app="nginx"} | line_format "{{.msg"`,
	}
	for _, q := range lokiAccepts {
		t.Run("loki_accepts:"+q, func(t *testing.T) { mustValidateV(t, q) })
	}
}

// ─── parse-only edge cases ────────────────────────────────────────────────────

func TestEdge_ParseOnly_Misc(t *testing.T) {
	valid := []string{
		// multi-label matchers
		`{a="1", b="2", c="3"}`,
		// regex in selector
		`{app=~"(api|web|worker)"}`,
		// negated regex in selector
		`{app!~"(debug|test)"}`,
		// long pipeline
		`{app="nginx"} |= "error" | json | level="error" | drop trace_id | line_format "{{.msg}}"`,
		// deeply nested vector agg
		`sum by (a) (sum by (b) (rate({app="api"}[5m])))`,
		// chained binary ops
		`rate({a="b"}[5m]) + rate({a="b"}[5m]) + rate({a="b"}[5m])`,
		// chained comparisons (parses left-to-right)
		`count_over_time({a="b"}[5m]) > 10`,
		// absent_over_time
		`absent_over_time({app="api", env="prod"}[5m])`,
		// rate with filter
		`rate({app="api"} |= "error" [5m])`,
		// bytes_over_time with filter
		`bytes_over_time({app="api"} | json [5m])`,
		// quantile with pipeline
		`quantile_over_time(0.5, {app="api"} | json | unwrap latency [5m])`,
		// topk with complex inner
		`topk(5, sum by (app) (rate({app="api"}[5m])))`,
		// sort_desc with complex inner
		`sort_desc(topk(10, sum by (app) (rate({app="api"}[5m]))))`,
		// binary with offset on one side
		`rate({app="api"}[5m]) - rate({app="api"}[5m] offset 1h)`,
		// group_left with include labels
		`rate({app="api"}[5m]) * on (app) group_left (team, owner) rate({app="api"}[5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}

	bad := []string{
		`{app="nginx"} | json | {`,  // brace mid-pipeline
		`sum rate({app="api"}[5m])`, // missing paren
		`rate({app="api"}[5m]`,      // unclosed paren
		`{app="nginx"} |`,           // trailing pipe
	}
	for _, q := range bad {
		t.Run("bad:"+q, func(t *testing.T) { mustFail(t, q) })
	}
}

func TestEdge_RegexpPatternDoubleQuoted(t *testing.T) {
	// Loki accepts double-quoted strings for | regexp and | pattern.
	valid := []string{
		`{app="a"} | regexp "(?P<status>[0-9]+)"`,
		`{app="a"} | regexp "\"status\":(?P<status>[0-9]+)"`,
		`{app="a"} | pattern "<ip> - <user> <_>"`,
		`{app="a"} | regexp ` + "`" + `(?P<lvl>\w+)` + "`",
		`{app="a"} | pattern ` + "`" + `<ip> - <user>` + "`",
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_BoolModifier(t *testing.T) {
	// Loki supports the bool modifier on comparison binary operators.
	valid := []string{
		`rate({app="a"}[5m]) > bool 0`,
		`rate({app="a"}[5m]) >= bool 1`,
		`rate({app="a"}[5m]) < bool 100`,
		`rate({app="a"}[5m]) <= bool 50`,
		`rate({app="a"}[5m]) == bool 0`,
		`rate({app="a"}[5m]) != bool 0`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

func TestEdge_BinaryOpOnLogQuery_Rejected(t *testing.T) {
	// Binary arithmetic/comparison on log stream queries must be rejected.
	invalid := []string{
		`{app="a"} > 2`,
		`{app="a"} + {app="b"}`,
		`{app="a"} * 5`,
		`{app="a"} - {app="b"}`,
	}
	for _, q := range invalid {
		t.Run(q, func(t *testing.T) { mustRejectV(t, q) })
	}
}

// ─── regression tests (named per fix) ────────────────────────────────────────

func TestRegression_BacktickLabelMatchers(t *testing.T) {
	// Grafana Drilldown sends backtick-quoted label matchers; parser must accept TokRawString
	// in label matcher positions (fix: parseLabelMatcher now accepts TokRawString).
	valid := []string{
		"{app=`api-gateway`}",
		"{app=`api-gateway`,env=`production`}",
		"{app=~`api-.*`,env=`production`}",
		"{app=`api-gateway`,level!=`debug`}",
		"{app=`api-gateway`} | json",
		"count_over_time({app=`api-gateway`}[5m])",
		"rate({app=`api-gateway`,env=`production`}[5m])",
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustValidateV(t, q) })
	}
}

func TestRegression_DistinctStageRejected(t *testing.T) {
	// | distinct is not a valid LogQL stage; proxy must reject it to match Loki behaviour.
	invalid := []string{
		`{app="api"} | distinct level`,
		`{app="api"} | distinct app`,
		`{app="api"} | json | distinct level`,
		`{app="api"} | logfmt | distinct msg`,
	}
	for _, q := range invalid {
		t.Run(q, func(t *testing.T) { mustRejectV(t, q) })
	}
}

// ─── ip() filter – pipeline integration ──────────────────────────────────────

func TestEdge_IpFilter_PipelineIntegration(t *testing.T) {
	valid := []string{
		`{app="a"} |= ip("192.168.1.1")`,
		`{app="a"} |= ip("::1")`,
		`{app="a"} |= ip("10.0.0.0/8")`,
		`{app="a"} |= ip("2001:db8::/32")`,
		`{app="a"} |= ip("192.168.0.1-192.168.0.255")`,
		`{app="a"} != ip("127.0.0.1")`,
		`{app="a"} |~ "error" |= ip("10.0.0.0/8")`,
		`rate({app="a"} |= ip("192.168.1.0/24") [5m])`,
		`count_over_time({app="a"} != ip("10.0.0.1") [5m])`,
		`{app="a"} | json | status>=500 |= ip("192.168.1.1")`,
		`{app="a"} |= ip("::ffff:192.168.1.1")`,
		`{app="a"} |= ip("0.0.0.0/0")`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustValidateV(t, q) })
	}

	// Loki 3.7.1 does not validate ip() argument syntax at parse time.
	// Any string is accepted; runtime evaluation may fail instead.
	syntacticallyInvalidButLokiAccepts := []string{
		`{app="a"} |= ip("999.999.999.999")`,
		`{app="a"} |= ip("not-an-ip")`,
		`{app="a"} |= ip("256.0.0.1/24")`,
		`{app="a"} |= ip("192.168.1.1/33")`,
		`{app="a"} |= ip("::gggg")`,
		`{app="a"} |= ip("")`,
	}
	for _, q := range syntacticallyInvalidButLokiAccepts {
		t.Run("loki_accepts:"+q, func(t *testing.T) { mustValidateV(t, q) })
	}
}

// ─── label_replace / label_join – extended coverage ──────────────────────────

func TestEdge_LabelReplace_Extended(t *testing.T) {
	valid := []string{
		// basic capture group
		`label_replace(rate({app="a"}[5m]), "dst", "$1", "src", "(.*)")`,
		// capture group with suffix
		`label_replace(rate({app="a"}[5m]), "dst", "${1}-suffix", "src", "(.+)")`,
		// no-match replacement (empty regex target)
		`label_replace(sum by(app)(rate({app="a"}[5m])), "tier", "backend", "app", "^nonexistent$")`,
		// multi-series result input
		`label_replace(sum by(app, level)(count_over_time({env="production"}[5m])), "app_env", "$1-prod", "app", "(.*)")`,
		// label_replace on vector agg
		`label_replace(avg by(level)(rate({app="a"}[5m])), "lvl", "$1", "level", "(.*)")`,
		// label_join with two labels
		`label_join(sum by(app, level)(count_over_time({env="production"}[5m])), "combined", "/", "app", "level")`,
		// label_join with single label
		`label_join(sum by(app)(rate({env="production"}[5m])), "svc_copy", "_", "app")`,
		// label_join with three labels
		`label_join(sum by(app, env, level)(count_over_time({env="production"}[5m])), "full", ".", "app", "env", "level")`,
		// nested label_replace
		`label_replace(label_replace(rate({app="a"}[5m]), "a", "$1", "b", "(.*)"), "c", "x", "d", "(.*)")`,
		// label_join of label_replace output
		`label_join(label_replace(sum by(app)(rate({app="a"}[5m])), "svc", "$1", "app", "(.*)"), "combined", "/", "svc", "env")`,
		// label_replace inside topk
		`topk(5, label_replace(sum by(app)(rate({env="production"}[5m])), "service", "$1", "app", "(.*)"))`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── subqueries – all over_time ops on metric subqueries ─────────────────────

func TestEdge_Subqueries_Extended(t *testing.T) {
	valid := []string{
		// Already tested: rate(count_over_time(...)[range:step])
		// Additional over_time ops applied to subquery:
		`avg_over_time(count_over_time({app="a"}[5m])[30m:5m])`,
		`min_over_time(count_over_time({app="a"}[5m])[30m:5m])`,
		`sum_over_time(count_over_time({app="a"}[5m])[30m:5m])`,
		`max_over_time(count_over_time({app="a"}[5m])[1h:5m])`,
		`quantile_over_time(0.5, count_over_time({app="a"}[5m])[30m:5m])`,
		`stddev_over_time(count_over_time({app="a"}[5m])[30m:5m])`,
		`stdvar_over_time(count_over_time({app="a"}[5m])[30m:5m])`,
		// Nested: subquery inside vector agg
		`sum by(app)(avg_over_time(count_over_time({app="a"}[5m])[30m:5m]))`,
		`max by(level)(min_over_time(count_over_time({app="a"}[5m])[30m:5m]))`,
		// Subquery over bytes_rate
		`max_over_time(bytes_rate({app="a"}[5m])[1h:5m])`,
		// Multiple subquery levels (subquery of subquery would be invalid syntax, skip)
		// Subquery with unwrap inner
		`max_over_time(sum_over_time({app="a"} | json | unwrap duration_ms [5m])[1h:5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── @ modifier – extended contexts ──────────────────────────────────────────

func TestEdge_AtModifier_Extended(t *testing.T) {
	valid := []string{
		// All range aggregation types with @ start()
		`rate({app="a"}[5m] @ start())`,
		`count_over_time({app="a"}[5m] @ start())`,
		`bytes_rate({app="a"}[5m] @ start())`,
		`bytes_over_time({app="a"}[5m] @ start())`,
		`sum_over_time({app="a"} | json | unwrap duration_ms [5m] @ start())`,
		// @ end()
		`rate({app="a"}[5m] @ end())`,
		`count_over_time({app="a"}[5m] @ end())`,
		// @ unix timestamp
		`rate({app="a"}[5m] @ 1609459200)`,
		`bytes_over_time({app="a"}[5m] @ 1609459200)`,
		// @ inside vector aggregation
		`sum by(app)(rate({app="a"}[5m] @ start()))`,
		`avg by(level)(count_over_time({app="a"}[5m] @ end()))`,
		`topk(5, sum by(app)(rate({app="a"}[5m] @ 1609459200)))`,
		// @ on both sides of binary op
		`rate({app="a"}[5m] @ start()) / rate({app="a"}[5m] @ end())`,
		`sum(rate({app="a"}[5m] @ start())) + sum(rate({app="a"}[5m] @ end()))`,
		// @ combined with label filter
		`rate({app="a"} | json | method="GET" [5m] @ start())`,
		// @ with offset — both can coexist
		`rate({app="a"}[5m] offset 1h @ start())`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── offset – extended contexts ───────────────────────────────────────────────

func TestEdge_Offset_Extended(t *testing.T) {
	valid := []string{
		// All range aggregation types with offset
		`count_over_time({app="a"}[5m] offset 5m)`,
		`bytes_rate({app="a"}[5m] offset 10m)`,
		`bytes_over_time({app="a"}[5m] offset 1h)`,
		`sum_over_time({app="a"} | json | unwrap duration_ms [5m] offset 30m)`,
		`avg_over_time({app="a"} | json | unwrap latency [5m] offset 1h)`,
		`absent_over_time({app="a"}[5m] offset 5m)`,
		// Offset inside vector aggregation
		`sum by(app)(rate({app="a"}[5m] offset 1h))`,
		`avg by(level)(count_over_time({app="a"}[5m] offset 30m))`,
		`topk(5, sum by(app)(rate({app="a"}[5m] offset 1h)))`,
		`max by(app)(bytes_rate({app="a"}[5m] offset 5m))`,
		// Offset on binary op sides
		`rate({app="a"}[5m] offset 1h) / rate({app="a"}[5m])`,
		`rate({app="a"}[5m]) - rate({app="a"}[5m] offset 1h)`,
		`sum(rate({app="a"}[5m] offset 1h)) + sum(rate({app="a"}[5m]))`,
		// Offset with various durations
		`rate({app="a"}[1h] offset 1d)`,
		`count_over_time({app="a"}[5m] offset 24h)`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── binary op precedence ─────────────────────────────────────────────────────

func TestEdge_BinaryOp_Precedence(t *testing.T) {
	// All these metric expressions must parse without error. The parser must
	// respect standard operator precedence: ^ > */% > +-  > comparisons > and/unless > or
	valid := []string{
		// multiplication before addition
		`rate({a="b"}[5m]) + rate({a="b"}[5m]) * 2`,
		`rate({a="b"}[5m]) * 2 + rate({a="b"}[5m])`,
		// exponentiation highest
		`rate({a="b"}[5m]) ^ 2 * rate({a="b"}[5m])`,
		`rate({a="b"}[5m]) * rate({a="b"}[5m]) ^ 2`,
		// explicit grouping overrides precedence
		`(rate({a="b"}[5m]) + rate({a="b"}[5m])) * 2`,
		`(rate({a="b"}[5m]) + rate({a="b"}[5m])) / (rate({a="b"}[5m]) + 1)`,
		// comparison lower than arithmetic
		`rate({a="b"}[5m]) + rate({a="b"}[5m]) > 0`,
		`rate({a="b"}[5m]) * 2 > bool rate({a="b"}[5m])`,
		// and/or lower than comparison
		`sum(rate({a="b"}[5m])) > 0 and sum(rate({a="b"}[5m])) < 1000`,
		`sum(rate({a="b"}[5m])) > 0 or sum(rate({a="b"}[5m])) > 100`,
		// unless between and and or in precedence
		`sum(rate({a="b"}[5m])) > 0 unless sum(rate({a="b"}[5m])) == 0`,
		// chained same-precedence ops (left-associative)
		`rate({a="b"}[5m]) + rate({a="b"}[5m]) + rate({a="b"}[5m])`,
		`rate({a="b"}[5m]) * rate({a="b"}[5m]) * rate({a="b"}[5m])`,
		// division chain
		`sum(rate({a="b"}[5m])) / sum(rate({a="b"}[5m])) * 100`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── semantic validation – deep nesting ───────────────────────────────────────

func TestEdge_Semantic_DeepNesting(t *testing.T) {
	// Loki 3.7.1 accepts __error__/__error_details__ inside rate()/bytes_rate()
	// at parse time — any rejection happens at evaluation time, not parse time.
	valid := []string{
		// __error__ in rate nested inside vector agg — Loki accepts at parse time
		`sum by(app)(rate({__error__=""}[5m]))`,
		`max by(level)(rate({__error__!=""}[5m]))`,
		`avg(rate({__error__="timeout"}[5m]))`,
		// __error_details__ in bytes_rate nested inside vector agg
		`sum by(app)(bytes_rate({__error_details__=""}[5m]))`,
		`count(bytes_rate({__error__=""}[5m]))`,
		// doubly nested
		`sum(max by(app)(rate({__error__=""}[5m])))`,
		// __error__ via label filter inside rate
		`sum by(app)(rate({app="a"} | json | __error__!="" [5m]))`,
		`rate({app="a"} | json | __error_details__!="" [5m])`,
		// count_over_time and log queries
		`sum by(app)(count_over_time({__error__!=""}[5m]))`,
		`{app="a"} | json | __error__!=""`,
		`count_over_time({app="a"} | json | __error__!="" [5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustValidateV(t, q) })
	}
}

// ─── pattern line filter (|> and !>) ─────────────────────────────────────────

func TestEdge_PatternLineFilter(t *testing.T) {
	// |> and !> are the pattern-match line filter operators (Loki 2.8+).
	// Pattern syntax uses <field> captures and <_> wildcards.
	valid := []string{
		`{app="a"} |> "GET <_>"`,
		`{app="a"} !> "GET <_>"`,
		`{app="a"} |> "<method> <path> <status>"`,
		"{app=\"a\"} |> `GET <_>`",
		`{app="a"} |= "GET" |> "<_> /api <_>"`,
		`{app="a"} !> "<_> /health <_>" |> "GET <_>"`,
		`rate({app="a"} |> "GET <_>" [5m])`,
		`count_over_time({app="a"} !> "<_> /health <_>" [5m])`,
		`{app="a"} | json |> "<_> error <_>"`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── label filter boolean combinations ────────────────────────────────────────

func TestEdge_LabelFilter_BooleanOps(t *testing.T) {
	// Label filter boolean expressions are captured raw by the parser.
	// All of these should parse without error.
	valid := []string{
		`{app="a"} | json | level="error" or level="warn"`,
		`{app="a"} | json | status>=200 and status<300`,
		`{app="a"} | json | level!="debug" and duration>100`,
		`{app="a"} | json | method="GET" or method="POST"`,
		`{app="a"} | logfmt | level="error" and msg=~".*timeout.*"`,
		`{app="a"} | json | status>=500 or status<200`,
		`rate({app="a"} | json | status>=200 and status<300 [5m])`,
		`count_over_time({app="a"} | logfmt | level="error" and duration>1000 [5m])`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}

// ─── drop / keep – extended edge cases ───────────────────────────────────────

func TestEdge_DropKeep_Extended(t *testing.T) {
	valid := []string{
		// drop with negated regex matcher
		`{app="a"} | json | drop level!~"error|warn"`,
		// drop with numeric comparison
		`{app="a"} | json | drop status<=200`,
		`{app="a"} | json | drop status>500`,
		// keep with __error__ label
		`{app="a"} | json | keep __error__`,
		`{app="a"} | json | keep level, msg, __error__, __error_details__`,
		// chained drop stages
		`{app="a"} | json | drop level | drop trace_id`,
		`{app="a"} | json | drop level | drop trace_id | drop span_id`,
		// drop in metric range context
		`rate({app="a"} | json | drop trace_id [5m])`,
		`count_over_time({app="a"} | json | drop __error__ [5m])`,
		// keep in metric range context
		`count_over_time({app="a"} | json | keep status [5m])`,
		// drop with multiple mixed conditions
		`{app="a"} | json | drop level="debug", trace_id, span_id`,
		// logfmt + drop/keep
		`{app="a"} | logfmt | drop level="debug"`,
		`{app="a"} | logfmt | keep level, msg, duration`,
		// drop with complex regex
		`{app="a"} | json | drop level=~"(debug|trace|info)"`,
	}
	for _, q := range valid {
		t.Run(q, func(t *testing.T) { mustParse(t, q) })
	}
}
