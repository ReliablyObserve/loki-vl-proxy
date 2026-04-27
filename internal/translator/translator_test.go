package translator

import (
	"testing"
)

func TestTranslateLogQL(t *testing.T) {
	tests := []struct {
		name    string
		logql   string
		want    string
		wantErr bool
	}{
		{
			name:  "empty query",
			logql: "",
			want:  "*",
		},
		{
			name:  "stream selector only — single label",
			logql: `{app="nginx"}`,
			want:  `app:=nginx`,
		},
		{
			name:  "stream selector with multiple labels",
			logql: `{app="nginx",host="host-42"}`,
			want:  `app:=nginx host:=host-42`,
		},
		{
			name:  "line contains filter — substring semantics",
			logql: `{app="nginx"} |= "error"`,
			want:  `app:=nginx ~"error"`,
		},
		{
			name:  "line contains filter with backtick raw string",
			logql: "{app=\"nginx\"} |= `api`",
			want:  `app:=nginx ~"api"`,
		},
		{
			name:  "line contains backtick raw string with logfmt pipeline",
			logql: "{app=\"nginx\"} |= `api` | logfmt",
			want:  `app:=nginx ~"api" | unpack_logfmt`,
		},
		{
			name:  "line contains backtick raw string containing pipe char",
			logql: "{app=\"nginx\"} |= `api|v1` | logfmt",
			want:  `app:=nginx ~"api|v1" | unpack_logfmt`,
		},
		{
			name:  "line not contains filter — substring semantics",
			logql: `{app="nginx"} != "debug"`,
			want:  `app:=nginx NOT ~"debug"`,
		},
		{
			name:    "invalid selector syntax returns error",
			logql:   `{app="nginx"`,
			wantErr: true,
		},
		{
			name:  "regexp filter",
			logql: `{app="nginx"} |~ "err.*"`,
			want:  `app:=nginx ~"err.*"`,
		},
		{
			name:  "negative regexp filter",
			logql: `{app="nginx"} !~ "debug.*"`,
			want:  `app:=nginx NOT ~"debug.*"`,
		},
		{
			name:  "pattern line filter",
			logql: `{app="nginx"} |> "test <_> pattern"`,
			want:  `app:=nginx ~"test .* pattern"`,
		},
		{
			name:  "negative pattern line filter",
			logql: `{app="nginx"} !> "test <_> pattern"`,
			want:  `app:=nginx NOT ~"test .* pattern"`,
		},
		{
			name:  "pattern line filter with alternation",
			logql: `{app="nginx"} |> "test <_> pattern" or "other <_> value"`,
			want:  `app:=nginx ~"(?:test .* pattern)|(?:other .* value)"`,
		},
		{
			name:  "json parser",
			logql: `{app="nginx"} | json`,
			want:  `app:=nginx | unpack_json`,
		},
		{
			name:  "logfmt parser",
			logql: `{app="nginx"} | logfmt`,
			want:  `app:=nginx | unpack_logfmt`,
		},
		{
			name:  "pattern parser",
			logql: `{app="nginx"} | pattern "<ip> - - <_>"`,
			want:  `app:=nginx | extract "<ip> - - <_>"`,
		},
		{
			name:  "pattern wildcard no-op is dropped",
			logql: `{app="nginx"} | pattern "(.*)" | status="500"`,
			want:  `app:=nginx status:=500`,
		},
		{
			name:  "extract wildcard no-op is dropped defensively",
			logql: "{app=\"nginx\"} | extract `(.*)` | status=`500`",
			want:  `app:=nginx status:=500`,
		},
		{
			name:  "regexp parser",
			logql: `{app="nginx"} | regexp "(?P<ip>\\d+\\.\\d+)"`,
			want:  `app:=nginx | extract_regexp "(?P<ip>\\d+\\.\\d+)"`,
		},
		{
			name:  "label equal filter after pipe",
			logql: `{app="nginx"} | status == "200"`,
			want:  `app:=nginx status:=200`,
		},
		{
			name:  "label not equal filter after pipe",
			logql: `{app="nginx"} | status != "500"`,
			want:  `app:=nginx -status:=500`,
		},
		{
			name:  "drop labels",
			logql: `{app="nginx"} | drop trace_id, span_id`,
			want:  `app:=nginx | delete trace_id, span_id`,
		},
		{
			name:  "keep labels",
			logql: `{app="nginx"} | keep app, message`,
			want:  `app:=nginx | fields _time, _msg, _stream, app, message`,
		},
		{
			name:  "multiple line filters — both substring",
			logql: `{app="nginx"} |= "error" |= "timeout"`,
			want:  `app:=nginx ~"error" ~"timeout"`,
		},
		{
			name:  "go template to logsql format",
			logql: `{app="nginx"} | line_format "{{.status}} {{.method}}"`,
			want:  `app:=nginx | format "<status> <method>"`,
		},
		{
			name:  "multi-label with level filter",
			logql: `{app="api",namespace="prod",level="error"}`,
			want:  `app:=api namespace:=prod level:=error`,
		},
		{
			name:  "negative label in stream selector",
			logql: `{app="api",level!="info"}`,
			want:  `app:=api -level:=info`,
		},
		{
			name:  "regex label in stream selector",
			logql: `{app=~"api-.*",namespace="prod"}`,
			want:  `app:~"api-.*" namespace:=prod`,
		},
		{
			name:  "negative regex in stream selector",
			logql: `{namespace!~"kube-.*"}`,
			want:  `-namespace:~"kube-.*"`,
		},
		{
			name:  "regex with alternation",
			logql: `{namespace=~"prod|staging"}`,
			want:  `namespace:~"prod|staging"`,
		},
		// Substring semantics test — critical correctness
		{
			name:  "substring matches partial words like Loki does",
			logql: `{app="nginx"} |= "err"`,
			// Must use ~"err" not "err" — Loki matches "error", "stderr", etc.
			want: `app:=nginx ~"err"`,
		},
		{
			name:  "chained substring + negative substring",
			logql: `{app="nginx"} |= "error" != "timeout"`,
			want:  `app:=nginx ~"error" NOT ~"timeout"`,
		},
		// Parser + filter chain tests
		{
			name:  "json then label filter",
			logql: `{app="api"} | json | status >= 400`,
			want:  `app:=api | unpack_json | filter status:>=400`,
		},
		{
			name:  "json then multiple filters",
			logql: `{app="api"} | json | status >= 500 | method = "POST"`,
			want:  `app:=api | unpack_json | filter status:>=500 | filter method:=POST`,
		},
		{
			name:  "logfmt then label filter",
			logql: `{app="api"} | logfmt | duration > "1s"`,
			want:  `app:=api | unpack_logfmt | filter duration:>1s`,
		},
		{
			name:  "json then keep",
			logql: `{app="api"} | json | keep level, status`,
			want:  `app:=api | unpack_json | fields _time, _msg, _stream, level, status`,
		},
		{
			name:  "drilldown pattern stats query shape",
			logql: `{foo="bar"} |> ` + "`" + `test <_> pattern` + "`" + ` | pattern ` + "`" + `test <field_1> pattern` + "`" + ` | keep field_1 | line_format ""`,
			want:  `foo:=bar ~"test .* pattern" | extract "test <field_1> pattern" | fields _time, _msg, _stream, field_1 | format ""`,
		},
		{
			name:  "json then drop",
			logql: `{app="api"} | json | drop __error__`,
			want:  `app:=api | unpack_json | delete __error__`,
		},
		{
			name:  "bare unwrap with no field — Grafana query builder incomplete state",
			logql: `{env="production"} | unwrap `,
			want:  `env:=production`,
		},
		{
			name:  "bare unwrap no trailing space",
			logql: `{env="production"} | unwrap`,
			want:  `env:=production`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateLogQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestConvertGoTemplate(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"{{.status}}"`, `"<status>"`},
		{`"{{.method}} {{.path}}"`, `"<method> <path>"`},
		{`"plain text"`, `"plain text"`},
	}

	for _, tt := range tests {
		got := convertGoTemplate(tt.input)
		if got != tt.want {
			t.Errorf("convertGoTemplate(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMetricQueryTranslation(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "rate",
			logql: `rate({app="nginx"}[5m])`,
			want:  `app:=nginx | stats by (_stream, level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (_stream, level) sum(__lvp_rate)`,
		},
		{
			name:  "count_over_time",
			logql: `count_over_time({app="nginx"}[5m])`,
			want:  `app:=nginx | stats count()`,
		},
		{
			name:  "sum of rate by label",
			logql: `sum(rate({app="nginx"}[5m])) by (host)`,
			want:  `app:=nginx | stats by (host) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (host) sum(__lvp_rate)`,
		},
		{
			name:  "stddev outer aggregation",
			logql: `stddev(rate({app="nginx"}[5m])) by (host)`,
			want:  `app:=nginx | stats by (host) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (host) sum(__lvp_rate)`,
		},
		{
			name:  "stdvar outer aggregation",
			logql: `stdvar(rate({app="nginx"}[5m])) by (host)`,
			want:  `app:=nginx | stats by (host) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (host) sum(__lvp_rate)`,
		},
		// without() clause tests moved to fixes_test.go — now correctly returns error
		{
			name:  "quantile_over_time",
			logql: `quantile_over_time(0.95, {app="nginx"} | unwrap duration [5m])`,
			want:  `app:=nginx | stats quantile(0.95, duration)`,
		},
		{
			name:  "quantile_over_time with by",
			logql: `sum(quantile_over_time(0.99, {app="nginx"} | unwrap latency [5m])) by (host)`,
			want:  `app:=nginx | stats by (host) quantile(0.99, latency)`,
		},
		{
			name:  "absent_over_time",
			logql: `absent_over_time({app="nginx"}[5m])`,
			want:  `app:=nginx | stats count()`,
		},
		{
			name:  "avg_over_time with unwrap",
			logql: `avg_over_time({app="nginx"} | unwrap response_time [5m])`,
			want:  `app:=nginx | stats avg(response_time)`,
		},
		{
			name:  "rate_counter with unwrap",
			logql: `rate_counter({app="nginx"} | unwrap requests_total [5m])`,
			want:  `app:=nginx | stats __rate_counter__(requests_total)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("TranslateLogQL() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestMetricQueryTranslation_DedupesByLabelsAfterTranslation(t *testing.T) {
	labelFn := func(label string) string {
		if label == "detected_level" {
			return "level"
		}
		return label
	}

	got, err := TranslateLogQLWithLabels(`sum by (level, detected_level) (count_over_time({foo="bar"}[5m]))`, labelFn)
	if err != nil {
		t.Fatalf("TranslateLogQLWithLabels() error = %v", err)
	}
	want := `foo:=bar | stats by (level) count()`
	if got != want {
		t.Fatalf("TranslateLogQLWithLabels() = %q, want %q", got, want)
	}
}

func TestMetricQueryTranslation_MalformedDottedDrilldownStage(t *testing.T) {
	labelFn := func(label string) string {
		switch label {
		case "deployment_environment":
			return "deployment.environment"
		case "k8s_namespace_name":
			return "k8s.namespace.name"
		case "detected_level":
			return "level"
		default:
			return label
		}
	}

	got, err := TranslateLogQLWithLabels(`sum by (level, detected_level) (count_over_time({deployment_environment="dev", k8s_namespace_name="sample_ns"} | k8s . `+"`cluster.`"+`[1m]))`, labelFn)
	if err != nil {
		t.Fatalf("TranslateLogQLWithLabels() error = %v", err)
	}
	want := `"deployment.environment":=dev "k8s.namespace.name":=sample_ns ~"k8s\.cluster\." | stats by (level) count()`
	if got != want {
		t.Fatalf("TranslateLogQLWithLabels() = %q, want %q", got, want)
	}
}

func TestConvertGoTemplate_DottedFieldNames(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"{{.name}}"`, `"<name>"`},
		{`"{{.service.name}}"`, `"<service.name>"`},
		{`"{{.k8s.pod.name}} {{.level}}"`, `"<k8s.pod.name> <level>"`},
	}
	for _, tc := range tests {
		got := convertGoTemplate(tc.input)
		if got != tc.want {
			t.Errorf("convertGoTemplate(%s) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestExtractUnwrapField_ConversionWrappers(t *testing.T) {
	tests := []struct {
		name  string
		inner string
		want  string
	}{
		{"plain field", `{app="x"} | unwrap latency [5m]`, "latency"},
		{"duration wrapper", `{app="x"} | unwrap duration(response_time) [5m]`, "response_time"},
		{"bytes wrapper", `{app="x"} | unwrap bytes(body_size) [5m]`, "body_size"},
		{"no unwrap", `{app="x"} [5m]`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractUnwrapField(tc.inner)
			if got != tc.want {
				t.Errorf("extractUnwrapField(%q) = %q, want %q", tc.inner, got, tc.want)
			}
		})
	}
}

func TestBinaryOps_Extended(t *testing.T) {
	tests := []struct {
		name  string
		logql string
	}{
		{"modulo", `rate({app="x"}[5m]) % 10`},
		{"power", `rate({app="x"}[5m]) ^ 2`},
		{"comparison", `rate({app="x"}[5m]) > 100`},
		{"equality", `rate({app="x"}[5m]) == 0`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := tryTranslateBinaryMetricExpr(tc.logql, nil)
			if !ok {
				t.Errorf("expected binary expression to be recognized: %q", tc.logql)
				return
			}
			if result == "" {
				t.Error("expected non-empty result")
			}
		})
	}
}

// TestBareLabelMatcherMustNotProduceDoubleQuotedString is a regression guard for
// a production bug observed against e2e-proxy-underscore. When a LogQL stream
// matcher arrived without surrounding `{...}` (e.g., `app="json-test"` instead
// of `{app="json-test"}`), the translator's bare-text fallback wrapped the
// raw matcher in double quotes and produced LogsQL like `"app="json-test""`,
// which VictoriaLogs rejects with an "unexpected token" parse error.
//
// The correct VL LogsQL output for the equivalent braced query is `app:=json-test`.
// `translateBareFilter` is a phrase-filter fallback for arbitrary text and must
// never be reached for label-matcher syntax — callers are responsible for
// ensuring the input is a valid LogQL stream selector. This test pins the
// observed bug so it cannot silently regress: the buggy double-quoted output
// must not be emitted by any translation path that the proxy invokes.
func TestBareLabelMatcherMustNotProduceDoubleQuotedString(t *testing.T) {
	cases := []struct {
		name  string
		logql string
		// notWant is the buggy output observed against VL. The translator
		// must never emit this string — it is a malformed phrase filter that
		// VL parses as `"app=` ... `json-test` ... `""` and rejects.
		notWant string
	}{
		{
			name:    "app matcher without braces (production bug repro)",
			logql:   `app="json-test"`,
			notWant: `"app="json-test""`,
		},
		{
			name:    "level matcher without braces (production bug repro)",
			logql:   `level="warn"`,
			notWant: `"level="warn""`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				// Erroring out is an acceptable outcome — it just must not
				// silently emit the malformed phrase filter.
				return
			}
			if got == tc.notWant {
				t.Fatalf("BUG: translator wrapped raw matcher in double quotes\n  input: %q\n  got:   %q (this is the production bug)", tc.logql, got)
			}
		})
	}
}

// TestBracedLabelMatcherTranslatesToVLFieldFilter pins the correct translation
// for the matchers that triggered the production bug above. These are the
// queries the proxy actually receives from Grafana / Drilldown and they MUST
// translate to VL `:=` field filters (not LogQL `=` matchers wrapped in quotes).
func TestBracedLabelMatcherTranslatesToVLFieldFilter(t *testing.T) {
	cases := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "app=json-test braced",
			logql: `{app="json-test"}`,
			want:  `app:=json-test`,
		},
		{
			name:  "level=warn braced",
			logql: `{level="warn"}`,
			want:  `level:=warn`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("TranslateLogQL(%q) returned error: %v", tc.logql, err)
			}
			if got != tc.want {
				t.Fatalf("TranslateLogQL(%q)\n  got:  %q\n  want: %q", tc.logql, got, tc.want)
			}
		})
	}
}

func TestTranslateLabelFormat_MultiRename(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "single rename",
			logql: `{app="x"} | label_format level="{{.severity}}"`,
			want:  `app:=x | format "<severity>" as level`,
		},
		{
			name:  "multi rename",
			logql: `{app="x"} | label_format level="{{.severity}}", status="{{.code}}"`,
			want:  `app:=x | format "<severity>" as level | format "<code>" as status`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("TranslateLogQL() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}
