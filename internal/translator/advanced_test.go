package translator

import "testing"

// =============================================================================
// Advanced LogQL Translation Tests
//
// These cover real-world patterns from production Loki deployments.
// TDD approach: tests define the spec, code follows.
// =============================================================================

func TestAdvanced_MetricQueries(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "bytes_over_time",
			logql: `bytes_over_time({app="nginx"}[5m])`,
			want:  `app:=nginx | stats sum(len(_msg))`,
		},
		{
			name:  "bytes_rate",
			logql: `bytes_rate({app="nginx"}[5m])`,
			want:  `app:=nginx | stats rate_sum(len(_msg))`,
		},
		{
			name:  "avg_over_time with unwrap",
			logql: `avg_over_time({app="api"} | json | unwrap duration [5m])`,
			want:  `app:=api | unpack_json | stats avg(duration)`,
		},
		{
			name:  "max_over_time with unwrap",
			logql: `max_over_time({app="api"} | logfmt | unwrap response_size [5m])`,
			want:  `app:=api | unpack_logfmt | stats max(response_size)`,
		},
		{
			name:  "sum by namespace of rate",
			logql: `sum by (namespace) (rate({cluster="prod"}[5m]))`,
			want:  `cluster:=prod | stats by (namespace) rate()`,
		},
		{
			name:  "topk of rate — simplified",
			logql: `topk(10, rate({region="us-east-1"}[5m]))`,
			want:  `region:=us-east-1 | stats rate()`,
		},
		{
			name:  "count_over_time with line filter",
			logql: `count_over_time({app="nginx"} |= "error" [5m])`,
			want:  `app:=nginx ~"error" | stats count()`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestAdvanced_ComplexPipelines(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  string
	}{
		{
			name:  "logfmt then filter then drop",
			logql: `{app="api"} | logfmt | status = "500" | drop __error__`,
			want:  `app:=api | unpack_logfmt | filter status:=500 | delete __error__`,
		},
		{
			name:  "json then line_format",
			logql: `{app="api"} | json | line_format "{{.method}} {{.path}} {{.status}}"`,
			want:  `app:=api | unpack_json | format "<method> <path> <status>"`,
		},
		{
			name:  "chained negative line filters",
			logql: `{app="nginx"} != "/health" != "/ready" != "/metrics"`,
			want:  `app:=nginx NOT ~"/health" NOT ~"/ready" NOT ~"/metrics"`,
		},
		{
			name:  "line filter then regex then json",
			logql: `{app="api"} |= "error" |~ "status=[45]\\d{2}" | json`,
			want:  `app:=api ~"error" ~"status=[45]\\d{2}" | unpack_json`,
		},
		{
			name:  "five stage pipeline",
			logql: `{app="api"} |= "POST" | json | status >= 400 | line_format "{{.method}} {{.path}}" | keep method, path`,
			want:  `app:=api ~"POST" | unpack_json | filter status:>=400 | format "<method> <path>" | fields _time, _msg, _stream, method, path`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, tt.want)
			}
		})
	}
}

func TestAdvanced_NginxAccessLogPattern(t *testing.T) {
	// Common nginx access log parsing pattern
	logql := `{app="nginx"} | pattern "<ip> - - <_> \"<method> <uri> <_>\" <status> <size>" | status >= 400`
	want := `app:=nginx | extract "<ip> - - <_> \"<method> <uri> <_>\" <status> <size>" | filter status:>=400`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_EmptyStreamSelector(t *testing.T) {
	// Loki requires stream selector but some tools send empty
	logql := `{} |= "error"`
	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// Empty stream selector → just the line filter
	if got != `~"error"` {
		t.Errorf("got %q, want %q", got, `~"error"`)
	}
}

func TestAdvanced_LabelFormatWithTemplate(t *testing.T) {
	logql := `{app="api"} | json | label_format request_info="{{.method}} {{.path}}"`
	want := `app:=api | unpack_json | format "<method> <path>" as request_info`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_RegexpParser(t *testing.T) {
	logql := `{app="nginx"} | regexp "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+) .* (?P<status>\\d{3})"`
	want := `app:=nginx | extract_regexp "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+) .* (?P<status>\\d{3})"`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_RegexLabelFilter(t *testing.T) {
	logql := `{app="api"} | json | status =~ "5.."`
	want := `app:=api | unpack_json | filter status:~"5.."`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_NegativeRegexLabelFilter(t *testing.T) {
	logql := `{app="api"} | json | method !~ "GET|HEAD"`
	want := `app:=api | unpack_json | filter -method:~"GET|HEAD"`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_MultipleStreamLabelsWithMixedOps(t *testing.T) {
	logql := `{cluster="prod",namespace=~"api-.*",app!="debug-tool",level!~"debug|trace"}`
	want := `cluster:=prod namespace:~"api-.*" -app:=debug-tool -level:~"debug|trace"`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("TranslateLogQL()\n  got  = %q\n  want = %q", got, want)
	}
}

func TestAdvanced_BareLabelQuery(t *testing.T) {
	// Some Grafana variables send just a label selector
	logql := `{namespace="prod"}`
	want := `namespace:=prod`

	got, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
