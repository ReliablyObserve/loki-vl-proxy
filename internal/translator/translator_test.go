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
			name:  "stream selector only",
			logql: `{app="nginx"}`,
			want:  `{app="nginx"}`,
		},
		{
			name:  "stream selector with multiple labels",
			logql: `{app="nginx",host="host-42"}`,
			want:  `{app="nginx",host="host-42"}`,
		},
		{
			name:  "line contains filter",
			logql: `{app="nginx"} |= "error"`,
			want:  `{app="nginx"} "error"`,
		},
		{
			name:  "line not contains filter",
			logql: `{app="nginx"} != "debug"`,
			want:  `{app="nginx"} -"debug"`,
		},
		{
			name:  "regexp filter",
			logql: `{app="nginx"} |~ "err.*"`,
			want:  `{app="nginx"} ~"err.*"`,
		},
		{
			name:  "negative regexp filter",
			logql: `{app="nginx"} !~ "debug.*"`,
			want:  `{app="nginx"} NOT ~"debug.*"`,
		},
		{
			name:  "json parser",
			logql: `{app="nginx"} | json`,
			want:  `{app="nginx"} | unpack_json`,
		},
		{
			name:  "logfmt parser",
			logql: `{app="nginx"} | logfmt`,
			want:  `{app="nginx"} | unpack_logfmt`,
		},
		{
			name:  "pattern parser",
			logql: `{app="nginx"} | pattern "<ip> - - <_>"`,
			want:  `{app="nginx"} | extract "<ip> - - <_>"`,
		},
		{
			name:  "regexp parser",
			logql: `{app="nginx"} | regexp "(?P<ip>\\d+\\.\\d+)"`,
			want:  `{app="nginx"} | extract_regexp "(?P<ip>\\d+\\.\\d+)"`,
		},
		{
			name:  "label equal filter",
			logql: `{app="nginx"} | status == "200"`,
			want:  `{app="nginx"} status:=200`,
		},
		{
			name:  "label not equal filter",
			logql: `{app="nginx"} | status != "500"`,
			want:  `{app="nginx"} -status:=500`,
		},
		{
			name:  "drop labels",
			logql: `{app="nginx"} | drop trace_id, span_id`,
			want:  `{app="nginx"} | delete trace_id, span_id`,
		},
		{
			name:  "keep labels",
			logql: `{app="nginx"} | keep app, message`,
			want:  `{app="nginx"} | fields app, message`,
		},
		{
			name:  "multiple line filters",
			logql: `{app="nginx"} |= "error" |= "timeout"`,
			want:  `{app="nginx"} "error" "timeout"`,
		},
		{
			name:  "go template to logsql format",
			logql: `{app="nginx"} | line_format "{{.status}} {{.method}}"`,
			want:  `{app="nginx"} | format "<status> <method>"`,
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
			want:  `{app="nginx"} | stats rate()`,
		},
		{
			name:  "count_over_time",
			logql: `count_over_time({app="nginx"}[5m])`,
			want:  `{app="nginx"} | stats count()`,
		},
		{
			name:  "sum of rate by label",
			logql: `sum(rate({app="nginx"}[5m])) by (host)`,
			want:  `{app="nginx"} | stats by (host) rate()`,
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
