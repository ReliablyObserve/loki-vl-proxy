package translator

import (
	"strings"
	"testing"
)

// A `| detected_level <op> "<value>"` stage that no parser, line rewrite or
// label stage precedes compares Loki's ingest-time value (Loki v3.7.9
// pkg/distributor/field_detection.go extractLogLevel), which the chain
// derives from what VictoriaLogs stored. It never reads a level key the line
// holds over the stored stream label, as `| unpack_logfmt | filter level`
// did, and a line without any level is unknown.

// conformance: severity/detected-level-filter-json-level, severity-detected-level-derivation
func TestDetectedLevelFilterStageTranslation(t *testing.T) {
	logChain := strings.TrimSuffix(BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGroup, nil), groupChainDelete)
	metricChain := BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil)
	cases := []struct {
		name   string
		logql  string
		metric bool
		// want lists substrings in the order they must appear.
		want []string
		// not lists substrings that must not appear.
		not []string
	}{
		{
			name:  "log equality reads the chain's value",
			logql: `{env="production"} | detected_level="error"`,
			want:  []string{`env:="production" (`, logChain, ` | filter __dl:="error"`, levelChainEnd},
			not:   []string{"unpack_logfmt | filter", "level:=\"error\""},
		},
		{
			name:  "log regexp of levels",
			logql: `{service_name="api-gateway"} | detected_level=~"error|warn"`,
			want:  []string{logChain, ` | filter (__dl:="error" OR __dl:="warn")`, levelChainEnd},
		},
		{
			name:  "log inequality has no prefilter",
			logql: `{env="production"} | detected_level!="info"`,
			want:  []string{`env:="production" ` + logChain, ` | filter -__dl:="info"`, levelChainEnd},
		},
		{
			name:  "log unknown",
			logql: `{env="production"} | detected_level="unknown"`,
			want:  []string{logChain, ` | filter __dl:="unknown"`, levelChainEnd},
		},
		{
			name:  "a line filter after the level filter stays a query filter",
			logql: `{env="production"} | detected_level="error" !> "level=error msg=<_>"`,
			want:  []string{`env:="production" (`, `NOT ~"(?s)level=error msg=.+" | format if`, ` | filter __dl:="error"`, levelChainEnd},
			not:   []string{`| filter NOT`},
		},
		{
			name:  "a parser after the level filter runs on the stored rows",
			logql: `{app="api"} | detected_level="warn" | json | status="500"`,
			want:  []string{logChain, ` | filter __dl:="warn"`, levelChainEnd + " | unpack_json", `| filter status:="500"`},
		},
		{
			name:  "a drop of the error labels keeps the level inputs",
			logql: `{app="api"} | drop __error__ | detected_level="warn"`,
			want:  []string{` | filter __dl:="warn"`, levelChainEnd},
		},
		{
			name:   "metric filter writes detected_level once and lets the witness through",
			logql:  `sum by (detected_level) (count_over_time({env="production"} | detected_level!="info" [1m]))`,
			metric: true,
			want:   []string{metricChain, ` | filter (-detected_level:="info") OR __lw:*`, "| stats by (detected_level) count()"},
			not:    []string{"unpack_logfmt | filter"},
		},
		{
			name:   "metric filter without a level grouping",
			logql:  `sum by (service_name) (count_over_time({env="production"} | detected_level="error" [5m]))`,
			metric: true,
			want:   []string{"| filter (", metricChain, ` | filter (detected_level:="error") OR __lw:*`},
		},
		{
			name:   "metric not-empty keeps every line",
			logql:  `sum by (detected_level) (count_over_time({env="production"} | detected_level!="" [1m]))`,
			metric: true,
			want:   []string{metricChain, "| stats by (detected_level) count()"},
			not:    []string{"level:!", "OR __lw:*"},
		},
		{
			name:   "metric empty value keeps no line",
			logql:  `sum(count_over_time({env="production"} | detected_level="" [1m]))`,
			metric: true,
			want:   []string{matchNoRows},
		},
		{
			name:  "consecutive level filters share one chain",
			logql: `{app="x"} | detected_level!="debug" | detected_level!="info"`,
			want:  []string{logChain, ` | filter (-__dl:="debug") (-__dl:="info")`, levelChainEnd},
		},
		{
			name:   "consecutive metric level filters share one chain",
			logql:  `sum by (detected_level) (count_over_time({app="x"} | detected_level!="debug" | detected_level=~"info|warn" [5m]))`,
			metric: true,
			want:   []string{metricChain, ` | filter ((-detected_level:="debug") ((detected_level:="info" OR detected_level:="warn"))) OR __lw:*`},
		},
		// Outside the intact inputs the stage keeps its rendering.
		{
			name:  "after a parser",
			logql: `{app="api"} | json | detected_level="error"`,
			want:  []string{`| unpack_json`, `| filter level:="error"`},
			not:   []string{"__dl_s"},
		},
		{
			name:  "after a label_format",
			logql: `{app="api"} | label_format level="x" | detected_level="error"`,
			not:   []string{"__dl_s"},
		},
		{
			name:  "an or of matchers",
			logql: `{app="api"} | detected_level="error" or detected_level="warn"`,
			not:   []string{"__dl_s"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			var err error
			if tc.metric {
				got, err = TranslateLogQLWithStreamFields(tc.logql, nil, nil)
			} else {
				got, err = TranslateLogQL(tc.logql)
			}
			if err != nil {
				t.Fatalf("translate %q: %v", tc.logql, err)
			}
			at := 0
			for _, w := range tc.want {
				i := strings.Index(got[at:], w)
				if i < 0 {
					t.Fatalf("translation of %q lacks %q after offset %d:\n%s", tc.logql, short(w), at, got)
				}
				at += i + len(w)
			}
			for _, n := range tc.not {
				if strings.Contains(got, n) {
					t.Fatalf("translation of %q holds %q:\n%s", tc.logql, n, got)
				}
			}
			if strings.Count(got, detectedLevelChainMarker) > 1 {
				t.Fatalf("translation of %q runs the chain twice:\n%s", tc.logql, got)
			}
		})
	}
}

func short(s string) string {
	if len(s) > 120 {
		return s[:60] + " … " + s[len(s)-60:]
	}
	return s
}

// conformance: severity-detected-level-derivation
func TestDetectedLevelProbeBase(t *testing.T) {
	cases := []struct{ base, want string }{
		{`env:="production"`, `env:="production"`},
		{`env:="production" | filter (level:(i(error)))`, `env:="production"`},
		{`env:="production" | delete x | filter a:="b"`, `env:="production" | delete x`},
		{`env:="production" | delete x`, `env:="production" | delete x`},
		// A pipe inside a quoted value is no pipe.
		{`env:="a | filter b"`, `env:="a | filter b"`},
	}
	for _, tc := range cases {
		if got := DetectedLevelProbeBase(tc.base); got != tc.want {
			t.Fatalf("DetectedLevelProbeBase(%q) = %q, want %q", tc.base, got, tc.want)
		}
	}
}
