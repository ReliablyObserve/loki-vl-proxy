package translator

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// A label name_extracted after a json or logfmt parser holds the parsed value
// of the key name when the stream also has a label name (Loki renames the
// parsed label then: parser.go duplicateSuffix) and the key name_extracted
// otherwise. unpack_json and unpack_logfmt overwrite the stored field of the
// stream label's name, so the translation unpacks the key again into a
// scratch field and reads the stream label's presence from _stream (or, for
// service_name, which every stream has in Loki, from the key alone).
//
// conformance: semantics/extracted-suffix-collision, loki_api_v1_query_range
func TestExtractedSuffixLabelFilter(t *testing.T) {
	const scratch = ` | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level`
	const levelFilter = `((__lxp_stream:~"[{,]level=\"" __lxp_level:* __lxp_level:="x") OR (NOT (__lxp_stream:~"[{,]level=\"" __lxp_level:*) ((level_extracted:="x" OR "level.extracted":="x" OR "level-extracted":="x"))))`
	tests := []struct {
		name, logql, want string
	}{
		{
			name:  "json",
			logql: `{app="api"} | json | level_extracted="debug"`,
			want: `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_"` + scratch +
				` | filter ((__lxp_stream:~"[{,]level=\"" __lxp_level:* __lxp_level:="debug") OR (NOT (__lxp_stream:~"[{,]level=\"" __lxp_level:*) ((level_extracted:="debug" OR "level.extracted":="debug" OR "level-extracted":="debug"))))` +
				` | delete __lxp*`,
		},
		{
			name:  "logfmt existence check on service_name needs no stream lookup",
			logql: `{app="api"} | logfmt | service_name_extracted!=""`,
			want: `app:="api" | unpack_logfmt | unpack_logfmt from _msg fields (service_name) result_prefix "__lxp0_"` +
				` | copy __lxp0_service_name as __lxp_service_name` +
				` | filter ((__lxp_service_name:* __lxp_service_name:!"") OR (NOT (__lxp_service_name:*) (service_name_extracted:!""))) | delete __lxp*`,
		},
		{
			name:  "or after the renamed label",
			logql: `{app="api"} | json | level_extracted="x" or status="5"`,
			want:  `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_"` + scratch + ` | filter (` + levelFilter + ` or status:="5") | delete __lxp*`,
		},
		{
			name:  "or before the renamed label",
			logql: `{app="api"} | json | status="5" or level_extracted="x"`,
			want:  `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_"` + scratch + ` | filter (status:="5" or ` + levelFilter + `) | delete __lxp*`,
		},
		{
			name:  "and keeps the renamed label's own grouping",
			logql: `{app="api"} | json | status="5" and level_extracted="x"`,
			want:  `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_"` + scratch + ` | filter (status:="5" and ` + levelFilter + `) | delete __lxp*`,
		},
		{
			name:  "two renamed labels share one unpack",
			logql: `{app="api"} | json | level_extracted="x" and app_extracted!="y"`,
			want:  `fields (level, app) result_prefix "__lxp0_"`,
		},
		{
			name:  "an extraction list that names the label",
			logql: `{app="api"} | json level | level_extracted="x"`,
			want:  `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_"` + scratch + ` | filter ` + levelFilter + ` | delete __lxp*`,
		},
		{
			name:  "an extraction list renames the target, not the key",
			logql: `{app="api"} | json lv="level" | lv_extracted="x"`,
			want: `fields (level) result_prefix "__lxp0_" | copy "level" as lv | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_lv` +
				` | filter ((__lxp_stream:~"[{,]lv=\"" __lxp_lv:*`,
		},
		{
			name:  "an extraction list that does not name the label never collides",
			logql: `{app="api"} | json status | level_extracted="x"`,
			want:  `app:="api" | unpack_json | filter (level_extracted:="x" OR "level.extracted":="x" OR "level-extracted":="x")`,
		},
		{
			name:  "the scratch unpack runs before a line_format rewrites the line",
			logql: `{app="api"} | json | line_format "{{.msg}}" | level_extracted="x"`,
			want:  `app:="api" | unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_" | format "<msg>"` + scratch + ` | filter ` + levelFilter + ` | delete __lxp*`,
		},
		{
			name:  "the first parser decides the value",
			logql: `{app="api"} | json | logfmt | level_extracted="x"`,
			want: `| unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_" | unpack_logfmt | unpack_logfmt from _msg fields (level) result_prefix "__lxp1_"` +
				` | copy _stream as __lxp_stream | copy __lxp1_level as __lxp_level | format if (__lxp0_level:*) "<__lxp0_level>" as __lxp_level | filter`,
		},
		{
			name:  "a filter between two parsers sees only the parsers before it",
			logql: `{app="api"} | json | level_extracted="x" | logfmt`,
			want: `| unpack_json | unpack_json from _msg fields (level) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level` +
				` | filter ` + levelFilter + ` | unpack_logfmt | unpack_logfmt from _msg fields (level) result_prefix "__lxp1_" | delete __lxp*`,
		},
		{
			name:  "each filter gets the values of the parsers before it, across a line_format",
			logql: `{app="api"} | json | level_extracted="x" | line_format "{{.msg}}" | logfmt | app_extracted="y"`,
			want: `| unpack_json | unpack_json from _msg fields (level, app) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level` +
				` | filter ` + levelFilter + ` | format "<msg>" | unpack_logfmt | unpack_logfmt from _msg fields (level, app) result_prefix "__lxp1_"` +
				` | copy __lxp1_app as __lxp_app | format if (__lxp0_app:*) "<__lxp0_app>" as __lxp_app | filter`,
		},
		{
			name:  "a label the query sets itself wins",
			logql: `{app="api"} | json | label_format level_extracted="z" | level_extracted="x"`,
			want:  `app:="api" | unpack_json | format "z" as level_extracted | filter level_extracted:="x"`,
		},
		{
			name:  "a name without the suffix is untouched",
			logql: `{app="api"} | json | level="x"`,
			want:  `app:="api" | unpack_json | filter level:="x"`,
		},
		{
			name:  "keep keeps the stored field the label is read from",
			logql: `{app="api"} | json | keep level_extracted`,
			want:  `app:="api" | unpack_json | fields _time, _msg, _stream, level_extracted, level`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(tt.want, "app:=") {
				if got != tt.want {
					t.Errorf("TranslateLogQL(%s)\n got  %s\n want %s", tt.logql, got, tt.want)
				}
			} else if !strings.Contains(got, tt.want) {
				t.Errorf("TranslateLogQL(%s) = %s, want it to hold %s", tt.logql, got, tt.want)
			}
		})
	}
}

// A stats pipe grouping by name_extracted reads the same value, the first
// parser's when the query has two.
//
// conformance: semantics/extracted-suffix-collision, loki_api_v1_query_range
func TestExtractedSuffixStatsGrouping(t *testing.T) {
	for _, tt := range []struct{ logql, want string }{
		{`sum by (level_extracted) (count_over_time({app="api"} | logfmt [5m]))`,
			`| unpack_logfmt from _msg fields (level) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level` +
				` | format if (__lxp_stream:~"[{,]level=\"" __lxp_level:*) "<__lxp_level>" as level_extracted | delete __lxp* | stats by (level_extracted) count()`},
		{`sum by (level_extracted) (count_over_time({app="api"} | json [5m]))`,
			`| unpack_json from _msg fields (level) result_prefix "__lxp0_" | copy _stream as __lxp_stream | copy __lxp0_level as __lxp_level` +
				` | format if (__lxp_stream:~"[{,]level=\"" __lxp_level:*) "<__lxp_level>" as level_extracted | delete __lxp* | stats by (level_extracted) count()`},
		{`sum by (level_extracted) (count_over_time({app="api"} | logfmt | json [5m]))`,
			`| unpack_logfmt from _msg fields (level) result_prefix "__lxp0_" | unpack_json from _msg fields (level) result_prefix "__lxp1_"` +
				` | copy _stream as __lxp_stream | copy __lxp1_level as __lxp_level | format if (__lxp0_level:*) "<__lxp0_level>" as __lxp_level`},
	} {
		got, err := TranslateLogQLWithCapabilities(tt.logql, nil, nil, logsql.Capabilities{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, tt.want) {
			t.Errorf("TranslateLogQL(%s) = %s, want it to hold %s", tt.logql, got, tt.want)
		}
	}
}

// A grouping by a label a filter already computed reuses its scratch fields:
// one parser pipe and one scratch unpack per row, not one more per use.
//
// conformance: semantics/extracted-suffix-collision, loki_api_v1_query_range
func TestExtractedSuffixStatsReusesFilterScratch(t *testing.T) {
	got, err := TranslateLogQLWithCapabilities(`sum by (level_extracted) (count_over_time({app="api"} | json | level_extracted!="" [5m]))`, nil, nil, logsql.CapabilitiesFor("1.52.0"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(got, "unpack_json"); n != 2 {
		t.Errorf("%d unpack_json in %s, want the parser and one scratch unpack", n, got)
	}
	if n := strings.Count(got, "copy _stream"); n != 1 {
		t.Errorf("%d copy _stream in %s, want 1", n, got)
	}
	if strings.Count(got, "delete __lxp*") != 1 || strings.Index(got, "delete __lxp*") < strings.LastIndex(got, "as level_extracted") {
		t.Errorf("scratch fields must live until the grouping label is built: %s", got)
	}
}

// Only a bare parser pipe is a parser of the query: neither a scratch unpack
// nor the conditional unpacks of the detected_level chain add one.
//
// conformance: semantics/extracted-suffix-collision, loki_api_v1_query_range
func TestExtractedSuffixStatsCountsBareParsersOnly(t *testing.T) {
	query := `app:="api" | unpack_json | unpack_json if (-level:*) from _msg fields (level) result_prefix "__j_"` +
		` | unpack_logfmt if (-level:*) from _msg fields (level) result_prefix "__l_" | unpack_json from _msg fields (x) result_prefix "__lxp0_"`
	got := resolveStatsKeys(query, "level_extracted", "count()")
	if !strings.Contains(got, `| unpack_json from _msg fields (level) result_prefix "__lxp0_"`) || strings.Contains(got, "__lxp1_") || strings.Contains(got, "__lxp2_") {
		t.Errorf("want one scratch unpack for the one parser: %s", got)
	}
	if !strings.HasSuffix(got, "as level_extracted | delete __lxp*") {
		t.Errorf("grouping pipes: %s", got)
	}
}
