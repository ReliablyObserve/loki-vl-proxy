package translator

import "testing"

// TestTranslateLogQL_BareFilterAfterPipeGetsFilterPrefix locks the | filter
// prefix on filters that follow a non-filter pipe stage. Without it every
// VictoriaLogs version rejects the query ("unexpected token after
// [unpack_json]", "cannot parse 'from' field name"), checked against v1.50.0,
// v1.51.0 and v1.52.0. | filter is accepted by every supported VictoriaLogs
// version, and a filter following a | filter stage stays an implicit AND
// inside it.
func TestTranslateLogQL_BareFilterAfterPipeGetsFilterPrefix(t *testing.T) {
	cases := []struct {
		logql string
		want  string
	}{
		// Line filters after parsers and formatting stages.
		{`{app="api"} |= "x" | json |= "y"`, `app:="api" ~"x" | unpack_json | filter ~"y"`},
		{`{app="api"} | logfmt |= "y" != "z"`, `app:="api" | unpack_logfmt | filter ~"y" NOT ~"z"`},
		{`{app="api"} | json |~ "y.*"`, `app:="api" | unpack_json | filter ~"y.*"`},
		{`{app="api"} | json |> "<_> err <_>"`, `app:="api" | unpack_json | filter ~".* err .*"`},
		{`{app="api"} | keep level |= "y"`, `app:="api" | fields _time, _msg, _stream, level | filter ~"y"`},
		{`{app="api"} | drop status |= "y" != "z"`, `app:="api" | delete status | filter ~"y" NOT ~"z"`},
		{`{app="api"} | line_format "{{.status}}" |= "y"`, `app:="api" | format "<status>" | filter ~"y"`},
		{`{app="api"} | decolorize |= "y"`, `app:="api" | decolorize | filter ~"y"`},
		{`{app="api"} | regexp "(?P<status>\\d+)" |= "y"`, `app:="api" | extract_regexp "(?P<status>\\d+)" | filter ~"y"`},
		{`{app="api"} | pattern "<_> <status> <_>" |= "y"`, `app:="api" | extract "<_> <status> <_>" | filter ~"y"`},
		// Empty-value label filters translate to -field:*, not a field:op filter.
		{`{app="api"} | json | level=""`, `app:="api" | unpack_json | filter -level:*`},
		{`{app="api"} | decolorize | level=""`, `app:="api" | decolorize | filter -level:*`},
		{`{app="api"} | drop status | level=""`, `app:="api" | delete status | filter -level:*`},
		// Already-valid forms keep their shape.
		{`{app="api"} | level="" | json`, `app:="api" -level:* | unpack_json`},
		{`{app="api"} | logfmt | status>200 |= "y" | level="x"`, `app:="api" | unpack_logfmt | filter status:>200 ~"y" | filter level:="x"`},
		{`{app="api"} | json | detected_level="error" |= "y"`, `app:="api" | unpack_json | filter level:="error" ~"y"`},
		{`{env="production"} | detected_level="error" !> "level=error msg=<_>"`, `env:="production" | unpack_logfmt | filter level:="error" NOT ~"level=error msg=.*"`},
	}
	for _, tc := range cases {
		t.Run(tc.logql, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("TranslateLogQL(%q): %v", tc.logql, err)
			}
			if got != tc.want {
				t.Fatalf("TranslateLogQL(%q)\n got: %s\nwant: %s", tc.logql, got, tc.want)
			}
		})
	}
}
