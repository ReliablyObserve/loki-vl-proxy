package translator

import (
	"strings"
	"testing"
)

// TestLock_PipelineAssembly_NoBareFilterAfterPipeStage pins the bug fix from
// commit «inserted on push» — the proxy used to emit malformed LogsQL when
// a label filter followed a pipe stage (`| line_format`, `| label_format`,
// `| format`, `| drop`, `| keep`, etc.) without an intervening parser.
//
// Reproducer (the user-reported case from a Tempo→Logs derived field):
//
//	LogQL : {service="x"} | line_format "<app>: <__line__>" | level="error"
//	Bug   : | format "<app>: <__line__>" level:="error" | sort by (_time desc)
//	        ^^^ implicit AND glued to format target — VL rejects with
//	        `unexpected token after [format ...]: "level"; expecting '|' or ')'`
//	Fix   : | format "<app>: <__line__>" | filter level:="error" | sort by (_time desc)
//
// The assembly site (translatePipelineStage) decides whether a bare filter
// composes via implicit-AND or via `| filter`:
//   - After stream selector only           → implicit AND (compact, preferred)
//   - After parser (json / logfmt / pattern) → `| filter <expr>`
//   - After PIPE STAGE without parser       → `| filter <expr>` (the fix)
//
// These cases are locked below. Any future change that re-introduces the
// glued-to-pipe-stage form will fail with a named test pointing at the
// original bug.
func TestLock_PipelineAssembly_NoBareFilterAfterPipeStage(t *testing.T) {
	cases := []struct {
		name           string
		logql          string
		mustContain    string
		mustNotContain []string
	}{
		{
			name:        "line_format then label filter — needs | filter",
			logql:       `{service_name="notification-service"} | line_format "<app>: <__line__>" | level="error"`,
			mustContain: `| filter level:="error"`,
			mustNotContain: []string{
				`__line__>" level:`, // the original bug — bare filter glued to format target
				`__line__>" -level:`,
			},
		},
		{
			name:        "label_format then label filter — needs | filter",
			logql:       `{service_name="x"} | label_format new_label="value" | new_label="value"`,
			mustContain: `| filter new_label:="value"`,
			mustNotContain: []string{
				`new_label="value" new_label:=`, // glued to label_format target
			},
		},
		{
			name:        "drop then label filter — needs | filter",
			logql:       `{service_name="x"} | drop env | level="error"`,
			mustContain: `| filter level:="error"`,
			mustNotContain: []string{
				`drop env level:`, // glued to drop list
			},
		},
		{
			name:        "keep then label filter — needs | filter",
			logql:       `{service_name="x"} | keep service_name,level | level="error"`,
			mustContain: `| filter level:="error"`,
			mustNotContain: []string{
				`keep service_name, level level:`, // glued to keep list
			},
		},
		{
			name:        "decolorize then label filter — needs | filter",
			logql:       `{service_name="x"} | decolorize | level="error"`,
			mustContain: `| filter level:="error"`,
			mustNotContain: []string{
				`decolorize level:`, // glued to decolorize
			},
		},
		{
			name:        "bare label filter directly after stream selector — implicit AND (no | filter)",
			logql:       `{app="api"} | status="200 OK"`,
			mustContain: `status:="200 OK"`,
			mustNotContain: []string{
				`| filter status:`, // no parser, no pipe stage → must use compact implicit-AND
			},
		},
		{
			name:        "json parser then label filter — | filter (parser-state behavior preserved)",
			logql:       `{app="api"} | json | service.name = "bar"`,
			mustContain: `| filter "service.name":="bar"`,
			mustNotContain: []string{
				`unpack_json "service.name":`, // missing | filter — pre-fix bug variant
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("translation failed: %v", err)
			}
			if !strings.Contains(got, tc.mustContain) {
				t.Errorf("expected output to contain %q\n  got: %q", tc.mustContain, got)
			}
			for _, banned := range tc.mustNotContain {
				if strings.Contains(got, banned) {
					t.Errorf("output unexpectedly contains banned substring %q\n  got: %q", banned, got)
				}
			}
		})
	}
}
