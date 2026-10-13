//go:build e2e

package e2e_compat

import (
	"cmp"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// detected_level is the level Loki sets at ingest, normalized
// (distributor/field_detection.go): a level stream label, else a level key of
// the JSON body, whatever the spelling (info, INFO, inf, information). A
// `sum by (detected_level)` over `| json` is answered by the row evaluator, or
// by the stats pushdown with the `drop __error__` form, and both must group
// the spellings of one level into one series like Loki, with the level in the
// body, as a stream label, or both (the stream's wins). The pushdown computes
// the value in VictoriaLogs, the row evaluator in the proxy.
// conformance: semantics/json-metric-detected-level-derivation
func TestRangeMetricCompatibilityJSONDetectedLevel(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	body := fmt.Sprintf("json-level-body-%d", now.UnixNano())
	stream := fmt.Sprintf("json-level-stream-%d", now.UnixNano())
	both := fmt.Sprintf("json-level-both-%d", now.UnixNano())
	spellings := []string{"info", "INFO", "Info", "inf", "information", "warn", "WARN", "warning", "error", "ERR", "debug"}
	var bodyLines, streamLines, bothLines []jsonVolumeStreamLine
	for i := 0; i < 66; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		level := spellings[i%len(spellings)]
		other := spellings[(i+3)%len(spellings)]
		bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: ts, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, level, i)})
		streamLines = append(streamLines, jsonVolumeStreamLine{ts: ts, level: level, msg: fmt.Sprintf(`{"n":%d}`, i)})
		bothLines = append(bothLines, jsonVolumeStreamLine{ts: ts, level: level, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, other, i)})
	}
	// A line without any level, the last of each fixture.
	last := s0.Add(331 * time.Second)
	bodyLines = append(bodyLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	streamLines = append(streamLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	bothLines = append(bothLines, jsonVolumeStreamLine{ts: last, msg: `{"n":-1}`})
	ingestJSONVolumeFixture(t, map[string][]jsonVolumeStreamLine{body: bodyLines, stream: streamLines, both: bothLines})

	route, _ := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "in-process": route}
	start, end := s0.Add(-2*time.Minute), s0.Add(9*time.Minute)
	for _, tc := range []struct{ name, app string }{
		{"level in the body", body},
		{"level as a stream label with mixed spellings", stream},
		{"level in both, the stream's wins", both},
	} {
		for _, pipeline := range []string{`| json`, `| json | drop __error__`} {
			for _, function := range []string{"count_over_time", "bytes_over_time"} {
				query := `sum by (detected_level) (` + function + `({app="` + tc.app + `"} ` + pipeline + ` [1m]))`
				t.Run(tc.name+"/"+function+" "+pipeline, func(t *testing.T) {
					loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
					if len(loki) < 4 {
						t.Fatalf("Loki sanity: expected several detected_level series, got %d: %v", len(loki), loki)
					}
					for name, base := range targets {
						assertSlidingParity(t, query+" ["+name+"]", loki, slidingRangeSeries(t, base, query, start, end, time.Minute, nil))
					}
				})
			}
		}
	}
}

// detectedLevelProbe is one stored line and the detected_level Loki gives it
// (want, checked against Loki itself by the test). labels are further stream
// labels and meta structured metadata; dropOnly lines have a parser error, so
// only the `drop __error__` pipeline is valid for them. route, when set, is
// the evaluator the `drop __error__` query must use: "stats" for a line the
// VictoriaLogs pipes read like Loki, "raw" for one a guard probe hands to the
// raw-row evaluator.
type detectedLevelProbe struct {
	name     string
	labels   map[string]string
	meta     map[string]string
	msg      string
	want     string
	dropOnly bool
	route    string
	// rowOnly marks a line the stats pushdown reads differently from Loki
	// without a guard able to see it (semantics/detected-level-pipes-known-
	// differences): only the raw-row evaluator's form, `| json`, is compared.
	rowOnly bool
	// otlp lines are pushed as OTLP records (the severity VictoriaLogs stores
	// depends on its version: severity_text and severity_number from v1.50, one
	// severity field before) under the service name; severityNumber and
	// severityText are the record's.
	otlp           bool
	severityNumber int
	severityText   string
}

// selector returns the stream selector of the probe's app (an OTLP record
// carries the app as its service name).
func (c detectedLevelProbe) selector(app string) string {
	if c.otlp {
		return `{service_name="` + app + `"}`
	}
	return `{app="` + app + `"}`
}

var detectedLevelProbes = []detectedLevelProbe{
	{name: "pino numeric level", msg: `{"level":30,"msg":"hi"}`, want: "unknown", route: "stats"},
	{name: "pino numeric level with an error object", msg: `{"level":50,"msg":"boom","err":{"type":"Error"}}`, want: "error", route: "stats"},
	{name: "boolean level", msg: `{"level":true}`, want: "unknown"},
	{name: "null level", msg: `{"level":null}`, want: "unknown"},
	{name: "unknown spelling", msg: `{"level":"notice"}`, want: "unknown", route: "stats"},
	{name: "unknown spelling and an error word", msg: `{"level":"notice","msg":"error connecting"}`, want: "error", route: "stats"},
	{name: "padded level", msg: `{"level":" info"}`, want: "info", route: "stats"},
	{name: "severity key", msg: `{"severity":"ERROR"}`, want: "error", route: "stats"},
	{name: "Level key", msg: `{"Level":"Information"}`, want: "info", route: "stats"},
	{name: "LEVEL key", msg: `{"LEVEL":"warn"}`, want: "warn", route: "stats"},
	{name: "nested log.level", msg: `{"log":{"level":"warn"}}`, want: "warn", route: "stats"},
	{name: "no level key, error word", msg: `{"msg":"request failed with error"}`, want: "error", route: "stats"},
	{name: "object level", msg: `{"level":{"name":"info"}}`, want: "info"},
	{name: "array level", msg: `{"level":["info"]}`, want: "info"},
	{name: "body detected_level key", msg: `{"detected_level":"error","level":"info"}`, want: "info", route: "stats"},
	{name: "text after the closing brace", msg: `{"level":"info"} trailing`, want: "info", route: "raw"},
	{name: "no level at all", msg: `{"n":1}`, want: "unknown", route: "stats"},
	{name: "plain level key", msg: `{"level":"info"}`, want: "info", route: "stats"},
	{name: "severity before level", msg: `{"msg":"x","severity":"error","level":"info"}`, want: "error", route: "raw"},
	{name: "level before severity", msg: `{"level":"info","severity":"error"}`, want: "info", route: "raw"},
	{name: "body detected_level key alone", msg: `{"detected_level":"verbose"}`, want: "unknown", route: "stats"},
	{name: "level key mentioned in the message", msg: `{"level":"info","msg":"set log level=debug"}`, want: "info", route: "stats"},
	{name: "level name followed by a colon inside a string", msg: `{"msg":"level: high","level":"warn"}`, want: "warn", route: "stats"},
	{name: "syntax error after the level", msg: `{"level":"warn","x":{bad`, want: "warn", dropOnly: true, route: "raw"},
	{name: "body detected_level key, syntax error", msg: `{"detected_level":"warn","x":{bad`, want: "warn", dropOnly: true},
	{name: "body detected_level and level keys, syntax error", msg: `{"detected_level":"warn","level":"info","x":{bad`, want: "warn", dropOnly: true, route: "raw"},
	{name: "level key, syntax error", msg: `{"level":"info","x":{bad`, want: "info", dropOnly: true, route: "raw"},
	{name: "stream severity before stored level metadata", labels: map[string]string{"severity": "error"}, meta: map[string]string{"level": "info"}, msg: `{"n":1}`, want: "error", rowOnly: true},
	{name: "stored severity metadata", meta: map[string]string{"severity": "error"}, msg: `{"n":1}`, want: "error", route: "stats"},
	{name: "stream level, unknown spelling", labels: map[string]string{"level": "notice"}, msg: `{"n":1}`, want: "notice", route: "stats"},
	{name: "stream severity", labels: map[string]string{"severity": "error"}, msg: `{"n":1}`, want: "error", route: "stats"},
	{name: "stream level wins over the body", labels: map[string]string{"level": "INFO"}, msg: `{"level":"error"}`, want: "info", route: "stats"},
	{name: "stream detected_level keeps its stored spelling", labels: map[string]string{"detected_level": "INFO"}, msg: `{"n":1}`, want: "INFO", route: "raw"},
	{name: "stream detected_level wins over the body", labels: map[string]string{"detected_level": "Warning"}, msg: `{"level":"error"}`, want: "Warning", route: "raw"},
	{name: "stored level metadata", meta: map[string]string{"level": "WARN"}, msg: `{"level":"info"}`, want: "warn", route: "stats"},
	{name: "stored detected_level metadata", meta: map[string]string{"detected_level": "Error"}, msg: `{"level":"info"}`, want: "error", route: "stats"},
	// A line that is JSON for Loki (a brace first and last) never goes to the
	// logfmt parser; one that only starts with a brace is logfmt for it.
	{name: "logfmt-looking text inside a JSON message", msg: `{"msg":"error: cannot set level=debug now"}`, want: "error", route: "stats"},
	{name: "logfmt after a brace", msg: `{ error level=info`, want: "info", dropOnly: true, route: "raw"},
	{name: "logfmt level keys of two spellings", msg: `Level=info LEVEL=error`, want: "info", dropOnly: true, route: "raw"},
	// Loki reads the raw bytes of a JSON value, so an escaped spelling is no
	// known level for it; keys it decodes, as VictoriaLogs does.
	{name: "escaped level value", msg: `{"level":"\u0069nfo"}`, want: "unknown", route: "raw"},
	{name: "escaped level key", msg: `{"\u006cevel":"warn"}`, want: "warn", route: "stats"},
	// OTLP records. A severity text VictoriaLogs synthesised from the number
	// (Unspecified, Info2) is no level for Loki, which reads the number, or the
	// line when the record has none.
	{name: "OTLP without severity", otlp: true, msg: "hello", want: "unknown", dropOnly: true, route: "stats"},
	{name: "OTLP without severity, error word", otlp: true, msg: "request error", want: "error", dropOnly: true, route: "stats"},
	{name: "OTLP fine severity number", otlp: true, severityNumber: 10, msg: "hello", want: "info", dropOnly: true, route: "stats"},
	{name: "OTLP severity number", otlp: true, severityNumber: 13, msg: "hello", want: "warn", dropOnly: true, route: "stats"},
	{name: "OTLP severity text and number", otlp: true, severityNumber: 17, severityText: "Error", msg: "hello", want: "error", dropOnly: true, route: "stats"},
	{name: "OTLP own text", otlp: true, severityText: "notice", msg: "hello", want: "notice", dropOnly: true, route: "stats"},
	{name: "OTLP own text over its number", otlp: true, severityNumber: 9, severityText: "Fine", msg: "hello", want: "Fine", dropOnly: true, route: "stats"},
}

// Every shape of level a line can hold gets the detected_level Loki's ingest
// gave it, on both routes of `sum by (detected_level) (... | json ...)`: a
// level key that is not a known string spelling falls to a keyword scan of the
// whole line (pino's numeric levels, `notice`), the first level key in
// document order wins whatever its name (severity, Level, LEVEL, log.level),
// a stream label detected_level keeps its stored spelling, and a body key
// named detected_level never replaces the value. The proxy's two routes are
// compared with Loki's own answer, which must also be the one recorded in the
// table; the pushdown must serve the lines the VictoriaLogs pipes read like
// Loki and hand the others to the raw-row evaluator.
// conformance: semantics/json-metric-detected-level-derivation
func TestRangeMetricCompatibilityJSONDetectedLevelProbes(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	apps := make([]string, len(detectedLevelProbes))
	fixture := map[string][]jsonVolumeStreamLine{}
	var otlpApps []string
	for i, probe := range detectedLevelProbes {
		apps[i] = fmt.Sprintf("json-level-probe-%02d-%d", i, now.UnixNano())
		if probe.otlp {
			otlpApps = append(otlpApps, apps[i])
			var records [][]byte
			for k := 0; k < 2; k++ {
				records = append(records, otlpLogRecord(s0.Add(time.Duration(10+k)*time.Second), probe.severityNumber, probe.severityText, probe.msg, nil))
			}
			data := otlpLogsData(map[string]string{"service.name": apps[i]}, records)
			dlPost(t, lokiURL+"/otlp/v1/logs", "application/x-protobuf", data)
			dlPost(t, vlURL+"/insert/opentelemetry/v1/logs", "application/x-protobuf", data)
			continue
		}
		for k := 0; k < 2; k++ {
			line := jsonVolumeStreamLine{ts: s0.Add(time.Duration(10+k) * time.Second), msg: probe.msg, labels: map[string]string{}, meta: probe.meta, vlMeta: probe.meta}
			for name, value := range probe.labels {
				if name == "level" {
					line.level = value
				} else {
					line.labels[name] = value
				}
			}
			fixture[apps[i]] = append(fixture[apps[i]], line)
		}
	}
	ingestJSONVolumeFixture(t, fixture)
	start, end := s0, s0.Add(2*time.Minute)
	if len(otlpApps) > 0 {
		forceVLFlush(t)
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		for _, app := range otlpApps {
			dlWaitBothBackends(t, dlFixture{start: start, end: end}, `{service_name="`+app+`"}`, fmt.Sprintf(`{service.name=%q}`, app), 2)
		}
	}

	route, rec := newJSONVolumeRouteProxy(t)
	for i, probe := range detectedLevelProbes {
		for _, pipeline := range []string{`| json`, `| json | drop __error__`} {
			if probe.dropOnly && pipeline == `| json` || probe.rowOnly && pipeline != `| json` {
				continue
			}
			query := `sum by (detected_level) (count_over_time(` + probe.selector(apps[i]) + ` ` + pipeline + ` [1m]))`
			t.Run(probe.name+" "+pipeline, func(t *testing.T) {
				loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
				if len(loki) != 1 {
					t.Fatalf("Loki sanity: expected one detected_level series, got %v", loki)
				}
				for key := range loki {
					if key != "detected_level="+probe.want {
						t.Errorf("Loki answers {%s}, the table says detected_level=%s", key, probe.want)
					}
				}
				rec.reset()
				assertSlidingParity(t, query+" [in-process]", loki, slidingRangeSeries(t, route, query, start, end, time.Minute, nil))
				calls := rec.reset()
				assertSlidingParity(t, query+" [proxy]", loki, slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil))
				if pipeline != `| json | drop __error__` || probe.route == "" {
					return
				}
				stats, raw := false, false
				for _, call := range calls {
					stats = stats || strings.HasPrefix(call, "/select/logsql/stats_query_range ")
					raw = raw || strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1")
				}
				if probe.route == "stats" && (raw || !stats) || probe.route == "raw" && !raw {
					t.Errorf("route %q expected: stats call=%v raw rows read=%v; VictoriaLogs calls %q", probe.route, stats, raw, calls)
				}
			})
		}
	}

	// A filter on detected_level compares Loki's structured metadata of every
	// line, and Logs Drilldown's detected_level breakdown (not the native
	// histogram: the label translator maps detected_level to level) keeps its
	// guard. One selector holds the lines the VictoriaLogs pipes read like
	// Loki and one the lines they do not.
	var clean, risky, valid, otlp []string
	for i, probe := range detectedLevelProbes {
		if probe.otlp {
			otlp = append(otlp, apps[i])
			continue
		}
		switch {
		case probe.route == "stats":
			clean = append(clean, apps[i])
		case probe.route == "raw":
			risky = append(risky, apps[i])
		}
		if !probe.dropOnly {
			valid = append(valid, apps[i])
		}
	}
	drilldown := map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}
	for _, tc := range []struct {
		name     string
		selector []string
		pipeline string
		headers  map[string]string
		guarded  bool   // the selector holds only lines the pipes read like Loki
		label    string // the selector's label (default app)
	}{
		{"Drilldown breakdown, clean lines", clean, `| json | drop __error__, __error_details__ | detected_level != ""`, drilldown, true, ""},
		{"Drilldown breakdown, guarded lines", risky, `| json | drop __error__, __error_details__ | detected_level != ""`, drilldown, false, ""},
		{"equals, clean lines", clean, `| json | drop __error__ | detected_level="info"`, nil, true, ""},
		{"regexp, guarded lines", risky, `| json | drop __error__ | detected_level=~"warn|error|INFO"`, nil, false, ""},
		{"not equals, all lines", append(append([]string{}, clean...), risky...), `| json | drop __error__ | detected_level!="unknown"`, nil, false, ""},
		{"without the error drop, valid lines", valid, `| json | detected_level="info"`, nil, false, ""},
		{"error label, valid lines", valid, `| json | __error__="" | detected_level!=""`, nil, false, ""},
		{"equals, OTLP lines", otlp, `| json | drop __error__ | detected_level="info"`, nil, true, "service_name"},
		{"regexp, OTLP lines", otlp, `| json | drop __error__ | detected_level=~"warn|error|notice"`, nil, true, "service_name"},
		{"not equals, OTLP lines", otlp, `| json | drop __error__ | detected_level!="unknown"`, nil, true, "service_name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			label := cmp.Or(tc.label, "app")
			query := `sum by (detected_level) (count_over_time({` + label + `=~"` + strings.Join(tc.selector, "|") + `"} ` + tc.pipeline + ` [1m]))`
			loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, tc.headers)
			rec.reset()
			assertSlidingParity(t, query+" [in-process]", loki, slidingRangeSeries(t, route, query, start, end, time.Minute, tc.headers))
			calls := rec.reset()
			assertSlidingParity(t, query+" [proxy]", loki, slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, tc.headers))
			if !tc.guarded {
				return
			}
			stats, guards, raw := 0, 0, 0
			for _, call := range calls {
				switch {
				case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
					stats++
				case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"):
					guards++
				case strings.HasPrefix(call, "/select/logsql/query "):
					raw++
				}
			}
			if stats != 1 || guards == 0 || raw != 0 {
				t.Errorf("expected one guarded stats request without raw rows: stats=%d guards=%d raw=%d", stats, guards, raw)
			}
		})
	}
}
