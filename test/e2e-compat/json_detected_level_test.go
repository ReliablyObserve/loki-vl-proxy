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
	{name: "stream detected_level wins over a stream level", labels: map[string]string{"detected_level": "INFO", "level": "warn"}, msg: `{"n":1}`, want: "INFO", route: "raw"},
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
	// A spelling the keyword scan cannot find (information is no whole word
	// of the scan): only the decoded key gives info, as in Loki v3.7.7.
	{name: "escaped level key, long spelling", msg: `{"\u006cevel":"Information"}`, want: "info", route: "stats"},
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
	// The keyword scan of Loki v3.7.7 (detectLevelFromLogLine): case, whole
	// words only, the first keyword by position, Loki's boundary bytes. A plain
	// line is no JSON, so only `| json | drop __error__` is valid for it.
	{name: "keyword in capitals", msg: "request ERROR upstream", want: "error", dropOnly: true, route: "stats"},
	{name: "keyword in title case", msg: "Error: connection refused", want: "error", dropOnly: true, route: "stats"},
	{name: "keyword in mixed case", msg: "eRrOr in module", want: "error", dropOnly: true, route: "stats"},
	{name: "plural is no keyword", msg: "3 errors found", want: "unknown", dropOnly: true, route: "stats"},
	{name: "keyword inside a word", msg: "terror alert", want: "unknown", dropOnly: true, route: "stats"},
	{name: "long spelling of warn", msg: "disk warning: low", want: "warn", dropOnly: true, route: "stats"},
	{name: "plural of warn", msg: "warnings ignored", want: "unknown", dropOnly: true, route: "stats"},
	{name: "two keywords, the first by position", msg: "info then error", want: "info", dropOnly: true, route: "stats"},
	{name: "two keywords, the long spelling first", msg: "warning: error", want: "warn", dropOnly: true, route: "stats"},
	{name: "critical before error", msg: "critical error", want: "critical", dropOnly: true, route: "stats"},
	{name: "error before critical", msg: "error then critical", want: "error", dropOnly: true, route: "stats"},
	{name: "hyphen before the keyword", msg: "foo-error", want: "unknown", dropOnly: true, route: "stats"},
	{name: "underscore before the keyword", msg: "foo_error", want: "unknown", dropOnly: true, route: "stats"},
	{name: "digit before the keyword", msg: "5error", want: "unknown", dropOnly: true, route: "stats"},
	{name: "digits after the keyword", msg: "error404", want: "unknown", dropOnly: true, route: "stats"},
	{name: "full stop after the keyword", msg: "error.", want: "unknown", dropOnly: true, route: "stats"},
	{name: "short spelling of error", msg: "err: timeout", want: "error", dropOnly: true, route: "stats"},
	{name: "keyword in brackets", msg: "(warn) slow", want: "warn", dropOnly: true, route: "stats"},
	{name: "keyword in quotes", msg: "retry \"debug\" mode", want: "debug", dropOnly: true, route: "stats"},
	{name: "keyword after an equals sign", msg: "x=trace on", want: "trace", dropOnly: true, route: "stats"},
	{name: "fatal in capitals", msg: "FATAL: out of memory", want: "fatal", dropOnly: true, route: "stats"},
	{name: "no-break space before the keyword", msg: "\u00a0error", want: "unknown", dropOnly: true, route: "stats"},
	{name: "letter before the keyword", msg: "éerror", want: "unknown", dropOnly: true, route: "stats"},
	{name: "letter after the keyword", msg: "errorж", want: "unknown", dropOnly: true, route: "stats"},
	{name: "carriage return before the keyword", msg: "x\rerror", want: "unknown", dropOnly: true, route: "stats"},
	{name: "plural in a JSON message", msg: `{"msg":"3 errors found"}`, want: "unknown", route: "stats"},
	{name: "two keywords in a JSON message", msg: `{"msg":"info then error"}`, want: "info", route: "stats"},
	{name: "long spelling first in a JSON message", msg: `{"msg":"warning: error"}`, want: "warn", route: "stats"},
	{name: "keyword alone in a JSON message", msg: `{"msg":"ERROR"}`, want: "error", route: "stats"},
	{name: "hyphen before the keyword in a JSON message", msg: `{"msg":"foo-error"}`, want: "unknown", route: "stats"},
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
					raw = raw || strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1") && !isLevelPresenceCall(call)
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
			stats, attempts, guards, raw := 0, 0, 0, 0
			for _, call := range calls {
				switch {
				case isStoredLevelAttempt(call):
					attempts++
				case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
					stats++
				case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"):
					guards++
				case isLevelPresenceCall(call):
				case strings.HasPrefix(call, "/select/logsql/query "):
					raw++
				}
			}
			// The stored-level attempt answers a window of stored levels (its
			// guards are the labels' only, none here), the chain the rest, guarded.
			if (attempts+stats != 1 && !(attempts == 1 && stats == 1)) || raw != 0 || (stats == 1 && guards == 0) {
				t.Errorf("expected one guarded stats request without raw rows: stored-level attempts=%d chain stats=%d guards=%d raw=%d", attempts, stats, guards, raw)
			}
		})
	}
}

// isLevelPresenceCall reports whether a recorded VictoriaLogs call is the
// presence probe that prunes the detected_level pipes: one stats query, no rows.
func isLevelPresenceCall(call string) bool {
	return strings.HasPrefix(call, "/select/logsql/query ") && strings.Contains(call, " | stats count() if (") && strings.Contains(call, " as p0, ")
}

// isStoredLevelAttempt reports whether a recorded call is the stats query a
// plan reading detected_level sends first: the buckets grouped by copies of the
// stored detected_level and level fields. It answers a window whose rows all
// carry a stored level; any other window asks again with the chain.
func isStoredLevelAttempt(call string) bool {
	return strings.HasPrefix(call, "/select/logsql/stats_query_range ") && strings.Contains(call, "copy detected_level as __dl_d, level as __dl_l")
}

// levelStatsOutcome returns the guard probes a plan reading detected_level may
// send, given the probes of its parsed labels (plain), the three of the chain
// (level), the stored-level attempts and the chain stats requests it made: the
// attempt alone answered (the labels' probes only), the attempt then the chain
// (both sets; the labels' probes ask once when the proxy remembers verdicts,
// as in production, and twice when it has no cache, as in these in-process
// proxies, which the unit tests count exactly), or the chain alone (a
// remembered verdict).
func levelStatsOutcome(plain, level, attempts, stats int) (lo, hi int, ok bool) {
	switch {
	case attempts == 1 && stats == 0:
		return plain, plain, true
	case attempts == 1 && stats == 1:
		return plain + level, 2*plain + level, true
	case attempts == 0 && stats == 1:
		return plain + level, plain + level, true
	}
	return 0, 0, false
}

// A plan that groups by `level` and detected_level unpacks `level` from the
// body, and the pipes that derive detected_level read the same line: they
// share one parse of it. A level stored under another name decides the row's
// detected_level, and the parsed `level` still counts as the label; a stored
// `level` keeps the label and the body's moves to level_extracted. Each line
// sits in a window with a second line no stored level decides, so that the
// derivation runs in VictoriaLogs, and the labels are Loki's.
// conformance: semantics/json-metric-detected-level-derivation
func TestRangeMetricCompatibilityJSONDetectedLevelSharedUnpack(t *testing.T) {
	type probe struct {
		name            string
		labels, meta    map[string]string
		msg             string
		level, detected string // "" level: the line has no level label
		otlp            bool
		severityNumber  int
		severityText    string
	}
	probes := []probe{
		{name: "stored severity, body level", meta: map[string]string{"severity": "warn"}, msg: `{"level":"error","n":1}`, level: "error", detected: "warn"},
		{name: "stored lvl, body level", meta: map[string]string{"lvl": "info"}, msg: `{"level":"error","n":1}`, level: "error", detected: "info"},
		{name: "stored Level, body level", meta: map[string]string{"Level": "info"}, msg: `{"level":"debug","n":1}`, level: "debug", detected: "info"},
		{name: "stored LEVEL, body level of another spelling", meta: map[string]string{"LEVEL": "WARN"}, msg: `{"level":"Information"}`, level: "Information", detected: "warn"},
		{name: "stream level, body level", labels: map[string]string{"level": "warn"}, msg: `{"level":"error","n":1}`, level: "warn", detected: "warn"},
		{name: "stored level, body level", meta: map[string]string{"level": "WARN"}, msg: `{"level":"info"}`, level: "WARN", detected: "warn"},
		{name: "stored severity, no body level", meta: map[string]string{"severity": "warn"}, msg: `{"n":1}`, detected: "warn"},
		{name: "body level only", msg: `{"level":"debug","n":1}`, level: "debug", detected: "debug"},
		{name: "body level of an unknown spelling, keyword in the line", msg: `{"level":"notice","msg":"error connecting"}`, level: "notice", detected: "error"},
		{name: "OTLP severity number, body level", otlp: true, severityNumber: 17, msg: `{"level":"debug","n":1}`, level: "debug", detected: "error"},
		{name: "OTLP severity number and text, body level", otlp: true, severityNumber: 13, severityText: "Warn", msg: `{"level":"info"}`, level: "info", detected: "warn"},
	}
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	apps := make([]string, len(probes))
	fixture := map[string][]jsonVolumeStreamLine{}
	var otlpApps []string
	for i, p := range probes {
		apps[i] = fmt.Sprintf("json-level-shared-%02d-%d", i, now.UnixNano())
		if p.otlp {
			otlpApps = append(otlpApps, apps[i])
			records := [][]byte{
				otlpLogRecord(s0.Add(10*time.Second), p.severityNumber, p.severityText, p.msg, nil),
				otlpLogRecord(s0.Add(11*time.Second), 0, "", `{"n":2}`, nil),
			}
			data := otlpLogsData(map[string]string{"service.name": apps[i]}, records)
			dlPost(t, lokiURL+"/otlp/v1/logs", "application/x-protobuf", data)
			dlPost(t, vlURL+"/insert/opentelemetry/v1/logs", "application/x-protobuf", data)
			continue
		}
		line := func(at time.Duration, msg string, withLabels bool) jsonVolumeStreamLine {
			l := jsonVolumeStreamLine{ts: s0.Add(at), msg: msg, labels: map[string]string{}}
			if withLabels {
				l.meta, l.vlMeta = p.meta, p.meta
				for name, value := range p.labels {
					if name == "level" {
						l.level = value
					} else {
						l.labels[name] = value
					}
				}
			}
			return l
		}
		// the probe's line and one no stored level decides, in separate streams
		fixture[apps[i]] = append(fixture[apps[i]], line(10*time.Second, p.msg, true), line(11*time.Second, `{"n":2}`, false))
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
	for i, p := range probes {
		selector := `{app="` + apps[i] + `"}`
		if p.otlp {
			selector = `{service_name="` + apps[i] + `"}`
		}
		query := `sum by (level, detected_level) (count_over_time(` + selector + ` | json | drop __error__ [1m]))`
		t.Run(p.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
			wantKey := "detected_level=" + p.detected
			if p.level != "" {
				wantKey = wantKey + ",level=" + p.level
			}
			if _, ok := loki[wantKey]; !ok || len(loki) != 2 {
				t.Fatalf("Loki answers %v, the table says {%s} and detected_level=unknown", loki, wantKey)
			}
			rec.reset()
			assertSlidingParity(t, query+" [in-process]", loki, slidingRangeSeries(t, route, query, start, end, time.Minute, nil))
			calls := rec.reset()
			assertSlidingParity(t, query+" [proxy]", loki, slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil))
			// One stats request carries the derivation and one parse of the line;
			// no raw rows are read.
			chain, raw := 0, 0
			for _, call := range calls {
				switch {
				case strings.HasPrefix(call, "/select/logsql/stats_query_range ") && !isStoredLevelAttempt(call):
					chain++
					if n := strings.Count(call, "unpack_json"); n != 1 {
						t.Errorf("%d unpack_json pipes in the chain request, want 1: %.300s", n, call)
					}
				case strings.HasPrefix(call, "/select/logsql/query ") && !strings.HasSuffix(call, " | limit 1") && !isLevelPresenceCall(call):
					raw++
				}
			}
			if chain != 1 || raw != 0 {
				t.Errorf("expected one chain stats request without raw rows: chain=%d raw=%d calls %q", chain, raw, calls)
			}
		})
	}
}
