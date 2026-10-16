//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// levelKeysProbeLines are the lines the gate in front of the keys probe's
// two-keys patterns is run over, one stream each: lines the gate rejects,
// lines it lets through and the patterns reject, and lines the patterns match.
// risky is the verdict of the probe, which is the patterns' (and the other
// two branches') whatever the gate lets through.
var levelKeysProbeLines = []struct {
	name, msg string
	risky     bool
}{
	{"one JSON key", `{"level":"info","msg":"x"}`, false},
	{"one logfmt key", "ts=1 level=info msg=hi", false},
	{"JSON key twice", `{"level":"info","level":"warn"}`, true},
	{"JSON level and severity", `{"level":"info","severity":"warn"}`, true},
	{"JSON dotted key and SeverityText", `{"log.level":"info","SeverityText":"warn"}`, true},
	{"JSON nested key", `{"level":"a","k":{"level":"b"}}`, true},
	{"JSON Level and LEVEL", `{"Level":"a","LEVEL":"b"}`, true},
	{"JSON severity_text spellings", `{"severity_text":"a","Severity_Text":"b"}`, true},
	{"JSON severity_text and SEVERITY_TEXT", `{"severity_text":"a","SEVERITY_TEXT":"b"}`, true},
	{"JSON lvl and Lvl", `{"lvl":"a","Lvl":"b"}`, true},
	{"words in a message, no key twice", `{"msg":"the level was high","severity":"x"}`, false},
	{"two words, one key", `{"msg":"level level","level":"x"}`, false},
	{"key and a longer word", `{"level":"info","é":"level","k":"levels"}`, false},
	{"logfmt key twice", "level=info level=warn", true},
	{"logfmt tab separated", "level=info\tlvl=warn", true},
	{"logfmt newline separated", "level=info\nseverity=warn", true},
	{"logfmt second key in a quoted value", `level=info msg="level=debug"`, false},
	{"escape in a level value", `{"level":"a\u0041"}`, true},
	{"brace without its closing brace and a logfmt key", `{"level":"info"} tail level=1`, true},
}

// extractLevelTwiceGate returns the gate of a recorded keys probe: the
// parenthesised alternation that starts with its first sequence.
func extractLevelTwiceGate(t *testing.T, query string) (gate string, start int) {
	t.Helper()
	start = strings.Index(query, `(_msg:seq("`)
	if start < 0 {
		t.Fatalf("no twice gate in %s", query)
	}
	depth := 0
	for i := start; i < len(query); i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return query[start : i+1], start
			}
		}
	}
	t.Fatalf("unbalanced gate in %s", query)
	return "", 0
}

// ingestVictoriaLogsLines writes the lines of each app into VictoriaLogs only
// (one stream per app) and waits until they are searchable. The probes under
// test read nothing else, and a test that pushed them to Loki too would leave
// Loki's ingesters and index in a state the other tests of the suite do not
// expect (ingestJSONVolumeFixture flushes it).
func ingestVictoriaLogsLines(t *testing.T, apps map[string][]jsonVolumeStreamLine) {
	t.Helper()
	for app, lines := range apps {
		var rows strings.Builder
		for _, line := range lines {
			row, _ := json.Marshal(map[string]string{"_time": line.ts.UTC().Format(time.RFC3339Nano), "_msg": line.msg, "app": app})
			rows.Write(row)
			rows.WriteByte('\n')
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app", rows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
		}
	}
	forceVLFlush(t)
	deadline := time.Now().Add(60 * time.Second)
	for app, lines := range apps {
		for {
			form := url.Values{"query": {`app:=` + fmt.Sprintf("%q", app) + ` | stats count() as n`}, "start": {lines[0].ts.Add(-time.Hour).UTC().Format(time.RFC3339Nano)}, "end": {lines[0].ts.Add(time.Hour).UTC().Format(time.RFC3339Nano)}}
			_, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
			if strings.Contains(string(body), fmt.Sprintf(`"n":"%d"`, len(lines))) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d lines never became searchable in VictoriaLogs", app, len(lines))
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// The gate in front of the keys probe's two-keys patterns changes no verdict:
// the probe the proxy sent, and the same probe without the gate, give the same
// answer on each line of the fixture, one line to a stream, on this
// VictoriaLogs version. A verdict over the whole window would pass as soon as
// one risky line was in it.
// conformance: semantics/json-metric-detected-level-derivation
func TestRangeMetricCompatibilityDetectedLevelKeysGate(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	apps := make([]string, len(levelKeysProbeLines))
	fixture := map[string][]jsonVolumeStreamLine{}
	for i, line := range levelKeysProbeLines {
		apps[i] = fmt.Sprintf("level-keys-gate-%02d-%d", i, now.UnixNano())
		fixture[apps[i]] = []jsonVolumeStreamLine{{ts: s0.Add(10 * time.Second), msg: line.msg}}
	}
	ingestVictoriaLogsLines(t, fixture)
	start, end := s0, s0.Add(2*time.Minute)

	// The probe is whatever the proxy sends: captured for a line that clears
	// every guard, then asked of each stream with its base.
	route, rec := newJSONVolumeRouteProxy(t)
	rec.reset()
	query := `sum by (detected_level) (count_over_time({app="` + apps[0] + `"} | json | drop __error__ [1m]))`
	slidingRangeSeries(t, route, query, start, end, time.Minute, nil)
	var template string
	for _, call := range rec.reset() {
		if strings.HasPrefix(call, "/select/logsql/query ") && strings.Contains(call, "(?s:.*)") && strings.Contains(call, `(_msg:seq("`) {
			template = strings.TrimPrefix(call, "/select/logsql/query ")
		}
	}
	base := `app:="` + apps[0] + `"`
	if !strings.HasPrefix(template, base+" ") {
		t.Fatalf("keys probe not found among the calls, or it does not start with %s: %q", base, template)
	}
	gate, at := extractLevelTwiceGate(t, template)
	if !strings.Contains(template[at+len(gate):], "(?s:.*)") {
		t.Fatalf("the gate does not stand in front of the two-keys patterns: %s", template)
	}
	ungated := template[:at] + template[at+len(gate)+1:]
	if strings.Contains(ungated, "seq(") || strings.Count(ungated, "(?s:.*)") != 2 {
		t.Fatalf("the ungated probe lost more than the gate: %s", ungated)
	}

	asks := func(probe, app string) bool {
		probe = strings.Replace(probe, base, `app:="`+app+`"`, 1)
		form := url.Values{"query": {probe}, "start": {start.UTC().Format(time.RFC3339Nano)}, "end": {end.UTC().Format(time.RFC3339Nano)}}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
		if status != http.StatusOK {
			t.Fatalf("VictoriaLogs %d: %s", status, body)
		}
		return strings.TrimSpace(string(body)) != ""
	}
	risky := 0
	for i, line := range levelKeysProbeLines {
		t.Run(line.name, func(t *testing.T) {
			gated, plain := asks(template, apps[i]), asks(ungated, apps[i])
			if gated != plain {
				t.Errorf("gated probe says risky=%v, the ungated one %v, for %q", gated, plain, line.msg)
			}
			if gated != line.risky {
				t.Errorf("probe says risky=%v for %q, expected %v", gated, line.msg, line.risky)
			}
		})
		if line.risky {
			risky++
		}
	}
	if risky == 0 || risky == len(levelKeysProbeLines) {
		t.Fatalf("the fixture holds %d risky lines of %d", risky, len(levelKeysProbeLines))
	}
}
