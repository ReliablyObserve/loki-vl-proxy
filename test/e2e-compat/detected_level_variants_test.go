//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The pipes that derive detected_level in VictoriaLogs were made cheaper in
// three ways, each of which must give the answer of the pipes it replaced on
// every row, whatever the row holds: the keyword regexp runs behind a filter
// on the keyword words, the plan's own unpack_json is the chain's (the labels
// are copied from its prefixed fields), and the presence probe and witness find
// an escape with a substring filter. This test ingests generated rows into
// VictoriaLogs alone (no Loki: the answers against Loki are the probe tests')
// and runs the pipes as they were and as they are, per row.
// conformance: semantics/json-metric-detected-level-derivation

// variantLine is a generated row: the body and the stored fields besides it.
type variantLine struct {
	tag    string
	msg    string
	stored map[string]string
}

var variantBodies = []string{
	// the keyword scan: case, whole words, boundaries, several keywords
	"request ERROR upstream", "Error: connection refused", "eRrOr in module", "3 errors found", "terror alert",
	"disk warning: low", "warnings ignored", "info then error", "warning: error", "critical error", "error then critical",
	"foo-error", "foo_error", "5error", "error404", "error.", "err: timeout", "(warn) slow", `retry "debug" mode`,
	"x=trace on", "FATAL: out of memory", " error", "éerror", "errorж", "x\rerror", "İNFO ready", "hello world",
	"GET /api/v1/users 200 12ms", "Trace enter f", "[info] started", "{debug}", "a\terror", "a\nwarn", "x info y",
	// JSON bodies: a level key of every kind of value and name, the keyword scan behind them
	`{"level":"info"}`, `{"level":"INFO","event":"a"}`, `{"level":"Information"}`, `{"level":"warning"}`, `{"level":"ERR","event":"b"}`,
	`{"level":"notice"}`, `{"level":"notice","msg":"error connecting"}`, `{"level":30,"msg":"hi"}`, `{"level":50,"err":{"type":"Error"}}`,
	`{"level":true}`, `{"level":null}`, `{"level":" info"}`, `{"level":""}`, `{"level":{"name":"info"}}`, `{"level":["info"]}`,
	`{"severity":"ERROR"}`, `{"Level":"Information"}`, `{"LEVEL":"warn"}`, `{"log":{"level":"warn"}}`, `{"a":{"level":"error"}}`,
	`{"lvl":"dbg"}`, `{"SeverityText":"fatal"}`, `{"severity_text":"trc","event":"c"}`, `{"msg":"x","severity":"error","level":"info"}`,
	`{"level":"info","severity":"error"}`, `{"msg":"request failed with error"}`, `{"msg":"3 errors found"}`, `{"msg":"info then error"}`,
	`{"msg":"warning: error"}`, `{"msg":"ERROR"}`, `{"msg":"foo-error"}`, `{"n":1}`, `{"event":"d","n":2}`, `{"detected_level":"error","level":"info"}`,
	`{"detected_level":"verbose"}`, `{"level":"info"} trailing`, `{"level":"warn","x":{bad`, `{"level":"info","x":{bad`,
	`{"level":"warn"}`, `{"level":"Information"}`, `{"level":"info"}`, `{"msg":"error: cannot set level=debug now"}`,
	`{"msg":"set level=debug","level":"info"}`, `{"msg": "level: high", "level": "warn"}`, `  {"level":"error"}`,
	// logfmt bodies
	`level=warn msg=x`, `Level=info LEVEL=error`, `msg="error x" LvL=warn`, `msg="unterminated error level=info`, `{ error level=info`,
	`level=info event=e1`, `lvl=err event=e2`, `ts=1 level=debug`, `msg=hello`, `x=1 y=2`,
}

var variantStored = []map[string]string{
	nil, nil, nil, nil, nil, nil, nil, nil,
	{"level": "info"}, {"level": "WARN"}, {"level": "notice"}, {"Level": "info"}, {"LEVEL": "error"}, {"lvl": "debug"},
	{"severity": "warn"}, {"severity": "Info2"}, {"severity": "Unspecified"}, {"severity_text": "Unspecified", "severity_number": "0"},
	{"severity_text": "Info2", "severity_number": "10"}, {"severity_text": "Warn", "severity_number": "13"}, {"severity_number": "17"},
	{"severity_number": "30"}, {"severity_text": "notice", "severity_number": "0"}, {"detected_level": "INFO"},
	{"level": "info", "severity": "error"}, {"Level": "info", "level": "warn"},
}

// variantQuery runs query over the fixture's window on VictoriaLogs and returns
// the rows by tag as "level|event|detected_level".
func variantQuery(t *testing.T, query string, start, end time.Time) map[string]string {
	t.Helper()
	form := url.Values{"query": {query}, "start": {start.UTC().Format(time.RFC3339Nano)}, "end": {end.UTC().Format(time.RFC3339Nano)}}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs answered %d for %.300s: %.300s", status, query, body)
	}
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var row map[string]string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("row %q: %v", line, err)
		}
		out[row["tag"]] = row["level"] + "|" + row["event"] + "|" + row["detected_level"]
	}
	return out
}

func TestRangeMetricCompatibilityDetectedLevelPipeVariants(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-3 * time.Hour).Truncate(time.Minute)
	app := fmt.Sprintf("level-variants-%d", now.UnixNano())
	rng := rand.New(rand.NewSource(42))
	var lines []variantLine
	// every body with no stored field, then every body with random stored fields
	for i, body := range variantBodies {
		lines = append(lines, variantLine{tag: "a" + strconv.Itoa(i), msg: body})
	}
	for i := 0; i < 1400; i++ {
		lines = append(lines, variantLine{tag: "b" + strconv.Itoa(i), msg: variantBodies[rng.Intn(len(variantBodies))], stored: variantStored[rng.Intn(len(variantStored))]})
	}
	var rows strings.Builder
	stored, bodyKeys := map[string]bool{}, map[string]bool{}
	for i, l := range lines {
		fields := map[string]string{"_time": s0.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano), "_msg": l.msg, "app": app, "tag": l.tag}
		for k, v := range l.stored {
			fields[k] = v
			stored[k] = true
		}
		row, _ := json.Marshal(fields)
		rows.Write(row)
		rows.WriteByte('\n')
	}
	for _, key := range translator.DetectedLevelBodyKeys() {
		bodyKeys[key] = true // the chain pruned to what the fixture uses: every key (some line holds each of the words)
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app", rows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	start, end := s0.Add(-time.Minute), s0.Add(time.Duration(len(lines)+60)*time.Second)
	base := fmt.Sprintf("app:=%q", app)
	if got := variantQuery(t, base+` | stats by (tag) count() c`, start, end); len(got) != len(lines) {
		t.Fatalf("VictoriaLogs holds %d of %d rows", len(got), len(lines))
	}

	labels := []string{"level", "event"}
	const stats = " | stats by (tag, level, event, detected_level) count() c"
	// The pipes as they were: the plan's own unpack follows the chain, and the
	// keyword regexp is not gated.
	gateRE := regexp.MustCompile(`(extract_regexp if \([^)]*__dl:"") _msg:\((?:i\(\w+\)(?: OR )?)+\)\)`)
	ungated := func(chain string) string {
		out := gateRE.ReplaceAllString(chain, "$1)")
		if out == chain {
			t.Fatal("the chain has no keyword gate to take out")
		}
		return out
	}
	was := func(chain, planUnpack string) string {
		return base + " " + ungated(chain) + planUnpack + stats
	}
	var present *translator.DetectedLevelPresence
	{
		var st []string
		for _, name := range translator.DetectedLevelFieldNames {
			if stored[name] {
				st = append(st, name)
			}
		}
		present = &translator.DetectedLevelPresence{Stored: st, BodyKeys: translator.DetectedLevelBodyKeys()}
	}
	for _, tc := range []struct {
		name    string
		present *translator.DetectedLevelPresence
		cond    string // the condition of the plan's own unpack
	}{
		{"full chain", nil, ""},
		{"chain pruned to the stored fields of the fixture", present, ""},
		{"full chain, plan unpack on rows lacking a label", nil, "(-level:*) or (-event:*)"},
		{"pruned chain, plan unpack on rows lacking a label", present, "(-level:*) or (-event:*)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			existing := translator.DetectedLevelFieldNames
			planUnpack := " | unpack_json fields (level, event) keep_original_fields"
			if tc.cond != "" {
				planUnpack = " | unpack_json if (" + tc.cond + ") fields (level, event) keep_original_fields"
			}
			chain := translator.BuildDetectedLevelChain(existing, translator.DetectedLevelChainGrouping, tc.present)
			old := variantQuery(t, was(chain, planUnpack), start, end)
			gated := variantQuery(t, base+" "+chain+planUnpack+stats, start, end)
			shared := &translator.SharedUnpack{Keys: labels, Cond: tc.cond}
			sharedRows := variantQuery(t, base+" "+translator.BuildDetectedLevelChainShared(existing, translator.DetectedLevelChainGrouping, tc.present, shared)+stats, start, end)
			if len(old) != len(lines) || len(sharedRows) != len(lines) {
				t.Fatalf("rows: was %d, now %d, fixture %d", len(old), len(sharedRows), len(lines))
			}
			bad := 0
			for _, l := range lines {
				if old[l.tag] != sharedRows[l.tag] || old[l.tag] != gated[l.tag] {
					if bad++; bad <= 15 {
						t.Errorf("row %s %q stored %v: was %q, gated %q, shared %q", l.tag, l.msg, l.stored, old[l.tag], gated[l.tag], sharedRows[l.tag])
					}
				}
			}
			if bad > 0 {
				t.Fatalf("%d of %d rows differ", bad, len(lines))
			}
		})
	}
}

// The probe and the witness look for the two bytes backslash and u with a
// substring filter; it selects the lines the regular expression did.
func TestRangeMetricCompatibilityEscapeSubstringFilter(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("level-escape-%d", now.UnixNano())
	msgs := []string{`{"x":"Abc"}`, "no escape here", `u only \ then u`, `a\\u double`, `tail\u`, `U upper \U0041`, `x\ u`, `\u`, `u\`, `\\u`, "a/u", "é\\u00e9", `\u0000`, `p\\\u1`}
	var rows strings.Builder
	for i, m := range msgs {
		row, _ := json.Marshal(map[string]string{"_time": now.Add(-time.Duration(i+1) * time.Second).UTC().Format(time.RFC3339Nano), "_msg": m, "app": app, "tag": strconv.Itoa(i)})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app", rows.String(), map[string]string{"Content-Type": "application/stream+json"}); status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	tags := func(filter string) string {
		form := url.Values{"query": {fmt.Sprintf("app:=%q %s | fields tag", app, filter)}}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
		if status != http.StatusOK {
			t.Fatalf("%d %s", status, body)
		}
		var got []string
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if line == "" {
				continue
			}
			var row map[string]string
			_ = json.Unmarshal([]byte(line), &row)
			got = append(got, row["tag"])
		}
		sort.Strings(got)
		return strings.Join(got, ",")
	}
	var want []string
	for i, m := range msgs {
		if strings.Contains(m, `\u`) {
			want = append(want, strconv.Itoa(i))
		}
	}
	sort.Strings(want)
	byRegexp, substring := tags(`_msg:~"\\\\u"`), tags(`_msg:*"\\u"*`)
	if byRegexp != strings.Join(want, ",") || substring != byRegexp {
		t.Fatalf("regexp selects %q, substring %q, the lines holding a backslash and u are %q", byRegexp, substring, strings.Join(want, ","))
	}
}
