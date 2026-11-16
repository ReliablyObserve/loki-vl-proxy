package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// vlChainDetectedLevel is what the detected_level chain the proxy sends
// computes in VictoriaLogs for a row whose stream fields are stream and whose
// fields (the stream fields among them, VictoriaLogs holds them as fields too)
// are f: Loki's rule of the read path (deriveRowLevel), a non-stream
// detected_level, then the stream's level fields, then the row's, then the
// line. The emulator applies it where the chain is (vlemu.Emulator.DeriveFields);
// TestVLChainDetectedLevelMatchesVictoriaLogs pins it to VictoriaLogs.
func vlChainDetectedLevel(stream, f map[string]string) string {
	row := make(map[string]string, len(f))
	for k, v := range f {
		if sv, ok := stream[k]; !ok || sv != v {
			row[k] = v
		}
	}
	delete(row, "_msg")
	return deriveRowLevel(levelFieldsFromLabels(stream), levelFieldsFromLabels(row), []byte(f["_msg"]), false, true).String()
}

type vlChainRecording struct {
	Query string `json:"query"`
	Rows  []struct {
		Stream [][2]string `json:"stream"`
		Fields [][2]string `json:"fields"`
	} `json:"rows"`
	Answers map[string][]struct {
		Stream        string `json:"stream"`
		Level         string `json:"level"`
		DetectedLevel string `json:"detected_level"`
	} `json:"answers"`
}

// TestVLChainDetectedLevelMatchesVictoriaLogs: VictoriaLogs v1.49.0 and
// v1.53.0 (their own storage and query engine, test/oracle/vl store) run the
// proxy's detected_level chain over streams labelled level="ERROR",
// severity="error", LEVEL="warning", lvl="x", level="Debug" and Severity="fatal"
// and answer error, error, warn, x, debug and fatal: the chain reads every
// level field Loki reads and normalises the value. A stream level field wins
// over a level stored on the row, as in Loki (a stream labelled
// severity="error" whose row holds level="debug" is error, LEVEL="warning"
// over level="info" is warn, lvl="WRN" over level="error" is warn), a stored
// detected_level comes first, and a row with no level field reads its line
// (level=error is error). The emulator's rule
// (vlChainDetectedLevel) gives the same answer for every recorded row, so the
// tests that run on the emulator see what VictoriaLogs computes. The recorded
// query holds the chain the proxy sends today.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation
func TestVLChainDetectedLevelMatchesVictoriaLogs(t *testing.T) {
	raw, err := os.ReadFile("testdata/detected_level_vl_chain_answers.json")
	if err != nil {
		t.Fatal(err)
	}
	var rec vlChainRecording
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	chain := strings.TrimSpace(strings.TrimSuffix(fullLevelChain(), levelChainDelete))
	if !strings.Contains(rec.Query, chain) {
		t.Fatalf("the recorded query no longer holds the chain the proxy sends:\n%s", chain)
	}
	fields, streams := map[string]map[string]string{}, map[string]map[string]string{}
	for _, row := range rec.Rows {
		f, st := map[string]string{}, map[string]string{}
		var parts []string
		for _, kv := range row.Stream {
			f[kv[0]], st[kv[0]] = kv[1], kv[1]
			parts = append(parts, fmt.Sprintf("%s=%q", kv[0], kv[1]))
		}
		for _, kv := range row.Fields {
			f[kv[0]] = kv[1]
		}
		sort.Strings(parts)
		key := "{" + strings.Join(parts, ",") + "}"
		fields[key], streams[key] = f, st
	}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		answers := rec.Answers[version]
		if len(answers) != len(rec.Rows) {
			t.Fatalf("%s: %d answers for %d rows", version, len(answers), len(rec.Rows))
		}
		for _, a := range answers {
			f, ok := fields[a.Stream]
			if !ok {
				t.Fatalf("%s: answer for an unknown stream %s", version, a.Stream)
			}
			if got := vlChainDetectedLevel(streams[a.Stream], f); got != a.DetectedLevel {
				t.Errorf("%s %s: emulator rule %q, VictoriaLogs %q", version, a.Stream, got, a.DetectedLevel)
			}
		}
	}
}

// oneLevelStreams are streams whose own fields decide detected_level in Loki,
// with the value Loki gives (TestStreamDetectedLevelMatchesLokiGolden: live
// Loki answers level="Debug" with debug and lvl="x" with x, and normalises
// Warning to warn and ERR to error), plus a stream with no level field.
var oneLevelStreams = []struct {
	stream, want string
}{
	{`{app="uw",level="ERROR",pod="a"}`, "error"},
	{`{app="uw",pod="b",severity="error"}`, "error"},
	{`{LEVEL="warning",app="uw",pod="c"}`, "warn"},
	{`{app="uw",lvl="x",pod="d"}`, "x"},
	{`{app="uw",level="Debug",pod="e"}`, "debug"},
	{`{app="uw",level="info",pod="f"}`, "info"},
	{`{app="uw",ns="n",pod="g"}`, ""}, // no level field: the line bodies decide in Loki
}

func oneLevelRows(json bool) []vlemu.Row {
	base := int64(1_800_000_000) * 1e9
	var rows []vlemu.Row
	for i := 0; i < 20*len(oneLevelStreams); i++ {
		msg := fmt.Sprintf("amount=%d id=%d", i%9+1, i)
		if json {
			msg = fmt.Sprintf(`{"amount":%d,"id":%d}`, i%9+1, i)
		}
		rows = append(rows, vlemu.Row{TS: base - int64(i)*7e9 - 3e8, Stream: oneLevelStreams[i%len(oneLevelStreams)].stream, Value: float64(i%9 + 1), Msg: msg})
	}
	return rows
}

// oneLevelStreamOf returns the stream a series was built from: the one whose
// labels, pod aside when the query removed it, all appear on the series.
func oneLevelStreamOf(t *testing.T, series map[string]string) (map[string]string, string) {
	t.Helper()
	var found map[string]string
	want := ""
	for _, s := range oneLevelStreams {
		labels := parseStreamLabels(s.stream)
		match := true
		for k, v := range labels {
			if k == "pod" {
				if got, ok := series["pod"]; ok && got != v {
					match = false
				}
				continue
			}
			if series[k] != v {
				match = false
			}
		}
		if match {
			if found != nil {
				t.Fatalf("series %v matches two streams", series)
			}
			found, want = labels, s.want
		}
	}
	if found == nil {
		t.Fatalf("series %v matches no stream", series)
	}
	return found, want
}

// TestOneDetectedLevelPerStreamAcrossQueryTypes: Loki labels every line of a
// stream whose own fields hold a level with one detected_level (its ingest
// rule), so every metric series built from that stream carries the same
// value, whatever the query type or the step. The proxy builds these series
// on several paths: VictoriaLogs' detected_level chain (plain range
// functions), the bare-parser stats read by stream, the raw-row reads of
// `| json`, the per-stream reads of without folds, sort and topk, and the
// stats answers relabelled by stream (unwrap at a step the range divides).
// The proxy-side paths copied the level label as stored (ERROR, Debug) and
// ignored the other level fields (severity, LEVEL, lvl). Every path now gives
// the stream's normalised level, at an instant and at steps 60 and 300, on
// VictoriaLogs v1.49.0 and v1.53.0; the level label keeps its stored value, as
// in Loki. A stream with no level field never gains another value than
// unknown.
//
// conformance: semantics/detected-level-one-value-per-stream, metric-series-identity, severity-detected-level-derivation
func TestOneDetectedLevelPerStreamAcrossQueryTypes(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	queries := []string{
		`rate({app="uw"}[1m])`,
		`count_over_time({app="uw"}[5m])`,
		`count_over_time({app="uw"} | logfmt [5m])`,
		`bytes_over_time({app="uw"} | logfmt [5m])`,
		`rate({app="uw"} | json [1m])`,
		`sort(rate({app="uw"} | json [1m]))`,
		`sort_desc(count_over_time({app="uw"} | logfmt [5m]))`,
		`label_replace(rate({app="uw"} | json [1m]), "x", "$1", "pod", "(.*)")`,
		`topk(3, count_over_time({app="uw"} | logfmt [5m]))`,
		`sum without (pod) (count_over_time({app="uw"}[5m]))`,
		`sum without (pod) (rate({app="uw"} | json amount | unwrap amount [5m]))`,
		`max_over_time({app="uw"} | logfmt amount | unwrap amount [5m])`,
		`count_over_time({app="uw"} | logfmt [5m]) > 0`,
	}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, q := range queries {
			rows := oneLevelRows(strings.Contains(q, "json"))
			for _, mode := range []string{"instant", "60", "300"} {
				t.Run(version+"/"+mode+"/"+q, func(t *testing.T) {
					got := oneLevelSeries(t, rows, version, q, base, mode)
					if len(got) == 0 {
						t.Fatal("no series")
					}
					for _, s := range got {
						stream, want := oneLevelStreamOf(t, s)
						dl, has := s["detected_level"]
						if want == "" {
							if has && dl != levelUnknown {
								t.Fatalf("series %v of a stream without a level field: detected_level %q", s, dl)
							}
							continue
						}
						if !has || dl != want {
							t.Fatalf("series %v: detected_level %q (present %v), want %q", s, dl, has, want)
						}
						if v, ok := stream["level"]; ok && s["level"] != v {
							t.Fatalf("series %v: level %q, want the stored %q", s, s["level"], v)
						}
					}
				})
			}
		}
		// by (detected_level): one series per level value, the same values.
		q := `sum by (detected_level) (count_over_time({app="uw"}[5m]))`
		for _, mode := range []string{"instant", "60", "300"} {
			got := oneLevelSeries(t, oneLevelRows(false), version, q, base, mode)
			var levels []string
			for _, s := range got {
				levels = append(levels, s["detected_level"])
			}
			sort.Strings(levels)
			if want := []string{"debug", "error", "info", "unknown", "warn", "x"}; fmt.Sprint(levels) != fmt.Sprint(want) {
				t.Errorf("%s %s %s: %v, want %v", version, mode, q, levels, want)
			}
		}
	}
}

// oneLevelSeries runs query against the emulator, the detected_level chain
// computed as VictoriaLogs does (vlChainDetectedLevel).
func oneLevelSeries(t *testing.T, rows []vlemu.Row, version, query string, base int64, mode string) []map[string]string {
	t.Helper()
	return streamSeriesLabelsWith(t, &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: version, DeriveFields: vlChainDetectedLevel, RowStreamFields: true}, query, base, mode)
}

// TestEnsureStreamDetectedLevelEdges: the stream rule on one series' labels.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation
func TestEnsureStreamDetectedLevelEdges(t *testing.T) {
	for _, c := range []struct {
		name          string
		labels        map[string]string
		stream        map[string]string
		want          string // "-" for absent
		wantLevel     string // "-" for absent
		wantUntouched bool
	}{
		{"stream level normalised", map[string]string{"level": "ERROR"}, map[string]string{"level": "ERROR"}, "error", "ERROR", false},
		{"live Loki: Debug is debug", map[string]string{"level": "Debug"}, map[string]string{"level": "Debug"}, "debug", "Debug", false},
		{"live Loki: lvl x kept", map[string]string{"lvl": "x"}, map[string]string{"lvl": "x"}, "x", "-", false},
		{"severity", map[string]string{"severity": "error"}, map[string]string{"severity": "error"}, "error", "-", false},
		{"LEVEL warning", map[string]string{"LEVEL": "warning"}, map[string]string{"LEVEL": "warning"}, "warn", "-", false},
		{"Loki's field order: level before severity", map[string]string{}, map[string]string{"level": "info", "severity": "error"}, "info", "-", false},
		{"Loki oracle: Severity before lvl", map[string]string{}, map[string]string{"lvl": "debug", "Severity": "fatal"}, "fatal", "-", false},
		{"the log read path's rule (deriveRowLevel): the stream's level field before the row's", map[string]string{"level": "debug"}, map[string]string{"severity": "error"}, "error", "debug", false},
		{"row level field normalised (live Loki: Warning is warn)", map[string]string{"level": " Warning "}, map[string]string{"app": "a"}, "warn", " Warning ", false},
		{"unknown word kept", map[string]string{"level": "notice"}, map[string]string{"level": "notice"}, "notice", "notice", false},
		{"present detected_level stays", map[string]string{"detected_level": "warn", "level": "error"}, map[string]string{"level": "error"}, "warn", "error", false},
		{"no level anywhere: absent", map[string]string{"app": "a"}, map[string]string{"app": "a"}, "-", "-", false},
		{"blank level: absent", map[string]string{"level": "  "}, nil, "-", "  ", false},
		{"nil labels", nil, map[string]string{"level": "error"}, "-", "-", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ensureStreamDetectedLevel(c.labels, c.stream)
			if c.wantUntouched {
				if c.labels != nil {
					t.Fatalf("labels %v", c.labels)
				}
				return
			}
			got, ok := c.labels["detected_level"]
			if c.want == "-" {
				if ok {
					t.Fatalf("detected_level %q, want absent", got)
				}
			} else if got != c.want {
				t.Fatalf("detected_level %q, want %q", got, c.want)
			}
			level, ok := c.labels["level"]
			if c.wantLevel == "-" {
				if ok {
					t.Fatalf("level %q appeared", level)
				}
			} else if level != c.wantLevel {
				t.Fatalf("level %q, want the stored %q", level, c.wantLevel)
			}
		})
	}
}

// TestLevelGroupingApplyStreamRule: a stats answer relabelled by stream takes
// detected_level by the stream rule; an aggregated answer keeps the mirror of
// the level column it grouped by, and an answer VictoriaLogs derived keeps it.
//
// conformance: semantics/detected-level-one-value-per-stream, metric-series-identity
func TestLevelGroupingApplyStreamRule(t *testing.T) {
	stream := map[string]string{"app": "a", "severity": "error"}
	labels := map[string]string{"app": "a", "severity": "error"}
	levelGroupingRequest{}.apply(labels, map[string]string{}, stream, true)
	if labels["detected_level"] != "error" {
		t.Fatalf("stream answer: %v", labels)
	}
	labels = map[string]string{"app": "a", "level": "ERROR"}
	levelGroupingRequest{}.apply(labels, map[string]string{}, map[string]string{"app": "a", "level": "ERROR"}, true)
	if labels["detected_level"] != "error" || labels["level"] != "ERROR" {
		t.Fatalf("stream answer: %v", labels)
	}
	labels = map[string]string{"app": "a", "detected_level": "", "level": "ERROR"}
	levelGroupingRequest{}.apply(labels, map[string]string{}, map[string]string{"app": "a", "level": "ERROR"}, true)
	if v, ok := labels["detected_level"]; !ok || v != "" {
		t.Fatalf("a derived (removed) detected_level was filled: %v", labels)
	}
	// No-change guard, not a Loki answer: an aggregated answer without a
	// stream keeps the mirror of the level column it grouped by. Every route
	// that groups by detected_level sends the chain, so VictoriaLogs returns the
	// derived (normalised) label and this mirror does not run for them (the
	// diff of 7,800 requests behind TestStreamLevelSeriesFenceCoversParserShapes
	// left every `sum by (detected_level)` form unchanged, and
	// TestOneDetectedLevelPerStreamAcrossQueryTypes checks its six values).
	labels = map[string]string{"level": "warn"}
	levelGroupingRequest{known: true, detectedLevel: true}.apply(labels, map[string]string{"level": "warn"}, nil, false)
	if labels["detected_level"] != "warn" || labels["level"] != "" {
		t.Fatalf("aggregated by (detected_level) answer: %v", labels)
	}
}

// FuzzEnsureStreamDetectedLevel: on any stream of two labels and any level
// value the label is the stream's level field earliest in levelFieldNames (the
// reference walks the 14 names in order), else the level value, normalised; a
// present detected_level and the level label are never changed.
func FuzzEnsureStreamDetectedLevel(f *testing.F) {
	for _, s := range [][5]string{{"level", "ERROR", "", "", ""}, {"severity", "Warning", "lvl", "x", "debug"}, {"lvl", "x", "Severity", "fatal", ""}, {"app", "a", "pod", "b", " INFO "}, {"Severity", "", "LEVEL", "warn", "err"}, {"log.level", "Dbg", "SEVERITY_TEXT", "info", ""}} {
		f.Add(s[0], s[1], s[2], s[3], s[4], "")
	}
	f.Add("level", "error", "", "", "", "warn")
	f.Fuzz(func(t *testing.T, key, value, key2, value2, level, present string) {
		stream := map[string]string{key: value, key2: value2}
		labels := map[string]string{}
		if level != "" {
			labels["level"] = level
		}
		if present != "" {
			labels["detected_level"] = present
		}
		ensureStreamDetectedLevel(labels, stream)
		if level != "" && labels["level"] != level {
			t.Fatalf("level %q changed to %q", level, labels["level"])
		}
		ref, refOK := "", false
		for _, name := range levelFieldNames {
			if v := stream[name]; v != "" {
				ref, refOK = normalizedLevel(v), true
				break
			}
		}
		got, ok := labels["detected_level"]
		switch {
		case strings.TrimSpace(present) != "":
			if got != present {
				t.Fatalf("present %q changed to %q", present, got)
			}
		case refOK:
			if got != ref {
				t.Fatalf("stream %v: %q, the first level field gives %q", stream, got, ref)
			}
		case strings.TrimSpace(level) != "":
			if got != normalizedLevel(strings.TrimSpace(level)) {
				t.Fatalf("level %q: %q", level, got)
			}
		case present == "" && ok:
			t.Fatalf("no level anywhere, detected_level %q", got)
		}
	})
}

// streamSeriesLabelsWith is streamSeriesLabels over a given emulator: the
// label sets of query's answer, at an instant (mode "instant") or on
// query_range at step mode, sorted.
func streamSeriesLabelsWith(t *testing.T, emu *vlemu.Emulator, query string, base int64, mode string) []map[string]string {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if mode == "instant" {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(base/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(base/1e9-900, 10))
		params.Set("end", strconv.FormatInt(base/1e9, 10))
		params.Set("step", mode)
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	out := make([]map[string]string, 0, len(resp.Data.Result))
	for _, s := range resp.Data.Result {
		out = append(out, s.Metric)
	}
	sortLabelSets(out)
	return out
}

// TestStreamLevelSeriesFenceCoversParserShapes: a diff of range functions x
// pipelines x outer forms x streams keyed level="ERROR", level="Debug",
// severity, LEVEL, lvl and none, at an instant and at steps 60 and 300,
// against the previous build changed label sets for queries with a parser
// stage (json, logfmt, unpack, regexp, pattern) or an unwrap; the chain now
// reading a stream's level fields first changes the answers of every
// chain-served metric query over a stream whose rows store another level. So
// every metric query is fenced, in every outer form and pipeline, and a log
// query that names no detected_level keeps its key.
//
// conformance: semantics/detected-level-one-value-per-stream, cache-response-key-versions
func TestStreamLevelSeriesFenceCoversParserShapes(t *testing.T) {
	outers := []string{"%s", "sum without (pod) (%s)", "max without (pod) (%s)", "sum without () (%s)", "topk(1, %s)", "bottomk(1, %s)", "sort(%s)", "sort_desc(%s)", `label_replace(%s, "x", "$1", "pod", "(.*)")`, "sum by (app) (%s)", "sum(%s)", "sum by (detected_level) (%s)", "sum by (level) (%s)", "count(%s)", "avg by (pod) (%s)", "%s > 0", "%s * 2", "-%s"}
	pipelines := []string{" | logfmt", " | json", " | logfmt amount", " | json amount", " | logfmt | amount > 2", ` | regexp "amount=(?P<amt>[0-9]+)"`, ` | pattern "amount=<amt> <_>"`, " | unpack", " | logfmt | label_format x=amount", " | logfmt | drop id", " | logfmt | keep app, pod, level", " |logfmt", " |\tjson", " | json\n", " | JSON", " |\u00a0Logfmt", " | Pattern \"amount=<amt> <_>\"", " |\vunpack",
		"", ` |= "amount"`, ` | line_format "{{.amount}}"`, " | decolorize", ` |= "| json"`, ` | label_format jsonx="a"`, " | drop logfmt_x"}
	for _, pl := range pipelines {
		for _, fn := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
			for _, o := range outers {
				if q := fmt.Sprintf(o, fmt.Sprintf(`%s({app="uw"}%s [5m])`, fn, pl)); streamLevelSeriesResponseKeyVersion(q) == "" {
					t.Errorf("not fenced: %q", q)
				}
			}
		}
	}
	for _, q := range []string{`{app="a"} | json`, `{app="a"} | logfmt | level="error"`, `{app="a"} |= "x"`} {
		if streamLevelSeriesResponseKeyVersion(q) != "" {
			t.Errorf("log query fenced: %q", q)
		}
	}
	if streamLevelSeriesResponseKeyVersion(`{app="a"} | detected_level="error"`) == "" {
		t.Error("a detected_level filter is not fenced")
	}
}

// TestStreamLevelOneValueFence: answers the previous build cached for a query
// whose series it built per stream on a proxy-side path (stored under the v1
// fence, or under no fence) are never served; the fresh answer is cached
// under the v2 key, so a repeat makes no VictoriaLogs call.
//
// conformance: semantics/detected-level-one-value-per-stream, cache-response-key-versions
func TestStreamLevelOneValueFence(t *testing.T) {
	for _, c := range []struct {
		query string
		old   string // the fence the previous build put into the key
	}{
		{`count_over_time({app="uw"} | logfmt [5m])`, ""},
		{`sort(rate({app="uw"} | json [1m]))`, "@stream-level-series-v1:"},
		{`sum without (pod) (rate({app="uw"} | json amount | unwrap amount [5m]))`, "@stream-level-series-v1:"},
	} {
		t.Run(c.query, func(t *testing.T) {
			rows := oneLevelRows(strings.Contains(c.query, "json"))
			emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0", DeriveFields: vlChainDetectedLevel}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			target := "/loki/api/v1/query_range?" + url.Values{"query": {c.query}, "start": {"1799999100"}, "end": {"1800000000"}, "step": {"300"}}.Encode()
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			key, ok := p.compatCacheKey("query_range", req)
			if !ok || !strings.Contains(key, streamLevelSeriesResponseVersion) {
				t.Fatalf("compat key %q lacks the fence", key)
			}
			for name, k := range map[string]string{"inner": p.queryRangeCacheKey(req, c.query), "steps": p.queryStepsBaseKey(req, c.query)} {
				if !strings.Contains(k, streamLevelSeriesResponseVersion) {
					t.Errorf("%s key %q lacks the fence", name, k)
				}
			}
			const stale = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"uw","level":"ERROR","stale":"yes"},"values":[[1800000000,"1"]]}]}}`
			p.compatCache.Set(strings.Replace(key, streamLevelSeriesResponseVersion, c.old, 1), []byte(stale))
			rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "stale") || !strings.Contains(rec.Body.String(), `"detected_level":"error"`) {
				t.Fatalf("%d %s: want the fresh answer, not the cached one", rec.Code, rec.Body.String())
			}
			before := len(emu.Calls)
			if again := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); again.Body.String() != rec.Body.String() || len(emu.Calls) != before {
				t.Fatalf("repeat: %d new VictoriaLogs calls, same body %v", len(emu.Calls)-before, again.Body.String() == rec.Body.String())
			}
		})
	}
}

// TestStreamLevelChainFence: VictoriaLogs' detected_level chain now reads a stream's level fields before the ones
// stored on its rows, which changes the answer of every chain-served query over such a stream (a parser-less rate,
// a by (detected_level) count, a `| detected_level` filter). An answer the previous build cached under the key
// without the fence (these queries had none) is never served; the fresh answer, debug no more but the stream's error, is cached under
// the new key, so a repeat makes no VictoriaLogs call. A log query that names no detected_level keeps its key.
//
// conformance: semantics/detected-level-one-value-per-stream, cache-response-key-versions
func TestStreamLevelChainFence(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	var rows []vlemu.Row
	for i := 0; i < 40; i++ {
		rows = append(rows, vlemu.Row{TS: base - int64(i)*7e9 - 3e8, Stream: `{app="uw",pod="m",severity="error"}`, Labels: map[string]string{"level": "debug"}, Value: 1, Msg: "plain"})
	}
	for _, q := range []string{`rate({app="uw"}[1m])`, `sum by (detected_level) (count_over_time({app="uw"}[5m]))`} {
		t.Run(q, func(t *testing.T) {
			if streamLevelSeriesResponseKeyVersion(q) != streamLevelSeriesResponseVersion {
				t.Fatalf("not fenced")
			}
			emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0", DeriveFields: vlChainDetectedLevel, RowStreamFields: true}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			target := "/loki/api/v1/query_range?" + url.Values{"query": {q}, "start": {"1799999100"}, "end": {"1800000000"}, "step": {"300"}}.Encode()
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set("X-Scope-OrgID", "0")
			key, ok := p.compatCacheKey("query_range", req)
			if !ok || !strings.Contains(key, streamLevelSeriesResponseVersion) {
				t.Fatalf("compat key %q lacks the fence", key)
			}
			const stale = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"uw","detected_level":"debug","stale":"yes"},"values":[[1800000000,"1"]]}]}}`
			p.compatCache.Set(strings.Replace(key, streamLevelSeriesResponseVersion, "", 1), []byte(stale))
			rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
			if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "stale") || !strings.Contains(rec.Body.String(), `"detected_level":"error"`) || strings.Contains(rec.Body.String(), `"detected_level":"debug"`) {
				t.Fatalf("%d %s: want the fresh answer, error, not the cached one", rec.Code, rec.Body.String())
			}
			before := len(emu.Calls)
			if again := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); again.Body.String() != rec.Body.String() || len(emu.Calls) != before {
				t.Fatalf("repeat: %d new VictoriaLogs calls, same body %v", len(emu.Calls)-before, again.Body.String() == rec.Body.String())
			}
		})
	}
	for q, fenced := range map[string]bool{`{app="uw"}`: false, `{app="uw"} |= "x"`: false, `{app="uw"} | json`: false, `{app="uw"} | detected_level="error"`: true, `{app="uw"} |= "[x"`: false, `{app="uw"} | json | keep detected_level`: true} {
		if got := streamLevelSeriesResponseKeyVersion(q) != ""; got != fenced {
			t.Errorf("%q: fenced %v, want %v", q, got, fenced)
		}
	}
}

// TestBuildMetricSeriesEntryLevelRule: the series of a raw row or of a stats group read by stream takes Loki's
// detected_level when VictoriaLogs did not derive it. VictoriaLogs answers a raw row with its stream fields as fields,
// so the row's level of a stream labelled level="ERROR" is ERROR: the stream's level fields decide first, normalised,
// then the row's level field, normalised, then unknown. A value VictoriaLogs derived is kept (an empty one removes
// the label), a detected_level stream label stays, and the level label keeps the row's stored value.
//
// conformance: semantics/detected-level-one-value-per-stream, metric-series-identity
func TestBuildMetricSeriesEntryLevelRule(t *testing.T) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	str := func(s string) *string { return &s }
	for _, c := range []struct {
		name, stream, level string
		derived             *string
		want, wantLevel     string // "-" absent
	}{
		{"stream level ERROR, the row holds it", `{app="a",level="ERROR"}`, "ERROR", nil, "error", "ERROR"},
		{"stream level Debug", `{app="a",level="Debug"}`, "Debug", nil, "debug", "Debug"},
		{"stream severity beats the row's level", `{app="a",severity="error"}`, "debug", nil, "error", "debug"},
		{"stream LEVEL", `{LEVEL="warning",app="a"}`, "", nil, "warn", "-"},
		{"stream lvl unknown word", `{app="a",lvl="x"}`, "", nil, "x", "-"},
		{"row level only", `{app="a"}`, "Warning", nil, "warn", "Warning"},
		{"row level unknown word", `{app="a"}`, "notice", nil, "notice", "notice"},
		{"nothing", `{app="a"}`, "", nil, "unknown", "-"},
		{"a detected_level stream label stays", `{app="a",detected_level="fatal"}`, "", nil, "fatal", "-"},
		{"VictoriaLogs derived", `{app="a",level="ERROR"}`, "ERROR", str("error"), "error", "ERROR"},
		{"VictoriaLogs removed it", `{app="a",level="ERROR"}`, "ERROR", str(""), "-", "ERROR"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := p.buildMetricSeriesEntry(c.stream, c.level, c.derived, nil, false, nil)
			got, ok := e.metricLabels["detected_level"]
			if !ok {
				got = "-"
			}
			lvl, ok := e.metricLabels["level"]
			if !ok {
				lvl = "-"
			}
			if got != c.want || lvl != c.wantLevel {
				t.Fatalf("labels %v: detected_level %q level %q, want %q and %q", e.metricLabels, got, lvl, c.want, c.wantLevel)
			}
		})
	}
}
