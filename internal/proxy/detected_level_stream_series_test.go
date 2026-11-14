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

// streamSeriesLabels sends query through the real handler against emulated
// VictoriaLogs rows and returns the label set of every answered series, sorted.
func streamSeriesLabels(t *testing.T, rows []vlemu.Row, version, query string, base int64, step string, instant bool) []map[string]string {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: version}
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
	if instant {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(base/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(base/1e9-900, 10))
		params.Set("end", strconv.FormatInt(base/1e9, 10))
		params.Set("step", step)
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

func sortLabelSets(sets []map[string]string) {
	sort.Slice(sets, func(i, j int) bool { return canonicalLabelsKey(sets[i]) < canonicalLabelsKey(sets[j]) })
}

// streamRows writes a few logfmt rows carrying amount for every stream, inside
// the last 15 minutes before base.
func levelStreamRows(streams []string) []vlemu.Row {
	base := int64(1_800_000_000) * 1e9
	var rows []vlemu.Row
	for i := 0; i < 24*len(streams); i++ {
		stream := streams[i%len(streams)]
		rows = append(rows, vlemu.Row{TS: base - int64(i)*7e9 - 3e8, Stream: stream, Value: float64(i%9 + 1), Msg: fmt.Sprintf("amount=%d id=%d", i%9+1, i)})
	}
	return rows
}

func streamSelector(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// TestStreamGroupedSeriesDetectedLevelMatchesLoki: Loki derives detected_level
// at ingest, and a level stream label decides it for every line of the stream.
// The per-stream stats read of a `without` fold (and of topk/bottomk without)
// carries no level column, and the proxy labelled those series
// detected_level="unknown" where Loki answered the stream's level. The
// recorded Loki answers of the three registry requests are the expected
// series: the streams are rebuilt from Loki's own topk answer (every
// payment-service pod), so the proxy's label sets must equal Loki's.
//
// conformance: semantics/without-fold-stream-detected-level, semantics/unwrap-sum-without-collapses, semantics/topk-bottomk-without-strips-label, quality/query-range-settled-steps, metric-series-identity
func TestStreamGroupedSeriesDetectedLevelMatchesLoki(t *testing.T) {
	raw, err := os.ReadFile("testdata/detected_level_stream_label_loki_answers.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Answers []struct {
			Case       string              `json:"case"`
			Query      string              `json:"query"`
			Step       string              `json:"step"`
			LokiSeries []map[string]string `json:"loki_series"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	var streams []string
	for _, a := range recorded.Answers {
		if a.Case != "semantics/topk-bottomk-without-strips-label" {
			continue
		}
		for _, s := range a.LokiSeries {
			stream := make(map[string]string, len(s))
			for k, v := range s {
				if k != "detected_level" {
					stream[k] = v
				}
			}
			streams = append(streams, streamSelector(stream))
		}
	}
	if len(streams) != 17 {
		t.Fatalf("recorded topk answer holds %d streams, want 17", len(streams))
	}
	rows := levelStreamRows(streams)
	base := int64(1_800_000_000) * 1e9
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, a := range recorded.Answers {
			want := a.LokiSeries
			sortLabelSets(want)
			t.Run(version+"/"+a.Case, func(t *testing.T) {
				got := streamSeriesLabels(t, rows, version, a.Query, base, a.Step, false)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s\n got  %v\n want %v (Loki)", a.Query, got, want)
				}
			})
		}
	}
}

// TestStreamGroupedSeriesDetectedLevelEdges: the series of a per-stream stats
// read take detected_level from the stream's level field as Loki's ingest
// does: the first level key Loki reads (level, LEVEL, severity, lvl, ...),
// normalised (ERROR is error, warning warn, dbg debug), an unknown word kept as
// it is, unknown when the stream has none (Loki's rule for stream labels:
// TestStreamDetectedLevelMatchesLokiGolden), and a stream detected_level label
// left as it was. `sum without (detected_level)` drops the label, `sum without (level)`
// keeps the derived one; topk and bottomk only rank inside the groups and keep
// every label of the series (semantics/topk-bottomk-without-strips-label).
//
// conformance: semantics/without-fold-stream-detected-level, metric-series-identity
func TestStreamGroupedSeriesDetectedLevelEdges(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	cases := []struct {
		name   string
		labels string // stream labels besides app and pod
		want   string // detected_level, "-" for none
		levelQ string // the without list
	}{
		{"level", `level="error"`, "error", "pod"},
		{"level upper value", `level="ERROR"`, "error", "pod"},
		{"LEVEL key", `LEVEL="warning"`, "warn", "pod"},
		{"severity key", `severity="dbg"`, "debug", "pod"},
		{"lvl key", `lvl="fatal"`, "fatal", "pod"},
		{"level beats severity", `level="info",severity="error"`, "info", "pod"},
		{"unknown word kept", `level="notice"`, "notice", "pod"},
		{"no level", `ns="x"`, "unknown", "pod"},
		{"stream detected_level unchanged", `detected_level="warn",level="error"`, "warn", "pod"}, // as before the fix, not a Loki-proven row
		{"without detected_level", `level="error"`, "-", "pod, detected_level"},
		{"without level", `level="error"`, "error", "pod, level"},
	}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, c := range cases {
			streams := []string{`{app="uw",` + c.labels + `,pod="a"}`, `{app="uw",` + c.labels + `,pod="b"}`}
			rows := levelStreamRows(streams)
			for _, query := range []string{
				`sum without (` + c.levelQ + `) (rate({app="uw"} | logfmt amount | unwrap amount [5m]))`,
				`sum without (` + c.levelQ + `) (max_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
				`topk without (` + c.levelQ + `) (1, rate({app="uw"} | logfmt amount | unwrap amount [5m]))`,
			} {
				want, excluded := c.want, c.levelQ
				if strings.HasPrefix(query, "topk") && c.want == "-" {
					want = "error"
				}
				if strings.HasPrefix(query, "topk") {
					excluded = "pod"
				}
				t.Run(fmt.Sprintf("%s/%s/%s", version, c.name, query), func(t *testing.T) {
					got := streamSeriesLabels(t, rows, version, query, base, "60", false)
					if len(got) == 0 {
						t.Fatalf("no series")
					}
					for _, s := range got {
						v, ok := s["detected_level"]
						if want == "-" {
							if ok {
								t.Fatalf("series %v keeps detected_level", s)
							}
							continue
						}
						if v != want {
							t.Fatalf("series %v: detected_level %q, want %q", s, v, want)
						}
						if strings.Contains(excluded, "level") && !strings.Contains(excluded, "detected_level") {
							if _, ok := s["level"]; ok {
								t.Fatalf("series %v keeps the excluded level", s)
							}
						}
					}
				})
			}
		}
	}
}

// TestStreamDetectedLevelMatchesLokiGolden: the stream rule against Loki's
// recorded answers (testdata/detected_level_golden.json: live Loki, the Loki
// oracle and VictoriaLogs rows): every row whose stream carries a level key and
// whose fields hold no detected_level, which a stream rule must win for. A
// level="Debug" stream label is debug and an lvl="x" one x on live Loki;
// Severity beats lvl.
//
// conformance: semantics/without-fold-stream-detected-level, severity-detected-level-derivation
func TestStreamDetectedLevelMatchesLokiGolden(t *testing.T) {
	checked := 0
	for _, c := range loadDetectedLevelGolden(t) {
		if c.Fields["detected_level"] != "" || c.Stream["detected_level"] != "" {
			continue
		}
		has := false
		for _, name := range levelFieldNames {
			if c.Stream[name] != "" {
				has = true
			}
		}
		if !has {
			continue
		}
		checked++
		if got := streamDetectedLevel(c.Stream); got != c.Want {
			t.Errorf("%s (%s): stream %v gives %q, Loki %q", c.Name, c.Source, c.Stream, got, c.Want)
		}
	}
	if checked < 4 {
		t.Fatalf("only %d golden rows decided by the stream", checked)
	}
	if got := streamDetectedLevel(map[string]string{"app": "x"}); got != levelUnknown {
		t.Fatalf("a stream without a level field gives %q, want unknown", got)
	}
}

// TestStreamLevelSeriesFenceCoversTheChangedShapes: every outer form whose
// label sets the fix changed (a diff of range functions x parsers x outer
// forms against the previous build: without folds, unwrap functions, topk,
// bottomk, sort, sort_desc, label_replace) is fenced over every inner range
// function, and the forms whose answers did not change keep their keys.
//
// conformance: semantics/without-fold-stream-detected-level, cache-response-key-versions
func TestStreamLevelSeriesFenceCoversTheChangedShapes(t *testing.T) {
	var inners, unwraps []string
	for _, pl := range []string{"", " | logfmt", " | json", " | logfmt amount"} {
		inners = append(inners, `count_over_time({app="uw"}`+pl+` [5m])`, `rate({app="uw"}`+pl+` [1m])`, `bytes_over_time({app="uw"}`+pl+` [5m])`)
		for _, fn := range []string{"rate", "max_over_time", "sum_over_time", "avg_over_time", "first_over_time"} {
			unwraps = append(unwraps, fn+`({app="uw"}`+pl+` | unwrap amount [5m])`)
		}
		unwraps = append(unwraps, `quantile_over_time(0.5, {app="uw"}`+pl+` | unwrap amount [5m])`)
	}
	fenced := []string{"sum without (pod) (%s)", "max without (pod) (%s)", "sum without () (%s)", "topk(1, %s)", "topk by (pod) (1, %s)", "topk without (pod) (1, %s)", "bottomk(1, %s)", "sort(%s)", "sort_desc(%s)", `label_replace(%s, "x", "$1", "pod", "(.*)")`}
	kept := []string{"%s", "sum by (app) (%s)", "sum(%s)", "sum by (detected_level) (%s)", "count(%s)", "avg by (pod) (%s)", "quantile(0.5, %s)", "stddev(%s)", "%s * 2", "-%s"}
	for _, in := range inners {
		for _, f := range fenced {
			if q := fmt.Sprintf(f, in); streamLevelSeriesResponseKeyVersion(q) == "" {
				t.Errorf("not fenced: %s", q)
			}
		}
		for _, f := range kept {
			q := fmt.Sprintf(f, in)
			// A parser stage is fenced in every form since the stream rule reached the bare-parser and raw-row
			// reads (TestStreamLevelSeriesFenceCoversParserShapes).
			if fenced := streamLevelSeriesResponseKeyVersion(q) != ""; fenced != mayHoldParserStage(q) {
				t.Errorf("fenced %v, parser stage %v: %s", fenced, mayHoldParserStage(q), q)
			}
		}
	}
	for _, in := range unwraps {
		for _, f := range append(fenced, kept...) {
			if q := fmt.Sprintf(f, in); streamLevelSeriesResponseKeyVersion(q) == "" {
				t.Errorf("unwrap query not fenced: %s", q)
			}
		}
	}
}

// TestMayHoldDropStageNeverHidesARegexpMatch: the precheck in front of the
// source-drop regexp of the detected_level mutation fence is exact on the
// side that matters: every query the regexp matches passes it (the corpus of
// the fence differential, an edge table in every case and blank), so no key
// changes.
//
// conformance: cache-response-key-versions
func TestMayHoldDropStageNeverHidesARegexpMatch(t *testing.T) {
	queries := append([]string{
		`{a="b"} | drop level`, `{a="b"} | DROP level`, `{a="b"} | Drop Severity`, "{a=\"b\"} |\tdrop\nlvl", "{a=\"b\"} |\f\rdrop lvl",
		`{a="b"} |drop log.level`, `{a="b"} | json | drop severity_text, x`, `sum(rate({a="b"} | logfmt | drop severity_number [1m]))`,
		`{a="b"} | dropx level`, `{a="b"} drop level`, `{a="b"} |= "drop level"`, `{a="b"} | keep level`, `|`, `| dro`, ``,
	}, fenceSyntheticQueries...)
	queries = append(queries, fenceCorpusYAMLQueries(t)...)
	matched := 0
	for _, q := range queries {
		re := detectedLevelSourceDropStageRE.MatchString(q)
		if re {
			matched++
		}
		if re && !mayHoldDropStage(q) {
			t.Errorf("%q: the regexp matches, the precheck says no", q)
		}
	}
	if matched < 6 {
		t.Fatalf("only %d queries match the regexp: the table does not cover it", matched)
	}
	for _, q := range []string{`{a="b"} drop level`, `{a="b"} |= "x"`, `sum(rate({a="b"}[1m]))`, ``} {
		if mayHoldDropStage(q) {
			t.Errorf("%q: no drop stage, the precheck says yes", q)
		}
	}
}

// FuzzMayHoldDropStage: on any input, a match of the source-drop regexp
// implies the precheck, so skipping the regexp never changes a key.
func FuzzMayHoldDropStage(f *testing.F) {
	for _, s := range []string{`{a="b"} | drop level`, `| DROP lvl`, "|\t\f drop severity", `|dro`, `{a="b"} | dropx level`, `drop level`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if detectedLevelSourceDropStageRE.MatchString(q) && !mayHoldDropStage(q) {
			t.Fatalf("%q: the regexp matches, the precheck says no", q)
		}
	})
}

// TestStreamLevelSeriesFence: the answers cached before the fix said unknown,
// so every query that can read its series per stream carries the fence in the
// compat range key, the inner query_range key and the settled-steps key built
// from it, and a stale answer stored under the old key is never served. The
// fenced words are those of every query whose label sets changed in a diff of
// range functions x parsers x wrappers against the previous build; other
// queries keep their keys.
//
// conformance: semantics/without-fold-stream-detected-level, cache-response-key-versions
func TestStreamLevelSeriesFence(t *testing.T) {
	for _, c := range []struct {
		query string
		fence bool
	}{
		{`sum without (pod) (rate({app="a"} | logfmt amount | unwrap amount [5m]))`, true},
		{`topk(1, rate({app="a"} | logfmt amount | unwrap amount [5m]))`, true},
		{`sum without () (count_over_time({app="a"}[5m]))`, true},
		{`max_over_time({app="a"} | unwrap amount [5m])`, true},
		{`sort_desc(rate({app="a"} | json [1m]))`, true},
		{`bottomk(1, count_over_time({app="a"} | logfmt [5m]))`, true},
		{`label_replace(rate({app="a"} | json [1m]), "x", "$1", "pod", "(.*)")`, true},
		{`sort_desc (rate({app="a"} | json [1m]))`, true},
		{`topk by (pod) (1, rate({app="a"} | json [1m]))`, true},
		// The parser reads keywords in any case and any Unicode blank before a parenthesis.
		{`TOPK(1, rate({app="a"} | json [1m]))`, true},
		{`BottomK(1, rate({app="a"} | json [1m]))`, true},
		{`sum WITHOUT (pod) (rate({app="a"}[1m]))`, true},
		{`max_over_time({app="a"} | UNWRAP amount [5m])`, true},
		{`SORT(rate({app="a"} | json [1m]))`, true},
		{"sort\v(rate({app=\"a\"} | json [1m]))", true},
		{"Sort_Desc\u00a0(rate({app=\"a\"} | json [1m]))", true},
		{"LABEL_REPLACE\u0085(rate({app=\"a\"} | json [1m]), \"x\", \"$1\", \"pod\", \"(.*)\")", true},
		{`SUM BY (app) (count_over_time({app="a"}[5m]))`, false},
		{`sum by (app) (count_over_time({app="a"}[5m]))`, false},
		{`sum by (detected_level) (rate({app="a"} | json [1m]))`, true},
		{`count(rate({app="a"} | logfmt | status = "sort" [1m]))`, true},
		{`sum by (detected_level) (rate({app="a"} |= "json" [1m]))`, false},
		{`count(rate({app="a"} | line_format "x" | status = "sort" [1m]))`, false},
		{`{app="topk"} |= "without"`, false},
		{`rate({app="a"} | json [1m]) / rate({app="a"} | logfmt [1m])`, true},
		{`rate({app="a"} |= "json" [1m]) / rate({app="a"} | decolorize [1m])`, false},
		{`rate({app="a"}[5m])`, false},
		{`{app="a"} |= "x"`, false},
	} {
		if got := streamLevelSeriesResponseKeyVersion(c.query) != ""; got != c.fence {
			t.Errorf("%s: fenced %v, want %v", c.query, got, c.fence)
		}
	}

	const stale = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"uw","detected_level":"unknown","level":"error","stale":"yes"},"values":[[1800000000,"1"]]}]}}`
	query := `sum without (pod) (rate({app="uw"} | logfmt amount | unwrap amount [5m]))`
	rows := levelStreamRows([]string{`{app="uw",level="error",pod="a"}`, `{app="uw",level="error",pod="b"}`})
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0"}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	target := "/loki/api/v1/query_range?" + url.Values{"query": {query}, "start": {"1799999100"}, "end": {"1800000000"}, "step": {"60"}}.Encode()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	key, ok := p.compatCacheKey("query_range", req)
	if !ok || !strings.Contains(key, streamLevelSeriesResponseVersion) {
		t.Fatalf("compat key %q lacks the fence", key)
	}
	for name, k := range map[string]string{"inner": p.queryRangeCacheKey(req, query), "steps": p.queryStepsBaseKey(req, query)} {
		if !strings.Contains(k, streamLevelSeriesResponseVersion) {
			t.Errorf("%s key %q lacks the fence", name, k)
		}
	}
	p.compatCache.Set(strings.Replace(key, streamLevelSeriesResponseVersion, "", 1), []byte(stale))
	rec := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "stale") || strings.Contains(rec.Body.String(), `"unknown"`) {
		t.Fatalf("%d %s: want the fresh answer, not the cached one", rec.Code, rec.Body.String())
	}
	// Repeat: the fresh answer is cached under the fenced key, so VictoriaLogs is not asked again.
	before := len(emu.Calls)
	if again := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); again.Body.String() != rec.Body.String() || len(emu.Calls) != before {
		t.Fatalf("repeat: %d new VictoriaLogs calls, same body %v", len(emu.Calls)-before, again.Body.String() == rec.Body.String())
	}
}

// FuzzStreamLevelSeriesFenceSpelling: the parser reads keywords in any case
// and any Unicode blank, so a query the fence covers stays covered with its
// letters' case flipped and its blanks written as \v, \f, a no-break space or
// a next-line character.
func FuzzStreamLevelSeriesFenceSpelling(f *testing.F) {
	for _, q := range []string{`topk(1, rate({app="a"} | json [1m]))`, `bottomk by (pod) (1, count_over_time({app="a"} | logfmt [5m]))`, `sum without (pod) (rate({app="a"}[1m]))`,
		`max_over_time({app="a"} | unwrap amount [5m])`, `sort (rate({app="a"} | json [1m]))`, `sort_desc(rate({app="a"}[1m]))`, `sum by (app) (rate({app="a"} | logfmt [1m]))`, `label_replace(rate({app="a"}[1m]), "x", "$1", "pod", "(.*)")`} {
		f.Add(q, uint64(0x5a5a5a5a5a5a5a5a), uint8(1))
	}
	blanks := [...]string{" ", "\v", "\f", "\u00a0", "\u0085", "\t\n"}
	f.Fuzz(func(t *testing.T, q string, mask uint64, blank uint8) {
		if streamLevelSeriesResponseKeyVersion(q) == "" {
			return
		}
		var b strings.Builder
		for i := 0; i < len(q); i++ {
			c := q[i]
			switch {
			case c == ' ':
				b.WriteString(blanks[int(blank)%len(blanks)])
				continue
			case mask>>(uint(i)%64)&1 == 1 && c >= 'a' && c <= 'z':
				c -= 'a' - 'A'
			case mask>>(uint(i)%64)&1 == 1 && c >= 'A' && c <= 'Z':
				c += 'a' - 'A'
			}
			b.WriteByte(c)
		}
		if v := b.String(); streamLevelSeriesResponseKeyVersion(v) == "" {
			t.Fatalf("%q is fenced, %q is not", q, v)
		}
	})
}
