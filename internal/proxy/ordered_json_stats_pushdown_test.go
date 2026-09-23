package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// jsonVolumeLine is one stored line: level is the stream label (empty when the
// stream has none) and msg the raw log line.
type jsonVolumeLine struct {
	ts    time.Time
	level string
	msg   string
}

type jsonVolumeFakeVL struct {
	mu         sync.Mutex
	lines      []jsonVolumeLine
	statsCalls []string
	guardCalls []string
	rawCalls   int
}

// vlUnpackJSONLevel mirrors VictoriaLogs unpack_json for the level key: only a
// line that is one valid JSON object starting at its first byte yields fields.
func vlUnpackJSONLevel(msg string) string {
	if msg == "" || msg[0] != '{' {
		return ""
	}
	var fields map[string]any
	if json.Unmarshal([]byte(msg), &fields) != nil {
		return ""
	}
	switch v := fields["level"].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return ""
}

var jsonVolumeMsgFilterRE = regexp.MustCompile(`_msg:~("(?:[^"\\]|\\.)*")`)

// fakeVLLevelFilterRE matches the `| filter [-]level:="v"` pipe the pushdown
// renders for a Loki level=/!= label filter.
var fakeVLLevelFilterRE = regexp.MustCompile(`\| filter (-?)level:="([^"]*)"`)

func newJSONVolumeFakeVL(t testing.TB, lines []jsonVolumeLine) (*httptest.Server, *jsonVolumeFakeVL) {
	t.Helper()
	fake := &jsonVolumeFakeVL{lines: lines}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		query := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			fake.mu.Lock()
			fake.statsCalls = append(fake.statsCalls, query)
			fake.mu.Unlock()
			start, end, step, offset, ok := slidingBucketParams(t, r)
			if !ok {
				return
			}
			keep := strings.Contains(query, "fields (level) keep_original_fields")
			groupStart := strings.Index(query, "stats by (")
			if groupStart < 0 {
				t.Errorf("fake VL: ungrouped stats query %q", query)
				return
			}
			rest := query[groupStart+len("stats by ("):]
			groupBy := strings.Split(rest[:strings.Index(rest, ")")], ", ")
			bytesMetric := strings.Contains(query, "sum_len(_msg) as c")
			type key struct{ name, level string }
			points := map[key]map[int64]float64{}
			add := func(k key, ts int64, v float64) {
				if points[k] == nil {
					points[k] = map[int64]float64{}
				}
				points[k][ts] += v
			}
			for _, line := range fake.lines {
				ts := line.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				level := line.level
				unpacked, unpack := "", false
				switch {
				case strings.Contains(query, "| unpack_json"):
					unpacked, unpack = vlUnpackJSONLevel(line.msg), true
				case strings.Contains(query, "| unpack_logfmt"):
					unpacked, unpack = slidingLogfmtFields(line.msg)["level"], true
				}
				if unpack && (!keep || level == "") {
					// Without keep_original_fields unpacking overwrites stored fields.
					level = unpacked
				}
				if m := fakeVLLevelFilterRE.FindStringSubmatch(query); m != nil && (level == m[2]) == (m[1] == "-") {
					continue
				}
				bucket := vlTruncate(ts, step, offset)
				value := 1.0
				if bytesMetric {
					value = float64(len(line.msg))
					add(key{"__sample_count", level}, bucket, 1)
				}
				add(key{"c", level}, bucket, value)
			}
			keys := make([]key, 0, len(points))
			for k := range points {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].name+keys[i].level < keys[j].name+keys[j].level })
			var sb strings.Builder
			sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"metric":{"__name__":%q`, k.name)
				for _, field := range groupBy {
					value := ""
					if field == "level" {
						value = k.level
					}
					// VictoriaLogs reports every grouped field, empty or not.
					fmt.Fprintf(&sb, `,%q:%q`, field, value)
				}
				sb.WriteString(`},"values":[`)
				tss := make([]int64, 0, len(points[k]))
				for ts := range points[k] {
					tss = append(tss, ts)
				}
				sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
				for j, ts := range tss {
					if j > 0 {
						sb.WriteByte(',')
					}
					fmt.Fprintf(&sb, `[%s,%q]`, strconv.FormatFloat(float64(ts)/1e9, 'f', -1, 64), strconv.FormatFloat(points[k][ts], 'f', -1, 64))
				}
				sb.WriteString(`]}`)
			}
			sb.WriteString(`]}}`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(sb.String()))
		case "/select/logsql/query":
			start := parseFakeVLTime(t, r.Form.Get("start"))
			end := parseFakeVLTime(t, r.Form.Get("end"))
			guard := strings.HasSuffix(query, " | limit 1")
			logfmtGuard := guard && !strings.Contains(query, "unpack_json")
			var msgRE *regexp.Regexp
			fake.mu.Lock()
			if logfmtGuard {
				fake.guardCalls = append(fake.guardCalls, query)
			} else if guard {
				fake.guardCalls = append(fake.guardCalls, query)
				match := jsonVolumeMsgFilterRE.FindStringSubmatch(query)
				pattern, err := strconv.Unquote(match[1])
				if err != nil {
					t.Errorf("fake VL: guard pattern %q: %v", match[1], err)
				}
				msgRE = regexp.MustCompile(pattern)
			} else {
				fake.rawCalls++
			}
			fake.mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, line := range fake.lines {
				ts := line.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				if logfmtGuard && (line.level != "" || !logfmtLineIsRisk(line.msg, "level")) {
					continue
				}
				if guard && !logfmtGuard && (line.level != "" || !msgRE.MatchString(line.msg) || vlUnpackJSONLevel(line.msg) != "") {
					continue
				}
				row := map[string]string{"_time": line.ts.UTC().Format(time.RFC3339Nano), "_msg": line.msg, "_stream": `{app="api"}`, "app": "api"}
				if line.level != "" {
					row["_stream"] = `{app="api",level="` + line.level + `"}`
					row["level"] = line.level
				}
				encoded, _ := json.Marshal(row)
				_, _ = w.Write(append(encoded, '\n'))
				if guard {
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake
}

// lokiJSONVolumeReference evaluates the metric with Loki semantics for fixture
// lines whose invalid JSON holds no level key: level is the stream label, else
// the top-level JSON key; detected_level falls back to unknown; the sample at t
// covers (t-window, t] and steps without lines are absent.
func lokiJSONVolumeReference(lines []jsonVolumeLine, fn string, groupBy []string, start, end time.Time, step, window time.Duration) map[string]map[int64]string {
	return lokiLevelVolumeReference(lines, "json", fn, groupBy, start, end, step, window)
}

// lokiLevelVolumeReference is lokiJSONVolumeReference for parser "json",
// "logfmt" (fixture lines are simple key=value tokens) or "" (no parser).
func lokiLevelVolumeReference(lines []jsonVolumeLine, parser, fn string, groupBy []string, start, end time.Time, step, window time.Duration) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for t := start; !t.After(end); t = t.Add(step) {
		values := map[string]float64{}
		for _, line := range lines {
			if !line.ts.After(t.Add(-window)) || line.ts.After(t) {
				continue
			}
			level := line.level
			switch {
			case level != "":
			case parser == "json":
				level = vlUnpackJSONLevel(line.msg)
			case parser == "logfmt":
				level = slidingLogfmtFields(line.msg)["level"]
			}
			labels := map[string]string{}
			for _, name := range groupBy {
				switch {
				case name == "level" && level != "":
					labels[name] = level
				case name == "detected_level" && level != "":
					labels[name] = level
				case name == "detected_level":
					labels[name] = "unknown"
				}
			}
			key := canonicalLabelsKey(labels)
			switch fn {
			case "bytes_over_time":
				values[key] += float64(len(line.msg))
			default:
				values[key]++
			}
		}
		for key, v := range values {
			if fn == "rate" {
				v /= window.Seconds()
			}
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][t.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out
}

func runJSONVolumeQueryRange(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, rec.Code, rec.Body)
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: invalid matrix (%v): %s", query, err, rec.Body)
	}
	got := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		key := canonicalLabelsKey(series.Metric)
		if got[key] == nil {
			got[key] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			got[key][int64(ts)], _ = pair[1].(string)
		}
	}
	return got
}

func jsonVolumeFixture(s0 time.Time) []jsonVolumeLine {
	lines := make([]jsonVolumeLine, 0, 120)
	for i := 0; i < 120; i++ { // one line every 5s for 10 minutes, including window edges
		lines = append(lines, jsonVolumeFixtureLine(s0.Add(time.Duration(i)*5*time.Second), i))
	}
	return lines
}

// jsonVolumeFixtureLine returns the i-th line of the rotating fixture shapes.
func jsonVolumeFixtureLine(ts time.Time, i int) jsonVolumeLine {
	switch i % 6 {
	case 0: // stream level wins over a different JSON level (_extracted in Loki)
		return jsonVolumeLine{ts, "info", `{"level":"error","msg":"collision"}`}
	case 1: // unparseable JSON in a stream with a level
		return jsonVolumeLine{ts, "warn", `{"msg": "truncated`}
	case 2: // level only in the JSON body
		return jsonVolumeLine{ts, "", `{"level":"debug","n":` + strconv.Itoa(i) + `}`}
	case 3: // valid JSON without a level key
		return jsonVolumeLine{ts, "", `{"counter":` + strconv.Itoa(i) + `}`}
	case 4: // plain text without a level
		return jsonVolumeLine{ts, "", "plain text " + strings.Repeat("x", i%5)}
	default: // nested objects beside a top-level level key
		return jsonVolumeLine{ts, "", `{"a":{"b":"c"},"level":"warn"}`}
	}
}

// Grafana's Explore logs volume for `{...} | json` must not scan raw rows: with
// parser errors dropped every line counts once, so VictoriaLogs stats buckets
// over unpack_json keep_original_fields are exact.
func TestOrderedJSONLogsVolumeUsesStatsBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := jsonVolumeFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, tc := range []struct {
		query   string
		fn      string
		groupBy []string
		step    time.Duration
		window  time.Duration
		stats   string
	}{
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__[1m]))`, "count_over_time", []string{"level", "detected_level"}, time.Minute, time.Minute, "unpack_json fields (level) keep_original_fields | stats by (level, detected_level) count() as c"},
		{`sum by (level) (rate({app="api"} | json | drop __error__, __error_details__ [2m]))`, "rate", []string{"level"}, 30 * time.Second, 2 * time.Minute, "unpack_json fields (level) keep_original_fields | stats by (level) count() as c"},
		{`sum by (detected_level) (bytes_over_time({app="api"} | json | drop __error__ [90s]))`, "bytes_over_time", []string{"detected_level"}, time.Minute, 90 * time.Second, "unpack_json fields (level) keep_original_fields | stats by (level, detected_level) sum_len(_msg) as c, count() as __sample_count"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newJSONVolumeFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			want := lokiJSONVolumeReference(lines, tc.fn, tc.groupBy, start, end, tc.step, tc.window)
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, tc.step)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.rawCalls != 0 || len(fake.statsCalls) != 1 || len(fake.guardCalls) != 1 {
				t.Fatalf("expected one guard and one stats call without raw rows: raw=%d stats=%q guard=%q", fake.rawCalls, fake.statsCalls, fake.guardCalls)
			}
			if !strings.HasSuffix(fake.statsCalls[0], tc.stats) {
				t.Fatalf("stats query %q does not end with %q", fake.statsCalls[0], tc.stats)
			}
		})
	}
}

// Loki's JSON parser keeps keys read before a syntax error, and skips leading
// whitespace, while VictoriaLogs unpack_json adds no field for such lines. A
// level-less stream holding one keeps the exact raw evaluator.
func TestOrderedJSONLogsVolumePartialJSONKeepsRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	for _, msg := range []string{`{"level":"error","msg": truncated`, ` {"level":"error"}`, `{"level":"error"} trailing`} {
		t.Run(msg, func(t *testing.T) {
			lines := []jsonVolumeLine{{s0.Add(10 * time.Second), "info", `{"msg":"ok"}`}, {s0.Add(20 * time.Second), "", msg}}
			srv, fake := newJSONVolumeFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			query := `sum by (level) (count_over_time({app="api"} | json | drop __error__[1m]))`
			got := runJSONVolumeQueryRange(t, p, query, s0.Add(time.Minute), s0.Add(time.Minute), time.Minute)
			at := s0.Add(time.Minute).Unix()
			want := map[string]map[int64]string{
				canonicalLabelsKey(map[string]string{"level": "info"}):  {at: "1"},
				canonicalLabelsKey(map[string]string{"level": "error"}): {at: "1"},
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("partially parsed level lost\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			// The bucket query runs beside the guard and its result is discarded.
			if fake.rawCalls != 1 || len(fake.guardCalls) != 1 {
				t.Fatalf("expected the guard to keep the raw evaluator: raw=%d guard=%d", fake.rawCalls, len(fake.guardCalls))
			}
		})
	}
}

// Without a JSON parser only the detected_level volume shapes compile; the
// other routes keep every other plain and logfmt metric.
func TestLevelVolumePlanRequiresDetectedLevelAndDroppedErrors(t *testing.T) {
	for _, query := range []string{
		`sum by (level) (count_over_time({env="production"} | drop __error__[1m]))`,
		`sum by (detected_level) (count_over_time({env="production"}[1m]))`,
		`sum by (detected_level) (count_over_time({env="production"} | logfmt [1m]))`,
		`sum by (detected_level, service_name) (count_over_time({env="production"} | drop __error__[1m]))`,
		`sum by (detected_level) (count_over_time({env="production"} | json | logfmt | drop __error__[1m]))`,
		`sum by (detected_level) (count_over_time({env="production"} | logfmt level | drop __error__[1m]))`,
	} {
		if plan, ok := compileOrderedJSONMetric(query); ok {
			t.Errorf("%s: unexpected plan %+v", query, plan)
		}
	}
}

func TestOrderedJSONStatsPushdownEligibility(t *testing.T) {
	for _, tc := range []struct {
		query  string
		fields []string
	}{
		{`sum by (level, detected_level) (count_over_time({env="production"} | json | drop __error__[1m]))`, []string{"level"}},
		{`sum by (detected_level) (count_over_time({env="production"} |= "x" | json | drop __error_details__, __error__[1m]))`, []string{"level"}},
		{`sum by (app, level) (bytes_rate({env="production"} | json | drop __error__ [1m]))`, []string{"app", "level"}},
		{`sum by (level) (rate({env="production"} | json | drop __error__ | json[1m]))`, nil},
		{`sum by (level) (count_over_time({env="production"} | json [1m]))`, nil},
		{`sum by (level) (count_over_time({env="production"} | json | drop __error_details__ [1m]))`, nil},
		{`sum by (__error__) (count_over_time({env="production"} | json | drop __error__ [1m]))`, nil},
		{`sum by (service_name) (count_over_time({env="production"} | json | drop __error__ [1m]))`, nil},
		{`sum without (level) (count_over_time({env="production"} | json | drop __error__ [1m]))`, nil},
		{`sum by (level) (count_over_time({env="production"} | json | drop __error__ | level="info" [1m]))`, []string{"level"}},
		{`sum by (level, detected_level) (count_over_time({env="production"} | json | status=` + "`200`" + ` | drop __error__ [1m]))`, []string{"level"}},
		// A label filter before the parser reads the stored label; VictoriaLogs applies it.
		{`sum by (level) (count_over_time({env="production"} | level="info" | json | drop __error__ [1m]))`, []string{"level"}},
		{`sum by (level) (count_over_time({env="production"} |= "x" | level="info" | json | drop __error__ [1m]))`, []string{"level"}},
		// After a drop the filter no longer reads the stored label: it stays a stage of the raw evaluator.
		{`sum by (level) (count_over_time({env="production"} | drop level | level="" | json | drop __error__ [1m]))`, nil},
		// A filter on __error__ before the parser compares the empty value; the raw evaluator keeps it.
		{`sum by (level) (count_over_time({env="production"} | __error__="" | json | drop __error__ [1m]))`, nil},
		{`sum by (level) (count_over_time({env="production"} | json | trace_id="x" | drop __error__ [1m]))`, []string{"level"}},
		{`sum by (level) (count_over_time({env="production"} | json | drop __error__, level [1m]))`, nil},
		{`count_over_time({env="production"} | json | drop __error__ [1m])`, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | drop __error__[1m]))`, []string{"level"}},
		{`sum by (detected_level) (rate({env="production"} |= "x" | logfmt | drop __error__, __error_details__[1m]))`, []string{"level"}},
		{`sum by (detected_level) (count_over_time({env="production"} | logfmt | level="error" | drop __error__[1m]))`, []string{"level"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				t.Fatal("expected an ordered JSON plan")
			}
			if !reflect.DeepEqual(plan.unpackFields, tc.fields) {
				t.Fatalf("unpack fields %q, want %q", plan.unpackFields, tc.fields)
			}
		})
	}
}

// logfmtLineIsRisk evaluates logfmtParseRisk's patterns for a line without a
// stored field, with the regexp engine VictoriaLogs uses.
func logfmtLineIsRisk(line, field string) bool {
	re := newLogfmtRiskPatterns(field)
	match := func(pattern string) bool { return regexp.MustCompile(pattern).MatchString(line) }
	return match(re.key) && (!match(re.wellFormed) || match(re.repeated) || (match(re.keyAssignment) && !match(re.safeValue)))
}

// Loki's logfmt decoder and VictoriaLogs unpack_logfmt extract the same level
// only from single-space separated key, key=value and key="value" tokens.
func TestLogfmtParseRiskPatterns(t *testing.T) {
	for _, tc := range []struct {
		line  string
		risky bool
	}{
		{`level=warn ts=2026-09-14T21:14:23+02:00 op=UPDATE duration_ms=3194`, false},
		{`msg="disk full" level=error retry`, false},
		{`msg="a level=warn b" user=x`, false},
		{`loglevel=debug msg=ok`, false},
		{`no level here`, false},
		{`{"level":"error"}`, false},
		{"ts=1\tlevel=error", true},         // Loki splits on tabs
		{"ts=1\tlevel=error extra", true},   // same, followed by tokens
		{`level=info level=error`, true},    // repeated key
		{`level level=error`, true},         // bare copy before the value
		{`msg="x"level=error`, true},        // VictoriaLogs stops after the quoted value
		{`msg="a \"b\"" level=error`, true}, // escaped quote
		{`level="err\u006fr"`, true},        // escape inside the level value
		{`a=b=c level=error`, true},         // Loki rejects '=' inside a value
		{`level=错误`, true},                  // non-ASCII value
		{`  level=error`, true},             // leading spaces
		{`level=`, false},                   // empty on both sides
		{`level=level msg=ok`, false},
		{`level='error' msg=x`, true},      // unpack_logfmt unquotes single quotes
		{"level=\x60warn\x60 msg=x", true}, // and backticks
		{`msg='a b' level=warn`, true},
		{`level="a b" x=1 level=warn`, true},
	} {
		if got := logfmtLineIsRisk(tc.line, "level"); got != tc.risky {
			t.Errorf("%q: risky=%v, want %v", tc.line, got, tc.risky)
		}
	}
}

// Grafana's logs volume for a plain selector and for `| logfmt` groups by the
// detected_level alias. VictoriaLogs stats buckets answer both: level keeps
// the stored value, detected_level mirrors it and falls back to unknown.
func TestLevelVolumePlainAndLogfmtUseStatsBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var lines []jsonVolumeLine
	for i := 0; i < 120; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		switch i % 4 {
		case 0: // stored level wins over a different body level
			lines = append(lines, jsonVolumeLine{ts, "info", "level=error msg=collision"})
		case 1:
			lines = append(lines, jsonVolumeLine{ts, "", "level=warn op=update n=" + strconv.Itoa(i)})
		case 2:
			lines = append(lines, jsonVolumeLine{ts, "", "op=select n=" + strconv.Itoa(i)})
		default:
			lines = append(lines, jsonVolumeLine{ts, "debug", `{"msg":"json body"}`})
		}
	}
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, tc := range []struct {
		query, parser string
		guards        int
		stats         string
	}{
		{`sum by (level, detected_level) (count_over_time({app="api"} | drop __error__[1m]))`, "", 0, `app:="api" | stats by (level, detected_level) count() as c`},
		{`sum by (level, detected_level) (count_over_time({app="api"} | logfmt | drop __error__[1m]))`, "logfmt", 1, "unpack_logfmt fields (level) keep_original_fields | stats by (level, detected_level) count() as c"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			lines := lines
			if tc.parser == "" {
				// Loki detects a body level at ingest for streams without one;
				// the plain fixture keeps level-less bodies free of levels.
				lines = append([]jsonVolumeLine(nil), lines...)
				for i := range lines {
					if lines[i].level == "" {
						lines[i].msg = strings.ReplaceAll(lines[i].msg, "level=", "stage=")
					}
				}
			}
			srv, fake := newJSONVolumeFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			want := lokiLevelVolumeReference(lines, tc.parser, "count_over_time", []string{"level", "detected_level"}, start, end, time.Minute, time.Minute)
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.rawCalls != 0 || len(fake.statsCalls) != 1 || len(fake.guardCalls) != tc.guards {
				t.Fatalf("expected %d guard and one stats call without raw rows: raw=%d stats=%q guard=%q", tc.guards, fake.rawCalls, fake.statsCalls, fake.guardCalls)
			}
			if !strings.HasSuffix(fake.statsCalls[0], tc.stats) {
				t.Fatalf("stats query %q does not end with %q", fake.statsCalls[0], tc.stats)
			}
		})
	}
}

// -ordered-json-metric-max-bytes bounds the raw rows read and names itself in
// the error, like the row limit does.
func TestOrderedJSONMetricMaxBytesFlag(t *testing.T) {
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row, _ := json.Marshal(map[string]string{"_time": stamp.Format(time.RFC3339Nano), "_msg": strings.Repeat("x", 1<<10), "_stream": `{app="test"}`})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 8; i++ {
			_, _ = w.Write(append(row, '\n'))
		}
	}))
	defer backend.Close()
	plan, _ := compileOrderedJSONMetric(`sum(rate({app="test"}|json[5m]))`)
	for _, tc := range []struct {
		limit int64
		fails bool
	}{{0, false}, {4 << 10, true}, {64 << 10, false}} {
		p := newTestProxy(t, backend.URL)
		p.orderedJSONMaxBytes = tc.limit
		_, err := p.collectOrderedJSONMetric(t.Context(), plan, stamp.Add(time.Second), stamp.Add(time.Second), time.Second)
		if (err != nil) != tc.fails || (err != nil && !strings.Contains(err.Error(), "-ordered-json-metric-max-bytes")) {
			t.Fatalf("limit %d: err=%v, want failure %v naming the flag", tc.limit, err, tc.fails)
		}
	}
}

// Grafana Explore adds label filters from the query builder after `| json`,
// e.g. `{env="production"} | json | status=` + "`200`" + `; its logs volume
// must stay on stats buckets instead of scanning raw rows up to the row cap.
func TestOrderedJSONLogsVolumeWithLabelFilterUsesStatsBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := jsonVolumeFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	for _, tc := range []struct {
		query, op, stats string
	}{
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | level="warn" | drop __error__[1m]))`, "=", `unpack_json fields (level) keep_original_fields | filter level:="warn" | stats by (level, detected_level) count() as c`},
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__ | level!="warn" [1m]))`, "!=", `unpack_json fields (level) keep_original_fields | filter -level:="warn" | stats by (level, detected_level) count() as c`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newJSONVolumeFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			var kept []jsonVolumeLine
			for _, line := range lines {
				level := line.level
				if level == "" {
					level = vlUnpackJSONLevel(line.msg)
				}
				if (level == "warn") == (tc.op == "=") {
					kept = append(kept, line)
				}
			}
			want := lokiJSONVolumeReference(kept, "count_over_time", []string{"level", "detected_level"}, start, end, time.Minute, time.Minute)
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.rawCalls != 0 || len(fake.statsCalls) != 1 || !strings.HasSuffix(fake.statsCalls[0], tc.stats) {
				t.Fatalf("expected one stats call ending %q without raw rows: raw=%d stats=%q", tc.stats, fake.rawCalls, fake.statsCalls)
			}
		})
	}
}

func TestLogsQLLabelFilter(t *testing.T) {
	// VictoriaLogs unquotes double-quoted strings with Go rules, so every
	// backslash a Loki regexp carries must survive as an escape.
	for _, tc := range []struct{ op, value, want string }{
		{"=", "200", ` | filter status:="200"`},
		{"!=", `a"b`, ` | filter -status:="a\"b"`},
		{"=~", `2\d\d`, ` | filter status:~"^(?:2\\d\\d)$"`},
		{"!~", "5..", ` | filter -status:~"^(?:5..)$"`},
		{"=~", `/api/.*\.json`, ` | filter status:~"^(?:/api/.*\\.json)$"`},
	} {
		condition, err := translator.NewDropCondition("status", tc.op, tc.value)
		if err != nil {
			t.Fatal(err)
		}
		if got := logsQLLabelFilter(condition); got != tc.want {
			t.Errorf("%s %q: got %s, want %s", tc.op, tc.value, got, tc.want)
		}
	}
}

// A refreshed or widened volume query re-checks only the part of its window
// not already found free of lines the parsers read differently.
func TestCachedStatsPushdownRiskChecksOnlyUncoveredWindow(t *testing.T) {
	s0 := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	srv, fake := newJSONVolumeFakeVL(t, jsonVolumeFixture(s0))
	p := newSlidingTestProxy(t, srv.URL)
	ctx := context.Background()
	calls := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.guardCalls)
	}
	for i, tc := range []struct {
		from, to time.Duration
		calls    int
	}{
		{0, 30 * time.Minute, 1},
		{5 * time.Minute, 20 * time.Minute, 1},
		{0, 40 * time.Minute, 2},
		{-10 * time.Minute, 40 * time.Minute, 3},
	} {
		if risky, err := p.cachedStatsPushdownRisk(ctx, "json", `app:="api"`, []string{"level"}, nil, nil, s0.Add(tc.from), s0.Add(tc.to)); err != nil || risky {
			t.Fatalf("step %d: risky=%v err=%v", i, risky, err)
		}
		if got := calls(); got != tc.calls {
			t.Fatalf("step %d: %d guard calls, want %d", i, got, tc.calls)
		}
	}
}

// VictoriaLogs' unpack_logfmt does not end a token at a tab, Loki's decoder
// does. The pushdown must decline such a line and the pipeline must be
// evaluated over the rows, which is the only way to return Loki's value.
func TestLogfmtParseRiskUsesRowEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := []jsonVolumeLine{
		{s0.Add(10 * time.Second), "", "n=1\tlevel=warn"},
		{s0.Add(20 * time.Second), "", "n=2 level=warn"},
	}
	srv, fake := newJSONVolumeFakeVL(t, lines)
	p := newSlidingTestProxy(t, srv.URL)
	query := `sum by (level, detected_level) (count_over_time({app="api"} | logfmt | drop __error__[1m]))`
	got := runJSONVolumeQueryRange(t, p, query, s0.Add(time.Minute), s0.Add(time.Minute), time.Minute)
	at := s0.Add(time.Minute).Unix()
	want := map[string]map[int64]string{
		canonicalLabelsKey(map[string]string{"level": "warn", "detected_level": "warn"}): {at: "2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tab-separated logfmt line lost\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.rawCalls != 1 {
		t.Fatalf("expected the row evaluator to answer: raw=%d stats=%d guard=%d", fake.rawCalls, len(fake.statsCalls), len(fake.guardCalls))
	}
}
