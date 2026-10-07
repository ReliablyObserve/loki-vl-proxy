package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Which unwrap range aggregations Loki v3.7.7 fails on a value its conversion
// rejects, and which the detection evaluates. The commented status is Loki
// 3.7.7's answer captured on a fixture holding such values
// (semantics/unwrap-conversion-error).
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-conversion-error-postfilter-forms, parser-error-and-label-collision
func TestUnwrapErrorProbePlans(t *testing.T) {
	const sel = `{service_name="pay"}`
	for _, tc := range []struct {
		query string
		// probes is the number of aggregations checked; 0 means the query cannot
		// fail on a conversion error (Loki 200) or is outside what the detection
		// evaluates (registered).
		probes int
		conv   string
		hints  []string
	}{
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap t [5m])`, probes: 1},                                     // 400
		{query: `rate(` + sel + ` | logfmt | unwrap v [5m])`, probes: 1},                                              // 400
		{query: `quantile_over_time(0.9, ` + sel + ` | logfmt | unwrap v [5m])`, probes: 1},                           // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__="" [5m])`, probes: 0},                      // 200
		{query: `sum_over_time(` + sel + ` | logfmt | __error__="" | unwrap v [5m])`, probes: 1},                      // 400
		{query: `sum_over_time(` + sel + ` | logfmt | drop __error__ | unwrap v [5m])`, probes: 1},                    // 400
		{query: `sum_over_time(` + sel + ` | logfmt | drop __error__, __error_details__ | unwrap v [5m])`, probes: 1}, // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__!="SampleExtractionErr" [5m])`, probes: 0},  // 200
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__=~".*" [5m])`, probes: 1},
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error_details__="" [5m])`, probes: 1}, // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error_details__!="" [5m])`, probes: 0},
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | n="99" [5m])`, probes: 1},                                                      // 200: no row passes
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | n!="99" [5m])`, probes: 1},                                                     // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap duration(dur) [5m])`, probes: 1, conv: "duration"},                                 // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(dur) [5m])`, probes: 1, conv: "duration"},                         // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap bytes(size) [5m])`, probes: 1, conv: "bytes"},                                      // 400
		{query: `sum_over_time(` + sel + ` | unwrap detected_level [5m])`, probes: 1},                                                           // 400
		{query: `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`, probes: 1, hints: []string{"service_name", "v"}}, // 400
		{query: `sum(sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`, probes: 1, hints: []string{"v"}},                                    // 400
		{query: `sum by (__error__) (sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`, probes: 1, hints: []string{"__error__", "v"}},       // 400
		{query: `max by (n) (max_over_time(` + sel + ` | logfmt | unwrap v [5m]))`, probes: 1},                                                  // 400, every label
		{query: `max_over_time(` + sel + ` | logfmt | unwrap v [5m]) by (n)`, probes: 1, hints: []string{"n", "v"}},                             // 400
		{query: `sum without (n) (sum_over_time(` + sel + ` | logfmt | unwrap v [5m]))`, probes: 1},
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v [5m]) * 2`, probes: 1}, // 400
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v [5m]) / sum_over_time(` + sel + ` | logfmt | unwrap n [5m])`, probes: 2},
		{query: `count_over_time(` + sel + ` | logfmt [5m])`, probes: 0},
		// Outside what the detection evaluates: VictoriaLogs' answer to the post
		// filter differs from Loki's for a failing row, so the query answers as
		// before (semantics/unwrap-conversion-error-postfilter-forms).
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__="SampleExtractionErr" [5m])`, probes: 0}, // 400 in Loki
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__!="" [5m])`, probes: 0},                   // 400 in Loki
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | v > 3 [5m])`, probes: 0},                           // 400 in Loki
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | n > 3 [5m])`, probes: 0},
		{query: `sum_over_time(` + sel + ` | logfmt | unwrap v | __error__="" or v="1" [5m])`, probes: 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr, err := logqlpkg.Parse(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			probes := unwrapErrorProbes(expr)
			if len(probes) != tc.probes {
				t.Fatalf("got %d checked aggregations %+v, want %d", len(probes), probes, tc.probes)
			}
			if tc.probes == 0 {
				return
			}
			probe := probes[0]
			if probe.conv != tc.conv {
				t.Errorf("conv %q, want %q", probe.conv, tc.conv)
			}
			var hints []string
			for name := range probe.hints {
				if !strings.HasSuffix(name, "_extracted") {
					hints = append(hints, name)
				}
			}
			sort.Strings(hints)
			if strings.Join(hints, ",") != strings.Join(tc.hints, ",") {
				t.Errorf("hints %v, want %v", hints, tc.hints)
			}
		})
	}
}

// The detection rides on the metric's own stats query: the rejected rows are
// flagged, let through the gate with the value 0 and grouped apart in every
// stats pipe; queries it cannot rewrite exactly are sent unchanged.
// conformance: semantics/unwrap-conversion-error
func TestUnwrapCountingQuery(t *testing.T) {
	all := func(string, string) bool { return true }
	base := `app:="a" | unpack_logfmt`
	for _, tc := range []struct {
		name, query, want string
		ok                bool
	}{
		{"grouped", base + translator.UnwrapGate("v") + " | stats by (app) sum(__lvp_v) as c, count() as __sample_count",
			base + unwrapCountedGate("v", "") + " | stats by (app, __lvp_bad) sum(__lvp_v) as c, count() as __sample_count", true},
		{"ungrouped", base + translator.UnwrapGate("v") + " | stats max(__lvp_v)",
			base + unwrapCountedGate("v", "") + " | stats by (__lvp_bad) max(__lvp_v)", true},
		{"explicit empty grouping", base + translator.UnwrapGate("v") + " | stats by () avg(__lvp_v)",
			base + unwrapCountedGate("v", "") + " | stats by (__lvp_bad) avg(__lvp_v)", true},
		{"two-level rate", base + translator.UnwrapGateFor("d", "duration") + " | stats by (app) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | filter __lvp_rate:* | stats by (app) sum(__lvp_rate)",
			base + unwrapCountedGate("d", "duration") + " | stats by (app, __lvp_bad) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | filter __lvp_rate:* | stats by (app, __lvp_bad) sum(__lvp_rate)", true},
		{"window phase between gate and stats", base + translator.UnwrapGateFor("b", "bytes") + windowPhaseFilter(time.Unix(1700000000, 0), 5*time.Minute, time.Minute) + " | stats by (_stream) max(__lvp_v) as c",
			base + unwrapCountedGate("b", "bytes") + windowPhaseFilter(time.Unix(1700000000, 0), 5*time.Minute, time.Minute) + " | stats by (_stream, __lvp_bad) max(__lvp_v) as c", true},
		{"a pipe that could drop the flagged groups", base + translator.UnwrapGate("v") + " | stats by (app) sum(__lvp_v) as c | sort by (c) desc | limit 5", "", false},
		{"no gate", base + " | stats by (app) count()", "", false},
		{"a gate without stats", base + translator.UnwrapGate("v"), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, gotBase, _, _, ok := unwrapCountingQuery(tc.query, all)
			if ok != tc.ok {
				t.Fatalf("ok=%v, want %v (%s)", ok, tc.ok, got)
			}
			if !ok {
				if got != tc.query {
					t.Fatalf("a query the rewrite does not know must be sent unchanged: %s", got)
				}
				return
			}
			if got != tc.want || gotBase != base {
				t.Fatalf("got  %s\nwant %s\nbase %q", got, tc.want, gotBase)
			}
		})
	}
	if _, _, _, _, ok := unwrapCountingQuery(base+translator.UnwrapGate("v")+" | stats sum(__lvp_v)", func(f, c string) bool { return f != "v" }); ok {
		t.Fatal("a field no checked aggregation unwraps must not be counted")
	}
}

// The flagged groups are removed from a matrix or vector answer before any
// caller reads it, and the buckets they held are reported.
// conformance: semantics/unwrap-conversion-error
func TestStripUnwrapCounter(t *testing.T) {
	matrix := `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"__name__":"c","g":"a","__lvp_bad":"1"},"values":[[1700000000,"0"],[1700000060,"0"]]},` +
		`{"metric":{"__name__":"c","g":"a","__lvp_bad":"0"},"values":[[1700000060,"4"]]},` +
		`{"metric":{"__name__":"c","g":"b"},"values":[[1700000000,"6"]]}]}}`
	out, times, err := stripUnwrapCounter([]byte(matrix))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"data":{"result":[{"metric":{"__name__":"c","g":"a"},"values":[[1700000060,"4"]]},{"metric":{"__name__":"c","g":"b"},"values":[[1700000000,"6"]]}],"resultType":"matrix"},"status":"success"}`
	if string(out) != want || len(times) != 2 || times[0] != 1700000000 || times[1] != 1700000060 {
		t.Fatalf("got %s %v", out, times)
	}
	vector := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__lvp_bad":"1"},"value":[1700000300,"0"]},{"metric":{"__lvp_bad":"0"},"value":[1700000300,"7"]}]}}`
	out, times, err = stripUnwrapCounter([]byte(vector))
	if err != nil || len(times) != 1 || !strings.Contains(string(out), `"result":[{"metric":{},"value":[1700000300,"7"]}]`) {
		t.Fatalf("got %s %v %v", out, times, err)
	}
	if out, times, err = stripUnwrapCounter([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)); err != nil || len(times) != 0 || !strings.Contains(string(out), `"result":[]`) {
		t.Fatalf("empty answer: %s %v %v", out, times, err)
	}
	// Without a flagged group the kept rows' label goes, wherever VictoriaLogs put it.
	clean := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","__lvp_bad":"0"},"values":[[1,"2"]]},{"metric":{"__lvp_bad":"0","g":"a"},"values":[[1,"3"]]},{"metric":{"__lvp_bad":"0"},"values":[[1,"4"]]}]}}`
	out, times, err = stripUnwrapCounter([]byte(clean))
	if err != nil || len(times) != 0 || string(out) != `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"values":[[1,"2"]]},{"metric":{"g":"a"},"values":[[1,"3"]]},{"metric":{},"values":[[1,"4"]]}]}}` {
		t.Fatalf("clean answer: %s %v %v", out, times, err)
	}
}

// Loki's value of the unwrapped label, re-derived from the stored line with
// Loki's parsers (the counter cannot tell VictoriaLogs' rendering of an array or
// a boolean from a string, nor a parsed key from a stored field of the same
// name). Each row is checked against Loki 3.7.7's answer on the same line.
// conformance: semantics/unwrap-conversion-error, parser-error-and-label-collision
func TestUnwrapLokiLabels(t *testing.T) {
	jsonLine := `{"a":[1,2],"b":null,"c":true,"d":5,"e":{"x":1},"f":"[1,2]","g":"7","s":"abc"}`
	stream := map[string]string{"service_name": "pay", "detected_level": "unknown"}
	for _, tc := range []struct {
		name, query, line string
		stored            map[string]string // structured metadata
		label, want       string
		ok                bool
	}{
		{"json array is skipped", `sum_over_time({a="b"} | json | unwrap a [5m])`, jsonLine, nil, "a", "", true},                     // Loki 200
		{"json null is skipped", `sum_over_time({a="b"} | json | unwrap b [5m])`, jsonLine, nil, "b", "", true},                      // Loki 200
		{"json boolean is a value", `sum_over_time({a="b"} | json | unwrap c [5m])`, jsonLine, nil, "c", "true", true},               // Loki 400
		{"json number", `sum_over_time({a="b"} | json | unwrap d [5m])`, jsonLine, nil, "d", "5", true},                              // Loki 200
		{"json object is flattened", `sum_over_time({a="b"} | json | unwrap e [5m])`, jsonLine, nil, "e", "", true},                  // Loki 200
		{"json nested key", `sum_over_time({a="b"} | json | unwrap e_x [5m])`, jsonLine, nil, "e_x", "1", true},                      // Loki 200
		{"json string that looks like an array", `sum_over_time({a="b"} | json | unwrap f [5m])`, jsonLine, nil, "f", "[1,2]", true}, // Loki 400
		{"unpack without _entry adds nothing", `sum_over_time({a="b"} | unpack | unwrap s [5m])`, jsonLine, nil, "s", "", true},      // Loki 200
		{"unpack keeps strings of a packed entry", `sum_over_time({a="b"} | unpack | unwrap s [5m])`, `{"_entry":"x","s":"abc","d":5}`, nil, "s", "abc", true},
		{"unpack skips numbers", `sum_over_time({a="b"} | unpack | unwrap d [5m])`, `{"_entry":"x","s":"abc","d":5}`, nil, "d", "", true},
		{"logfmt", `sum_over_time({a="b"} | logfmt | unwrap y [5m])`, "x=abc y=3", nil, "y", "3", true},
		// Loki's decoder: the first non-empty value of a key wins (parser_test.go
		// "duplicate from line property"); VictoriaLogs' unpack_logfmt keeps the last.
		{"logfmt duplicate key, first wins", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, "v=5 v=abc", nil, "v", "5", true},      // Loki 200
		{"logfmt empty value, a later one wins", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, "v= v=abc", nil, "v", "abc", true}, // Loki 400
		{"logfmt bare key has no value", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, "v w=1", nil, "v", "", true},
		{"logfmt quoted value with escapes", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, `v="a\"b \u00e9"`, nil, "v", `a"b é`, true},
		{"logfmt quoted value", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, `v="12 ms"`, nil, "v", "12 ms", true},
		{"logfmt unterminated quote is skipped", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, `v="abc w=1`, nil, "v", "", true},
		{"logfmt value holding = is skipped, the next pair is read", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, `v=a=b v=7`, nil, "v", "7", true},
		{"logfmt U+FFFD becomes a space", `sum_over_time({a="b"} | logfmt | unwrap bytes(v) [1m])`, "v=5KB�", nil, "v", "5KB ", true}, // Loki 200 (humanize reads "5KB ")
		{"logfmt invalid UTF-8 becomes a space", `sum_over_time({a="b"} | logfmt | unwrap v [1m])`, "v=5\xff", nil, "v", "5 ", true},
		{"logfmt key sanitised", `sum_over_time({a="b"} | logfmt | unwrap v_w [1m])`, "v.w=abc", nil, "v_w", "abc", true},
		{"unpack, the last value of a key wins", `sum_over_time({a="b"} | unpack | unwrap s [5m])`, `{"_entry":"x","s":"1","s":"abc"}`, nil, "s", "abc", true},
		{"a parsed key named like structured metadata reads the stored value", `sum_over_time({a="b"} | logfmt | unwrap x [5m])`, "x=abc y=3", map[string]string{"x": "5"}, "x", "5", true}, // Loki 200
		{"the parsed key is x_extracted", `sum_over_time({a="b"} | logfmt | unwrap x_extracted [5m])`, "x=abc y=3", map[string]string{"x": "5"}, "x_extracted", "abc", true},                // Loki 400
		{"a parsed key named like a stream label", `sum_over_time({a="b"} | json | unwrap service_name [5m])`, `{"service_name":"x"}`, nil, "service_name", "pay", true},
		{"json on a logfmt line extracts nothing", `sum_over_time({a="b"} | json | unwrap y [5m])`, "x=abc y=3", nil, "y", "", true}, // Loki 200
		{"regexp captures", `sum_over_time({a="b"} | regexp "took (?P<ms>\\S+)" | unwrap ms [5m])`, "took 12x", nil, "ms", "12x", true},
		{"drop removes the label", `sum_over_time({a="b"} | logfmt | drop y | unwrap y [5m])`, "y=abc", nil, "y", "", true},
		{"label_format writing the label is not reproduced", `sum_over_time({a="b"} | logfmt | label_format y="{{.x}}" | unwrap y [5m])`, "x=abc", nil, "y", "", false},
		{"pattern is not reproduced", `sum_over_time({a="b"} | pattern "<_> <y>" | unwrap y [5m])`, "a abc", nil, "y", "", false},
		{"json with arguments is not reproduced", `sum_over_time({a="b"} | json y="v" | unwrap y [5m])`, `{"v":"abc"}`, nil, "y", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := logqlpkg.Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			probes := unwrapErrorProbes(expr)
			if len(probes) != 1 {
				t.Fatalf("%d checked aggregations", len(probes))
			}
			base := cloneStringMap(stream)
			for k, v := range tc.stored {
				base[k] = v
			}
			labels, _, ok := unwrapLokiLabels(probes[0], base, tc.line, tc.line)
			if ok != tc.ok {
				t.Fatalf("ok=%v, want %v", ok, tc.ok)
			}
			if ok && labels[tc.label] != tc.want {
				t.Fatalf("%s=%q, want %q (labels %v)", tc.label, labels[tc.label], tc.want, labels)
			}
		})
	}
	// A JSON parser error on a line, with __error__ among the parser hints,
	// preserves the error: Loki does not fail the query on that line.
	expr, _ := logqlpkg.Parse(`sum by (__error__) (sum_over_time({a="b"} | json | unwrap v [5m]))`)
	labels, _, ok := unwrapLokiLabels(unwrapErrorProbes(expr)[0], cloneStringMap(stream), `{"v":"abc", broken`, "")
	if !ok || labels["__preserve_error__"] != "true" {
		t.Fatalf("expected a preserved parser error: %v %v", labels, ok)
	}
}

// requestsRecorded reads loki_vl_proxy_requests_total for an endpoint and status.
func requestsRecorded(p *Proxy, endpoint, status string) int {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	total := 0
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "loki_vl_proxy_requests_total{") && strings.Contains(line, `direction="downstream"`) &&
			strings.Contains(line, `endpoint="`+endpoint+`"`) && strings.Contains(line, `status="`+status+`"`) {
			n, _ := strconv.Atoi(line[strings.LastIndex(line, " ")+1:])
			total += n
		}
	}
	return total
}

// conversionFakeVL plays VictoriaLogs for the detection: the stats query answers
// a matrix with a flagged group when the counted query is sent and the fixture
// says the bucket held a rejected value; the lookup answers the rows of the
// fixture whose field is present and rejected; the stored-row read answers by
// stream id and time.
type conversionFakeVL struct {
	mu        sync.Mutex
	rows      []map[string]string // as VictoriaLogs returns them after the pipeline
	stored    []map[string]string // as stored
	badBucket int64               // seconds; 0: no flagged group
	stats     []string
	lookups   []url.Values
	reads     int
}

func newConversionFakeVL(t *testing.T, fake *conversionFakeVL) *httptest.Server {
	t.Helper()
	lookupRE := regexp.MustCompile(`\| filter "([^"]+)":\* -"[^"]+":~("(?:[^"\\]|\\.)*")(?: \| math .*)? \| limit 3$`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		fake.mu.Lock()
		defer fake.mu.Unlock()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			fake.stats = append(fake.stats, query)
			kind, point := "matrix", `"values":[[%d,"1"]]`
			if r.URL.Path == "/select/logsql/stats_query" {
				kind, point = "vector", `"value":[%d,"1"]`
			}
			result := ""
			if fake.badBucket != 0 && strings.Contains(query, unwrapBadField) {
				result = `{"metric":{"__name__":"c","app":"pay","__lvp_bad":"1"},` + strings.Replace(point, "%d", strconv.FormatInt(fake.badBucket, 10), 1) + `}`
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"` + kind + `","result":[` + result + `]}}`))
		case "/select/logsql/query":
			if strings.HasPrefix(query, "_stream_id:") {
				fake.reads++
				for _, row := range fake.stored {
					if strings.Contains(query, row["_stream_id"]) {
						b, _ := json.Marshal(row)
						_, _ = w.Write(append(b, '\n'))
					}
				}
				return
			}
			if m := lookupRE.FindStringSubmatch(query); m != nil {
				fake.lookups = append(fake.lookups, r.Form)
				pattern, _ := strconv.Unquote(m[2])
				re := regexp.MustCompile(pattern)
				for _, row := range fake.rows {
					if v := row[m[1]]; v != "" && !re.MatchString(v) {
						b, _ := json.Marshal(row)
						_, _ = w.Write(append(b, '\n'))
					}
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// End to end through the handlers: a clean metric sends its stats query with
// the counter and reads no row; a flagged bucket costs one bounded lookup and
// one stored-row read and answers Loki's 400 as text/plain, unless Loki's
// parser gives the line another value (an array, a key named like stored
// metadata), where the metric's answer stands. The status the route records is
// the status sent.
// conformance: semantics/unwrap-conversion-error, status-400, parser-error-and-label-collision, loki_api_v1_query_range, loki_api_v1_query
func TestUnwrapConversionErrorAnswersLokiText(t *testing.T) {
	at := time.Unix(1700000000, 0)
	stream := `{detected_level="unknown",service_name="pay"}`
	row := func(ts time.Time, msg string, fields map[string]string) map[string]string {
		out := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_stream": stream, "_stream_id": "s1", "_msg": msg, "service_name": "pay", "detected_level": "unknown"}
		for k, v := range fields {
			out[k] = v
		}
		return out
	}
	tail := "Use a label filter to intentionally skip this error. (e.g | __error__!=\"SampleExtractionErr\").\n" +
		"To skip all potential errors you can match empty errors.(e.g __error__=\"\")\n" +
		"The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__=\"\" )\n"
	run := func(p *Proxy, path string, params url.Values) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
		if strings.HasSuffix(path, "query_range") {
			p.handleQueryRange(rec, req)
		} else {
			p.handleQuery(rec, req)
		}
		return rec
	}
	rangeParams := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
	}
	badTS := at.Add(3*time.Minute + 7*time.Second)
	logfmtBad := row(badTS, "n=1 v=x7 t=abc", map[string]string{"n": "1", "v": "x7", "t": "abc"})
	storedLogfmt := row(badTS, "n=1 v=x7 t=abc", nil)
	for _, tc := range []struct {
		name, path   string
		params       url.Values
		rows, stored []map[string]string
		series       string // "": the metric's 200
	}{
		{"range, every label", "/loki/api/v1/query_range", rangeParams(`sum_over_time({service_name="pay"} | logfmt | unwrap v [5m])`),
			[]map[string]string{logfmtBad}, []map[string]string{storedLogfmt},
			`{__error__="SampleExtractionErr", __error_details__="strconv.ParseFloat: parsing \"x7\": invalid syntax", detected_level="unknown", n="1", service_name="pay", t="abc", v="x7"}`},
		{"range, sum pushes its grouping into the parser hints", "/loki/api/v1/query_range", rangeParams(`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v [5m]))`),
			[]map[string]string{logfmtBad}, []map[string]string{storedLogfmt},
			`{__error__="SampleExtractionErr", __error_details__="strconv.ParseFloat: parsing \"x7\": invalid syntax", detected_level="unknown", service_name="pay", v="x7"}`},
		{"duration conversion", "/loki/api/v1/query_range", rangeParams(`sum by (service_name) (max_over_time({service_name="pay"} | logfmt | unwrap duration(t) [5m]))`),
			[]map[string]string{logfmtBad}, []map[string]string{storedLogfmt},
			// max_over_time takes no grouping from the outer sum: every label.
			`{__error__="SampleExtractionErr", __error_details__="time: invalid duration \"abc\"", detected_level="unknown", n="1", service_name="pay", t="abc", v="x7"}`},
		{"instant", "/loki/api/v1/query", url.Values{"query": {`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v [5m]))`}, "time": {strconv.FormatInt(badTS.Add(time.Minute).UnixNano(), 10)}},
			[]map[string]string{logfmtBad}, []map[string]string{storedLogfmt},
			`{__error__="SampleExtractionErr", __error_details__="strconv.ParseFloat: parsing \"x7\": invalid syntax", detected_level="unknown", service_name="pay", v="x7"}`},
		{"a JSON array VictoriaLogs renders as a string", "/loki/api/v1/query_range", rangeParams(`sum by (service_name) (sum_over_time({service_name="pay"} | json | unwrap v [5m]))`),
			[]map[string]string{row(badTS, `{"v":[1,2]}`, map[string]string{"v": "[1,2]"})}, []map[string]string{row(badTS, `{"v":[1,2]}`, nil)}, ""},
		{"a parsed key named like stored metadata", "/loki/api/v1/query_range", rangeParams(`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap x [5m]))`),
			[]map[string]string{row(badTS, "x=abc", map[string]string{"x": "abc"})}, []map[string]string{row(badTS, "x=abc", map[string]string{"x": "5"})}, ""},
		{"a line outside every window", "/loki/api/v1/query_range", rangeParams(`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v [1m]))`),
			[]map[string]string{row(at.Add(-2*time.Minute), "v=x", map[string]string{"v": "x"})}, []map[string]string{row(at.Add(-2*time.Minute), "v=x", nil)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &conversionFakeVL{rows: tc.rows, stored: tc.stored, badBucket: badTS.Truncate(time.Minute).Unix()}
			p := newTestProxy(t, newConversionFakeVL(t, fake).URL)
			rec := run(p, tc.path, tc.params)
			if tc.series == "" {
				if rec.Code != http.StatusOK {
					t.Fatalf("Loki answers this line with the metric: got %d %s", rec.Code, rec.Body)
				}
				return
			}
			want := "pipeline error: 'SampleExtractionErr' for series: '" + tc.series + "'.\n" + tail
			if rec.Code != http.StatusBadRequest || rec.Body.String() != want {
				t.Fatalf("got %d %q\nwant 400 %q", rec.Code, rec.Body.String(), want)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
				t.Fatalf("content type %q, Loki answers text/plain; charset=utf-8", ct)
			}
			// The request is recorded once, with the status sent.
			endpoint := "query_range"
			if strings.HasSuffix(tc.path, "/query") {
				endpoint = "query"
			}
			if ok, bad := requestsRecorded(p, endpoint, "200"), requestsRecorded(p, endpoint, "400"); ok != 0 || bad != 1 {
				t.Fatalf("requests recorded: %d with 200, %d with 400; want only one 400", ok, bad)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.lookups) != 1 || fake.reads != 1 || len(fake.stats) == 0 || !strings.Contains(fake.stats[0], unwrapBadField) {
				t.Fatalf("want the counter in the stats query, one lookup and one stored-row read: stats=%q lookups=%d reads=%d", fake.stats, len(fake.lookups), fake.reads)
			}
			// The lookup reads only the flagged bucket's span (a step on either
			// side) inside Loki's windows; an instant query has one window.
			from, _ := time.Parse(time.RFC3339Nano, fake.lookups[0].Get("start"))
			to, _ := time.Parse(time.RFC3339Nano, fake.lookups[0].Get("end"))
			if to.Sub(from) > 5*time.Minute {
				t.Fatalf("the lookup must stay inside the flagged bucket: %s..%s", from, to)
			}
		})
	}

	// A clean metric (no flagged group) reads no row, and an intercepted one is
	// sent without the counter.
	for _, q := range []string{
		`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v [5m]))`,
		`sum by (service_name) (sum_over_time({service_name="pay"} | logfmt | unwrap v | __error__="" [5m]))`,
	} {
		fake := &conversionFakeVL{rows: []map[string]string{logfmtBad}, stored: []map[string]string{storedLogfmt}}
		p := newTestProxy(t, newConversionFakeVL(t, fake).URL)
		if rec := run(p, "/loki/api/v1/query_range", rangeParams(q)); rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", q, rec.Code, rec.Body)
		}
		fake.mu.Lock()
		if len(fake.lookups) != 0 || fake.reads != 0 || len(fake.stats) == 0 || strings.Contains(fake.stats[0], unwrapBadField) == strings.Contains(q, "__error__") {
			t.Fatalf("%s: stats=%q lookups=%d reads=%d", q, fake.stats, len(fake.lookups), fake.reads)
		}
		fake.mu.Unlock()
	}
}

// Every value a conversion accepts is one of the forms the detection skips, so a
// row it counts always fails the conversion (Loki's SampleExtractionErr).
// The forms the lookup skips but Loki rejects are the overflowing ones,
// registered in semantics/unwrap-gate-parsefloat-divergences.
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapAcceptablePatternCoversConversions(t *testing.T) {
	alphabets := map[string]string{
		"":         "0123456789.+-eExXpP_aAfFinINty ",
		"duration": "0123456789.+-nsuµμmh d",
		"bytes":    "0123456789., kKmMgGiIbBtTpPeE ",
	}
	for conv, alphabet := range alphabets {
		re := regexp.MustCompile(unwrapAcceptablePattern(conv))
		letters := []rune(alphabet)
		seed := uint64(1)
		next := func() uint64 {
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			return seed
		}
		accepted := 0
		for i := 0; i < 400000; i++ {
			n := int(next()%7) + 1
			var b strings.Builder
			for j := 0; j < n; j++ {
				b.WriteRune(letters[next()%uint64(len(letters))])
			}
			v := b.String()
			if _, err := convertUnwrap(v, conv); err == nil {
				accepted++
				if !re.MatchString(v) {
					t.Fatalf("conv %q accepts %q but the detection would report it", conv, v)
				}
			}
		}
		if accepted < 1000 {
			t.Fatalf("conv %q: only %d accepted values generated", conv, accepted)
		}
	}
	for conv, values := range map[string][]string{
		"":         {"1e308", "0x1p4", "infinity", "NaN", "1e1_0", "1_000", "+Inf", "1.", ".5"},
		"duration": {"1.5s", "250ms", "-.5ms", "5.s", "100µs", "100μs", "1h2m3s", "+0"},
		"bytes":    {"2KiB", "1kb", "5 kB", ",5", "1.5,0", "1 KB", "1EiB", "42"},
	} {
		re := regexp.MustCompile(unwrapAcceptablePattern(conv))
		for _, v := range values {
			if _, err := convertUnwrap(v, conv); err != nil {
				t.Fatalf("%q must convert with %q: %v", v, conv, err)
			}
			if !re.MatchString(v) {
				t.Errorf("conv %q: the detection would report %q, which converts", conv, v)
			}
		}
	}
	for conv, values := range map[string][]string{
		"":         {"abc", "x7", "1.2.3", "1.2.3e4", "250ms", "86282s", "1d", " 1", "1 ", "true", "0x1", ""},
		"duration": {"1d", "42", "abc", "1.5", "s"},
		"bytes":    {"lots", "1.5x", " 5", "-1", "1KBs", "1.2.3", "1.2KB.", "ib"},
	} {
		re := regexp.MustCompile(unwrapAcceptablePattern(conv))
		for _, v := range values {
			if v != "" && re.MatchString(v) {
				t.Errorf("conv %q: the detection skips %q, which Loki rejects", conv, v)
			}
		}
	}
}
