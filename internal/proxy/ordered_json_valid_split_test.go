package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/jsonparser"
	"github.com/valyala/fastjson"
)

// vlFastjsonUnpackFields mirrors VictoriaLogs' unpack_json exactly
// v1.52 (lib/logstorage/pipe_unpack_json.go, json_parser.go): JSON whitespace
// around the value is trimmed (trimJSONWhitespace; v1.47 and v1.40 do not trim);
// a value whose first byte is then not `{` yields nothing; otherwise fastjson parses the whole value
// (trailing text fails it), nested objects are flattened with a dot, nulls are
// skipped, numbers, booleans and arrays keep their JSON text and strings are
// decoded.
func vlFastjsonUnpackFields(msg string) map[string]string {
	msg = strings.Trim(msg, " \t\n\r")
	if msg == "" || msg[0] != '{' {
		return nil
	}
	var parser fastjson.Parser
	v, err := parser.Parse(msg)
	if err != nil {
		return nil
	}
	o, err := v.Object()
	if err != nil {
		return nil
	}
	out := map[string]string{}
	var walk func(prefix string, o *fastjson.Object)
	walk = func(prefix string, o *fastjson.Object) {
		o.Visit(func(k []byte, v *fastjson.Value) {
			switch v.Type() {
			case fastjson.TypeNull:
			case fastjson.TypeObject:
				child, _ := v.Object()
				walk(prefix+string(k)+".", child)
			case fastjson.TypeString:
				out[prefix+string(k)] = string(v.GetStringBytes())
			default:
				out[prefix+string(k)] = string(v.MarshalTo(nil))
			}
		})
	}
	walk("", o)
	return out
}

// lokiJSONAccepts mirrors Loki v3.7.7's JSONParser.Process acceptance
// (pkg/logql/log/parser.go:74-135): jsonparser.ObjectEach over the line,
// descending into a nested object only when one of the required labels
// starts with its sanitized key path (parser_hints.go ShouldExtractPrefix,
// parser.go buildSanitizedPrefixFromBuffer; nil required walks every object).
// The required labels hold __error__, so parsing never stops early.
func lokiJSONAccepts(line string, required []string) bool {
	var walk func(data []byte, path [][]byte) error
	walk = func(data []byte, path [][]byte) error {
		return jsonparser.ObjectEach(data, func(key, value []byte, kind jsonparser.ValueType, _ int) error {
			if kind != jsonparser.Object {
				return nil
			}
			next := append(append([][]byte(nil), path...), append([]byte(nil), key...))
			var sanitized []byte
			for i, part := range next {
				part = []byte(strings.TrimSpace(string(part)))
				if len(part) == 0 {
					continue
				}
				if i > 0 && len(sanitized) > 0 {
					sanitized = append(sanitized, '_')
				}
				if len(sanitized) == 0 && part[0] >= '0' && part[0] <= '9' {
					sanitized = append(sanitized, '_')
				}
				for _, r := range string(part) {
					if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
						sanitized = append(sanitized, byte(r))
					} else {
						sanitized = append(sanitized, '_')
					}
				}
			}
			descend := required == nil
			for _, label := range required {
				descend = descend || strings.HasPrefix(label, string(sanitized))
			}
			if !descend {
				return nil
			}
			return walk(value, next)
		})
	}
	return walk([]byte(line), nil) == nil
}

// jsonSplitClass reports how the split pushdown files a line: counted by
// VictoriaLogs (the spliced line parses and holds no hazard), read as a raw
// row (it opens with a brace after JSON whitespace), or dropped. The
// predicates are the LogsQL the pushdown sends, evaluated with VictoriaLogs
// semantics.
func jsonSplitClass(t testing.TB, line string) (counted, rawRow bool) {
	values := map[string]string{"_msg": line}
	if !fakeLogsQLMatch(t, jsonObjectStartFilter, values) {
		return false, false
	}
	spliced := line
	if strings.HasPrefix(line, "{") {
		spliced = `{"` + jsonValidField + `":1,` + line[1:]
	}
	valid := vlFastjsonUnpackFields(spliced)[jsonValidField] != ""
	if valid && !fakeLogsQLMatch(t, jsonLokiRejectHazard, values) {
		return true, false
	}
	return false, true
}

// jsonSplitHintSets are the parser hints the differential checks: every object
// walked (the strictest), a query with no parsed label, and two that reach
// into nested objects.
var jsonSplitHintSets = [][]string{nil, {"__error__"}, {"a", "__error__"}, {"a_b", "__error__"}}

var jsonSplitSeeds = []string{
	`{"service": {"name": "api-gateway"}, "method": "PATCH", "status": 502, "ok": true, "x": null, "arr": [1, "a", {"b": 2}], "level": "error"}`,
	`{"a":1}`, `{}`, `{ }`, `{"a":{"b":{"c":"d"}}}`, `{"a":"b\"c","d\\e":1}`, `{"kéy":"v"}`,
	`{"a":NaN}`, `{"a":Infinity}`, `{"a":-Infinity}`, `{"a":.5}`, `{"a":+1}`, `{"a":e5}`, `{"a":-}`, `{"a":1e5}`,
	`{"a":"\ud800"}`, `{"\ud800":1}`, `{"\udc00x":1}`, `{"a\qb":1}`, `{"a":1,}`, `{"a":1} trailing`, `  {"a":1}`,
	`{"a":[NaN, .5, +1]}`, `{"a":{"b":NaN}}`, `{"":1}`, `{"a":1,"a":2}`, "{\"a\":\"\xff\"}", "{\"a\":\"x\ny\"}",
	`{"a":tru}`, `{"a":nul}`, `{"a":nan}`, `{"a":nAn}`, `{"a":inf}`, `{"a":-nan}`, `{"a":1.2.3}`, `{"a":--1}`,
	"\f{\"a\":1}", "{\"a\":\fNaN}", `{"a_b":{"c":NaN}}`, `{"a":{"b":.5}}`, `{"x":{"y":+1}}`,
}

var (
	jsonSplitTokens = []string{"1", "-1", "0.5", "1e5", "-0", ".5", "+1", "e5", "E1", "NaN", "nan", "nAn", "-NaN", "Infinity", "inf", "-inf", "+inf", "INF", "-", "1.2.3", "--1",
		"true", "false", "null", `"s"`, `"a\"b"`, `"\ud800"`, `"A"`, `"\q"`, `"�"`, `[]`, `[1,NaN]`, `[.5]`, `{}`, `{"x":1}`}
	jsonSplitKeys   = []string{"a", "b", "a_b", "", " a ", `a\"b`, `a\\b`, `A`, `\ud800`, `\udc00`, `a\qb`, "é", "1a", "__vlp", "a.b"}
	jsonSplitSpaces = []string{"", " ", "  ", "\t", "\n", "\r", "\f"}
	jsonSplitAlpha  = []string{"{", "}", "[", "]", `"`, ":", ",", `\`, ".", "e", "E", "+", "-", "0", "1", "9", " ", "\t", "\n", "\r", "\f",
		"t", "r", "u", "f", "a", "l", "s", "n", "N", "I", "i", "x", `\u`, `\ud800`, `\udc00`, `A`, "NaN", "Inf", "inf", "nan",
		"true", "false", "null", "�", "é", `\"`, `\\`, `":`, `":1`, `,"k":`, `{"`, `"}`}
)

func jsonSplitObject(rng *rand.Rand, depth int) string {
	var b strings.Builder
	b.WriteString("{")
	for i, n := 0, rng.Intn(5); i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(jsonSplitSpaces[rng.Intn(len(jsonSplitSpaces))] + `"` + jsonSplitKeys[rng.Intn(len(jsonSplitKeys))] + `"`)
		b.WriteString(jsonSplitSpaces[rng.Intn(len(jsonSplitSpaces))] + ":" + jsonSplitSpaces[rng.Intn(len(jsonSplitSpaces))])
		if depth < 3 && rng.Intn(4) == 0 {
			b.WriteString(jsonSplitObject(rng, depth+1))
		} else {
			b.WriteString(jsonSplitTokens[rng.Intn(len(jsonSplitTokens))])
		}
	}
	if rng.Intn(20) == 0 {
		b.WriteString(",")
	}
	b.WriteString(jsonSplitSpaces[rng.Intn(len(jsonSplitSpaces))] + "}")
	return b.String()
}

func jsonSplitMutate(rng *rand.Rand, s string) string {
	for i, n := 0, 1+rng.Intn(4); i < n; i++ {
		pos := rng.Intn(len(s) + 1)
		switch rng.Intn(3) {
		case 0:
			s = s[:pos] + jsonSplitAlpha[rng.Intn(len(jsonSplitAlpha))] + s[pos:]
		case 1:
			if pos < len(s) {
				s = s[:pos] + s[min(len(s), pos+1+rng.Intn(3)):]
			}
		default:
			if pos < len(s) {
				s = s[:pos] + jsonSplitAlpha[rng.Intn(len(jsonSplitAlpha))] + s[pos+1:]
			}
		}
	}
	return s
}

// jsonSplitLine draws one line: a generated object, possibly mutated, with
// leading space or trailing text now and then, or a mutated seed.
func jsonSplitLine(rng *rand.Rand) string {
	if rng.Intn(3) == 0 {
		return jsonSplitMutate(rng, jsonSplitSeeds[rng.Intn(len(jsonSplitSeeds))])
	}
	s := jsonSplitObject(rng, 0)
	if rng.Intn(4) == 0 {
		s = jsonSplitMutate(rng, s)
	}
	if rng.Intn(10) == 0 {
		s = jsonSplitSpaces[rng.Intn(len(jsonSplitSpaces))] + s
	}
	if rng.Intn(10) == 0 {
		s += []string{" trailing", "}", ",", "\n", " "}[rng.Intn(5)]
	}
	return s
}

// checkJSONSplitLine asserts the split's soundness for one line: a line
// VictoriaLogs counts is accepted by Loki's parser under every hint set (so
// Loki counts it once and VictoriaLogs' count is exact), and a line the split
// drops is rejected under every hint set (so Loki drops it too).
func checkJSONSplitLine(t testing.TB, line string) (counted, rawRow bool) {
	counted, rawRow = jsonSplitClass(t, line)
	for _, hints := range jsonSplitHintSets {
		accepted := lokiJSONAccepts(line, hints)
		switch {
		case counted && !accepted:
			t.Errorf("VictoriaLogs counts %q, Loki's parser rejects it (hints %q)", line, hints)
		case !counted && !rawRow && accepted:
			t.Errorf("the split drops %q, Loki's parser accepts it (hints %q)", line, hints)
		}
	}
	return counted, rawRow
}

// The lines the split pushdown lets VictoriaLogs count are a subset of the
// lines Loki's JSON parser accepts under any parser hints, and the lines it
// drops are a subset of the lines Loki rejects; everything else is read as a
// raw row and evaluated by the row evaluator. Checked over a seeded corpus of
// generated and mutated JSON (non-standard numbers, NaN and Inf, escaped and
// broken keys, trailing text, leading whitespace, nesting), and with the
// hazard exclusion removed the same corpus must find lines VictoriaLogs
// parses and Loki rejects, so the corpus exercises the exclusion.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitPredicateDifferential(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 40_000
	}
	rng := rand.New(rand.NewSource(20261012))
	counted, rawRows, unguarded := 0, 0, 0
	for i := 0; i < n; i++ {
		line := jsonSplitSeeds[i%len(jsonSplitSeeds)]
		if i >= len(jsonSplitSeeds) {
			line = jsonSplitLine(rng)
		}
		c, r := checkJSONSplitLine(t, line)
		if c {
			counted++
		}
		if r {
			rawRows++
			spliced := line
			if strings.HasPrefix(line, "{") {
				spliced = `{"` + jsonValidField + `":1,` + line[1:]
			}
			if vlFastjsonUnpackFields(spliced)[jsonValidField] != "" && !lokiJSONAccepts(line, nil) {
				unguarded++ // only the hazard exclusion keeps this line from VictoriaLogs' count
			}
		}
		if t.Failed() {
			return
		}
	}
	t.Logf("lines=%d counted by VictoriaLogs=%d raw rows=%d hazard-excluded Loki rejects=%d", n, counted, rawRows, unguarded)
	if counted < n/50 || unguarded < n/1000 {
		t.Fatalf("the corpus no longer exercises the split: counted=%d hazard-excluded=%d", counted, unguarded)
	}
}

// FuzzJSONValidSplitPredicate runs the differential over fuzzed lines.
func FuzzJSONValidSplitPredicate(f *testing.F) {
	for _, seed := range jsonSplitSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		checkJSONSplitLine(t, line)
	})
}

// The hazard filter flags each construct VictoriaLogs' parser accepts and
// Loki's rejects, anywhere in the line, and leaves ordinary JSON alone.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONLokiRejectHazardTable(t *testing.T) {
	for _, tc := range []struct {
		line           string
		hazard, counts bool
	}{
		{`{"a":1,"b":"x","c":true,"d":null,"e":[1,2],"f":{"g":-1.5e3}}`, false, true},
		{`{"a":NaN}`, true, false},
		{`{"a": nan}`, true, false},
		{`{"a":INF}`, true, false},
		{`{"a":-NaN}`, false, true}, // jsonparser reads any token starting with '-' as a number
		{`{"a":"NaN"}`, false, true},
		{`{"a":.5}`, true, false},
		{`{"a":e5}`, true, false},
		{`{"a":E5}`, true, false},
		{`{"a":+1}`, true, false},
		{"{\"a\":\t+1}", true, false},
		{`{"a\qb":1}`, true, false},
		{`{"\ud800":1}`, true, false},
		{`{"a\"b":1}`, true, false},
		{`{"a":{"b":NaN}}`, true, false},
		{`{"msg":"took 5ms: e"}`, true, false}, // inside a string: a superset, read as a raw row
		{`{"a":"\\"}`, false, true},
		{`{"a":"\""}`, false, true},
		{`{"url":"http://x/a+b"}`, false, true},
		{`{"n":"inf"}`, false, true},
		{`{"level":"info"}`, false, true},
	} {
		t.Run(tc.line, func(t *testing.T) {
			if got := fakeLogsQLMatch(t, jsonLokiRejectHazard, map[string]string{"_msg": tc.line}); got != tc.hazard {
				t.Fatalf("hazard=%v, want %v", got, tc.hazard)
			}
			if counted, _ := checkJSONSplitLine(t, tc.line); counted != tc.counts {
				t.Fatalf("counted by VictoriaLogs=%v, want %v", counted, tc.counts)
			}
		})
	}
}

// Which `| json | __error__=""` metrics take the split pushdown.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitEligibility(t *testing.T) {
	for _, tc := range []struct {
		query               string
		pushdown, validOnly bool
	}{
		{`sum by (app) (count_over_time({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum(count_over_time({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum by (method) (rate({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum by (level, detected_level) (bytes_over_time({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum by (method) (count_over_time({env="production"} |= "GET" | json | __error__="" | status="500" [5m]))`, true, true},
		{`sum by (method) (count_over_time({env="production"} | json | __error__="" | drop __error_details__ [5m]))`, true, true},
		// detected_level may be grouped by (the stored level fields decide it),
		// not filtered on: the filter would need the derived level for the
		// lines read as raw rows.
		{`sum by (detected_level) (count_over_time({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum by (app, detected_level) (rate({env="production"} | json | __error__="" [5m]))`, true, true},
		{`sum by (method) (count_over_time({env="production"} | json | __error__="" | detected_level="info" [5m]))`, false, false},
		{`sum by (detected_level) (count_over_time({env="production"} | json | __error__="" | detected_level!="debug" [5m]))`, false, false},
		// service_name is the stream label Loki assigns, derived before any unpack.
		{`sum by (service_name) (count_over_time({service_name="api-gateway"} | json | __error__="" [5m]))`, true, true},
		{`sum by (service_name, method) (count_over_time({env="production"} | json | __error__="" [5m]))`, true, true},
		// After a drop of __error__ the filter passes every line: the plain pushdown.
		{`sum by (method) (count_over_time({env="production"} | json | drop __error__ | __error__="" [5m]))`, true, false},
		{`sum by (method) (count_over_time({env="production"} | json | __error__!="" [5m]))`, false, false},
		{`sum by (method) (count_over_time({env="production"} | json | __error__="JSONParserErr" [5m]))`, false, false},
		{`sum by (method) (count_over_time({env="production"} | logfmt | __error__="" [5m]))`, false, false},
		{`sum without (method) (count_over_time({env="production"} | json | __error__="" [5m]))`, false, false},
		{`count_over_time({env="production"} | json | __error__="" [5m])`, false, false},
		// The split blanks the grouped fields of the lines it reads as raw rows:
		// _msg, _time and _stream are not fields to blank, so these keep the
		// row evaluator they always had.
		{`sum by (_msg) (count_over_time({env="production"} | json | __error__="" [5m]))`, false, false},
		{`sum by (_stream) (count_over_time({env="production"} | json | __error__="" [5m]))`, false, false},
		{`sum by (app, _time) (count_over_time({env="production"} | json | __error__="" [5m]))`, false, false},
		{`sum by (app) (count_over_time({env="production"} | json | __error__="" | _msg="x" [5m]))`, false, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				if tc.pushdown {
					t.Fatal("expected an ordered JSON plan")
				}
				return
			}
			if plan.pushdown != tc.pushdown || plan.validOnly != tc.validOnly {
				t.Fatalf("pushdown=%v validOnly=%v, want %v %v", plan.pushdown, plan.validOnly, tc.pushdown, tc.validOnly)
			}
		})
	}
}

// jsonSplitRows is a seeded fixture of the shapes the split must answer like
// Loki: valid JSON of several apps (one with the level in structured
// metadata), lines VictoriaLogs parses and Loki rejects, truncated and
// trailing-text JSON, leading whitespace, plain and logfmt lines.
func jsonSplitRows(rng *rand.Rand, s0 time.Time, n int, hazards bool) []pushdownRow {
	statuses := []string{"200", "404", "500"}
	var rows []pushdownRow
	for i := 0; i < n; i++ {
		ts := s0.Add(time.Duration(rng.Intn(600_000)) * time.Millisecond)
		if i%25 == 0 { // on an evaluation time or a window edge
			ts = s0.Add(time.Duration(rng.Intn(11)) * time.Minute)
		}
		app := []string{"api", "web", "worker"}[rng.Intn(3)]
		row := pushdownRow{ts: ts, stream: map[string]string{"app": app, "env": "prod"}}
		status := statuses[rng.Intn(len(statuses))]
		switch k := rng.Intn(20); {
		case k < 12:
			row.msg = `{"status":"` + status + `","method":"GET","n":` + strconv.Itoa(i) + `}`
		case k == 12:
			row.msg = `{"status":` + status + `,"nested":{"a":{"b":1}},"arr":[1,2]}`
		case k == 13:
			row.msg = `{"status":"` + status + `","msg":"truncated`
		case k == 14:
			row.msg = `{"status":"` + status + `"} trailing`
		case k == 15:
			row.msg = "  {\"status\":\"" + status + "\"}"
		case k == 16:
			row.msg = "plain text " + strconv.Itoa(i)
		case k == 17:
			row.msg = "status=" + status + " method=GET"
		case k == 18 && hazards:
			row.msg = []string{`{"status":"` + status + `","ratio":NaN}`, `{"status":"` + status + `","v":.5}`, `{"st\qatus":"` + status + `"}`}[rng.Intn(3)]
		default:
			row.stream["level"] = "info"
			row.msg = `{"status":"` + status + `","level":"error"}`
		}
		rows = append(rows, row)
	}
	return rows
}

func runSplitQuery(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) (int, map[string]map[int64]string, string) {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		return rec.Code, nil, rec.Body.String()
	}
	return rec.Code, decodeExtractedMatrix(t, rec.Body.Bytes()), ""
}

var jsonSplitQueries = []string{
	`sum(count_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (app) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (status) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (app) (rate({env="prod"} | json | __error__="" [2m]))`,
	`sum by (status) (bytes_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (app) (bytes_rate({env="prod"} | json | __error__="" [1m]))`,
	`sum by (app) (count_over_time({env="prod"} | json | __error__="" | status="500" [1m]))`,
	`sum by (level, detected_level) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (service_name) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
	`sum by (service_name, status) (bytes_over_time({env="prod"} | json | __error__="" [1m]))`,
}

// withStoredLevels gives every row a level the way a stream label stores it,
// in several spellings of the same few levels.
func withStoredLevels(rows []pushdownRow) []pushdownRow {
	spellings := []string{"info", "INFO", "error", "Warning", "inf", "debug"}
	out := make([]pushdownRow, len(rows))
	for i, row := range rows {
		stream := map[string]string{}
		for k, v := range row.stream {
			stream[k] = v
		}
		stream["level"] = spellings[i%len(spellings)]
		row.stream = stream
		out[i] = row
	}
	return out
}

// assertStoredLevelStats fails when a stats query sent for a split plan
// grouped by detected_level is not the stored-level form: the copies of the
// stored fields, and none of the pipes, probes or flags of the derivation chain.
func assertStoredLevelStats(t *testing.T, fake *pushdownFakeVL, label string) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stats) == 0 {
		t.Fatalf("%s: no stats query", label)
	}
	for _, stats := range fake.stats {
		if !strings.Contains(stats, "copy detected_level as "+storedDetectedLevelField+", level as "+storedLevelField) {
			t.Fatalf("%s: stats query does not read the stored level fields: %s", label, stats)
		}
		if strings.Contains(stats, `result_prefix "__j_"`) || strings.Contains(stats, levelWitnessField) || strings.Contains(stats, "__dl_h") {
			t.Fatalf("%s: stats query carries the derivation chain: %s", label, stats)
		}
		if strings.Index(stats, "copy detected_level") > strings.Index(stats, jsonValidField) {
			t.Fatalf("%s: the stored level fields are copied after the sentinel splice: %s", label, stats)
		}
	}
	if len(fake.presenceCalls) != 0 {
		t.Fatalf("%s: %d level presence probes sent", label, len(fake.presenceCalls))
	}
}

// rowEvaluatorMatrix answers plan with the proxy's row evaluator over every
// selected row, the path these shapes took before the split.
func rowEvaluatorMatrix(t *testing.T, p *Proxy, plan *orderedJSONMetricPlan, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	series, err := p.collectOrderedJSONMetric(context.Background(), plan, start, end, step)
	if err != nil {
		t.Fatalf("row evaluator: %v", err)
	}
	body, err := buildOrderedJSONMetric(context.Background(), plan, series, start, end, step, true, 1<<30)
	if err != nil {
		t.Fatalf("row evaluator: %v", err)
	}
	return decodeExtractedMatrix(t, body)
}

// The split pushdown answers every `| json | __error__=""` shape exactly as
// the row evaluator (whose parser follows Loki's), over a seeded fixture with
// VictoriaLogs' own parser semantics in the fake backend: one stats scan, a
// raw read only of the buckets holding lines VictoriaLogs cannot vouch for,
// and a second scan only when the hazard probe finds a line.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitMatchesRawEvaluator(t *testing.T) {
	seeds := 10
	if testing.Short() {
		seeds = 4
	}
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		hazards := seed%2 == 0
		mixed := jsonSplitRows(rng, s0, 400, hazards)
		for _, query := range jsonSplitQueries {
			// A query grouped by detected_level is served by the split only when
			// the stored level fields decide the level of every counted line;
			// the fixture with body-only levels leaves it to the row evaluator.
			// Either way the answer is the row evaluator's.
			variants := []bool{false}
			if strings.Contains(query, "detected_level") {
				variants = []bool{true, false}
			}
			for _, stored := range variants {
				rows := mixed
				if stored {
					rows = withStoredLevels(mixed)
				}
				served := stored || !strings.Contains(query, "detected_level")
				label := fmt.Sprintf("seed %d stored=%v: %s", seed, stored, query)
				srv, fake := newPushdownFakeVL(t, rows, nil)
				fake.jsonUnpack = vlFastjsonUnpackFields
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				plan, ok := compileOrderedJSONMetric(query)
				if !ok || !plan.pushdown || !plan.validOnly {
					t.Fatalf("%s: expected a split pushdown plan", label)
				}
				want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
				fake.mu.Lock()
				fake.raw, fake.rawQueries, fake.stats, fake.guards = 0, nil, nil, nil
				fake.mu.Unlock()
				code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
				if code != http.StatusOK {
					t.Fatalf("%s: status %d: %s", label, code, body)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: differs from the row evaluator\n got: %v\nwant: %v", label, got, want)
				}
				fake.mu.Lock()
				stats, rawQueries := len(fake.stats), fake.rawQueries
				fake.mu.Unlock()
				if !served {
					// The stored fields do not decide every line: the row evaluator
					// read every selected row and derived each level itself.
					full := false
					for _, raw := range rawQueries {
						full = full || !strings.Contains(raw.query, " | filter "+jsonInvalidCondition(hazards))
					}
					if !full {
						t.Fatalf("%s: expected the row evaluator over every row, got %d bounded raw reads", label, len(rawQueries))
					}
					continue
				}
				wantStats := 1
				if hazards {
					wantStats = 2
				}
				if stats != wantStats || len(rawQueries) == 0 || len(rawQueries) > maxOrderedJSONRowSpans {
					t.Fatalf("%s: stats=%d raw=%d, want %d stats scans and a bounded raw read", label, stats, len(rawQueries), wantStats)
				}
				for _, raw := range rawQueries {
					if !strings.Contains(raw.query, " | filter "+jsonInvalidCondition(hazards)) {
						t.Fatalf("%s: raw read %q is not limited to the lines VictoriaLogs cannot vouch for", label, raw.query)
					}
				}
				if strings.Contains(query, "detected_level") {
					assertStoredLevelStats(t, fake, label)
				}
			}
		}
	}
}

// With only lines VictoriaLogs vouches for, the split is one stats scan:
// no raw row is read, and the raw read of an invalid line covers only the
// buckets around it and keeps only the fields the evaluator reads.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitReadsOnlyRawRowBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i)*5*time.Second + time.Millisecond), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"status":"200"}`})
	}
	query := `sum by (status) (count_over_time({env="prod"} | json | __error__="" [1m]))`
	run := func(rows []pushdownRow) *pushdownFakeVL {
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.jsonUnpack = vlFastjsonUnpackFields
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		plan, _ := compileOrderedJSONMetric(query)
		want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
		code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
		if code != http.StatusOK || !reflect.DeepEqual(got, want) {
			t.Fatalf("status %d %s\n got: %v\nwant: %v", code, body, got, want)
		}
		return fake
	}
	if fake := run(rows); fake.raw != 0 || len(fake.stats) != 1 {
		t.Fatalf("expected one stats scan and no raw rows: stats=%d raw=%d", len(fake.stats), fake.raw)
	}
	broken := pushdownRow{ts: s0.Add(5*time.Minute + 7*time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"status":"500"} trailing`}
	fake := run(append(append([]pushdownRow(nil), rows...), broken))
	if len(fake.rawQueries) != 1 {
		t.Fatalf("expected one raw read, got %d", len(fake.rawQueries))
	}
	raw := fake.rawQueries[0]
	if span := time.Duration(raw.end - raw.start); span > 2*time.Minute || raw.start > broken.ts.UnixNano() || raw.end <= broken.ts.UnixNano() {
		t.Fatalf("raw read [%d, %d) is not the buckets around the line at %d", raw.start, raw.end, broken.ts.UnixNano())
	}
	if !strings.Contains(raw.query, " | fields _time, _stream, _msg, level, ") {
		t.Fatalf("raw read %q keeps every field", raw.query)
	}
}

// A line VictoriaLogs vouches for is counted however wide and up to 300 levels
// deep (fastjson MaxDepth) it is: the
// row evaluator's guards (nesting depth, field count, sizes) now apply only to
// the raw rows. Loki has no such guard, so the count matches Loki.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitCountsDeepLinesLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	deep := strings.Repeat(`{"a":`, 80) + "1" + strings.Repeat("}", 80)
	rows := []pushdownRow{
		{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: deep},
		{ts: s0.Add(40 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"b":1}`},
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	if !lokiJSONAccepts(deep, []string{"app", "__error__"}) {
		t.Fatal("Loki's parser must accept the deep line")
	}
	code, got, body := runSplitQuery(t, p, `sum by (app) (count_over_time({env="prod"} | json | __error__="" [1m]))`, s0.Add(time.Minute), s0.Add(time.Minute), time.Minute)
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	if want := map[string]map[int64]string{`{app="api"}`: {s0.Add(time.Minute).Unix(): "2"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// The raw rows keep the fields the row evaluator reads: the stream, the line,
// level and every stored field that can expose a required label.
func TestOrderedJSONRowProjection(t *testing.T) {
	p := &Proxy{labelTranslator: NewLabelTranslator(LabelStyleUnderscores, nil)}
	for _, tc := range []struct{ query, want string }{
		{`sum by (app) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
			" | fields _time, _stream, _msg, level, __error__, app, app*"},
		{`sum by (service_version) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
			" | fields _time, _stream, _msg, level, __error__, service_version, `service.version`, service*"},
		{`sum by (level_extracted) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
			" | fields _time, _stream, _msg, level, __error__, level*, level_extracted"},
		// No hints: every label can matter.
		{`sum without (app) (count_over_time({env="prod"} | json | __error__="" [1m]))`, ""},
		// Sanitized from a field without a common prefix (a leading digit or symbol).
		{`sum by (key_1a) (count_over_time({env="prod"} | json | __error__="" [1m]))`, ""},
		{`sum by (_x) (count_over_time({env="prod"} | json | __error__="" [1m]))`, ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				t.Fatal("expected an ordered JSON plan")
			}
			if got := p.orderedJSONRowProjection(plan); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Loki's JSON parser skips only space, tab, line feed and carriage return
// before the opening brace, so a line starting with a form feed fails the
// query like any other unparsed line, found by the one-row lookup.
// conformance: semantics/pipeline-error-text-parity
func TestOrderedJSONPipelineErrorFormFeedLine(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api"}, msg: `{"status":"200"}`},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api"}, msg: "\f{\"status\":\"500\"}"},
	}
	if lokiJSONAccepts(rows[1].msg, nil) {
		t.Fatal("Loki's parser must reject a line opening with a form feed")
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	params := url.Values{"query": {`sum by (status) (count_over_time({app="api"} | json [1m]))`}, "start": {strconv.FormatInt(s0.Add(time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(s0.Add(time.Minute).UnixNano(), 10)}, "step": {"60"}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "JSONParserErr") {
		t.Fatalf("got %d %s, want Loki's pipeline error", rec.Code, rec.Body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 {
		t.Fatalf("expected the error from the one-row lookup, read %d raw scans", fake.raw)
	}
}

// orderedJSONRowSpans merges touching buckets and collapses many spans.
func TestOrderedJSONRowSpans(t *testing.T) {
	b := 10 * time.Second
	s := int64(b)
	if got := orderedJSONRowSpans([]int64{3 * s, s, 2 * s, 7 * s}, b); len(got) != 2 || got[0][0].UnixNano() != s || got[0][1].UnixNano() != 4*s || got[1][0].UnixNano() != 7*s || got[1][1].UnixNano() != 8*s {
		t.Fatalf("got %v", got)
	}
	var many []int64
	for i := 0; i < 3*maxOrderedJSONRowSpans; i++ {
		many = append(many, int64(i)*3*s)
	}
	if got := orderedJSONRowSpans(many, b); len(got) != 1 || got[0][0].UnixNano() != 0 || got[0][1].UnixNano() != many[len(many)-1]+s {
		t.Fatalf("got %v", got)
	}
}

// service_name derived for a stream whose source field is structured metadata
// only: the stats half and the raw-row half must agree with the row evaluator.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitServiceNameFromMetadata(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 30; i++ {
		ts := s0.Add(time.Duration(i)*7*time.Second + time.Millisecond)
		rows = append(rows,
			pushdownRow{ts: ts, stream: map[string]string{"env": "prod"}, vl: map[string]string{"app": "meta-app"}, loki: map[string]string{"app": "meta-app"}, msg: `{"status":"200"}`},
			pushdownRow{ts: ts.Add(time.Second), stream: map[string]string{"env": "prod"}, vl: map[string]string{"app": "meta-app"}, loki: map[string]string{"app": "meta-app"}, msg: `{"status":"500"} trailing`},
			pushdownRow{ts: ts.Add(2 * time.Second), stream: map[string]string{"env": "prod", "app": "stream-app"}, msg: `{"status":"200"}`},
		)
	}
	for _, query := range []string{
		`sum by (service_name) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
		`sum by (service_name, status) (count_over_time({env="prod"} | json | __error__="" [1m]))`,
	} {
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.jsonUnpack = vlFastjsonUnpackFields
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		plan, _ := compileOrderedJSONMetric(query)
		want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
		code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
		if code != http.StatusOK || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: status %d %s\n got: %v\nwant: %v", query, code, body, got, want)
		}
	}
}

// Groups made only of lines the label filter drops must not exist: VictoriaLogs
// emits a group (with zero counts) for every row reaching stats, so the filter
// is applied before stats and those groups never count against the series
// limit. 20 distinct paths overall, 2 with status 500, limit 5: Loki answers
// the two series.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitFilteredOutGroupsStayUnderSeriesLimit(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 20; i++ {
		status := "200"
		if i < 2 {
			status = "500"
		}
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"path":"/p%d","status":"%s"}`, i, status)})
	}
	for _, function := range []string{"count_over_time", "bytes_over_time"} {
		query := `sum by (path) (` + function + `({env="prod"} | json | __error__="" | status="500" [5m]))`
		t.Run(function, func(t *testing.T) { runSplitFilteredOutGroups(t, rows, query, start, end) })
	}
}

func runSplitFilteredOutGroups(t *testing.T, rows []pushdownRow, query string, start, end time.Time) {
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 5
	plan, _ := compileOrderedJSONMetric(query)
	if !plan.validOnly {
		t.Fatal("plan must be a split plan")
	}
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	if len(want) != 2 {
		t.Fatalf("fixture: row evaluator answers %d series, want 2", len(want))
	}
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK {
		t.Fatalf("split status %d while Loki answers: %s", code, body)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split %v, row evaluator %v", got, want)
	}
}

// A line VictoriaLogs cannot parse forms an extra empty-label group (zero
// counts, rows only): it is not a series Loki returns, so it does not count
// against the series limit. Exactly 5 real paths with limit 5 answer.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitRawOnlyGroupIsNotASeries(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 5; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"path":"/p%d"}`, i)})
	}
	rows = append(rows, pushdownRow{ts: s0.Add(150 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"path":"/p0"} trailing`})
	query := `sum by (path) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 5
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	if len(want) != 5 {
		t.Fatalf("fixture: row evaluator answers %d series, want 5", len(want))
	}
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK {
		t.Fatalf("split status %d while Loki answers: %s", code, body)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split %v, row evaluator %v", got, want)
	}
	// One more real path is a real overflow, with Loki's error.
	rows = append(rows, pushdownRow{ts: s0.Add(160 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"path":"/p9"}`})
	fake.rows = rows
	if code, _, body := runSplitQuery(t, p, query, start, end, time.Minute); code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series") {
		t.Fatalf("six real series: status %d body %s, want the series limit error", code, body)
	}
}

// The service name VictoriaLogs derives from the stream labels is the first
// source field that is non-empty (deriveServiceName), whatever the value holds:
// a label value with a line feed is a name too.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitServiceNameFromStreamLabelWithNewline(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api\nx", "env": "prod"}, msg: `{"a":1}`},
		{ts: s0.Add(110 * time.Second), stream: map[string]string{"app": "web", "env": "prod"}, msg: `{"a":1}`},
		{ts: s0.Add(120 * time.Second), stream: map[string]string{"env": "prod"}, msg: `{"a":1}`},
	}
	query := `sum by (service_name) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, _ := compileOrderedJSONMetric(query)
	if !plan.validOnly || !plan.groupServiceName {
		t.Fatal("plan must be a split plan grouped by service_name")
	}
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("split %d %v %s, row evaluator %v", code, got, body, want)
	}
	if len(want) != 3 {
		t.Fatalf("fixture: want 3 service names, got %v", want)
	}
}

// The hazard probe covers the lines that open as the split reads them (a brace
// after JSON whitespace), the same filter the split applies. VictoriaLogs
// v1.52 trims JSON whitespace before unpack_json's brace check where v1.47 and
// v1.40 do not, so a probe anchored at the first byte would be right on one
// and wrong on the other.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitHazardProbeUsesTheSplitBraceFilter(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"a":1}`},
		{ts: s0.Add(110 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: " \t{\"a\":.5}"},
	}
	query := `sum(count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("split %d %v %s, row evaluator %v", code, got, body, want)
	}
	probes := 0
	for _, guard := range fake.guards {
		if strings.Contains(guard, jsonLokiRejectHazard) {
			probes++
			if !strings.Contains(guard, "| filter "+jsonObjectStartFilter+" ") {
				t.Fatalf("hazard probe %q does not use the split's brace filter", guard)
			}
		}
	}
	if probes == 0 {
		t.Fatal("no hazard probe ran")
	}
}

// VictoriaLogs v1.52 trims JSON whitespace around a value before unpack_json
// checks for the opening brace (pipe_unpack_json.go trimJSONWhitespace).
func TestVLFastjsonUnpackFieldsTrimsJSONWhitespace(t *testing.T) {
	if got := vlFastjsonUnpackFields(" \t\r\n{\"a\":1} \n"); !reflect.DeepEqual(got, map[string]string{"a": "1"}) {
		t.Fatalf("got %v", got)
	}
	if got := vlFastjsonUnpackFields("\f{\"a\":1}"); got != nil {
		t.Fatalf("a form feed is not JSON whitespace, got %v", got)
	}
}

// A positive hazard probe discards the first scan, so the scan is cancelled
// instead of awaited: the answer comes from the second one without waiting for
// the first to finish.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitHazardProbeCancelsTheFirstScan(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"a":1}`},
		{ts: s0.Add(110 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"a":.5}`},
	}
	query := `sum(count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	cancelled := make(chan struct{})
	var sent atomic.Int64
	fake.onStats = func(r *http.Request, q string) {
		if strings.Contains(q, jsonLokiRejectHazard) {
			return
		}
		// The first scan, without the hazard exclusion: it never finishes on
		// its own.
		sent.Add(1)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-time.After(3 * time.Second):
		}
	}
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	began := time.Now()
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("split %d %v %s, row evaluator %v", code, got, body, want)
	}
	if elapsed := time.Since(began); elapsed > 2500*time.Millisecond {
		t.Fatalf("answered in %s: the first scan was awaited", elapsed)
	}
	select {
	case <-cancelled: // the server sees the closed connection asynchronously
	case <-time.After(2 * time.Second):
		// The scan may also be cancelled before its request leaves the proxy.
		if sent.Load() != 0 {
			t.Fatal("the first scan was not cancelled")
		}
	}
}

// The series limit applies to the merged answer, never to the groups
// VictoriaLogs returns: 8 groups under a limit of 5 (at most twice the limit,
// so the read holds them all) merge into 8 series and end with Loki's error
// before any raw row is read, with or without lines read as raw rows.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitMergedAnswerOverTheLimitReturnsLokiError(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	for _, withRaw := range []bool{false, true} {
		var rows []pushdownRow
		for i := 0; i < 8; i++ {
			rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
		}
		if withRaw {
			// A line only Loki's parser accepts, so the plan also reads raw rows.
			rows = append(rows, pushdownRow{ts: s0.Add(150 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"trace_id":"t0"} trailing`})
		}
		for _, function := range []string{"count_over_time", "bytes_over_time", "bytes_rate"} {
			query := `sum by (trace_id) (` + function + `({env="prod"} | json | __error__="" [5m]))`
			t.Run(fmt.Sprintf("raw=%v/%s", withRaw, function), func(t *testing.T) {
				srv, fake := newPushdownFakeVL(t, rows, nil)
				fake.jsonUnpack = vlFastjsonUnpackFields
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				p.maxStatsQuerySeries = 5
				code, _, body := runSplitQuery(t, p, query, start, end, time.Minute)
				if code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (5) reached for a single query") {
					t.Fatalf("status %d body %.200s, want Loki's series limit error", code, body)
				}
				if fake.raw != 0 {
					t.Fatalf("%d raw-row queries issued: the error comes from the split's merged answer", fake.raw)
				}
			})
		}
	}
}

// More than twice the series limit of groups is only a memory guard: the split
// does not answer and the row evaluator does, whatever the groups merge into.
// It ends with Loki's series limit error when its own series are over the
// limit, and answers when they are not. The response size cap leads to the
// same place.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitOverTwiceTheLimitGroupsFallBackToRowEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 60; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
	}
	query := `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	for _, tc := range []struct {
		name  string
		bytes int64
	}{
		{"group guard", maxStatsBucketResponseBytes},
		{"group guard before the size cap", 2500},
		{"too large to read", 600},
		{"smaller than a group", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := maxStatsBucketResponseBytes
			maxStatsBucketResponseBytes = tc.bytes
			t.Cleanup(func() { maxStatsBucketResponseBytes = old })
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			p.maxStatsQuerySeries = 5
			code, _, body := runSplitQuery(t, p, query, start, end, time.Minute)
			if code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (5) reached for a single query") {
				t.Fatalf("status %d body %.200s, want Loki's series limit error", code, body)
			}
			if fake.raw == 0 {
				t.Fatal("the row evaluator did not answer")
			}
			// The limit the row evaluator's series fit in answers.
			p.maxStatsQuerySeries = 60
			plan, _ := compileOrderedJSONMetric(query)
			want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
			if code, got, body := runSplitQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) || len(want) != 60 {
				t.Fatalf("limit 60: status %d %.200s, got %d series, want %d", code, body, len(got), len(want))
			}
		})
	}
}

// Level spellings Loki folds into one detected_level (info, INFO, inf and
// information are four VictoriaLogs groups, one series) are separate
// VictoriaLogs groups, which the series limit never counts: 8 spellings under
// a limit of 5 are 2 series. Loki derives detected_level at ingest, normalized
// (distributor/field_detection.go normalizeLogLevel), from a stream label or
// the JSON body; the expected answer counts the lines that way, with the level
// in the body or as a stream label and with or without a raw candidate (a line
// only Loki's parser accepts, which the row evaluator derives the level of
// too). Past the memory guard (8 groups under a limit of 3) the row evaluator
// answers, with the same series.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitLevelSpellingsMergeUnderTheLimit(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	levels := []string{"info", "INFO", "Info", "inf", "information", "warn", "WARN", "warning"}
	query := `sum by (detected_level) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	for _, inStream := range []bool{false, true} {
		for _, withRaw := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/raw=%v", inStream, withRaw), func(t *testing.T) {
				var rows []pushdownRow
				for i, level := range levels {
					row := pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"level":%q,"n":%d}`, level, i)}
					if inStream {
						row.stream["level"] = level
						row.msg = fmt.Sprintf(`{"n":%d}`, i)
					}
					rows = append(rows, row)
				}
				if withRaw {
					raw := pushdownRow{ts: s0.Add(150 * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: `{"level":"INFO"} trailing`}
					if inStream {
						raw.stream["level"] = "Info"
						raw.msg = `{"n":9} trailing`
					}
					rows = append(rows, raw)
				}
				want := map[string]map[int64]string{}
				for at := start; !at.After(end); at = at.Add(time.Minute) {
					counts := map[string]int{}
					for i, row := range rows {
						if row.ts.After(at.Add(-5*time.Minute)) && !row.ts.After(at) {
							level := "info" // the raw candidate
							if i < len(levels) {
								level = canonicalLevel([]byte(levels[i]))
							}
							counts[level]++
						}
					}
					for level, n := range counts {
						key := canonicalLabelsKey(map[string]string{"detected_level": level})
						if want[key] == nil {
							want[key] = map[int64]string{}
						}
						want[key][at.Unix()] = strconv.Itoa(n)
					}
				}
				if len(want) != 2 {
					t.Fatalf("fixture: %d expected series, want 2 (info, warn)", len(want))
				}
				srv, fake := newPushdownFakeVL(t, rows, nil)
				fake.jsonUnpack = vlFastjsonUnpackFields
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				plan, _ := compileOrderedJSONMetric(query)
				if !plan.validOnly || !plan.pushdown {
					t.Fatal("plan must be a split plan")
				}
				if got := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute); !reflect.DeepEqual(got, want) {
					t.Fatalf("row evaluator %v, want %v", got, want)
				}
				// 8 VictoriaLogs groups (9 with a raw candidate) under a limit of
				// 5 or 10: more groups than the limit, 2 series. The read holds
				// them (the guard is twice the limit), so the split answers.
				for _, limit := range []int{5, 10} {
					p.maxStatsQuerySeries = limit
					fake.raw = 0
					code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
					if code != http.StatusOK || !reflect.DeepEqual(got, want) {
						t.Fatalf("limit %d: status %d %.200s, got %v, want %v", limit, code, body, got, want)
					}
					if inStream && !withRaw && fake.raw != 0 {
						t.Fatalf("limit %d: %d raw-row queries issued", limit, fake.raw)
					}
					if !inStream && fake.raw == 0 {
						t.Fatalf("limit %d: a level held only by the body was counted from stats", limit)
					}
				}
				if inStream {
					assertStoredLevelStats(t, fake, "stored")
				}
				// Past the guard the row evaluator answers, the same series.
				p.maxStatsQuerySeries = 3
				fake.raw = 0
				code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
				if code != http.StatusOK || !reflect.DeepEqual(got, want) {
					t.Fatalf("limit 3: status %d %.200s, got %v, want %v", code, body, got, want)
				}
				if fake.raw == 0 {
					t.Fatal("limit 3: the row evaluator did not answer")
				}
			})
		}
	}
}

// A stats response too large to read does not fail a query within the series
// limit either: the raw evaluator answers it, as it did before the split.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitResponseTooLargeFallsBackToRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 12; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
	}
	query := `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	old := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = 600
	t.Cleanup(func() { maxStatsBucketResponseBytes = old })
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	fake.raw = 0
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) || len(want) != 12 {
		t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
	}
	if fake.raw == 0 {
		t.Fatal("the raw evaluator did not answer")
	}
}

// Groups the label filter drops stay out of the series count whatever the
// response size cap does: the filter runs before stats, so even a response
// stopped early by a small cap holds only real series.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitFilteredOutGroupsUnderSizeCap(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 20; i++ {
		status := "200"
		if i < 2 {
			status = "500"
		}
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"path":"/p%d","status":"%s"}`, i, status)})
	}
	for _, size := range []int64{1200, 2000, 4000} {
		for _, function := range []string{"count_over_time", "bytes_over_time"} {
			query := `sum by (path) (` + function + `({env="prod"} | json | __error__="" | status="500" [5m]))`
			t.Run(fmt.Sprintf("%s/%d", function, size), func(t *testing.T) {
				old := maxStatsBucketResponseBytes
				maxStatsBucketResponseBytes = size
				t.Cleanup(func() { maxStatsBucketResponseBytes = old })
				runSplitFilteredOutGroups(t, rows, query, start, end)
			})
		}
	}
}

// With the real series exactly at the limit plus the empty-label group the
// lines VictoriaLogs cannot parse are blanked into, the response read passes
// and the series merged after it are over the limit: the query ends with
// Loki's error before any raw row is read.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitSeriesLimitFailsBeforeRawRows(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	stream := map[string]string{"app": "api", "env": "prod"}
	var rows []pushdownRow
	for i := 0; i < 5; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: stream, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
	}
	// A valid line without the key is the series with no labels; the line
	// only Loki's parser accepts makes the plan read raw rows.
	rows = append(rows,
		pushdownRow{ts: s0.Add(110 * time.Second), stream: stream, msg: `{"other":1}`},
		pushdownRow{ts: s0.Add(150 * time.Second), stream: stream, msg: `{"trace_id":"t0"} trailing`})
	for _, function := range []string{"count_over_time", "bytes_over_time"} {
		query := `sum by (trace_id) (` + function + `({env="prod"} | json | __error__="" [5m]))`
		t.Run(function, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			p.maxStatsQuerySeries = 5
			code, _, body := runSplitQuery(t, p, query, start, end, time.Minute)
			if code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (5) reached for a single query") {
				t.Fatalf("status %d body %s, want Loki's series limit error", code, body)
			}
			if fake.raw != 0 {
				t.Fatalf("%d raw-row queries issued for a query over the series limit", fake.raw)
			}
			// One series fewer is within the limit.
			p.maxStatsQuerySeries = 6
			plan, _ := compileOrderedJSONMetric(query)
			want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
			if code, got, body := runSplitQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("limit 6: status %d %s, got %v, want %v", code, body, got, want)
			}
		})
	}
}

// A stats response is read only up to a bound, never whole: far more groups
// than the series limit end the read early instead of holding the response (a
// 300k group response is parsed into a 300k entry map otherwise). A plain
// pushdown stops at the series limit with Loki's error, a split read stops at
// twice the limit as a memory guard and says nothing of the series, because
// its groups merge into fewer series, and the query then goes to the row
// evaluator, which answers.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitReadStopsAtTwiceTheSeriesLimit(t *testing.T) {
	start := time.Unix(1700000400, 0).UTC()
	end := start.Add(5 * time.Minute)
	for _, tc := range []struct {
		name string
		aux  string
		want error
	}{{"non-split", "", nil}, {"split", jsonInvalidRowsName, errStatsGroupGuard}} {
		t.Run(tc.name, func(t *testing.T) {
			body := statsResponseBody(300000, tc.aux != "", start.Unix())
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
			defer srv.Close()
			p := newFilterPushdownProxy(t, srv.URL)
			p.maxStatsQuerySeries = 500
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			_, _, err := p.collectRangeMetricHitsAux(context.Background(), "*", []string{"trace_id"}, []string{"trace_id"}, false, "count() as c", start, end, time.Minute, tc.aux)
			runtime.ReadMemStats(&after)
			if tc.want == nil && !isSeriesLimitError(err) || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("allocated %.2f MiB reading a %.1f MiB response", float64(allocated)/(1<<20), float64(len(body))/(1<<20))
			if allocated > 8<<20 {
				t.Fatalf("allocated %.1f MiB reading a %.1f MiB response over the limit", float64(allocated)/(1<<20), float64(len(body))/(1<<20))
			}
		})
	}
}

// A split query whose stats response holds 300k groups is answered by the row
// evaluator, with bounded memory on the stats read and no series limit error
// from the group count: the rows it reads here are none, so the answer is empty.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitHugeResponseFallsBackWithBoundedMemory(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	body := statsResponseBody(300000, true, s0.Unix())
	var rawReads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			_, _ = w.Write(body)
			return
		}
		rawReads.Add(1) // the probes and the row evaluator find no row
	}))
	defer srv.Close()
	p := newFilterPushdownProxy(t, srv.URL)
	p.maxStatsQuerySeries = 500
	query := `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	code, got, rsp := runSplitQuery(t, p, query, start, end, time.Minute)
	runtime.ReadMemStats(&after)
	if code != http.StatusOK || len(got) != 0 {
		t.Fatalf("status %d %.200s, got %v: want the row evaluator's empty answer", code, rsp, got)
	}
	if rawReads.Load() == 0 {
		t.Fatal("the row evaluator did not answer")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 24<<20 {
		t.Fatalf("allocated %.1f MiB for a %.1f MiB stats response", float64(allocated)/(1<<20), float64(len(body))/(1<<20))
	}
}

// statsResponseBody renders a stats_query_range response of groups trace_id
// groups, each with a count result and, with aux, an aux result.
func statsResponseBody(groups int, aux bool, ts int64) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < groups; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"metric":{"__name__":"c","trace_id":"t%07d"},"values":[[%d,"1"]]}`, i, ts)
		if aux {
			fmt.Fprintf(&b, `,{"metric":{"__name__":%q,"trace_id":"t%07d"},"values":[[%d,"0"]]}`, jsonInvalidRowsName, i, ts)
		}
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// The lines read as raw rows carry no labels in the stats query: whatever
// service name, level or parsed key they hold, they form one group, because
// the raw read evaluates them itself and only their buckets are used. Fifty
// lines of which Loki rejects all (a number without a leading digit) leave the
// three real series within a limit of ten, and the stats response holds one
// group for them.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitRawCandidatesFormOneGroup(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 3; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": fmt.Sprintf("good%d", i), "level": "info", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"ok%d"}`, i)})
	}
	for i := 0; i < 50; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(110+i) * time.Second), stream: map[string]string{"app": fmt.Sprintf("bad%d", i), "level": []string{"error", "warn", "debug"}[i%3], "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"bad%d","x":.5}`, i)})
	}
	query := `sum by (trace_id, service_name, detected_level) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	var replay url.Values
	fake.onStats = func(r *http.Request, q string) {
		if replay == nil || strings.Contains(q, jsonLokiRejectHazard) {
			replay = r.Form
		}
	}
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 10
	plan, _ := compileOrderedJSONMetric(query)
	if !plan.validOnly {
		t.Fatal("plan must be a split plan")
	}
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	if len(want) != 3 {
		t.Fatalf("fixture: row evaluator answers %d series, want 3", len(want))
	}
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("split %d %s, got %v, want %v", code, body, got, want)
	}
	rawGroups, allGroups := replayStatsGroups(t, srv.URL, replay)
	if len(rawGroups) != 1 {
		t.Fatalf("raw candidates form %d groups, want 1: %v", len(rawGroups), rawGroups)
	}
	if len(allGroups) != 4 {
		t.Fatalf("stats response holds %d groups, want the 3 series and the raw group: %v", len(allGroups), allGroups)
	}
}

// replayStatsGroups sends a stats_query_range request the proxy made to the
// fake again and returns the groups holding raw candidates (the aux result is
// above zero) and all groups of the response.
func replayStatsGroups(t *testing.T, backend string, params url.Values) (raw, all map[string]bool) {
	t.Helper()
	resp, err := http.PostForm(backend+"/select/logsql/stats_query_range", params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	raw, all = map[string]bool{}, map[string]bool{}
	for _, res := range decoded.Data.Result {
		labels := map[string]string{}
		for k, v := range res.Metric {
			if k != "__name__" {
				labels[k] = v
			}
		}
		key := canonicalLabelsKey(labels)
		all[key] = true
		if res.Metric["__name__"] != jsonInvalidRowsName {
			continue
		}
		for _, point := range res.Values {
			if point[1] != "0" {
				raw[key] = true
			}
		}
	}
	return raw, all
}

// The same holds without a hazard rescan: lines VictoriaLogs cannot parse and
// Loki rejects too (truncated JSON) over twenty distinct stream label sets are
// raw candidates in the first scan. They form one group, so they neither count
// against the series limit (the two real services under a limit of three
// answer) nor grow the response.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitRawCandidatesOverManyStreamsFormOneGroup(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 2; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": fmt.Sprintf("good%d", i), "env": "prod"}, msg: `{"a":1}`})
	}
	for i := 0; i < 20; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(110+i) * time.Second), stream: map[string]string{"app": fmt.Sprintf("bad%d", i), "env": "prod"}, msg: `{"a":1`})
	}
	query := `sum by (service_name) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	var replay url.Values
	fake.onStats = func(r *http.Request, q string) { replay = r.Form }
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 3
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	if len(want) != 2 {
		t.Fatalf("fixture: row evaluator answers %d series, want 2", len(want))
	}
	code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusOK || !reflect.DeepEqual(got, want) {
		t.Fatalf("split %d %s, got %v, want %v", code, body, got, want)
	}
	if len(fake.stats) != 1 {
		t.Fatalf("expected one stats scan, no hazard rescan: %q", fake.stats)
	}
	rawGroups, allGroups := replayStatsGroups(t, srv.URL, replay)
	if len(rawGroups) != 1 || len(allGroups) != 3 {
		t.Fatalf("raw candidates form %d groups of %d, want 1 of 3: %v", len(rawGroups), len(allGroups), allGroups)
	}
}

// Six valid series with no line read as raw rows under a limit of 5 end with
// Loki's error from the merged answer, and with a limit of 6 answer.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitLimitPlusOneSeriesWithoutRawRows(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 6; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
	}
	query := `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 5
	code, _, body := runSplitQuery(t, p, query, start, end, time.Minute)
	if code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (5) reached for a single query") {
		t.Fatalf("status %d body %.200s, want Loki's series limit error", code, body)
	}
	if fake.raw != 0 {
		t.Fatalf("%d raw-row queries issued: the error comes from the split's merged answer", fake.raw)
	}
	p.maxStatsQuerySeries = 6
	plan, _ := compileOrderedJSONMetric(query)
	want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
	if code, got, body := runSplitQuery(t, p, query, start, end, time.Minute); code != http.StatusOK || !reflect.DeepEqual(got, want) || len(want) != 6 {
		t.Fatalf("limit 6: status %d %.200s, got %v, want %v", code, body, got, want)
	}
}

// A response too large to read is the split's own fallback: a plain pushdown
// plan keeps the error it always returned for it, and the split plan hands the
// query to the row evaluator without one.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitOversizedResponseOnlyFallsBackForSplitPlans(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 12; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d","status":"500"}`, i)})
	}
	old := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = 400
	t.Cleanup(func() { maxStatsBucketResponseBytes = old })
	for _, tc := range []struct {
		name, query string
		split       bool
	}{
		{"split", `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`, true},
		{"plain", `sum by (trace_id) (count_over_time({env="prod"} | json | drop __error__ | status="500" [5m]))`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown || plan.validOnly != tc.split {
				t.Fatalf("plan pushdown=%v validOnly=%v, want a pushdown with validOnly=%v", plan != nil && plan.pushdown, plan != nil && plan.validOnly, tc.split)
			}
			_, served, _, err := p.orderedJSONStatsBucketsWithReason(context.Background(), plan, start, end, time.Minute)
			switch {
			case tc.split && (served || err != nil):
				t.Fatalf("split plan: served=%v err=%v, want the row evaluator to answer", served, err)
			case !tc.split && !errors.Is(err, errBodyTooLarge):
				t.Fatalf("plain plan: served=%v err=%v, want the size error as before", served, err)
			}
		})
	}
}

// Grafana Drilldown keeps the busiest series of an over-limit answer instead of
// failing, which the split's merged answer does not rank: its requests take the
// row evaluator, and no split query reaches VictoriaLogs.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitDrilldownRequestsKeepTheRowEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	var rows []pushdownRow
	for i := 0; i < 8; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(100+i) * time.Second), stream: map[string]string{"app": "api", "env": "prod"}, msg: fmt.Sprintf(`{"trace_id":"t%d"}`, i)})
	}
	query := `sum by (trace_id) (count_over_time({env="prod"} | json | __error__="" [5m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	p.maxStatsQuerySeries = 5
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %.200s, want the busiest series with a warning", rec.Code, rec.Body.String())
	}
	if len(fake.stats) != 0 || fake.raw == 0 {
		t.Fatalf("%d stats queries, %d raw-row queries: the row evaluator alone must answer", len(fake.stats), fake.raw)
	}
	if got := len(decodeExtractedMatrix(t, rec.Body.Bytes())); got != 5 {
		t.Fatalf("%d series, want the 5 busiest", got)
	}
}

// The raw-candidate marker is set once and the later conditions test it: the
// regexp conditions of jsonInvalidCondition appear in the query only in the
// marker, it never becomes a grouped field, and an answer's labels never carry
// it.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitMarksRawCandidatesOnce(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: map[string]string{"app": "api", "env": "prod", "level": "info"}, msg: `{"path":"/a","status":"500","x":.5}`},
		{ts: s0.Add(110 * time.Second), stream: map[string]string{"app": "web", "env": "prod", "level": "error"}, msg: `{"path":"/b","status":"500"}`},
		{ts: s0.Add(120 * time.Second), stream: map[string]string{"app": "web", "env": "prod", "level": "WARN"}, msg: `{"path":"/c","status":"200"}`},
	}
	for _, query := range []string{
		`sum by (path, service_name, detected_level) (count_over_time({env="prod"} | json | __error__="" [5m]))`,
		`sum by (path) (bytes_over_time({env="prod"} | json | __error__="" | status="500" [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, _ := compileOrderedJSONMetric(query)
			want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
			fake.raw = 0
			code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
			if code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
			}
			if fake.raw == 0 {
				t.Fatal("fixture: no line was read as a raw row")
			}
			for key := range got {
				if strings.Contains(key, jsonRawField) {
					t.Fatalf("series %s carries the marker", key)
				}
			}
			if len(fake.stats) == 0 {
				t.Fatal("no stats query")
			}
			for _, stats := range fake.stats {
				marker := jsonRawMarker(strings.Contains(stats, jsonLokiRejectHazard))
				if strings.Count(stats, marker) != 1 {
					t.Fatalf("stats query does not set the marker once: %s", stats)
				}
				if rest := strings.Replace(stats, marker, "", 1); strings.Contains(rest, jsonValidField+":*") {
					t.Fatalf("stats query tests the line again after the marker: %s", stats)
				}
				if strings.Contains(stats, "stats by ("+jsonRawField) || strings.Contains(stats, ", "+jsonRawField+",") || strings.Contains(stats, ", "+jsonRawField+")") {
					t.Fatalf("stats query groups by the marker: %s", stats)
				}
			}
		})
	}
}

// A step wider than the window leaves gaps between the evaluation windows. The
// stats scan covers the gaps too, but a line in a gap belongs to no sample, so
// its group is no series of Loki's answer and does not count against the limit
// (3 visible trace ids plus 4 only in the gaps, limit 5, step 5m, window 1m).
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitGapGroupsAreNoSeries(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(12*time.Minute)
	stream := map[string]string{"app": "api", "env": "prod"}
	var rows []pushdownRow
	for i := 0; i < 3; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(90*time.Second + time.Duration(i)*time.Second), stream: stream, msg: fmt.Sprintf(`{"trace_id":"seen%d"}`, i)})
	}
	for i := 0; i < 4; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(3*time.Minute + time.Duration(i)*20*time.Second), stream: stream, msg: fmt.Sprintf(`{"trace_id":"gap%d"}`, i)})
	}
	// A line only Loki's parser accepts in a gap makes the plan read raw rows.
	rows = append(rows, pushdownRow{ts: s0.Add(4 * time.Minute), stream: stream, msg: `{"trace_id":"gap9"} trailing`})
	for _, function := range []string{"count_over_time", "bytes_over_time"} {
		query := `sum by (trace_id) (` + function + `({env="prod"} | json | __error__="" [1m]))`
		t.Run(function, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			p.maxStatsQuerySeries = 5
			plan, _ := compileOrderedJSONMetric(query)
			want := rowEvaluatorMatrix(t, p, plan, start, end, 5*time.Minute)
			if len(want) != 3 {
				t.Fatalf("fixture: row evaluator answers %d series, want 3", len(want))
			}
			code, got, body := runSplitQuery(t, p, query, start, end, 5*time.Minute)
			if code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
			}
		})
	}
}

// A split plan with no label to read (an ungrouped sum, no filter) has nothing
// to unpack: the sentinel splice is its only parse, and no `unpack_json fields
// ()` is sent. The answer is the row evaluator's.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitWithoutLabelsSendsNoEmptyUnpack(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	stream := map[string]string{"app": "api", "env": "prod"}
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: stream, msg: `{"a":1}`},
		{ts: s0.Add(110 * time.Second), stream: stream, msg: `{"a":2} trailing`},
		{ts: s0.Add(120 * time.Second), stream: stream, msg: `not json`},
		{ts: s0.Add(130 * time.Second), stream: stream, msg: `{"a":3,"x":.5}`},
	}
	for _, function := range []string{"count_over_time", "bytes_over_time"} {
		query := `sum(` + function + `({env="prod"} | json | __error__="" [5m]))`
		t.Run(function, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, _ := compileOrderedJSONMetric(query)
			if !plan.validOnly || !plan.pushdown || len(plan.unpackKeys()) != 0 {
				t.Fatalf("plan must be a split plan without labels: %+v", plan)
			}
			want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
			code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
			if code != http.StatusOK || !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d %.200s, got %v, want %v", code, body, got, want)
			}
			if len(fake.stats) == 0 {
				t.Fatal("no stats query")
			}
			for _, stats := range fake.stats {
				if strings.Contains(stats, "fields ()") || strings.Contains(stats, "keep_original_fields") {
					t.Fatalf("stats query unpacks no label but says so: %s", stats)
				}
				if !strings.Contains(stats, jsonValidField) {
					t.Fatalf("stats query lost the sentinel splice: %s", stats)
				}
			}
		})
	}
}

// A plan that derives detected_level reads every stored level field of a raw
// row (the stored level fields, the OTLP severity_text, severity_number and
// severity), none of which a label of the plan names: its raw read keeps every
// field instead of projecting to the plan's labels.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestOrderedJSONRowProjectionKeepsTheStoredLevelFields(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	stream := map[string]string{"app": "api", "env": "prod"}
	rows := []pushdownRow{
		{ts: s0.Add(100 * time.Second), stream: stream, vl: map[string]string{"severity": "error"}, msg: `{"n":1}`},
		{ts: s0.Add(110 * time.Second), stream: stream, vl: map[string]string{"severity_text": "Warn", "severity_number": "13"}, msg: `{"n":2}`},
		{ts: s0.Add(120 * time.Second), stream: stream, vl: map[string]string{"level": "info"}, msg: `{"n":3}`},
	}
	for _, query := range []string{
		`sum by (detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`,
		`sum by (app, detected_level) (count_over_time({env="prod"} | json | drop __error__ [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.derivesLevel {
				t.Fatal("expected a plan deriving detected_level")
			}
			if got := p.orderedJSONRowProjection(plan); got != "" {
				t.Fatalf("projection %q hides the stored level fields from the derivation", got)
			}
			got := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
			for _, level := range []string{"error", "warn", "info"} {
				found := false
				for key := range got {
					found = found || strings.Contains(key, `detected_level="`+level+`"`)
				}
				if !found {
					t.Fatalf("no %s series in %v", level, got)
				}
			}
			for _, raw := range fake.rawQueries {
				if strings.Contains(raw.query, " | fields ") {
					t.Fatalf("raw read projects the rows: %s", raw.query)
				}
			}
		})
	}
}

// A split plan grouped by detected_level is served by the stats only when the
// stored level fields decide the level of every counted line as Loki's ingest
// does; in every other window the row evaluator answers. Whichever answers, the
// matrix is the row evaluator's, which derives the level of each line itself:
// a window mixing stored and body-only levels, a stored detected_level label,
// a stored spelling Loki does not know, the level in structured metadata, only
// an OTLP severity, and lines read as raw rows with and without a stored level.
// conformance: semantics/json-error-filter-metric-split-pushdown
func TestJSONValidSplitDetectedLevelWindowsMatchTheRowEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(5*time.Minute)
	row := func(sec int, stream, vl map[string]string, msg string) pushdownRow {
		labels := map[string]string{"app": "api", "env": "prod"}
		for k, v := range stream {
			labels[k] = v
		}
		return pushdownRow{ts: s0.Add(time.Duration(sec) * time.Second), stream: labels, vl: vl, msg: msg}
	}
	lvl := func(level string) map[string]string { return map[string]string{"level": level} }
	const (
		split    = "split"
		fallback = "fallback"
	)
	for _, tc := range []struct {
		name string
		want string
		rows []pushdownRow
	}{
		{"stored on every line", split, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1}`), row(110, lvl("INFO"), nil, `{"n":2}`), row(120, lvl("warning"), nil, `{"n":3}`), row(130, lvl("error"), nil, `{"n":4,"level":"debug"}`),
		}},
		{"stored in structured metadata", split, []pushdownRow{
			row(100, nil, lvl("warn"), `{"n":1}`), row(110, nil, lvl("Information"), `{"n":2}`),
		}},
		{"stored spellings Loki does not normalise", split, []pushdownRow{
			row(100, lvl("notice"), nil, `{"n":1}`), row(110, lvl("NOTICE"), nil, `{"n":2}`), row(120, lvl("Trace"), nil, `{"n":3}`), row(130, lvl("fatal"), nil, `{"n":4}`), row(140, lvl("crit"), nil, `{"n":5}`),
		}},
		{"stored and body-only", fallback, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1}`), row(110, nil, nil, `{"n":2,"level":"error"}`), row(120, nil, nil, `{"n":3}`),
		}},
		{"body-only only", fallback, []pushdownRow{
			row(100, nil, nil, `{"level":"WARN"}`), row(110, nil, nil, `{"level":"notice"}`), row(120, nil, nil, `{"msg":"request failed with an error"}`),
		}},
		{"stored detected_level label", fallback, []pushdownRow{
			row(100, map[string]string{"detected_level": "Custom"}, nil, `{"n":1}`), row(110, lvl("info"), nil, `{"n":2}`),
		}},
		{"a stream severity beside metadata level", fallback, []pushdownRow{
			row(100, map[string]string{"severity": "error"}, lvl("info"), `{"n":1}`), row(110, lvl("info"), nil, `{"n":2}`),
		}},
		{"a body severity key beside a stored level", split, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1,"severity":"error"}`), row(110, nil, lvl("warn"), `{"severity":"error","level":"debug"}`),
		}},
		{"stored OTLP severity fields beside a stored level", fallback, []pushdownRow{
			row(100, nil, map[string]string{"level": "info", "severity_text": "Error", "severity_number": "17"}, `{"n":1}`),
		}},
		{"only an OTLP severity", fallback, []pushdownRow{
			row(100, nil, map[string]string{"severity": "error"}, `{"n":1}`), row(110, nil, map[string]string{"severity_text": "Warn", "severity_number": "13"}, `{"n":2}`),
		}},
		{"stored on every counted line, a raw line without one", split, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1}`), row(110, lvl("error"), nil, `{"n":2}`), row(120, nil, nil, `{"n":3,"level":"warn"} trailing`),
		}},
		{"stored on every counted line, a raw line with another one", split, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1}`), row(110, lvl("error"), nil, `{"n":2}`), row(120, lvl("DEBUG"), nil, `{"n":3,"level":"warn"} trailing`), row(130, lvl("Info"), nil, `{"n":4,"x":.5}`),
		}},
		{"a counted line without a stored level beside a raw one", fallback, []pushdownRow{
			row(100, lvl("info"), nil, `{"n":1}`), row(110, nil, nil, `{"n":2}`), row(120, lvl("error"), nil, `{"n":3} trailing`),
		}},
	} {
		for _, function := range []string{"count_over_time", "bytes_over_time"} {
			query := `sum by (detected_level) (` + function + `({env="prod"} | json | __error__="" [5m]))`
			t.Run(tc.name+"/"+function, func(t *testing.T) {
				srv, fake := newPushdownFakeVL(t, tc.rows, nil)
				fake.jsonUnpack = vlFastjsonUnpackFields
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				plan, ok := compileOrderedJSONMetric(query)
				if !ok || !plan.validOnly || !plan.pushdown {
					t.Fatal("plan must be a split plan")
				}
				want := rowEvaluatorMatrix(t, p, plan, start, end, time.Minute)
				if len(want) == 0 {
					t.Fatal("fixture: the row evaluator answers no series")
				}
				fake.mu.Lock()
				fake.raw, fake.rawQueries, fake.stats, fake.guards = 0, nil, nil, nil
				fake.mu.Unlock()
				code, got, body := runSplitQuery(t, p, query, start, end, time.Minute)
				if code != http.StatusOK {
					t.Fatalf("status %d: %s", code, body)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("differs from the row evaluator\n got: %v\nwant: %v", got, want)
				}
				fake.mu.Lock()
				rawQueries := fake.rawQueries
				fake.mu.Unlock()
				full := false
				for _, raw := range rawQueries {
					bounded := strings.Contains(raw.query, " | filter "+jsonInvalidCondition(false)) || strings.Contains(raw.query, " | filter "+jsonInvalidCondition(true))
					full = full || !bounded
				}
				if gotPath := map[bool]string{true: fallback, false: split}[full]; gotPath != tc.want {
					t.Fatalf("answered by the %s, want the %s", gotPath, tc.want)
				}
				if tc.want == split {
					assertStoredLevelStats(t, fake, tc.name)
				}
			})
		}
	}
}
