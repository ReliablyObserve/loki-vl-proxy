package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
)

// jsonProbeSetup is one way the pushdown asks the JSON parse-risk probes: the
// base they read, the fields and the stored spellings of the labels.
type jsonProbeSetup struct {
	name   string
	base   string
	fields []string
	stored map[string]string
}

// jsonProbeSetups are the probes of a name_extracted label (the stored label
// deleted, both keys read: pushdownParseRisk), of a plain parsed label, of a
// label holding an underscore (the key-spelling probe applies) and of a label
// stored under another spelling.
var jsonProbeSetups = []jsonProbeSetup{
	{name: "level_extracted", base: `app:="api" | delete level`, fields: []string{"level", "level_extracted"}},
	{name: "method", base: `app:="api"`, fields: []string{"method"}},
	{name: "user_id", base: `app:="api"`, fields: []string{"user_id"}},
	{name: "service_version", base: `app:="api"`, fields: []string{"service_version"}, stored: map[string]string{"service_version": "service.version"}},
}

// jsonProbeUnpacks are the unpack_json semantics of the supported
// VictoriaLogs lines (lib/logstorage/pipe_unpack_json.go): before v1.52 a
// line yields fields only when its first byte opens the object; from v1.52
// the JSON whitespace around it is trimmed first (vlFastjsonUnpackFields).
var jsonProbeUnpacks = []struct {
	name   string
	unpack func(string) map[string]string
}{
	{"v1.49-v1.51", func(msg string) map[string]string {
		if msg == "" || msg[0] != '{' {
			return nil
		}
		return vlFastjsonUnpackFields(msg)
	}},
	{"v1.52+", vlFastjsonUnpackFields},
}

// jsonProbeVerdicts runs the partial-parse and key-spelling probes of setup,
// gated or not, over one row with the fake's VictoriaLogs semantics and the
// given unpack_json.
func jsonProbeVerdicts(t testing.TB, setup jsonProbeSetup, row pushdownRow, gated bool, unpack func(string) map[string]string) (partial, spelling bool) {
	ctx := context.Background()
	fake := &pushdownFakeVL{jsonUnpack: unpack}
	partial = fake.guardMatches(t, orderedJSONPartialParseQuery(ctx, setup.base, setup.fields, setup.stored, gated), row)
	if orderedJSONKeyAliasPattern(setup.fields) != "" {
		spelling = fake.guardMatches(t, orderedJSONKeySpellingQuery(ctx, setup.base, setup.fields, setup.stored, gated), row)
	}
	return partial, spelling
}

// jsonProbeGateLines are the line kinds that tell Loki's JSON parser from
// unpack_json, and lines that only look like them, each with whether the
// probes of a name_extracted label (stream label level, both keys read) find
// it: a found line keeps the exact raw evaluator (jsonProbeUntrimmedLines:
// only where unpack_json does not trim the whitespace before the object).
var jsonProbeGateLines = []struct {
	name, msg string
	extracted bool
}{
	{"whitespace before the object", ` {"level":"error"}`, false},
	{"generator line", `{"service": {"name": "api-gateway"}, "method": "GET", "status": 200, "user_id": "user_1", "level": "info", "region": "us-east-1"}`, false},
	{"compact line", `{"level":"info","method":"GET","user_id":"u1"}`, false},
	{"space before the colon", `{"level" : "info", "method" : "GET"}`, false},
	{"quote and space inside a value", `{"msg":" leading space","level":"info"}`, false},
	{"non-ASCII space inside a value", "{\"msg\":\"a\u00a0b\",\"level\":\"info\"}", false},
	{"empty string value", `{"msg":"","level":"info"}`, false},
	{"names as value words", `{"msg":"level changed for method user_id","path":"/x"}`, false},
	{"plain text", `plain text level error`, false},
	{"logfmt", `level=info method=GET`, false},
	{"other key case", `{"LEVEL":"error"}`, false},
	{"syntax error after the key", `{"level":"error","msg": truncated`, true},
	{"text after the object", `{"level":"error"} trailing`, true},
	{"repeated key", `{"level":"info","level":"error"}`, true},
	{"array", `{"level":["error"]}`, true},
	{"escaped key", `{"lev\u0065l":"error"}`, true},
	{"replacement character", "{\"level\":\"e\ufffdr\"}", true},
	{"escaped replacement character", `{"level":"e\ufffdr"}`, true},
	{"ASCII space around the key", `{" level ":"error"}`, true},
	{"no-break space before the key", "{\"\u00a0level\":\"error\"}", true},
	{"ideographic space after the key", "{\"level\u3000\":\"error\"}", true},
	{"next line before the key", "{\"\u0085level\":\"error\"}", true},
	{"blank key", `{"":"x","level":"info"}`, true},
	{"blank key of spaces", `{"  ":"x","level":"info"}`, true},
	{"dotted spelling", `{"level.extracted":"x"}`, true},
	{"hyphen spelling", `{"level-extracted":"x"}`, true},
	{"non-ASCII letter spelling", "{\"level\u00c9extracted\":\"x\"}", true},
	{"nested keys", `{"level":{"extracted":"x"}}`, true},
	{"space after the renamed key", `{"level_extracted ":"x"}`, true},
	{"space before a nested piece", `{"level":{" extracted":"x"}}`, true},
	// A superset the alias pattern accepts: it only costs the raw evaluator.
	{"top-level piece with a space", `{" extracted":"x","level":"info"}`, true},
}

// jsonProbeUntrimmedLines are the lines the probes of a name_extracted label
// find under the unpack_json of VictoriaLogs before v1.52 only, which yields
// nothing for them while Loki and later VictoriaLogs read the object.
var jsonProbeUntrimmedLines = map[string]bool{"whitespace before the object": true}

// The gated JSON parse-risk probes find exactly the lines the probes found
// before the gates, on every line kind and every way the pushdown asks them,
// and the probes of a name_extracted label find the lines Loki reads
// differently from unpack_json and only those.
// conformance: quality/json-label-probe-gates, semantics/json-extracted-metric-pushdown, parser-error-and-label-collision
func TestJSONLabelProbeGatesEdgeTable(t *testing.T) {
	for _, version := range jsonProbeUnpacks {
		for _, setup := range jsonProbeSetups {
			for _, line := range jsonProbeGateLines {
				for _, storedLevel := range []bool{false, true} {
					stream := map[string]string{"app": "api"}
					if storedLevel {
						stream["level"] = "warn"
					}
					row := pushdownRow{stream: stream, msg: line.msg}
					label := fmt.Sprintf("%s / %s / %s (stream level %v)", version.name, setup.name, line.name, storedLevel)
					gp, gs := jsonProbeVerdicts(t, setup, row, true, version.unpack)
					up, us := jsonProbeVerdicts(t, setup, row, false, version.unpack)
					if gp != up || gs != us {
						t.Errorf("%s: gated probes (partial %v, spelling %v) differ from ungated (%v, %v)", label, gp, gs, up, us)
					}
					want := line.extracted || jsonProbeUntrimmedLines[line.name] && version.name == "v1.49-v1.51"
					if setup.name == "level_extracted" && (up || us) != want {
						t.Errorf("%s: probes find the line = %v, want %v", label, up || us, want)
					}
				}
			}
		}
	}
}

// jsonKeySpaceNonASCII spells out the runes of jsonKeySpace outside ASCII, the
// class Loki trims around a key (unicode.IsSpace plus \p{Z}): a rune missing
// from it would let a key spelled with that space past the alias gate.
// conformance: quality/json-label-probe-gates
func TestJSONKeySpaceNonASCIIClass(t *testing.T) {
	space := regexp.MustCompile(`^` + jsonKeySpace + `$`)
	gate := regexp.MustCompile(`^` + jsonKeySpaceNonASCII + `$`)
	ascii := regexp.MustCompile(`^"[\t-\r ]$`)
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		s := string(r)
		if r < 0x80 {
			if space.MatchString(s) != ascii.MatchString(`"`+s) {
				t.Fatalf("ASCII rune %U: in jsonKeySpace %v, in the quote-space gate %v", r, space.MatchString(s), ascii.MatchString(`"`+s))
			}
			continue
		}
		if want := space.MatchString(s); gate.MatchString(s) != want {
			t.Fatalf("rune %U: in jsonKeySpace %v, in jsonKeySpaceNonASCII %v", r, want, !want)
		}
		if unicode.IsSpace(r) && !gate.MatchString(s) {
			t.Fatalf("rune %U is a space Loki trims but not in jsonKeySpaceNonASCII", r)
		}
	}
}

// Every gate is a filter its regexp's matches pass: over random keys and
// spellings, a line the alias pattern or a nested pattern matches passes its
// gate, and a line the key patterns match passes theirs.
// conformance: quality/json-label-probe-gates
func TestJSONLabelProbeGatesAreSupersets(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 4000; i++ {
		msg := randomJSONProbeLine(rng)
		row := pushdownRow{stream: map[string]string{"app": "api"}, msg: msg}
		values := map[string]string{"_msg": msg}
		for _, setup := range jsonProbeSetups {
			if alias := orderedJSONKeyAliasPattern(setup.fields); alias != "" && regexp.MustCompile(alias).MatchString(msg) {
				if !fakeLogsQLMatch(t, orderedJSONKeyAliasGates(setup.fields), values) {
					t.Fatalf("%s: alias pattern matches %q, its gates do not", setup.name, msg)
				}
			}
			_, loose, firsts := orderedJSONNestedSplitsFirsts(setup.fields)
			for j, pattern := range loose {
				if regexp.MustCompile(pattern).MatchString(msg) && !fakeLogsQLMatch(t, orderedJSONNestedGate(firsts[j]), values) {
					t.Fatalf("%s: nested pattern %s matches %q, its gate does not", setup.name, pattern, msg)
				}
			}
			for _, field := range setup.fields {
				name := regexp.QuoteMeta(field)
				for _, rest := range []string{"", `\s*\[`} {
					if regexp.MustCompile(jsonKeyPattern(name)+rest).MatchString(msg) && !regexp.MustCompile(jsonKeyGate(name, rest)).MatchString(msg) {
						t.Fatalf("%s: key pattern of %s%s matches %q, its gate does not", setup.name, field, rest, msg)
					}
				}
				twice := jsonKeyPattern(name) + `(?s:.*)` + jsonKeyPattern(name)
				if regexp.MustCompile(twice).MatchString(msg) && !fakeLogsQLMatch(t, logsqlTwiceFilter([]string{field}), values) {
					t.Fatalf("%s: repeated key of %s matches %q, the twice filter does not", setup.name, field, msg)
				}
			}
			for _, version := range jsonProbeUnpacks {
				gp, gs := jsonProbeVerdicts(t, setup, row, true, version.unpack)
				up, us := jsonProbeVerdicts(t, setup, row, false, version.unpack)
				if gp != up || gs != us {
					t.Fatalf("%s %s: %q: gated probes (%v, %v) differ from ungated (%v, %v)", version.name, setup.name, msg, gp, gs, up, us)
				}
			}
		}
	}
}

// randomJSONProbeLine draws a JSON-ish line from keys, spellings, spaces and
// breakages the probes look for.
func randomJSONProbeLine(rng *rand.Rand) string {
	keys := []string{"level", "level_extracted", "extracted", "method", "user_id", "user", "id", "service", "version", "service_version", "", "LEVEL", "msg"}
	seps := []string{"", "", "", " ", "\t", "\u00a0", "\u3000", "\u0085", ".", "-", "\u00c9", "_"}
	values := []string{`"info"`, `"error"`, `""`, `["a"]`, `{"extracted":"x"}`, `{"version":"1"}`, `{"id":"7"}`, `1`, `"e` + "\ufffd" + `r"`, `"\ufffd"`, `" x"`}
	var sb strings.Builder
	if rng.Intn(8) == 0 {
		sb.WriteString(" ")
	}
	sb.WriteString("{")
	for i, n := 0, 1+rng.Intn(4); i < n; i++ {
		if i > 0 {
			sb.WriteString([]string{",", ", "}[rng.Intn(2)])
		}
		key := keys[rng.Intn(len(keys))]
		if rng.Intn(4) == 0 && strings.Contains(key, "_") {
			key = strings.Replace(key, "_", seps[rng.Intn(len(seps))], 1)
		}
		if rng.Intn(6) == 0 {
			key = seps[rng.Intn(len(seps))] + key
		}
		if rng.Intn(6) == 0 {
			key += seps[rng.Intn(len(seps))]
		}
		if rng.Intn(10) == 0 {
			key = strings.Replace(key, "e", `\u0065`, 1)
		}
		sb.WriteString(`"` + key + `"`)
		sb.WriteString([]string{":", ": ", " : "}[rng.Intn(3)])
		sb.WriteString(values[rng.Intn(len(values))])
	}
	switch rng.Intn(6) {
	case 0:
		sb.WriteString(` trailing`)
	case 1:
		// a syntax error after the keys
	default:
		sb.WriteString("}")
	}
	return sb.String()
}

// FuzzJSONLabelProbeGates checks that the gated probes find the lines the
// ungated ones find, for any line, with and without the stream label, under
// the unpack_json of every supported VictoriaLogs line.
func FuzzJSONLabelProbeGates(f *testing.F) {
	for _, line := range jsonProbeGateLines {
		f.Add(line.msg, true)
	}
	f.Add(`{"level":"info"}`, false)
	f.Fuzz(func(t *testing.T, msg string, storedLevel bool) {
		if len(msg) > 512 {
			return
		}
		stream := map[string]string{"app": "api"}
		if storedLevel {
			stream["level"] = "info"
		}
		row := pushdownRow{stream: stream, msg: msg}
		for _, setup := range jsonProbeSetups {
			for _, version := range jsonProbeUnpacks {
				gp, gs := jsonProbeVerdicts(t, setup, row, true, version.unpack)
				up, us := jsonProbeVerdicts(t, setup, row, false, version.unpack)
				if gp != up || gs != us {
					t.Fatalf("%s %s: %q: gated probes (%v, %v) differ from ungated (%v, %v)", version.name, setup.name, msg, gp, gs, up, us)
				}
			}
		}
	})
}

// generatorJSONRows mirrors the e2e log generator's api-gateway lines
// (Python's json.dumps spacing): every body holds level and every stream
// carries the level label, so a name_extracted label reads every line.
func generatorJSONRows(rng *rand.Rand, s0 time.Time, n int) []pushdownRow {
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	rows := make([]pushdownRow, 0, n)
	for i := 0; i < n; i++ {
		status := []int{200, 200, 200, 201, 404, 500}[rng.Intn(6)]
		level := "info"
		if status >= 500 {
			level = "error"
		} else if status >= 400 {
			level = "warn"
		}
		msg := fmt.Sprintf(`{"service": {"name": "api-gateway"}, "method": %q, "path": "/api/v1/users/%d", "status": %d, "duration_ms": %d, "trace_id": "%016x%016x", "user_id": "user_%d", "level": %q, "region": "us-east-1", "version": "v1"}`,
			methods[rng.Intn(4)], rng.Intn(99999), status, rng.Intn(300), rng.Uint64(), rng.Uint64(), rng.Intn(9999), level)
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 2 * time.Second), stream: map[string]string{"app": "api", "level": level}, msg: msg})
	}
	return rows
}

// jsonProbeGateQueries are the name_extracted shapes of the daily run (X1,
// E7, LS8, X3) and plain json metrics over a parsed label, an underscore label
// and a label filter.
var jsonProbeGateQueries = []string{
	`sum by (level) (count_over_time({app="api"} | json | level_extracted!="" [1m]))`,
	`sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
	`sum by (level_extracted) (count_over_time({app="api"} | json | level_extracted="error" | drop __error__ [1m]))`,
	`sum(rate({app="api"} | json | level_extracted="error" [1m]))`,
	`sum by (method) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
	`sum by (user_id) (count_over_time({app="api"} | json | drop __error__ [1m]))`,
	`sum by (region) (count_over_time({app="api"} | json | method="GET" [1m]))`,
}

// On lines where every body holds plain keys (the generator's), the probes of
// these shapes run no key regexp: VictoriaLogs runs a regexp led by a quote at
// every quote of a line, and these ran up to ten such regexps over every line
// of the range, three to six times the line work of the stats query they guard.
// Only gates led by a literal, the brace anchor and the escape substrings run.
// conformance: quality/json-label-probe-gates, semantics/json-extracted-metric-pushdown
func TestJSONLabelProbesRunNoKeyRegexpOnPlainLines(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := generatorJSONRows(rand.New(rand.NewSource(7)), s0, 120)
	heavy := []string{`"` + jsonKeySpace, `(?:"` + jsonKeySpace, `^\s*\{(?s:.*)`}
	for _, query := range jsonProbeGateQueries {
		t.Run(query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			var mu sync.Mutex
			ran := map[string]int{}
			fakeRegexpTraces.Store(t, func(pattern, value string) {
				if value == "" || !strings.HasPrefix(value, "{") {
					return // a regexp over another field than _msg
				}
				mu.Lock()
				ran[pattern]++
				mu.Unlock()
			})
			defer fakeRegexpTraces.Delete(t)
			runJSONVolumeQueryRange(t, p, query, s0.Add(time.Minute), s0.Add(4*time.Minute), time.Minute)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 || len(fake.stats) != 1 || len(fake.guards) == 0 {
				t.Fatalf("expected the probes and one stats call without raw rows: raw=%d stats=%d guards=%d", fake.raw, len(fake.stats), len(fake.guards))
			}
			for pattern, n := range ran {
				for _, lead := range heavy {
					if strings.HasPrefix(pattern, lead) {
						t.Errorf("key regexp %s ran %d times over plain lines", pattern, n)
					}
				}
			}
		})
	}
}

// jsonProbeDifferentialRows draws generator lines with, in two seeds out of
// three, a few of every line kind the probes look for.
func jsonProbeDifferentialRows(rng *rand.Rand, s0 time.Time, n int, risky bool) []pushdownRow {
	rows := generatorJSONRows(rng, s0, n)
	if !risky {
		return rows
	}
	for i := range rows {
		if rng.Intn(12) != 0 {
			continue
		}
		if rng.Intn(2) == 0 {
			rows[i].msg = jsonProbeGateLines[rng.Intn(len(jsonProbeGateLines))].msg
		} else {
			rows[i].msg = randomJSONProbeLine(rng)
		}
		if rng.Intn(3) == 0 {
			delete(rows[i].stream, "level")
		}
	}
	return rows
}

// A differential run over generator lines mixed with every line kind the
// probes look for: each answer equals the Loki-semantics reference evaluator
// (the stats pushdown when the probes find no line, the exact raw evaluator
// when they do), and on clean generator lines every shape takes the pushdown.
// conformance: quality/json-label-probe-gates, semantics/json-extracted-metric-pushdown, parser-error-and-label-collision
func TestJSONLabelProbeGatesDifferential(t *testing.T) {
	seeds := 24
	if testing.Short() {
		seeds = 9
	}
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(8*time.Minute)
	pushed, flagged := 0, 0
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		clean := seed%3 == 0
		rows := jsonProbeDifferentialRows(rng, s0, 260, !clean)
		srv, fake := newPushdownFakeVL(t, rows, nil)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		for _, query := range jsonProbeGateQueries {
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatalf("%s does not compile to a stats pushdown", query)
			}
			want, refErr := lokiPushdownReferenceErr(plan, rows, start, end, time.Minute)
			fake.mu.Lock()
			fake.raw, fake.stats, fake.guards = 0, nil, nil
			fake.mu.Unlock()
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			fake.mu.Lock()
			raw, stats := fake.raw, len(fake.stats)
			fake.mu.Unlock()
			label := fmt.Sprintf("seed %d: %s", seed, query)
			if refErr != nil {
				if rec.Code != http.StatusBadRequest {
					t.Errorf("%s: Loki fails this query (%v), the proxy answered %d", label, refErr, rec.Code)
				}
				continue
			}
			if rec.Code != http.StatusOK {
				t.Errorf("%s: status %d: %s", label, rec.Code, rec.Body)
				continue
			}
			if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
				t.Errorf("%s (stats=%d raw=%d)\n got: %v\nwant: %v", label, stats, raw, got, want)
			}
			if raw == 0 && stats == 1 {
				pushed++
			} else {
				flagged++
			}
			if clean && (raw != 0 || stats != 1) {
				t.Errorf("%s: clean generator lines must take the stats pushdown: stats=%d raw=%d", label, stats, raw)
			}
		}
	}
	t.Logf("stats pushdown: %d, raw evaluator kept by a probe: %d", pushed, flagged)
	if pushed == 0 || flagged == 0 {
		t.Fatalf("the run must exercise both the pushdown and the probes: pushed %d, flagged %d", pushed, flagged)
	}
}

// The gated probe text stays within VictoriaLogs' default query length for
// these shapes, and a query over it gives way to the ungated probe.
// conformance: quality/json-label-probe-gates
func TestJSONLabelProbeGatesFallBackWhenTooLong(t *testing.T) {
	ctx := context.Background()
	for _, setup := range jsonProbeSetups {
		for _, gated := range []bool{true, false} {
			if q := orderedJSONPartialParseQuery(ctx, setup.base, setup.fields, setup.stored, gated); len(q) > vlDefaultMaxQueryLen {
				t.Fatalf("%s: partial-parse probe of %d bytes", setup.name, len(q))
			}
			if q := orderedJSONKeySpellingQuery(ctx, setup.base, setup.fields, setup.stored, gated); len(q) > vlDefaultMaxQueryLen {
				t.Fatalf("%s: key-spelling probe of %d bytes", setup.name, len(q))
			}
		}
	}

	var mu sync.Mutex
	var sent []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		mu.Lock()
		sent = append(sent, query)
		mu.Unlock()
		if strings.Contains(query, "_msg:seq(") {
			http.Error(w, "query length exceeds -search.maxQueryLen", http.StatusBadRequest)
			return
		}
		_, _ = w.Write(nil)
	}))
	defer srv.Close()
	p := newFilterPushdownProxy(t, srv.URL)
	setup := jsonProbeSetups[0]
	risky, err := p.orderedJSONPartialParseRisk(ctx, setup.base, setup.fields, setup.stored, time.Unix(0, 0), time.Unix(60, 0))
	if err != nil || risky {
		t.Fatalf("refused gated probe: risky=%v err=%v", risky, err)
	}
	if len(sent) != 2 || !strings.Contains(sent[0], "_msg:seq(") || strings.Contains(sent[1], "_msg:seq(") {
		t.Fatalf("expected the gated probe, then the ungated one: %q", sent)
	}

	// A base whose ungated probe just fits VictoriaLogs' default length.
	pad := func(n int) string { return `app:="api"` + strings.Repeat(" ", n) + ` | delete level` }
	// The shortest base for which no gated text fits while the ungated one does.
	long := ""
	for n := vlDefaultMaxQueryLen - 4000; n < vlDefaultMaxQueryLen; n++ {
		if len(orderedJSONPartialParseQuery(ctx, pad(n), setup.fields, nil, true)) > vlDefaultMaxQueryLen {
			long = pad(n)
			break
		}
	}
	if long == "" || len(orderedJSONPartialParseQuery(ctx, long, setup.fields, nil, false)) > vlDefaultMaxQueryLen {
		t.Fatal("no base leaves the gated probe over the length and the ungated one within it")
	}
	sent = nil
	if _, err := p.orderedJSONPartialParseRisk(ctx, long, setup.fields, setup.stored, time.Unix(0, 0), time.Unix(60, 0)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || strings.Contains(sent[0], "_msg:seq(") {
		t.Fatalf("a base too long for the gated probe sends only the ungated one: %d queries", len(sent))
	}
}
