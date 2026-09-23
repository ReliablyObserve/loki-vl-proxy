package proxy

import (
	"bytes"
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
)

// pushdownRow is one stored line: stream labels (spelled alike on both
// sides), structured metadata in its VictoriaLogs spelling (vl) and its Loki
// spelling (loki), and the raw log line.
type pushdownRow struct {
	ts     time.Time
	stream map[string]string
	vl     map[string]string
	loki   map[string]string
	msg    string
}

// pushdownFakeVL evaluates the LogsQL the stats pushdown and its probes emit
// with VictoriaLogs semantics over fixture rows: unpack_json flattens nested
// keys with a dot and keeps keys raw, unpack_logfmt splits on spaces, format
// copies a stored field, filter pipes compare stored or unpacked values and
// stats buckets group by the named fields.
type pushdownFakeVL struct {
	mu     sync.Mutex
	rows   []pushdownRow
	stored func(string) string // Loki label -> stored spelling
	stats  []string
	guards []string
	raw    int
}

var (
	pushdownMsgFilterRE = regexp.MustCompile(`_msg:~("(?:[^"\\]|\\.)*")`)
	pushdownFormatRE    = regexp.MustCompile("^format if \\(`?([^`:]+)`?:\\*\\) \"<([^>]+)>\" as (\\S+)$")
	pushdownFilterRE    = regexp.MustCompile("^(-?)`?([^`:]+)`?:(=|~)(\"(?:[^\"\\\\]|\\\\.)*\")$")
	pushdownStoredRE    = regexp.MustCompile("`?([A-Za-z0-9_.]+)`?:\\*")
)

// vlUnpackJSONFields mirrors unpack_json: a line that is one JSON object after
// trimming whitespace yields its keys raw, nested objects flattened with a
// dot, arrays kept as their JSON text; anything else yields nothing.
func vlUnpackJSONFields(msg string) map[string]string {
	msg = strings.TrimSpace(msg)
	if msg == "" || msg[0] != '{' {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(msg))
	dec.UseNumber()
	var object map[string]any
	if dec.Decode(&object) != nil || dec.More() {
		return nil
	}
	out := map[string]string{}
	var walk func(prefix string, object map[string]any)
	walk = func(prefix string, object map[string]any) {
		for key, value := range object {
			switch v := value.(type) {
			case map[string]any:
				walk(prefix+key+".", v)
			case string:
				out[prefix+key] = v
			case json.Number:
				out[prefix+key] = v.String()
			case bool:
				out[prefix+key] = strconv.FormatBool(v)
			case nil:
				out[prefix+key] = ""
			default:
				raw, _ := json.Marshal(v)
				out[prefix+key] = string(raw)
			}
		}
	}
	walk("", object)
	return out
}

func (f *pushdownFakeVL) values(row pushdownRow) map[string]string {
	values := map[string]string{"_msg": row.msg}
	for k, v := range row.stream {
		values[k] = v
	}
	for k, v := range row.vl {
		values[k] = v
	}
	return values
}

func fieldList(pipe string) []string {
	open, closing := strings.Index(pipe, "("), strings.Index(pipe, ")")
	if open < 0 || closing < open {
		return nil
	}
	return strings.Split(pipe[open+1:closing], ", ")
}

// applyPipes runs the non-stats pipes of a query over one row and reports
// whether the row survives its filters.
func (f *pushdownFakeVL) applyPipes(t testing.TB, query string, row pushdownRow) (map[string]string, bool) {
	values := f.values(row)
	for _, pipe := range strings.Split(query, " | ")[1:] {
		switch {
		case strings.HasPrefix(pipe, "unpack_json fields (") || strings.HasPrefix(pipe, "unpack_logfmt fields ("):
			var unpacked map[string]string
			if strings.HasPrefix(pipe, "unpack_json") {
				unpacked = vlUnpackJSONFields(row.msg)
			} else {
				unpacked = slidingLogfmtFields(row.msg)
			}
			keep := strings.HasSuffix(pipe, " keep_original_fields")
			for _, field := range fieldList(pipe) {
				if keep && values[field] != "" {
					continue
				}
				values[field] = unpacked[field]
			}
		case strings.HasPrefix(pipe, "format if ("):
			m := pushdownFormatRE.FindStringSubmatch(pipe)
			if m == nil {
				t.Errorf("fake VL: unsupported format pipe %q", pipe)
				return nil, false
			}
			if values[m[1]] != "" {
				values[m[3]] = values[m[1]]
			}
		case strings.HasPrefix(pipe, "filter "):
			m := pushdownFilterRE.FindStringSubmatch(strings.TrimPrefix(pipe, "filter "))
			if m == nil {
				t.Errorf("fake VL: unsupported filter pipe %q", pipe)
				return nil, false
			}
			want, _ := strconv.Unquote(m[4])
			var match bool
			if m[3] == "=" {
				match = values[m[2]] == want
			} else {
				match = regexp.MustCompile(want).MatchString(values[m[2]])
			}
			if match == (m[1] == "-") {
				return values, false
			}
		case strings.HasPrefix(pipe, "stats "), strings.HasPrefix(pipe, "limit "):
		default:
			t.Errorf("fake VL: unsupported pipe %q", pipe)
			return nil, false
		}
	}
	return values, true
}

// guardMatches evaluates a `| limit 1` probe for one row the way the proxy's
// probes are meant to read: the stored-field probe fires on a stored value,
// the JSON probe on a line an absent label could still be parsed from
// (partially, from a differently spelled key), the logfmt probe on a token
// shape the two decoders split differently.
func (f *pushdownFakeVL) guardMatches(t testing.TB, query string, row pushdownRow) bool {
	values := f.values(row)
	var patterns []string
	for _, m := range pushdownMsgFilterRE.FindAllStringSubmatch(query, -1) {
		pattern, err := strconv.Unquote(m[1])
		if err != nil {
			t.Errorf("fake VL: probe pattern %s: %v", m[1], err)
			return false
		}
		patterns = append(patterns, pattern)
	}
	match := func(pattern string) bool { return regexp.MustCompile(pattern).MatchString(row.msg) }
	if len(patterns) == 0 {
		// The stored-field probe: a stored value for a filter label whose body
		// yields none (the unpack without keep_original_fields overwrites it).
		stored := false
		for _, m := range pushdownStoredRE.FindAllStringSubmatch(query, -1) {
			stored = stored || values[m[1]] != ""
		}
		if !stored {
			return false
		}
		var unpacked map[string]string
		switch {
		case strings.Contains(query, "| unpack_json fields ("):
			unpacked = vlUnpackJSONFields(row.msg)
		case strings.Contains(query, "| unpack_logfmt fields ("):
			unpacked = slidingLogfmtFields(row.msg)
		default:
			return true
		}
		for _, field := range fieldList(query[strings.Index(query, "| unpack_"):]) {
			if unpacked[field] == "" {
				return true
			}
		}
		return false
	}
	absent := func(field string) bool {
		return values[field] == "" && (f.stored == nil || values[f.stored(field)] == "")
	}
	if strings.Contains(query, "| unpack_json fields (") {
		unpack := strings.SplitN(query, "| unpack_json fields (", 2)[1]
		fields := strings.Split(unpack[:strings.Index(unpack, ")")], ", ")
		anyAbsent := false
		for _, field := range fields {
			anyAbsent = anyAbsent || absent(field)
		}
		// After the candidate pattern(s) the second filter carries one key
		// pattern per field (paired with an empty unpacked value), then the
		// repeated-key, array, escaped-key and U+FFFD patterns, then the alias.
		alias := orderedJSONLabelAliasPattern(fields)
		rest := patterns[1+len(fields):]
		if alias != "" {
			if len(patterns) < 3+len(fields) || patterns[1] != alias || patterns[len(patterns)-1] != alias {
				t.Errorf("fake VL: alias pattern missing from probe %q", query)
			}
			rest = patterns[2+len(fields) : len(patterns)-1]
		}
		candidate := match(patterns[0]) || (alias != "" && match(alias))
		if !anyAbsent || !candidate {
			return false
		}
		if alias != "" && match(alias) {
			return true
		}
		unpacked := vlUnpackJSONFields(row.msg)
		for _, field := range fields {
			if absent(field) && unpacked[field] == "" && regexp.MustCompile(`"\s*`+regexp.QuoteMeta(field)+`\s*"\s*:`).MatchString(row.msg) {
				return true
			}
		}
		for _, pattern := range rest {
			if match(pattern) {
				return true
			}
		}
		return false
	}
	// logfmt probe: one condition per field, plus the alias condition.
	alias := ""
	for _, pattern := range patterns {
		if strings.HasPrefix(pattern, `(?:^|\s)`) {
			alias = pattern
		}
	}
	for _, m := range regexp.MustCompile(`\(-([A-Za-z0-9_]+):\*`).FindAllStringSubmatch(query, -1) {
		field := m[1]
		if absent(field) && (logfmtLineIsRisk(row.msg, field) || (alias != "" && match(alias))) {
			return true
		}
	}
	return false
}

func newPushdownFakeVL(t testing.TB, rows []pushdownRow, stored func(string) string) (*httptest.Server, *pushdownFakeVL) {
	t.Helper()
	fake := &pushdownFakeVL{rows: rows, stored: stored}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		query := r.Form.Get("query")
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			fake.mu.Lock()
			fake.stats = append(fake.stats, query)
			fake.mu.Unlock()
			start, end, step, offset, ok := slidingBucketParams(t, r)
			if !ok {
				return
			}
			statsPipe := query[strings.LastIndex(query, "| stats "):]
			groupBy := fieldList(statsPipe)
			if !strings.Contains(statsPipe, "stats by (") {
				groupBy = nil
			}
			bytesMetric := strings.Contains(statsPipe, "sum_len(_msg) as c")
			type point struct {
				name   string
				labels string
			}
			points := map[point]map[int64]float64{}
			labelSets := map[string]map[string]string{}
			add := func(k point, ts int64, v float64) {
				if points[k] == nil {
					points[k] = map[int64]float64{}
				}
				points[k][ts] += v
			}
			for _, row := range fake.rows {
				ts := row.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				values, kept := fake.applyPipes(t, query, row)
				if !kept {
					continue
				}
				labels := map[string]string{}
				for _, field := range groupBy {
					labels[field] = values[field] // VictoriaLogs reports every grouped field, empty or not
				}
				key := canonicalLabelsKey(labels)
				labelSets[key] = labels
				bucket := vlTruncate(ts, step, offset)
				if bytesMetric {
					add(point{"c", key}, bucket, float64(len(row.msg)))
					add(point{"__sample_count", key}, bucket, 1)
				} else {
					add(point{"c", key}, bucket, 1)
				}
			}
			keys := make([]point, 0, len(points))
			for k := range points {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].name+keys[i].labels < keys[j].name+keys[j].labels })
			var sb strings.Builder
			sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"metric":{"__name__":%q`, k.name)
				for _, field := range groupBy {
					fmt.Fprintf(&sb, `,%q:%q`, field, labelSets[k.labels][field])
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
			fake.mu.Lock()
			if guard {
				fake.guards = append(fake.guards, query)
			} else {
				fake.raw++
			}
			fake.mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, row := range fake.rows {
				ts := row.ts.UnixNano()
				if ts < start || ts >= end {
					continue
				}
				if guard && !fake.guardMatches(t, query, row) {
					continue
				}
				encoded := map[string]string{"_time": row.ts.UTC().Format(time.RFC3339Nano), "_msg": row.msg}
				var streamParts []string
				for k, v := range row.stream {
					encoded[k] = v
					streamParts = append(streamParts, k+"="+strconv.Quote(v))
				}
				sort.Strings(streamParts)
				encoded["_stream"] = "{" + strings.Join(streamParts, ",") + "}"
				for k, v := range row.vl {
					encoded[k] = v
				}
				line, _ := json.Marshal(encoded)
				_, _ = w.Write(append(line, '\n'))
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

// lokiPushdownReference evaluates the plan with Loki semantics over the rows
// through the proxy's raw evaluator, whose parser mirrors Loki's (proven
// against Loki by the e2e parity suite): the sample at t covers (t-window, t]
// and a step without lines is absent. A line reaching the aggregation with a
// parser error fails the fixture, because Loki fails such a query.
func lokiPushdownReference(t testing.TB, plan *orderedJSONMetricPlan, rows []pushdownRow, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	out := map[string]map[int64]string{}
	for at := start; !at.After(end); at = at.Add(step) {
		values := map[string]float64{}
		for _, row := range rows {
			if !row.ts.After(at.Add(-plan.window)) || row.ts.After(at) {
				continue
			}
			base := cloneStringMap(row.stream)
			for k, v := range row.loki {
				base[k] = v
			}
			labels, ok, err := plan.processWithStreamLabels(row.msg, base, row.stream)
			if err != nil {
				t.Fatalf("reference: %q: %v", row.msg, err)
			}
			if !ok {
				continue
			}
			if labels["__error__"] != "" {
				t.Fatalf("reference: %q reaches the aggregation with %s; Loki fails such a query", row.msg, labels["__error__"])
			}
			key := canonicalLabelsKey(plan.groupLabels(labels))
			switch plan.function {
			case "bytes_over_time", "bytes_rate":
				values[key] += float64(len(row.msg))
			default:
				values[key]++
			}
		}
		for key, v := range values {
			if plan.function == "rate" || plan.function == "bytes_rate" {
				v /= plan.window.Seconds()
			}
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][at.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out
}

// filterPushdownFixture is a JSON stream in the shape of an OTel collector
// log: service_version is structured metadata (stored as service.version),
// pipeline and level are keys of the body, one line in six is unparseable and
// one is plain text.
func filterPushdownFixture(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 120; i++ {
		ts := s0.Add(time.Duration(i) * 5 * time.Second)
		row := pushdownRow{ts: ts, stream: map[string]string{"app": "api"}}
		switch i % 8 {
		case 0:
			row.stream["level"] = "info"
			row.msg = `{"level":"info","service_version":"0.96.0","pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`
		case 1: // the body's level collides with the stream label: level_extracted
			row.stream["level"] = "warn"
			row.msg = `{"level":"error","service_version":"0.96.0","pipeline":"logs/loki"}`
		case 2:
			row.msg = `{"level":"error","service_version":"0.95.0","pipeline":"logs/loki"}`
		case 3: // no service_version key
			row.msg = `{"level":"debug","pipeline":"traces/otlp"}`
		case 4: // unparseable, no keys before the error
			row.msg = `{"msg": "truncated ` + strconv.Itoa(i)
		case 5:
			row.msg = "plain text line " + strconv.Itoa(i)
		case 6: // structured metadata wins over the body's key (service_version_extracted)
			row.stream["level"] = "info"
			row.vl, row.loki = map[string]string{"service.version": "0.96.0"}, map[string]string{"service_version": "0.96.0"}
			row.msg = `{"pipeline":"logs/loki","service_version":"9.9.9"}`
		default:
			row.vl, row.loki = map[string]string{"service.version": "0.95.0"}, map[string]string{"service_version": "0.95.0"}
			row.msg = `{"level":"warn","pipeline":"logs/loki"}`
		}
		rows = append(rows, row)
	}
	return rows
}

func newFilterPushdownProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	p := newSlidingTestProxy(t, backendURL)
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	if got := p.labelTranslator.ToVL("service_version"); got != "service.version" {
		t.Fatalf("service_version is stored as %q, want service.version", got)
	}
	return p
}

// The metric shapes Grafana Explore and Logs Drilldown send for a `| json`
// stream filtered on parsed keys (Explore's logs volume with query-builder
// filters, a user's grouped and ungrouped sums without `drop __error__`, a
// Drilldown field breakdown) are answered from VictoriaLogs stats buckets
// without a raw-row scan, with the label filters pushed down as filter pipes,
// a structured-metadata label read from its stored spelling, and the values
// equal to Loki's.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-underscore-label, semantics/json-filter-pushdown-without-error-drop, semantics/json-filter-pushdown-ungrouped-sum, semantics/json-filter-pushdown-translated-label
func TestOrderedJSONFilterPushdownUsesStatsBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := filterPushdownFixture(s0)
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	const stored = " | format if (`service.version`:*) \"<service.version>\" as service_version"
	for _, tc := range []struct {
		query  string
		guards int
		stats  string
	}{
		// Explore's logs volume for `{...} | json | service_version=`0.96.0` | pipeline=`logs/loki``.
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | service_version="0.96.0" | pipeline="logs/loki" | drop __error__ [1m]))`, 1,
			`unpack_json fields (level, service_version, pipeline) keep_original_fields` + stored + ` | filter service_version:="0.96.0" | filter pipeline:="logs/loki" | stats by (level, detected_level) count() as c`},
		// A user's grouped sum: the filter excludes unparsed lines, so errors need not be dropped.
		{`sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, 2,
			`unpack_json fields (level, pipeline) keep_original_fields | filter pipeline:="logs/loki" | stats by (level) count() as c`},
		// The same without grouping.
		{`sum(count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, 2,
			`unpack_json fields (pipeline) keep_original_fields | filter pipeline:="logs/loki" | stats count() as c`},
		{`sum(rate({app="api"} | json | pipeline=~"logs/.*" [2m]))`, 2,
			`unpack_json fields (pipeline) keep_original_fields | filter pipeline:~"^(?:logs/.*)$" | stats count() as c`},
		// A Drilldown field breakdown on a structured-metadata label.
		{`sum by (service_version) (count_over_time({app="api"} | json | drop __error__ | service_version!="" [1m]))`, 1,
			`unpack_json fields (service_version) keep_original_fields` + stored + ` | filter -service_version:="" | stats by (service_version) count() as c`},
		// A field breakdown on a body key without dropping errors: `!=""` rejects unparsed lines.
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, 2,
			`unpack_json fields (pipeline) keep_original_fields | filter -pipeline:="" | stats by (pipeline) count() as c`},
		{`sum by (level, detected_level) (bytes_over_time({app="api"} | json | service_version=~"0\\.9[0-9]\\.0" | drop __error__ [1m]))`, 1,
			`unpack_json fields (level, service_version) keep_original_fields` + stored + ` | filter service_version:~"^(?:0\\.9[0-9]\\.0)$" | stats by (level, detected_level) sum_len(_msg) as c, count() as __sample_count`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v plan=%+v", ok, plan)
			}
			want := lokiPushdownReference(t, plan, rows, start, end, time.Minute)
			if len(want) == 0 {
				t.Fatal("fixture yields no series")
			}
			got := runJSONVolumeQueryRange(t, p, tc.query, start, end, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 || len(fake.stats) != 1 || len(fake.guards) != tc.guards {
				t.Fatalf("expected %d probes and one stats call without raw rows: raw=%d stats=%q guards=%q", tc.guards, fake.raw, fake.stats, fake.guards)
			}
			if !strings.HasSuffix(fake.stats[0], tc.stats) {
				t.Fatalf("stats query %q does not end with %q", fake.stats[0], tc.stats)
			}
		})
	}
}

// Each probe keeps the exact raw evaluator for a line the two parsers read
// differently, and the result still equals Loki's: a nested object Loki
// flattens to the filtered label, a key spelled with a character Loki
// sanitizes to an underscore, a partial line holding the filtered key before
// its syntax error when errors are not dropped, and a stored label on a line
// when errors are not dropped.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe, semantics/partial-parse-before-error, semantics/json-filter-pushdown-without-error-drop
func TestOrderedJSONFilterPushdownProbesKeepRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	base := func(i int) pushdownRow {
		return pushdownRow{ts: s0.Add(time.Duration(i) * 10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"service_version":"0.96.0","pipeline":"logs/loki"}`}
	}
	volume := `sum by (level, detected_level) (count_over_time({app="api"} | json | service_version="0.96.0" | pipeline="logs/loki" | drop __error__ [1m]))`
	for _, tc := range []struct {
		name, query, msg string
		vl, loki         map[string]string
		guards           int
		pipelineError    bool // Loki fails the query: the line passes the filter with a parser error
	}{
		{"nested key", volume, `{"service":{"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 1, false},
		{"dotted key", volume, `{"service.version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 1, false},
		{"hyphenated key", volume, `{"service-version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 1, false},
		{"key before syntax error", `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, `{"pipeline":"logs/loki","msg": truncated`, nil, nil, 2, true},
		{"stored filter label", `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, `{"n":1}`, map[string]string{"pipeline": "logs/loki"}, map[string]string{"pipeline": "logs/loki"}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []pushdownRow{base(0), base(1), base(2)}
			risky := base(3)
			risky.stream = map[string]string{"app": "api"}
			risky.msg, risky.vl, risky.loki = tc.msg, tc.vl, tc.loki
			rows = append(rows, risky)
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("expected a pushdown plan, got ok=%v", ok)
			}
			at := s0.Add(time.Minute)
			if tc.pipelineError {
				params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "pipeline error") {
					t.Fatalf("expected Loki's pipeline error, got %d %s", rec.Code, rec.Body)
				}
			} else {
				want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
				got := runJSONVolumeQueryRange(t, p, tc.query, at, at, time.Minute)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 1 || len(fake.guards) != tc.guards {
				t.Fatalf("expected the probe to keep the raw evaluator: raw=%d guards=%q stats=%q", fake.raw, fake.guards, fake.stats)
			}
		})
	}
}

// An ingestion route that stores the body's keys as fields (the e2e
// generator's VictoriaLogs JSON route stores pipeline beside _msg) keeps the
// pushdown without `drop __error__`: the stored-field probe fires only for a
// stored value whose body yields none.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop
func TestOrderedJSONFilterPushdownStoredBodyKeyKeepsStats(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 12; i++ {
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api", "level": "info"},
			vl: map[string]string{"pipeline": "logs/loki"}, loki: map[string]string{"pipeline": "logs/loki"},
			msg: `{"pipeline":"logs/loki","n":` + strconv.Itoa(i) + `}`})
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	query := `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`
	plan, _ := compileOrderedJSONMetric(query)
	at := s0.Add(time.Minute)
	want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
	got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 || len(fake.stats) != 1 || len(fake.guards) != 2 {
		t.Fatalf("expected two probes and one stats call without raw rows: raw=%d stats=%q guards=%q", fake.raw, fake.stats, fake.guards)
	}
	// The two probes run concurrently, so find the stored-field one by shape.
	probe := ` | filter pipeline:* | unpack_json fields (pipeline) | filter pipeline:="" | limit 1`
	if !strings.HasSuffix(fake.guards[0], probe) && !strings.HasSuffix(fake.guards[1], probe) {
		t.Fatalf("stored-field probe %q does not end with %q", fake.guards, probe)
	}
}

// The spelling probe matches JSON text in which Loki's parser yields the
// label from a key spelled differently, and only such text.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe
func TestOrderedJSONLabelAliasPattern(t *testing.T) {
	for _, tc := range []struct {
		label, text string
		match       bool
	}{
		{"service_version", `{"service_version":"1"}`, false},
		{"service_version", `{"service-version":"1"}`, true},
		{"service_version", `{"service.version":"1"}`, true},
		{"service_version", `{"service version":"1"}`, true},
		{"service_version", `{"service/version":"1"}`, true},
		{"service_version", `{"servicé_version":"1"}`, false},
		{"service_version", `{"service":{"version":"1"}}`, true},
		{"service_version", `{"service": { "x": 1, "version": "1" }}`, true},
		{"service_version", `{"service":{"ver-sion":"1"}}`, false}, // service_ver_sion in Loki
		{"service_version", `{"version":"1","service":"2"}`, false},
		{"service_version", `{"service":"a","version":"b"}`, false},
		{"service_version", `{"a":{"service_version":"1"}}`, false},
		// The e2e generator's shape: a service object beside a top-level version.
		{"service_version", `{"service": {"name": "api-gateway"}, "method": "GET", "level": "info", "version": "v1"}`, false},
		{"service_version", `{"service":{"name":"api","meta":{"a":{"b":1}}},"version":"v1"}`, false},
		{"service_version", `{"service":{"msg":"{\"version\":1}"},"version":"v1"}`, false},
		{"service_version", `{"service":{"a":{"b":{"c":1}},"version":"1"}}`, true},              // sibling three deep
		{"service_version", `{"service":{"msg":"}","version":"1"}}`, true},                      // brace inside a string
		{"service_version", `{"service":{"":{"version":"1"}}}`, true},                           // blank key skipped by Loki
		{"service_version", `{"service":{"a":{"b":{"c":{"d":{"e":1}}}}},"version":"1"}}`, true}, // sibling too deep: matched outright
		{"service_version", `{"service":{"a":{"b":{"c":{"d":{"e":1}}}}}},"version":"1"}`, true}, // same, though version is outside
		{"service_version", `{"x":{"y":{"z":{"w":{"v":1}}}}},"version":2}`, false},              // deep nesting outside the parent
		{"service_version", `{"x":{"y":{"z":{"w":1}}}},"service":{"name":1},"version":2}`, false},
		{"service_version", `{" service ":{"version":"1"}}`, true},                       // Loki trims unicode spaces
		{"k8s_pod_labels_app", `{"k8s":{"x":1},"pod":{"labels":{"y":2}},"app":3}`, true}, // three underscores: parts in order suffice
		{"k8s_pod_name", `{"k8s":{"pod":{"name":"p"}}}`, true},
		{"k8s_pod_name", `{"k8s.pod.name":"p"}`, true},
		{"k8s_pod_name", `{"k8s_pod":{"name":"p"}}`, true},
		{"k8s_pod_name", `{"k8s":{"pod_name":"p"}}`, true},
		{"k8s_pod_name", `{"k8s_pod_name":"p"}`, false},
		{"k8s_pod_name", `{"k8s":{"pod":"p"},"name":"n"}`, false},
		{"_1st", `{"1st":"a"}`, true},
		{"_1st", `{"_1st":"a"}`, false},
		{"_1st", `{"-1st":"a"}`, true},
		{"a__b", `{"a\"b":"1"}`, true},
		{"a__b", `{"a":{"":{"b":"1"}}}`, false},
		{"a__b", `{"a":{"_b":"1"}}`, true},
		{"a__b", `{"a_":{"b":"1"}}`, true},
	} {
		pattern := orderedJSONLabelAliasPattern([]string{tc.label})
		if pattern == "" {
			t.Fatalf("%s: no pattern", tc.label)
		}
		if got := regexp.MustCompile(pattern).MatchString(tc.text); got != tc.match {
			t.Errorf("%s on %s: match=%v, want %v (pattern %s)", tc.label, tc.text, got, tc.match, pattern)
		}
	}
	if got := orderedJSONLabelAliasPattern([]string{"level", "pipeline"}); got != "" {
		t.Errorf("labels without an underscore have no other spelling, got %s", got)
	}
	for _, tc := range []struct {
		text  string
		match bool
	}{
		{`service_version=1 x=2`, false},
		{`service-version=1`, true},
		{`x=1 service.version=1`, true},
		{`service version=1`, false},
		{`msg="service-version=1"`, false},
	} {
		if got := regexp.MustCompile(logfmtKeyAliasPattern([]string{"service_version"})).MatchString(tc.text); got != tc.match {
			t.Errorf("logfmt %q: match=%v, want %v", tc.text, got, tc.match)
		}
	}
}

// The pushdown admits a label filter without `drop __error__` only when the
// filter rejects the empty value, and reads labels through their parsed key
// unless Loki never yields them from one.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop, semantics/json-filter-pushdown-ungrouped-sum
func TestOrderedJSONFilterPushdownEligibility(t *testing.T) {
	for _, tc := range []struct {
		query        string
		pushdown     bool
		fields       []string
		errorFilters []string
	}{
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="logs/loki" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum(count_over_time({env="production"} | json | pipeline="logs/loki" [1m]))`, true, nil, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!="logs/loki" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!="" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline=~".*" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline=~"logs/.*" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!~"logs/.*" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline!~".*" [1m]))`, true, []string{"level"}, []string{"pipeline"}},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="x" | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level) (count_over_time({env="production"} | json | pipeline="x" | trace_id!="" | drop __error_details__ [1m]))`, true, []string{"level"}, []string{"pipeline", "trace_id"}},
		{`sum by (service_version, detected_level) (count_over_time({env="production"} | json | drop __error__ [1m]))`, true, []string{"service_version", "level"}, nil},
		{`sum by (k8s_pod_name) (count_over_time({env="production"} | json | drop __error__ | k8s_pod_name!="" [1m]))`, true, []string{"k8s_pod_name"}, nil},
		{`sum(count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (__error__) (count_over_time({env="production"} | json | pipeline="x" [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | __error__="" [1m]))`, false, nil, nil},
		{`sum by (level_extracted) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (service_name) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (level) (count_over_time({env="production"} | json | detected_level="error" | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (a_b_c_d_e_f_g_h) (count_over_time({env="production"} | json | drop __error__ [1m]))`, false, nil, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | service_version="1" | drop __error__ [1m]))`, true, []string{"level"}, nil},
		{`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | service_version="1" [1m]))`, true, []string{"level"}, []string{"service_version"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok {
				if tc.pushdown {
					t.Fatal("expected an ordered JSON plan")
				}
				return
			}
			if plan.pushdown != tc.pushdown || !reflect.DeepEqual(plan.unpackFields, tc.fields) || !reflect.DeepEqual(plan.pushdownErrorFilters, tc.errorFilters) {
				t.Fatalf("pushdown=%v fields=%q errorFilters=%q, want %v %q %q", plan.pushdown, plan.unpackFields, plan.pushdownErrorFilters, tc.pushdown, tc.fields, tc.errorFilters)
			}
		})
	}
}

// A `| logfmt` volume filtered on an underscore label keeps the raw
// evaluator when a line spells the key with a character Loki sanitizes.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe
func TestLogfmtFilterPushdownAliasKeepsRawEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api"}, msg: "level=info service_version=1 pipeline=logs"},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api"}, msg: "level=warn service_version=2 pipeline=logs"},
	}
	query := `sum by (level, detected_level) (count_over_time({app="api"} | logfmt | pipeline="logs" | drop __error__ [1m]))`
	for _, tc := range []struct {
		name, msg string
		raw       int
	}{
		{"same spelling", "level=error pipeline=logs service_version=3", 0},
		{"sanitized key", "level=error pipe-line=logs", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(rows, pushdownRow{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api"}, msg: tc.msg})
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			query := strings.ReplaceAll(query, "pipeline", "pipe_line")
			plan, ok := compileOrderedJSONMetric(query)
			if !ok || !plan.pushdown {
				t.Fatal("expected a pushdown plan")
			}
			at := s0.Add(time.Minute)
			want := lokiPushdownReference(t, plan, rows, at, at, time.Minute)
			got := runJSONVolumeQueryRange(t, p, query, at, at, time.Minute)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != tc.raw || len(fake.guards) != 1 {
				t.Fatalf("raw=%d guards=%q, want raw=%d", fake.raw, fake.guards, tc.raw)
			}
		})
	}
}

// The evaluator counter distinguishes stats-bucket answers from raw-row
// evaluations on /metrics, and says why the raw evaluator answered: a probe
// found a line the parsers read differently, or the pipeline is not one the
// pushdown covers.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-underscore-label
func TestRangeMetricEvaluatorCounter(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := filterPushdownFixture(s0)
	rows = append(rows, pushdownRow{ts: s0.Add(2 * time.Minute), stream: map[string]string{"app": "api"}, msg: `{"service-version":"0.96.0","pipeline":"logs/loki"}`})
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	start, end := s0.Add(time.Minute), s0.Add(10*time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | pipeline="logs/loki" | drop __error__ [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level_extracted) (count_over_time({app="api"} | json | drop __error__ [1m]))`, start, end, time.Minute)
	runJSONVolumeQueryRange(t, p, `sum by (level) (count_over_time({app="api"} | json | service_version="0.96.0" | drop __error__ [1m]))`, start, end, time.Minute)
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.Bytes()
	for _, want := range []string{
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="raw_rows",reason="ineligible"} 1`,
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="raw_rows",reason="probe"} 1`,
		`loki_vl_proxy_parser_metric_evaluations_total{evaluator="vl_stats_buckets",reason="pushdown"} 2`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("/metrics lacks %q in:\n%s", want, body)
		}
	}
}
