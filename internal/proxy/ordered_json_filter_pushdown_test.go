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

var pushdownFormatRE = regexp.MustCompile("^format if \\(`?([^`:]+)`?:\\*\\) \"<([^>]+)>\" as (\\S+)$")

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

// splitLogsQLPipes splits a query at " | " outside quoted strings.
func splitLogsQLPipes(query string) []string {
	var parts []string
	var quote byte
	last := 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == ' ' && strings.HasPrefix(query[i:], " | "):
			parts = append(parts, query[last:i])
			last = i + 3
			i += 2
		}
	}
	return append(parts, query[last:])
}

// fakeLogsQLFilter parses and evaluates the filter expressions the pushdown
// and its probes emit with VictoriaLogs semantics over a row's values:
// `field:="v"`, `field:~"re"`, `field:*`, `field:"phrase"` and `field:word`
// (a word delimited by non-word characters; letters, digits and underscores
// are word characters), `-`/`!` negation, parentheses, implicit and, `or`.
type fakeLogsQLFilter struct {
	t    testing.TB
	src  string
	pos  int
	vals map[string]string
}

func (f *fakeLogsQLFilter) skipSpace() {
	for f.pos < len(f.src) && f.src[f.pos] == ' ' {
		f.pos++
	}
}

func (f *fakeLogsQLFilter) peek(s string) bool {
	f.skipSpace()
	return strings.HasPrefix(f.src[f.pos:], s)
}

func (f *fakeLogsQLFilter) expr() bool {
	result := f.term()
	for f.peek("or ") || f.peek("OR ") {
		f.pos += 3
		result = f.term() || result
	}
	return result
}

func (f *fakeLogsQLFilter) term() bool {
	result := f.factor()
	for {
		f.skipSpace()
		if f.pos >= len(f.src) || f.peek(")") || f.peek("or ") || f.peek("OR ") {
			return result
		}
		if f.peek("and ") || f.peek("AND ") {
			f.pos += 4
		}
		result = f.factor() && result
	}
}

func (f *fakeLogsQLFilter) factor() bool {
	f.skipSpace()
	if f.peek("-") || f.peek("!") {
		f.pos++
		return !f.factor()
	}
	if f.peek("(") {
		f.pos++
		result := f.expr()
		if !f.peek(")") {
			f.t.Errorf("fake VL: expected ) at %d in %q", f.pos, f.src)
			return false
		}
		f.pos++
		return result
	}
	return f.fieldFilter()
}

func (f *fakeLogsQLFilter) quoted() string {
	f.skipSpace()
	if f.pos < len(f.src) && f.src[f.pos] == '`' {
		end := strings.IndexByte(f.src[f.pos+1:], '`')
		s := f.src[f.pos+1 : f.pos+1+end]
		f.pos += end + 2
		return s
	}
	if f.pos < len(f.src) && f.src[f.pos] == '"' {
		end := f.pos + 1
		for end < len(f.src) && f.src[end] != '"' {
			if f.src[end] == '\\' {
				end++
			}
			end++
		}
		s, err := strconv.Unquote(f.src[f.pos : end+1])
		if err != nil {
			f.t.Errorf("fake VL: bad string at %d in %q: %v", f.pos, f.src, err)
		}
		f.pos = end + 1
		return s
	}
	start := f.pos
	for f.pos < len(f.src) && f.src[f.pos] != ' ' && f.src[f.pos] != ')' && f.src[f.pos] != ':' {
		f.pos++
	}
	return f.src[start:f.pos]
}

func (f *fakeLogsQLFilter) fieldFilter() bool {
	name := f.quoted()
	if !strings.HasPrefix(f.src[f.pos:], ":") {
		f.t.Errorf("fake VL: expected : after %q at %d in %q", name, f.pos, f.src)
		return false
	}
	f.pos++
	value := f.vals[name]
	switch {
	case strings.HasPrefix(f.src[f.pos:], "*"):
		f.pos++
		return value != ""
	case strings.HasPrefix(f.src[f.pos:], "="):
		f.pos++
		return value == f.quoted()
	case strings.HasPrefix(f.src[f.pos:], "~"):
		f.pos++
		return regexp.MustCompile(f.quoted()).MatchString(value)
	case strings.HasPrefix(f.src[f.pos:], "!"):
		f.pos++
		return value != f.quoted()
	default:
		word := f.quoted()
		return regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(word) + `([^A-Za-z0-9_]|$)`).MatchString(value)
	}
}

func fakeLogsQLMatch(t testing.TB, expr string, values map[string]string) bool {
	f := &fakeLogsQLFilter{t: t, src: expr, vals: values}
	result := f.expr()
	f.skipSpace()
	if f.pos != len(f.src) {
		t.Errorf("fake VL: trailing text at %d in %q", f.pos, expr)
	}
	return result
}

// unpackOptions parses `[from f] [fields (a, b)] [keep_original_fields] [result_prefix "p"]`.
func unpackOptions(pipe string) (fields []string, keep bool, prefix string) {
	if i := strings.Index(pipe, "fields ("); i >= 0 {
		fields = strings.Split(pipe[i+8:i+8+strings.Index(pipe[i+8:], ")")], ", ")
		for j, field := range fields {
			fields[j] = strings.Trim(field, "`")
		}
	}
	if i := strings.Index(pipe, `result_prefix "`); i >= 0 {
		prefix = pipe[i+15 : i+15+strings.Index(pipe[i+15:], `"`)]
	}
	return fields, strings.Contains(pipe, "keep_original_fields"), prefix
}

// applyPipes evaluates a query (its base filter and every pipe before the
// stats pipe) over one row with VictoriaLogs semantics and reports the row's
// values and whether it survives the filters.
func (f *pushdownFakeVL) applyPipes(t testing.TB, query string, row pushdownRow) (map[string]string, bool) {
	values := f.values(row)
	pipes := splitLogsQLPipes(query)
	if !fakeLogsQLMatch(t, pipes[0], values) {
		return values, false
	}
	for _, pipe := range pipes[1:] {
		switch {
		case strings.HasPrefix(pipe, "unpack_json") || strings.HasPrefix(pipe, "unpack_logfmt"):
			var unpacked map[string]string
			if strings.HasPrefix(pipe, "unpack_json") {
				unpacked = vlUnpackJSONFields(values["_msg"])
			} else {
				unpacked = slidingLogfmtFields(values["_msg"])
			}
			fields, keep, prefix := unpackOptions(pipe)
			if fields == nil {
				for field := range unpacked {
					fields = append(fields, field)
				}
			}
			for _, field := range fields {
				if keep && values[prefix+field] != "" {
					continue
				}
				if v, ok := unpacked[field]; ok {
					values[prefix+field] = v
				} else if !keep {
					values[prefix+field] = ""
				}
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
		case strings.HasPrefix(pipe, "replace_regexp ("):
			m := regexp.MustCompile(`^replace_regexp \(("(?:[^"\\]|\\.)*"), ("(?:[^"\\]|\\.)*")\) at (\S+)$`).FindStringSubmatch(pipe)
			if m == nil {
				t.Errorf("fake VL: unsupported replace_regexp pipe %q", pipe)
				return nil, false
			}
			re, _ := strconv.Unquote(m[1])
			repl, _ := strconv.Unquote(m[2])
			values[m[3]] = regexp.MustCompile(re).ReplaceAllString(values[m[3]], repl)
		case strings.HasPrefix(pipe, "filter "):
			if !fakeLogsQLMatch(t, strings.TrimPrefix(pipe, "filter "), values) {
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

// guardMatches evaluates a `| limit 1` probe for one row.
func (f *pushdownFakeVL) guardMatches(t testing.TB, query string, row pushdownRow) bool {
	_, kept := f.applyPipes(t, query, row)
	return kept
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
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | service_version="0.96.0" | pipeline="logs/loki" | drop __error__ [1m]))`, 2,
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
		{`sum by (service_version) (count_over_time({app="api"} | json | drop __error__ | service_version!="" [1m]))`, 2,
			`unpack_json fields (service_version) keep_original_fields` + stored + ` | filter -service_version:="" | stats by (service_version) count() as c`},
		// A field breakdown on a body key without dropping errors: `!=""` rejects unparsed lines.
		{`sum by (pipeline) (count_over_time({app="api"} | json | pipeline!="" [1m]))`, 2,
			`unpack_json fields (pipeline) keep_original_fields | filter -pipeline:="" | stats by (pipeline) count() as c`},
		{`sum by (level, detected_level) (bytes_over_time({app="api"} | json | service_version=~"0\\.9[0-9]\\.0" | drop __error__ [1m]))`, 2,
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
		{"nested key", volume, `{"service":{"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"dotted key", volume, `{"service.version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"hyphenated key", volume, `{"service-version":"0.96.0","pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"spaced nested parent", volume, `{" service ":{"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"blank key in the nesting", volume, `{"service":{"":{"version":"0.96.0"}},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"sibling nested five deep", volume, `{"service":{"a":{"b":{"c":{"d":{"e":1}}}}},"version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		{"brace inside a sibling string", volume, `{"service":{"msg":"}","version":"0.96.0"},"pipeline":"logs/loki"}`, nil, nil, 2, false},
		// Loki reads the nested keys before the syntax error and, with the
		// error dropped, counts the line; unpack_json reads nothing.
		{"nested key before syntax error", volume, `{"service":{"version":"0.96.0"},"pipeline":"logs/loki","msg": truncated`, nil, nil, 2, false},
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

// The key-spelling probe's parts: the word prefilter holds every piece of
// the label between underscores, the flat key regexp matches a key spelled
// with a sanitized rune, spaces or a blank name (and only such keys; nesting
// is left to unpack_json), and the nested splits name every dotted field a
// nesting can produce. None of the patterns needs balanced braces, so
// VictoriaLogs prepares them in microseconds instead of a third of a second.
// conformance: parser-error-and-label-collision, semantics/json-label-spelling-probe
func TestOrderedJSONKeySpellingProbeParts(t *testing.T) {
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
		{"service_version", `{"service":{"version":"1"}}`, false}, // nesting: verified by unpack_json
		{"service_version", `{"version":"1","service":"2"}`, false},
		{"service_version", `{"a":{"service_version":"1"}}`, false},
		{"service_version", `{"service": {"name": "api-gateway"}, "method": "GET", "level": "info", "version": "v1"}`, false},
		{"service_version", `{" service ":{"version":"1"}}`, true},    // Loki trims unicode spaces
		{"service_version", `{"service":{"":{"version":"1"}}}`, true}, // blank key skipped by Loki
		{"k8s_pod_name", `{"k8s.pod.name":"p"}`, true},
		{"k8s_pod_name", `{"k8s-pod":{"name":"p"}}`, true}, // a piece spelled with a sanitized rune
		{"k8s_pod_name", `{"k8s_pod":{"name":"p"}}`, false},
		{"k8s_pod_name", `{"k8s_pod_name":"p"}`, false},
		{"_1st", `{"1st":"a"}`, true},
		{"_1st", `{"_1st":"a"}`, false},
		{"_1st", `{"-1st":"a"}`, true},
		{"a__b", `{"a\"b":"1"}`, true},
		{"a__b", `{"a":{"_b":"1"}}`, false},
		{"a__b", `{"a_":{"b":"1"}}`, false},
	} {
		pattern := orderedJSONKeyAliasPattern([]string{tc.label})
		if pattern == "" {
			t.Fatalf("%s: no pattern", tc.label)
		}
		if strings.Contains(pattern, `\{(?:`) {
			t.Fatalf("%s: pattern nests braces: %s", tc.label, pattern)
		}
		if got := regexp.MustCompile(pattern).MatchString(tc.text); got != tc.match {
			t.Errorf("%s on %s: match=%v, want %v (pattern %s)", tc.label, tc.text, got, tc.match, pattern)
		}
	}
	if got := orderedJSONKeyAliasPattern([]string{"level", "pipeline"}); got != "" {
		t.Errorf("labels without an underscore have no other spelling, got %s", got)
	}
	for _, tc := range []struct {
		label string
		spans []string
	}{
		{"service_version", []string{"service", "service_version", "version"}},
		{"k8s_pod_name", []string{"k8s", "k8s_pod", "k8s_pod_name", "pod", "pod_name", "name"}},
		{"_1st", []string{"_1st", "1st"}},
		{"a__b", []string{"a", "a_", "a__b", "_b", "b"}},
		{"level", []string{"level"}},
	} {
		if got := orderedJSONLabelSpans(tc.label); !reflect.DeepEqual(got, tc.spans) {
			t.Errorf("spans of %s: %q, want %q", tc.label, got, tc.spans)
		}
	}
	if got := logsqlWordPrefilter([]string{"service", "or", "service"}); got != `(_msg:"service" or _msg:"or")` {
		t.Errorf("word prefilter: %s", got)
	}
	for _, tc := range []struct {
		labels []string
		want   string
	}{
		{[]string{"level", "service_version"}, `(_msg:"service_version" or (_msg:"service" _msg:"version"))`},
		{[]string{"_1st"}, `((_msg:"_1st" or _msg:"1st") or _msg:"1st")`},
		{[]string{"a__b"}, `(_msg:"a__b" or (_msg:"a" _msg:"_b") or (_msg:"a_" _msg:"b") or (_msg:"a" _msg:"b"))`},
		{[]string{"level"}, ""},
	} {
		if got := orderedJSONSpellingPrefilter(tc.labels); got != tc.want {
			t.Errorf("spelling prefilter of %q: %s, want %s", tc.labels, got, tc.want)
		}
	}
	for _, tc := range []struct {
		label  string
		dotted []string
		loose  []string
	}{
		{"service_version", []string{"service.version"}, []string{`{"service":{"version":1}}`, `{"service": {"name": "a"}, "version": "v1"}`}},
		{"k8s_pod_name", []string{"k8s.pod_name", "k8s_pod.name", "k8s.pod.name"}, []string{`{"k8s":{"pod":{"name":"p"}}}`, `{"k8s_pod":{"name":"p"}}`}},
		{"_1st_x", []string{"_1st.x", "1st.x"}, []string{`{"1st":{"x":1}}`}},
		{"a__b", []string{"a._b", "a_.b"}, nil},
		{"level", nil, nil},
	} {
		dotted, loose := orderedJSONNestedSplits([]string{tc.label})
		if !reflect.DeepEqual(dotted, tc.dotted) {
			t.Errorf("nested names of %s: %q, want %q", tc.label, dotted, tc.dotted)
		}
		for _, text := range tc.loose {
			matched := false
			for _, pattern := range loose {
				matched = matched || regexp.MustCompile(pattern).MatchString(text)
			}
			if !matched {
				t.Errorf("no loose pattern of %s matches %s: %q", tc.label, text, loose)
			}
		}
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

// A label filter before the parser reads stream labels and structured
// metadata, which VictoriaLogs filters in the base query, so a Drilldown
// field breakdown filtered on a stream label (`{env="production",
// namespace="monitoring"} | detected_level="info" | json | drop __error__,
// __error_details__ | export_ms!="" | pipeline="logs/loki"`) is answered
// from stats buckets instead of the raw-row evaluator that took 1.6 s over
// the namespace's 24 h while VictoriaLogs answered in 4 ms.
// conformance: parser-error-and-label-collision, semantics/json-filter-pushdown-without-error-drop, semantics/label-filter-before-parser-pushdown
func TestOrderedJSONFilterPushdownLabelFilterBeforeParser(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 24; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"app": "api", "namespace": "prod", "level": "info"},
			msg: `{"level":"info","export_ms":"` + strconv.Itoa(10+i%3) + `","pipeline":"logs/loki"}`}
		if i%4 == 1 {
			row.stream["namespace"] = "dev"
		}
		if i%4 == 2 {
			row.msg = "plain text " + strconv.Itoa(i)
		}
		rows = append(rows, row)
	}
	query := `sum by (export_ms) (count_over_time({app="api"} | namespace="prod" | json | drop __error__, __error_details__ | export_ms!="" | pipeline="logs/loki" [1m]))`
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	plan, ok := compileOrderedJSONMetric(query)
	if !ok || !plan.pushdown {
		t.Fatalf("expected a pushdown plan, got ok=%v plan=%+v", ok, plan)
	}
	if !strings.Contains(plan.fetchQuery, `namespace="prod"`) || len(plan.stages) != 4 {
		t.Fatalf("the filter before the parser belongs to the fetch query: %q stages=%d", plan.fetchQuery, len(plan.stages))
	}
	var selected []pushdownRow
	for _, row := range rows {
		if row.stream["namespace"] == "prod" {
			selected = append(selected, row)
		}
	}
	start, end := s0.Add(time.Minute), s0.Add(2*time.Minute)
	want := lokiPushdownReference(t, plan, selected, start, end, time.Minute)
	if len(want) != 3 {
		t.Fatalf("fixture yields %d series, want 3", len(want))
	}
	got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got, want)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != 0 || len(fake.stats) != 1 || !strings.Contains(fake.stats[0], `namespace:="prod"`) {
		t.Fatalf("expected one stats call filtered on the stream label without raw rows: raw=%d stats=%q", fake.raw, fake.stats)
	}
}

// A `| json` metric that Loki fails on a line that is not a JSON object
// (no `drop __error__`, no filter an unparsed line fails) is failed the way
// Loki fails it: the exact text of logqlmodel.PipelineError with the line's
// series, and before the rows are scanned, from one `| limit 1` lookup of
// such a line. A line at the window's left edge is excluded, as Loki
// excludes it.
// conformance: parser-error-and-label-collision, status-400, semantics/pipeline-error-text-parity
func TestOrderedJSONPipelineErrorMatchesLokiText(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := []pushdownRow{
		{ts: s0.Add(10 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"200"}`},
		{ts: s0.Add(20 * time.Second), stream: map[string]string{"app": "api", "level": "info"}, msg: `{"status":"500"}`},
		{ts: s0.Add(30 * time.Second), stream: map[string]string{"app": "api", "level": "warn"}, msg: `plain text`},
	}
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	run := func(query string, at time.Time) (int, string) {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		var body struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Error
	}
	query := `sum by (status) (count_over_time({app="api"} | json [1m]))`
	code, msg := run(query, s0.Add(time.Minute))
	// The series carries the labels Loki derives at ingest (detected_level,
	// service_name), which the proxy derives on the read path.
	want := "pipeline error: 'JSONParserErr' for series: '{__error__=\"JSONParserErr\", __error_details__=\"Value looks like object, but can't find closing '}' symbol\", app=\"api\", detected_level=\"warn\", level=\"warn\", service_name=\"api\"}'.\n" +
		"Use a label filter to intentionally skip this error. (e.g | __error__!=\"JSONParserErr\").\n" +
		"To skip all potential errors you can match empty errors.(e.g __error__=\"\")\n" +
		"The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__=\"\" )\n"
	if code != http.StatusBadRequest || msg != want {
		t.Fatalf("got %d %q\nwant 400 %q", code, msg, want)
	}
	fake.mu.Lock()
	if fake.raw != 0 || len(fake.guards) != 1 || !strings.HasSuffix(fake.guards[0], ` | filter -_msg:~"^\\s*\\{" | limit 1`) {
		t.Fatalf("expected the error from one lookup without a raw scan: raw=%d guards=%q", fake.raw, fake.guards)
	}
	fake.mu.Unlock()
	// The plain line sits exactly one window before the evaluation: outside
	// Loki's left-open window, so the query succeeds.
	if code, msg := run(query, s0.Add(90*time.Second)); code != http.StatusOK {
		t.Fatalf("a line at the window's left edge must not fail the query: %d %s", code, msg)
	}
	// A filter an unparsed line fails excludes it, so the pushdown answers
	// without the lookup.
	fake.mu.Lock()
	raw, guards := fake.raw, len(fake.guards)
	fake.mu.Unlock()
	if code, _ := run(`sum by (status) (count_over_time({app="api"} | json | status!="" [1m]))`, s0.Add(time.Minute)); code != http.StatusOK {
		t.Fatalf("a filter rejecting the empty value excludes unparsed lines: %d", code)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.raw != raw || len(fake.guards) != guards+2 {
		t.Fatalf("expected the stats pushdown and its two probes to answer the filtered query: raw=%d guards=%q", fake.raw-raw, fake.guards[guards:])
	}
}
