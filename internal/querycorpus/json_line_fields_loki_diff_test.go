package querycorpus_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The Loki differential of JSON lines that VictoriaLogs stored without their line (profiles/json-line-stored-without-line):
// a Loki push of a JSON line to VictoriaLogs' Loki push route without disable_message_parsing=1 stores every key of
// the line as a field and VictoriaLogs' -defaultMsgValue placeholder in _msg. Real Loki (the engine oracle for metric
// queries, the process oracle for each line of a log query, test/oracle/loki) holds the pushed lines; real
// VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) holds the rows as that push stores them; the
// proxy's real query and query_range handlers answer in front of it.
//
//   - Data (jlFixture): two streams of JSON lines pushed without structured metadata (stored as fields with the
//     placeholder in _msg), two streams of logfmt lines (stored in _msg as pushed), and one stream of JSON lines pushed
//     with structured metadata beside them (trace_id), which VictoriaLogs stores among the line's fields. The JSON lines
//     are flat, their values strings and their keys sorted, so the line the proxy rebuilds from the fields is the
//     pushed line byte for byte.
//   - Variants: VictoriaLogs' default placeholder, and a customised -defaultMsgValue the proxy is told about
//     (-backend-default-msg-value).
//   - Modes: the proxy as deployed by default, and with -json-line-fields-as-line-keys.
//
// The answers are recorded (jsonLineAnswersPath; LVP_RUN_ORACLES=1 records them again). The proxy's answers with the
// flag off are pinned in the same file byte for byte (recorded once with LVP_JSONLINE_PIN_OFF=1, kept by a later
// recording): the flag off changes nothing.

const jsonLineAnswersPath = "testdata/json-line-fields-loki-diff.jsonl.gz"

// jlCustomPlaceholder is the -defaultMsgValue of the customised variant.
const jlCustomPlaceholder = "no message stored"

// jlStream is one stream of the fixture: its labels, whether its lines are JSON (stored as fields), and the
// structured metadata pushed beside every line.
type jlStream struct {
	Labels   map[string]string `json:"labels"`
	JSON     bool              `json:"json"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

var jlStreams = []jlStream{
	{Labels: map[string]string{"app": "jl", "fmt": "json", "pod": "a", "service_name": "jl"}, JSON: true},
	{Labels: map[string]string{"app": "jl", "fmt": "json", "pod": "b", "service_name": "jl"}, JSON: true},
	{Labels: map[string]string{"app": "jl", "fmt": "logfmt", "pod": "c", "service_name": "jl"}},
	{Labels: map[string]string{"app": "jl", "fmt": "logfmt", "pod": "d", "service_name": "jl"}},
	{Labels: map[string]string{"app": "jlsm", "pod": "e", "service_name": "jlsm"}, JSON: true, Metadata: map[string]string{"trace_id": "t1"}},
}

// jlEntry is one line: its stream, time, the line Loki stores and its keys (the fields VictoriaLogs stores for a JSON
// line).
type jlEntry struct {
	Stream int         `json:"stream"`
	TS     int64       `json:"ts"`
	Line   string      `json:"line"`
	Keys   [][2]string `json:"keys"`
}

// jlFixture is the data of every query: a line every 37 seconds per stream over the metric window and 20 minutes
// before it, each stream on a phase of its own, half a millisecond past a millisecond.
func jlFixture() []jlEntry {
	var out []jlEntry
	span := metricWindow + 20*time.Minute
	first := metricEnd.Add(-span)
	const every = 37 * time.Second
	for s, st := range jlStreams {
		for i := 0; ; i++ {
			at := first.Add(time.Duration(i)*every + time.Duration(s)*5*time.Second + time.Duration((i*7919+s*31)%900)*time.Millisecond + 500*time.Microsecond)
			if !at.Before(metricEnd) {
				break
			}
			keys := [][2]string{{"amount", strconv.Itoa((i*7919 + s*104729) % 500)}}
			if i%4 == 0 {
				keys = append(keys, [2]string{"region", "eu"})
			}
			keys = append(keys, [2]string{"route", "/pay"}, [2]string{"status", []string{"200", "500"}[i%2]})
			var line string
			if st.JSON {
				parts := make([]string, len(keys))
				for k, kv := range keys {
					parts[k] = strconv.Quote(kv[0]) + ":" + strconv.Quote(kv[1])
				}
				line = "{" + strings.Join(parts, ",") + "}"
			} else {
				parts := make([]string, len(keys))
				for k, kv := range keys {
					parts[k] = kv[0] + "=" + kv[1]
				}
				line = strings.Join(parts, " ")
			}
			out = append(out, jlEntry{Stream: s, TS: at.UnixNano(), Line: line, Keys: keys})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	return out
}

// jlLokiStreams are the fixture as Loki holds it: the pushed lines, their structured metadata and the detected_level
// Loki's ingest gives a line without a level (unknown).
func jlLokiStreams(entries []jlEntry) []oraclerun.EngineStream {
	out := make([]oraclerun.EngineStream, len(jlStreams))
	for i, s := range jlStreams {
		out[i].Labels = s.Labels
	}
	for _, e := range entries {
		out[e.Stream].Entries = append(out[e.Stream].Entries, oraclerun.EngineEntry{TS: e.TS, Line: e.Line, Metadata: jlLokiMetadata(e.Stream)})
	}
	return out
}

// jlLokiMetadata is the structured metadata Loki holds for a line of stream s.
func jlLokiMetadata(s int) map[string]string {
	out := map[string]string{"detected_level": "unknown"}
	for k, v := range jlStreams[s].Metadata {
		out[k] = v
	}
	return out
}

// jlVLRows are the fixture as VictoriaLogs' Loki push route stores it without disable_message_parsing: the stream
// labels, the structured metadata, then for a JSON line every key as a field and the placeholder in _msg, for another
// line the line in _msg.
func jlVLRows(entries []jlEntry, placeholder string, toVL func(string) string) []oraclerun.ServeRow {
	out := make([]oraclerun.ServeRow, 0, len(entries))
	for _, e := range entries {
		st := jlStreams[e.Stream]
		var stream, fields [][2]string
		for _, k := range sortedKeys(st.Labels) {
			stream = append(stream, [2]string{toVL(k), st.Labels[k]})
		}
		for _, k := range sortedKeys(st.Metadata) {
			fields = append(fields, [2]string{toVL(k), st.Metadata[k]})
		}
		if st.JSON {
			fields = append(fields, e.Keys...)
			fields = append(fields, [2]string{"_msg", placeholder})
		} else {
			fields = append(fields, [2]string{"_msg", e.Line})
		}
		out = append(out, oraclerun.ServeRow{TS: e.TS, Stream: stream, Fields: fields})
	}
	return out
}

// jlVariant is a placeholder VictoriaLogs writes and what the proxy is told of it.
type jlVariant struct {
	name, placeholder, configured string
}

var jlVariants = []jlVariant{
	{name: "default", placeholder: "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"},
	{name: "custom", placeholder: jlCustomPlaceholder, configured: jlCustomPlaceholder},
}

// jlMode is a proxy setting of the differential.
type jlMode struct {
	name string
	set  func(*proxy.Config)
}

func jlModes() []jlMode {
	return []jlMode{
		{name: "off", set: func(*proxy.Config) {}},
		{name: "on", set: func(c *proxy.Config) { c.JSONLineFieldsAsLineKeys = true }},
	}
}

// jlLogQueries are the log queries, over every stream of app=jl; jlMetricQueries the metric queries.
var jlLogQueries = []string{
	`{app="jl"}`,
	`{app="jl"} | logfmt`,
	`{app="jl"} | logfmt amount`,
	`{app="jl"} | status="200"`,
	`{app="jl"} | logfmt | status="200"`,
	`{app="jl"} | region!="eu"`,
	`{app="jl"} |= "status"`,
	`{app="jl", fmt="json"} | json`,
	`{app="jl", fmt="json"} | json amount`,
	`{app="jl", fmt="json"} | json | status="200"`,
	`{app="jl", fmt="json"} | json | region!="eu"`,
	`{app="jl", fmt="json"} | json | line_format "{{.status}}"`,
}

var jlMetricQueries = []string{
	`sum by (pod) (count_over_time({app="jl"} [5m]))`,
	`sum by (status) (count_over_time({app="jl"} | logfmt [5m]))`,
	`sum by (pod, status) (count_over_time({app="jl"} | logfmt status [5m]))`,
	`max_over_time({app="jl"} | logfmt amount | unwrap amount [5m])`,
	`max by (pod) (max_over_time({app="jl"} | logfmt amount | unwrap amount [5m]))`,
	`sum by (pod) (sum_over_time({app="jl"} | logfmt | unwrap amount [5m]))`,
	`max_over_time({app="jl"} | unwrap amount [5m])`,
	`sum by (pod) (count_over_time({app="jl"} | logfmt | __error__="" [5m]))`,
	`sum by (pod) (count_over_time({app="jl"} | status="200" [5m]))`,
	`sum by (pod) (count_over_time({app="jl"} | region!="eu" [5m]))`,
	`sum by (pod) (rate({app="jl"} | logfmt | status="500" [5m]))`,
	`sum by (pod) (count_over_time({app="jl", fmt="json"} | json [5m]))`,
	`sum by (status) (count_over_time({app="jl", fmt="json"} | json [5m]))`,
	`sum by (pod, status) (count_over_time({app="jl", fmt="json"} | json status [5m]))`,
	`max_over_time({app="jl", fmt="json"} | json amount | unwrap amount [5m])`,
	`sum by (pod) (sum_over_time({app="jl", fmt="json"} | json | unwrap amount [5m]))`,
	`sum by (pod) (count_over_time({app="jl", fmt="json"} | json | __error__="" [5m]))`,
	`sum by (pod) (count_over_time({app="jl", fmt="json"} | json | status="200" [5m]))`,
	`sum by (pod) (rate({app="jl", fmt="json"} | json | status="500" [5m]))`,
}

// jlSMQueries run over the stream pushed with structured metadata beside its JSON lines.
var jlSMQueries = []string{
	`{app="jlsm"}`,
	`{app="jlsm"} | json`,
	`sum by (trace_id) (count_over_time({app="jlsm"} [5m]))`,
}

// jlCustomQueries are the queries of the customised variant.
var jlCustomQueries = []string{
	`{app="jl"}`,
	`{app="jl", fmt="json"} | json`,
	`max_over_time({app="jl"} | logfmt amount | unwrap amount [5m])`,
	`sum by (status) (count_over_time({app="jl", fmt="json"} | json [5m]))`,
}

// jlQuery is one query of one variant, and its requests.
type jlQuery struct {
	q       string
	variant jlVariant
	log     bool
	reqs    []metricRequest
}

func (jq jlQuery) id() string { return jq.variant.name + " " + jq.q }

func jlLogRequest(q string) metricRequest {
	form := url.Values{
		"query":     {q},
		"start":     {strconv.FormatInt(metricEnd.Add(-metricWindow-30*time.Minute).Unix(), 10)},
		"end":       {strconv.FormatInt(metricEnd.Unix(), 10)},
		"limit":     {"5000"},
		"direction": {"backward"},
	}
	return metricRequest{kind: "log", form: form}
}

func jlQueries() []jlQuery {
	var out []jlQuery
	add := func(v jlVariant, q string) {
		jq := jlQuery{q: q, variant: v, log: !strings.Contains(q, "[5m]")}
		if jq.log {
			jq.reqs = []metricRequest{jlLogRequest(q)}
		} else {
			jq.reqs = metricRequests(q, 5*time.Minute)
		}
		out = append(out, jq)
	}
	def, custom := jlVariants[0], jlVariants[1]
	for _, q := range jlLogQueries {
		add(def, q)
	}
	for _, q := range jlMetricQueries {
		add(def, q)
	}
	for _, q := range jlSMQueries {
		add(def, q)
	}
	for _, q := range jlCustomQueries {
		add(custom, q)
	}
	return out
}

// jlRecord is a query's recorded answers: Loki's per request kind (for a log query, the expected entries as JSON),
// VictoriaLogs' per version by the hash of the request key, and the proxy's answers with the flag off, per version
// and request kind.
type jlRecord struct {
	ID     string                               `json:"id"`
	Inputs string                               `json:"inputs"`
	Loki   map[string]metricLokiAnswer          `json:"loki"`
	VL     map[string]map[string]metricVLAnswer `json:"vl"`
	Off    map[string]map[string]string         `json:"off,omitempty"`
}

func (jq jlQuery) inputsHash(entries []jlEntry) string {
	b, _ := json.Marshal(struct {
		S []jlStream
		E []jlEntry
		P string
	}{jlStreams, entries, jq.variant.placeholder})
	var s strings.Builder
	s.Write(b)
	for _, r := range jq.reqs {
		s.WriteString("\x00" + r.kind + "\x00" + r.form.Encode())
	}
	return shortHash(s.String())
}

func readJLAnswers(t *testing.T) map[string]*jlRecord {
	t.Helper()
	f, err := os.Open(jsonLineAnswersPath)
	if err != nil {
		t.Fatalf("%s: %v: LVP_RUN_ORACLES=1 records it", jsonLineAnswersPath, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*jlRecord{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var r jlRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		out[r.ID] = &r
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeJLAnswers(t *testing.T, recs []*jlRecord) {
	t.Helper()
	f, err := os.Create(jsonLineAnswersPath)
	if err != nil {
		t.Fatal(err)
	}
	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	zw.ModTime = time.Time{}
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// jlEntryAnswer is one entry of a log answer: its line, and its labels by Loki's categories (detected_level, which
// Loki derives at ingest and the proxy on the read path, left out).
type jlEntryAnswer struct {
	Line     string            `json:"line"`
	Stream   map[string]string `json:"stream,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Parsed   map[string]string `json:"parsed,omitempty"`
}

// jlLokiLogAnswer is Loki's answer to a log query over the fixture, by entry time: what Loki's pipeline makes of
// each line of every stream the selector selects.
func jlLokiLogAnswer(t *testing.T, q string, entries []jlEntry) metricLokiAnswer {
	t.Helper()
	cases := make([]oraclerun.ProcessCase, len(entries))
	for i, e := range entries {
		cases[i] = oraclerun.ProcessCase{ID: strconv.Itoa(i), Query: q, Labels: jlStreams[e.Stream].Labels, Line: e.Line,
			Metadata: jlLokiMetadata(e.Stream), WithCategories: true}
	}
	answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), cases)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]jlEntryAnswer{}
	for i, a := range answers {
		if a.Error != "" {
			return metricLokiAnswer{Status: 400, Error: a.Error}
		}
		if !a.Selected || !a.Keep {
			continue
		}
		out[strconv.FormatInt(entries[i].TS, 10)] = jlEntryAnswer{Line: a.Line, Stream: a.Stream, Metadata: jlWithoutLevel(a.StructuredMetadata), Parsed: jlWithoutLevel(a.Parsed)}
	}
	body, _ := json.Marshal(out)
	return metricLokiAnswer{Status: 200, Body: body}
}

func jlWithoutLevel(m map[string]string) map[string]string {
	delete(m, "detected_level")
	if len(m) == 0 {
		return nil
	}
	return m
}

// decodeJLLog reads a categorized log answer by entry time.
func decodeJLLog(body []byte) (map[string]jlEntryAnswer, error) {
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if resp.Data.ResultType != "streams" {
		return nil, fmt.Errorf("result type %q", resp.Data.ResultType)
	}
	out := map[string]jlEntryAnswer{}
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			if len(v) < 2 {
				return nil, fmt.Errorf("value of %d elements", len(v))
			}
			var ts string
			e := jlEntryAnswer{Stream: s.Stream}
			if err := json.Unmarshal(v[0], &ts); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(v[1], &e.Line); err != nil {
				return nil, err
			}
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				if err := json.Unmarshal(v[2], &meta); err != nil {
					return nil, err
				}
				e.Metadata, e.Parsed = jlWithoutLevel(meta.StructuredMetadata), jlWithoutLevel(meta.Parsed)
			}
			if _, dup := out[ts]; dup {
				return nil, fmt.Errorf("entry %s twice", ts)
			}
			out[ts] = e
		}
	}
	return out, nil
}

// compareJLLog holds a log answer to Loki's: the same entries, each with the same line and labels by category.
func compareJLLog(loki metricLokiAnswer, run metricHandlerRun) metricDiff {
	d := metricDiff{classes: map[string]string{}}
	if loki.Status != run.status {
		d.add(fmt.Sprintf("answers status %d where Loki answers %d", run.status, loki.Status), fmt.Sprintf("proxy %.200q, Loki %.200q", proxyErrorText(run.body), loki.Error))
		return d
	}
	if loki.Status != 200 {
		return d
	}
	var want map[string]jlEntryAnswer
	if err := json.Unmarshal(loki.Body, &want); err != nil {
		d.add("Loki's answer does not decode", err.Error())
		return d
	}
	got, err := decodeJLLog([]byte(run.body))
	if err != nil {
		d.add("the answer does not decode", err.Error())
		return d
	}
	for ts, w := range want {
		g, ok := got[ts]
		if !ok {
			d.add("misses an entry Loki returns", ts+" "+w.Line)
			continue
		}
		if g.Line != w.Line {
			d.add("returns another line", fmt.Sprintf("%q, Loki %q", g.Line, w.Line))
		}
		for _, c := range []struct {
			name      string
			got, want map[string]string
		}{{"stream labels", g.Stream, w.Stream}, {"structured metadata", g.Metadata, w.Metadata}, {"parsed labels", g.Parsed, w.Parsed}} {
			if labelsKey(c.got) != labelsKey(c.want) {
				d.add("has other "+c.name, fmt.Sprintf("%s: %v, Loki %v", w.Line, c.got, c.want))
			}
		}
	}
	for ts, g := range got {
		if _, ok := want[ts]; !ok {
			d.add("returns an entry Loki drops", ts+" "+g.Line)
		}
	}
	return d
}

// runJLHandler serves a request from a fresh proxy of the profile, the variant and the mode (every cache cold) in
// front of front.
func runJLHandler(t *testing.T, prof diffProfile, v jlVariant, mode jlMode, mr metricRequest, front *metricFront) metricHandlerRun {
	t.Helper()
	srv := httptest.NewServer(front)
	defer srv.Close()
	cfg := proxy.Config{
		BackendURL:             srv.URL,
		Cache:                  cache.New(time.Minute, 1000),
		LogLevel:               "error",
		LabelStyle:             prof.style,
		MetadataFieldMode:      prof.mode,
		EmitStructuredMetadata: true,
		BackendDefaultMsgValue: v.configured,
	}
	mode.set(&cfg)
	p, err := proxy.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	path := "/loki/api/v1/query_range"
	if mr.instant {
		path = "/loki/api/v1/query"
	}
	req := httptest.NewRequest(http.MethodGet, path+"?"+mr.form.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	if mr.kind == "log" {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	rec := httptest.NewRecorder()
	front.mu.Lock()
	front.sent, front.missing = nil, nil
	front.mu.Unlock()
	mux.ServeHTTP(rec, req)
	front.mu.Lock()
	defer front.mu.Unlock()
	return metricHandlerRun{status: rec.Code, body: rec.Body.String(), sent: uniqueSorted(front.sent), missing: uniqueSorted(front.missing)}
}

// canonicalJLBody is an answer with its JSON objects' keys sorted, its result's series or streams sorted and its
// statistics left out: the proxy writes label maps and series in map order and times its own work, which differs
// from run to run; every other value is kept.
func canonicalJLBody(body string) string {
	var v map[string]any
	if json.Unmarshal([]byte(body), &v) != nil {
		return body
	}
	if data, ok := v["data"].(map[string]any); ok {
		delete(data, "stats")
		if result, ok := data["result"].([]any); ok {
			keys := make([]string, len(result))
			for i, r := range result {
				b, _ := json.Marshal(r)
				keys[i] = string(b)
			}
			sort.Strings(keys)
			for i := range result {
				result[i] = json.RawMessage(keys[i])
			}
		}
	}
	out, _ := json.Marshal(v)
	return string(out)
}

// jlErrorSeries is the series a pipeline error names: the first errored series the proxy meets, in map order.
var jlErrorSeries = regexp.MustCompile(`for series: '\{.*?\}'\.`)

// recordJLAnswers runs the oracles: Loki over every request, and every request the handler sends under every
// profile and mode, through VictoriaLogs' query server of each version. The pinned flag-off answers are kept unless
// LVP_JSONLINE_PIN_OFF=1 records them again.
func recordJLAnswers(t *testing.T, queries []jlQuery, entries []jlEntry) {
	t.Helper()
	root := filepath.Join("..", "..")
	var prior map[string]*jlRecord
	if os.Getenv("LVP_JSONLINE_PIN_OFF") != "1" {
		prior = readJLAnswers(t)
	}
	recs := make([]*jlRecord, len(queries))
	var sets []oraclerun.EngineDataSet
	setOf := map[int]int{}
	for i, jq := range queries {
		recs[i] = &jlRecord{ID: jq.id(), Inputs: jq.inputsHash(entries), Loki: map[string]metricLokiAnswer{}, VL: map[string]map[string]metricVLAnswer{}, Off: map[string]map[string]string{}}
		if jq.log {
			recs[i].Loki["log"] = jlLokiLogAnswer(t, jq.q, entries)
			continue
		}
		set := oraclerun.EngineDataSet{Streams: jlLokiStreams(entries)}
		for _, r := range jq.reqs {
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: strconv.Itoa(i) + "/" + r.kind, Instant: r.instant, Form: r.form.Encode()})
		}
		setOf[i] = len(sets)
		sets = append(sets, set)
	}
	loki, err := oraclerun.LokiEngine(filepath.Join(root, "test", "oracle", "loki"), sets)
	if err != nil {
		t.Fatal(err)
	}
	for i, jq := range queries {
		if jq.log {
			continue
		}
		for _, r := range jq.reqs {
			a := loki[strconv.Itoa(i)+"/"+r.kind]
			recs[i].Loki[r.kind] = metricLokiAnswer{Status: a.Status, Body: a.Body, Error: a.Error}
		}
	}
	for _, prof := range diffProfiles() {
		data := make([]oraclerun.ServeDataSet, len(queries))
		for i, jq := range queries {
			data[i] = oraclerun.ServeDataSet{Tenant: diffTenant(jq.id(), "jsonline"), Rows: jlVLRows(entries, jq.variant.placeholder, prof.toVL())}
		}
		srv, err := oraclerun.StartVLServe(filepath.Join(root, "test", "oracle", "vl"), prof.modfile, data)
		if err != nil {
			t.Fatal(err)
		}
		for i, jq := range queries {
			front := &metricFront{version: prof.version, upstream: srv.URL, tenant: diffTenant(jq.id(), "jsonline"), recorded: map[string]metricVLAnswer{}}
			recs[i].Off[prof.version] = map[string]string{}
			for _, mode := range jlModes() {
				for _, r := range jq.reqs {
					run := runJLHandler(t, prof, jq.variant, mode, r, front)
					runJLHandler(t, prof, jq.variant, mode, r, front)
					if mode.name == "off" {
						recs[i].Off[prof.version][r.kind] = canonicalJLBody(run.body)
					}
				}
			}
			recs[i].VL[prof.version] = front.recorded
			if pace, err := time.ParseDuration(os.Getenv("LVP_RECORD_PACE")); err == nil && pace > 0 {
				time.Sleep(pace)
			}
		}
		srv.Stop()
	}
	for i := range recs {
		if p := prior[recs[i].ID]; p != nil && p.Inputs == recs[i].Inputs {
			recs[i].Off = p.Off
		}
	}
	writeJLAnswers(t, recs)
	t.Logf("recorded %s: %d queries", jsonLineAnswersPath, len(recs))
}

// TestJSONLineFieldsMatchLoki replays the recorded answers through the handlers: with the flag off every answer is
// the pinned one byte for byte; with the flag on every query over lines pushed without structured metadata answers
// as Loki does.
//
// conformance: profiles/json-line-stored-without-line
func TestJSONLineFieldsMatchLoki(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" && os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("runs every query on a fresh proxy per request; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	entries := jlFixture()
	queries := jlQueries()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordJLAnswers(t, queries, entries)
	}
	answers := readJLAnswers(t)
	report := os.Getenv("LVP_JSONLINE_REPORT") == "1"
	matched, runs := map[string]int{}, map[string]int{}
	seenKnown := map[string]bool{}
	for _, jq := range queries {
		rec := answers[jq.id()]
		if rec == nil || rec.Inputs != jq.inputsHash(entries) {
			t.Fatalf("the recorded answers of %s are stale: record them again with LVP_RUN_ORACLES=1", jq.id())
		}
		for _, prof := range diffProfiles() {
			for _, mode := range jlModes() {
				for _, mr := range jq.reqs {
					run := runJLHandler(t, prof, jq.variant, mode, mr, &metricFront{version: prof.version, answers: rec.VL[prof.version]})
					if missing := answerRequests(run.missing); len(missing) > 0 {
						t.Fatalf("%s %s %s %s: VictoriaLogs request %s was not recorded: record them again with LVP_RUN_ORACLES=1", prof.name, mode.name, mr.kind, jq.id(), missing[0])
					}
					if mode.name == "off" {
						if want, ok := rec.Off[prof.version][mr.kind]; !ok {
							t.Errorf("%s %s %s: no pinned flag-off answer", prof.name, mr.kind, jq.id())
						} else if jlErrorSeries.ReplaceAllString(canonicalJLBody(run.body), "for series: <series>.") != jlErrorSeries.ReplaceAllString(want, "for series: <series>.") {
							t.Errorf("%s off %s %s: the answer with the flag off changed:\n got %s\nwant %s", prof.name, mr.kind, jq.id(), canonicalJLBody(run.body), want)
						}
					}
					if os.Getenv("LVP_JSONLINE_SENT") == "1" && prof.name == diffProfiles()[0].name && (mr.kind == "instant" || mr.kind == "log" || os.Getenv("LVP_JSONLINE_SENT_ALL") == "1") {
						for _, k := range run.sent {
							u, _ := url.ParseQuery(k[strings.Index(k, "?")+1:])
							t.Logf("SENT %s %s | %s | %s extra=%v", mode.name, jq.id(), k[:strings.Index(k, "?")], u.Get("query"), u["extra_filters"])
						}
					}
					var d metricDiff
					if jq.log {
						d = compareJLLog(rec.Loki[mr.kind], run)
					} else {
						d = compareMetric(rec.Loki[mr.kind], run)
					}
					key := mode.name
					runs[key]++
					known := ""
					if mode.name == "on" {
						known = jlKnownOn[jq.q]
						seenKnown[jq.q] = true
					}
					if len(d.classes) == 0 {
						matched[key]++
						if known != "" && !report {
							t.Errorf("%s on %s %s matches Loki: remove it from jlKnownOn (%s)", prof.name, mr.kind, jq.id(), known)
						}
						continue
					}
					if mode.name == "on" && known == "" && !report {
						for c, ex := range d.classes {
							t.Errorf("%s on %s %s: %s: %s", prof.name, mr.kind, jq.id(), c, ex)
						}
					}
					if report && prof.name == diffProfiles()[0].name {
						var cs []string
						for c, ex := range d.classes {
							cs = append(cs, c+": "+ex)
						}
						sort.Strings(cs)
						t.Logf("DIFF %s %s | %s | %s", mode.name, mr.kind, jq.id(), strings.Join(cs, "; "))
					}
				}
			}
		}
	}
	checkLogfmtUnwrapStreams(t, answers[jlVariants[0].name+" "+jlLogfmtUnwrap])
	for q := range jlKnownOn {
		if !seenKnown[q] {
			t.Errorf("jlKnownOn lists %s, which the differential does not run", q)
		}
	}
	for _, m := range jlModes() {
		t.Logf("mode %s: %d of %d runs match Loki", m.name, matched[m.name], runs[m.name])
	}
	if !report && matched["on"] < 176 {
		t.Errorf("with the flag on only %d runs match Loki, at least 176 did", matched["on"])
	}
}

// jlLogfmtUnwrap is the query whose Loki answer holds only the logfmt streams: Loki's logfmt parser reads no amount
// from a JSON line, so no sample of a JSON line is unwrapped.
const jlLogfmtUnwrap = `max_over_time({app="jl"} | logfmt amount | unwrap amount [5m])`

// checkLogfmtUnwrapStreams holds jlLogfmtUnwrap's recorded Loki answers to their expected shape: series, all of the
// logfmt streams (the handler's answers match these with the flag on).
func checkLogfmtUnwrapStreams(t *testing.T, rec *jlRecord) {
	t.Helper()
	if rec == nil {
		t.Fatalf("no answers recorded for %s", jlLogfmtUnwrap)
	}
	for kind, a := range rec.Loki {
		res, err := decodeMetricBody(a.Body)
		if err != nil || len(res.series) == 0 {
			t.Fatalf("%s %s: Loki answers no series (%v)", kind, jlLogfmtUnwrap, err)
		}
		for _, s := range res.series {
			if s.labels["fmt"] != "logfmt" {
				t.Errorf("%s %s: Loki answers the series %v, which is no logfmt stream", kind, jlLogfmtUnwrap, s.labels)
			}
		}
	}
}

// jlKnownOn are the queries that differ from Loki with the flag on, every run, with what they read that the flag does
// not change.
var jlKnownOn = map[string]string{
	// A label filter an empty value passes, before any parser: Loki keeps every placeholder line (it has no label of
	// the line's keys), VictoriaLogs' filter reads the stored field.
	`{app="jl"} | region!="eu"`:                                      "label filter an empty value passes, before a parser",
	`sum by (pod) (count_over_time({app="jl"} | region!="eu" [5m]))`: "label filter an empty value passes, before a parser",
	// A line filter reads the stored _msg, the placeholder.
	`{app="jl"} |= "status"`: "line filter",
	// | logfmt groups a placeholder line under empty values in Loki; VictoriaLogs groups it by its stored fields.
	`sum by (status) (count_over_time({app="jl"} | logfmt [5m]))`:             "logfmt grouping keys",
	`sum by (pod, status) (count_over_time({app="jl"} | logfmt status [5m]))`: "logfmt grouping keys",
	// Structured metadata pushed beside such a line is stored among its keys: the flag hides it (documented).
	`{app="jlsm"}`:        "structured metadata beside the line",
	`{app="jlsm"} | json`: "structured metadata beside the line",
}
