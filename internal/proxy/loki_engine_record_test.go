package proxy

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// Recorded Loki engine answers for the differential tests.
//
// A differential test generates data and requests (seeded, so every run generates the same ones), asks the proxy,
// and compares with what Loki answers for the same data and request. Loki's answer comes from the Loki engine
// oracle (test/oracle/loki engine, Loki's own query engine and HTTP encoder over an in-memory store), run with
// LVP_RUN_ORACLES=1 and recorded in a gzipped JSON lines file under testdata/loki_engine. A default run reads the
// file. Every recorded answer carries a digest of its data set and request: a test whose data or request changed
// finds no answer and says to record again, so a recording can never stand for other data.

// lokiEngineAnswer is one recorded answer.
type lokiEngineAnswer struct {
	ID     string          `json:"id"`
	Key    string          `json:"key"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func lokiEngineDir() string { return filepath.Join("testdata", "loki_engine") }

// lokiEngineKey is the digest of a request and the data set it runs over.
func lokiEngineKey(set oraclerun.EngineDataSet, req oraclerun.EngineRequest) string {
	h := sha256.New()
	data, _ := json.Marshal(set.Streams)
	h.Write(data)
	fmt.Fprintf(h, "\x00%d\x00%v\x00%v\x00%s", set.MaxSeries, req.Instant, req.Unaligned, req.Form)
	return hex.EncodeToString(h.Sum(nil)[:12])
}

// lokiEngineAnswers returns Loki's answer to every request of sets, by request id (ids are unique over sets),
// read from testdata/loki_engine/<name>.jsonl.gz; with LVP_RUN_ORACLES=1 it runs the engine oracle and writes the
// file first.
func lokiEngineAnswers(t testing.TB, name string, sets []oraclerun.EngineDataSet) map[string]lokiEngineAnswer {
	t.Helper()
	path := filepath.Join(lokiEngineDir(), name+".jsonl.gz")
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordLokiEngineAnswers(t, path, sets)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("%v: record Loki's answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]lokiEngineAnswer{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		var a lokiEngineAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		byKey[a.ID+"\x00"+a.Key] = a
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	out := map[string]lokiEngineAnswer{}
	for _, set := range sets {
		for _, req := range set.Requests {
			a, ok := byKey[req.ID+"\x00"+lokiEngineKey(set, req)]
			if !ok {
				t.Fatalf("%s: no recorded Loki answer for request %s (%s): record again with LVP_RUN_ORACLES=1", path, req.ID, req.Form)
			}
			out[req.ID] = a
		}
	}
	return out
}

func recordLokiEngineAnswers(t testing.TB, path string, sets []oraclerun.EngineDataSet) {
	t.Helper()
	answers, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), sets)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	// A fixed header keeps the file's bytes a function of its content.
	zw.ModTime, zw.Name = zw.ModTime.UTC(), ""
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	for _, set := range sets {
		for _, req := range set.Requests {
			a, ok := answers[req.ID]
			if !ok {
				t.Fatalf("the engine oracle gave no answer for %s", req.ID)
			}
			if err := enc.Encode(lokiEngineAnswer{ID: req.ID, Key: lokiEngineKey(set, req), Status: a.Status, Body: a.Body, Error: a.Error}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// lokiSeries is a decoded query or query_range answer: per series (its canonical label text) its labels and its
// points, by time in milliseconds, as the value text Loki wrote.
type lokiSeries struct {
	ResultType string
	Labels     map[string]map[string]string
	Points     map[string]map[int64]string
	Scalar     *[2]any
}

// decodeLokiSeries reads a query or query_range answer body (Loki's or the proxy's).
func decodeLokiSeries(body []byte) (lokiSeries, error) {
	var r struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return lokiSeries{}, err
	}
	out := lokiSeries{ResultType: r.Data.ResultType, Labels: map[string]map[string]string{}, Points: map[string]map[int64]string{}}
	if r.Data.ResultType == "scalar" {
		var s [2]any
		if err := json.Unmarshal(r.Data.Result, &s); err != nil {
			return out, err
		}
		out.Scalar = &s
		return out, nil
	}
	var series []struct {
		Metric map[string]string `json:"metric"`
		Value  []any             `json:"value"`
		Values [][]any           `json:"values"`
	}
	if err := json.Unmarshal(r.Data.Result, &series); err != nil {
		return out, err
	}
	for _, s := range series {
		key := canonicalLabelText(s.Metric)
		if _, dup := out.Points[key]; dup {
			return out, fmt.Errorf("series %s twice", key)
		}
		out.Labels[key] = s.Metric
		points := map[int64]string{}
		values := s.Values
		if s.Value != nil {
			values = [][]any{s.Value}
		}
		for _, p := range values {
			if len(p) != 2 {
				return out, fmt.Errorf("point %v", p)
			}
			ts, ok := p[0].(float64)
			v, ok2 := p[1].(string)
			if !ok || !ok2 {
				return out, fmt.Errorf("point %v", p)
			}
			points[int64(math.Round(ts*1000))] = v
		}
		out.Points[key] = points
	}
	return out, nil
}

// canonicalLabelText is a label set as {a="x", b="y"}, names sorted, values quoted.
func canonicalLabelText(m map[string]string) string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(m[k]))
	}
	b.WriteByte('}')
	return b.String()
}

// engineStreams groups emulator rows into the Loki streams the engine oracle reads: each row's stream labels, and
// its line and structured metadata as entry gives them.
func engineStreams(rows []vlemu.Row, entry func(vlemu.Row) (string, map[string]string)) []oraclerun.EngineStream {
	var out []oraclerun.EngineStream
	index := map[string]int{}
	for _, r := range rows {
		i, ok := index[r.Stream]
		if !ok {
			i = len(out)
			index[r.Stream] = i
			out = append(out, oraclerun.EngineStream{Labels: vlemu.StreamLabels(r.Stream)})
		}
		line, metadata := entry(r)
		out[i].Entries = append(out[i].Entries, oraclerun.EngineEntry{TS: r.TS, Line: line, Metadata: metadata})
	}
	return out
}

// unwrapRowEntry writes an emulator row as a Loki line for a query that unwraps field after parser: with no parser
// the line is the emulator's and the field is structured metadata (a VictoriaLogs field that is no stream label);
// with logfmt or json the line holds the field and the labels the row's parser extracts.
func unwrapRowEntry(parser, field string, r vlemu.Row) (string, map[string]string) {
	names := make([]string, 0, len(r.Labels))
	for k := range r.Labels {
		names = append(names, k)
	}
	sort.Strings(names)
	switch parser {
	case "logfmt":
		parts := []string{field + "=" + r.Text()}
		for _, k := range names {
			parts = append(parts, k+"="+strconv.Quote(r.Labels[k]))
		}
		return strings.Join(parts, " "), nil
	case "json":
		obj := map[string]string{field: r.Text()}
		for _, k := range names {
			obj[k] = r.Labels[k]
		}
		b, _ := json.Marshal(obj)
		return string(b), nil
	}
	line := r.Msg
	if line == "" {
		line = field + "=" + r.Text()
	}
	return line, map[string]string{field: r.Text()}
}

// lokiMetricRequest is a request for query over [start, end] (unix ns) every step, at Loki's own bounds; step 0 is
// an instant query at end.
func lokiMetricRequest(id, query string, start, end int64, step time.Duration) oraclerun.EngineRequest {
	if step == 0 {
		return oraclerun.EngineRequest{ID: id, Instant: true, Form: url.Values{"query": {query}, "time": {strconv.FormatInt(end, 10)}}.Encode()}
	}
	return oraclerun.EngineRequest{ID: id, Unaligned: true, Form: url.Values{
		"query": {query}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)},
		"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}.Encode()}
}

// lokiMetricPoints reads a recorded answer as series (label text) -> time in ns -> value; a non-200 answer fails
// the test.
func lokiMetricPoints(t testing.TB, label string, a lokiEngineAnswer) (map[string]map[int64]float64, map[string]map[string]string) {
	t.Helper()
	if a.Status != 200 {
		t.Fatalf("%s: Loki answered %d %s", label, a.Status, a.Error)
	}
	s, err := decodeLokiSeries(a.Body)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	out := map[string]map[int64]float64{}
	for key, points := range s.Points {
		out[key] = map[int64]float64{}
		for ms, text := range points {
			v, err := strconv.ParseFloat(text, 64)
			if err != nil {
				t.Fatalf("%s: value %q", label, text)
			}
			out[key][ms*int64(time.Millisecond)] = v
		}
	}
	return out, s.Labels
}

// sameSampleText reports whether two value texts are the same number (NaN equals NaN, and -0 equals 0 only when
// both are written alike).
func sameSampleText(a, b string) bool {
	if a == b {
		return true
	}
	x, err1 := strconv.ParseFloat(a, 64)
	y, err2 := strconv.ParseFloat(b, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	if math.IsNaN(x) || math.IsNaN(y) {
		return math.IsNaN(x) && math.IsNaN(y)
	}
	return x == y && math.Signbit(x) == math.Signbit(y)
}

// lokiProcessRecord is one recorded pipeline answer (test/oracle/loki process) with the digest of its case.
type lokiProcessRecord struct {
	Key    string                  `json:"key"`
	Answer oraclerun.ProcessAnswer `json:"answer"`
}

func lokiProcessKey(c oraclerun.ProcessCase) string {
	c.ID = ""
	data, _ := json.Marshal(c)
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:12])
}

// lokiProcessAnswers returns what Loki's pipeline makes of each case's line (test/oracle/loki process), in the order
// of cases, read from testdata/loki_engine/<name>.jsonl.gz; with LVP_RUN_ORACLES=1 it runs the oracle and writes the
// file first. A case whose query, labels or line changed finds no answer and says to record again.
func lokiProcessAnswers(t testing.TB, name string, cases []oraclerun.ProcessCase) []oraclerun.ProcessAnswer {
	t.Helper()
	path := filepath.Join(lokiEngineDir(), name+".jsonl.gz")
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		answers, err := oraclerun.LokiProcess(filepath.Join("..", "..", "test", "oracle", "loki"), cases)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
		enc := json.NewEncoder(zw)
		enc.SetEscapeHTML(false)
		for i, c := range cases {
			if err := enc.Encode(lokiProcessRecord{Key: lokiProcessKey(c), Answer: answers[i]}); err != nil {
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
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("%v: record Loki's answers with LVP_RUN_ORACLES=1", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]oraclerun.ProcessAnswer{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r lokiProcessRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		byKey[r.Key] = r.Answer
	}
	out := make([]oraclerun.ProcessAnswer, len(cases))
	for i, c := range cases {
		a, ok := byKey[lokiProcessKey(c)]
		if !ok {
			t.Fatalf("%s: no recorded Loki answer for %q over %q: record again with LVP_RUN_ORACLES=1", path, c.Query, c.Line)
		}
		out[i] = a
	}
	return out
}

// Inline recordings: a test that builds its requests one at a time asks for each answer where it needs it
// (lokiEngineAnswerOf). In a recording run (LVP_RUN_ORACLES=1) the engine oracle answers at once and the file is
// written when the top-level test ends; otherwise the file is read once.
var (
	lokiInlineMu      sync.Mutex
	lokiInlineEngine  string
	lokiInlineRecords = map[string][]lokiEngineAnswer{}
	lokiInlineFiles   = map[string]map[string]lokiEngineAnswer{}
)

// lokiEngineAnswerOf is Loki's answer to the one request of set, from testdata/loki_engine/<name>.jsonl.gz.
func lokiEngineAnswerOf(t testing.TB, name string, set oraclerun.EngineDataSet) lokiEngineAnswer {
	t.Helper()
	if len(set.Requests) != 1 {
		t.Fatalf("lokiEngineAnswerOf takes one request, got %d", len(set.Requests))
	}
	req := set.Requests[0]
	key := lokiEngineKey(set, req)
	path := filepath.Join(lokiEngineDir(), name+".jsonl.gz")
	lokiInlineMu.Lock()
	defer lokiInlineMu.Unlock()
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		if lokiInlineEngine == "" {
			dir, err := os.MkdirTemp("", "loki-engine-inline")
			if err != nil {
				t.Fatal(err)
			}
			bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "loki", "engine"), "", dir, "engine")
			if err != nil {
				t.Fatal(err)
			}
			lokiInlineEngine = bin
		}
		a := runInlineEngine(t, set)
		a.ID, a.Key = req.ID, key
		if _, started := lokiInlineRecords[name]; !started {
			root := t
			if tt, ok := t.(*testing.T); ok {
				root = tt
			}
			root.Cleanup(func() { writeInlineRecords(t, name, path) })
		}
		lokiInlineRecords[name] = append(lokiInlineRecords[name], a)
		return a
	}
	file, ok := lokiInlineFiles[name]
	if !ok {
		file = map[string]lokiEngineAnswer{}
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("%v: record Loki's answers with LVP_RUN_ORACLES=1", err)
		}
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(zr)
		sc.Buffer(make([]byte, 1<<20), 1<<28)
		for sc.Scan() {
			var a lokiEngineAnswer
			if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
				t.Fatal(err)
			}
			file[a.Key] = a
		}
		_ = f.Close()
		lokiInlineFiles[name] = file
	}
	a, ok := file[key]
	if !ok {
		t.Fatalf("%s: no recorded Loki answer for %s: record again with LVP_RUN_ORACLES=1", path, req.Form)
	}
	return a
}

func runInlineEngine(t testing.TB, set oraclerun.EngineDataSet) lokiEngineAnswer {
	t.Helper()
	dir, err := os.MkdirTemp("", "loki-engine-run")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	in, out := filepath.Join(dir, "in.jsonl"), filepath.Join(dir, "out.jsonl")
	data, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(in, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := oraclerun.Run(lokiInlineEngine, "-in", in, "-out", out); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var a lokiEngineAnswer
	if err := json.Unmarshal(body, &a); err != nil {
		t.Fatal(err)
	}
	return a
}

func writeInlineRecords(t testing.TB, name, path string) {
	lokiInlineMu.Lock()
	defer lokiInlineMu.Unlock()
	records := lokiInlineRecords[name]
	sort.SliceStable(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Error(err)
		return
	}
	f, err := os.Create(path)
	if err != nil {
		t.Error(err)
		return
	}
	zw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	enc := json.NewEncoder(zw)
	enc.SetEscapeHTML(false)
	seen := map[string]bool{}
	for _, a := range records {
		if seen[a.Key] {
			continue
		}
		seen[a.Key] = true
		if err := enc.Encode(a); err != nil {
			t.Error(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Error(err)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

// pushdownEngineStreams are pushdown rows as Loki streams: each row's stream labels, line and the structured
// metadata Loki holds for it (pushdownRow.loki). A stream with no service_name gets the one Loki's ingest
// discovers from its app label, as a Loki with its default discover_service_name stores it.
func pushdownEngineStreams(rows []pushdownRow) []oraclerun.EngineStream {
	var out []oraclerun.EngineStream
	index := map[string]int{}
	for _, r := range rows {
		key := canonicalLabelText(r.stream)
		i, ok := index[key]
		if !ok {
			i = len(out)
			index[key] = i
			labels := map[string]string{}
			for k, v := range r.stream {
				labels[k] = v
			}
			if _, has := labels["service_name"]; !has && labels["app"] != "" {
				labels["service_name"] = labels["app"]
			}
			out = append(out, oraclerun.EngineStream{Labels: labels})
		}
		out[i].Entries = append(out[i].Entries, oraclerun.EngineEntry{TS: r.ts.UnixNano(), Line: r.msg, Metadata: r.loki})
	}
	return out
}

// lokiPushdownMatrix is Loki's query_range answer for query over rows (unaligned), as decodeExtractedMatrix reads
// a matrix.
func lokiPushdownMatrix(t *testing.T, name string, rows []pushdownRow, query string, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows), Requests: []oraclerun.EngineRequest{lokiMetricRequest("q", query, start.UnixNano(), end.UnixNano(), step)}}
	a := lokiEngineAnswerOf(t, name, set)
	if a.Status != 200 {
		t.Fatalf("%s: Loki answered %d %s", query, a.Status, a.Error)
	}
	return decodeExtractedMatrix(t, a.Body)
}
