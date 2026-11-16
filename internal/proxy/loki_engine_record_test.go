package proxy

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

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

// engineStreams groups emulator rows into the Loki streams the engine oracle reads: each row's stream labels and
// its line as line gives it (the emulator's own line when line is nil).
func engineStreams(rows []vlemu.Row, line func(vlemu.Row) string) []oraclerun.EngineStream {
	var out []oraclerun.EngineStream
	index := map[string]int{}
	for _, r := range rows {
		i, ok := index[r.Stream]
		if !ok {
			i = len(out)
			index[r.Stream] = i
			out = append(out, oraclerun.EngineStream{Labels: vlemu.StreamLabels(r.Stream)})
		}
		out[i].Entries = append(out[i].Entries, oraclerun.EngineEntry{TS: r.TS, Line: line(r)})
	}
	return out
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
