package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// The rewrite of a range aggregation whose pipeline ends in a bare | keep:
// the shapes it groups by the kept labels and the ones it leaves alone.
// conformance: semantics/keep-leaves-labels-loki-drops
func TestKeepSeriesGrouping_Rewrite(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// The case of the 2026-11-04 run: one series, count 1.
		{`count(rate({service_name="api-gateway"} | json | keep foo [5m]))`,
			`count (sum by (foo) (rate({service_name="api-gateway"} | json | keep foo[5m])))`},
		{`rate({app="x"} | keep namespace [5m])`, `sum by (namespace) (rate({app="x"} | keep namespace[5m]))`},
		{`count_over_time({app="x"} | logfmt | keep level, msg [1m])`, `sum by (level, msg) (count_over_time({app="x"} | logfmt | keep level, msg[1m]))`},
		{`bytes_over_time({app="x"} | keep a [1m])`, `sum by (a) (bytes_over_time({app="x"} | keep a[1m]))`},
		{`bytes_rate({app="x"} | keep a [1m])`, `sum by (a) (bytes_rate({app="x"} | keep a[1m]))`},
		// Stages after the keep that leave the labels alone.
		{`rate({app="x"} | json | keep a |= "x" | a="1" | line_format "{{.a}}" | decolorize [1m])`,
			`sum by (a) (rate({app="x"} | json | keep a |= "x" | a="1" | line_format "{{.a}}" | decolorize[1m]))`},
		// The last keep decides; an earlier one already removed its labels in the pipeline.
		{`rate({app="x"} | keep a, b | keep b [1m])`, `sum by (b) (rate({app="x"} | keep a, b | keep b[1m]))`},
		// Both sides of a binary operation.
		{`rate({app="x"} | keep a [1m]) / rate({app="y"} | keep a [1m])`,
			`sum by (a) (rate({app="x"} | keep a[1m])) / sum by (a) (rate({app="y"} | keep a[1m]))`},
	} {
		got, changed := keepSeriesGrouping(tc.in)
		if !changed || got != tc.want {
			t.Errorf("%s\n got: %s (changed=%v)\nwant: %s", tc.in, got, changed, tc.want)
		}
		// A child request of a rewritten query is not rewritten again.
		if again, changed := keepSeriesGrouping(got); changed {
			t.Errorf("%s: rewritten twice: %s", tc.in, again)
		}
	}
	for _, q := range []string{
		`{app="x"} | json | keep level`,                                 // a log query
		`rate({app="x"} | json [1m])`,                                   // no keep
		`rate({app="x"} | keep level="error" [1m])`,                     // a conditional keep
		`rate({app="x"} | keep a | json [1m])`,                          // a parser after the keep adds labels
		`rate({app="x"} | keep a | label_format b="c" [1m])`,            // label_format adds one
		`rate({app="x"} | keep a | drop a [1m])`,                        // drop removes one
		`max_over_time({app="x"} | logfmt | keep a, n | unwrap n [1m])`, // an unwrap: its series also drop the unwrapped label
		`rate({app="x"} | logfmt | keep a, n | unwrap n [1m])`,
		`absent_over_time({app="x"} | keep a [1m])`, // labels from the selector
		// An error label the query reads: keep never removes it, so a
		// grouping by the kept labels would fold the markers into one series.
		`sum by(__error__)(rate({app="x"} | json | keep __error__ [5m]))`,
		`rate({app="x"} | json | keep level, __error__, __error_details__ [1m])`,
		`rate({app="x"} | json | keep __error__ [1m])`,
		`sum by (__error__) (rate({app="x"} | json | keep kind [5m]))`,
		`rate({app="x"} | json | keep foo | __error__!="" [5m])`,
		`rate({app="x"} | json | keep foo | drop __preserve_error__ [5m])`,
		`not a query (`,
	} {
		if got, changed := keepSeriesGrouping(q); changed || got != q {
			t.Errorf("%s: rewritten to %s", q, got)
		}
	}
}

// keepRows draws rows of three pods, half with a level stream label, whose
// lines are JSON or logfmt holding foo, level (colliding with the stream
// label or not) and other keys in random combinations.
func keepRows(rng *rand.Rand, s0 time.Time, n int, logfmt bool) []pushdownRow {
	rows := make([]pushdownRow, 0, n)
	pick := func(vs ...string) string { return vs[rng.Intn(len(vs))] }
	for i := 0; i < n; i++ {
		row := pushdownRow{ts: s0.Add(time.Duration(i) * 3 * time.Second), stream: map[string]string{"app": "api", "pod": pick("p1", "p2", "p3")}}
		// Under | logfmt the proxy reads a stream label a line's key shadows
		// from _stream with the tenant's stream label names
		// (plainStreamLabelNames), which it lists in the background; in this
		// emulation the proxy did not request that listing, so only JSON rows
		// carry the collision here.
		if !logfmt && rng.Intn(2) == 0 {
			row.stream["level"] = pick("info", "warn")
		}
		keys := map[string]string{"n": strconv.Itoa(i)}
		if rng.Intn(2) == 0 {
			keys["foo"] = pick("a", "b")
		}
		if rng.Intn(3) > 0 {
			keys["level"] = pick("info", "error")
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		rng.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
		parts := make([]string, 0, len(names))
		for _, k := range names {
			if logfmt {
				parts = append(parts, k+"="+keys[k])
			} else {
				parts = append(parts, strconv.Quote(k)+":"+strconv.Quote(keys[k]))
			}
		}
		if logfmt {
			row.msg = strings.Join(parts, " ")
		} else {
			row.msg = "{" + strings.Join(parts, ",") + "}"
		}
		rows = append(rows, row)
	}
	return rows
}

// sameMatrixValues compares two decoded matrices, the values to the last bits
// a different summation order changes.
func sameMatrixValues(got, want map[string]map[int64]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, points := range want {
		if len(got[key]) != len(points) {
			return false
		}
		for ts, w := range points {
			g, ok := got[key][ts]
			if !ok {
				return false
			}
			gv, gerr := strconv.ParseFloat(g, 64)
			wv, werr := strconv.ParseFloat(w, 64)
			if gerr != nil || werr != nil || math.Abs(gv-wv) > 1e-9*math.Max(1, math.Abs(wv)) {
				return false
			}
		}
	}
	return true
}

// keepFakeVL serves the rows with VictoriaLogs semantics: the pipes before
// the first stats pipe as pushdownFakeVL runs them (unpack_json,
// unpack_logfmt, fields, extract_regexp from _stream, ...), the stats, math
// and stats pipes after it as vlemu.Emulator does, and every other request
// (raw rows, probes, field listings) by pushdownFakeVL. It counts the stats
// and raw-row requests.
type keepFakeVL struct {
	stats, raw atomic.Int32
	// span is the time range of the last stats request, in nanoseconds.
	span atomic.Int64
}

func newKeepFakeVL(t *testing.T, rows []pushdownRow) (*httptest.Server, *pushdownFakeVL, *keepFakeVL) {
	t.Helper()
	inner, fake := newPushdownFakeVL(t, rows, nil)
	t.Cleanup(inner.Close)
	target, _ := url.Parse(inner.URL)
	rest := httputil.NewSingleHostReverseProxy(target)
	counts := &keepFakeVL{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			counts.stats.Add(1)
			_ = r.ParseForm()
			if from, to, ok := fakeVLRequestRange(r); ok {
				counts.span.Store(to - from)
			}
			if testing.Verbose() {
				t.Logf("fake VL stats: %s", r.Form.Get("query"))
			}
			if strings.Contains(r.Form.Get("query"), ") if (") {
				// A split's conditional stats (a strict | json count): pushdownFakeVL
				// runs those as the split tests read them.
				body := r.PostForm.Encode()
				r.Body, r.ContentLength = io.NopCloser(strings.NewReader(body)), int64(len(body))
				rest.ServeHTTP(w, r)
				return
			}
			before, _, found := strings.Cut(r.Form.Get("query"), " | stats ")
			if !found {
				t.Errorf("fake VL: no stats pipe in %s", r.Form.Get("query"))
				return
			}
			emu := &vlemu.Emulator{T: t}
			for _, row := range rows {
				if values, kept := fake.applyPipes(t, before, row); kept {
					msg := values["_msg"]
					if msg == "" {
						msg = row.msg
					}
					emu.Rows = append(emu.Rows, vlemu.Row{TS: row.ts.UnixNano(), Stream: "", Labels: values, Msg: msg})
				}
			}
			emu.ServeStats(w, r)
		case "/select/logsql/query":
			counts.raw.Add(1)
			rest.ServeHTTP(w, r)
		default:
			rest.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake, counts
}

// fakeVLRequestRange is the start and end of a VictoriaLogs request.
func fakeVLRequestRange(r *http.Request) (int64, int64, bool) {
	parse := func(v string) (int64, bool) {
		if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return ts.UnixNano(), true
		}
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	from, ok1 := parse(r.Form.Get("start"))
	to, ok2 := parse(r.Form.Get("end"))
	return from, to, ok1 && ok2
}

// lokiKeepReference evaluates fn(X | parser | keep labels [window]) with
// Loki's semantics (pkg/logql/log/parser.go, keep_labels.go, range_vector.go):
// the stream labels and service_name discovered at ingest, a parsed key named
// like a stream label renamed name_extracted, every label the keep does not
// name deleted, and the samples of equal label sets summed. outer is "" or a
// vector aggregation without grouping (sum, count).
func lokiKeepReference(rows []pushdownRow, parser string, keep []string, fn, outer string, window time.Duration, start, end time.Time, step time.Duration) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for at := start; !at.After(end); at = at.Add(step) {
		values := map[string]float64{}
		labelSets := map[string]map[string]string{}
		for _, row := range rows {
			if !row.ts.After(at.Add(-window)) || row.ts.After(at) {
				continue
			}
			labels := cloneStringMap(row.stream)
			labels["service_name"] = row.stream["app"]
			parsed := map[string]string{}
			switch parser {
			case "json":
				var obj map[string]string
				_ = json.Unmarshal([]byte(row.msg), &obj)
				parsed = obj
			case "logfmt":
				for _, kv := range strings.Fields(row.msg) {
					k, v, _ := strings.Cut(kv, "=")
					parsed[k] = v
				}
			}
			for k, v := range parsed {
				if _, ok := row.stream[k]; ok {
					k += "_extracted"
				}
				labels[k] = v
			}
			kept := map[string]string{}
			for _, k := range keep {
				if v, ok := labels[k]; ok && v != "" {
					kept[k] = v
				}
			}
			key := canonicalLabelsKey(kept)
			labelSets[key] = kept
			if fn == "bytes_over_time" {
				values[key] += float64(len(row.msg))
			} else {
				values[key]++
			}
		}
		if len(values) == 0 {
			continue
		}
		emit := func(key string, v float64) {
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][at.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
		total := 0.0
		for key, v := range values {
			if fn == "rate" {
				v /= window.Seconds()
			}
			total += v
			if outer == "" {
				emit(key, v)
			}
		}
		switch outer {
		case "sum":
			emit(canonicalLabelsKey(nil), total)
		case "count":
			emit(canonicalLabelsKey(nil), float64(len(values)))
		}
	}
	return out
}

// A differential run of range aggregations over a pipeline ending in | keep:
// random rows (stream level or not, a body level colliding with it or not,
// foo present or absent) and random keep lists, parsers, functions and outer
// aggregations; every answer equals the Loki-semantics reference, in which
// the streams that keep the same labels are one series.
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range
func TestKeepSeriesGrouping_Differential(t *testing.T) {
	seeds, perSeed := 30, 10
	if testing.Short() {
		seeds = 8
	}
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	keeps := [][]string{{"foo"}, {"pod"}, {"level"}, {"foo", "pod"}, {"level", "foo"}, {"zzz"}, {"level_extracted"}, {"service_name"}}
	checked := 0
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		parser := []string{"json", "logfmt", ""}[rng.Intn(3)]
		rows := keepRows(rng, s0, 220, parser == "logfmt")
		srv, fake, _ := newKeepFakeVL(t, rows)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		seen := map[string]bool{}
		for q := 0; q < perSeed; q++ {
			keep := keeps[rng.Intn(len(keeps))]
			// Without a parser no line key is a label; logfmt rows hold no
			// collision, so level_extracted only has a meaning for JSON rows.
			if (parser == "" && keep[0] == "foo") || (parser != "json" && keep[0] == "level_extracted") {
				keep = []string{"pod"}
			}
			fn := []string{"count_over_time", "rate", "bytes_over_time"}[rng.Intn(3)]
			outer := []string{"", "sum", "count"}[rng.Intn(3)]
			if parser == "json" && outer != "" && slices.Contains(keep, "level") {
				// Under an outer fold a JSON keep is grouped in VictoriaLogs'
				// stats by the kept field, which holds the line's level where
				// a stream label of that name is shadowed
				// (semantics/stream-label-shadowed-after-json-parser).
				keep = []string{"foo", "pod"}
			}
			pipeline := ""
			if parser != "" {
				pipeline = " | " + parser
			}
			query := fmt.Sprintf(`%s({app="api"}%s | keep %s [1m])`, fn, pipeline, strings.Join(keep, ", "))
			if outer != "" {
				query = outer + "(" + query + ")"
			}
			if seen[query] {
				continue
			}
			seen[query] = true
			want := lokiKeepReference(rows, parser, keep, fn, outer, time.Minute, start, end, time.Minute)
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Errorf("seed %d: %s: status %d: %s", seed, query, rec.Code, rec.Body)
				continue
			}
			if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !sameMatrixValues(got, want) {
				t.Errorf("seed %d: %s\n got: %v\nwant: %v", seed, query, got, want)
			}
			checked++
		}
	}
	t.Logf("queries checked: %d", checked)
}

// The instant form of the 2026-11-04 case: count over a keep is the number of
// label sets the keep leaves (Loki 1 for a label no line has), on the query
// route too.
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query
func TestKeepSeriesGrouping_InstantCount(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := keepRows(rand.New(rand.NewSource(3)), s0, 220, false)
	srv, fake, _ := newKeepFakeVL(t, rows)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	at := s0.Add(6 * time.Minute)
	for _, keep := range []string{"zzz", "foo", "pod", "foo, pod"} {
		query := fmt.Sprintf(`count(rate({app="api"} | json | keep %s [5m]))`, keep)
		want := lokiKeepReference(rows, "json", strings.Split(keep, ", "), "rate", "count", 5*time.Minute, at, at, time.Minute)
		params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
		rec := httptest.NewRecorder()
		p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || rec.Code != http.StatusOK || len(resp.Data.Result) != 1 {
			t.Fatalf("%s: %d %s", query, rec.Code, rec.Body)
		}
		if got, w := resp.Data.Result[0].Value[1], want[canonicalLabelsKey(nil)][at.Unix()]; got != w || len(resp.Data.Result[0].Metric) != 0 {
			t.Errorf("%s: %v %v, want {} %s", query, resp.Data.Result[0].Metric, got, w)
		}
	}
}

// The answers of a keep query cached before the fix (level_extracted kept,
// one series per stream) are not served after it: the response keys and the
// log window fragments of a keep query carry the v2 namespace.
// conformance: semantics/keep-leaves-labels-loki-drops
func TestKeepSeriesGrouping_CacheKeysFenced(t *testing.T) {
	const query = `{namespace="prod"} | json | keep level`
	if got := keepMetadataProjectionResponseKeyVersion(query); got != "@keep-metadata-projection-v2:" {
		t.Fatalf("response key version %q", got)
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	stages := make([]string, 0, len(lq.Pipeline))
	for _, stage := range lq.Pipeline {
		stages = append(stages, stage.String())
	}
	if newLogQueryShape(query).fingerprint == logQueryShapeFingerprint(strings.Join(stages, "\n")) {
		t.Fatal("the window fragments of a keep query share the pre-fix fingerprint")
	}
	const plain = `{namespace="prod"} | json`
	if newLogQueryShape(plain).fingerprint != logQueryShapeFingerprint("| json") {
		t.Fatal("the window fingerprint of a query without keep changed")
	}
}

// FuzzKeepSeriesGrouping: any query either stays as it is or is rewritten to
// a query that parses and is not rewritten again.
func FuzzKeepSeriesGrouping(f *testing.F) {
	for _, seed := range []string{
		`count(rate({service_name="api-gateway"} | json | keep foo [5m]))`,
		`rate({app="x"} | keep a, b | keep b [1m]) / rate({app="y"} | keep a [1m])`,
		`sum by (a) (count_over_time({app="x"} | keep a [1m]))`,
		`rate({app="x"} | keep level="error" [1m])`,
		`{app="x"} | json | keep level`,
		`topk(2, bytes_rate({app="x"} | json | keep __error__ [1m]))`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		out, changed := keepSeriesGrouping(query)
		if !changed {
			if out != query {
				t.Fatalf("unchanged query returned as %q", out)
			}
			return
		}
		if _, err := logqlpkg.Parse(out); err != nil {
			t.Fatalf("%q rewritten to %q, which does not parse: %v", query, out, err)
		}
		if again, changed := keepSeriesGrouping(out); changed {
			t.Fatalf("%q rewritten twice: %q then %q", query, out, again)
		}
	})
}

// A Grafana range that moves keeps the settled-steps cache for a count over
// a JSON keep (bench/ab K3 `count(rate({...} | json | keep foo [5m]))`): the
// first request reads its window from VictoriaLogs stats, a repeat one step
// later reads only the new step, and both answer like Loki. Grouping it as
// `count(sum by (foo) (...))` sent it to the ordered JSON fold, which
// re-reads every row of the window on each repeat.
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range
func TestKeepSeriesGrouping_JSONFoldKeepsSettledSteps(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := keepRows(rand.New(rand.NewSource(9)), s0, 600, false)
	srv, fake, counts := newKeepFakeVL(t, rows)
	c := cache.NewWithMaxBytes(time.Minute, 100000, 64<<20)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", LabelStyle: LabelStyleUnderscores,
		QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour, QueryRangeFreshness: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	fake.stored = p.labelTranslator.ToVL
	const query = `count(rate({app="api"} | json | keep foo [5m]))`
	for i, shift := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		start, end := s0.Add(6*time.Minute+shift), s0.Add(26*time.Minute+shift)
		want := lokiKeepReference(rows, "json", []string{"foo"}, "rate", "count", 5*time.Minute, start, end, time.Minute)
		stats, raw := counts.stats.Load(), counts.raw.Load()
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: %d %s", i, rec.Code, rec.Body)
		}
		if got := decodeExtractedMatrix(t, rec.Body.Bytes()); !sameMatrixValues(got, want) {
			t.Errorf("request %d\n got: %v\nwant: %v", i, got, want)
		}
		dStats, dRaw := counts.stats.Load()-stats, counts.raw.Load()-raw
		span := time.Duration(counts.span.Load())
		t.Logf("request %d: %d stats over %s, %d raw-row requests", i, dStats, span, dRaw)
		if dRaw != 0 || dStats != 1 {
			t.Errorf("request %d read %d stats and %d raw-row requests; want one stats call and no raw rows", i, dStats, dRaw)
		}
		if i > 0 && span > 10*time.Minute {
			t.Errorf("request %d read %s of stats; a repeat one step later reads the new step and its range, not the window", i, span)
		}
	}
}
