package proxy

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// foldParseReference is what the two passes give an answer within the
// pooled parser's bound: the translation (trimAndTranslateStatsQRFJ), the
// wrap, and the tumbling relabel of the wrapped answer
// (trimStatsQRByTimeFJShifted), the calls proxyStatsQueryRangeDirect and its
// caller make when the one parse does not answer.
func foldParseReference(p *Proxy, body []byte, keep func(int64) bool, query string, relabelKeep func(int64) bool, relabel func(int64) int64) (translated, want []byte) {
	translated = wrapAsLokiResponse(p.trimAndTranslateStatsQRFJ(context.Background(), body, keep, query), "matrix")
	return translated, trimStatsQRByTimeFJShifted(translated, relabelKeep, relabel)
}

// assertFoldParseMatches holds the one parse to the two passes: when it
// answers, the series count it reports is the translated answer's and its
// bytes are the relabelled answer's (but for the order of a translated
// series' labels, which the translation writes in map order on every path).
func assertFoldParseMatches(t *testing.T, p *Proxy, body []byte, keep func(int64) bool, query string, relabelKeep func(int64) bool, relabel func(int64) int64) bool {
	t.Helper()
	got, series, ok := p.trimTranslateRelabelStatsQRFJ(body, keep, query, relabelKeep, relabel)
	if !ok {
		return false
	}
	translated, want := foldParseReference(p, body, keep, query, relabelKeep, relabel)
	if _, isErr := vlStatsErrorBody(translated); isErr {
		t.Fatalf("the one parse answered a translated error body\ninput %.400s", body)
	}
	if n := lokiResultSeriesCount(translated); series != n {
		t.Fatalf("translated series %d, the two passes %d\ninput %.400s", series, n, body)
	}
	if !sameStatsAnswer(got, want) {
		t.Fatalf("one parse differs from two passes\ninput %.400s\n got %.400s\nwant %.400s", body, got, want)
	}
	return true
}

// sumWithoutBody is a VictoriaLogs answer to the stats query of
// `sum without (pod) (count_over_time({...}[1m]))` folded upstream: series
// that keep their _stream once pod is cut, one point per bucket of the grid.
func sumWithoutBody(series, buckets int, startSec, step int64, extra string) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < series; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"metric":{"__name__":"count(*)","_stream":"{app=\"app-%02d\",env=\"production\",namespace=\"prod\"}","level":"info"%s},"values":[`, i, extra)
		for k := 0; k < buckets; k++ {
			if k > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, startSec+int64(k)*step, 1+(i+k)%9)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// Edge cases of the one parse against the two passes: a 24h sum without
// answer, the edges the relabel drops (a series left with none is dropped,
// and so is every series), labels the translation renames, escapes or cannot
// write back plainly, float bucket labels, a stats block, numeric values,
// and the forms it leaves to the two passes (several result slots, an error
// key, no resultType, nothing to translate, a point the first pass trims, a
// malformed point, a value with escapes, no metric or no values, and an
// answer over the pooled parser's bound).
// conformance: quality/stats-tumbling-relabel-one-parse, semantics/without-sliding-sum-folded-buckets
func TestStatsFoldRelabelOneParseMatchesTwoPasses(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	const step = int64(60)
	window := step * int64(time.Second)
	first := int64(1700000000)
	start := first*int64(time.Second) + window // the first relabelled bucket
	end := start + 1439*window
	query := `sum without (pod) (count_over_time({env="production"}[1m]))`
	relabelKeep, relabel := snappedTumblingRelabel(start, end, window)
	keepAll := func(int64) bool { return true }
	sw := sumWithoutBody(27, 1441, first, step, "")
	for _, tc := range []struct {
		name   string
		body   []byte
		keep   func(int64) bool
		rkeep  func(int64) bool
		folds  bool
		reason string
	}{
		{name: "sum without, 24h", body: sw, keep: keepAll, folds: true},
		{name: "no trim function", body: sw, folds: true},
		{name: "relabel drops the edges", body: sumWithoutBody(3, 10, first-2*step, step, ""), keep: keepAll, folds: true},
		{name: "relabel drops a series", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1699999000,"1"]]},{"metric":{"__name__":"c","pod":"b"},"values":[[1700000000,"2"]]}]}}`), keep: keepAll, folds: true},
		{name: "relabel drops every series", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1699999000,"1"]]}]}}`), keep: keepAll, folds: true},
		{name: "dotted, escaped and unicode labels", body: sumWithoutBody(5, 30, first, step, `,"service.name":"a<b>&é","q":"say \"hi\"","bs":"a\\b"`), keep: keepAll, folds: true},
		{name: "control character label", body: sumWithoutBody(2, 5, first, step, `,"ctl":"a\u0001b"`), keep: keepAll, folds: true},
		{name: "float bucket labels", body: bytes.ReplaceAll(sw, []byte(`00,"`), []byte(`00.0000001,"`)), keep: keepAll, folds: true},
		{name: "millisecond bucket labels", body: bytes.ReplaceAll(sumWithoutBody(3, 20, first, step, ""), []byte(`0,"`), []byte(`0.001,"`)), keep: keepAll, folds: true},
		{name: "stats block", body: bytes.Replace(sw, []byte(`]}}`), []byte(`],"stats":{"seriesFetched":"7","x":[1,"a\"b"]}}}`), 1), keep: keepAll, folds: true},
		{name: "number sample values", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1700000060,3],[1700000120,1e2]]}]}}`), keep: keepAll, folds: true},
		{name: "no status", body: bytes.Replace(sw, []byte(`"status":"success",`), nil, 1), keep: keepAll, folds: true},
		{name: "results slot too", body: append(bytes.TrimSuffix(sumWithoutBody(3, 5, first, step, ""), []byte(`}`)), []byte(`,"results":[{"metric":{"a":"b"},"values":[[1700000000,"1"]]}]}`)...), keep: keepAll},
		{name: "error key", body: append(bytes.TrimSuffix(sumWithoutBody(3, 5, first, step, ""), []byte(`}`)), []byte(`,"error":"x"}`)...), keep: keepAll},
		{name: "no resultType", body: bytes.Replace(sumWithoutBody(3, 5, first, step, ""), []byte(`"resultType":"matrix",`), nil, 1), keep: keepAll},
		{name: "nothing to translate", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000060,"1"]]}]}}`), keep: keepAll},
		{name: "a point the first pass trims", body: sumWithoutBody(3, 5, first, step, ""), keep: func(ts int64) bool { return ts < first*int64(time.Second)+2*window }},
		{name: "a point of one element", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1700000060]]}]}}`), keep: keepAll},
		{name: "a string timestamp", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[["1700000060","1"]]}]}}`), keep: keepAll},
		{name: "a value with an escape", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1700000060,"a\"b"]]}]}}`), keep: keepAll},
		{name: "no metric", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"values":[[1700000060,"1"]]},{"metric":{"__name__":"c"},"values":[[1700000060,"1"]]}]}}`), keep: keepAll},
		{name: "no values", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"value":[1700000060,"1"]}]}}`), keep: keepAll},
		{name: "empty result", body: []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), keep: keepAll},
		{name: "over the pooled parser's bound", body: sumWithoutBody(60, 1441, first, step, ""), keep: keepAll},
		{name: "not JSON", body: []byte(`{"status":"success","data":{`), keep: keepAll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rkeep := relabelKeep
			if tc.rkeep != nil {
				rkeep = tc.rkeep
			}
			if folded := assertFoldParseMatches(t, p, tc.body, tc.keep, query, rkeep, relabel); folded != tc.folds {
				t.Fatalf("folded %v, want %v (%d bytes) %s", folded, tc.folds, len(tc.body), tc.reason)
			}
		})
	}
}

// Random answers of every form the one parse takes or leaves (series counts,
// bucket grids and offsets, float labels, labels that need escapes, trims and
// relabel windows): wherever it answers, it answers what the two passes do.
// conformance: quality/stats-tumbling-relabel-one-parse
func TestStatsFoldRelabelOneParseDifferential(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	rnd := rand.New(rand.NewSource(1104))
	labels := []string{"", `,"service.name":"api"`, `,"note":"<&>"`, `,"q":"\"x\""`, `,"level":""`, `,"detected_level":"warn"`}
	folded := 0
	runs := 600
	if testing.Short() {
		runs = 150
	}
	for i := 0; i < runs; i++ {
		step := []int64{1, 5, 10, 60, 300}[rnd.Intn(5)]
		window := step * int64(time.Second)
		first := int64(1700000000) + int64(rnd.Intn(7200))
		buckets := 1 + rnd.Intn(80)
		body := sumWithoutBody(1+rnd.Intn(12), buckets, first, step, labels[rnd.Intn(len(labels))])
		if rnd.Intn(4) == 0 {
			body = bytes.ReplaceAll(body, []byte(`,"`), []byte(`.5,"`)) // float labels (and broken labels: left to the two passes)
		}
		if rnd.Intn(5) == 0 {
			body = bytes.ReplaceAll(body, []byte(`"__name__":"count(*)",`), nil)
		}
		start := first*int64(time.Second) + int64(rnd.Intn(3))*window
		end := start + int64(rnd.Intn(buckets+2))*window
		if rnd.Intn(6) == 0 {
			end = 0
		}
		relabelKeep, relabel := snappedTumblingRelabel(start, end, window)
		var keep func(int64) bool
		switch rnd.Intn(3) {
		case 1:
			keep = func(int64) bool { return true }
		case 2:
			cut := first*int64(time.Second) + int64(rnd.Intn(buckets+1))*window
			keep = func(ts int64) bool { return ts <= cut }
		}
		if assertFoldParseMatches(t, p, body, keep, `sum without (pod) (count_over_time({env="production"}[1m]))`, relabelKeep, relabel) {
			folded++
		}
	}
	if folded < runs/4 {
		t.Fatalf("the one parse answered %d of %d answers", folded, runs)
	}
}

// FuzzStatsFoldRelabelOneParseDifferential: whatever the stats answer, the
// trim and the relabel window, the one parse answers what the two passes do,
// with the translated series count they see.
func FuzzStatsFoldRelabelOneParseDifferential(f *testing.F) {
	for _, body := range []string{
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"count(*)","pod":"a"},"values":[[1700000000,"1"],[1700000060,"2"]]},{"metric":{"service.name":"api"},"values":[[1700000120,"3"]]}]}}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"a"},"values":[[1700000000.5,"1"]]},{"metric":{"pod":"b"},"values":[]}],"stats":{"x":1}}}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"<\"\\"},"values":[[1700000000,2],[1700000060,"x"]]}]}}`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"count(*)","_stream":"{app=\"a\",env=\"production\"}","level":"info"},"values":[[1700000060,"4"]]}]}}`,
		`{broken`,
	} {
		f.Add([]byte(body), int64(0), int64(60), uint8(3))
	}
	f.Fuzz(func(t *testing.T, body []byte, startShift, step int64, endSteps uint8) {
		if len(body) > 64<<10 || step <= 0 || step > 3600 {
			t.Skip()
		}
		p := newTestProxy(t, "http://unused")
		p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
		window := step * int64(time.Second)
		start := int64(1700000000)*int64(time.Second) + (startShift%7200)*int64(time.Second)
		relabelKeep, relabel := snappedTumblingRelabel(start, start+int64(endSteps)*window, window)
		for _, keep := range []func(int64) bool{nil, func(ts int64) bool { return ts <= start+int64(endSteps/2)*window }} {
			assertFoldParseMatches(t, p, body, keep, `sum without (pod) (count_over_time({app="a"}[1m]))`, relabelKeep, relabel)
		}
	})
}

// BenchmarkTumblingRelabelWarm measures range == step answers served from
// kept stats buckets (every bucket cached after a first pass over the moved
// windows), as the daily run's warm repeats are: the work left per request
// is the proxy's own. The SW1 shape (`sum without (pod)` of a 1m count over
// 24h at a 60s step, 27 series) and smaller answers take the one parse; a
// 6h breakdown at a 10s step (40 series, over the pooled parser's bound)
// takes the per-series pass, unchanged.
func BenchmarkTumblingRelabelWarm(b *testing.B) {
	for _, bc := range []struct {
		name   string
		series int
		rng    time.Duration
		step   time.Duration
		query  string
	}{
		{"SW1_24h_step60_27series", 27, 24 * time.Hour, time.Minute, `sum without (pod) (count_over_time({env="production", instrumentation!="otel", app!="otel-collector"}[1m]))`},
		{"sum_by_1h_step60_10series", 10, time.Hour, time.Minute, `sum by (event) (count_over_time({env="production"}[1m]))`},
		{"sum_by_6h_step60_40series", 40, 6 * time.Hour, time.Minute, `sum by (event) (count_over_time({env="production"}[1m]))`},
		{"breakdown_6h_step10_40series", 40, 6 * time.Hour, 10 * time.Second, `sum by (event) (count_over_time({env="production"}[10s]))`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			srv := statsRangeSynthVL(b, bc.series)
			defer srv.Close()
			p := newExtentTestProxy(b, srv.URL, true, 256<<20)
			end := time.Unix(1700001000, 0).UTC().Truncate(time.Hour).Add(7 * time.Hour)
			for run := 0; run < 31; run++ {
				e := end.Add(-time.Duration(run) * bc.step)
				if code, body := extentQueryRange(b, p, bc.query, e.Add(-bc.rng), e, bc.step, nil); code != 200 {
					b.Fatalf("%d %.300s", code, body)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e := end.Add(-time.Duration(i%30+1) * bc.step)
				// Trailing blanks make every request a new one for the response
				// cache; the kept buckets answer it.
				if code, body := extentQueryRange(b, p, bc.query+strings.Repeat(" ", i+1), e.Add(-bc.rng), e, bc.step, nil); code != 200 {
					b.Fatalf("%d %.300s", code, body)
				}
			}
		})
	}
}
