package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// foldRelabelReference is what the two passes give: the per-series
// translation, the wrap, and the per-series tumbling relabel of the result.
func foldRelabelReference(p *Proxy, body []byte, keep func(int64) bool, query string, relabelKeep func(int64) bool, relabel func(int64) int64) (translated, want []byte, handled bool) {
	translated, _, handled = p.trimAndTranslateStatsQRBySeries(body, keep, query)
	if !handled {
		return nil, nil, false
	}
	wrapped, err := wrapAsLokiResponse(translated, "matrix")
	if err != nil {
		// A body that is no answer: the comparison reports it as a translated error body.
		wrapped = []byte(`{"error":"not an answer"}`)
	}
	want, handled = trimStatsQRBySeries(wrapped, relabelKeep, relabel)
	return wrapped, want, handled
}

// assertFoldRelabelMatches holds the one-pass rewrite to the two passes: when
// it answers, the translated answer it reports (size and series count, which
// decide the series limit and the relabel's path) is the two passes' and its
// bytes are theirs.
func assertFoldRelabelMatches(t *testing.T, p *Proxy, body []byte, keep func(int64) bool, query string, relabelKeep func(int64) bool, relabel func(int64) int64) bool {
	t.Helper()
	got, size, series, ok := p.trimTranslateRelabelStatsQRBySeries(body, keep, query, relabelKeep, relabel)
	if !ok {
		return false
	}
	translated, want, handled := foldRelabelReference(p, body, keep, query, relabelKeep, relabel)
	if !handled {
		t.Fatalf("the one-pass rewrite answered an input the two passes do not handle\ninput %.400s", body)
	}
	if _, isErr := vlStatsErrorBody(translated); isErr {
		t.Fatalf("the one-pass rewrite answered a translated error body")
	}
	if size != len(translated) || series != lokiResultSeriesCount(translated) {
		t.Fatalf("translated size/series %d/%d, the two passes %d/%d", size, series, len(translated), lokiResultSeriesCount(translated))
	}
	if !sameStatsAnswer(got, want) {
		t.Fatalf("one pass differs from two\ninput %.400s\n got %.400s\nwant %.400s", body, got, want)
	}
	return true
}

// sameStatsAnswer: the same bytes, but for the order of a series' labels,
// which the translation writes in map order (marshalStringMapJSONTo) on
// every path: the same length and the same JSON.
func sameStatsAnswer(a, b []byte) bool {
	return bytes.Equal(a, b) || len(a) == len(b) && statsJSONEquivalent(a, b)
}

func foldChurnBody(series, perBucket int, step int64, extra string) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < series; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		ts := 1700000000 + step*int64(i/perBucket)
		fmt.Fprintf(&b, `{"metric":{"__name__":"count(*)","pod":"pod-%07d"%s},"values":[[%d,"%d"]]}`, i, extra, ts, 1+i%3)
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// Edge cases of the one-pass rewrite against the two passes (the same bytes,
// but for label order where the translation writes a label map): the points the
// relabel drops at both edges (a series left with none is dropped), the
// trim before the relabel, labels the translation renames or escapes,
// float bucket labels, a stats block, many points per series, and the forms
// it leaves to the two passes (several result slots, an error key, no
// resultType, nothing to translate, a series it cannot read).
// conformance: quality/stats-tumbling-relabel-one-pass
func TestStatsFoldRelabelMatchesTwoPasses(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	const step = int64(60)
	window := step * int64(time.Second)
	start := int64(1700000000)*int64(time.Second) + window // the first relabelled bucket
	query := `sum by (pod) (count_over_time({app="a"}[60s]))`
	end := start + 30*window
	relabelKeep, relabel := snappedTumblingRelabel(start, end, window)
	keepAll := func(int64) bool { return true }
	keepBefore := func(ts int64) bool { return ts <= start+20*window }
	for _, tc := range []struct {
		name    string
		body    []byte
		keep    func(int64) bool
		folds   bool
		relabel bool // relabelKeep drops points
	}{
		{"churn", foldChurnBody(20000, 700, step, ""), keepAll, true, true},
		{"trimmed before the relabel", foldChurnBody(20000, 700, step, ""), keepBefore, true, true},
		{"no trim", foldChurnBody(20000, 700, step, ""), nil, true, true},
		{"dotted and escaped labels", foldChurnBody(12000, 400, step, `,"service.name":"a<b>&\"c\\u00e9"`), keepAll, true, true},
		{"float bucket labels", bytes.ReplaceAll(foldChurnBody(20000, 700, step, ""), []byte(`00,"`), []byte(`00.0000001,"`)), keepAll, true, true},
		{"stats block", bytes.Replace(foldChurnBody(20000, 700, step, ""), []byte(`]}}`), []byte(`],"stats":{"seriesFetched":"7"}}}`), 1), keepAll, true, true},
		{"many points", buildStatsMatrixBody(1000, 60), keepAll, true, false},
		{"results slot too", append(bytes.TrimSuffix(foldChurnBody(20000, 700, step, ""), []byte(`}`)), []byte(`,"results":[{"metric":{"a":"b"},"values":[[1700000000,"1"]]}]}`)...), keepAll, false, false},
		{"error key", append(bytes.TrimSuffix(foldChurnBody(20000, 700, step, ""), []byte(`}`)), []byte(`,"error":"x"}`)...), keepAll, false, false},
		{"no resultType", bytes.Replace(foldChurnBody(20000, 700, step, ""), []byte(`"resultType":"matrix",`), nil, 1), keepAll, false, false},
		{"nothing to translate", bytes.ReplaceAll(foldChurnBody(20000, 700, step, ""), []byte(`"__name__":"count(*)",`), nil), nil, false, false},
		{"unreadable last series", bytes.Replace(foldChurnBody(20000, 700, step, ""), []byte(`]}}`), []byte(`,{"metric":{"pod":"x"},"values":[[1700000000,"1"]],"bad":[}]}}`), 1), keepAll, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.body) <= maxPooledStatsParserInputSize {
				t.Fatal("the fixture must take the per-series path")
			}
			if folded := assertFoldRelabelMatches(t, p, tc.body, tc.keep, query, relabelKeep, relabel); folded != tc.folds {
				t.Fatalf("folded %v, want %v", folded, tc.folds)
			}
		})
	}
}

// FuzzStatsFoldRelabelDifferential: whatever the stats answer, the trim and
// the relabel window, the one-pass rewrite answers what the two passes do,
// with the translated size and series count they see.
func FuzzStatsFoldRelabelDifferential(f *testing.F) {
	for _, body := range []string{
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"count(*)","pod":"a"},"values":[[1700000000,"1"],[1700000060,"2"]]},{"metric":{"service.name":"api"},"values":[[1700000120,"3"]]}]}}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000000.5,"1"]]},{"metric":{"pod":"b"},"values":[]},{"metric":{"pod":"c"}}],"stats":{"x":1}}}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","pod":"<"},"values":[[1700000000,"1"],[1700000060]]}]}}`,
		`{"data":{"resultType":"matrix","result":[{"metric":{"__name__":"c"},"value":[1700000000,"1"],"values":[[1700000000,"1"]]}]},"status":"success"}`,
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
			assertFoldRelabelMatches(t, p, body, keep, `sum by (pod) (count_over_time({app="a"}[1m]))`, relabelKeep, relabel)
		}
	})
}

// foldFakeVL answers stats_query_range with perBucket single-point series in
// every bucket of the request (VictoriaLogs' bucket grid, step and offset),
// ignoring the query, as a high-cardinality Drilldown breakdown answers.
func foldFakeVL(t *testing.T, perBucket int) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/stats_query_range" {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
			return
		}
		start, end, step, offset, ok := slidingBucketParams(t, r)
		if !ok {
			return
		}
		start = vlTruncate(start, step, offset)
		end = vlTruncate(end-1, step, offset) + step
		var b strings.Builder
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		first := true
		for bucket := start; bucket < end; bucket += step {
			for j := 0; j < perBucket; j++ {
				if !first {
					b.WriteByte(',')
				}
				first = false
				secs := strconv.FormatFloat(float64(bucket)/1e9, 'f', -1, 64)
				fmt.Fprintf(&b, `{"metric":{"__name__":"count(*)","pod":"p%d-%04d"},"values":[[%s,"%d"]]}`, bucket/int64(time.Second), j, secs, 1+j%5)
			}
		}
		b.WriteString(`]}}`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The series limit sees the translated answer exactly as before: a Logs
// Drilldown breakdown under the limit, over it (the busiest series and Loki's
// warning), any other client over it (Loki's 400), a topk over it, and
// smaller answers (one parse of the whole answer) give the same status,
// warning and bytes with the one pass and with the two passes, and the
// answers within the limit take the one pass.
// conformance: quality/stats-tumbling-relabel-one-pass, quality/stats-tumbling-relabel-one-parse, series-limits-and-partial-results
func TestStatsFoldRelabelKeepsTheSeriesLimit(t *testing.T) {
	end := time.Unix(1700086400, 0).UTC()
	for _, tc := range []struct {
		name      string
		query     string
		hours     int
		perBucket int
		limit     int
		drilldown bool
		folds     bool
	}{
		{"drilldown under the limit", `sum by (pod) (count_over_time({app="a"}[1m]))`, 6, 60, 1000000, true, true},
		{"drilldown over the limit", `sum by (pod) (count_over_time({app="a"}[1m]))`, 6, 60, 5000, true, false},
		{"explore under the limit", `sum by (pod) (count_over_time({app="a"}[1m]))`, 6, 60, 1000000, false, true},
		{"explore over the limit", `sum by (pod) (count_over_time({app="a"}[1m]))`, 6, 60, 5000, false, false},
		{"topk over the limit", `topk(10, sum by (pod) (count_over_time({app="a"}[1m])))`, 6, 60, 5000, true, false},
		{"small answer", `sum by (pod) (count_over_time({app="a"}[1m]))`, 1, 5, 1000000, true, true},
		{"small answer over the limit", `sum by (pod) (count_over_time({app="a"}[1m]))`, 1, 5, 100, false, false},
		{"sum without, 24h", `sum without (pod) (count_over_time({app="a"}[1m]))`, 24, 1, 1000000, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := foldFakeVL(t, tc.perBucket)
			answers := map[bool]*httptest.ResponseRecorder{}
			var folded int
			for _, fold := range []bool{false, true} {
				c := cache.New(time.Minute, 1000)
				t.Cleanup(c.Close)
				p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", MaxStatsQuerySeries: tc.limit})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
				p.storeBackendVersion("v1.52.0", "v1.52.0")
				p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
				params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(end.Add(-time.Duration(tc.hours)*time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {"60"}}
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
				if tc.drilldown {
					req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
				}
				foldTumblingRelabel = fold
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, req)
				foldTumblingRelabel = true
				answers[fold] = rec
				if fold {
					folded = foldedCount(t, p)
				}
			}
			two, one := answers[false], answers[true]
			if one.Code != two.Code || one.Header().Get("Warning") != two.Header().Get("Warning") || !sameStatsAnswer(one.Body.Bytes(), two.Body.Bytes()) {
				t.Fatalf("one pass %d %q %.300s\ntwo passes %d %q %.300s", one.Code, one.Header().Get("Warning"), one.Body.Bytes(), two.Code, two.Header().Get("Warning"), two.Body.Bytes())
			}
			if (folded > 0) != tc.folds {
				t.Fatalf("one pass taken %d times, want taken: %v (status %d, %d bytes)", folded, tc.folds, one.Code, one.Body.Len())
			}
		})
	}
}

// foldedCount is the number of answers the one pass rewrote.
func foldedCount(t *testing.T, p *Proxy) int {
	t.Helper()
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	const prefix = `loki_vl_proxy_internal_operation_total{operation="trim_translate_stats_qr",outcome="folded"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			n, _ := strconv.Atoi(strings.TrimSpace(rest))
			return n
		}
	}
	return 0
}
