package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkJSONWrappedMetricHandler measures the query_range handler for JSON metrics under functions the ordered
// JSON evaluator does not own, where the JSON parser error check runs, and for controls it does not touch, over
// 5000 raw rows whose lines all parse (the success path: no mark, no lookup).
func BenchmarkJSONWrappedMetricHandler(b *testing.B) {
	base := time.Unix(1700000000, 0).UTC()
	for _, query := range []string{
		`sort(rate({namespace="prod"} | json [2m]))`,
		`topk(3, rate({namespace="prod"} | json [2m]))`,
		`sort(rate({namespace="prod"} | json | __error__="" [2m]))`,
		`sort(rate({namespace="prod"}[2m]))`,
	} {
		b.Run(query, func(b *testing.B) {
			p, backend := makeBenchProxy(b, ndJSON5000)
			defer backend.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params := url.Values{}
				params.Set("query", query)
				params.Set("start", strconv.FormatInt(base.Unix(), 10))
				// A distinct end per iteration keeps the response cache out of the loop.
				params.Set("end", strconv.FormatInt(base.Add(5*time.Minute).Unix()+int64(i%1000), 10))
				params.Set("step", "30")
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkJSONWrappedMetricStats measures the query_range handler on the stats route (step equal to the range) for
// a JSON metric under sort, in front of a VictoriaLogs that answers 60 series of 10 points (as marked groups when
// the query is marked: every line parsed), and a control without a JSON parser.
func BenchmarkJSONWrappedMetricStats(b *testing.B) {
	var series strings.Builder
	for s := 0; s < 60; s++ {
		if s > 0 {
			series.WriteByte(',')
		}
		series.WriteString(`{"metric":{"__name__":"c","_stream":"{app=\"pay\",pod=\"p` + strconv.Itoa(s) + `\"}"MARK},"values":[`)
		for i := 0; i < 10; i++ {
			if i > 0 {
				series.WriteByte(',')
			}
			series.WriteString(`[` + strconv.Itoa(1700000000+60*i) + `,"` + strconv.Itoa(s+i) + `"]`)
		}
		series.WriteString(`]}`)
	}
	plain := `{"status":"success","data":{"resultType":"matrix","result":[` + strings.ReplaceAll(series.String(), "MARK", "") + `]}}`
	marked := `{"status":"success","data":{"resultType":"matrix","result":[` + strings.ReplaceAll(series.String(), "MARK", `,"__vlp":"1"`) + `]}}`
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if strings.Contains(r.Form.Get("query"), "__vlp") {
			_, _ = w.Write([]byte(marked))
			return
		}
		_, _ = w.Write([]byte(plain))
	}))
	defer vl.Close()
	at := time.Unix(1700000000, 0)
	for _, query := range []string{`sort(rate({app="pay"} | json [1m]))`, `sort(rate({app="pay"}[1m]))`} {
		b.Run(query, func(b *testing.B) {
			p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"},
					"end": {strconv.FormatInt(at.Add(10*time.Minute).UnixNano()+int64(i%1000)*int64(time.Second), 10)}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil))
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkJSONErrorProbes measures the decision on the parsed query.
func BenchmarkJSONErrorProbes(b *testing.B) {
	for _, query := range []string{
		`sort(rate({namespace="prod"} | json [2m]))`,
		`sum by (app) (rate({namespace="prod"} | json [2m])) / on (app) sum by (app) (rate({namespace="prod"} | json [2m]))`,
	} {
		expr, err := parseLogQLExpr(noFlowCtx, query)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if len(jsonErrorProbes(expr)) == 0 {
					b.Fatal("no probe")
				}
			}
		})
	}
}

// BenchmarkRawRowHead measures what a checked raw-row read spends per row on the fields it needs before the sample:
// the time, stream and level plus the mark. "lookups" is a lookup per field and one for the mark (the mark costs a
// full scan of the row's keys when the row is not marked); "one pass" reads all four in one pass over the keys.
func BenchmarkRawRowHead(b *testing.B) {
	lines := strings.Split(strings.TrimSpace(string(ndJSON5000)), "\n")
	rows := make([]*fj.Value, len(lines))
	for i, line := range lines {
		var q fj.Parser
		v, err := q.Parse(line)
		if err != nil {
			b.Fatal(err)
		}
		rows[i] = v
	}
	var sink int
	b.Run("lookups", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range rows {
				sink += len(v.GetStringBytes("_time")) + len(v.GetStringBytes("_stream")) + len(v.GetStringBytes("level"))
				if v.Get(jsonErrorField) != nil {
					sink++
				}
			}
		}
	})
	b.Run("one pass", func(b *testing.B) {
		c := &jsonErrorCheck{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range rows {
				ts, stream, level, marked := c.rawRowHead(v)
				sink += len(ts) + len(stream) + len(level)
				if marked {
					sink++
				}
			}
		}
	})
	_ = sink
}

// BenchmarkRawRowContextCheck measures the per-row cancellation check of a raw-row read at the depth of a checked
// request's context chain: ctx.Err() walks the chain on every row, a channel taken once does not.
func BenchmarkRawRowContextCheck(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 4; i++ {
		ctx = context.WithValue(ctx, jsonErrorMarkOffKey{}, i)
	}
	limited := &io.LimitedReader{N: 1 << 30}
	b.Run("ctx.Err per row", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if checkManualMetricRead(ctx, limited) != nil {
				b.Fatal("refused")
			}
		}
	})
	b.Run("done channel per row", func(b *testing.B) {
		done := ctx.Done()
		for i := 0; i < b.N; i++ {
			if checkManualMetricReadDone(ctx, done, limited) != nil {
				b.Fatal("refused")
			}
		}
	})
	for _, function := range []string{"count_over_time", "rate"} {
		b.Run("window-reducer/"+function, func(b *testing.B) {
			at := time.Unix(1700002800, 0)
			series := map[string]manualSeriesSamples{"one": {Metric: map[string]string{"k": "body"}, Samples: []rangeMetricSample{{ts: at.Add(-time.Second).UnixNano(), value: 1}}}}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := hitsRangeMetricSeriesBounded(function, series, at, at.Add(6*time.Hour), 100*time.Millisecond, 24*time.Hour, 1<<30, nil, ctx, maxMetricEvalSamples); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

}
