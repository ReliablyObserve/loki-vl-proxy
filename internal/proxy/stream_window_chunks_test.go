package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// shortLivedStreamRows are the bench/ab seed's shape: every 10s each of four services starts a new pod stream whose
// three lines sit half a second past the batch second, so a day holds hundreds of thousands of streams that live a
// few seconds each and a step's window holds a few dozen per service.
func shortLivedStreamRows(start, end int64) []vlemu.Row {
	var rows []vlemu.Row
	batch := 0
	for at := start; at < end; at += int64(10 * time.Second) {
		for svc := 0; svc < 4; svc++ {
			// Service s3 starts no pod in every fifth batch, so the counts move between steps.
			if svc == 3 && batch%5 == 0 {
				continue
			}
			stream := fmt.Sprintf(`{env="production",service_name="s%d",pod="p%d-%d"}`, svc, svc, batch)
			for line := int64(0); line < 3; line++ {
				rows = append(rows, vlemu.Row{TS: at + 500_000_000 + line*1_000, Stream: stream, Value: 1})
			}
		}
		batch++
	}
	return rows
}

// activeStreamsPerStep is Loki's answer to `count by (service_name) (count_over_time(sel[window]))` over rows: per
// step T, the streams of each service with a line in (T-window, T]; divide is applied to each value (300 for the
// rate/count ratio, 1 for the count).
func activeStreamsPerStep(rows []vlemu.Row, start, end int64, step, window time.Duration, divide float64) map[string]map[int64]float64 {
	want := map[string]map[int64]float64{}
	for ts := start; ts <= end; ts += int64(step) {
		active := map[string]map[string]bool{}
		for _, row := range rows {
			if row.TS > ts-int64(window) && row.TS <= ts {
				svc := vlemu.StreamLabels(row.Stream)["service_name"]
				if active[svc] == nil {
					active[svc] = map[string]bool{}
				}
				active[svc][row.Stream] = true
			}
		}
		for svc, streams := range active {
			key := fmt.Sprintf(`{"service_name":%q}`, svc)
			if want[key] == nil {
				want[key] = map[int64]float64{}
			}
			want[key][ts/1e9] = float64(len(streams)) / divide
		}
	}
	return want
}

// TestStreamIdentityWindowsChunkedPastBodyBound is B2 of bench/ab (aggregation-over-binary) and the count by it is
// read as, at 24h: the per-stream buckets of many short-lived streams over the whole range passed the stats body
// bound (64 MiB, maxStatsBucketResponseBytes) and the request answered 500 ResourceExhausted, where the answer is a
// few groups per step. The windows are now read in time chunks after that read, each folded as it is read
// (listed_window_chunks.go), and the answer is Loki's: every value equal to the active streams counted from the
// rows. A repeat on the same proxy reads in chunks at once (no read over the bound), and a request whose read fits the
// bound is not chunked.
//
// conformance: semantics/series-level-aggregation-bounded-memory, semantics/aggregation-over-binary-dropped, metric-series-identity, loki_api_v1_query_range
func TestStreamIdentityWindowsChunkedPastBodyBound(t *testing.T) {
	const (
		step   = 60 * time.Second
		window = 5 * time.Minute
	)
	end := binaryAggregationAt
	start := end - int64(3*time.Hour)
	rows := shortLivedStreamRows(start-int64(window)-int64(time.Minute), end)
	for _, tc := range []struct {
		query  string
		divide float64
	}{
		{`sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`, window.Seconds()},
		{`count by (service_name) (count_over_time({env="production"}[5m]))`, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			want := activeStreamsPerStep(rows, start, end, step, window, tc.divide)
			emu := &vlemu.Emulator{T: t, Rows: rows, Version: "v1.54.0"}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			// The series limit of the bench/ab and e2e stacks (-max-stats-query-series=1000000): the read holds a series
			// per stream before the fold. The buffered bound keeps the raw rows under it, as a day of the seed's lines is
			// under the 64 MiB default: the folded answer fits it, the raw rows do not.
			p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
				BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: 1_000_000,
				ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 128 << 10}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}

			// The whole range's read fits the default bound: one read, no chunks.
			var chunks []statsReadMeter
			listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) { chunks = append(chunks, m) }
			defer func() { listedWindowChunksObserve = nil }()
			whole := streamWindowAnswer(t, p, tc.query, start, end-int64(time.Hour), step)
			if len(chunks) != 0 {
				t.Fatalf("a read within the bound was chunked: %d chunks", len(chunks))
			}
			compareBinaryAggregation(t, tc.query+" (within the bound)", whole, trimSteps(want, end-int64(time.Hour)))

			// A bound the whole range's buckets pass and a quarter of the steps' buckets do not.
			old := maxStatsBucketResponseBytes
			maxStatsBucketResponseBytes = streamWindowBound(t, emu, rows)
			defer func() { maxStatsBucketResponseBytes = old }()
			p.cache.PurgeAll() // no answer or bucket of the first request is reused
			chunks = nil
			got := streamWindowAnswer(t, p, tc.query, start, end, step)
			compareBinaryAggregation(t, tc.query, got, want)
			if len(chunks) < 2 {
				t.Fatalf("the read past the bound was not chunked: %d chunks", len(chunks))
			}
			for i, m := range chunks {
				if m.overBytes {
					t.Fatalf("chunk %d of %d passed the bound: %+v", i, len(chunks), m)
				}
			}

			// The repeat on a grid half a step later (no answer, step or bucket of the request before is reusable):
			// chunked at once, so no read is thrown away.
			chunks = nil
			before := len(emu.EndpointCalls("/select/logsql/stats_query_range"))
			shift := int64(step / 2)
			moved := streamWindowAnswer(t, p, tc.query, start+shift, end-shift, step)
			compareBinaryAggregation(t, tc.query+" (half a step later)", moved, activeStreamsPerStep(rows, start+shift, end-shift, step, window, tc.divide))
			if len(chunks) < 2 || len(emu.EndpointCalls("/select/logsql/stats_query_range"))-before != len(chunks) {
				t.Fatalf("the repeat read %d chunks in %d stats calls, want only its chunks", len(chunks), len(emu.EndpointCalls("/select/logsql/stats_query_range"))-before)
			}
		})
	}
}

// streamWindowAnswer is the answer to query over [start, end] at step: 200 or the test fails.
func streamWindowAnswer(t *testing.T, p *Proxy, query string, start, end int64, step time.Duration) map[string]map[int64]float64 {
	t.Helper()
	got, body := binaryAggregationAnswerVia(t, p, "0", query, start, end, step)
	if got == nil {
		t.Fatalf("%s over [%d, %d]: %.300s", query, start/1e9, end/1e9, body)
	}
	return got
}

// streamWindowBound is a stats body bound between what a quarter of the request's buckets and the whole request's
// take: half of the whole read, which the emulator answers for the rows' stream series.
func streamWindowBound(t *testing.T, emu *vlemu.Emulator, rows []vlemu.Row) int64 {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	params := url.Values{"query": {`env:="production" | stats by (service_name, _stream) count() as c`},
		"start": {strconv.FormatInt(rows[0].TS/1e9, 10)}, "end": {strconv.FormatInt(rows[len(rows)-1].TS/1e9+1, 10)}, "step": {"60s"}}
	res, err := http.PostForm(backend.URL+"/select/logsql/stats_query_range", params)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := readBodyLimited(res.Body, 64<<20)
	if err != nil || res.StatusCode != http.StatusOK || len(body) < 100_000 {
		t.Fatalf("whole read: %d bytes, status %d, %v", len(body), res.StatusCode, err)
	}
	return int64(len(body)) / 2
}

// trimSteps keeps the points of want up to end.
func trimSteps(want map[string]map[int64]float64, end int64) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for key, points := range want {
		for ts, v := range points {
			if ts <= end/1e9 {
				if out[key] == nil {
					out[key] = map[int64]float64{}
				}
				out[key][ts] = v
			}
		}
	}
	return out
}
