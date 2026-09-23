package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// bareParserCase is one bare (not outer-aggregated) parser range metric and the
// backend route expected to answer it.
type bareParserCase struct {
	query, fn            string
	window               time.Duration
	start, end           time.Time
	route                string // "stats", "hits" or "raw"
	wantStep, wantOffset string // checked when set; wantOffset "none" means absent
}

func newBareParserTestProxy(t testing.TB, backendURL, version string, streamFields []string) *Proxy {
	t.Helper()
	p, err := New(Config{BackendURL: backendURL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", StreamFields: streamFields})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if version != "" {
		p.storeBackendVersion(version, version)
	}
	return p
}

// runBareParserCases compares every case with the Loki reference and checks
// that exactly one backend route served it.
func runBareParserCases(t *testing.T, lines []slidingFixtureLine, version string, streamFields []string, cases []bareParserCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s..%s", tc.query, tc.start.Format("15:04:05"), tc.end.Format("15:04:05")), func(t *testing.T) {
			srv, fake := newSlidingFakeVL(t, lines)
			p := newBareParserTestProxy(t, srv.URL, version, streamFields)
			got := runSlidingQueryRange(t, p, tc.query, tc.start, tc.end, time.Minute, nil)
			assertSlidingSeriesEqual(t, tc.query, lokiSlidingReference(lines, tc.fn, true, 0, tc.start, tc.end, time.Minute, tc.window), got)

			stats, raw := fake.snapshot()
			hits := fake.hitsSnapshot()
			var served []slidingStatsCall
			switch tc.route {
			case "stats":
				if len(stats) != 1 || len(hits) != 0 || raw != 0 {
					t.Fatalf("expected one stats_query_range call, got stats=%+v hits=%+v raw=%d", stats, hits, raw)
				}
				if !strings.Contains(stats[0].query, "stats by (_stream)") {
					t.Fatalf("stats buckets must keep per-stream series, got query %q", stats[0].query)
				}
				served = stats
			case "hits":
				if len(hits) != 1 || len(stats) != 0 || raw != 0 {
					t.Fatalf("expected one /hits call, got stats=%+v hits=%+v raw=%d", stats, hits, raw)
				}
				served = hits
			case "raw":
				if raw == 0 || len(stats) != 0 || len(hits) != 0 {
					t.Fatalf("expected the raw evaluator, got stats=%+v hits=%+v raw=%d", stats, hits, raw)
				}
				return
			}
			if tc.wantStep != "" && served[0].step != tc.wantStep {
				t.Fatalf("expected %s buckets, got step=%q", tc.wantStep, served[0].step)
			}
			wantOffset := tc.wantOffset
			if wantOffset == "none" {
				wantOffset = ""
			}
			if tc.wantOffset != "" && served[0].offset != wantOffset {
				t.Fatalf("expected offset %q, got %q", wantOffset, served[0].offset)
			}
		})
	}
}

// tickLines writes one line every 10s on whole 10s marks for two streams, so
// every minute window edge holds a line, with line sizes that vary per line.
func tickLines(s0 time.Time, shift time.Duration) []slidingFixtureLine {
	var lines []slidingFixtureLine
	for i := 0; i < 180; i++ {
		ts := s0.Add(time.Duration(i)*10*time.Second + shift).UnixNano()
		lines = append(lines,
			slidingFixtureLine{ts: ts, app: "bare-a", msg: "tick" + strings.Repeat("x", i%4)},
			slidingFixtureLine{ts: ts, app: "bare-b", msg: fmt.Sprintf("n=%d", i%7)})
	}
	return lines
}

// numberLines keeps the lines that carry the unwrap field n; Loki rejects
// unwrap over lines without it.
func numberLines(lines []slidingFixtureLine) []slidingFixtureLine {
	var out []slidingFixtureLine
	for _, line := range lines {
		if _, ok := slidingLogfmtFields(line.msg)["n"]; ok {
			out = append(out, line)
		}
	}
	return out
}

// Bare parser range metrics served from stats_query_range buckets must follow
// Loki's (t-range, t] windows for sliding (range != step) and tumbling
// (range == step) evaluation, with one series per stream.
func TestBareParserRangeMetric_StatsBucketsMatchLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC() // minute-aligned
	lines := tickLines(s0, 0)
	aligned, alignedEnd := s0.Add(5*time.Minute), s0.Add(25*time.Minute)
	full, fullEnd := s0.Add(-2*time.Minute), s0.Add(33*time.Minute)
	unaligned, unalignedEnd := s0.Add(30*time.Second), s0.Add(20*time.Minute+30*time.Second)
	runBareParserCases(t, lines, "v1.50.0", nil, []bareParserCase{
		{query: `count_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "count_over_time", window: 90 * time.Second, start: aligned, end: alignedEnd, route: "stats", wantStep: "30s", wantOffset: "-1ns"},
		{query: `count_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "count_over_time", window: 90 * time.Second, start: full, end: fullEnd, route: "stats"},
		{query: `bytes_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "bytes_over_time", window: 90 * time.Second, start: full, end: fullEnd, route: "stats", wantStep: "30s"},
		{query: `rate({app=~"bare-.*"} | logfmt [2m])`, fn: "rate", window: 2 * time.Minute, start: full, end: fullEnd, route: "stats", wantStep: "60s", wantOffset: "-1ns"},
		{query: `rate({app=~"bare-.*"} | logfmt [2m])`, fn: "rate", window: 2 * time.Minute, start: unaligned, end: unalignedEnd, route: "stats", wantOffset: "-30000000001ns"},
		{query: `bytes_rate({app=~"bare-.*"} | logfmt [2m])`, fn: "bytes_rate", window: 2 * time.Minute, start: full, end: fullEnd, route: "stats"},
		{query: `count_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "count_over_time", window: 90 * time.Second, start: unaligned, end: unalignedEnd, route: "stats", wantOffset: "-1ns"},
		// range == step: Loki's (t-1m, t], not VictoriaLogs' [t, t+1m) buckets.
		{query: `count_over_time({app=~"bare-.*"} | logfmt [1m])`, fn: "count_over_time", window: time.Minute, start: full, end: fullEnd, route: "stats", wantStep: "60s", wantOffset: "-1ns"},
		{query: `bytes_over_time({app=~"bare-.*"} | logfmt [1m])`, fn: "bytes_over_time", window: time.Minute, start: unaligned, end: unalignedEnd, route: "stats", wantOffset: "-30000000001ns"},
		{query: `count_over_time({app=~"bare-.*"} | logfmt | drop __error__ [1m])`, fn: "count_over_time", window: time.Minute, start: full, end: fullEnd, route: "stats"},
		// range < step keeps the raw evaluator.
		{query: `count_over_time({app=~"bare-.*"} | logfmt [30s])`, fn: "count_over_time", window: 30 * time.Second, start: full, end: fullEnd, route: "raw"},
	})
}

// Declared stream fields serve bare count_over_time and rate sliding windows
// from /hits, which must use the same anchored buckets.
func TestBareParserRangeMetric_HitsBucketsMatchLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := tickLines(s0, 0)
	full, fullEnd := s0.Add(-2*time.Minute), s0.Add(33*time.Minute)
	unaligned, unalignedEnd := s0.Add(30*time.Second), s0.Add(20*time.Minute+30*time.Second)
	runBareParserCases(t, lines, "v1.50.0", []string{"app"}, []bareParserCase{
		{query: `count_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "count_over_time", window: 90 * time.Second, start: full, end: fullEnd, route: "hits", wantStep: "30s", wantOffset: "-1ns"},
		{query: `rate({app=~"bare-.*"} | logfmt [2m])`, fn: "rate", window: 2 * time.Minute, start: unaligned, end: unalignedEnd, route: "hits", wantStep: "60s", wantOffset: "-30000000001ns"},
		{query: `count_over_time({app=~"bare-.*"} | logfmt [2m])`, fn: "count_over_time", window: 2 * time.Minute, start: full, end: fullEnd, route: "hits"},
		// Byte metrics are not served by /hits.
		{query: `bytes_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "bytes_over_time", window: 90 * time.Second, start: full, end: fullEnd, route: "stats", wantStep: "30s"},
	})
}

// Unwrap aggregations that compose from bucket values must also use windows
// that are exact bucket unions.
func TestBareParserRangeMetric_UnwrapBucketsMatchLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var lines []slidingFixtureLine
	for _, line := range numberLines(tickLines(s0, 0)) {
		lines = append(lines, line, slidingFixtureLine{ts: line.ts, app: "bare-c", msg: "n=" + fmt.Sprint(line.ts/int64(time.Second)%11)})
	}
	full, fullEnd := s0.Add(-2*time.Minute), s0.Add(33*time.Minute)
	unaligned, unalignedEnd := s0.Add(30*time.Second), s0.Add(20*time.Minute+30*time.Second)
	var cases []bareParserCase
	for _, fn := range []string{"sum_over_time", "max_over_time", "min_over_time"} {
		cases = append(cases,
			bareParserCase{query: fn + `({app=~"bare-.*"} | logfmt | unwrap n [90s])`, fn: fn, window: 90 * time.Second, start: full, end: fullEnd, route: "stats", wantStep: "30s", wantOffset: "-1ns"},
			bareParserCase{query: fn + `({app=~"bare-.*"} | logfmt | unwrap n [1m])`, fn: fn, window: time.Minute, start: unaligned, end: unalignedEnd, route: "stats", wantOffset: "-30000000001ns"},
			bareParserCase{query: fn + `({app=~"bare-.*"} | logfmt | unwrap n [30s])`, fn: fn, window: 30 * time.Second, start: full, end: fullEnd, route: "stats", wantStep: "30s"},
		)
	}
	runBareParserCases(t, lines, "v1.50.0", nil, cases)
}

// Data gaps stay absent, and byte windows that hold only empty lines are
// present zero-valued samples, not gaps.
func TestBareParserRangeMetric_GapsAndEmptyLines(t *testing.T) {
	hour0 := time.Unix(1699999200, 0).UTC()
	var lines []slidingFixtureLine
	for _, h := range []int{0, 5} {
		for i := 0; i < 60; i++ {
			msg := strings.Repeat("y", 3+i%5)
			if i >= 20 && i < 30 {
				msg = "" // ten minutes of empty lines
			}
			lines = append(lines, slidingFixtureLine{ts: hour0.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Minute).UnixNano(), app: "gap-bare", msg: msg})
		}
	}
	start, end := hour0, hour0.Add(8*time.Hour)
	for _, streamFields := range [][]string{nil, {"app"}} {
		t.Run(fmt.Sprintf("streamFields=%v", streamFields), func(t *testing.T) {
			countRoute := "stats"
			if streamFields != nil {
				countRoute = "hits"
			}
			var cases []bareParserCase
			for _, window := range []time.Duration{90 * time.Second, 5 * time.Minute} {
				expr := fmt.Sprintf("[%ds]", int(window.Seconds()))
				cases = append(cases,
					bareParserCase{query: `count_over_time({app="gap-bare"} | logfmt ` + expr + `)`, fn: "count_over_time", window: window, start: start, end: end, route: countRoute},
					bareParserCase{query: `bytes_over_time({app="gap-bare"} | logfmt ` + expr + `)`, fn: "bytes_over_time", window: window, start: start, end: end, route: "stats"},
					bareParserCase{query: `bytes_rate({app="gap-bare"} | logfmt ` + expr + `)`, fn: "bytes_rate", window: window, start: start, end: end, route: "stats"},
				)
			}
			// The reference must hold zero-valued byte samples and gap steps.
			ref := lokiSlidingReference(lines, "bytes_over_time", true, 0, start, end, time.Minute, 90*time.Second)
			zero := 0
			for _, v := range ref["gap-bare"] {
				if v == "0" {
					zero++
				}
			}
			if zero == 0 || len(ref["gap-bare"]) >= int(end.Sub(start)/time.Minute) {
				t.Fatalf("fixture must exercise empty-line windows and gaps: %d zero samples of %d", zero, len(ref["gap-bare"]))
			}
			runBareParserCases(t, lines, "v1.50.0", streamFields, cases)
		})
	}
}

// VictoriaLogs before v1.45 ignores the offset arg, and an unknown version is
// treated the same: epoch-aligned grids keep buckets without offset (fixture
// lines sit off the window edges), other grids use the raw evaluator.
func TestBareParserRangeMetric_BackendWithoutOffsetSupport(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := tickLines(s0, 3*time.Second)
	aligned, alignedEnd := s0.Add(5*time.Minute), s0.Add(20*time.Minute)
	unaligned, unalignedEnd := s0.Add(5*time.Minute+30*time.Second), s0.Add(20*time.Minute+30*time.Second)
	for _, version := range []string{"v1.44.0", ""} {
		for _, streamFields := range [][]string{nil, {"app"}} {
			t.Run(fmt.Sprintf("version=%q/streamFields=%v", version, streamFields), func(t *testing.T) {
				countRoute := "stats"
				if streamFields != nil {
					countRoute = "hits"
				}
				// An unknown version (failed or pending probe) keeps anchored
				// buckets: a backend that ignores the arg has the epoch-aligned
				// buckets it would have had anyway, while raw rows would scale
				// the query with the stored data.
				unalignedRoute, unalignedStep, unalignedOffset := "raw", "", ""
				alignedOffset, gcdOffset := "none", ""
				if version == "" {
					unalignedRoute, unalignedStep, unalignedOffset = "stats", "60s", "-30000000001ns"
					alignedOffset, gcdOffset = "-1ns", "-10000000001ns"
				}
				runBareParserCases(t, lines, version, streamFields, []bareParserCase{
					{query: `count_over_time({app=~"bare-.*"} | logfmt [2m])`, fn: "count_over_time", window: 2 * time.Minute, start: aligned, end: alignedEnd, route: countRoute, wantStep: "60s", wantOffset: alignedOffset},
					{query: `count_over_time({app=~"bare-.*"} | logfmt [2m])`, fn: "count_over_time", window: 2 * time.Minute, start: unaligned, end: unalignedEnd, route: map[bool]string{true: countRoute, false: "raw"}[version == ""], wantStep: unalignedStep, wantOffset: unalignedOffset},
					{query: `bytes_over_time({app=~"bare-.*"} | logfmt [1m])`, fn: "bytes_over_time", window: time.Minute, start: aligned, end: alignedEnd, route: "stats", wantStep: "60s", wantOffset: alignedOffset},
					{query: `bytes_over_time({app=~"bare-.*"} | logfmt [90s])`, fn: "bytes_over_time", window: 90 * time.Second, start: s0.Add(5*time.Minute + 10*time.Second), end: alignedEnd, route: map[bool]string{true: "stats", false: "raw"}[version == ""], wantStep: map[bool]string{true: "30s", false: ""}[version == ""], wantOffset: gcdOffset},
				})
				runBareParserCases(t, numberLines(lines), version, streamFields, []bareParserCase{
					{query: `sum_over_time({app=~"bare-.*"} | logfmt | unwrap n [2m])`, fn: "sum_over_time", window: 2 * time.Minute, start: aligned, end: alignedEnd, route: "stats", wantStep: "60s", wantOffset: alignedOffset},
					{query: `max_over_time({app=~"bare-.*"} | logfmt | unwrap n [2m])`, fn: "max_over_time", window: 2 * time.Minute, start: unaligned, end: unalignedEnd, route: unalignedRoute, wantStep: unalignedStep, wantOffset: unalignedOffset},
				})
			})
		}
	}
}

// Streams whose labels translate to one series are merged; first/last over the
// merged buckets must still follow time order.
func TestBareParserRangeMetric_MergedStreamsKeepTimeOrder(t *testing.T) {
	t0 := int64(1700000040) // minute-aligned
	// VictoriaLogs has no first/last stats function, so first_over_time and
	// last_over_time are evaluated from raw rows. Rows of two streams that merge
	// into one series arrive out of time order.
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Errorf("unexpected backend path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/stream+json")
		fmt.Fprintf(w, `{"_time":%q,"_stream":"{app=\"merged\"}","_msg":"n=1","app":"merged","n":"1"}`+"\n",
			time.Unix(t0+60, 1).UTC().Format(time.RFC3339Nano))
		fmt.Fprintf(w, `{"_time":%q,"_stream":"{app=\"merged\",service_name=\"merged\"}","_msg":"n=5","app":"merged","service_name":"merged","n":"5"}`+"\n",
			time.Unix(t0, 1).UTC().Format(time.RFC3339Nano))
	}))
	defer vl.Close()
	p := newBareParserTestProxy(t, vl.URL, "v1.50.0", nil)
	eval := time.Unix(t0+120, 0)
	for fn, want := range map[string]string{"last_over_time": "1", "first_over_time": "5"} {
		query := fn + `({app="merged"} | logfmt | unwrap n [2m])`
		got := runSlidingQueryRange(t, p, query, eval, eval, time.Minute, nil)
		if v := got["merged"][eval.Unix()]; v != want {
			t.Fatalf("%s: got %q, want %q (%v)", query, v, want, got)
		}
	}
}

// Bare `| json` range metrics are evaluated by the ordered JSON evaluator from
// raw lines, which already follows Loki's windows.
func TestBareParserRangeMetric_JSONUsesExactEvaluator(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 180; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 10 * time.Second).UnixNano(), app: "bare-json", msg: `{"n":1}`})
	}
	full, fullEnd := s0.Add(-2*time.Minute), s0.Add(33*time.Minute)
	var cases []bareParserCase
	for _, fn := range []string{"count_over_time", "bytes_over_time", "rate", "bytes_rate"} {
		cases = append(cases,
			bareParserCase{query: fn + `({app="bare-json"} | json [90s])`, fn: fn, window: 90 * time.Second, start: full, end: fullEnd, route: "raw"},
			bareParserCase{query: fn + `({app="bare-json"} | json [1m])`, fn: fn, window: time.Minute, start: full, end: fullEnd, route: "raw"},
		)
	}
	runBareParserCases(t, lines, "v1.50.0", nil, cases)
}

// BenchmarkBareParserRangeMetric measures a day of 60s steps for sliding and
// tumbling bare parser metrics served from stats buckets.
func BenchmarkBareParserRangeMetric(b *testing.B) {
	s0 := time.Unix(1700006400, 0).UTC() // day-aligned
	var lines []slidingFixtureLine
	for i := 0; i < 8640; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i)*10*time.Second + 3*time.Second).UnixNano(), app: "bench-bare", msg: fmt.Sprintf("n=%d", i%7)})
	}

	for _, query := range []string{
		`count_over_time({app="bench-bare"} | logfmt [5m])`,
		`bytes_over_time({app="bench-bare"} | logfmt [90s])`,
		`count_over_time({app="bench-bare"} | logfmt [1m])`,
		`sum_over_time({app="bench-bare"} | logfmt | unwrap n [5m])`,
		`max_over_time({app="bench-bare"} | logfmt | unwrap n [90s])`,
	} {
		b.Run(query, func(b *testing.B) {
			srv, _ := newSlidingFakeVL(b, lines)
			defer srv.Close()
			p := newBareParserTestProxy(b, srv.URL, "v1.50.0", nil)
			start, end := s0.Add(time.Hour), s0.Add(24*time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A distinct end per iteration keeps the response cache out of the loop.
				got := runSlidingQueryRange(b, p, query, start, end.Add(-time.Duration(i%1000)*time.Minute), time.Minute, nil)
				if len(got) != 1 {
					b.Fatalf("expected one series, got %d", len(got))
				}
			}
		})
	}
}
