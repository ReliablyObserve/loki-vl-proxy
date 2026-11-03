package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// lokiStepAlignNanos is Loki's StepAlignMiddleware
// (pkg/querier/queryrange/queryrangebase/step_align.go:20-24) applied to one
// bound, with the step in milliseconds as the codec keeps it
// (pkg/querier/queryrange/codec.go:436), returned in nanoseconds.
func lokiStepAlignNanos(t time.Time, stepMs int64) int64 {
	return (t.UnixMilli() / stepMs) * stepMs * int64(time.Millisecond)
}

// The bounds of a metric query_range request move down to multiples of the
// step exactly as Loki's step-align middleware moves them, and only the
// requests Loki aligns (metric range queries with a step that pass decoding)
// are moved.
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestAlignQueryRangeToStep_LokiStepAlignMiddleware(t *testing.T) {
	now := time.Unix(1700003600, 123456789)
	metric := `sum(count_over_time({app="a"}[5m]))`
	ns := func(sec int64) string { return strconv.FormatInt(sec*int64(time.Second), 10) }
	cases := []struct {
		name       string
		params     url.Values
		header     map[string]string
		post       bool
		aligned    bool
		start, end int64 // expected Unix nanoseconds when aligned
		stepMs     int64 // step Loki aligns to, to cross-check start and end
	}{
		{
			// 1700000000 mod 137 = 17 and 1700003600 mod 137 = 55: both
			// bounds go down, the first point lands 17s before the start.
			name:    "unaligned start and end at step 137s",
			params:  url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}},
			aligned: true, start: 1699999983 * 1e9, end: 1700003545 * 1e9, stepMs: 137000,
		},
		{
			name:    "nanosecond bounds and a duration step: milliseconds first, then down",
			params:  url.Values{"query": {metric}, "start": {"1700000012500000000"}, "end": {"1700000999999999999"}, "step": {"1m"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700000940 * 1e9, stepMs: 60000,
		},
		{
			name:    "Grafana's step form, 60000ms",
			params:  url.Values{"query": {metric}, "start": {"1700000012500000000"}, "end": {"1700000999999999999"}, "step": {"60000ms"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700000940 * 1e9, stepMs: 60000,
		},
		{
			name:    "already on the grid: unchanged",
			params:  url.Values{"query": {metric}, "start": {ns(1699999980)}, "end": {ns(1700000940)}, "step": {"60"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700000940 * 1e9, stepMs: 60000,
		},
		{
			name:    "a step that does not divide the range",
			params:  url.Values{"query": {metric}, "start": {ns(1700000003)}, "end": {ns(1700000100)}, "step": {"7s"}},
			aligned: true, start: 1700000001 * 1e9, end: 1700000099 * 1e9, stepMs: 7000,
		},
		{
			name:    "an end in the future goes down like any other",
			params:  url.Values{"query": {metric}, "start": {ns(1700000001)}, "end": {ns(1700010001)}, "step": {"60"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700010000 * 1e9, stepMs: 60000,
		},
		{
			name:    "a fractional-millisecond step aligns on its whole milliseconds",
			params:  url.Values{"query": {metric}, "start": {"1700000000000400000"}, "end": {"1700000000010700000"}, "step": {"0.0015"}},
			aligned: true, start: 1700000000000 * 1e6, end: 1700000000010 * 1e6, stepMs: 1,
		},
		{
			name:    "binary expression",
			params:  url.Values{"query": {`sum(rate({app="a"}[5m])) / sum(rate({app="a"}[1m]))`}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}},
			aligned: true, start: 1699999983 * 1e9, end: 1700003545 * 1e9, stepMs: 137000,
		},
		{
			name:    "vector literal",
			params:  url.Values{"query": {`vector(1)`}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}},
			aligned: true, start: 1699999983 * 1e9, end: 1700003545 * 1e9, stepMs: 137000,
		},
		{
			name:    "no start: Loki's end minus since, then aligned",
			params:  url.Values{"query": {metric}, "end": {ns(1700003600)}, "since": {"2h"}, "step": {"137"}},
			aligned: true, start: (1700003600 - 7200) / 137 * 137 * 1e9, end: 1700003545 * 1e9, stepMs: 137000,
		},
		{
			name:    "no start and no end: now minus 1h and now, then aligned",
			params:  url.Values{"query": {metric}, "step": {"60"}},
			aligned: true, start: lokiStepAlignNanos(now.Add(-time.Hour), 60000), end: lokiStepAlignNanos(now, 60000), stepMs: 60000,
		},
		{
			name:   "POST form",
			params: url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}},
			post:   true, aligned: true, start: 1699999983 * 1e9, end: 1700003545 * 1e9, stepMs: 137000,
		},
		{
			name:    "start on the grid, end off it: only the end moves",
			params:  url.Values{"query": {metric}, "start": {ns(1699999980)}, "end": {ns(1700003600)}, "step": {"60"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700003580 * 1e9, stepMs: 60000,
		},
		{
			name:    "start off the grid, end on it: only the start moves",
			params:  url.Values{"query": {metric}, "start": {ns(1700000017)}, "end": {ns(1700003580)}, "step": {"60"}},
			aligned: true, start: 1699999980 * 1e9, end: 1700003580 * 1e9, stepMs: 60000,
		},
		{
			name:    "half-second step on nanosecond bounds",
			params:  url.Values{"query": {metric}, "start": {"1700000000700000000"}, "end": {"1700000060250000000"}, "step": {"0.5"}},
			aligned: true, start: 1700000000500 * 1e6, end: 1700000060000 * 1e6, stepMs: 500,
		},
		{
			name:    "250ms step in duration form",
			params:  url.Values{"query": {metric}, "start": {"1700000000300000000"}, "end": {"1700000001999999999"}, "step": {"250ms"}},
			aligned: true, start: 1700000000250 * 1e6, end: 1700000001750 * 1e6, stepMs: 250,
		},
		{
			// Loki rounds the fraction of a decimal bound to milliseconds
			// (pkg/loghttp/params.go:166-171): .3 is .300, not the float's
			// .29999..., which a 100ms grid would move down to .200; .9996 is
			// the next whole second.
			name:    "decimal-second bounds are rounded to milliseconds first, as Loki does",
			params:  url.Values{"query": {metric}, "start": {"1700000000.3"}, "end": {"1700000010.9996"}, "step": {"0.1"}},
			aligned: true, start: 1700000000300 * 1e6, end: 1700000011000 * 1e6, stepMs: 100,
		},
		{
			name:    "step larger than the range: both bounds land on one grid point",
			params:  url.Values{"query": {metric}, "start": {ns(1700000010)}, "end": {ns(1700000030)}, "step": {"10m"}},
			aligned: true, start: 1699999800 * 1e9, end: 1699999800 * 1e9, stepMs: 600000,
		},
		{
			name:    "start equal to end",
			params:  url.Values{"query": {metric}, "start": {ns(1700000017)}, "end": {ns(1700000017)}, "step": {"60"}},
			aligned: true, start: 1699999980 * 1e9, end: 1699999980 * 1e9, stepMs: 60000,
		},
		{
			// Go's integer division truncates toward zero, so Loki moves a
			// bound before 1970 up, not down; the proxy uses the same formula.
			name:    "a bound before the epoch moves toward zero, as Loki's integer division does",
			params:  url.Values{"query": {metric}, "start": {"-90"}, "end": {"30"}, "step": {"60"}},
			aligned: true, start: -60 * 1e9, end: 0, stepMs: 60000,
		},
		{
			// Alignment is on Unix epoch milliseconds: a zone offset, or the
			// day a zone changes to summer time, does not move the grid.
			name:    "RFC3339 bounds with a zone offset on a DST change day align in UTC",
			params:  url.Values{"query": {metric}, "start": {"2026-03-29T03:00:17+02:00"}, "end": {"2026-03-29T04:00:59.999+02:00"}, "step": {"60"}},
			aligned: true, start: time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC).UnixNano(), end: time.Date(2026, 3, 29, 2, 0, 0, 0, time.UTC).UnixNano(), stepMs: 60000,
		},
		{name: "log query", params: url.Values{"query": {`{app="a"} |= "x"`}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}}},
		{name: "no step", params: url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700003600)}}},
		{name: "step under a millisecond", params: url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700000000)}, "step": {"0.0001"}}},
		{name: "over 11,000 points: Loki answers 400 first", params: url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700000000 + 11001)}, "step": {"1"}}},
		{name: "start after end", params: url.Values{"query": {metric}, "start": {ns(1700003600)}, "end": {ns(1700000000)}, "step": {"137"}}},
		// Loki reads the end as nanoseconds (1677-09-21) and derives the
		// start an hour earlier, before the first instant int64 nanoseconds
		// hold; it aligns on time.Time and answers. The proxy cannot write
		// that start, so the request goes on as it came instead of wrapping
		// to 2262.
		{name: "derived start before the int64 nanosecond range", params: url.Values{"query": {metric}, "end": {"-9223372036854775000"}, "step": {"1"}}},
		// Bounds after 2262-04-11 or before 1677-09-21: Loki keeps them as a
		// time.Time; the proxy's readers would wrap them to the other end of
		// the int64 nanosecond range, so alignment leaves the request.
		{name: "decimal seconds after the int64 nanosecond range", params: url.Values{"query": {metric}, "start": {"9223372036.855"}, "step": {"31536000"}}},
		{name: "integer seconds after the int64 nanosecond range", params: url.Values{"query": {metric}, "start": {"9223372037"}, "end": {"9223372038"}, "step": {"60"}}},
		{name: "integer milliseconds after the int64 nanosecond range", params: url.Values{"query": {metric}, "start": {"1700000000000"}, "end": {"9223372036855"}, "step": {"31536000"}}},
		{name: "integer seconds before the int64 nanosecond range", params: url.Values{"query": {metric}, "start": {"-9223372037"}, "end": {"0"}, "step": {"31536000"}}},
		{name: "the minimum int64", params: url.Values{"query": {metric}, "start": {"-9223372036854775808"}, "end": {"0"}, "step": {"31536000"}}},
		{name: "unparsable query", params: url.Values{"query": {`sum(count_over_time({app="a"}[5m])`}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}}},
		{name: "unresolved Grafana token", params: url.Values{"query": {`sum(count_over_time({app="a"}[$__auto]))`}, "start": {ns(1700000000)}, "end": {ns(1700003600)}, "step": {"137"}}},
		{
			name:   "Grafana query-split residual chunk",
			params: url.Values{"query": {metric}, "start": {ns(1700000000)}, "end": {ns(1700000030)}, "step": {"60"}},
			header: map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.post {
				req = httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(tc.params.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			} else {
				req = httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+tc.params.Encode(), nil)
			}
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			got, ok := alignQueryRangeToStep(req, now)
			if ok != tc.aligned {
				t.Fatalf("aligned = %v, want %v", ok, tc.aligned)
			}
			if !ok {
				if got != req || requestStepAligned(got) {
					t.Fatal("a request Loki does not align must go on as it came")
				}
				return
			}
			if !requestStepAligned(got) {
				t.Fatal("an aligned request must be marked")
			}
			rawStart, rawEnd, _ := lokiRangeBounds(req, now)
			if want := lokiStepAlignNanos(rawStart, tc.stepMs); want != tc.start {
				t.Fatalf("table start %d is not Loki's %d", tc.start, want)
			}
			if want := lokiStepAlignNanos(rawEnd, tc.stepMs); want != tc.end {
				t.Fatalf("table end %d is not Loki's %d", tc.end, want)
			}
			wantStart, wantEnd := encodeAlignedBound(tc.start), encodeAlignedBound(tc.end)
			query := got.URL.Query()
			for _, read := range []struct {
				where      string
				start, end string
			}{
				{"Form", got.FormValue("start"), got.FormValue("end")},
				{"URL", query.Get("start"), query.Get("end")},
			} {
				if read.start != wantStart || read.end != wantEnd {
					t.Fatalf("%s bounds = [%s, %s], want [%s, %s]", read.where, read.start, read.end, wantStart, wantEnd)
				}
				// The handler reads the moved bounds back as the same instants.
				if s, ok := parseLokiTimeToUnixNano(read.start); !ok || s != tc.start {
					t.Fatalf("%s start %q reads back as %d, want %d", read.where, read.start, s, tc.start)
				}
				if e, ok := parseLokiTimeToUnixNano(read.end); !ok || e != tc.end {
					t.Fatalf("%s end %q reads back as %d, want %d", read.where, read.end, e, tc.end)
				}
			}
			if tc.post && (got.PostForm.Get("start") != wantStart || got.PostForm.Get("end") != wantEnd) {
				t.Fatalf("PostForm bounds = [%s, %s], want [%s, %s]", got.PostForm.Get("start"), got.PostForm.Get("end"), wantStart, wantEnd)
			}
			if req.FormValue("start") != tc.params.Get("start") || req.FormValue("end") != tc.params.Get("end") {
				t.Fatal("the caller's request must not be modified")
			}
		})
	}
}

// The middleware only runs for query_range with the flag on: instant queries
// and every other endpoint, and every request with the flag off, reach the
// handler as they came.
// conformance: semantics/query-range-step-alignment
func TestStepAlignMiddleware_OnlyQueryRangeWithTheFlag(t *testing.T) {
	params := url.Values{"query": {`sum(count_over_time({app="a"}[5m]))`}, "start": {"1700000000000000000"}, "end": {"1700003600000000000"}, "step": {"137"}, "time": {"1700003600000000000"}}
	for _, tc := range []struct {
		endpoint string
		on       bool
		moved    bool
	}{
		{"query_range", true, true},
		{"query_range", false, false},
		{"query", true, false},
		{"volume_range", true, false},
		{"patterns", true, false},
	} {
		p := &Proxy{alignQueriesWithStep: tc.on}
		var seen *http.Request
		h := p.stepAlignMiddleware(tc.endpoint, func(_ http.ResponseWriter, r *http.Request) { seen = r })
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+tc.endpoint+"?"+params.Encode(), nil)
		h(httptest.NewRecorder(), req)
		if moved := seen.FormValue("start") != params.Get("start"); moved != tc.moved {
			t.Fatalf("%s with the flag %v: moved = %v, want %v", tc.endpoint, tc.on, moved, tc.moved)
		}
	}
}

// A request alignment would leave where it is reaches the handler as it came:
// the same *http.Request, unmarked, with no allocation in the middleware. A
// bound off the grid, a missing bound, or an on-grid decimal the proxy reads
// differently from Loki is moved as before, and the caller's request is not
// written. A log query passes through unparsed.
// conformance: semantics/query-range-step-alignment
func TestStepAlignMiddleware_OnGridRequestsPassThrough(t *testing.T) {
	p := &Proxy{alignQueriesWithStep: true}
	metric := `sum by (app) (count_over_time({app=~".+"}[5m]))`
	for _, tc := range []struct {
		name               string
		query, start, end  string
		step               string
		post, passes, mark bool
	}{
		{"nanoseconds on the grid", metric, "1700000040000000000", "1700003640000000000", "60000ms", false, true, false},
		{"seconds on the grid", metric, "1700000040", "1700003640", "60", false, true, false},
		{"RFC3339 on the grid", metric, "2023-11-14T22:14:00Z", "2023-11-14T23:14:00Z", "1m", false, true, false},
		{"POST on the grid", metric, "1700000040000000000", "1700003640000000000", "60s", true, true, false},
		{"pre-epoch on the grid", metric, "-120", "60", "60", false, true, false},
		{"Grafana shape: end off the grid", metric, "1700000040000000000", "1700003647000000000", "60000ms", false, false, true},
		{"start off the grid", metric, "1700000053000000000", "1700003640000000000", "60000ms", false, false, true},
		{"POST off the grid", metric, "1700000053000000000", "1700003647000000000", "60s", true, false, true},
		{"no bounds: derived from now", metric, "", "", "60000ms", false, false, true},
		{"sub-millisecond past the grid", metric, "1700000040000500000", "1700003640000000000", "60000ms", false, false, true},
		// Loki rounds .9996 to the next whole second, on the grid, and the
		// proxy's parser rounds the fraction to the millisecond the same way
		// (lokiFloatSecondsToNanos), so the request is on the grid and passes
		// through. It used to read .9996 as sent and write the bound again.
		{"decimal Loki rounds onto the grid", metric, "1700000000.3", "1700000010.9996", "100ms", false, true, false},
		{"log query off the grid", `{app="a"} |= "x"`, "1700000053000000000", "1700003647000000000", "60000ms", false, true, false},
		{"no step", metric, "1700000053000000000", "1700003647000000000", "", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{"query": {tc.query}, "start": {tc.start}, "end": {tc.end}, "step": {tc.step}}
			for _, key := range []string{"start", "end", "step"} {
				if form.Get(key) == "" {
					form.Del(key)
				}
			}
			newReq := func() *http.Request {
				if tc.post {
					req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form.Encode()))
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					return req
				}
				return httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
			}
			var seen *http.Request
			h := p.stepAlignMiddleware("query_range", func(_ http.ResponseWriter, r *http.Request) { seen = r })
			req := newReq()
			rawQuery := req.URL.RawQuery
			h(httptest.NewRecorder(), req)
			if passes := seen == req; passes != tc.passes {
				t.Fatalf("passed through = %v, want %v", passes, tc.passes)
			}
			if requestStepAligned(seen) != tc.mark {
				t.Fatalf("marked aligned = %v, want %v", requestStepAligned(seen), tc.mark)
			}
			if req.FormValue("start") != tc.start || req.FormValue("end") != tc.end || req.URL.RawQuery != rawQuery || (tc.post && req.PostForm.Get("start") != tc.start) {
				t.Fatal("the caller's request was modified")
			}
			if !tc.passes {
				return
			}
			// What the middleware skips is exactly what alignment would leave:
			// the same instants, read back by the proxy's parser.
			if aligned, ok := alignQueryRangeToStep(newReq(), time.Now()); ok {
				for _, key := range []string{"start", "end"} {
					was, _ := parseLokiTimeToUnixNano(tc.start)
					if key == "end" {
						was, _ = parseLokiTimeToUnixNano(tc.end)
					}
					if now, _ := parseLokiTimeToUnixNano(aligned.FormValue(key)); now != was {
						t.Fatalf("skipped %s %d, alignment gives %d", key, was, now)
					}
				}
			}
			warm := newReq()
			h(httptest.NewRecorder(), warm) // memoizes the query's validation
			rec := httptest.NewRecorder()
			if allocs := testing.AllocsPerRun(50, func() { h(rec, warm) }); allocs > 0 {
				t.Fatalf("the middleware allocates %.0f times on a request it passes on", allocs)
			}
		})
	}
}

// The middleware leaves a query that starts with a stream selector without a
// lookup: no LogQL metric expression starts with one, and every such form,
// infix operators included, is rejected or is a log query.
// conformance: semantics/query-range-step-alignment
func TestStepAlign_StreamSelectorPrefixIsNeverMetric(t *testing.T) {
	for _, q := range []string{
		`{a="b"}`, `{a="b"} |= "x"`, `{a="b"} | json`, ` {a="b"} | logfmt | level="info"`,
		`{a="b"} + 1`, `{a="b"} > 0`, `{a="b"} or vector(0)`, `{a="b"} and {c="d"}`, `{a="b"} / {c="d"}`,
		`{a="b"} |= "x" + 1`, `{a="b"}[5m]`, `{a="b"} | json | unwrap x [5m]`, `{a="b"} offset 5m`, `{a="b"} by (x)`,
	} {
		if cachedQueryValidation(q).sample {
			t.Fatalf("%q validates as a metric expression", q)
		}
		form := url.Values{"query": {q}, "start": {"1700000053000000000"}, "end": {"1700003647000000000"}, "step": {"60s"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		if _, ok := alignQueryRangeToStep(req, time.Unix(1700003700, 0)); ok {
			t.Fatalf("%q was aligned", q)
		}
	}
}

// rawQueryWithBounds replaces start and end where the client wrote them and
// keeps every other parameter byte for byte; whatever form the query string
// takes, it decodes to the client's parameters with the two bounds set
// (reencodeWithBounds, the generic form).
// conformance: semantics/query-range-step-alignment
func TestRawQueryWithBounds(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
	}{
		{"in place", "query=sum%28x%29&start=17&end=19&step=60", "query=sum%28x%29&start=10&end=20&step=60"},
		{"order kept", "end=19&step=60&start=17", "end=20&step=60&start=10"},
		{"absent bounds appended", "query=x&step=60", "query=x&step=60&start=10&end=20"},
		{"empty", "", "start=10&end=20"},
		{"empty pieces dropped", "&&start=17&&end=19&", "start=10&end=20"},
		{"bare key", "start&end=19", "start=10&end=20"},
		{"duplicate start re-encoded", "start=1&start=2&end=19", "end=20&start=10"},
		{"escaped key re-encoded", "st%61rt=1&end=19", "end=20&start=10"},
		{"plus in key re-encoded", "a+b=1&start=17&end=19", "a+b=1&end=20&start=10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rawQueryWithBounds(tc.raw, "10", "20")
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			assertBoundsReplaced(t, tc.raw, got, "10", "20")
		})
	}
	if got := rawQueryWithBounds("start=1&end=2", "1969-12-31T23:59:59.5Z", "20"); got != "start=1969-12-31T23%3A59%3A59.5Z&end=20" {
		t.Fatalf("an RFC3339 bound is escaped: %q", got)
	}
}

func assertBoundsReplaced(t testing.TB, raw, got, start, end string) {
	t.Helper()
	want, _ := url.ParseQuery(reencodeWithBounds(raw, start, end))
	have, err := url.ParseQuery(got)
	if _, rawErr := url.ParseQuery(raw); err != nil && rawErr == nil {
		t.Fatalf("%q does not decode: %v", got, err)
	}
	if have.Encode() != want.Encode() {
		t.Fatalf("%q decodes to %v, want %v", got, have, want)
	}
}

// FuzzRawQueryWithBounds checks the in-place rewrite over arbitrary query
// strings against the generic decode, set and encode.
func FuzzRawQueryWithBounds(f *testing.F) {
	for _, seed := range []string{"query=x&start=1&end=2&step=60", "", "start=1&start=2", "st%61rt=1", "a=%zz&start=1", "a;b=1&end=2", "start&end"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		assertBoundsReplaced(t, raw, rawQueryWithBounds(raw, "1700000040000000000", "1700003640000000000"), "1700000040000000000", "1700003640000000000")
	})
}

// newAlignTestProxy builds a proxy over backendURL. caches turns on the
// response cache (Tier0) and the stats extent cache; without them every
// request is evaluated from VictoriaLogs' answer.
func newAlignTestProxy(t testing.TB, backendURL string, align, caches bool, limits map[string]any) *Proxy {
	t.Helper()
	cfg := Config{
		BackendURL:               backendURL,
		Cache:                    cache.NewDisabled(),
		LogLevel:                 "error",
		AlignQueriesWithStep:     align,
		QueryRangeExtentCacheTTL: time.Hour,
		TenantDefaultLimits:      limits,
	}
	if caches {
		cfg.Cache = cache.NewWithMaxBytes(time.Minute, 100000, 256<<20)
		cfg.CompatCache = cache.New(time.Minute, 1000)
		cfg.QueryRangeExtentCache = true
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		p.cache.Close()
		if p.compatCache != nil {
			p.compatCache.Close()
		}
	})
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	return p
}

// alignRouteQueryRange sends a query_range request through the proxy's routes
// (its middleware chain included).
func alignRouteQueryRange(t testing.TB, mux http.Handler, p *Proxy, query string, start, end time.Time, step time.Duration) (int, []byte) {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatInt(step.Milliseconds(), 10) + "ms"},
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	p.statsExtentStores.wait()
	return rec.Code, rec.Body.Bytes()
}

func alignRouteMux(p *Proxy) *http.ServeMux {
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	return mux
}

// alignSamples decodes a matrix answer into app -> unix second -> value.
func alignSamples(t testing.TB, body []byte) map[string]map[int64]string {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		t.Fatalf("not a matrix answer (%v): %s", err, body)
	}
	out := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		app := series.Metric["app"]
		if out[app] == nil {
			out[app] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			v, _ := pair[1].(string)
			out[app][int64(ts)] = v
		}
	}
	return out
}

func alignFixture(s0 time.Time, span time.Duration) []slidingFixtureLine {
	var lines []slidingFixtureLine
	for i := 0; time.Duration(i)*13*time.Second < span; i++ {
		app := []string{"a", "b", "c"}[i%3]
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i)*13*time.Second + 250*time.Millisecond).UnixNano(), app: app, msg: "n=" + strconv.Itoa(i%17) + " msg=tick" + strings.Repeat("x", i%5)})
	}
	return lines
}

// With the flag on, the answer is Loki's on the step grid: every sample sits
// at a multiple of the step, the first one at or before the requested start
// and the last one at the end moved down, with the values Loki computes for
// those times. With the flag off the grid starts at the requested start, as
// before. A range just under the 11,000-point limit and a range at
// max_query_length are accepted on the bounds the client sent, even when the
// moved bounds would exceed them (Loki checks before it aligns).
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestQueryRangeStepAlignment_AnswersOnLokisGrid(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	lines := alignFixture(s0, 4*time.Hour)
	srv, _ := newSlidingFakeVL(t, lines)
	start, end := s0.Add(30*time.Minute+17*time.Second), s0.Add(3*time.Hour+41*time.Second+500*time.Millisecond)
	cases := []struct {
		query  string
		fn     string
		byApp  bool
		step   time.Duration
		window time.Duration
	}{
		{`sum by (app) (count_over_time({app=~".+"}[5m]))`, "count_over_time", true, 137 * time.Second, 5 * time.Minute},
		{`sum by (app) (rate({app=~".+"}[2m]))`, "rate", true, 60 * time.Second, 2 * time.Minute},
		{`sum(bytes_over_time({app=~".+"}[90s]))`, "bytes_over_time", false, 60 * time.Second, 90 * time.Second},
		{`sum by (app) (count_over_time({app=~".+"}[1m]))`, "count_over_time", true, 15 * time.Second, time.Minute},
	}
	for _, align := range []bool{true, false} {
		p := newAlignTestProxy(t, srv.URL, align, false, nil)
		mux := alignRouteMux(p)
		for _, tc := range cases {
			wantStart, wantEnd := start, end
			if align {
				stepMs := tc.step.Milliseconds()
				wantStart, wantEnd = time.Unix(0, lokiStepAlignNanos(start, stepMs)), time.Unix(0, lokiStepAlignNanos(end, stepMs))
			}
			want := lokiSlidingReference(lines, tc.fn, tc.byApp, 0, wantStart, wantEnd, tc.step, tc.window)
			code, body := alignRouteQueryRange(t, mux, p, tc.query, start, end, tc.step)
			if code != http.StatusOK {
				t.Fatalf("align=%v %s: %d %s", align, tc.query, code, body)
			}
			got := alignSamples(t, body)
			if !tc.byApp {
				got = map[string]map[int64]string{"": got[""]}
			}
			assertSlidingSeriesEqual(t, tc.query, want, got)
			if align {
				for app, points := range got {
					for ts := range points {
						if ts%int64(tc.step/time.Second) != 0 {
							t.Fatalf("%s app=%q: sample at %d is off the %s grid", tc.query, app, ts, tc.step)
						}
					}
				}
			}
		}
	}

	// An offset moves the evaluated window, not the grid: Loki aligns the
	// bounds the client sent and shifts the window by the offset.
	p := newAlignTestProxy(t, srv.URL, true, false, nil)
	mux := alignRouteMux(p)
	step, offset := 60*time.Second, 433*time.Second
	shifted := make([]slidingFixtureLine, len(lines))
	for i, line := range lines {
		shifted[i] = line
		shifted[i].ts = line.ts + int64(offset)
	}
	alignedStart, alignedEnd := time.Unix(0, lokiStepAlignNanos(start, 60000)), time.Unix(0, lokiStepAlignNanos(end, 60000))
	want := lokiSlidingReference(shifted, "count_over_time", true, 0, alignedStart, alignedEnd, step, 5*time.Minute)
	code, body := alignRouteQueryRange(t, mux, p, `sum by (app) (count_over_time({app=~".+"}[5m] offset 433s))`, start, end, step)
	if code != http.StatusOK {
		t.Fatalf("offset: %d %s", code, body)
	}
	assertSlidingSeriesEqual(t, "offset 433s", want, alignSamples(t, body))

	// A step longer than the range: both bounds move down to the same grid
	// point, and Loki answers one sample there, before the requested start.
	for _, tc := range []struct {
		name       string
		start, end time.Time
		step       time.Duration
	}{
		{"step longer than the range", s0.Add(time.Hour + 10*time.Second), s0.Add(time.Hour + 30*time.Second), 10 * time.Minute},
		{"start equal to end", s0.Add(2*time.Hour + 17*time.Second), s0.Add(2*time.Hour + 17*time.Second), time.Minute},
	} {
		stepMs := tc.step.Milliseconds()
		alignedStart, alignedEnd := time.Unix(0, lokiStepAlignNanos(tc.start, stepMs)), time.Unix(0, lokiStepAlignNanos(tc.end, stepMs))
		if !alignedStart.Equal(alignedEnd) || !alignedStart.Before(tc.start) {
			t.Fatalf("%s: the fixture must move both bounds onto one grid point before the start", tc.name)
		}
		want := lokiSlidingReference(lines, "count_over_time", true, 0, alignedStart, alignedEnd, tc.step, 5*time.Minute)
		code, body := alignRouteQueryRange(t, mux, p, `sum by (app) (count_over_time({app=~".+"}[5m]))`, tc.start, tc.end, tc.step)
		if code != http.StatusOK {
			t.Fatalf("%s: %d %s", tc.name, code, body)
		}
		got := alignSamples(t, body)
		assertSlidingSeriesEqual(t, tc.name, want, got)
		for app, points := range got {
			if _, ok := points[alignedStart.Unix()]; len(points) != 1 || !ok {
				t.Fatalf("%s app=%q: want one sample at %d, got %v", tc.name, app, alignedStart.Unix(), points)
			}
		}
	}

	// Limits are checked on the bounds the client sent.
	sevens := time.Unix(s0.Unix()/7*7, 0)
	edges := []struct {
		name       string
		limits     map[string]any
		start, end time.Time
		step       time.Duration
	}{
		// 11,000.5 steps as sent, which Loki's integer division counts as
		// 11,000; 11,001 once the start moves down 0.9s and the end 0.4s.
		{"resolution", nil, s0.Add(30*time.Second + 900*time.Millisecond), s0.Add(30*time.Second + 900*time.Millisecond + 11000*time.Second + 500*time.Millisecond), time.Second},
		// One hour as sent; at step 7s the start moves down 6s and the end
		// 1s, so the moved range is 3605s.
		{"max_query_length", map[string]any{"max_query_length": "1h"}, sevens.Add(706 * time.Second), sevens.Add(706*time.Second + time.Hour), 7 * time.Second},
	}
	for _, edge := range edges {
		p := newAlignTestProxy(t, srv.URL, true, false, edge.limits)
		stepMs := edge.step.Milliseconds()
		if moved := lokiStepAlignNanos(edge.end, stepMs) - lokiStepAlignNanos(edge.start, stepMs); moved <= int64(edge.end.Sub(edge.start)) {
			t.Fatalf("%s: the moved range %s must be longer than the range sent %s", edge.name, time.Duration(moved), edge.end.Sub(edge.start))
		}
		code, body := alignRouteQueryRange(t, alignRouteMux(p), p, `sum(count_over_time({app=~".+"}[1m]))`, edge.start, edge.end, edge.step)
		if code != http.StatusOK {
			t.Fatalf("%s: the bounds as sent are within the limit, Loki answers 200; got %d %s", edge.name, code, body)
		}
	}
}

// alignRefresh is one request of a refresh sequence.
type alignRefresh struct {
	start, end time.Time
	step       time.Duration
}

// alignRefreshes returns refresh sequences that move a window by less than a
// step at a time, the way an auto-refreshing dashboard does: both bounds
// moving (an API client), or a start already on the grid with a raw end
// (Grafana's query splitting floors the start; the end is now).
func alignRefreshes(seed int64, from time.Time) []alignRefresh {
	rng := rand.New(rand.NewSource(seed))
	var out []alignRefresh
	for _, step := range []time.Duration{15 * time.Second, time.Minute, 137 * time.Second} {
		for _, span := range []time.Duration{time.Hour, 3 * time.Hour} {
			end := from.Add(span + time.Duration(rng.Int63n(int64(30*time.Minute))))
			for i := 0; i < 8; i++ {
				end = end.Add(time.Duration(1+rng.Int63n(int64(step/time.Second)-1)) * time.Second / 2)
				start := end.Add(-span)
				out = append(out, alignRefresh{start, end, step})
				grafanaStart := time.Unix(0, lokiStepAlignNanos(start, step.Milliseconds()))
				out = append(out, alignRefresh{grafanaStart, end, step})
			}
		}
	}
	return out
}

// With the flag on, random refresh sequences moving by less than a step give
// byte for byte the answer of the uncached aligned path, and a refresh that
// stays inside one step reaches VictoriaLogs no more: the moved request is the
// same request on the grid, answered by the response cache, and once that is
// gone (a restart or eviction) by the extent cache from stored buckets.
// conformance: semantics/query-range-step-alignment, quality/query-range-extent-cache, loki_api_v1_query_range
func TestQueryRangeStepAlignment_SubStepRefreshServedFromCaches(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	lines := alignFixture(s0, 5*time.Hour)
	queries := []string{
		`sum by (app) (count_over_time({app=~".+"}[5m]))`,
		`sum by (app) (rate({app=~".+"}[1m]))`,
		`sum(bytes_over_time({app=~".+"}[2m]))`,
		`sum by (app) (count_over_time({app=~".+"}[$__auto]))`,
		`topk(2, sum by (app) (rate({app=~".+"}[5m])))`,
		`sum(rate({app="a"}[1m])) / sum(rate({app=~".+"}[1m]))`,
	}
	refreshes := alignRefreshes(7, s0)
	cachedSrv, cachedShim := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	refSrv, _ := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	cached := newAlignTestProxy(t, cachedSrv.URL, true, true, nil)
	ref := newAlignTestProxy(t, refSrv.URL, true, false, nil)
	cachedMux, refMux := alignRouteMux(cached), alignRouteMux(ref)
	repeats, repeatCalls := 0, 0
	for _, query := range queries {
		seen := map[[2]int64]bool{}
		for _, w := range refreshes {
			q := strings.ReplaceAll(query, "$__auto", strconv.FormatInt(int64(w.step/time.Second), 10)+"s")
			wantCode, want := alignRouteQueryRange(t, refMux, ref, q, w.start, w.end, w.step)
			callsBefore, _ := cachedShim.bucketsRead()
			gotCode, got := alignRouteQueryRange(t, cachedMux, cached, q, w.start, w.end, w.step)
			callsAfter, _ := cachedShim.bucketsRead()
			if gotCode != wantCode || !bytes.Equal(got, want) {
				t.Fatalf("%s [%s, %s] step %s: the caches changed the answer\n cached: %d %s\nuncached: %d %s", q, w.start.Format(time.RFC3339Nano), w.end.Format(time.RFC3339Nano), w.step, gotCode, got, wantCode, want)
			}
			if wantCode != http.StatusOK || !bytes.Contains(want, []byte(`"values":[[`)) {
				t.Fatalf("%s: the run must answer with data, got %d %s", q, wantCode, want)
			}
			grid := [2]int64{lokiStepAlignNanos(w.start, w.step.Milliseconds()), lokiStepAlignNanos(w.end, w.step.Milliseconds())}
			if seen[grid] {
				repeats++
				repeatCalls += callsAfter - callsBefore
			}
			seen[grid] = true
		}
	}
	if repeats < len(queries)*20 || repeatCalls != 0 {
		t.Fatalf("%d refreshes landed on a grid already answered and made %d VictoriaLogs calls; want many and none", repeats, repeatCalls)
	}

	// Without the response cache, the extent cache answers the same moved
	// requests from stored buckets.
	cached.compatCache.Close()
	cached.compatCache = nil
	cachedShim.reset()
	for _, query := range queries[:3] {
		for _, w := range refreshes {
			_, want := alignRouteQueryRange(t, refMux, ref, query, w.start, w.end, w.step)
			_, got := alignRouteQueryRange(t, cachedMux, cached, query, w.start, w.end, w.step)
			if !bytes.Equal(got, want) {
				t.Fatalf("%s: the extent cache changed the answer", query)
			}
		}
	}
	if calls, _ := cachedShim.bucketsRead(); calls != 0 {
		t.Fatalf("every bucket of these windows was stored; the extent cache made %d VictoriaLogs calls", calls)
	}
	if outcomes := extentOutcomes(t, cached); outcomes["hit"] == 0 {
		t.Fatalf("the extent cache must answer the moved requests from stored buckets: %v", outcomes)
	}
}

// With the flag on, a refresh inside one step after the edge response cache
// is gone (a frontend restart) is answered by the routing-scoped shared tier
// of the read-reply cache, which is keyed on the moved bounds like the edge
// cache: byte for byte the uncached aligned answer, with no VictoriaLogs call.
// The extent cache is off here, so only the shared tier can answer it.
// conformance: semantics/query-range-step-alignment, loki_api_v1_query_range
func TestQueryRangeStepAlignment_SharedTierAnswersMovedRefresh(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	lines := alignFixture(s0, 5*time.Hour)
	cachedSrv, cachedShim := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	refSrv, _ := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	cached, err := New(Config{
		BackendURL:  cachedSrv.URL,
		Cache:       cache.NewWithMaxBytes(time.Minute, 100000, 256<<20),
		CompatCache: cache.New(time.Minute, 1000),
		LogLevel:    "error",
		// the default; the extent cache stays off
		AlignQueriesWithStep: true,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = cached.Shutdown(context.Background())
		cached.cache.Close()
		cached.compatCache.Close()
	})
	cached.storeBackendVersion("v1.50.0", "v1.50.0")
	cached.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	ref := newAlignTestProxy(t, refSrv.URL, true, false, nil)
	cachedMux, refMux := alignRouteMux(cached), alignRouteMux(ref)
	queries := []string{
		`sum by (app) (count_over_time({app=~".+"}[5m]))`,
		`sum by (app) (rate({app=~".+"}[1m]))`,
		`sum(bytes_over_time({app=~".+"}[2m]))`,
	}
	for _, query := range queries {
		for _, step := range []time.Duration{15 * time.Second, time.Minute, 137 * time.Second} {
			// both bounds a fifth of a step past a grid point, then moved by half a
			// step: still inside the same step on both sides
			start := time.Unix(0, lokiStepAlignNanos(s0.Add(time.Hour), step.Milliseconds())).Add(step / 5)
			end := time.Unix(0, lokiStepAlignNanos(s0.Add(3*time.Hour), step.Milliseconds())).Add(step / 5)
			_, want := alignRouteQueryRange(t, refMux, ref, query, start.Add(step/2), end.Add(step/2), step)
			if code, _ := alignRouteQueryRange(t, cachedMux, cached, query, start, end, step); code != http.StatusOK {
				t.Fatalf("%s step %s: first request answered %d", query, step, code)
			}
			// a restart: the edge cache is empty, the shared tier keeps its entries
			cached.compatCache.Close()
			cached.compatCache = cache.New(time.Minute, 1000)
			callsBefore, _ := cachedShim.bucketsRead()
			code, got := alignRouteQueryRange(t, cachedMux, cached, query, start.Add(step/2), end.Add(step/2), step)
			callsAfter, _ := cachedShim.bucketsRead()
			if code != http.StatusOK || !bytes.Equal(got, want) {
				t.Fatalf("%s step %s: the shared tier changed the answer\n  cached: %d %s\nuncached: %s", query, step, code, got, want)
			}
			if !bytes.Contains(want, []byte(`"values":[[`)) {
				t.Fatalf("%s step %s: the run must answer with data, got %s", query, step, want)
			}
			if callsAfter != callsBefore {
				t.Fatalf("%s step %s: a refresh inside one step made %d VictoriaLogs calls after the edge cache was lost; the shared tier holds it", query, step, callsAfter-callsBefore)
			}
		}
	}
}

// A refresh near now is never served a stale answer. Loki recomputes the
// newest max_cache_freshness_per_query of every request, so a row that arrives
// late in the newest sample's window shows on the next refresh. With the flag
// on, refreshes inside one step are the same request, and the moved end can be
// up to a step older than -recent-tail-refresh-window: whether the request is
// near now is judged on the end the client sent.
// conformance: semantics/query-range-step-alignment
func TestQueryRangeStepAlignment_NearNowRefreshSeesLateRows(t *testing.T) {
	now := time.Now()
	// A step whose last grid point is 3 to 9 minutes old: older than the
	// near-now window (2m), newer than -query-range-freshness (10m), and the
	// next grid point more than a minute away.
	var step time.Duration
	for m := 10; m <= 40 && step == 0; m++ {
		d := time.Duration(m) * time.Minute
		since := now.Sub(time.Unix(0, lokiStepAlignNanos(now, d.Milliseconds())))
		if since > 3*time.Minute && since < 9*time.Minute && d-since > time.Minute {
			step = d
		}
	}
	if step == 0 {
		t.Fatalf("no step from 10m to 40m puts the last grid point 3 to 9 minutes before %s", now)
	}
	grid := time.Unix(0, lokiStepAlignNanos(now, step.Milliseconds()))
	lines := alignFixture(now.Add(-3*time.Hour), 3*time.Hour-30*time.Minute)
	late := append(append([]slidingFixtureLine(nil), lines...), slidingFixtureLine{ts: grid.Add(-time.Second).UnixNano(), app: "a", msg: "n=1 msg=late"})
	before, _ := newSlidingFakeVL(t, lines)
	after, _ := newSlidingFakeVL(t, late)
	var arrived atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if arrived.Load() {
			after.Config.Handler.ServeHTTP(w, r)
			return
		}
		before.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)
	p, err := New(Config{
		BackendURL:                    backend.URL,
		Cache:                         cache.NewWithMaxBytes(time.Minute, 100000, 256<<20),
		CompatCache:                   cache.New(time.Minute, 1000),
		LogLevel:                      "error",
		AlignQueriesWithStep:          true,
		QueryRangeExtentCache:         true,
		QueryRangeExtentCacheTTL:      time.Hour,
		RecentTailRefreshEnabled:      true,
		RecentTailRefreshMaxStaleness: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		p.cache.Close()
		p.compatCache.Close()
	})
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	mux := alignRouteMux(p)
	query := `sum(count_over_time({app=~".+"}[5m]))`
	value := func(body []byte) string { return alignSamples(t, body)[""][grid.Unix()] }
	_, first := alignRouteQueryRange(t, mux, p, query, now.Add(-2*time.Hour), now, step)
	arrived.Store(true)
	time.Sleep(5 * time.Millisecond)
	later := now.Add(20 * time.Millisecond)
	_, second := alignRouteQueryRange(t, mux, p, query, later.Add(-2*time.Hour), later, step)
	want := "1"
	if v := value(first); v != "" {
		n, _ := strconv.Atoi(v)
		want = strconv.Itoa(n + 1)
	}
	if got := value(second); got != want {
		t.Fatalf("the sample at %s after a late row: %q, want %q (before it: %q)", grid.UTC().Format(time.RFC3339), got, want, value(first))
	}
}

// subStepRefreshBench runs one request per iteration over a window moved by
// 13s each time at step 60s (a start phase repeats only after 60 requests),
// and reports the VictoriaLogs calls and buckets each request read.
func subStepRefreshBench(b *testing.B, align, grafanaStart bool) {
	s0 := time.Unix(1700001000, 0).UTC()
	lines := alignFixture(s0, 26*time.Hour)
	srv, shim := newExtentShim(b, func() http.Handler { srv, _ := newSlidingFakeVL(b, lines); return srv.Config.Handler }())
	p := newAlignTestProxy(b, srv.URL, align, true, nil)
	mux := alignRouteMux(p)
	query := `sum by (app) (count_over_time({app=~".+"}[5m]))`
	step := time.Minute
	end := s0.Add(24*time.Hour + 7*time.Second)
	request := func(i int) {
		e := end.Add(time.Duration(i) * 13 * time.Second)
		s := e.Add(-24 * time.Hour)
		if grafanaStart {
			s = time.Unix(0, lokiStepAlignNanos(s, step.Milliseconds()))
		}
		if code, body := alignRouteQueryRange(b, mux, p, query, s, e, step); code != http.StatusOK {
			b.Fatalf("%d %s", code, body)
		}
	}
	request(-1) // the first load
	shim.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request(i)
	}
	b.StopTimer()
	calls, buckets := shim.bucketsRead()
	b.ReportMetric(float64(calls)/float64(b.N), "vl_calls/op")
	b.ReportMetric(float64(buckets)/float64(b.N), "vl_buckets/op")
}

// A dashboard refreshing a 24h window every 13 s at step 60 s, the window
// moving by less than a step each time: raw bounds without alignment (an API
// client), a floored start with a raw end without alignment (Grafana's query
// splitting), and both with -align-queries-with-step.
func BenchmarkQueryRangeSubStepRefresh(b *testing.B) {
	b.Run("unaligned/raw_bounds", func(b *testing.B) { subStepRefreshBench(b, false, false) })
	b.Run("unaligned/grafana_start", func(b *testing.B) { subStepRefreshBench(b, false, true) })
	b.Run("aligned/raw_bounds", func(b *testing.B) { subStepRefreshBench(b, true, false) })
	b.Run("aligned/grafana_start", func(b *testing.B) { subStepRefreshBench(b, true, true) })
}
