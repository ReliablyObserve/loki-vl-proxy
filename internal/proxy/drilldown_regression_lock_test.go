// Drilldown / Explore regression lock-in tests.
//
// Background (do not delete this comment):
//
//	Grafana's Loki datasource splits metric range queries at the 24h
//	boundary (oneDayMs in querySplitting.ts) and merges chunk responses via
//	mergeFrames + closestIdx + splice in mergeResponses.ts. A residual chunk
//	whose range is < step produces a one-bucket frame; mergeFrames glues its
//	single-point series onto one edge of the merged chart. The proxy returns
//	an empty matrix for that residual (Drilldown only), and trims every other
//	chunk to its own start/end so the chunks merge cleanly.
//
//	Drilldown label and field breakdowns are answered exactly, as Loki
//	answers them: every series from VictoriaLogs stats on the request grid,
//	up to the tenant's max_query_series (drilldown_breakdown_exact_test.go).
//	TestLock_GrafanaMergedFrames_* replays the chunked merge end to end.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Shared helpers (kept here, not in newTestProxy, so this file is
// self-explanatory and resilient to refactors elsewhere in the test tree).
// ---------------------------------------------------------------------------

// recorderBackend records every request the proxy sends to VL and returns a
// response provided by the per-path handler map. Unknown paths return a
// success-shaped empty Loki matrix.
type recorderBackend struct {
	mu       sync.Mutex
	calls    map[string]int      // path -> count
	queries  map[string][]string // path -> received query strings
	steps    map[string][]string // path -> received step strings
	handlers map[string]func(http.ResponseWriter, *http.Request)
}

func newRecorderBackend() *recorderBackend {
	return &recorderBackend{
		calls:    map[string]int{},
		queries:  map[string][]string{},
		steps:    map[string][]string{},
		handlers: map[string]func(http.ResponseWriter, *http.Request){},
	}
}

func (b *recorderBackend) on(path string, h func(http.ResponseWriter, *http.Request)) {
	b.handlers[path] = h
}

func (b *recorderBackend) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		b.mu.Lock()
		b.calls[r.URL.Path]++
		b.queries[r.URL.Path] = append(b.queries[r.URL.Path], r.Form.Get("query"))
		b.steps[r.URL.Path] = append(b.steps[r.URL.Path], r.Form.Get("step"))
		h := b.handlers[r.URL.Path]
		b.mu.Unlock()
		if h != nil {
			h(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
}

// drilldownRequest builds an http.Request shaped like a Grafana stats query.
// source=="" leaves X-Query-Tags unset (Explore / direct API).
// source=="drilldown" sets X-Query-Tags: Source=grafana-lokiexplore-app.
// source=="grafana-ua" sets User-Agent: Grafana/11.5.0 (dashboard panels).
// source=="grafana-hdr" sets X-Grafana-Org-Id (backend-routed Explore).
func drilldownRequest(t *testing.T, query string, startSec, endSec int64, step, source string) *http.Request {
	t.Helper()
	form := url.Values{}
	form.Set("query", query)
	form.Set("start", strconv.FormatInt(startSec, 10))
	form.Set("end", strconv.FormatInt(endSec, 10))
	form.Set("step", step)
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+form.Encode(), nil)
	r = r.WithContext(context.WithValue(r.Context(), orgIDKey, "default"))
	r.Header.Set("X-Scope-OrgID", "default")
	switch source {
	case "drilldown":
		r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	case "grafana-ua":
		r.Header.Set("User-Agent", "Grafana/11.5.0")
	case "grafana-hdr":
		r.Header.Set("X-Grafana-Org-Id", "1")
	}
	return r
}

// ---------------------------------------------------------------------------
// Leftover-chunk suppression for Drilldown source.
// ---------------------------------------------------------------------------

// TestLock_LeftoverChunkSuppressedInHits pins the rule:
//
//	end - start < step AND Drilldown source signal → empty matrix
//
// The Grafana 24h+ querySplitting residual is a sub-step chunk that can only
// produce a one-bucket frame; Grafana's mergeFrames collapses its single-point
// series onto one edge of the merged chart (a right-edge spike or a left-edge
// cluster). Returning an empty matrix gives mergeFrames nothing to glue, and
// the chart loses at most one step on its edge. The response header keeps its
// historical value (X-Proxy-Drilldown-Path: hits-leftover-suppressed).
//
// Plain Grafana, non-Grafana callers and any range >= one full step are served
// normally.
func TestLock_LeftoverChunkSuppressedInHits(t *testing.T) {
	cases := []struct {
		name         string
		startSec     int64
		endSec       int64
		stepRaw      string
		source       string
		wantSuppress bool
	}{
		// Grafana, sub-step residual (range < step) → SUPPRESS.
		{"drilldown_60s_step120", 1700000000, 1700000060, "120", "drilldown", true},
		{"drilldown_119s_step120", 1700000000, 1700000119, "120", "drilldown", true},
		{"grafana_ua_60s", 1700000000, 1700000060, "120", "grafana-ua", false},
		{"grafana_hdr_60s", 1700000000, 1700000060, "120", "grafana-hdr", false},
		// Grafana, range >= step (legitimate >=1-bucket query) → do NOT suppress.
		{"drilldown_120s_step120", 1700000000, 1700000120, "120", "drilldown", false},
		{"drilldown_240s_step120", 1700000000, 1700000240, "120", "drilldown", false},
		{"drilldown_1h_step120", 1700000000, 1700003600, "120", "drilldown", false},
		// Non-Grafana caller with sub-step range → do NOT suppress.
		{"raw_caller_60s_step120", 1700000000, 1700000060, "120", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newRecorderBackend()
			served := func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000000,"3"]]}]}}`))
			}
			backend.on("/select/logsql/stats_query_range", served)
			vl := backend.server()
			defer vl.Close()
			p := newTestProxy(t, vl.URL)

			r := drilldownRequest(t,
				`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
				tc.startSec, tc.endSec, tc.stepRaw, tc.source)
			w := httptest.NewRecorder()
			p.proxyStatsQueryRange(w, r,
				`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

			gotSuppressed := w.Header().Get("X-Proxy-Drilldown-Path") == "hits-leftover-suppressed"
			if gotSuppressed != tc.wantSuppress {
				t.Errorf("%s: suppressed=%v, want %v (body=%s)", tc.name, gotSuppressed, tc.wantSuppress, w.Body.String())
			}
			if tc.wantSuppress && !strings.Contains(w.Body.String(), `"result":[]`) {
				t.Errorf("%s: suppressed but body is not an empty matrix: %s", tc.name, w.Body.String())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isGrafanaSourcedRequest accepts Drilldown, Explore, dashboard.
// ---------------------------------------------------------------------------

// TestLock_IsGrafanaSourcedRequest pins which client signals count as
// "Grafana source" for Grafana-only compatibility behavior such as
// partial-results conversion and high-cardinality optimization.
func TestLock_IsGrafanaSourcedRequest(t *testing.T) {
	cases := []struct {
		name string
		set  func(*http.Request)
		want bool
	}{
		{"drilldown_tag", func(r *http.Request) {
			r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		}, true},
		{"explore_tag", func(r *http.Request) {
			r.Header.Set("X-Query-Tags", "Source=lokiexplore")
		}, true},
		{"user_agent_grafana", func(r *http.Request) {
			r.Header.Set("User-Agent", "Grafana/11.5.0")
		}, true},
		{"x_grafana_org_id", func(r *http.Request) {
			r.Header.Set("X-Grafana-Org-Id", "1")
		}, true},
		{"x_grafana_request_id", func(r *http.Request) {
			r.Header.Set("X-Grafana-Request-Id", "abc")
		}, true},
		{"raw_curl", func(r *http.Request) {}, false},
		{"random_user_agent", func(r *http.Request) {
			r.Header.Set("User-Agent", "curl/8.4.0")
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
			tc.set(r)
			if got := isGrafanaSourcedRequest(r); got != tc.want {
				t.Errorf("isGrafanaSourcedRequest(%s) = %v, want %v",
					tc.name, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Grafana mergeFrames simulator — full chunked-merge contract.
// ---------------------------------------------------------------------------

// gfFrame is the simulator's mirror of @grafana/data DataFrame for the
// subset of mergeFrames behavior we replicate. Keeping the simulator embedded
// in this test file (instead of importing a helper) means a future refactor
// can't quietly weaken it.
type gfFrame struct {
	times  []int64            // ms (matches Grafana's time field type)
	series map[string][]int64 // label -> per-timestamp value
}

func gfFrameFromMatrix(matrix []byte, t *testing.T) gfFrame {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(matrix, &resp); err != nil {
		t.Fatalf("parse matrix: %v\nbody: %s", err, matrix)
	}
	f := gfFrame{series: map[string][]int64{}}
	if len(resp.Data.Result) == 0 {
		return f
	}
	// Build axis from series[0]; assert all series share it.
	for _, v := range resp.Data.Result[0].Values {
		ts, _ := v[0].(float64)
		f.times = append(f.times, int64(ts)*1000) // sec → ms
	}
	for _, s := range resp.Data.Result {
		var label string
		for _, v := range s.Metric {
			label = v // single by() clause → single label value
			break
		}
		vals := make([]int64, len(s.Values))
		for i, v := range s.Values {
			vs, _ := v[1].(string)
			n, _ := strconv.ParseInt(vs, 10, 64)
			vals[i] = n
		}
		f.series[label] = vals
	}
	return f
}

// gfClosestIdx replicates @grafana/data closestIdx.
func gfClosestIdx(target int64, arr []int64) int {
	if len(arr) == 0 {
		return -1
	}
	if target <= arr[0] {
		return 0
	}
	if target >= arr[len(arr)-1] {
		return len(arr) - 1
	}
	lo, hi := 0, len(arr)-1
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if arr[mid] < target {
			lo = mid
		} else {
			hi = mid
		}
	}
	if target-arr[lo] <= arr[hi]-target {
		return lo
	}
	return hi
}

// gfResolveIdx replicates Loki datasource mergeResponses.ts resolveIdx.
func gfResolveIdx(destTimes []int64, srcTime int64) int {
	idx := gfClosestIdx(srcTime, destTimes)
	if idx < 0 {
		return 0
	}
	if srcTime > destTimes[idx] {
		return idx + 1
	}
	return idx
}

// gfMerge replicates the in-place mutation of mergeFrames(dest, source).
func gfMerge(dest *gfFrame, src gfFrame) {
	for i, st := range src.times {
		dIdx := gfResolveIdx(dest.times, st)
		exists := dIdx < len(dest.times) && dest.times[dIdx] == st
		// Make sure dest knows about every source series.
		for label := range src.series {
			if _, ok := dest.series[label]; !ok {
				dest.series[label] = make([]int64, len(dest.times))
			}
		}
		// Merge or splice each value.
		for label, sv := range src.series {
			if exists {
				dest.series[label][dIdx] += sv[i]
			} else {
				dest.series[label] = append(dest.series[label][:dIdx],
					append([]int64{sv[i]}, dest.series[label][dIdx:]...)...)
			}
		}
		if !exists {
			dest.times = append(dest.times[:dIdx],
				append([]int64{st}, dest.times[dIdx:]...)...)
			for label := range dest.series {
				if _, inSrc := src.series[label]; !inSrc {
					dest.series[label] = append(dest.series[label][:dIdx],
						append([]int64{0}, dest.series[label][dIdx:]...)...)
				}
			}
		}
	}
}

// rightEdgePercent returns the fraction of distinct nonzero timestamps that
// land in the rightmost `binCount` bins. A real right-edge spike has > 0.4.
func rightEdgePercent(f gfFrame, binCount int) float64 {
	if len(f.times) == 0 {
		return 0
	}
	nzTs := map[int64]struct{}{}
	for _, vals := range f.series {
		for i, v := range vals {
			if v != 0 && i < len(f.times) {
				nzTs[f.times[i]] = struct{}{}
			}
		}
	}
	if len(nzTs) == 0 {
		return 0
	}
	span := f.times[len(f.times)-1] - f.times[0]
	if span <= 0 {
		return 0
	}
	bins := make([]int, binCount)
	for ts := range nzTs {
		b := int(float64(ts-f.times[0]) / float64(span) * float64(binCount))
		if b >= binCount {
			b = binCount - 1
		}
		bins[b]++
	}
	return float64(bins[binCount-1]) / float64(len(nzTs))
}

// runChunkSim simulates Grafana's querySplitting + mergeFrames flow against
// a fake VL backend wired with realistic stats_query_range responses per chunk. The
// fake backend's behavior is intentionally pessimistic for the right edge:
// each chunk's "top-N" includes some chunk-unique values that mergeFrames
// would otherwise stack at the chunk boundary.
//
// Returns the merged frame so individual tests can assert on it.
func runChunkSim(t *testing.T, source string, chunks [][2]int64, step time.Duration) gfFrame {
	t.Helper()
	// Each chunk returns 16 chunk-unique series.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			// Every source, Drilldown included, reads the chunk-unique series from
			// stats buckets spread over the whole requested range. A request sent
			// anywhere else gets an empty matrix and the merged frame stays empty.
			startNs := parseFakeVLTime(t, r.Form.Get("start"))
			endNs := parseFakeVLTime(t, r.Form.Get("end"))
			bucket, err := time.ParseDuration(r.Form.Get("step"))
			if err != nil || bucket <= 0 {
				t.Errorf("fake VL: bad step %q", r.Form.Get("step"))
				return
			}
			first := startNs - startNs%int64(bucket)
			var b strings.Builder
			b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i := 0; i < 16; i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, `{"metric":{"pod":"pod-c%d-i%d"},"values":[`, startNs/int64(time.Second), i)
				j := 0
				for ts := first; ts < endNs; ts += int64(bucket) {
					if (i+j)%3 == 0 {
						if !strings.HasSuffix(b.String(), "[") {
							b.WriteByte(',')
						}
						fmt.Fprintf(&b, `[%d,"20"]`, ts/int64(time.Second))
					}
					j++
				}
				b.WriteString(`]}`)
			}
			b.WriteString(`]}}`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(b.String()))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	// Sort chunks oldest→newest then process in reverse (Grafana metric order
	// is newest-first per runSplitGroupedQueries).
	sort.Slice(chunks, func(i, j int) bool { return chunks[i][0] < chunks[j][0] })

	var merged *gfFrame
	for i := len(chunks) - 1; i >= 0; i-- {
		c := chunks[i]
		r := drilldownRequest(t,
			`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
			c[0], c[1], strconv.FormatInt(int64(step.Seconds()), 10), source)
		w := httptest.NewRecorder()
		p.proxyStatsQueryRange(w, r,
			`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

		// Skip suppressed leftovers (Grafana would have nothing to merge).
		if w.Header().Get("X-Proxy-Drilldown-Path") == "hits-leftover-suppressed" {
			continue
		}
		var sanity struct {
			Data struct {
				Result []any `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &sanity); err != nil {
			t.Fatalf("chunk[%d]: parse: %v\nbody: %s", i, err, w.Body.String())
		}
		if len(sanity.Data.Result) == 0 {
			continue
		}
		f := gfFrameFromMatrix(w.Body.Bytes(), t)
		if merged == nil {
			merged = &f
		} else {
			gfMerge(merged, f)
		}
	}
	if merged == nil {
		return gfFrame{}
	}
	return *merged
}

// TestLock_GrafanaMergedFrames_NoRightEdgeSpike is the headline regression
// guard for the right-edge spike. It runs the embedded Grafana querySplitting +
// mergeFrames simulator for the four time ranges the user reported broken (24h,
// 25h, 2d, 7d) across ALL Grafana source tags — Drilldown, Explore via
// User-Agent, and Explore/dashboard via header — and asserts the rightmost bin
// holds < 40% of all nonzero timestamps in the merged frame.
//
// The non-Drilldown sources are kept on purpose even though residual suppression
// is now scoped to Drilldown: they verify that per-chunk axis trimming alone
// keeps Explore/dashboard metric ranges spike-free, so narrowing suppression to
// Drilldown did not reintroduce the spike for the other sources. Every source
// takes the same exact stats_query_range path.
//
// If a future PR regresses ANY of:
//   - leftover-chunk suppression (Drilldown)
//   - per-chunk axis trimming (all sources)
//   - one exact stats path for every source
//
// at least one of these subtests fails because the chunk-unique series
// stack at the right edge again.
func TestLock_GrafanaMergedFrames_NoRightEdgeSpike(t *testing.T) {
	const stepSec = 120
	const oneDayMs = int64(24 * 60 * 60 * 1000)
	stepMs := int64(stepSec * 1000)
	now := time.Date(2024, 11, 14, 22, 0, 0, 0, time.UTC).Unix()
	nowMs := now * 1000

	makeChunks := func(hours int) [][2]int64 {
		endMs := nowMs
		startMs := endMs - int64(hours)*3600*1000
		aligned := (oneDayMs / stepMs) * stepMs
		alignedStart := startMs - (startMs % stepMs)
		var chunks [][2]int64
		for cs := alignedStart; cs < endMs; cs += aligned {
			ce := cs + aligned - stepMs
			if ce > endMs {
				ce = endMs
			}
			chunks = append(chunks, [2]int64{cs / 1000, ce / 1000})
		}
		return chunks
	}

	rangeCases := []struct {
		name  string
		hours int
		bins  int
	}{
		{"24h", 24, 12},
		{"25h", 25, 12},
		{"2d", 48, 12},
		{"7d", 168, 14},
	}
	sources := []string{"drilldown", "grafana-ua", "grafana-hdr"}

	for _, source := range sources {
		for _, rc := range rangeCases {
			t.Run(fmt.Sprintf("%s_%s", source, rc.name), func(t *testing.T) {
				merged := runChunkSim(t, source, makeChunks(rc.hours), time.Duration(stepSec)*time.Second)
				if len(merged.series) == 0 {
					t.Fatalf("merged frame is empty — no chunks produced data?")
				}
				rep := rightEdgePercent(merged, rc.bins)
				if rep > 0.4 {
					t.Errorf("right-edge spike: rightmost bin holds %.0f%% of nonzero timestamps (>40%% threshold) — chunked-merge regression",
						rep*100)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// VL upstream errors never leak through to Grafana clients.
// ---------------------------------------------------------------------------

// TestLock_VLErrorsConvertedToPartialResults pins the contract that VL 4xx/5xx
// responses for Drilldown / Grafana-sourced stats queries are converted to
// HTTP 200 + Warning header + empty Loki matrix — mirroring Loki's own
// IsLogsDrilldownRequest carve-out (pkg/querier/queryrange/limits.go::
// seriesLimiter.Do upstream). The plugin must see a graceful empty chart
// with a warning badge, not a hard error toast.
//
// Why this matters: VL's parser-pipe row-scan limit (and several other VL
// query bounds) legitimately fires for high-cardinality Drilldown queries
// like `sum by (trace_id) (count_over_time({namespace="prod"}|json|trace_id!=""[2m]))`
// at 6h+ ranges. Loki returns 200 + partial-results for the same scenario;
// the proxy MUST match that behavior or it changes what Grafana renders
// (error vs warning) for byte-identical user input.
//
// Non-Grafana clients (curl, internal scripts) still see real errors so they
// can react meaningfully — only `X-Query-Tags: Source=grafana-lokiexplore-app`
// or `User-Agent: Grafana/*` or `X-Grafana-*` triggers the partial-results
// path. This mirrors the Loki source behavior.
func TestLock_VLErrorsConvertedToPartialResults(t *testing.T) {
	cases := []struct {
		name                string
		vlStatus            int
		vlBody              string
		grafanaSourced      bool
		expectStatus        int
		expectWarningHeader bool
		expectUpstreamHdr   bool
	}{
		{
			name:                "Grafana_VL_502_parserpipe_overflow",
			vlStatus:            http.StatusBadGateway,
			vlBody:              `{"error":"too many rows scanned by | stats by (trace_id)"}`,
			grafanaSourced:      true,
			expectStatus:        http.StatusOK,
			expectWarningHeader: true,
			expectUpstreamHdr:   true,
		},
		{
			name:                "Grafana_VL_503_queue_saturated",
			vlStatus:            http.StatusServiceUnavailable,
			vlBody:              `{"error":"too many concurrent queries"}`,
			grafanaSourced:      true,
			expectStatus:        http.StatusOK,
			expectWarningHeader: true,
			expectUpstreamHdr:   true,
		},
		{
			name:                "Grafana_VL_500_internal",
			vlStatus:            http.StatusInternalServerError,
			vlBody:              `{"error":"out of memory"}`,
			grafanaSourced:      true,
			expectStatus:        http.StatusOK,
			expectWarningHeader: true,
			expectUpstreamHdr:   true,
		},
		{
			name:                "NonGrafana_VL_502_passes_through",
			vlStatus:            http.StatusBadGateway,
			vlBody:              `{"error":"too many rows scanned"}`,
			grafanaSourced:      false,
			expectStatus:        http.StatusBadGateway, // real error visible
			expectWarningHeader: false,
			expectUpstreamHdr:   false,
		},
		{
			name:                "NonGrafana_VL_500_passes_through",
			vlStatus:            http.StatusInternalServerError,
			vlBody:              `{"error":"oom"}`,
			grafanaSourced:      false,
			expectStatus:        http.StatusInternalServerError,
			expectWarningHeader: false,
			expectUpstreamHdr:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.vlStatus)
				_, _ = w.Write([]byte(tc.vlBody))
			}))
			defer vlSrv.Close()

			p := newTestProxy(t, vlSrv.URL)

			req := httptest.NewRequest("POST", "/loki/api/v1/query_range",
				strings.NewReader(`query=sum(count_over_time({app="x"}[1m]))&start=1700000000000000000&end=1700001000000000000&step=60s`))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.grafanaSourced {
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			req.Header.Set("X-Scope-OrgID", "test-"+tc.name)
			_ = req.ParseForm()

			w := httptest.NewRecorder()
			p.proxyStatsQueryRangeDirect(w, req, `app:="x" | stats count() as v`)

			if w.Code != tc.expectStatus {
				t.Errorf("status: got %d, want %d (body=%q)", w.Code, tc.expectStatus, w.Body.String())
			}
			if tc.expectWarningHeader {
				if got := w.Header().Get("Warning"); got == "" {
					t.Errorf("expected Warning header, got none")
				}
				if got := w.Header().Get("X-Proxy-Upstream-Status"); got != strconv.Itoa(tc.vlStatus) {
					t.Errorf("X-Proxy-Upstream-Status: got %q, want %q", got, strconv.Itoa(tc.vlStatus))
				}
			} else {
				if got := w.Header().Get("Warning"); got != "" {
					t.Errorf("unexpected Warning header: %q", got)
				}
			}
			if tc.expectStatus == http.StatusOK {
				if !strings.Contains(w.Body.String(), `"status":"success"`) {
					t.Errorf("expected Loki success envelope, got: %s", w.Body.String())
				}
			}
		})
	}
}
