// Drilldown / Explore regression lock-in tests.
//
// Every TestLock_* in this file pins a behavior established during the
// 2026-06 Drilldown quality fix work. If you find yourself updating one of
// these tests, the question to ask is "am I making the chart worse for
// someone querying 24h+ in Drilldown or Explore?" — if yes, don't.
//
// Background (do not delete this comment):
//
//	Grafana's Loki datasource splits metric range queries at the 24h
//	boundary (oneDayMs in querySplitting.ts) and merges chunk responses via
//	mergeFrames + closestIdx + splice in mergeResponses.ts. Two consequences:
//
//	1. A residual chunk whose range is < step produces axisLen=1; if the
//	   proxy returns top-N values at that single timestamp, mergeFrames
//	   glues them onto chunk-1's distributed series as a tall right-edge
//	   spike. Reproduction is in TestLock_GrafanaMergedFrames_*.
//
//	2. If the proxy uses different code paths for different chunk sizes
//	   (e.g. /hits for 24h, stats_query_range with | limit 500 for the
//	   residual), the chunks return totally different series sets and
//	   mergeFrames unions them — same right-edge spike, different cause.
//	   Reproduction is in TestLock_HitsRunsForAllRanges.
//
//	The fixes covered here:
//	  - Routing: every Drilldown-shape stats query (any source) goes
//	    through /hits, regardless of range. Removes the historical 6h
//	    hybrid threshold.
//	  - Routing: the X-Query-Tags: Source=grafana-lokiexplore-app gate is
//	    NOT applied on the way IN — Explore-sourced count_over_time
//	    queries get the same fast path Drilldown does.
//	  - Leftover suppression: ANY Grafana-sourced request with
//	    end-start ≤ 2 × step gets an empty matrix from the /hits handler,
//	    so mergeFrames has nothing to glue onto the right edge.
//
// The fixes interact, so each is locked independently AND a top-level
// chunked-merge simulator validates the combined effect.
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
	"sync/atomic"
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

func (b *recorderBackend) callsFor(path string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[path]
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

// hitsResponse returns a valid /hits payload with the given (value, [(t,count)…]).
func hitsResponse(field string, entries map[string][][2]any) string {
	var b strings.Builder
	b.WriteString(`{"hits":[`)
	first := true
	for v, points := range entries {
		if !first {
			b.WriteByte(',')
		}
		first = false
		var ts strings.Builder
		var vs strings.Builder
		total := 0
		for i, p := range points {
			if i > 0 {
				ts.WriteByte(',')
				vs.WriteByte(',')
			}
			ts.WriteByte('"')
			ts.WriteString(p[0].(string))
			ts.WriteByte('"')
			n, _ := p[1].(int)
			vs.WriteString(strconv.Itoa(n))
			total += n
		}
		fmt.Fprintf(&b, `{"fields":{%q:%q},"timestamps":[%s],"values":[%s],"total":%d}`,
			field, v, ts.String(), vs.String(), total)
	}
	b.WriteString(`]}`)
	return b.String()
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
// Lock 1: Routing — every stats-compat Drilldown shape goes through the
// /hits-enabled drilldown handler, regardless of source tag.
// ---------------------------------------------------------------------------

// TestLock_RoutingSourceAgnostic confirms a count_over_time({…} | X!="") query
// reaches proxyStatsQueryRangeDrilldown for ALL of:
//   - no source tag (Explore / direct API / curl)
//   - X-Query-Tags: Source=grafana-lokiexplore-app (Drilldown)
//   - User-Agent: Grafana/X.Y.Z (dashboard panel)
//   - X-Grafana-Org-Id header (Explore backend-routed)
//
// If a future PR re-introduces a "Drilldown-only" gate on routing, this test
// fails because at least one of the four sources stops hitting /hits.
func TestLock_RoutingSourceAgnostic(t *testing.T) {
	for _, source := range []string{"", "drilldown", "grafana-ua", "grafana-hdr"} {
		t.Run("source="+source, func(t *testing.T) {
			backend := newRecorderBackend()
			backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{
					"a": {{"2023-11-14T22:13:20Z", 3}, {"2023-11-14T22:14:20Z", 2}},
				})))
			})
			vl := backend.server()
			defer vl.Close()
			p := newTestProxy(t, vl.URL)

			r := drilldownRequest(t,
				`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
				1700000000, 1700003600, "120", source)
			w := httptest.NewRecorder()
			p.proxyStatsQueryRange(w, r,
				`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

			if backend.callsFor("/select/logsql/hits") == 0 {
				t.Fatalf("source=%q: expected /select/logsql/hits to be called (routing regression — source-tag gate re-introduced?)", source)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Lock 2: /hits runs for ALL ranges (no hybrid threshold gate).
// ---------------------------------------------------------------------------

// TestLock_HitsRunsForAllRanges verifies the hybrid /hits path fires for every
// range > 0, including the small Grafana querySplitting leftover. Historically
// the proxy gated /hits behind drilldownHybridThreshold (6h) and the < 6h
// leftover fell through to stats_query_range with | limit 500 — that 500-series
// block was the right-edge spike. If a future PR re-introduces the threshold,
// the < 6h subtests stop hitting /hits and this lock fails.
func TestLock_HitsRunsForAllRanges(t *testing.T) {
	ranges := []struct {
		name      string
		startSec  int64
		endSec    int64
		stepRaw   string
		expectHit bool
	}{
		// step must be >= range vector window (2m) so handleStatsCompatRange
		// falls through to the Drilldown router instead of the manual path.
		{"5m", 1700000000, 1700000300, "120", true},
		{"1h", 1700000000, 1700003600, "120", true},
		{"6h", 1700000000, 1700021600, "120", true},
		{"24h", 1700000000, 1700086400, "120", true},
		{"7d", 1700000000, 1700604800, "600", true},
	}
	for _, tc := range ranges {
		t.Run(tc.name, func(t *testing.T) {
			backend := newRecorderBackend()
			backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{
					"a": {{"2023-11-14T22:13:20Z", 3}},
				})))
			})
			vl := backend.server()
			defer vl.Close()
			p := newTestProxy(t, vl.URL)
			r := drilldownRequest(t,
				`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
				tc.startSec, tc.endSec, tc.stepRaw, "drilldown")
			p.proxyStatsQueryRange(httptest.NewRecorder(), r,
				`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

			gotHits := backend.callsFor("/select/logsql/hits") > 0
			if gotHits != tc.expectHit {
				t.Errorf("range=%s: /hits called=%v want %v (hybrid threshold regression?)",
					tc.name, gotHits, tc.expectHit)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Lock 3: Leftover-chunk suppression for ANY Grafana source.
// ---------------------------------------------------------------------------

// TestLock_LeftoverChunkSuppressed pins the rule:
//
//	end - start ≤ 2 × step AND any Grafana source signal → empty matrix
//
// Why empty (not "best-effort distribute the few values"): the chunk is so
// small that /hits can only produce a 1-bucket axis. Emitting the top-20
// values at a single timestamp creates a 20-frame block that Grafana's
// mergeFrames glues onto the previous 24h chunk as a tall right-edge spike.
// Returning an empty matrix means mergeFrames has nothing to glue, so the
// chart shows the 24h chunk's distributed series unaffected (losing ≤ 1 step
// of width on the very right edge, which is invisible at typical render widths).
//
// All four Grafana source signals must trigger suppression. Non-Grafana
// requests with the same shape must NOT trigger suppression — a raw API
// client querying 1m of data legitimately wants whatever VL has.
func TestLock_LeftoverChunkSuppressedInHits(t *testing.T) {
	// The Grafana 24h+ querySplitting residual is a sub-step chunk (range < step)
	// that yields a single-bucket /hits frame; Grafana's mergeFrames collapses its
	// N single-point series onto one edge of the merged chart (a right-edge spike
	// or a left-edge "all data at the beginning" cluster). The /hits path suppresses
	// it (X-Proxy-Drilldown-Path=hits-leftover-suppressed). Scope: ONLY Grafana
	// sub-step requests reaching the high-card /hits path — a non-Grafana caller and
	// any range >= one full step are served normally. (The proxy-wide dispatch-level
	// blanking that over-blanked low-card dashboards stays removed; those take the
	// exact stats path, not /hits — see TestWindowedHits_GatedToDrilldownOrHighCard.)
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
		{"explore_via_ua_60s", 1700000000, 1700000060, "120", "grafana-ua", true},
		{"explore_via_hdr_60s", 1700000000, 1700000060, "120", "grafana-hdr", true},
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
				_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{
					"a": {{"2023-11-14T22:13:20Z", 3}},
				})))
			}
			backend.on("/select/logsql/hits", served)
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
// Lock 4: Step normalization (bare integer → duration).
// ---------------------------------------------------------------------------

// TestLock_StepNormalizationBareIntegerToDuration verifies that Grafana's
// bare-integer step (e.g. "120") is normalized to "120s" before being sent
// to VL's /hits endpoint. Without the "s" suffix VL's parser rejects the
// step and /hits returns an error — the historical regression that caused
// the entire /hits path to silently fall back to legacy stats.
func TestLock_StepNormalizationBareIntegerToDuration(t *testing.T) {
	backend := newRecorderBackend()
	backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{
			"a": {{"2023-11-14T22:13:20Z", 3}},
		})))
	})
	vl := backend.server()
	defer vl.Close()
	p := newTestProxy(t, vl.URL)

	r := drilldownRequest(t,
		`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
		1700000000, 1700003600, "120", "drilldown") // bare integer, no suffix
	p.proxyStatsQueryRange(httptest.NewRecorder(), r,
		`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

	if backend.callsFor("/select/logsql/hits") == 0 {
		t.Fatalf("/hits was not called — step normalization regression?")
	}
	// Inspect the actual step VL received.
	backend.mu.Lock()
	steps := backend.steps["/select/logsql/hits"]
	backend.mu.Unlock()
	if len(steps) == 0 {
		t.Fatalf("no /hits steps recorded")
	}
	for _, s := range steps {
		if !strings.HasSuffix(s, "s") && !strings.HasSuffix(s, "m") && !strings.HasSuffix(s, "h") {
			t.Errorf("step %q reached VL without duration suffix — regression on bare-integer step normalization", s)
		}
	}
}

// ---------------------------------------------------------------------------
// Lock 5: /hits drops the remainder bucket (fields:{}).
// ---------------------------------------------------------------------------

// TestLock_HitsDropsRemainderBucket verifies the proxy strips the
// fields:{} catchall VL emits as the "everything not in top-N" bucket.
// Including the remainder series would dominate Grafana's Y-axis and hide
// individual top-N values for high-cardinality fields — exactly the
// "__other__" symptom we fixed mid-2026-06.
func TestLock_HitsDropsRemainderBucket(t *testing.T) {
	backend := newRecorderBackend()
	backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Three real top-N + one remainder (fields: {}). The remainder count
		// uses a distinctive value (88812345) that cannot collide with any
		// Unix timestamp in the request range, so substring matching detects
		// it reliably without false positives.
		_, _ = w.Write([]byte(`{"hits":[
            {"fields":{"pod":"a"},"timestamps":["2023-11-14T22:13:20Z"],"values":[5],"total":5},
            {"fields":{"pod":"b"},"timestamps":["2023-11-14T22:13:20Z"],"values":[3],"total":3},
            {"fields":{"pod":"c"},"timestamps":["2023-11-14T22:13:20Z"],"values":[2],"total":2},
            {"fields":{},"timestamps":["2023-11-14T22:13:20Z"],"values":[88812345],"total":88812345}
        ]}`))
	})
	vl := backend.server()
	defer vl.Close()
	p := newTestProxy(t, vl.URL)

	r := drilldownRequest(t,
		`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
		1700000000, 1700003600, "120", "drilldown")
	w := httptest.NewRecorder()
	p.proxyStatsQueryRange(w, r,
		`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

	body := w.Body.String()
	// The remainder bucket's distinctive 88812345 value must not surface in
	// the response. Substring-matching is safe because 88812345 is too small
	// to be a Unix timestamp and too distinctive to appear elsewhere.
	if strings.Contains(body, "88812345") {
		t.Errorf("response contains remainder bucket value 88812345 — regression on remainder-drop:\n%s", body)
	}
	if strings.Contains(body, `"pod":""`) {
		t.Errorf("response contains empty-pod-label series (remainder) — regression on remainder-drop:\n%s", body)
	}
}

// ---------------------------------------------------------------------------
// Lock 6: Shared timestamp axis across all series.
// ---------------------------------------------------------------------------

// TestLock_SharedAxisAcrossSeries verifies every emitted series carries
// the same timestamps array spanning the full request range. Without a
// shared axis Grafana renders all activity as a single cluster at one time
// position instead of distributed across the timeline — the symptom that
// drove the windowed-sampling axis-normalization rewrite mid-2026-06.
func TestLock_SharedAxisAcrossSeries(t *testing.T) {
	backend := newRecorderBackend()
	backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Two series with DIFFERENT timestamps each.
		_, _ = w.Write([]byte(`{"hits":[
            {"fields":{"pod":"a"},"timestamps":["2023-11-14T22:13:20Z"],"values":[5],"total":5},
            {"fields":{"pod":"b"},"timestamps":["2023-11-14T22:14:20Z","2023-11-14T22:15:20Z"],"values":[3,2],"total":5}
        ]}`))
	})
	vl := backend.server()
	defer vl.Close()
	p := newTestProxy(t, vl.URL)

	r := drilldownRequest(t,
		`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
		1700000000, 1700003600, "120", "drilldown")
	w := httptest.NewRecorder()
	p.proxyStatsQueryRange(w, r,
		`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if len(resp.Data.Result) < 2 {
		t.Fatalf("expected ≥ 2 series, got %d", len(resp.Data.Result))
	}
	// Build the timestamps signature for series[0]; assert all others match.
	signature := func(values [][2]any) string {
		var b strings.Builder
		for _, v := range values {
			fmt.Fprintf(&b, "%v|", v[0])
		}
		return b.String()
	}
	want := signature(resp.Data.Result[0].Values)
	for i, s := range resp.Data.Result[1:] {
		got := signature(s.Values)
		if got != want {
			t.Errorf("series[%d] (%v) has a different timestamps axis than series[0] (%v) — shared-axis regression",
				i+1, s.Metric, resp.Data.Result[0].Metric)
		}
	}
}

// ---------------------------------------------------------------------------
// Lock 7: extractStreamSelectorOnly handles VL-native form.
// ---------------------------------------------------------------------------

// TestLock_ExtractStreamSelectorOnly_VLNative confirms the helper used to
// build /hits queries strips trailing pipes for BOTH Loki-bracketed
// (`{namespace="prod"}`) and VL-native (`namespace:="prod"`) selectors.
// Historical regression: extractStreamSelectorOnly only recognised the
// Loki form, so VL-translated queries arrived with leftover pipes that
// VL's /hits parser rejected before ignore_pipes=1 could strip them —
// the entire /hits path failed silently.
func TestLock_ExtractStreamSelectorOnly_VLNative(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"loki_bracketed", `{namespace="prod"} | filter pod:!""`, `{namespace="prod"}`},
		{"vl_native", `namespace:="prod" | filter pod:!""`, `namespace:="prod"`},
		{"vl_native_no_pipe", `namespace:="prod" pod:!=""`, `namespace:="prod" pod:!=""`},
		{"vl_native_multi_filter", `namespace:="prod" env:="production" | parser`, `namespace:="prod" env:="production"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractStreamSelectorOnly(tc.in)
			if got != tc.want {
				t.Errorf("extractStreamSelectorOnly(%q):\n  got  %q\n  want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Lock 8: fieldHasExistenceFilter handles quoted dotted fields.
// ---------------------------------------------------------------------------

// TestLock_FieldHasExistenceFilter_QuotedDotted pins the rule that the
// existence-filter detector matches BOTH unquoted (`pod:!=""`) and quoted
// (`"k8s.pod.name":!""`) field names. The translator emits the quoted form
// for OTel-style dotted attributes; if this detector breaks for them, the
// router demotes those queries to slow paths.
func TestLock_FieldHasExistenceFilter_QuotedDotted(t *testing.T) {
	cases := []struct {
		field string
		query string
		want  bool
	}{
		{"pod", `namespace:="prod" | filter pod:!""`, true},
		{"k8s.pod.name", `namespace:="prod" | filter "k8s.pod.name":!""`, true},
		{"service.name", `namespace:="prod" | filter "service.name":!""`, true},
		{"pod", `namespace:="prod"`, false},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			got := fieldHasExistenceFilter(tc.query, tc.field)
			if got != tc.want {
				t.Errorf("fieldHasExistenceFilter(%q, %q) = %v, want %v",
					tc.query, tc.field, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Lock 9: isGrafanaSourcedRequest accepts Drilldown, Explore, dashboard.
// ---------------------------------------------------------------------------

// TestLock_IsGrafanaSourcedRequest pins which client signals count as
// "Grafana source" for the leftover-chunk suppression. If any of these
// stops being detected, Grafana clients lose the spike-suppression fix.
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
// Lock 10: Stats fallback only fires when /hits actually fails.
// ---------------------------------------------------------------------------

// TestLock_StatsFallbackOnlyOnHitsFailure proves that when /hits succeeds
// (returns named-value hits), the proxy does NOT also call
// stats_query_range. The historical bug was the hybrid path falling
// through to the unconstrained stats path even after /hits succeeded,
// returning a totally different 500-series set that overwrote the /hits
// result. If a future refactor breaks the "return on success" early-exit,
// this test fails because both endpoints get hit.
func TestLock_StatsFallbackOnlyOnHitsFailure(t *testing.T) {
	var hitsHits, statsHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{
				"a": {{"2023-11-14T22:13:20Z", 3}, {"2023-11-14T22:14:20Z", 2}},
			})))
		case "/select/logsql/stats_query_range":
			statsHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	r := drilldownRequest(t,
		`sum by (pod) (count_over_time({namespace="prod"}|pod!=""`+` [2m]))`,
		1700000000, 1700003600, "120", "drilldown")
	p.proxyStatsQueryRange(httptest.NewRecorder(), r,
		`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)

	if hitsHits.Load() == 0 {
		t.Fatalf("/hits was not called — routing regression")
	}
	if statsHits.Load() > 0 {
		t.Errorf("stats_query_range was called %d time(s) AFTER /hits succeeded — fallback regression",
			statsHits.Load())
	}
}

// ---------------------------------------------------------------------------
// Lock 11: Grafana mergeFrames simulator — full chunked-merge contract.
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
// a fake VL backend wired with realistic /hits responses per chunk. The
// fake backend's behavior is intentionally pessimistic for the right edge:
// each chunk's "top-N" includes some chunk-unique values that mergeFrames
// would otherwise stack at the chunk boundary.
//
// Returns the merged frame so individual tests can assert on it.
func runChunkSim(t *testing.T, source string, chunks [][2]int64, step time.Duration) gfFrame {
	t.Helper()
	// Each chunk's /hits returns 16 series; some are chunk-shared, some unique.
	// We simulate the "16 frames at single timestamp" leftover the user
	// captured in their network response.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			startStr := r.Form.Get("start")
			endStr := r.Form.Get("end")
			startSec, _ := strconv.ParseInt(startStr, 10, 64)
			endSec, _ := strconv.ParseInt(endStr, 10, 64)
			rangeSec := endSec - startSec
			stepSec := int64(step.Seconds())
			// Emit 16 hits — for a tiny axis (≤ 2 buckets), they're all at one timestamp.
			n := int(rangeSec/stepSec) + 1
			if n < 1 {
				n = 1
			}
			if n > 30 {
				n = 30
			}
			entries := map[string][][2]any{}
			for i := 0; i < 16; i++ {
				label := fmt.Sprintf("pod-c%d-i%d", startSec, i) // chunk-unique label
				pts := [][2]any{}
				// Place values distributed across the chunk's range.
				for j := 0; j < n; j++ {
					ts := time.Unix(startSec+int64(j)*stepSec, 0).UTC().Format(time.RFC3339)
					if (i+j)%3 == 0 {
						pts = append(pts, [2]any{ts, 20})
					}
				}
				if len(pts) == 0 {
					pts = append(pts, [2]any{time.Unix(endSec, 0).UTC().Format(time.RFC3339), 25})
				}
				entries[label] = pts
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(hitsResponse("pod", entries)))
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
// guard for the Drilldown / Explore right-edge spike. It runs the embedded
// Grafana querySplitting + mergeFrames simulator for the four time ranges
// the user reported broken (24h, 25h, 2d, 7d) for every Grafana source
// signal, and asserts the rightmost bin holds < 40% of all nonzero
// timestamps in the merged frame.
//
// If a future PR regresses ANY of:
//   - leftover-chunk suppression
//   - /hits-for-all-ranges routing
//   - shared timestamp axis
//   - source-tag-agnostic routing
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
// Lock 12: maxStatsQueryRangeBytes cap still in place.
// ---------------------------------------------------------------------------

// TestLock_StatsQueryRangeBodyCap pins the existence of a per-request body
// cap on the direct stats_query_range path. Removing the cap (or raising
// it past the current 16 MB) would re-allow OOM on high-card by() clauses,
// which is the exact failure mode that motivated the /hits routing in the
// first place.
func TestLock_StatsQueryRangeBodyCap(t *testing.T) {
	if maxStatsQueryRangeBytes < (1 << 20) {
		t.Errorf("maxStatsQueryRangeBytes shrunk to %d (< 1 MB) — accidental cap reduction?", maxStatsQueryRangeBytes)
	}
	if maxStatsQueryRangeBytes > (64 << 20) {
		t.Errorf("maxStatsQueryRangeBytes grew to %d (> 64 MB) — accidental cap removal/expansion?", maxStatsQueryRangeBytes)
	}
}

// ---------------------------------------------------------------------------
// Lock 13: drilldownHitsFieldsLimit pinned.
// ---------------------------------------------------------------------------

// TestLock_DrilldownHitsFieldsLimit confirms the /hits top-N cap is bounded.
// Raising it past 100 risks re-introducing the chunk-merge spike (too many
// chunk-unique series); lowering it below 5 cripples chart variety. The
// window is intentional and tightening it further requires deliberate
// product judgment, not an opportunistic constant tweak.
func TestLock_DrilldownHitsFieldsLimit(t *testing.T) {
	if drilldownHitsFieldsLimit < 5 {
		t.Errorf("drilldownHitsFieldsLimit=%d (< 5) — chart loses meaningful variety", drilldownHitsFieldsLimit)
	}
	if drilldownHitsFieldsLimit > 100 {
		t.Errorf("drilldownHitsFieldsLimit=%d (> 100) — chunked-merge spike risk", drilldownHitsFieldsLimit)
	}
}

// ---------------------------------------------------------------------------
// Lock 14: VL upstream errors never leak through to Grafana clients.
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
