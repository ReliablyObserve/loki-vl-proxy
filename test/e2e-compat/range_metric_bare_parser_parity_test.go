//go:build e2e

package e2e_compat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// vlPathRecorder forwards to VictoriaLogs and counts the requested paths.
type vlPathRecorder struct {
	mu    sync.Mutex
	paths map[string]int
}

func (rec *vlPathRecorder) reset() map[string]int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	paths := rec.paths
	rec.paths = map[string]int{}
	return paths
}

// newDeclaredStreamFieldsProxy starts an in-process proxy with declared stream
// label fields, which serves bare count_over_time and rate sliding windows from
// VictoriaLogs /hits instead of stats_query_range. Its backend requests pass
// through a recorder so tests can prove which route answered.
func newDeclaredStreamFieldsProxy(t *testing.T) (string, *vlPathRecorder) {
	t.Helper()
	target, err := url.Parse(vlURL)
	if err != nil {
		t.Fatal(err)
	}
	rec := &vlPathRecorder{paths: map[string]int{}}
	forward := httputil.NewSingleHostReverseProxy(target)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.paths[r.URL.Path]++
		rec.mu.Unlock()
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)

	p, err := proxy.New(proxy.Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", StreamFields: []string{"service_name", "detected_level"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.ValidateBackendVersionCompatibility(ctx); err != nil {
		t.Fatalf("backend version probe: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL, rec
}

// Bare parser range metrics (no outer aggregation) must evaluate Loki's
// (t-range, t] windows exactly for range != step and range == step, with lines
// on every window edge, for every route that serves them: stats_query_range
// buckets, /hits buckets with declared stream fields, and the raw evaluators.
func TestRangeMetricCompatibilityBareParserWindows(t *testing.T) {
	fx := ensureSlidingFixtures(t)
	s0 := fx.s0
	start, end := s0.Add(-2*time.Minute), s0.Add(33*time.Minute)
	declared, recorder := newDeclaredStreamFieldsProxy(t)
	// Steady-state line counts per window prove the windows cover the edges.
	steady := map[string]string{"90s": "9", "2m": "12", "1m": "6"}

	// route is the backend path the declared-stream-fields proxy must use.
	type bareCase struct{ query, window, route string }
	const (
		hitsPath  = "/select/logsql/hits"
		statsPath = "/select/logsql/stats_query_range"
		rawPath   = "/select/logsql/query"
	)
	var cases []bareCase
	for _, fn := range []string{"count_over_time", "bytes_over_time", "rate"} {
		for _, window := range []string{"90s", "2m", "1m"} {
			route := statsPath
			if fn != "bytes_over_time" && window != "1m" {
				route = hitsPath
			}
			cases = append(cases,
				bareCase{fn + "(" + fx.bareLogfmt.selector() + " | logfmt [" + window + "])", window, route},
				// Bare | json keeps the ordered JSON evaluator over raw lines.
				bareCase{fn + "(" + fx.bareJSON.selector() + " | json [" + window + "])", window, rawPath})
		}
	}
	for _, fn := range []string{"sum_over_time", "max_over_time", "min_over_time"} {
		for _, window := range []string{"90s", "1m"} {
			cases = append(cases, bareCase{fn + "(" + fx.bareUnwrap.selector() + " | logfmt | unwrap n [" + window + "])", window, statsPath})
		}
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, nil)
			if len(loki) != 1 {
				t.Fatalf("Loki sanity: expected one series for the single-stream fixture, got %d: %v", len(loki), loki)
			}
			if strings.HasPrefix(tc.query, "count_over_time") {
				for _, points := range loki {
					if got := points[strconv.FormatInt(s0.Add(15*time.Minute).Unix(), 10)]; got != steady[tc.window] {
						t.Fatalf("Loki sanity: expected %s lines per [%s] window, got %q", steady[tc.window], tc.window, got)
					}
				}
			}
			assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, time.Minute, nil))

			recorder.reset()
			hits := slidingRangeSeries(t, declared, tc.query, start, end, time.Minute, nil)
			paths := recorder.reset()
			if paths[tc.route] == 0 {
				t.Fatalf("declared stream fields: expected a %s request, got %v", tc.route, paths)
			}
			if tc.route != rawPath && paths[rawPath] != 0 {
				t.Fatalf("declared stream fields: bucket route fell back to raw lines: %v", paths)
			}
			if len(hits) != 1 {
				t.Fatalf("declared stream fields: expected one series, got %d: %v", len(hits), hits)
			}
			// Declared fields are mapped to VictoriaLogs names (detected_level is
			// read from a level field), so the /hits route labels this fixture's
			// series differently from Loki: compare the single series' samples.
			assertSlidingParity(t, tc.query+" [declared stream fields]", unlabelledSlidingSeries(loki), unlabelledSlidingSeries(hits))
		})
	}
}

// unlabelledSlidingSeries keys the samples of a single-series result by an
// empty label set.
func unlabelledSlidingSeries(series map[string]map[string]string) map[string]map[string]string {
	for _, points := range series {
		return map[string]map[string]string{"": points}
	}
	return nil
}
