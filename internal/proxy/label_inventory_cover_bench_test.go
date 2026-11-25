package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// seedStableLabels writes three days of rows whose streams always carry the
// same label names (app, env, pod); only the values rotate, as on a cluster
// whose pods restart.
func seedStableLabels(f *fakeVictoriaLogs, now time.Time) {
	start := now.Add(-3 * 24 * time.Hour).Truncate(time.Hour)
	for ts := start; ts.Before(now); ts = ts.Add(20 * time.Second) {
		pod := "pod-" + strconv.Itoa(int(ts.Sub(start)/(3*time.Hour)))
		f.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api", "env": "prod", "pod": pod}})
	}
}

// labelsAnswer asks p for /labels over [start, end] (Unix ns) and returns the body.
func labelsAnswer(t testing.TB, p *Proxy, start, end time.Time) string {
	t.Helper()
	q := url.Values{"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+q.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	p.handleLabels(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/labels [%s, %s]: %d %s", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

// BenchmarkLabelsShiftedWindow times /labels over a 24h window against a
// VictoriaLogs that answers every call after delay, per request:
//   - back=3617s is the A/B shape "refresh labels (label browser)": the
//     window filled once, then three windows each moved back 3617 s;
//   - refresh=30s is a Grafana refresh that the response cache cannot answer:
//     the filled window, ending 20 s before an hour (the cache key's bucket
//     for a 24h window), moved forward by 30 s;
//   - cold is the first request, nothing cached.
//
// It reports the VictoriaLogs calls per request.
func BenchmarkLabelsShiftedWindow(b *testing.B) {
	now := time.Now()
	// Sealed, unaligned and at a fixed place in its hour.
	end0 := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	refresh0 := now.Truncate(time.Hour).Add(-2*time.Hour + 59*time.Minute + 40*time.Second)
	for _, mode := range []struct {
		name     string
		fill     time.Time   // zero: nothing filled first
		measured []time.Time // window ends
	}{
		{"back=3617s", end0, []time.Time{end0.Add(-3617 * time.Second), end0.Add(-2 * 3617 * time.Second), end0.Add(-3 * 3617 * time.Second)}},
		{"refresh=30s", refresh0, []time.Time{refresh0.Add(30 * time.Second)}},
		{"cold", time.Time{}, []time.Time{end0}},
	} {
		for _, delay := range []time.Duration{0, 2 * time.Millisecond} {
			b.Run(mode.name+"/vl_delay="+delay.String(), func(b *testing.B) {
				vl := &fakeVictoriaLogs{}
				seedStableLabels(vl, now)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(delay)
					vl.ServeHTTP(w, r)
				}))
				defer srv.Close()
				calls, requests := 0, 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Hour, 100000), LogLevel: "error"})
					if err != nil {
						b.Fatal(err)
					}
					if !mode.fill.IsZero() {
						_ = labelsAnswer(b, p, mode.fill.Add(-24*time.Hour), mode.fill)
					}
					vl.reset()
					b.StartTimer()
					for _, end := range mode.measured {
						_ = labelsAnswer(b, p, end.Add(-24*time.Hour), end)
					}
					b.StopTimer()
					c, _ := vl.reset()
					for path, n := range c {
						if path != "/metrics" && path != "/health" {
							calls += n
						}
					}
					requests += len(mode.measured)
					_ = p.Shutdown(context.Background())
					b.StartTimer()
				}
				b.ReportMetric(float64(calls)/float64(requests), "vl_calls/request")
			})
		}
	}
}
