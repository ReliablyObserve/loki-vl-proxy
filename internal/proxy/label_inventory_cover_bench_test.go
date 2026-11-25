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
// VictoriaLogs that answers every call after a delay (none, 2 ms, or growing
// with the listing's range), per request:
//   - back=3617s is the A/B shape "refresh labels (label browser)": the
//     window filled once, then three windows each moved back 3617 s, with the
//     first window ending at several places in an hour two hours back, and
//     near now;
//   - refresh=30s is a Grafana refresh that the response cache cannot answer:
//     the filled window, ending 20 s before an hour (the cache key's bucket
//     for a 24h window), moved forward by 30 s;
//   - cold is the first request, nothing cached.
//
// It reports the VictoriaLogs calls and the listed range (minutes of rows
// read) per request, background fills included.
func BenchmarkLabelsShiftedWindow(b *testing.B) {
	now := time.Now()
	type mode struct {
		name     string
		fill     time.Time   // zero: nothing filled first
		measured []time.Time // window ends
		// staleness is -metadata-max-staleness: 0 keeps every listing on the
		// path that reads VictoriaLogs; above 0 a names-only listing may be
		// answered from memory, and background fills run between requests.
		staleness time.Duration
	}
	back := func(name string, end0 time.Time) mode {
		return mode{name: "back=3617s/" + name, fill: end0, measured: []time.Time{end0.Add(-3617 * time.Second), end0.Add(-2 * 3617 * time.Second), end0.Add(-3 * 3617 * time.Second)}}
	}
	// Sealed and unaligned, at fixed places in an hour two hours back (every
	// hour of the moved windows' ends has ended an hour ago), and near now as
	// the A/B run asks (the first moved window's end lies in an hour that
	// ended less than an hour ago; the place in the hour is the wall clock's).
	hourAgo2 := now.Truncate(time.Hour).Add(-2 * time.Hour)
	end0 := hourAgo2.Add(26*time.Minute + 50*time.Second)
	refresh0 := hourAgo2.Add(59*time.Minute + 40*time.Second)
	modes := []mode{back("at=:26:50", end0)}
	for _, at := range []time.Duration{5*time.Minute + 13*time.Second, 20*time.Minute + 13*time.Second, 35*time.Minute + 13*time.Second, 50*time.Minute + 13*time.Second} {
		modes = append(modes, back(fmt.Sprintf("at=:%02d:13", int(at/time.Minute)), hourAgo2.Add(at)))
	}
	memory := back("at=:26:50/staleness=10s", end0)
	memory.staleness = 10 * time.Second
	modes = append(modes,
		memory,
		back("at=now", now.Truncate(time.Minute).Add(-time.Minute-13*time.Second)),
		mode{name: "refresh=30s", fill: refresh0, measured: []time.Time{refresh0.Add(30 * time.Second)}},
		mode{name: "cold", measured: []time.Time{end0}},
	)
	for _, mode := range modes {
		for _, cost := range []struct {
			name  string
			delay func(r *http.Request) time.Duration
		}{
			{"vl_delay=0s", func(*http.Request) time.Duration { return 0 }},
			{"vl_delay=2ms", func(*http.Request) time.Duration { return 2 * time.Millisecond }},
			// A listing's cost grows with the rows it reads: 0.5 ms plus 0.1 ms
			// per minute of range (an hour 6.5 ms); a row count reads block
			// headers only, 0.5 ms.
			{"vl_delay=span", func(r *http.Request) time.Duration {
				if r.URL.Path != "/select/logsql/stream_field_names" {
					return 500 * time.Microsecond
				}
				a, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
				e, _ := strconv.ParseInt(r.FormValue("end"), 10, 64)
				return 500*time.Microsecond + time.Duration(e-a)/time.Minute*100*time.Microsecond
			}},
		} {
			b.Run(mode.name+"/"+cost.name, func(b *testing.B) {
				vl := &fakeVictoriaLogs{}
				seedStableLabels(vl, now)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(cost.delay(r))
					vl.ServeHTTP(w, r)
				}))
				defer srv.Close()
				calls, requests := 0, 0
				var listed time.Duration
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Hour, 100000), LogLevel: "error", MetadataMaxStaleness: mode.staleness})
					if err != nil {
						b.Fatal(err)
					}
					if !mode.fill.IsZero() {
						_ = labelsAnswer(b, p, mode.fill.Add(-24*time.Hour), mode.fill)
					}
					if mode.staleness > 0 {
						time.Sleep(200 * time.Millisecond) // the first listing's background fills
					}
					vl.reset()
					b.StartTimer()
					for _, end := range mode.measured {
						_ = labelsAnswer(b, p, end.Add(-24*time.Hour), end)
						if mode.staleness > 0 {
							b.StopTimer()
							time.Sleep(50 * time.Millisecond) // the time between two requests
							b.StartTimer()
						}
					}
					b.StopTimer()
					c, spans := vl.reset()
					for _, d := range spans {
						listed += d
					}
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
				b.ReportMetric(listed.Minutes()/float64(requests), "listed_min/request")
			})
		}
	}
}
