package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// joinCheckTransport tracks the VictoriaLogs calls in flight: a call is in
// flight from RoundTrip until it returns an error or its body is closed. A
// call hold picks is kept until its context ends and returns late (an HTTP
// call already on the wire does not return the instant it is cancelled); a
// call decide picks is answered by decide once a held call is in flight, so
// the verdict it carries arrives while that call still runs.
type joinCheckTransport struct {
	inner  http.RoundTripper
	late   time.Duration
	hold   func(path, query string) bool
	decide func(path, query string) (status int, body string, ok bool)

	mu       sync.Mutex
	inFlight int
	held     int
	decided  int
	log      []string
	holding  chan struct{}
	once     sync.Once
}

func newJoinCheckTransport(inner http.RoundTripper) *joinCheckTransport {
	return &joinCheckTransport{inner: inner, late: 30 * time.Millisecond, holding: make(chan struct{})}
}

type closeHookBody struct {
	io.ReadCloser
	once sync.Once
	done func()
}

func (b *closeHookBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

func (j *joinCheckTransport) end() {
	j.mu.Lock()
	j.inFlight--
	j.mu.Unlock()
}

func (j *joinCheckTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	query := req.URL.Query().Get("query")
	if req.GetBody != nil {
		if body, err := req.GetBody(); err == nil {
			raw, _ := io.ReadAll(body)
			if form, err := url.ParseQuery(string(raw)); err == nil && form.Get("query") != "" {
				query = form.Get("query")
			}
		}
	}
	path := req.URL.Path
	j.mu.Lock()
	j.inFlight++
	j.log = append(j.log, path+" "+query)
	j.mu.Unlock()
	if j.hold != nil && j.hold(path, query) {
		j.mu.Lock()
		j.held++
		j.mu.Unlock()
		j.once.Do(func() { close(j.holding) })
		<-req.Context().Done()
		time.Sleep(j.late)
		j.end()
		return nil, context.Cause(req.Context())
	}
	if j.decide != nil {
		if status, body, ok := j.decide(path, query); ok {
			select {
			case <-j.holding:
			case <-time.After(2 * time.Second):
			}
			j.mu.Lock()
			j.decided++
			j.mu.Unlock()
			resp := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Request: req}
			resp.Body = &closeHookBody{ReadCloser: io.NopCloser(strings.NewReader(body)), done: j.end}
			return resp, nil
		}
	}
	resp, err := j.inner.RoundTrip(req)
	if err != nil {
		j.end()
		return nil, err
	}
	resp.Body = &closeHookBody{ReadCloser: resp.Body, done: j.end}
	return resp, nil
}

func (j *joinCheckTransport) snapshot() (inFlight, held, decided int, log []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.inFlight, j.held, j.decided, append([]string(nil), j.log...)
}

// Every early return of a stats pushdown attempt (a probe's verdict or error
// that ends it before the buckets are read) cancels the stats query and the
// other probes and waits for them: when the attempt returns, no VictoriaLogs
// call it started is still in flight. The stats query is held until it is
// cancelled and returns late, as a call already on the wire does; the probe
// that decides answers while it is held.
//
// conformance: data-probing-and-freshness, semantics/json-error-filter-metric-split-pushdown, semantics/json-detected-level-error-split
func TestOrderedJSONStatsEarlyReturnsJoinTheirCalls(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	rows := braceOnlyRows(t, withStoredLevels(jsonSplitRows(rand.New(rand.NewSource(1)), s0, 200, false)), false)
	const (
		strict   = `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`
		valid    = `sum by (status) (count_over_time({env="prod"} | json | __error__="" [1m]))`
		levelled = `sum by (detected_level) (count_over_time({env="prod"} | json | __error__="" [1m]))`
	)
	unparsedProbe := func(q string) bool { return strings.Contains(q, "| filter -"+jsonObjectStartFilter+" | limit 1") }
	hazardProbe := func(q string) bool { return strings.Contains(q, jsonLokiRejectHazard) }
	foreignProbe := func(q string) bool {
		return strings.Contains(q, "severity_number:*") && strings.HasSuffix(q, ") | limit 1")
	}
	parseRiskProbe := func(q string) bool {
		return !unparsedProbe(q) && !hazardProbe(q) && !foreignProbe(q) && !isLevelPresenceQuery(q)
	}
	const riskyLine = `{"_time":"2023-11-14T22:15:00Z","_msg":"x"}` + "\n"
	for _, tc := range []struct {
		name        string
		query       string
		storedLevel bool
		probe       func(query string) bool
		status      int
		wantErr     bool
		wantParse   bool
	}{
		{"unparsed line found", strict, true, unparsedProbe, http.StatusOK, false, false},
		{"unparsed probe fails", strict, true, unparsedProbe, http.StatusInternalServerError, true, false},
		{"parse risk found", valid, false, parseRiskProbe, http.StatusOK, false, true},
		{"parse risk probe fails", valid, false, parseRiskProbe, http.StatusInternalServerError, true, false},
		{"foreign level field found", levelled, true, foreignProbe, http.StatusOK, false, false},
		{"foreign level probe fails", levelled, true, foreignProbe, http.StatusInternalServerError, true, false},
		{"hazard probe fails", valid, false, hazardProbe, http.StatusInternalServerError, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.jsonUnpack = vlFastjsonUnpackFields
			fake.presence = true
			p := newFilterPushdownProxy(t, srv.URL)
			fake.stored = p.labelTranslator.ToVL
			j := newJoinCheckTransport(p.client.Transport)
			j.hold = func(path, _ string) bool { return path == "/select/logsql/stats_query_range" }
			j.decide = func(path, query string) (int, string, bool) {
				if path != "/select/logsql/query" || !tc.probe(query) {
					return 0, "", false
				}
				if tc.status != http.StatusOK {
					return tc.status, "backend failed", true
				}
				return http.StatusOK, riskyLine, true
			}
			p.client.Transport = j
			plan, ok := compileOrderedJSONMetric(tc.query)
			if !ok || !plan.pushdown {
				t.Fatalf("%s: expected a pushdown plan", tc.query)
			}
			body, served, parseDisagreement, err := p.orderedJSONStatsBuckets(context.Background(), plan, start, end, time.Minute, tc.storedLevel, false)
			inFlight, held, decided, log := j.snapshot()
			if inFlight != 0 {
				t.Fatalf("%d VictoriaLogs calls still in flight after the attempt returned; calls: %q", inFlight, log)
			}
			if held == 0 || decided != 1 {
				t.Fatalf("the stats query was held %d times and %d probes decided, want a held stats query and one decision; calls: %q", held, decided, log)
			}
			if body != nil || served || parseDisagreement != tc.wantParse || (err != nil) != tc.wantErr {
				t.Fatalf("got served=%v parseDisagreement=%v err=%v, want an early return (parseDisagreement=%v, error=%v)", served, parseDisagreement, err, tc.wantParse, tc.wantErr)
			}
		})
	}
}

// anyRisk returns on the first risky verdict or error and cancels the other
// probes; it returns only once they have returned.
//
// conformance: data-probing-and-freshness
func TestAnyRiskWaitsForTheProbesItCancels(t *testing.T) {
	for _, tc := range []struct {
		name    string
		risky   bool
		err     error
		wantErr bool
	}{
		{"risky verdict", true, nil, false},
		{"probe error", false, errors.New("backend failed"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			running := 0
			started := make(chan struct{}, 2)
			slow := func(ctx context.Context) (bool, error) {
				mu.Lock()
				running++
				mu.Unlock()
				started <- struct{}{}
				<-ctx.Done()
				time.Sleep(30 * time.Millisecond)
				mu.Lock()
				running--
				mu.Unlock()
				return false, ctx.Err()
			}
			decide := func(context.Context) (bool, error) {
				<-started
				<-started
				return tc.risky, tc.err
			}
			risky, err := anyRisk(context.Background(), []func(context.Context) (bool, error){slow, slow, decide})
			mu.Lock()
			left := running
			mu.Unlock()
			if left != 0 {
				t.Fatalf("%d probes still running after anyRisk returned", left)
			}
			if risky != tc.risky || (err != nil) != tc.wantErr {
				t.Fatalf("anyRisk = %v, %v; want %v, error %v", risky, err, tc.risky, tc.wantErr)
			}
		})
	}
	if risky, err := anyRisk(context.Background(), []func(context.Context) (bool, error){
		func(context.Context) (bool, error) { return false, nil },
		func(context.Context) (bool, error) { return false, nil },
	}); risky || err != nil {
		t.Fatalf("clean probes: anyRisk = %v, %v", risky, err)
	}
}

// BenchmarkOrderedJSONStatsEarlyReturn times an attempt that the unparsed-line
// probe ends while its stats query is on the wire: the backend holds the stats
// request until the proxy cancels it. With the join, the attempt returns once
// the cancelled call has returned.
func BenchmarkOrderedJSONStatsEarlyReturn(b *testing.B) {
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	rows := braceOnlyRows(b, withStoredLevels(jsonSplitRows(rand.New(rand.NewSource(1)), s0, 200, false)), false)
	query := `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`
	plan, ok := compileOrderedJSONMetric(query)
	if !ok {
		b.Fatal("no plan")
	}
	srv, fake := newPushdownFakeVL(b, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	fake.presence = true
	var current *joinCheckTransport
	var mu sync.Mutex
	fake.onStats = func(r *http.Request, _ string) {
		mu.Lock()
		j := current
		mu.Unlock()
		j.once.Do(func() { close(j.holding) })
		<-r.Context().Done()
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		p := newBenchFilterPushdownProxy(b, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		j := newJoinCheckTransport(p.client.Transport)
		j.decide = func(path, q string) (int, string, bool) {
			if path == "/select/logsql/query" && strings.Contains(q, "| filter -"+jsonObjectStartFilter+" | limit 1") {
				return http.StatusOK, `{"_time":"2023-11-14T22:15:00Z","_msg":"x"}` + "\n", true
			}
			return 0, "", false
		}
		p.client.Transport = j
		mu.Lock()
		current = j
		mu.Unlock()
		b.StartTimer()
		if _, served, _, err := p.orderedJSONStatsBuckets(context.Background(), plan, start, end, time.Minute, true, false); served || err != nil {
			b.Fatalf("served=%v err=%v", served, err)
		}
		b.StopTimer()
		_ = p.Shutdown(context.Background())
		b.StartTimer()
	}
}

// newBenchFilterPushdownProxy is newFilterPushdownProxy for a benchmark.
func newBenchFilterPushdownProxy(b *testing.B, backendURL string) *Proxy {
	b.Helper()
	p, err := New(Config{BackendURL: backendURL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", MetricsExportSensitiveLabels: true})
	if err != nil {
		b.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	p.log = slog.New(slog.NewTextHandler(io.Discard, nil)) // the cancelled calls are logged
	return p
}
