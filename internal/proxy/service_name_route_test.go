package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// routeVL is a VictoriaLogs stand-in for the service_name route: it records every data read (path, whether it was
// the proof, the stream filter it carried in extra_filters), answers the proof with a row when found is set (or a
// 500 when proofFails is), refuses a narrowed read with refuseNarrowed, and answers every log read with one row.
type routeVL struct {
	mu             sync.Mutex
	reads          []routeRead
	found          bool
	proofFails     bool
	refuseNarrowed bool
}

type routeRead struct {
	path     string
	proof    bool
	narrowed bool
	tenant   string
}

func (v *routeVL) handler(t *testing.T, value string) http.Handler {
	proof := logsql.Query{Filter: translate.ServiceNameStreamFilterProof(value), Pipes: []logsql.Pipe{logsql.PipeLimit{N: 1}}}.String()
	stream := translate.ServiceNameStreamFilter(value).String()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/select/logsql/query", "/select/logsql/hits", "/select/logsql/stats_query", "/select/logsql/stats_query_range":
		default:
			return
		}
		read := routeRead{path: r.URL.Path, proof: r.Form.Get("query") == proof, tenant: r.Header.Get("AccountID")}
		for _, f := range r.Form["extra_filters"] {
			if f == stream {
				read.narrowed = true
			} else if strings.Contains(f, "service_name") {
				t.Errorf("unexpected extra_filters %q", f)
			}
		}
		v.mu.Lock()
		v.reads = append(v.reads, read)
		found, fails, refuse := v.found, v.proofFails, v.refuseNarrowed
		v.mu.Unlock()
		switch {
		case read.proof && fails:
			http.Error(w, "boom", http.StatusInternalServerError)
		case read.proof && found:
			_, _ = w.Write([]byte(`{"_time":"2026-11-19T23:30:00Z","_msg":"x","service_name":"` + value + `"}` + "\n"))
		case read.proof:
		case read.narrowed && refuse:
			http.Error(w, "cannot parse extra_filters", http.StatusBadRequest)
		case r.URL.Path == "/select/logsql/query":
			_, _ = w.Write([]byte(`{"_time":"2026-11-19T23:30:00Z","_msg":"hello","_stream":"{service.name=\"` + value + `\"}","service.name":"` + value + `"}` + "\n"))
		case r.URL.Path == "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		case strings.Contains(r.URL.Path, "stats_query_range"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		}
	})
}

func (v *routeVL) take() []routeRead {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.reads
	v.reads = nil
	return out
}

func (v *routeVL) set(fn func(*routeVL)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fn(v)
}

func serviceNameRouteProxy(t *testing.T, backend string, cfg func(*Config)) *Proxy {
	t.Helper()
	c := Config{
		BackendURL:                         backend,
		Cache:                              cache.New(60*time.Second, 1000),
		LogLevel:                           "error",
		LabelStyle:                         LabelStyleUnderscores,
		MetadataFieldMode:                  MetadataFieldModeTranslated,
		FieldMappings:                      []FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}},
		ServiceNameStreamRoute:             true,
		ServiceNameStreamRouteMinWindow:    30 * time.Minute,
		ServiceNameStreamRouteViolationTTL: time.Hour,
	}
	if cfg != nil {
		cfg(&c)
	}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

var snRouteEnd = time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

// routeQuery sends a query_range of query over [snRouteEnd-shift-window, snRouteEnd-shift] as tenant.
func routeQuery(t *testing.T, p *Proxy, query string, window, shift time.Duration, tenant string, step string) *httptest.ResponseRecorder {
	t.Helper()
	end := snRouteEnd.Add(-shift)
	v := url.Values{"query": {query}, "start": {strconv.FormatInt(end.Add(-window).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"100"}}
	if step != "" {
		v.Set("step", step)
	}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+v.Encode(), map[string]string{"X-Scope-OrgID": tenant})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body.String())
	}
	return res
}

// sameJSONAnswer compares two answers as JSON values (object keys in any order).
func sameJSONAnswer(t *testing.T, name, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("%s: %v: %s", name, err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: %v: %s", name, err, want)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s: answer with the route\n%s\nwithout\n%s", name, got, want)
	}
}

func countReads(reads []routeRead) (data, narrowed, proofs int) {
	for _, r := range reads {
		switch {
		case r.proof:
			proofs++
		case r.narrowed:
			narrowed++
			data++
		default:
			data++
		}
	}
	return
}

// TestServiceNameRoute_CleanWindowIsNarrowed: a one-service log query over a window the proof finds clean is read
// once, narrowed by the stream filter, beside one proof read, and answers what the proxy without the route answers;
// the exact repeat is served from the response cache with no VictoriaLogs read.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_CleanWindowIsNarrowed(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.ServiceNameStreamRoute = false })
	for _, q := range []string{`{service_name="svc-a"}`, `{service_name="svc-a"} | json | logfmt | drop __error__, __error_details__`, `{service_name="svc-a", level="info"} |= "x"`} {
		want := routeQuery(t, off, q, time.Hour, 0, "0", "").Body.String()
		base, n0, pr0 := countReads(vl.take())
		if base == 0 || n0 != 0 || pr0 != 0 {
			t.Fatalf("%s: route off: %d reads, %d narrowed, %d proofs", q, base, n0, pr0)
		}
		got := routeQuery(t, on, q, time.Hour, 0, "0", "").Body.String()
		sameJSONAnswer(t, q, got, want)
		// A read the handler makes before it reaches the log read (the parser probe of `| json | logfmt`) is
		// made as without the route.
		if d, n, pr := countReads(vl.take()); d != base || n != 1 || pr != 1 {
			t.Errorf("%s: route on, clean: %d reads, %d narrowed, %d proofs; want %d, 1, 1", q, d, n, pr, base)
		}
		routeQuery(t, on, q, time.Hour, 0, "0", "")
		if reads := vl.take(); len(reads) != 0 {
			t.Errorf("%s: the exact repeat made %d VictoriaLogs reads", q, len(reads))
		}
	}
}

// TestServiceNameRoute_ViolationFallsBackAndIsRemembered: when the proof finds a row the narrowing leaves out, the
// read is sent again without the stream filter (the answer is the derived filter's), the finding is remembered for
// the tenant and service, and the next log read of that service is sent once, unnarrowed, with no proof; another
// tenant and another service are narrowed still.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_ViolationFallsBackAndIsRemembered(t *testing.T) {
	vl := &routeVL{found: true}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.ServiceNameStreamRoute = false })
	q := `{service_name="svc-a"}`
	want := routeQuery(t, off, q, time.Hour, 0, "0", "").Body.String()
	vl.take()
	sameJSONAnswer(t, "after a violation", routeQuery(t, on, q, time.Hour, 0, "0", "").Body.String(), want)
	reads := vl.take()
	if d, n, pr := countReads(reads); d != 2 || n != 1 || pr != 1 || reads[len(reads)-1].narrowed {
		t.Errorf("violation: %d reads, %d narrowed, %d proofs (%+v); want the narrowed read, the proof and the read without the stream filter last", d, n, pr, reads)
	}
	routeQuery(t, on, q, time.Hour, time.Minute, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
		t.Errorf("after a remembered violation: %d reads, %d narrowed, %d proofs; want 1, 0, 0", d, n, pr)
	}
	vl.set(func(v *routeVL) { v.found = false })
	routeQuery(t, on, q, time.Hour, 0, "2", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 1 || pr != 1 {
		t.Errorf("another tenant: %d reads, %d narrowed, %d proofs; want 1, 1, 1", d, n, pr)
	}
}

// TestServiceNameRoute_DoubtKeepsTheDerivedFilter: a failed proof, and a narrowed read VictoriaLogs refuses, send
// the read again without the stream filter; a failed proof is not remembered (the next read is proven again), a
// refused narrowing is.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_DoubtKeepsTheDerivedFilter(t *testing.T) {
	vl := &routeVL{proofFails: true}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, nil)
	q := `{service_name="svc-a"}`
	routeQuery(t, on, q, time.Hour, 0, "0", "")
	if reads := vl.take(); len(reads) != 3 || reads[2].narrowed || reads[2].proof {
		t.Fatalf("failed proof: %+v; want narrowed, proof, unnarrowed", reads)
	}
	vl.set(func(v *routeVL) { v.proofFails, v.refuseNarrowed = false, true })
	routeQuery(t, on, q, time.Hour, time.Minute, "0", "")
	if reads := vl.take(); len(reads) != 3 || reads[2].narrowed {
		t.Fatalf("a failed proof is not remembered, a refused narrowing falls back: %+v", reads)
	}
	routeQuery(t, on, q, time.Hour, 2*time.Minute, "0", "")
	if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
		t.Errorf("after a refused narrowing: %d reads, %d narrowed, %d proofs; want 1, 0, 0", d, n, pr)
	}
}

// TestServiceNameRoute_KeepsTheDerivedFilter: the classes the route leaves alone send what the proxy sends without
// it: a read shorter than the minimum window, metric queries, negated, regexp and empty matchers, a selector with a
// declared stream field, no service_name matcher, the route off, and -service-name-stream-label-only.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_KeepsTheDerivedFilter(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, func(c *Config) { c.StreamFields = []string{"app"} })
	for _, c := range []struct {
		query  string
		window time.Duration
		step   string
	}{
		{`{service_name="svc-a"}`, 15 * time.Minute, ""},
		{`sum(count_over_time({service_name="svc-a"}[1m]))`, time.Hour, "60"},
		{`sum by (level) (count_over_time({service_name="svc-a", level!=""}[1m]))`, time.Hour, "60"},
		{`{service_name!="svc-a", job="x"}`, time.Hour, ""},
		{`{service_name=~"svc-a|svc-b"}`, time.Hour, ""},
		{`{service_name!~"svc-a", job="x"}`, time.Hour, ""},
		{`{service_name="", job="x"}`, time.Hour, ""},
		{`{app="x", service_name="svc-a"}`, time.Hour, ""},
		{`{job="x"}`, time.Hour, ""},
	} {
		routeQuery(t, on, c.query, c.window, 0, "0", c.step)
		if d, n, pr := countReads(vl.take()); n != 0 || pr != 0 || d == 0 {
			t.Errorf("%s over %s: %d reads, %d narrowed, %d proofs; want the reads without the route", c.query, c.window, d, n, pr)
		}
	}
	for name, cfg := range map[string]func(*Config){
		"route off":  func(c *Config) { c.ServiceNameStreamRoute = false },
		"label only": func(c *Config) { c.ServiceNameStreamLabelOnly = true },
	} {
		p := serviceNameRouteProxy(t, srv.URL, cfg)
		routeQuery(t, p, `{service_name="svc-a"}`, time.Hour, 0, "0", "")
		if d, n, pr := countReads(vl.take()); d != 1 || n != 0 || pr != 0 {
			t.Errorf("%s: %d reads, %d narrowed, %d proofs; want 1, 0, 0", name, d, n, pr)
		}
	}
}

// TestServiceNameRoute_SplitWindowsShareTheirProofs: a log query the proxy reads in windows (-query-range-windowing)
// proves the first window and the rest, which every later window read of the request joins: at most two proof
// reads, whatever the number of windows.
//
// conformance: semantics/service-name-stream-route
func TestServiceNameRoute_SplitWindowsShareTheirProofs(t *testing.T) {
	vl := &routeVL{}
	srv := httptest.NewServer(vl.handler(t, "svc-a"))
	defer srv.Close()
	on := serviceNameRouteProxy(t, srv.URL, func(c *Config) {
		c.QueryRangeWindowingEnabled, c.QueryRangeSplitInterval = true, time.Hour
		c.ServiceNameStreamRouteMinWindow = 0
	})
	off := serviceNameRouteProxy(t, srv.URL, func(c *Config) {
		c.QueryRangeWindowingEnabled, c.QueryRangeSplitInterval = true, time.Hour
		c.ServiceNameStreamRoute = false
	})
	q := `{service_name="svc-a"}`
	want := routeQuery(t, off, q, 6*time.Hour, 0, "0", "").Body.String()
	base := len(vl.take())
	sameJSONAnswer(t, "split", routeQuery(t, on, q, 6*time.Hour, 0, "0", "").Body.String(), want)
	d, n, pr := countReads(vl.take())
	if d != base || n != d || pr == 0 || pr > 2 {
		t.Errorf("split windows: %d reads (%d without the route), %d narrowed, %d proofs; want every read narrowed and 1 or 2 proofs", d, base, n, pr)
	}
}

// BenchmarkWithServiceNameRoute is the work the route adds to a log query request before its reads: arming it for
// a one-service selector, and the check that leaves any other selector alone.
func BenchmarkWithServiceNameRoute(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 1000), LogLevel: "error", ServiceNameStreamRoute: true, ServiceNameStreamRouteViolationTTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(b.Context()) })
	for _, c := range []struct{ name, query string }{
		{"one service", `{service_name="svc-a"} | json`},
		{"other selector", `{app="x"} | json`},
	} {
		parsed, err := logqlpkg.Parse(c.query)
		if err != nil {
			b.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?start=1&end=3600000000000", nil)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = p.withServiceNameRoute(r, parsed)
			}
		})
	}
}
