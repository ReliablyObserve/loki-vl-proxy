package proxy

import (
	"bytes"
	"context"
	"encoding/json"
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

// dynamicIdentityCacheQueries are the shapes of a parser's dynamic identity the handlers serve from one stats query
// (a range up to the step: $__auto is the request's step).
var dynamicIdentityCacheQueries = []string{
	`sum by (app) (avg_over_time({app="api"} | json | unwrap n [$__auto]))`,
	`sum(stddev_over_time({app="api"} | json | unwrap n [$__auto]))`,
	`max by (app) (stdvar_over_time({app="api"} | json | unwrap n [$__auto]))`,
}

// vlCallCounter counts the requests VictoriaLogs answers and the bytes of its answers.
type vlCallCounter struct {
	inner        http.Handler
	calls, bytes atomic.Int64
}

func (c *vlCallCounter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	c.inner.ServeHTTP(rec, r)
	c.calls.Add(1)
	c.bytes.Add(int64(rec.Body.Len()))
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func (c *vlCallCounter) take() (calls, bytes int64) {
	return c.calls.Swap(0), c.bytes.Swap(0)
}

// A repeat of a parser's dynamic identity asks VictoriaLogs nothing and answers the same bytes; a refresh one step
// later asks only for the buckets it does not hold. The test logs VictoriaLogs' calls and bytes per request (cold,
// repeat, refresh) for the instant and the range shapes.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, quality/query-range-extent-cache
func TestDynamicIdentityCacheReuse(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	fakeSrv := httptest.NewServer(dynamicIdentityStatsStub(t))
	t.Cleanup(fakeSrv.Close)
	counter := &vlCallCounter{inner: fakeSrv.Config.Handler}
	shimSrv, shim := newExtentShim(t, counter)
	c := cache.NewWithMaxBytes(time.Minute, 100000, 256<<20)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: shimSrv.URL, Cache: c, LogLevel: "error", QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	get := func(path string, params url.Values) []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		p.statsExtentStores.wait()
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: status %d: %s", path, params.Get("query"), rec.Code, rec.Body)
		}
		return rec.Body.Bytes()
	}
	uncachedSrv, _ := newExtentShim(t, fakeSrv.Config.Handler)
	uncached := newExtentTestProxy(t, uncachedSrv.URL, false, 256<<20)
	end := s0.Add(3 * time.Hour)
	const step = 10 * time.Second
	for _, q := range dynamicIdentityCacheQueries {
		q = strings.ReplaceAll(q, "$__auto", step.String())
		if !p.dynamicUnwrapIdentity(q) {
			t.Fatalf("%s: not served from the dynamic identity", q)
		}
		// Instant: one stats call, cold and repeated. The proxy does not keep instant answers (nor does Loki by
		// default), so a repeat reads the same one call again.
		inst := url.Values{"query": {q}, "time": {strconv.FormatInt(end.Unix(), 10)}}
		counter.take()
		cold := get("/loki/api/v1/query", inst)
		coldCalls, coldBytes := counter.take()
		again := get("/loki/api/v1/query", inst)
		repCalls, repBytes := counter.take()
		if coldCalls != 1 || repCalls != 1 || !bytes.Equal(dynamicIdentityResult(t, cold), dynamicIdentityResult(t, again)) {
			t.Errorf("%s instant: cold %d calls, repeat %d calls, want 1 each and the same answer", q, coldCalls, repCalls)
		}
		t.Logf("%s instant: cold %d calls %d B, repeat %d calls %d B", q, coldCalls, coldBytes, repCalls, repBytes)
		// Range: cold, a repeat, and a refresh one minute later.
		window := func(e time.Time) url.Values {
			return url.Values{"query": {q}, "start": {strconv.FormatInt(e.Add(-time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(e.Unix(), 10)}, "step": {"10"}}
		}
		shim.reset()
		cold = get("/loki/api/v1/query_range", window(end))
		coldCalls, coldBytes = counter.take()
		_, coldBuckets := shim.bucketsRead()
		if !shimReadIdentity(shim) {
			t.Errorf("%s range: the stats calls do not group by the series hash", q)
		}
		shim.reset()
		again = get("/loki/api/v1/query_range", window(end))
		repCalls, repBytes = counter.take()
		if coldCalls == 0 || repCalls != 0 || !bytes.Equal(dynamicIdentityResult(t, cold), dynamicIdentityResult(t, again)) {
			t.Errorf("%s range: cold %d calls, repeat %d calls, want a repeat with no call and the same answer", q, coldCalls, repCalls)
		}
		shim.reset()
		refreshed := get("/loki/api/v1/query_range", window(end.Add(time.Minute)))
		refCalls, refBytes := counter.take()
		_, refBuckets := shim.bucketsRead()
		if refBuckets == 0 || refBuckets*10 > coldBuckets {
			t.Errorf("%s refresh: read %d buckets, the cold request %d: it should read the new ones only", q, refBuckets, coldBuckets)
		}
		t.Logf("%s range: cold %d calls %d B (%d buckets), repeat %d calls %d B, refresh %d calls %d B (%d buckets)", q, coldCalls, coldBytes, coldBuckets, repCalls, repBytes, refCalls, refBytes, refBuckets)
		// The refreshed answer is the uncached one.
		if _, want := extentQueryRange(t, uncached, q, end.Add(time.Minute-time.Hour), end.Add(time.Minute), step, nil); !bytes.Equal(dynamicIdentityResult(t, refreshed), dynamicIdentityResult(t, want)) {
			t.Errorf("%s refresh: the cached answer differs from the uncached one\n cached: %.300s\nuncached: %.300s", q, refreshed, want)
		}
	}
}

// dynamicIdentityStatsStub answers a stats query with one series and a stats_query_range call with one point per
// bucket of [start, end) (the extent shim widens them to the bucket edges), the same value in every bucket, as
// VictoriaLogs answers each bucket from its own rows.
func dynamicIdentityStatsStub(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("stub: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api","__lvp_bad":"0"},"value":[1700011800,"3"]}]}}`))
		case "/select/logsql/stats_query_range":
			start, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			end, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
			step, err := time.ParseDuration(r.Form.Get("step"))
			if err != nil || step <= 0 || end <= start {
				t.Errorf("stub: start %q end %q step %q", r.Form.Get("start"), r.Form.Get("end"), r.Form.Get("step"))
				return
			}
			var b strings.Builder
			b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api","__lvp_bad":"0"},"values":[`)
			for at := start; at < end; at += int64(step) {
				if at > start {
					b.WriteByte(',')
				}
				b.WriteString(`[` + strconv.FormatInt(at/int64(time.Second), 10) + `,"3"]`)
			}
			b.WriteString(`]}]}}`)
			_, _ = w.Write([]byte(b.String()))
		case "/select/logsql/query":
			// A raw-row read (a route that evaluates the rows itself): a few json lines in the window.
			w.Header().Set("Content-Type", "application/x-ndjson")
			for i := 0; i < 20; i++ {
				ts := time.Unix(1700011800-int64(i)*7, 0).UTC().Format(time.RFC3339Nano)
				_, _ = w.Write([]byte(`{"_time":"` + ts + `","_stream":"{app=\"api\"}","app":"api","_msg":"{\"n\":` + strconv.Itoa(i) + `}","n":"` + strconv.Itoa(i) + `"}` + "\n"))
			}
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		}
	})
}

// dynamicIdentityResult is the data.result of a Loki answer: what a repeat must give back byte for byte (the stats
// block holds the time the request took).
func dynamicIdentityResult(t *testing.T, body []byte) []byte {
	t.Helper()
	var resp struct {
		Data struct {
			Result json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Data.Result) == 0 {
		t.Fatalf("not a Loki answer: %.300s", body)
	}
	return resp.Data.Result
}

// shimReadIdentity reports stats_query_range calls the extent shim saw, every one grouped by a parser's series hash.
func shimReadIdentity(s *extentShim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if !strings.Contains(c.query, dynamicSeriesHashPipe) {
			return false
		}
	}
	return len(s.calls) > 0
}
