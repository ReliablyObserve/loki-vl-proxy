package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// smFakeVL answers the field listings of an OpenTelemetry-like window and counts them; every other read gets an
// empty stats answer, and the queries are recorded.
type smFakeVL struct {
	mu       sync.Mutex
	listings int
	queries  []string
	noMeta   bool
}

func (f *smFakeVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/select/logsql/field_names":
		if r.Form.Get("query") == `app:="a"` {
			f.listings++
		}
		if f.noMeta {
			_, _ = w.Write([]byte(`{"values":[{"value":"_msg","hits":2},{"value":"_time","hits":2},{"value":"app","hits":2}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"values":[{"value":"_msg","hits":2},{"value":"_stream","hits":2},{"value":"_time","hits":2},{"value":"app","hits":2},{"value":"service_name","hits":2},{"value":"service.name","hits":1},{"value":"trace_id","hits":1}]}`))
		return
	case "/select/logsql/stream_field_names":
		if r.Form.Get("query") == `app:="a"` {
			f.listings++
		}
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":2},{"value":"service_name","hits":2}]}`))
		return
	}
	f.queries = append(f.queries, r.Form.Get("query"))
	_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
}

func smTestProxy(t *testing.T, backend string, on bool) *Proxy {
	t.Helper()
	p, err := New(Config{BackendURL: backend, Cache: cache.New(time.Minute, 100), LogLevel: "error", StructuredMetadataSeriesIdentity: on})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func smStatsParams(query string) url.Values {
	return url.Values{"query": {query}, "start": {"1700000000000000000"}, "end": {"1700003600000000000"}, "step": {"60s"}}
}

// With -structured-metadata-series-identity off nothing a read sends, and no key, changes; with it a metric
// request's stats reads of a series per stream carry the window's metadata identity, listed once per request, and
// every other read is sent as it is.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestMetadataSeriesParams(t *testing.T) {
	vl := &smFakeVL{}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	perStream := `app:="a" | stats by (_stream, level) count()`
	byLabel := `app:="a" | stats by (app) count()`

	off := smTestProxy(t, srv.URL, false)
	ctx := off.withMetadataSeries(context.Background())
	params := smStatsParams(perStream)
	if got := off.metadataSeriesParams(ctx, "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("flag off: %s", got.Get("query"))
	}
	if metadataSeriesOf(ctx) != nil || off.statsReadSeriesLimit(ctx) != off.resolvedMaxStatsQuerySeries(ctx) {
		t.Fatal("flag off: the request is marked")
	}

	on := smTestProxy(t, srv.URL, true)
	ctx = on.withMetadataSeries(context.Background())
	if on.withMetadataSeries(ctx) != ctx {
		t.Fatal("marked twice")
	}
	got := on.metadataSeriesParams(ctx, "/select/logsql/stats_query_range", params).Get("query")
	want := `app:="a" | format if (service_name:*) "<service.name>" as service_name_extracted | format if (service_name:"") "<service.name>" as service_name | set_stream_fields app, service_name, service_name_extracted, trace_id | stats by (_stream, level) count()`
	if got != want {
		t.Fatalf("flag on:\n got %s\nwant %s", got, want)
	}
	listed := vl.listings
	// A chunk of the request's window reads the identity the request listed.
	chunk := smStatsParams(perStream)
	chunk.Set("start", "1700001800000000000")
	if again := on.metadataSeriesParams(ctx, "/select/logsql/stats_query", chunk).Get("query"); again != want || vl.listings != listed || listed == 0 {
		t.Fatalf("chunk: %d listings, %s", vl.listings, again)
	}
	for _, c := range []struct {
		path string
		p    url.Values
	}{
		{"/select/logsql/stats_query_range", smStatsParams(byLabel)},
		{"/select/logsql/hits", smStatsParams(perStream)},
		{"/select/logsql/stats_query_range", url.Values{"query": {perStream}}},
		{"/select/logsql/stats_query_range", smStatsParams(`app:="a" | stats by (_stream) count( `)},
		{"/select/logsql/stats_query_range", smStatsParams(want)},
	} {
		if got := on.metadataSeriesParams(ctx, c.path, c.p); got.Get("query") != c.p.Get("query") {
			t.Errorf("%s %v: rebuilt to %s", c.path, c.p, got.Get("query"))
		}
	}
	// A request that is not marked (no metric request) is sent as it is.
	if got := on.metadataSeriesParams(context.Background(), "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("unmarked: %s", got.Get("query"))
	}
	// A window without structured metadata keeps the stream's identity.
	plain := &smFakeVL{noMeta: true}
	plainSrv := httptest.NewServer(plain)
	defer plainSrv.Close()
	p2 := smTestProxy(t, plainSrv.URL, true)
	if got := p2.metadataSeriesParams(p2.withMetadataSeries(context.Background()), "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("no metadata: %s", got.Get("query"))
	}
	// A listing that fails sends the read as it is.
	down := smTestProxy(t, "http://127.0.0.1:1", true)
	if got := down.metadataSeriesParams(down.withMetadataSeries(context.Background()), "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("listing failed: %s", got.Get("query"))
	}
}

// The response keys of query and query_range keep their bytes with the flag off and are kept apart with it on;
// a read folded into its groups under the flag is bounded by the fold budget, not the tenant's series limit.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestMetadataSeriesKeysAndReadLimit(t *testing.T) {
	off := smTestProxy(t, "http://127.0.0.1:1", false)
	on := smTestProxy(t, "http://127.0.0.1:1", true)
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=count_over_time(%7Ba%3D%22b%22%7D%5B5m%5D)", nil)
	for _, endpoint := range []string{"query", "query_range"} {
		k0, k1 := off.responseProfileCacheKey(r, endpoint), on.responseProfileCacheKey(r, endpoint)
		if strings.Contains(k0, "/sm-series") || k1 != k0+"/sm-series" {
			t.Errorf("%s: off %q on %q", endpoint, k0, k1)
		}
	}
	if k0, k1 := off.responseProfileCacheKey(r, "labels"), on.responseProfileCacheKey(r, "labels"); k0 != k1 {
		t.Errorf("labels: %q %q", k0, k1)
	}
	ctx := on.withMetadataSeries(context.Background())
	if got := on.statsReadSeriesLimit(ctx); got != on.resolvedMaxStatsQuerySeries(ctx) {
		t.Errorf("unfolded read: %d", got)
	}
	folded, _ := withMatrixFold(ctx, []foldLevel{{}})
	rows, _ := on.manualMetricRowBudget()
	if got := on.statsReadSeriesLimit(folded); got != rows {
		t.Errorf("folded read: %d, want %d", got, rows)
	}
	offFolded, _ := withMatrixFold(off.withMetadataSeries(context.Background()), []foldLevel{{}})
	if got := off.statsReadSeriesLimit(offFolded); got != off.resolvedMaxStatsQuerySeries(offFolded) {
		t.Errorf("flag off folded read: %d", got)
	}
}
