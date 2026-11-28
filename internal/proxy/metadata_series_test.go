package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
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
	// failFields answers field_names with an error, failStats every stats read but the row count.
	failFields, failStats bool
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
		if f.failFields {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
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
	if f.failStats && r.URL.Path == "/select/logsql/stats_query_range" && !strings.HasSuffix(r.Form.Get("query"), "count() as rows") {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
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
	ctx := off.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`)
	params := smStatsParams(perStream)
	if got := smParams(t, off, ctx, "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("flag off: %s", got.Get("query"))
	}
	if metadataSeriesOf(ctx) != nil || off.statsReadSeriesLimit(ctx) != off.resolvedMaxStatsQuerySeries(ctx) {
		t.Fatal("flag off: the request is marked")
	}

	on := smTestProxy(t, srv.URL, true)
	ctx = on.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`)
	if on.withMetadataSeries(ctx, `count by (app) (count_over_time({a="b"}[5m]))`) != ctx {
		t.Fatal("marked twice")
	}
	got := smParams(t, on, ctx, "/select/logsql/stats_query_range", params).Get("query")
	want := `app:="a" | format if (service_name:*) "<service.name>" as service_name_extracted | format if (service_name:"") "<service.name>" as service_name | set_stream_fields app, service_name, service_name_extracted, trace_id | stats by (_stream, level) count()`
	if got != want {
		t.Fatalf("flag on:\n got %s\nwant %s", got, want)
	}
	listed := vl.listings
	// A chunk of the request's window reads the identity the request listed.
	chunk := smStatsParams(perStream)
	chunk.Set("start", "1700001800000000000")
	if again := smParams(t, on, ctx, "/select/logsql/stats_query", chunk).Get("query"); again != want || vl.listings != listed || listed == 0 {
		t.Fatalf("chunk: %d listings, %s", vl.listings, again)
	}
	for _, c := range []struct {
		path string
		p    url.Values
	}{
		{"/select/logsql/stats_query_range", smStatsParams(byLabel)},
		{"/select/logsql/hits", smStatsParams(perStream)},
		{"/select/logsql/stats_query_range", smStatsParams(want)},
	} {
		if got := smParams(t, on, ctx, c.path, c.p); got.Get("query") != c.p.Get("query") {
			t.Errorf("%s %v: rebuilt to %s", c.path, c.p, got.Get("query"))
		}
	}
	// A read of a series per stream that cannot take the identity fails: sent as it is, it would answer a series per
	// stream under the flag's response key.
	for _, c := range []url.Values{
		{"query": {perStream}},
		smStatsParams(`app:="a" | stats by (_stream) count( `),
	} {
		if got, err := on.metadataSeriesParams(ctx, "/select/logsql/stats_query_range", c); !errors.Is(err, errMetadataSeriesUnrebuilt) {
			t.Errorf("%v: %v %v", c, got, err)
		}
	}
	// A request that is not marked (no metric request) is sent as it is.
	if got := smParams(t, on, context.Background(), "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("unmarked: %s", got.Get("query"))
	}
	// A window without structured metadata keeps the stream's identity.
	plain := &smFakeVL{noMeta: true}
	plainSrv := httptest.NewServer(plain)
	defer plainSrv.Close()
	p2 := smTestProxy(t, plainSrv.URL, true)
	if got := smParams(t, p2, p2.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`), "/select/logsql/stats_query_range", params); got.Get("query") != perStream {
		t.Fatalf("no metadata: %s", got.Get("query"))
	}
	// A listing that fails fails the read.
	down := smTestProxy(t, "http://127.0.0.1:1", true)
	if got, err := down.metadataSeriesParams(down.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`), "/select/logsql/stats_query_range", params); !errors.Is(err, errMetadataSeriesUnrebuilt) {
		t.Fatalf("listing failed: %v %v", got, err)
	}
}

// A request whose read cannot take the identity because its fields cannot be listed fails, and its failure is not
// kept: the next request reads Loki's series.
func TestMetadataSeriesListingFailureIsNotAnswered(t *testing.T) {
	vl := &smFakeVL{failFields: true}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	on := smTestProxy(t, srv.URL, true)
	target := "/loki/api/v1/query_range?" + url.Values{"query": {`count by (app) (count_over_time({app="a"}[5m]))`}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}.Encode()
	if got := doCompatProxyRequest(on, target, map[string]string{"X-Scope-OrgID": "0"}); got.Code == http.StatusOK {
		t.Fatalf("listing failed, answered %d %s", got.Code, got.Body.String())
	}
	vl.mu.Lock()
	for _, q := range vl.queries {
		if !strings.Contains(q, "set_stream_fields") && strings.Contains(q, "_stream") {
			t.Errorf("read sent without the identity: %s", q)
		}
	}
	vl.failFields, vl.queries = false, nil
	vl.mu.Unlock()
	if got := doCompatProxyRequest(on, target, map[string]string{"X-Scope-OrgID": "0"}); got.Code != http.StatusOK {
		t.Fatalf("after recovery: %d %s", got.Code, got.Body.String())
	}
	vl.mu.Lock()
	defer vl.mu.Unlock()
	rebuilt := 0
	for _, q := range vl.queries {
		if strings.Contains(q, "set_stream_fields") {
			rebuilt++
		}
	}
	if rebuilt == 0 {
		t.Fatalf("after recovery no read carried the identity: %q", vl.queries)
	}
}

// A request whose stats reads fail and whose answer the proxy then evaluates from rows reads the rows with Loki's
// series identity: the evaluator of rows names a series by each row's _stream.
func TestMetadataSeriesRowReadsTakeTheIdentity(t *testing.T) {
	vl := &smFakeVL{failStats: true}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	on := smTestProxy(t, srv.URL, true)
	target := "/loki/api/v1/query_range?" + url.Values{"query": {`count by (app) (count_over_time({app="a"}[5m]))`}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}.Encode()
	_ = doCompatProxyRequest(on, target, map[string]string{"X-Scope-OrgID": "0"})
	vl.mu.Lock()
	defer vl.mu.Unlock()
	rows := 0
	for _, q := range vl.queries {
		if strings.Contains(q, "| stats ") {
			continue
		}
		rows++
		if !strings.Contains(q, "set_stream_fields") {
			t.Errorf("read of rows without the identity: %s", q)
		}
	}
	if rows == 0 {
		t.Fatalf("no read of rows: %q", vl.queries)
	}
}

func smParams(t *testing.T, p *Proxy, ctx context.Context, path string, params url.Values) url.Values {
	t.Helper()
	got, err := p.metadataSeriesParams(ctx, path, params)
	if err != nil {
		t.Fatalf("%s %v: %v", path, params, err)
	}
	return got
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
		if strings.Contains(k0, "/sm-series") || k1 != k0+"/sm-series2" {
			t.Errorf("%s: off %q on %q", endpoint, k0, k1)
		}
	}
	if k0, k1 := off.responseProfileCacheKey(r, "labels"), on.responseProfileCacheKey(r, "labels"); k0 != k1 {
		t.Errorf("labels: %q %q", k0, k1)
	}
	ctx := on.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`)
	if got := on.statsReadSeriesLimit(ctx); got != on.resolvedMaxStatsQuerySeries(ctx) {
		t.Errorf("unfolded read: %d", got)
	}
	folded, _ := withMatrixFold(ctx, []foldLevel{{}})
	rows, _ := on.manualMetricRowBudget()
	if got := on.statsReadSeriesLimit(folded); got != rows {
		t.Errorf("folded read: %d, want %d", got, rows)
	}
	offFolded, _ := withMatrixFold(off.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`), []foldLevel{{}})
	if got := off.statsReadSeriesLimit(offFolded); got != off.resolvedMaxStatsQuerySeries(offFolded) {
		t.Errorf("flag off folded read: %d", got)
	}
}

// The row counts of a long window are a count per minute: their answer passes the error-body bound (6 h: about
// 15 KB) and is read whole.
func TestMetadataSeriesRowHistogramReadsLongWindows(t *testing.T) {
	from, to := int64(1_700_000_000_000_000_000), int64(1_700_000_000_000_000_000)+int64(6*time.Hour)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b strings.Builder
		b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[`)
		for at, i := from, 0; at < to; at, i = at+int64(time.Minute), i+1 {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, at/int64(time.Second), 1000+i)
		}
		b.WriteString(`]}]}}`)
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()
	p := smTestProxy(t, srv.URL, true)
	read, ok, err := logsql.ReadSeriesIdentity(`app:="a" | stats by (_stream) count()`)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	h, err := p.seriesMetadataRowHistogram(context.Background(), read, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if rows := h.rows(from, to); rows < 360*1000 {
		t.Fatalf("rows %d", rows)
	}
}

// A read of the request that takes no identity (the level probe) starts the listing of its filter, and the request's
// reads of a series per stream over that filter wait for it instead of listing again.
func TestMetadataSeriesListsAheadOfTheReads(t *testing.T) {
	vl := &smFakeVL{}
	srv := httptest.NewServer(vl)
	defer srv.Close()
	on := smTestProxy(t, srv.URL, true)
	ctx := on.withMetadataSeries(context.Background(), `count by (app) (count_over_time({a="b"}[5m]))`)
	probe := smStatsParams(`app:="a" | stats count() as c`)
	if got := smParams(t, on, ctx, "/select/logsql/query", probe); got.Get("query") != probe.Get("query") {
		t.Fatalf("probe rebuilt: %s", got.Get("query"))
	}
	read := smStatsParams(`app:="a" | stats by (_stream, level) count()`)
	if got := smParams(t, on, ctx, "/select/logsql/stats_query_range", read).Get("query"); !strings.Contains(got, "set_stream_fields") {
		t.Fatalf("read not rebuilt: %s", got)
	}
	vl.mu.Lock()
	defer vl.mu.Unlock()
	if vl.listings != 2 {
		t.Fatalf("%d listings, want field_names and stream_field_names once", vl.listings)
	}
}
