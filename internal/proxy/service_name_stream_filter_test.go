package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// recordingVL is a VictoriaLogs stand-in that records the query of every data read (not the metadata listings the
// proxy refreshes in the background) and answers empty.
type recordingVL struct {
	mu      sync.Mutex
	queries []string
}

func (v *recordingVL) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/select/logsql/query", "/select/logsql/stats_query", "/select/logsql/stats_query_range", "/select/logsql/hits":
			v.mu.Lock()
			v.queries = append(v.queries, r.URL.Path+" "+r.Form.Get("query"))
			v.mu.Unlock()
		}
		switch {
		case strings.Contains(r.URL.Path, "stats_query_range"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case strings.Contains(r.URL.Path, "stats_query"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		}
	})
}

func (v *recordingVL) take() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.queries
	v.queries = nil
	return out
}

func serviceNameFlagProxy(t *testing.T, backend string, on bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                 backend,
		Cache:                      cache.New(60*time.Second, 1000),
		LogLevel:                   "error",
		LabelStyle:                 LabelStyleUnderscores,
		MetadataFieldMode:          MetadataFieldModeTranslated,
		FieldMappings:              []FieldMapping{{VLField: "service.name", LokiLabel: "service_name"}},
		ServiceNameStreamLabelOnly: on,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

// serviceNameDrilldownShapes are the queries Logs Drilldown sends for one service (issue #1763), and controls
// without a positive service_name matcher.
var serviceNameDrilldownShapes = []struct {
	query, step string
	gains       bool
}{
	{`{service_name="svc-a"}`, "", true},
	{`{service_name="svc-a"} | json | logfmt | drop __error__, __error_details__`, "", true},
	{`sum by (org_component) (count_over_time({service_name="svc-a"} | org_component!="" [1m]))`, "60", true},
	{`sum(count_over_time({service_name="svc-a", level != ""}[1m])) by (level)`, "60", true},
	{`sum by (detected_level) (count_over_time({service_name="svc-a"} | drop __error__ [1m]))`, "60", true},
	{`sum(count_over_time({service_name="svc-a"} | logfmt | drop __error__ [1m]))`, "60", true},
	{`{service_name=~"svc-a|svc-b"}`, "", false},
	{`{app="x", service_name!="svc-a"}`, "", false},
	{`{app="x"}`, "", false},
	{`sum(count_over_time({app="x"}[1m]))`, "60", false},
}

// TestServiceNameStreamLabelOnly_SendsTheStreamFilter: with -service-name-stream-label-only every VictoriaLogs read
// of a Drilldown query with a positive service_name matcher starts with the service_name stream filter, and the
// derived filter stays beside it; the queries without such a matcher send what the proxy sends with the flag off,
// which sends the derived filter alone, as before.
//
// conformance: semantics/service-name-stream-filter
func TestServiceNameStreamLabelOnly_SendsTheStreamFilter(t *testing.T) {
	vl := &recordingVL{}
	srv := httptest.NewServer(vl.handler())
	defer srv.Close()
	off, on := serviceNameFlagProxy(t, srv.URL, false), serviceNameFlagProxy(t, srv.URL, true)
	end := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	stream := translate.ServiceNameStreamFilter("svc-a").String()
	chain := translate.ServiceNameMatcherFilterExpr("svc-a", false, false).String()
	for _, c := range serviceNameDrilldownShapes {
		v := url.Values{"query": {c.query}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"100"}}
		if c.step != "" {
			v.Set("step", c.step)
		}
		send := func(p *Proxy) []string {
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+v.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", c.query, res.Code, res.Body.String())
			}
			return vl.take()
		}
		before, after := send(off), send(on)
		if len(after) == 0 || len(after) != len(before) {
			t.Fatalf("%s: %d reads with the flag, %d without", c.query, len(after), len(before))
		}
		for i := range after {
			if strings.Contains(before[i], " or service_name=") {
				t.Errorf("%s: the flag off sends the stream filter: %s", c.query, before[i])
			}
			if !c.gains || !strings.Contains(before[i], chain) {
				// A read without the derived filter (a metadata listing) is the same either way.
				if after[i] != before[i] {
					t.Errorf("%s: the flag changes a read without a positive service_name matcher:\n on  %s\n off %s", c.query, after[i], before[i])
				}
				continue
			}
			path, q, _ := strings.Cut(after[i], " ")
			if !strings.HasPrefix(q, stream+" "+chain) {
				t.Errorf("%s %s: does not start with the stream filter and the derived filter: %.300s", c.query, path, q)
			}
			if got, want := strings.Replace(q, stream+" ", "", 1), strings.SplitN(before[i], " ", 2)[1]; got != want {
				t.Errorf("%s %s: the read differs from the flag-off one by more than the stream filter:\n on  %.300s\n off %.300s", c.query, path, got, want)
			}
		}
		// The exact repeat of a settled window is answered from the cache, with the flag as without it.
		if again := send(on); len(again) != 0 {
			t.Errorf("%s: the repeat with the flag makes %d VictoriaLogs reads, want 0", c.query, len(again))
		}
	}
}

// TestServiceNameStreamLabelOnly_KeysApartOnlyWithTheFlag: an answer read through the stream filter is cached under
// a key of its own, so turning the flag off never serves it; with the flag off, and for a query without a
// service_name matcher, the key is the one the proxy writes without the flag.
//
// conformance: semantics/service-name-stream-filter
func TestServiceNameStreamLabelOnly_KeysApartOnlyWithTheFlag(t *testing.T) {
	off, on := serviceNameFlagProxy(t, "http://127.0.0.1:1", false), serviceNameFlagProxy(t, "http://127.0.0.1:1", true)
	key := func(p *Proxy, query string) (string, string) {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"2"}}.Encode(), nil)
		r.Header.Set("X-Scope-OrgID", "0")
		_ = r.ParseForm()
		return p.responseKey("query_range", r, r.URL.RawQuery), p.queryRangeCacheKey(r, query)
	}
	for _, c := range []struct {
		query string
		apart bool
	}{
		{`{service_name="svc-a"}`, true},
		{`sum(count_over_time({service_name="svc-a"}[1m]))`, true},
		{`{app="x"}`, false},
		{`sum by (level) (count_over_time({app="x"}[1m]))`, false},
	} {
		offCompat, offInner := key(off, c.query)
		onCompat, onInner := key(on, c.query)
		if (offCompat != onCompat) != c.apart || (offInner != onInner) != c.apart {
			t.Errorf("%s: keys apart = %v/%v, want %v", c.query, offCompat != onCompat, offInner != onInner, c.apart)
		}
	}
}

// TestServiceNameStreamLabelOnly_DeleteKeepsTheDerivedFilter: a delete selects its rows with the derived filter
// alone, flag or not: what it removes never depends on the operator's statement.
//
// conformance: semantics/service-name-stream-filter
func TestServiceNameStreamLabelOnly_DeleteKeepsTheDerivedFilter(t *testing.T) {
	on := serviceNameFlagProxy(t, "http://127.0.0.1:1", true)
	prof := loweringProfile(nil, on.streamFieldsMap, on.logsqlCapabilities(), false)
	if prof.ServiceNameStreamLabelOnly {
		t.Fatal("the shared lowering profile (delete, shadows, verdicts) carries the flag")
	}
}
