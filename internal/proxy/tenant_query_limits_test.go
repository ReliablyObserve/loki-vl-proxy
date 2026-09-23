package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// newTenantLimitsProxy builds a proxy with two mapped tenants, tenant-a and
// tenant-b, behind the full route chain.
func newTenantLimitsProxy(t *testing.T, backendURL string, cfg Config) (*Proxy, *http.ServeMux) {
	t.Helper()
	cfg.BackendURL = backendURL
	cfg.Cache = cache.New(60*time.Second, 1000)
	cfg.LogLevel = "error"
	cfg.TenantMap = map[string]TenantMapping{
		"tenant-a": {AccountID: "10", ProjectID: "0"},
		"tenant-b": {AccountID: "20", ProjectID: "0"},
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

// Every enforced Loki limit resolves -tenant-limits[tenant] ->
// -tenant-default-limits -> the proxy flag -> Loki's default.
//
// conformance: tenant-query-limits, limits/effective-limit-resolution, limits/per-tenant-override
func TestTenantQueryLimits_ResolutionOrder(t *testing.T) {
	type layer struct {
		name     string
		cfg      Config
		tenant   string
		expected queryLimits
	}
	flags := Config{
		MaxStatsQuerySeries:   700,
		DefaultMaxQueryLength: 48 * time.Hour,
		BackendTimeout:        90 * time.Second,
		ExecutionLimits:       ExecutionLimitsConfig{MaxEntriesLimitPerQuery: 7000},
	}
	defaults := map[string]any{
		"max_query_series": 600.0, "max_entries_limit_per_query": 6000.0, "max_query_length": "24h",
		"max_query_lookback": "72h", "max_query_range": "2h", "query_timeout": "80s",
	}
	tenant := map[string]map[string]any{"tenant-a": {
		"max_query_series": 50.0, "max_entries_limit_per_query": 0.0, "max_query_length": "1h",
		"max_query_lookback": "12h", "max_query_range": "10m", "query_timeout": "30s",
	}}
	withDefaults := flags
	withDefaults.TenantDefaultLimits = defaults
	withTenant := withDefaults
	withTenant.TenantLimits = tenant
	for _, tc := range []layer{
		{"Loki defaults", Config{}, "tenant-a", queryLimits{MaxQuerySeries: 500, MaxEntriesLimitPerQuery: DefaultMaxEntriesLimitPerQuery, QueryTimeout: 120 * time.Second}},
		{"flags", flags, "tenant-a", queryLimits{MaxQuerySeries: 700, MaxEntriesLimitPerQuery: 7000, MaxQueryLength: 48 * time.Hour, QueryTimeout: 90 * time.Second}},
		{"tenant default limits", withDefaults, "tenant-a", queryLimits{MaxQuerySeries: 600, MaxEntriesLimitPerQuery: 6000, MaxQueryLength: 24 * time.Hour, MaxQueryLookback: 72 * time.Hour, MaxQueryRange: 2 * time.Hour, QueryTimeout: 80 * time.Second, QueryTimeoutOverride: true}},
		{"per-tenant limits", withTenant, "tenant-a", queryLimits{MaxQuerySeries: 50, MaxEntriesLimitPerQuery: 0, MaxQueryLength: time.Hour, MaxQueryLookback: 12 * time.Hour, MaxQueryRange: 10 * time.Minute, QueryTimeout: 30 * time.Second, QueryTimeoutOverride: true}},
		{"another tenant keeps the defaults", withTenant, "tenant-b", queryLimits{MaxQuerySeries: 600, MaxEntriesLimitPerQuery: 6000, MaxQueryLength: 24 * time.Hour, MaxQueryLookback: 72 * time.Hour, MaxQueryRange: 2 * time.Hour, QueryTimeout: 80 * time.Second, QueryTimeoutOverride: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTenantLimitsProxy(t, "http://127.0.0.1:1", tc.cfg)
			if got := p.queryLimitsFor(tc.tenant); got != tc.expected {
				t.Fatalf("limits of %s:\n got %+v\nwant %+v", tc.tenant, got, tc.expected)
			}
		})
	}
}

// A multi-tenant request is held to its tenants combined as Loki combines
// them: the smallest max_query_series (SmallestPositiveIntPerTenant) and the
// smallest non-zero value of every other limit, 0 meaning unlimited.
//
// conformance: tenant-query-limits, limits/multi-tenant-limit-combination
func TestTenantQueryLimits_MultiTenantCombination(t *testing.T) {
	p, _ := newTenantLimitsProxy(t, "http://127.0.0.1:1", Config{TenantLimits: map[string]map[string]any{
		"tenant-a": {"max_query_series": 900.0, "max_entries_limit_per_query": 0.0, "max_query_length": "0s", "max_query_lookback": "48h", "max_query_range": "0s", "query_timeout": "90s"},
		"tenant-b": {"max_query_series": 300.0, "max_entries_limit_per_query": 2000.0, "max_query_length": "12h", "max_query_lookback": "0s", "max_query_range": "30m", "query_timeout": "1m"},
	}})
	want := queryLimits{MaxQuerySeries: 300, MaxEntriesLimitPerQuery: 2000, MaxQueryLength: 12 * time.Hour, MaxQueryLookback: 48 * time.Hour, MaxQueryRange: 30 * time.Minute, QueryTimeout: time.Minute, QueryTimeoutOverride: true}
	for _, orgID := range []string{"tenant-a|tenant-b", "tenant-b|tenant-a", " tenant-a | tenant-b "} {
		if got := p.queryLimitsFor(orgID); got != want {
			t.Fatalf("%q:\n got %+v\nwant %+v", orgID, got, want)
		}
	}
}

// Overrides the proxy would not enforce as published are rejected at startup.
//
// conformance: tenant-query-limits, limits/published-equals-enforced
func TestTenantQueryLimits_OverrideValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		defaults map[string]any
		wantErr  string
	}{
		{"series 0", map[string]any{"max_query_series": 0.0}, "max_query_series: must be a positive integer"},
		{"series fraction", map[string]any{"max_query_series": 1.5}, "max_query_series: 1.5 is not an integer"},
		{"entries negative", map[string]any{"max_entries_limit_per_query": -1.0}, "must be 0 (unlimited) or a positive integer"},
		{"length not a duration", map[string]any{"max_query_length": 3600.0}, "max_query_length: 3600 (float64) is not a duration string"},
		{"lookback garbage", map[string]any{"max_query_lookback": "soon"}, `max_query_lookback: "soon" is not a positive duration`},
		{"timeout zero", map[string]any{"query_timeout": "0s"}, "query_timeout: must be a positive duration"},
		{"bytes read unenforced", map[string]any{"max_query_bytes_read": "1GB"}, "max_query_bytes_read: the proxy does not enforce this limit"},
		{"volume series unenforced", map[string]any{"volume_max_series": 50.0}, "volume_max_series: the proxy does not enforce this limit"},
		{"timeout above backend timeout", map[string]any{"query_timeout": "5m"}, "query_timeout: 5m is above -backend-timeout (2m), which bounds every VictoriaLogs call; raise -backend-timeout to at least 5m"},
		{"accepted", map[string]any{"max_query_series": 10.0, "max_entries_limit_per_query": 0.0, "max_query_length": "30d1h", "max_query_lookback": "0s", "max_query_range": "1h", "query_timeout": "2m", "max_query_bytes_read": "0B", "volume_max_series": 1000.0, "retention_period": "744h"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Second, 10), LogLevel: "error", TenantDefaultLimits: tc.defaults})
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
	_, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Second, 10), LogLevel: "error", TenantLimits: map[string]map[string]any{"tenant-a": {"max_query_series": -3.0}}})
	if err == nil || !strings.Contains(err.Error(), `-tenant-limits["tenant-a"] max_query_series`) {
		t.Fatalf("per-tenant error must name the tenant, got %v", err)
	}
}

// The published model.Duration form matches Loki's.
func TestFormatLokiDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0: "0s", 721 * time.Hour: "30d1h", 5 * time.Minute: "5m", 2 * time.Minute: "2m", 90 * time.Minute: "1h30m",
		7 * 24 * time.Hour: "1w", 90 * 24 * time.Hour: "90d", 365 * 24 * time.Hour: "1y", 1500 * time.Millisecond: "1s500ms",
	} {
		if got := formatLokiDuration(d); got != want {
			t.Errorf("%v: got %q, want %q", d, got, want)
		}
	}
}

// publishedLimits reads the limits a tenant sees on both published endpoints
// and fails when they disagree.
func publishedLimits(t *testing.T, mux *http.ServeMux, orgID string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/drilldown-limits", nil)
	req.Header.Set("X-Scope-OrgID", orgID)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var drilldown struct {
		Limits map[string]any `json:"limits"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &drilldown) != nil {
		t.Fatalf("drilldown-limits for %q: %d %s", orgID, rec.Code, rec.Body.String())
	}
	if hasMultiTenantOrgID(orgID) {
		return drilldown.Limits
	}
	req = httptest.NewRequest(http.MethodGet, "/config/tenant/v1/limits", nil)
	req.Header.Set("X-Scope-OrgID", orgID)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var config map[string]any
	if rec.Code != http.StatusOK || yaml.Unmarshal(rec.Body.Bytes(), &config) != nil {
		t.Fatalf("tenant limits for %q: %d %s", orgID, rec.Code, rec.Body.String())
	}
	for _, key := range []string{limitMaxQuerySeries, limitMaxEntriesLimitPerQuery, limitMaxQueryLength, limitMaxQueryLookback, limitMaxQueryRange, limitQueryTimeout} {
		if fmt.Sprint(config[key]) != fmt.Sprint(drilldown.Limits[key]) {
			t.Fatalf("%s for %q: /config/tenant/v1/limits says %v, drilldown-limits says %v", key, orgID, config[key], drilldown.Limits[key])
		}
	}
	return drilldown.Limits
}

func publishedInt(t *testing.T, limits map[string]any, key string) int {
	t.Helper()
	n, err := tenantLimitInt(limits[key])
	if err != nil {
		t.Fatalf("published %s = %v: %v", key, limits[key], err)
	}
	return n
}

func publishedDuration(t *testing.T, limits map[string]any, key string) time.Duration {
	t.Helper()
	d, err := tenantLimitDuration(limits[key])
	if err != nil {
		t.Fatalf("published %s = %v: %v", key, limits[key], err)
	}
	return d
}

// What a tenant reads on /loki/api/v1/drilldown-limits and
// /config/tenant/v1/limits is what the proxy enforces on its requests. For
// every configuration layer and tenant, each published limit is read back
// and the request path is driven exactly at it and just past it.
//
// conformance: tenant-query-limits, limits/published-equals-enforced, limits/effective-limit-resolution, limits/per-tenant-override, limits/multi-tenant-limit-combination
// conformance: limits/max-entries-limit-rejects, limits/max-query-length-error, limits/max-query-range-interval, limits/max-query-lookback, limits/query-timeout-per-tenant, series-limits-and-partial-results
// conformance: loki_api_v1_drilldown_limits, loki_api_v1_query_range, loki_api_v1_query
func TestTenantQueryLimits_PublishedEqualsEnforced(t *testing.T) {
	// Recent data: max_query_lookback must not hide it.
	base := time.Now().Add(-20 * time.Minute).Truncate(time.Minute).UTC()
	fixture := seriesLimitFixture(8, base.Add(6*time.Minute), base.Add(8*time.Minute), base.Add(9*time.Minute))
	configs := map[string]Config{
		"flags": {MaxStatsQuerySeries: 5, DefaultMaxQueryLength: 20 * time.Minute, ExecutionLimits: ExecutionLimitsConfig{MaxEntriesLimitPerQuery: 40}},
		"tenant limits": {
			MaxStatsQuerySeries: 7,
			TenantDefaultLimits: map[string]any{"max_query_series": 6.0, "max_entries_limit_per_query": 30.0, "max_query_length": "15m", "max_query_range": "5m", "max_query_lookback": "1h", "query_timeout": "45s"},
			TenantLimits:        map[string]map[string]any{"tenant-a": {"max_query_series": 4.0, "max_entries_limit_per_query": 20.0, "max_query_length": "10m", "max_query_range": "2m", "max_query_lookback": "30m", "query_timeout": "20s"}},
		},
	}
	for name, cfg := range configs {
		for _, orgID := range []string{"tenant-a", "tenant-b", "tenant-a|tenant-b"} {
			t.Run(name+"/"+orgID, func(t *testing.T) {
				fake := &seriesLimitFakeVL{t: t, lines: fixture}
				srv := httptest.NewServer(fake)
				defer srv.Close()
				p, mux := newTenantLimitsProxy(t, srv.URL, cfg)
				published := publishedLimits(t, mux, orgID)
				serve := func(path string, params url.Values) *httptest.ResponseRecorder {
					req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
					req.Header.Set("X-Scope-OrgID", orgID)
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					return rec
				}
				start, end := base.Add(5*time.Minute), base.Add(10*time.Minute)

				// max_query_series: a result of exactly the limit passes, one more
				// fails. Every tenant of a multi-tenant request answers the same
				// pods, and Loki counts the series of all of them together.
				series := publishedInt(t, published, limitMaxQuerySeries)
				tenants := len(splitMultiTenantOrgIDs(orgID))
				if series >= 8 {
					t.Fatalf("fixture too small for published max_query_series %d", series)
				}
				fake.lines = seriesLimitFixture(series/tenants, base.Add(6*time.Minute))
				if rec := serve("/loki/api/v1/query_range", tumblingRangeParams(`sum by (pod) (count_over_time({app="top"}[1m]))`, start, end, time.Minute)); rec.Code != http.StatusOK {
					t.Fatalf("max_query_series %d: %d series must pass, got %d %s", series, series/tenants*tenants, rec.Code, rec.Body.String())
				}
				fake.lines = seriesLimitFixture(series/tenants+1, base.Add(6*time.Minute))
				rec := serve("/loki/api/v1/query_range", tumblingRangeParams(`sum by (pod) (count_over_time({app="top"} | json [1m]))`, start, end, time.Minute))
				if want := (&seriesLimitError{limit: series}).Error(); rec.Code != http.StatusBadRequest || errorText(t, rec) != want {
					t.Fatalf("max_query_series %d: %d series must fail with Loki's error, got %d %s", series, (series/tenants+1)*tenants, rec.Code, rec.Body.String())
				}
				fake.lines = fixture

				// max_entries_limit_per_query: a log query asking for the limit runs,
				// one more line is Loki's 400.
				entries := publishedInt(t, published, limitMaxEntriesLimitPerQuery)
				logParams := func(limit int) url.Values {
					params := tumblingRangeParams(`{app="top"}`, start, end, time.Minute)
					params.Set("limit", strconv.Itoa(limit))
					return params
				}
				if rec := serve("/loki/api/v1/query_range", logParams(entries)); rec.Code != http.StatusOK {
					t.Fatalf("max_entries_limit_per_query %d: limit=%d must pass, got %d %s", entries, entries, rec.Code, rec.Body.String())
				}
				rec = serve("/loki/api/v1/query_range", logParams(entries+1))
				if want := fmt.Sprintf(lokiMaxEntriesErrorTemplate, entries+1, entries); rec.Code != http.StatusBadRequest || errorText(t, rec) != want {
					t.Fatalf("max_entries_limit_per_query %d: limit=%d must fail with %q, got %d %s", entries, entries+1, want, rec.Code, rec.Body.String())
				}

				// max_query_length: a range of the limit passes, a second more fails.
				length := publishedDuration(t, published, limitMaxQueryLength)
				lengthParams := func(d time.Duration) url.Values {
					return tumblingRangeParams(`{app="top"}`, end.Add(-d), end, time.Minute)
				}
				if rec := serve("/loki/api/v1/query_range", lengthParams(length)); rec.Code != http.StatusOK {
					t.Fatalf("max_query_length %v: a range of %v must pass, got %d %s", length, length, rec.Code, rec.Body.String())
				}
				rec = serve("/loki/api/v1/query_range", lengthParams(length+time.Second))
				if want := lokiQueryTooLongError(length+time.Second, length); rec.Code != http.StatusBadRequest || errorText(t, rec) != want {
					t.Fatalf("max_query_length %v: must fail with %q, got %d %s", length, want, rec.Code, rec.Body.String())
				}

				// max_query_range: a [range] of the limit passes, a longer one fails.
				if maxRange := publishedDuration(t, published, limitMaxQueryRange); maxRange > 0 {
					window := func(d time.Duration) url.Values {
						return tumblingRangeParams(fmt.Sprintf(`sum(count_over_time({app="top"}[%s]))`, formatLokiDuration(d)), start, end, time.Minute)
					}
					if rec := serve("/loki/api/v1/query_range", window(maxRange)); rec.Code != http.StatusOK {
						t.Fatalf("max_query_range %v: [%v] must pass, got %d %s", maxRange, maxRange, rec.Code, rec.Body.String())
					}
					rec = serve("/loki/api/v1/query_range", window(maxRange+time.Minute))
					if want := fmt.Sprintf("[interval] value exceeds limit: [%s] > [%s]", formatLokiDuration(maxRange+time.Minute), formatLokiDuration(maxRange)); rec.Code != http.StatusBadRequest || errorText(t, rec) != want {
						t.Fatalf("max_query_range %v: must fail with %q, got %d %s", maxRange, want, rec.Code, rec.Body.String())
					}
				}

				// max_query_lookback: a request ending before now-lookback is empty
				// without a VictoriaLogs call; one reaching past it is moved.
				if lookback := publishedDuration(t, published, limitMaxQueryLookback); lookback > 0 {
					before := fake.snapshot()
					old := time.Now().Add(-lookback - time.Hour)
					rec := serve("/loki/api/v1/query_range", tumblingRangeParams(`{app="top"}`, old.Add(-5*time.Minute), old, time.Minute))
					if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"result":[]`) || fmt.Sprint(fake.snapshot()) != fmt.Sprint(before) {
						t.Fatalf("max_query_lookback %v: an old range must be empty without VictoriaLogs, got %d %s", lookback, rec.Code, rec.Body.String())
					}
				}

				// query_timeout: the request runs under the published deadline.
				timeout := publishedDuration(t, published, limitQueryTimeout)
				limits := p.queryLimitsFor(orgID)
				if timeout != limits.QueryTimeout {
					t.Fatalf("query_timeout: published %v, enforced %v", timeout, limits.QueryTimeout)
				}
				if limits.QueryTimeoutOverride {
					var deadline time.Time
					h := p.tenantLimitsMiddleware("query_range", func(_ http.ResponseWriter, r *http.Request) { deadline, _ = r.Context().Deadline() })
					req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
					req.Header.Set("X-Scope-OrgID", orgID)
					sent := time.Now()
					h(httptest.NewRecorder(), req)
					if deadline.Before(sent.Add(timeout)) || deadline.After(time.Now().Add(timeout)) {
						t.Fatalf("query_timeout %v: request deadline %v after the request", timeout, deadline.Sub(sent))
					}
				}
			})
		}
	}
}

// errorText returns the error message of a Loki JSON error response.
func errorText(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return body.Error
}

// -max-entries-limit-per-query-cap keeps the proxy's earlier behaviour: the
// limit is lowered to max_entries_limit_per_query instead of rejected.
//
// conformance: tenant-query-limits, limits/max-entries-limit-rejects
// conformance: loki_api_v1_query_range
func TestTenantQueryLimits_MaxEntriesCapCompatibility(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	fake := &seriesLimitFakeVL{t: t, lines: seriesLimitFixture(8, base.Add(6*time.Minute))}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	var sentLimit string
	capture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path == "/select/logsql/query" {
			sentLimit = r.Form.Get("limit")
		}
		fake.ServeHTTP(w, r)
	}))
	defer capture.Close()
	_, mux := newTenantLimitsProxy(t, capture.URL, Config{MaxEntriesLimitCap: true, ExecutionLimits: ExecutionLimitsConfig{MaxEntriesLimitPerQuery: 25}})
	params := tumblingRangeParams(`{app="top"}`, base.Add(5*time.Minute), base.Add(10*time.Minute), time.Minute)
	params.Set("limit", "5000")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || sentLimit != "25" {
		t.Fatalf("cap mode must answer with the limit lowered to 25, got %d (VictoriaLogs limit %q) %s", rec.Code, sentLimit, rec.Body.String())
	}
}

// A metric query carries no entry limit; Loki checks it on log queries only.
//
// conformance: tenant-query-limits, limits/max-entries-limit-rejects
func TestTenantQueryLimits_MaxEntriesIgnoresMetricQueries(t *testing.T) {
	p := &Proxy{}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`sum(count_over_time({app="x"}[5m]))`)+"&limit=999999", nil)
	expr, _ := parseLimitedQuery(r, "query_range")
	if _, msg := p.applyMaxEntriesLimit(r, expr, 10); msg != "" {
		t.Fatalf("metric query rejected: %s", msg)
	}
	r = httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query="+url.QueryEscape(`{app="x"}`)+"&limit=11", nil)
	expr, _ = parseLimitedQuery(r, "query")
	if _, msg := p.applyMaxEntriesLimit(r, expr, 10); msg != "max entries limit per query exceeded, limit > max_entries_limit_per_query (11 > 10)" {
		t.Fatalf("instant log query: got %q", msg)
	}
}

// max_query_lookback empties an instant query and the metadata requests whose
// time lies before now-lookback, and moves an earlier start of a range to it.
//
// conformance: tenant-query-limits, limits/max-query-lookback
func TestTenantQueryLimits_LookbackMovesStartAndEmptiesOldRequests(t *testing.T) {
	now := time.Unix(1800000000, 0)
	limits := queryLimits{MaxQueryLookback: time.Hour}
	minStart := now.Add(-time.Hour).UnixNano()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?start="+strconv.FormatInt(now.Add(-3*time.Hour).UnixNano(), 10)+"&end="+strconv.FormatInt(now.UnixNano(), 10), nil)
	moved, empty, msg := applyLookbackAndLength(r, "labels", limits, now)
	if empty != nil || msg != "" || moved.FormValue("start") != strconv.FormatInt(minStart, 10) {
		t.Fatalf("labels start must move to now-lookback, got start=%s empty=%v msg=%q", moved.FormValue("start"), empty, msg)
	}
	r = httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query="+url.QueryEscape(`sum(rate({app="x"}[5m]))`)+"&time="+strconv.FormatInt(now.Add(-2*time.Hour).Unix(), 10), nil)
	_, empty, _ = applyLookbackAndLength(r, "query", limits, now)
	body, _ := json.Marshal(empty)
	if string(body) != `{"data":{"result":[],"resultType":"vector","stats":{}},"status":"success"}` {
		t.Fatalf("old instant metric query must be an empty vector, got %s", body)
	}
	r = httptest.NewRequest(http.MethodGet, "/loki/api/v1/series?match[]="+url.QueryEscape(`{app="x"}`)+"&start=1&end=2", nil)
	_, empty, _ = applyLookbackAndLength(r, "series", limits, now)
	body, _ = json.Marshal(empty)
	if string(body) != `{"data":[],"status":"success"}` {
		t.Fatalf("old series request must be empty, got %s", body)
	}
}

// drilldown-limits answers a multi-tenant header with the limits the proxy
// enforces on it (Loki answers 401), and /config/tenant/v1/limits with Loki's
// 401.
//
// conformance: tenant-query-limits, limits/multi-tenant-limit-combination
// conformance: loki_api_v1_drilldown_limits
func TestTenantQueryLimits_MultiTenantPublishedLimits(t *testing.T) {
	_, mux := newTenantLimitsProxy(t, "http://127.0.0.1:1", Config{TenantLimits: map[string]map[string]any{
		"tenant-a": {"max_query_series": 40.0, "query_timeout": "40s"},
		"tenant-b": {"max_query_series": 90.0, "query_timeout": "20s"},
	}})
	limits := publishedLimits(t, mux, "tenant-a|tenant-b")
	if limits[limitMaxQuerySeries] != 40.0 || limits[limitQueryTimeout] != "20s" {
		t.Fatalf("combined published limits: %v", limits)
	}
	req := httptest.NewRequest(http.MethodGet, "/config/tenant/v1/limits", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || strings.TrimSpace(rec.Body.String()) != "multiple org IDs present" {
		t.Fatalf("want Loki's 401, got %d %q", rec.Code, rec.Body.String())
	}
}

// Loki decodes and parses a request before its limits run: a query that does
// not parse, or a range above 11,000 points, gets that error even when it is
// also over a limit.
//
// conformance: tenant-query-limits, limits/max-entries-limit-rejects, limits/max-query-length-error
func TestTenantQueryLimits_DecodeErrorsComeFirst(t *testing.T) {
	_, mux := newTenantLimitsProxy(t, "http://127.0.0.1:1", Config{
		ExecutionLimits:     ExecutionLimitsConfig{MaxEntriesLimitPerQuery: 10},
		TenantDefaultLimits: map[string]any{"max_query_length": "1h"},
	})
	serve := func(params url.Values) string {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("want 400, got %d %s", rec.Code, rec.Body.String())
		}
		return errorText(t, rec)
	}
	if msg := serve(url.Values{"query": {`{app="x"`}, "limit": {"11"}, "start": {"0"}, "end": {"60"}}); strings.Contains(msg, "max entries limit") {
		t.Fatalf("a parse error must win over max_entries_limit_per_query, got %q", msg)
	}
	if msg := serve(url.Values{"query": {`{app="x"}`}, "start": {"0"}, "end": {"86400"}, "step": {"1"}}); msg != errLokiStepTooSmall {
		t.Fatalf("the resolution error must win over max_query_length, got %q", msg)
	}
}

// query_timeout bounds the requests Loki runs under it (WrapQuerySpanAndTimeout)
// and never a live tail; max_query_range is checked after the limits
// middleware, as Loki's engine checks it; an instant log query passes Loki's
// frontend without max_query_lookback; label_replace keeps its range selector
// under max_query_range.
//
// conformance: tenant-query-limits, limits/query-timeout-per-tenant, limits/max-query-range-interval, limits/max-query-lookback
func TestTenantQueryLimits_LokiScopeAndOrder(t *testing.T) {
	p, _ := newTenantLimitsProxy(t, "http://127.0.0.1:1", Config{TenantDefaultLimits: map[string]any{
		"query_timeout": "30s", "max_query_range": "5m", "max_query_length": "1h", "max_query_lookback": "2h",
	}})
	deadline := func(endpoint string) bool {
		var has bool
		h := p.tenantLimitsMiddleware(endpoint, func(_ http.ResponseWriter, r *http.Request) { _, has = r.Context().Deadline() })
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		h(httptest.NewRecorder(), req)
		return has
	}
	for endpoint, want := range map[string]bool{"query_range": true, "labels": true, "volume_range": true, "tail": false, "patterns": false, "detected_fields": false} {
		if got := deadline(endpoint); got != want {
			t.Errorf("%s: query_timeout deadline %v, want %v", endpoint, got, want)
		}
	}

	serve := func(endpoint string, params url.Values) *httptest.ResponseRecorder {
		h := p.tenantLimitsMiddleware(endpoint, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		req := httptest.NewRequest(http.MethodGet, "/?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}
	now := time.Now()
	// Over max_query_length and max_query_range: Loki's length error first.
	rec := serve("query_range", url.Values{"query": {`sum(rate({app="x"}[10m]))`}, "start": {strconv.FormatInt(now.Add(-90*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"60"}})
	if msg := errorText(t, rec); !strings.HasPrefix(msg, "the query time range exceeds the limit") {
		t.Fatalf("length must be checked before the range selector, got %d %q", rec.Code, msg)
	}
	// Wholly before the lookback with a long range selector: Loki's empty answer.
	old := now.Add(-5 * time.Hour)
	rec = serve("query_range", url.Values{"query": {`sum(rate({app="x"}[10m]))`}, "start": {strconv.FormatInt(old.Add(-10*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(old.UnixNano(), 10)}, "step": {"60"}})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"resultType":"matrix"`) {
		t.Fatalf("a request before the lookback must be empty, got %d %s", rec.Code, rec.Body.String())
	}
	// An instant log query before the lookback runs, as in Loki.
	if rec = serve("query", url.Values{"query": {`{app="x"}`}, "time": {strconv.FormatInt(old.Unix(), 10)}}); rec.Code != http.StatusNoContent {
		t.Fatalf("an instant log query must reach the handler, got %d %s", rec.Code, rec.Body.String())
	}
	// A range selector inside label_replace is still limited.
	rec = serve("query_range", url.Values{"query": {`label_replace(rate({app="x"}[10m]), "a", "$1", "app", "(.*)")`}, "start": {strconv.FormatInt(now.Add(-10*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"60"}})
	if msg := errorText(t, rec); msg != "[interval] value exceeds limit: [10m] > [5m]" {
		t.Fatalf("label_replace range selector: got %d %q", rec.Code, msg)
	}
}
