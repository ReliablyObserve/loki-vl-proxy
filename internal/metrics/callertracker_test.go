package metrics

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func callerReq(tenant, user string, vlMs, vlBytes int64) CallerRequest {
	return CallerRequest{Tenant: tenant, User: user, UserSource: "grafana_user", Route: "/loki/api/v1/query_range",
		Status: 200, CallerCost: CallerCost{DurationMs: vlMs + 1, VLCalls: 1, VLMs: vlMs, VLBytes: vlBytes, VLRows: 10}}
}

// The heaviest callers come first by VictoriaLogs time and by bytes, with their summed cost; the recent requests
// are newest first; both filter by tenant and user.
//
// conformance: observability-slow-query-log
func TestCallerTrackerTopCallersAndFilters(t *testing.T) {
	ct := NewCallerTracker()
	ct.Record(callerReq("team-a", "alice", 100, 10))
	ct.Record(callerReq("team-a", "alice", 300, 10))
	ct.Record(callerReq("team-a", "bob", 50, 5000))
	ct.Record(callerReq("team-b", "carol", 200, 100))

	v := ct.View("", "", 20)
	if v.Callers != 3 || v.Overflowed {
		t.Fatalf("callers %d overflowed %v", v.Callers, v.Overflowed)
	}
	if got := v.TopByVLMs[0]; got.User != "alice" || got.Tenant != "team-a" || got.VLMs != 400 || got.Requests != 2 || got.VLCalls != 2 || got.VLRows != 20 {
		t.Fatalf("top by VictoriaLogs time %+v", got)
	}
	if got := v.TopByVLBytes[0]; got.User != "bob" || got.VLBytes != 5000 {
		t.Fatalf("top by VictoriaLogs bytes %+v", got)
	}
	if len(v.RecentRequests) != 4 || v.RecentRequests[0].User != "carol" || v.RecentRequests[3].VLMs != 100 {
		t.Fatalf("recent requests %+v", v.RecentRequests)
	}
	if v := ct.View("team-a", "", 20); v.Callers != 2 || len(v.RecentRequests) != 3 {
		t.Fatalf("tenant filter: %d callers, %d requests", v.Callers, len(v.RecentRequests))
	}
	if v := ct.View("", "bob", 20); v.Callers != 1 || len(v.RecentRequests) != 1 || v.TopByVLMs[0].User != "bob" {
		t.Fatalf("user filter %+v", v)
	}
	if v := ct.View("", "", 1); len(v.TopByVLMs) != 1 || len(v.TopByVLBytes) != 1 {
		t.Fatalf("top 1: %d / %d", len(v.TopByVLMs), len(v.TopByVLBytes))
	}
}

// Memory stays bounded: past the caller limit new callers are summed under one overflow caller, the recent
// requests are a ring, and a recent request's query is capped.
//
// conformance: observability-slow-query-log
func TestCallerTrackerIsBounded(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	ct := newCallerTracker(time.Minute, 3, 5, clock.now)
	for i := range 10 {
		r := callerReq("team-a", fmt.Sprintf("user-%d", i), 10, 1)
		r.Query = strings.Repeat("q", 2000)
		ct.Record(r)
	}
	if len(ct.cur) != 4 {
		t.Fatalf("%d callers kept, want 3 plus the overflow caller", len(ct.cur))
	}
	v := ct.View("", "", 20)
	if !v.Overflowed || v.Callers != 4 {
		t.Fatalf("overflowed %v callers %d", v.Overflowed, v.Callers)
	}
	for _, s := range v.TopByVLMs {
		if s.User == CallerOverflow && (s.Requests != 7 || s.VLMs != 70) {
			t.Fatalf("overflow caller %+v, want the 7 requests past the limit", s)
		}
	}
	if len(v.RecentRequests) != 5 || v.RecentRequests[0].User != "user-9" || v.RecentRequests[4].User != "user-5" {
		t.Fatalf("recent ring %+v", v.RecentRequests)
	}
	if q := v.RecentRequests[0].Query; len(q) != callerTrackerQueryBytes+3 {
		t.Fatalf("recent query not capped: %d bytes", len(q))
	}
}

// The view covers the current half-window and the one before it: a caller is forgotten once two windows passed.
//
// conformance: observability-slow-query-log
func TestCallerTrackerWindow(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	ct := newCallerTracker(time.Minute, 10, 10, clock.now)
	ct.Record(callerReq("team-a", "alice", 10, 1))
	clock.t = clock.t.Add(90 * time.Second)
	ct.Record(callerReq("team-a", "bob", 10, 1))
	if v := ct.View("", "", 20); v.Callers != 2 || v.WindowSeconds != 120 {
		t.Fatalf("one window later: %d callers, window %ds", v.Callers, v.WindowSeconds)
	}
	clock.t = clock.t.Add(70 * time.Second)
	if v := ct.View("", "", 20); v.Callers != 1 || v.TopByVLMs[0].User != "bob" {
		t.Fatalf("alice's half rotated out: %+v", v.TopByVLMs)
	}
	clock.t = clock.t.Add(5 * time.Minute)
	if v := ct.View("", "", 20); v.Callers != 0 {
		t.Fatalf("idle past two windows: %d callers", v.Callers)
	}
}

// /debug/queries serves the callers view with its ?tenant= and ?user= filters.
//
// conformance: observability-slow-query-log
func TestQueryTrackerHandlerServesTheCallers(t *testing.T) {
	qt := NewQueryTracker(10)
	qt.RecordRequest(callerReq("team-a", "alice", 10, 1))
	qt.RecordRequest(callerReq("team-b", "bob", 20, 2))
	rec := httptest.NewRecorder()
	qt.Handler(rec, httptest.NewRequest("GET", "/debug/queries?user=alice", nil))
	var out struct {
		Callers CallerView `json:"callers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Callers.Callers != 1 || out.Callers.TopByVLMs[0].User != "alice" || out.Callers.TopByVLMs[0].Tenant != "team-a" || len(out.Callers.RecentRequests) != 1 {
		t.Fatalf("callers %+v", out.Callers)
	}
}

// Per-tenant VictoriaLogs cost: exported with the other tenant metrics only, nothing for a request without a
// VictoriaLogs call, the tenant label bounded by -metrics.max-tenants, and mirrored in OTLP.
//
// conformance: observability-slow-query-log
func TestTenantVLCostMetrics(t *testing.T) {
	m := NewMetricsWithOptions(2, 10, true)
	m.RecordTenantVLCost("team-a", 2, 1500*time.Millisecond, 4096, 30)
	m.RecordTenantVLCost("team-a", 1, 500*time.Millisecond, 1024, 0)
	m.RecordTenantVLCost("team-b", 0, time.Second, 99, 99)
	body := scrape(m)
	for _, want := range []string{
		`loki_vl_proxy_tenant_vl_calls_total{tenant="team-a"} 3`,
		`loki_vl_proxy_tenant_vl_duration_seconds_total{tenant="team-a"} 2`,
		`loki_vl_proxy_tenant_vl_response_bytes_total{tenant="team-a"} 5120`,
		`loki_vl_proxy_tenant_vl_rows_total{tenant="team-a"} 30`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s\n%s", want, body)
		}
	}
	if strings.Contains(body, `loki_vl_proxy_tenant_vl_calls_total{tenant="team-b"}`) {
		t.Fatal("a request without a VictoriaLogs call added a tenant cost series")
	}
	m.RecordTenantVLCost("team-c", 1, time.Millisecond, 1, 1)
	m.RecordTenantVLCost("team-d", 1, time.Millisecond, 1, 1)
	if body := scrape(m); !strings.Contains(body, `loki_vl_proxy_tenant_vl_calls_total{tenant="__overflow__"} 1`) || strings.Contains(body, `"team-d"`) {
		t.Fatalf("tenant label not bounded\n%s", body)
	}
	p := &OTLPPusher{metrics: m}
	var names []string
	for _, metric := range p.tenantMetrics(1) {
		names = append(names, metric["name"].(string))
	}
	if got := strings.Join(names, ","); !strings.Contains(got, "loki_vl_proxy_tenant_vl_calls_total") || !strings.Contains(got, "loki_vl_proxy_tenant_vl_rows_total") {
		t.Fatalf("OTLP tenant metrics %s", got)
	}

	off := NewMetricsWithOptions(10, 10, false)
	off.RecordTenantVLCost("team-a", 1, time.Second, 1, 1)
	if strings.Contains(scrape(off), "loki_vl_proxy_tenant_vl_") {
		t.Fatal("tenant cost exported without -metrics.export-sensitive-labels")
	}
}

func scrape(m *Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

// Requests record while operators read the view (run under -race).
func TestCallerTrackerConcurrent(t *testing.T) {
	ct := newCallerTracker(time.Millisecond, 8, 16, time.Now)
	done := make(chan struct{})
	for g := range 4 {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range 500 {
				ct.Record(callerReq("team", fmt.Sprintf("u%d-%d", g, i%20), 1, 1))
				if i%50 == 0 {
					_ = ct.View("", "", 5)
				}
			}
		}()
	}
	for range 4 {
		<-done
	}
	if v := ct.View("", "", 5); len(v.RecentRequests) > 16 || v.Callers > 2*9 {
		t.Fatalf("unbounded: %d recent, %d callers", len(v.RecentRequests), v.Callers)
	}
}
