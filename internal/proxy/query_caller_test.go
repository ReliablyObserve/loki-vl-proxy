package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// callerLogHandler is slowLogHandler with the request's tenant scope, as the proxy's routes set it.
func callerLogHandler(p *Proxy) http.HandlerFunc {
	h := slowLogHandler(p)
	return func(w http.ResponseWriter, r *http.Request) { h(w, p.withRequestScope(r)) }
}

func serveCallerLog(p *Proxy, header http.Header) {
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api"}[1m])`), nil)
	req.RemoteAddr = "10.1.2.3:51000"
	for k, v := range header {
		req.Header[k] = v
	}
	p.requestLogger("query_range", "/loki/api/v1/query_range", callerLogHandler(p)).ServeHTTP(httptest.NewRecorder(), req)
}

// callerLines returns every request, upstream_request, slow_request and slow_upstream_request line.
func callerLines(t *testing.T, buf *bytes.Buffer) map[string][]map[string]any {
	t.Helper()
	out := map[string][]map[string]any{}
	for _, msg := range []string{"request", "upstream_request", "slow_request", "slow_upstream_request"} {
		out[msg] = slowLogLines(t, buf, msg)
		if len(out[msg]) != 1 {
			t.Fatalf("%s lines: %d\n%s", msg, len(out[msg]), buf)
		}
	}
	return out
}

// Every line that reports a query names who ran it (enduser.id and enduser.source), the tenant and the Grafana
// app's tags (all of X-Query-Tags when it holds more than the Source tag); the slow lines add where the tenant's queries go in VictoriaLogs and what the request cost there
// (calls, time, bytes, rows).
//
// conformance: observability-slow-query-log
func TestQueryLinesCarryTheCallerTenantAndCost(t *testing.T) {
	p, buf := slowLogProxy(t, 40*time.Millisecond)
	p.metricsTrustProxyHeaders = true
	p.tenantMap = map[string]TenantMapping{"team-a": {AccountID: "42", ProjectID: "7"}}
	serveCallerLog(p, http.Header{
		"X-Scope-Orgid":  {"team-a"},
		"X-Grafana-User": {"alice@example.com"},
		"X-Query-Tags":   {"Source=grafana-lokiexplore-app,Feature=patterns"},
	})
	lines := callerLines(t, buf)
	for msg, ls := range lines {
		l := ls[0]
		if l["enduser.id"] != "alice@example.com" || l["enduser.source"] != "grafana_user" || l["enduser.name"] != "alice@example.com" {
			t.Fatalf("%s: user %v / %v / %v", msg, l["enduser.id"], l["enduser.source"], l["enduser.name"])
		}
		if l["loki.tenant.id"] != "team-a" {
			t.Fatalf("%s: tenant %v", msg, l["loki.tenant.id"])
		}
		if l["grafana.client.source_tag"] != "grafana-lokiexplore-app" || l["grafana.client.surface"] != "grafana_drilldown" || l["grafana.query_tags"] != "Source=grafana-lokiexplore-app,Feature=patterns" {
			t.Fatalf("%s: grafana tags %v / %v / %v", msg, l["grafana.client.source_tag"], l["grafana.client.surface"], l["grafana.query_tags"])
		}
	}
	for _, msg := range []string{"slow_request", "slow_upstream_request"} {
		l := lines[msg][0]
		if l["vl.account_id"] != "42" || l["vl.project_id"] != "7" {
			t.Fatalf("%s: VictoriaLogs tenant %v:%v", msg, l["vl.account_id"], l["vl.project_id"])
		}
	}
	slow := lines["slow_request"][0]
	if slow["flow.vl.calls"] != float64(1) || slow["flow.vl.bytes"].(float64) <= 0 || slow["flow.vl.ms"].(float64) < 40 || slow["flow.vl.rows"] == nil {
		t.Fatalf("slow_request cost: calls %v bytes %v ms %v rows %v", slow["flow.vl.calls"], slow["flow.vl.bytes"], slow["flow.vl.ms"], slow["flow.vl.rows"])
	}
	if up := lines["slow_upstream_request"][0]; up["vl.body_bytes"].(float64) <= 0 || up["event.duration"].(float64) <= 0 {
		t.Fatalf("slow_upstream_request cost %v", up)
	}
}

// Without trusted user headers the caller is the tenant; a user header from a client the proxy does not trust is
// never logged (it is spoofable); a numeric tenant goes to its own VictoriaLogs account, label routing names its
// field, and a multi-tenant request names no account of its own.
//
// conformance: observability-slow-query-log
func TestQueryLinesCallerWithoutTrustedHeaders(t *testing.T) {
	p, buf := slowLogProxy(t, 40*time.Millisecond)
	serveCallerLog(p, http.Header{"X-Scope-Orgid": {"7"}, "X-Grafana-User": {"mallory"}})
	for msg, ls := range callerLines(t, buf) {
		if l := ls[0]; l["enduser.id"] != "7" || l["enduser.source"] != "tenant" || l["enduser.name"] != nil || l["grafana.query_tags"] != nil {
			t.Fatalf("%s: %v / %v / %v / %v", msg, l["enduser.id"], l["enduser.source"], l["enduser.name"], l["grafana.query_tags"])
		}
	}
	if strings.Contains(buf.String(), "mallory") {
		t.Fatalf("an untrusted X-Grafana-User reached the log\n%s", buf)
	}
	if slow := slowLogLines(t, buf, "slow_request")[0]; slow["vl.account_id"] != "7" || slow["vl.project_id"] != "0" {
		t.Fatalf("numeric tenant scope %v:%v", slow["vl.account_id"], slow["vl.project_id"])
	}

	p, buf = slowLogProxy(t, 40*time.Millisecond)
	p.tenantLabel = "tenant"
	serveCallerLog(p, http.Header{"X-Scope-Orgid": {"team-b"}})
	if slow := slowLogLines(t, buf, "slow_request")[0]; slow["vl.tenant_label"] != "tenant" || slow["vl.account_id"] != nil {
		t.Fatalf("label routing scope %v", slow)
	}

	p, buf = slowLogProxy(t, 40*time.Millisecond)
	serveCallerLog(p, http.Header{"X-Scope-Orgid": {"1|2"}})
	if slow := slowLogLines(t, buf, "slow_request")[0]; slow["loki.tenant.id"] != "1|2" || slow["vl.account_id"] != nil {
		t.Fatalf("multi-tenant scope %v", slow)
	}
}

// A client-sent identity is capped and its control characters escaped, so it can neither forge a log line nor
// grow one without bound, in JSON and text logs alike.
//
// conformance: observability-slow-query-log
func TestQueryLinesCallerIsCappedAndEscaped(t *testing.T) {
	// HTTP itself refuses CR, LF and the other C0 controls in a header value (the unit table covers them); what a
	// client can send is any byte from 0x80 up: C1 controls, U+2028 and U+2029 (line breaks to JavaScript log
	// viewers) and invalid UTF-8.
	forged := "bob\u2028time=2026-11-11 level=INFO msg=request enduser.id=admin\u0085\xff" + strings.Repeat("x", 400)
	for _, handler := range []string{"json", "text"} {
		p, buf := slowLogProxy(t, 40*time.Millisecond)
		if handler == "text" {
			p.log = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		}
		p.metricsTrustProxyHeaders = true
		serveCallerLog(p, http.Header{"X-Scope-Orgid": {"team-a"}, "X-Grafana-User": {forged}, "X-Query-Tags": {"Source=x\u2029msg=slow_request " + strings.Repeat("y", 600)}})
		out := buf.String()
		// One line per message: request, upstream_request, slow_request, slow_upstream_request.
		if n := strings.Count(strings.TrimSpace(out), "\n") + 1; n != 4 {
			t.Fatalf("%s: %d lines, want 4\n%s", handler, n, out)
		}
		if strings.Contains(out, "\x1b") || strings.Contains(out, " ") || strings.Contains(out, "\r") {
			t.Fatalf("%s: a control character reached the log\n%q", handler, out)
		}
		if strings.Contains(out, strings.Repeat("x", 200)) || strings.Contains(out, strings.Repeat("y", 300)) {
			t.Fatalf("%s: an identity was not capped\n%s", handler, out)
		}
	}
}

func TestSanitizeCallerValue(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"alice", "alice"},
		{"  alice  ", "alice"},
		{"zoë", "zoë"},
		{"a\nb", "a\\u000ab"},
		{"a\x1bb", "a\\u001bb"},
		{"a\x7fb", "a\\u007fb"},
		{"a\u0085b", "a\\u0085b"},
		{"a\u2028b", "a\\u2028b"},
		{"a\u2029b", "a\\u2029b"},
		{"a\xffb", "a\\xffb"},
		{strings.Repeat("a", 10), strings.Repeat("a", 10)},
		{strings.Repeat("a", 11), strings.Repeat("a", 10) + "..."},
		{strings.Repeat("a", 9) + "ë", strings.Repeat("a", 9) + "..."},
		{strings.Repeat("a", 7) + "\x01", strings.Repeat("a", 7) + "..."},
	} {
		if got := sanitizeCallerValue(tc.in, 10); got != tc.want {
			t.Errorf("sanitizeCallerValue(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = sanitizeCallerValue("alice@example.com", callerIdentityMaxBytes) }); n != 0 {
		t.Fatalf("a clean value allocates %v times", n)
	}
}

// -log-user-header names the user only when the connection comes from a trusted peer: the same header from any
// other peer, or a trusted address claimed in X-Forwarded-For, leaves the request under its tenant.
//
// conformance: observability-slow-query-log
func TestLogUserHeaderNamesTheUserFromTrustedPeersOnly(t *testing.T) {
	for _, tc := range []struct {
		name, cidrs, peer, forwardedFor, wantUser, wantSource string
	}{
		{"any peer trusted", "", "192.0.2.9:4000", "", "alice", "grafana_user"},
		{"trusted peer", "10.0.0.0/8, 192.0.2.1", "10.1.2.3:4000", "", "alice", "grafana_user"},
		{"trusted single address", "10.0.0.0/8, 192.0.2.1", "192.0.2.1:4000", "", "alice", "grafana_user"},
		{"untrusted peer", "10.0.0.0/8", "192.0.2.9:4000", "", "team-a", "tenant"},
		{"spoofed X-Forwarded-For", "10.0.0.0/8", "192.0.2.9:4000", "10.1.2.3", "team-a", "tenant"},
		{"IPv4-mapped peer", "10.0.0.0/8", "[::ffff:10.1.2.3]:4000", "", "alice", "grafana_user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, buf := slowLogProxy(t, 40*time.Millisecond)
			var err error
			if p.logUser, err = parseLogUserHeader("x-grafana-user", tc.cidrs); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`count_over_time({app="api"}[1m])`), nil)
			req.RemoteAddr = tc.peer
			req.Header.Set("X-Scope-OrgID", "team-a")
			req.Header.Set("X-Grafana-User", "alice")
			if tc.forwardedFor != "" {
				req.Header.Set("X-Forwarded-For", tc.forwardedFor)
			}
			p.requestLogger("query_range", "/loki/api/v1/query_range", callerLogHandler(p)).ServeHTTP(httptest.NewRecorder(), req)
			for msg, ls := range callerLines(t, buf) {
				if l := ls[0]; l["enduser.id"] != tc.wantUser || l["enduser.source"] != tc.wantSource {
					t.Fatalf("%s: %v / %v, want %s / %s", msg, l["enduser.id"], l["enduser.source"], tc.wantUser, tc.wantSource)
				}
			}
			if tc.wantUser != "alice" && strings.Contains(buf.String(), "alice") {
				t.Fatalf("an untrusted user header reached the log\n%s", buf)
			}
		})
	}
	for _, tc := range []struct{ header, cidrs, source string }{
		{"X-Grafana-User", "", "grafana_user"},
		{"x-forwarded-user", "", "forwarded_user"},
		{"X-Email", "10.0.0.0/8", "user_header"},
	} {
		if h, err := parseLogUserHeader(tc.header, tc.cidrs); err != nil || h.source != tc.source {
			t.Fatalf("parseLogUserHeader(%q, %q) = %+v, %v", tc.header, tc.cidrs, h, err)
		}
	}
	for _, tc := range []struct{ header, cidrs string }{
		{"", "10.0.0.0/8"},
		{"X-Grafana-User", "10.0.0.0/33"},
		{"X-Grafana-User", "grafana.internal"},
	} {
		if _, err := parseLogUserHeader(tc.header, tc.cidrs); err == nil {
			t.Fatalf("parseLogUserHeader(%q, %q) accepted", tc.header, tc.cidrs)
		}
	}
	if _, err := New(Config{BackendURL: "http://unused", LogUserHeaderTrustedCIDRs: "10.0.0.0/8"}); err == nil {
		t.Fatal("New accepted trusted CIDRs without -log-user-header")
	}
}

// The user -log-user-header names reaches the logs only: two users asking the same question share one cache
// entry and one VictoriaLogs request, the header is not sent to VictoriaLogs, and no metric names a user even
// with -metrics.export-sensitive-labels.
//
// conformance: observability-slow-query-log
func TestLogUserHeaderNeverSplitsTheCacheReachesVictoriaLogsOrLabelsAMetric(t *testing.T) {
	var calls atomic.Int32
	var forwarded atomic.Value
	forwarded.Store("")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if v := r.Header.Get("X-Grafana-User"); v != "" {
			forwarded.Store(v)
		}
		_, _ = io.WriteString(w, `{"values":[{"value":"app","hits":1}]}`)
	}))
	defer backend.Close()
	p, err := New(Config{
		BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
		TenantMap:                    map[string]TenantMapping{"team-a": {AccountID: "10", ProjectID: "0"}},
		LogUserHeader:                "X-Grafana-User",
		MetricsExportSensitiveLabels: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	a := doCompatProxyRequest(p, "/loki/api/v1/labels", map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "alice"})
	b := doCompatProxyRequest(p, "/loki/api/v1/labels", map[string]string{"X-Scope-OrgID": "team-a", "X-Grafana-User": "bob"})
	if a.Code != 200 || b.Code != 200 || a.Body.String() != b.Body.String() {
		t.Fatalf("answers %d %s / %d %s", a.Code, a.Body, b.Code, b.Body)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("VictoriaLogs calls: %d, want 1 (the second user is served from the cache)", n)
	}
	if v := forwarded.Load().(string); v != "" {
		t.Fatalf("X-Grafana-User reached VictoriaLogs: %q", v)
	}
	var users []string
	for _, l := range slowLogLines(t, &buf, "request") {
		users = append(users, fmt.Sprint(l["enduser.id"], "/", l["enduser.source"]))
	}
	if strings.Join(users, ",") != "alice/grafana_user,bob/grafana_user" {
		t.Fatalf("request lines name %v", users)
	}
	body := metricsBody(p)
	if !strings.Contains(body, `tenant="team-a"`) {
		t.Fatalf("sensitive labels are not exported; the check below proves nothing\n%s", body)
	}
	if strings.Contains(body, "alice") || strings.Contains(body, "bob") {
		t.Fatalf("a user reached a metric label")
	}
	pushed := make(chan string, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		select {
		case pushed <- string(b):
		default:
		}
	}))
	defer collector.Close()
	pusher := metrics.NewOTLPPusher(metrics.OTLPConfig{Endpoint: collector.URL, Interval: 10 * time.Millisecond}, p.metrics)
	pusher.Start()
	otlp := <-pushed
	pusher.Stop()
	if !strings.Contains(otlp, "team-a") {
		t.Fatalf("the OTLP push carries no tenant series; the check below proves nothing")
	}
	if strings.Contains(otlp, "alice") || strings.Contains(otlp, "bob") {
		t.Fatalf("a user reached an OTLP metric attribute")
	}
}

// /debug/queries names who ran the recent requests and which callers cost VictoriaLogs the most, by user and
// tenant, filterable by either; live tail is left out.
//
// conformance: observability-slow-query-log
func TestDebugQueriesShowsCallersAndTheirCost(t *testing.T) {
	p, _ := slowLogProxy(t, 40*time.Millisecond)
	p.enableQueryAnalytics = true
	p.logUser, _ = parseLogUserHeader("X-Grafana-User", "")
	for _, user := range []string{"alice", "bob", "alice"} {
		serveCallerLog(p, http.Header{"X-Scope-Orgid": {"team-a"}, "X-Grafana-User": {user}, "X-Query-Tags": {"Source=grafana-lokiexplore-app"}})
	}
	serveSlowLog(p, "tail", "/loki/api/v1/tail", slowLogHandler(p))

	rec := httptest.NewRecorder()
	p.queryTracker.Handler(rec, httptest.NewRequest(http.MethodGet, "/debug/queries?user=alice&tenant=team-a", nil))
	var out struct {
		Callers metrics.CallerView `json:"callers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Callers.Callers != 1 || len(out.Callers.TopByVLMs) != 1 {
		t.Fatalf("callers %+v", out.Callers)
	}
	top := out.Callers.TopByVLMs[0]
	if top.User != "alice" || top.UserSource != "grafana_user" || top.Tenant != "team-a" || top.Requests != 2 || top.Slow != 2 ||
		top.VLCalls != 2 || top.VLMs < 80 || top.VLBytes <= 0 || top.LastGrafanaSource != "grafana-lokiexplore-app" {
		t.Fatalf("top caller %+v", top)
	}
	if len(out.Callers.RecentRequests) != 2 || out.Callers.RecentRequests[0].Query != `count_over_time({app="api"}[1m])` {
		t.Fatalf("recent requests %+v", out.Callers.RecentRequests)
	}
	all := p.queryTracker
	rec = httptest.NewRecorder()
	all.Handler(rec, httptest.NewRequest(http.MethodGet, "/debug/queries", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Callers.Callers != 2 || len(out.Callers.RecentRequests) != 3 {
		t.Fatalf("unfiltered: %d callers, %d requests (live tail must not count)", out.Callers.Callers, len(out.Callers.RecentRequests))
	}
}

// The callers view applies the same trust and bounds as the log lines: a user header from an untrusted peer never
// reaches it, and an oversized or forged one is capped and escaped.
//
// conformance: observability-slow-query-log
func TestDebugQueriesCallerTrustAndBounds(t *testing.T) {
	p, _ := slowLogProxy(t, 0)
	p.enableQueryAnalytics = true
	p.logUser, _ = parseLogUserHeader("X-Grafana-User", "10.0.0.0/8")
	serve := func(peer, user string) {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`{app="api"}`), nil)
		req.RemoteAddr = peer
		req.Header.Set("X-Scope-OrgID", "team-a")
		req.Header.Set("X-Grafana-User", user)
		p.requestLogger("query_range", "/loki/api/v1/query_range", callerLogHandler(p)).ServeHTTP(httptest.NewRecorder(), req)
	}
	serve("192.0.2.9:4000", "mallory")
	serve("10.1.2.3:4000", "eve\u2028"+strings.Repeat("z", 400))
	rec := httptest.NewRecorder()
	p.queryTracker.Handler(rec, httptest.NewRequest(http.MethodGet, "/debug/queries", nil))
	body := rec.Body.String()
	if strings.Contains(body, "mallory") {
		t.Fatalf("an untrusted user header reached /debug/queries\n%s", body)
	}
	if strings.Contains(body, "\u2028") || strings.Contains(body, strings.Repeat("z", 200)) {
		t.Fatalf("a forged or oversized user reached /debug/queries unescaped or uncapped")
	}
	var out struct {
		Callers metrics.CallerView `json:"callers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	users := map[string]string{}
	for _, c := range out.Callers.TopByVLMs {
		users[c.UserSource] = c.User
	}
	if users["tenant"] != "team-a" || !strings.HasPrefix(users["grafana_user"], `eve\u2028zz`) || len(users["grafana_user"]) > callerIdentityMaxBytes+3 {
		t.Fatalf("callers %v", users)
	}
}

// VictoriaLogs calls made in parallel by one request share its caller: each upstream_request line names it, the
// first line resolving it (run under -race).
//
// conformance: observability-slow-query-log
func TestParallelUpstreamLinesShareTheCaller(t *testing.T) {
	p, buf := slowLogProxy(t, 0)
	p.logUser, _ = parseLogUserHeader("X-Grafana-User", "")
	h := func(w http.ResponseWriter, r *http.Request) {
		r = p.withRequestScope(r)
		done := make(chan struct{})
		for range 4 {
			go func() {
				defer func() { done <- struct{}{} }()
				if resp, err := p.vlGet(r.Context(), "/select/logsql/query", url.Values{"query": {`app:="api"`}}); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			}()
		}
		for range 4 {
			<-done
		}
		w.WriteHeader(http.StatusOK)
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`{app="api"}`), nil)
	req.Header.Set("X-Scope-OrgID", "team-a")
	req.Header.Set("X-Grafana-User", "alice")
	p.requestLogger("query_range", "/loki/api/v1/query_range", h).ServeHTTP(httptest.NewRecorder(), req)
	lines := slowLogLines(t, buf, "upstream_request")
	if len(lines) != 4 {
		t.Fatalf("upstream_request lines: %d", len(lines))
	}
	for _, l := range lines {
		if l["enduser.id"] != "alice" || l["enduser.source"] != "grafana_user" {
			t.Fatalf("upstream_request %v", l)
		}
	}
}
