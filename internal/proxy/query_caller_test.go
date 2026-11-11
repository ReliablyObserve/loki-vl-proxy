package proxy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
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
// app's tags; the slow lines add where the tenant's queries go in VictoriaLogs and what the request cost there
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
		"X-Query-Tags":   {"Source=grafana-lokiexplore-app"},
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
		if l["grafana.client.source_tag"] != "grafana-lokiexplore-app" || l["grafana.client.surface"] != "grafana_drilldown" || l["grafana.query_tags"] != "Source=grafana-lokiexplore-app" {
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
