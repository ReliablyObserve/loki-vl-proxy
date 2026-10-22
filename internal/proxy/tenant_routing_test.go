package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := strings.Split(strings.TrimSpace(b.buf.String()), "\n")
	if len(out) == 1 && out[0] == "" {
		return nil
	}
	return out
}

// tenantProbe admits orgID like the tenant middleware does and returns the
// AccountID/ProjectID a backend request for it carries. missing probes a
// request with no X-Scope-OrgID header.
func tenantProbe(t *testing.T, p *Proxy, orgID string, missing bool) (account, project string, admitErr error) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	if !missing {
		r.Header["X-Scope-Orgid"] = []string{orgID}
	}
	r = p.withRequestScope(r)
	admitErr = p.validateTenantHeader(r)
	backend := httptest.NewRequest(http.MethodGet, "http://vl/select/logsql/query", nil).WithContext(r.Context())
	p.forwardTenantHeaders(backend)
	return backend.Header.Get("AccountID"), backend.Header.Get("ProjectID"), admitErr
}

type tenantExpect struct {
	status           int // 200 admitted; otherwise the refusal status
	account, project string
}

func tenantStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if rpe, ok := err.(*requestPolicyError); ok {
		return rpe.status
	}
	return -1
}

var tenantRoutingConfigs = map[string]func(*Config){
	"default": func(c *Config) {
		c.TenantMap = map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}, "007": {AccountID: "9", ProjectID: "1"}}
	},
	"strict": func(c *Config) {
		c.StrictTenantRouting = true
		c.TenantMap = map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}, "fake": {AccountID: "6", ProjectID: "0"}, "7": {AccountID: "70", ProjectID: "0"}}
	},
	"strict+allow-global": func(c *Config) {
		c.StrictTenantRouting = true
		c.AllowGlobalTenant = true
		c.TenantMap = map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}}
	},
	"label": func(c *Config) {
		c.TenantLabel = "org_id"
		c.TenantMap = map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}}
	},
	"label+strict-flag": func(c *Config) {
		c.TenantLabel = "org_id"
		c.StrictTenantRouting = true
		c.TenantMap = map[string]TenantMapping{"teamA": {AccountID: "42", ProjectID: "0"}}
	},
}

// conformance: tenant-numeric-routing
func TestTenantRouting_Matrix(t *testing.T) {
	const missing = "<missing>"
	ok := func(acct, proj string) tenantExpect { return tenantExpect{http.StatusOK, acct, proj} }
	refused := tenantExpect{status: http.StatusForbidden}
	def := ok("", "")
	labelOK := def
	cases := map[string]map[string]tenantExpect{
		"default": {
			"7": ok("7", "0"), "4294967295": ok("4294967295", "0"), "42": ok("42", "0"), "10": ok("10", "0"),
			"0": def, "fake": def, "default": def, missing: def,
			"teamA": ok("42", "0"), "007": ok("9", "1"), // an exact tenant-map entry wins over the canonical rule
			"unmapped": refused, "*": refused,
			"00": refused, "+7": refused, "-1": refused, "-0": refused, "0042": refused, "07": refused,
			"4294967296": refused, "99999999999999999999": refused, "2024-01-01": refused, "123-456": refused, "+": refused, "-": refused, "1e3": refused, "٣": refused,
			"7|teamA": ok("", ""), "7|0": ok("", ""),
			"7|00": refused, "7|unmapped": refused, "7|*": {status: http.StatusBadRequest},
		},
		"strict": {
			"7": ok("70", "0"), "teamA": ok("42", "0"), "fake": ok("6", "0"), // mapped, including a mapped alias
			"42": refused, "0": refused, "default": refused, missing: refused, "*": refused,
			"007": refused, "unmapped": refused, "4294967295": refused,
			"teamA|fake": ok("", ""), "teamA|7": ok("", ""), "teamA|42": refused, "teamA|0": refused,
		},
		"strict+allow-global": {
			"7": refused, "teamA": ok("42", "0"), "42": refused, "007": refused, "unmapped": refused,
			"0": def, "fake": def, "default": def, missing: def, "*": def,
		},
		"label": {
			"7": labelOK, "007": labelOK, "+7": labelOK, "-1": labelOK, "4294967296": labelOK, "unmapped": labelOK,
			"teamA": ok("42", "0"), "0": refused, "fake": refused, missing: refused, "*": refused,
		},
		"label+strict-flag": {
			"7": labelOK, "007": labelOK, "unmapped": labelOK, "teamA": ok("42", "0"), "0": refused, missing: refused,
		},
	}
	for name, byOrg := range cases {
		for org, want := range byOrg {
			t.Run(name+"/"+org, func(t *testing.T) {
				p := newTestProxyWithOptions(t, tenantRoutingConfigs[name])
				if strings.Contains(org, "|") {
					r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
					r.Header.Set("X-Scope-OrgID", org)
					if got := tenantStatus(p.validateTenantHeader(p.withRequestScope(r))); got != want.status {
						t.Fatalf("status = %d, want %d", got, want.status)
					}
					return
				}
				acct, proj, err := tenantProbe(t, p, org, org == missing)
				if got := tenantStatus(err); got != want.status {
					t.Fatalf("status = %d (%v), want %d", got, err, want.status)
				}
				if want.status == http.StatusOK && (acct != want.account || proj != want.project) {
					t.Fatalf("AccountID/ProjectID = %q/%q, want %q/%q", acct, proj, want.account, want.project)
				}
			})
		}
	}
}

// conformance: tenant-numeric-routing
func TestTenantRouting_RefusalCarriesTheReason(t *testing.T) {
	p := newTestProxyWithOptions(t, tenantRoutingConfigs["default"])
	_, _, err := tenantProbe(t, p, "0042", false)
	msg := err.Error()
	for _, part := range []string{`unknown tenant "0042"`, "canonical", "tenant map"} {
		if !strings.Contains(msg, part) {
			t.Fatalf("message %q lacks %q", msg, part)
		}
	}
	// A name that is not a number keeps the plain Loki-style message.
	_, _, err = tenantProbe(t, p, "unmapped", false)
	if err.Error() != `unknown tenant "unmapped"` {
		t.Fatalf("message = %q", err.Error())
	}
	strict := newTestProxyWithOptions(t, tenantRoutingConfigs["strict"])
	_, _, err = tenantProbe(t, strict, "42", false)
	if !strings.HasPrefix(err.Error(), `unknown tenant "42"`) || !strings.Contains(err.Error(), "tenant.numeric-passthrough") {
		t.Fatalf("strict message = %q", err.Error())
	}
}

// Every org admitted in native mode is either routed to a VictoriaLogs tenant
// (AccountID set) or deliberately the default tenant; none falls through to
// 0:0 by accident, and the refused marker never reaches a backend after
// admission.
//
// conformance: tenant-numeric-routing
func TestTenantRouting_AdmittedOrgIsRoutedOrDeliberatelyDefault(t *testing.T) {
	orgs := []string{
		"", "0", "1", "7", "007", "00", "+7", "-1", "-0", "4294967295", "4294967296", "18446744073709551616",
		"fake", "default", "*", "teamA", "unmapped", "1e3", "0x10", "٣", "7 ", " 7", " 7", "7 ", " ",
		"7\n", "a|b", "7|8", strings.Repeat("9", 400), "team a", "../x",
	}
	defaultTenant := map[string]bool{"": true, "0": true, "fake": true, "default": true, "*": true}
	for name, cfg := range tenantRoutingConfigs {
		p := newTestProxyWithOptions(t, cfg)
		for _, org := range orgs {
			if strings.Contains(org, "|") {
				continue // multi-tenant orgs are resolved part by part below
			}
			acct, _, err := tenantProbe(t, p, org, org == "")
			if err != nil {
				continue
			}
			if acct == tenantRefusedHeader {
				t.Fatalf("%s: org %q admitted but would be refused at the backend", name, org)
			}
			_, mapped := p.tenantMap[org]
			labelMode := p.tenantLabel != ""
			if acct == "" && !defaultTenant[org] && !labelMode && !mapped {
				t.Fatalf("%s: org %q admitted without an AccountID and is not a default-tenant request", name, org)
			}
			if acct == "" && mapped {
				t.Fatalf("%s: mapped org %q carries no AccountID", name, org)
			}
		}
	}
}

// Every multi-tenant query route resolves each part of "7|8|teamA" on its own:
// each backend request carries that part's AccountID, never none (the default
// tenant by accident) and never the refused marker.
//
// conformance: tenant-numeric-routing
func TestTenantRouting_FanoutResolvesEachPart(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]map[string]bool{} // route -> "AccountID/ProjectID" values seen
	current := ""
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if seen[current] == nil {
			seen[current] = map[string]bool{}
		}
		seen[current][r.Header.Get("AccountID")+"/"+r.Header.Get("ProjectID")] = true
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"values": []any{}})
	}))
	defer vl.Close()
	query := url.QueryEscape(`{app="x"}`)
	routes := []string{
		"/loki/api/v1/query?query=" + url.QueryEscape(`count_over_time({app="x"}[5m])`),
		"/loki/api/v1/query_range?query=" + query,
		"/loki/api/v1/series?match[]=" + query,
		"/loki/api/v1/labels",
		"/loki/api/v1/label/app/values",
		"/loki/api/v1/index/stats?query=" + query,
		"/loki/api/v1/index/volume?query=" + query,
		"/loki/api/v1/index/volume_range?query=" + query,
		"/loki/api/v1/detected_fields?query=" + query,
		"/loki/api/v1/detected_field/app/values?query=" + query,
		"/loki/api/v1/detected_labels?query=" + query,
		"/loki/api/v1/patterns?query=" + query,
	}
	for _, route := range routes {
		if !isMultiTenantQueryPath(strings.SplitN(route, "?", 2)[0]) {
			t.Fatalf("%s is not a multi-tenant route", route)
		}
		p := newTestProxyWithOptions(t, tenantRoutingConfigs["default"], func(c *Config) { c.BackendURL = vl.URL })
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		mu.Lock()
		current = route
		mu.Unlock()
		r := httptest.NewRequest(http.MethodGet, route, nil)
		r.Header.Set("X-Scope-OrgID", "7|8|teamA")
		mux.ServeHTTP(httptest.NewRecorder(), r)

		mu.Lock()
		got := seen[route]
		mu.Unlock()
		if len(got) == 0 {
			t.Fatalf("%s: the backend saw no request", route)
		}
		for account := range got {
			if account != "7/0" && account != "8/0" && account != "42/0" {
				t.Errorf("%s: backend request carried AccountID/ProjectID %q, want one of the three parts", route, account)
			}
		}
		if route != routes[0] && len(got) != 3 {
			t.Errorf("%s: backend saw %v, want all three tenants", route, got)
		}
	}
}

func TestIsCanonicalTenantNumber(t *testing.T) {
	for s, want := range map[string]bool{
		"0": true, "7": true, "10": true, "4294967295": true,
		"": false, "00": false, "007": false, "07": false, "+7": false, "-1": false, "-0": false,
		"4294967296": false, " 7": false, "7 ": false, "1e3": false, "0x1": false, "٣": false, "1_0": false,
	} {
		if got := IsCanonicalTenantNumber(s); got != want {
			t.Errorf("IsCanonicalTenantNumber(%q) = %v, want %v", s, got, want)
		}
	}
}

func warnLog(p *Proxy) *lockedBuffer {
	out := &lockedBuffer{}
	p.log = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return out
}

// conformance: tenant-numeric-routing
func TestTenantRouting_RefusalIsLoggedOncePerOrg(t *testing.T) {
	p := newTestProxyWithOptions(t, tenantRoutingConfigs["default"])
	out := warnLog(p)
	for i := 0; i < 5; i++ {
		tenantProbe(t, p, "0042", false)
	}
	lines := out.lines()
	if len(lines) != 1 {
		t.Fatalf("want one warning for five refusals of the same org, got %d: %v", len(lines), lines)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["org_id"] != "0042" {
		t.Fatalf("org_id = %v", rec["org_id"])
	}
	reason, _ := rec["reason"].(string)
	fix, _ := rec["fix"].(string)
	if !strings.Contains(reason, "non-canonical numeric tenant") || !strings.Contains(reason, "Loki treats 007 and 7 as different tenants") ||
		!strings.Contains(reason, "not mapped to VictoriaLogs account 7") {
		t.Fatalf("reason = %q", reason)
	}
	if !strings.Contains(fix, "-tenant-map") || !strings.Contains(fix, "canonical number") {
		t.Fatalf("fix = %q", fix)
	}
	// Another org is its own line.
	tenantProbe(t, p, "+7", false)
	if got := len(out.lines()); got != 2 {
		t.Fatalf("a second org must log once: %d lines", got)
	}
	// A name that is simply unknown, an admitted org and a mapped one stay quiet.
	tenantProbe(t, p, "unmapped", false)
	tenantProbe(t, p, "2024-01-01", false)
	tenantProbe(t, p, "123-456", false)
	tenantProbe(t, p, "*", false)
	tenantProbe(t, p, "0", false)
	tenantProbe(t, p, "7", false)
	tenantProbe(t, p, "teamA", false)
	if got := len(out.lines()); got != 2 {
		t.Fatalf("quiet cases logged: %d lines", got)
	}
	// After the window the same org is reported again.
	p.tenantRefusals.mu.Lock()
	p.tenantRefusals.seen["0042"] = time.Now().Add(-2 * tenantRefusalLogWindow)
	p.tenantRefusals.mu.Unlock()
	tenantProbe(t, p, "0042", false)
	if got := len(out.lines()); got != 3 {
		t.Fatalf("an aged-out org must log again: %d lines", got)
	}
}

// conformance: tenant-numeric-routing
func TestTenantRouting_StrictRefusalIsLoggedAndLabelModeIsQuiet(t *testing.T) {
	p := newTestProxyWithOptions(t, tenantRoutingConfigs["strict"])
	out := warnLog(p)
	tenantProbe(t, p, "42", false)
	tenantProbe(t, p, "42", false)
	lines := out.lines()
	if len(lines) != 1 || !strings.Contains(lines[0], "strict tenant routing") || !strings.Contains(lines[0], "-tenant.numeric-passthrough=true") {
		t.Fatalf("strict refusal log = %v", lines)
	}
	label := newTestProxyWithOptions(t, tenantRoutingConfigs["label"])
	lout := warnLog(label)
	tenantProbe(t, label, "007", false)
	tenantProbe(t, label, "0", false)
	if got := lout.lines(); len(got) != 0 {
		t.Fatalf("label mode logged: %v", got)
	}
}

// conformance: tenant-numeric-routing
func TestTenantRouting_RefusalLogIsBounded(t *testing.T) {
	p := newTestProxyWithOptions(t, tenantRoutingConfigs["default"])
	out := warnLog(p)
	for i := 0; i < 3*tenantRefusalLogMax; i++ {
		tenantProbe(t, p, "-"+strconv.Itoa(i), false)
	}
	if got := len(out.lines()); got != tenantRefusalLogMax {
		t.Fatalf("flood logged %d lines, want the %d cap", got, tenantRefusalLogMax)
	}
	p.tenantRefusals.mu.Lock()
	size := len(p.tenantRefusals.seen)
	p.tenantRefusals.mu.Unlock()
	if size > tenantRefusalLogMax {
		t.Fatalf("dedup table grew to %d", size)
	}
	// While the table is full and nothing can have aged out yet, a new org is
	// skipped without scanning the table.
	p.tenantRefusals.mu.Lock()
	if p.tenantRefusals.fullUntil.IsZero() {
		p.tenantRefusals.mu.Unlock()
		t.Fatal("a full table must remember when its first entry ages out")
	}
	for k := range p.tenantRefusals.seen {
		p.tenantRefusals.seen[k] = time.Now().Add(-2 * tenantRefusalLogWindow)
	}
	p.tenantRefusals.mu.Unlock()
	skipped := len(out.lines())
	tenantProbe(t, p, "-8", false)
	if len(out.lines()) != skipped {
		t.Fatal("before the remembered expiry the full table must not be rescanned")
	}
	// Once that time passes, an aged-out table makes room again.
	p.tenantRefusals.mu.Lock()
	p.tenantRefusals.fullUntil = time.Now().Add(-time.Second)
	p.tenantRefusals.mu.Unlock()
	before := len(out.lines())
	tenantProbe(t, p, "-9", false)
	if len(out.lines()) != before+1 {
		t.Fatal("an aged-out table must accept new orgs")
	}
	// A very long org is truncated in the log.
	long := "0" + strings.Repeat("1", 5000)
	p.tenantRefusals.warn(p.log, long, "r", "f")
	var rec map[string]any
	lines := out.lines()
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &rec); err != nil {
		t.Fatal(err)
	}
	if got := rec["org_id"].(string); len(got) != tenantRefusalLogOrgMax {
		t.Fatalf("logged org length %d, want %d", len(got), tenantRefusalLogOrgMax)
	}
}

// A refused org in the request context never reaches the backend as the default
// tenant: the request carries an AccountID VictoriaLogs rejects.
//
// conformance: tenant-numeric-routing
func TestTenantRouting_RefusedOrgIsSentWithTheRefusedMarker(t *testing.T) {
	for _, tc := range []struct{ config, org string }{
		{"default", "0042"}, {"default", "unmapped"}, {"strict", "42"}, {"label", "\n"},
	} {
		p := newTestProxyWithOptions(t, tenantRoutingConfigs[tc.config])
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		r.Header["X-Scope-Orgid"] = []string{tc.org}
		r = p.withRequestScope(r)
		if p.validateTenantHeader(r) == nil {
			t.Fatalf("%s/%q: expected admission to refuse the org", tc.config, tc.org)
		}
		backend := httptest.NewRequest(http.MethodGet, "http://vl/select/logsql/query", nil).WithContext(r.Context())
		p.forwardTenantHeaders(backend)
		if got := backend.Header.Get("AccountID"); got != tenantRefusedHeader {
			t.Fatalf("%s/%q: AccountID = %q, want %q", tc.config, tc.org, got, tenantRefusedHeader)
		}
	}
}

// An empty map key never serves an absent org: it is the default tenant where
// that is allowed and refused where only an explicit tenant may be served, so
// admission and headers agree.
//
// conformance: tenant-numeric-routing
func TestTenantRouting_EmptyMapKeyDoesNotServeAnAbsentOrg(t *testing.T) {
	withEmpty := func(cfg func(*Config)) func(*Config) {
		return func(c *Config) {
			cfg(c)
			c.TenantMap[""] = TenantMapping{AccountID: "99", ProjectID: "9"}
		}
	}
	for _, tc := range []struct {
		config string
		want   int
	}{
		{"default", http.StatusOK}, {"strict", http.StatusForbidden}, {"label", http.StatusForbidden},
	} {
		p := newTestProxyWithOptions(t, withEmpty(tenantRoutingConfigs[tc.config]))
		acct, proj, err := tenantProbe(t, p, "", true)
		if got := tenantStatus(err); got != tc.want {
			t.Fatalf("%s: status %d, want %d", tc.config, got, tc.want)
		}
		if acct != "" || proj != "" {
			t.Fatalf("%s: an absent org carries AccountID/ProjectID %q/%q, want the default tenant", tc.config, acct, proj)
		}
		route := p.resolveTenant("", p.routingForContext(httptest.NewRequest(http.MethodGet, "/", nil).Context()))
		if tc.want == http.StatusOK && route.kind != tenantDefault {
			t.Fatalf("%s: empty org resolved to kind %d, want the default tenant", tc.config, route.kind)
		}
	}
}

func TestIsNumericTenantLike(t *testing.T) {
	for s, want := range map[string]bool{
		"7": true, "007": true, "+7": true, "-1": true, "-0": true, "4294967296": true, "99999999999999999999": true,
		"": false, "+": false, "-": false, "2024-01-01": false, "123-456": false, "7a": false, "1e3": false, "+-7": false, "٣": false, " 7": false,
	} {
		if got := isNumericTenantLike(s); got != want {
			t.Errorf("isNumericTenantLike(%q) = %v, want %v", s, got, want)
		}
	}
}
