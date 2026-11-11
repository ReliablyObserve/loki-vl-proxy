package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// lokiTenantIDCharsRE is the byte set Loki 3.7.9 accepted in the recorded
// sweep (testdata/tenant_auth_loki_answers.jsonl, labels/char-*): an
// independent statement of it, so the table and the fuzz target below check
// lokiTenantIDError against the recording rather than against itself.
var lokiTenantIDCharsRE = regexp.MustCompile(`^[A-Za-z0-9!'()*\-.:_]*$`)

// Every byte the recording asked about gets Loki's verdict.
//
// conformance: tenant-auth-follows-loki
func TestLokiTenantIDError_MatchesRecordedCharacterSweep(t *testing.T) {
	_, rows := readTenantAuthLokiAnswers(t)
	n := 0
	for _, row := range rows {
		if !strings.HasPrefix(row.ID, "labels/char-") {
			continue
		}
		n++
		var c byte
		for _, ch := range tenantAuthCases() {
			if ch.ID == row.ID {
				c = ch.Header["X-Scope-OrgID"][1]
			}
		}
		id := "x" + string([]byte{c}) + "x"
		got := lokiTenantIDError(id)
		if accepted := row.Status == http.StatusOK; accepted != (got == "") {
			t.Fatalf("%s %q: proxy refusal %q, Loki status %d", row.ID, id, got, row.Status)
		}
		if accepted := row.Status == http.StatusOK; accepted != lokiTenantIDCharsRE.MatchString(id) {
			t.Fatalf("%s %q: the reference set disagrees with Loki's %d", row.ID, id, row.Status)
		}
	}
	if n < 150 {
		t.Fatalf("only %d character rows recorded", n)
	}
}

// FuzzTenantHeader holds the tenant check to Loki's rules on any header: it
// ends quickly with bounded work; an admitted header holds only IDs Loki
// accepts; a refused one names Loki's reason; and an admitted single tenant
// is routed to its own mapping or number, never to another tenant's account.
//
// conformance: tenant-auth-follows-loki, tenant-numeric-routing
func FuzzTenantHeader(f *testing.F) {
	for _, seed := range []string{
		"", " ", "team-a", "team-a|team-b", "team-a|", "|team-a", "team-a|team-a", "|", "||", "a b", ".", "..",
		"..|team-a", "team-a|..", strings.Repeat("a", 150), strings.Repeat("a", 151), "zé", "x\x80x", "*", "team-a|*",
		"7", "007", "+7", "-1", "0", "fake", "x:y", "a'b", "(a)", "team-a | team-b", strings.Repeat("a|", 300),
	} {
		f.Add(seed, "/loki/api/v1/query_range")
		f.Add(seed, "/loki/api/v1/tail")
		f.Add(seed, "/loki/api/v1/delete")
		f.Add(seed, "/prometheus/api/v1/rules")
	}
	tenants := map[string]TenantMapping{"team-a": {AccountID: "1", ProjectID: "0"}, "team-b": {AccountID: "2", ProjectID: "0"}}
	p, err := New(Config{BackendURL: "http://unused", LogLevel: "error", AuthEnabled: true, TenantMap: tenants})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, header, path string) {
		if len(header) > 4096 || !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?# \t\r\n") {
			return // a header is capped by -http-max-header-bytes; the path is the route
		}
		r := httptest.NewRequest(http.MethodGet, "http://proxy/", nil)
		r.URL.Path = path
		r.Header["X-Scope-Orgid"] = []string{header}
		began := time.Now()
		normalizeTenantHeader(r)
		r = p.withRequestScope(r)
		err := p.validateTenantHeader(r)
		if d := time.Since(began); d > 250*time.Millisecond {
			t.Fatalf("tenant check of %d bytes took %s", len(header), d)
		}
		org := r.Header.Get("X-Scope-OrgID")
		if err != nil {
			rpe, ok := err.(*requestPolicyError)
			if !ok || rpe.msg == "" {
				t.Fatalf("refusal %v is no policy error", err)
			}
			if rpe.loki == lokiTenantInvalid && !strings.HasPrefix(rpe.msg, "tenant ID ") {
				t.Fatalf("Loki refusal text %q", rpe.msg)
			}
			return
		}
		if org == "" {
			t.Fatalf("header %q admitted without a tenant while -auth.enabled is on", header)
		}
		for _, id := range strings.Split(org, "|") {
			if !lokiTenantIDCharsRE.MatchString(id) || id == "." || id == ".." || len(id) > lokiTenantIDMaxLength {
				t.Fatalf("header %q admitted with member %q, which Loki refuses", header, id)
			}
		}
		if strings.Contains(org, "|") {
			return
		}
		route := p.resolveTenant(org, p.routingForContext(r.Context()))
		switch route.kind {
		case tenantMapped:
			if route.mapping != tenants[org] {
				t.Fatalf("%q routed to another tenant's mapping %+v", org, route.mapping)
			}
		case tenantNumeric:
			if !IsCanonicalTenantNumber(org) {
				t.Fatalf("%q routed as a number", org)
			}
		case tenantDefault:
			if !isDefaultTenantAlias(org) {
				t.Fatalf("%q routed to the default tenant", org)
			}
		}
	})
}

// The member helpers agree with a plain split and count.
func TestLokiTenantMembers(t *testing.T) {
	for _, tc := range []struct {
		in     string
		want   []string
		single string
	}{
		{"a|b", []string{"a", "b"}, ""},
		{"a|", []string{"a", ""}, ""},
		{"|a", []string{"", "a"}, ""},
		{"a|a", []string{"a"}, "a"},
		{"a|a|a", []string{"a"}, "a"},
		{"|", []string{""}, ""},
		{"a||b|a", []string{"a", "", "b"}, ""},
		{strings.Repeat("x|", 40) + "y", []string{"x", "y"}, ""},
		{strings.Repeat("x|", 40) + "x", []string{"x"}, "x"},
	} {
		got := lokiTenantMembers(tc.in)
		if strings.Join(got, ",") != strings.Join(tc.want, ",") || len(got) != len(tc.want) {
			t.Fatalf("%q: members %q, want %q", tc.in, got, tc.want)
		}
		single, ok := lokiTenantSingleMember(tc.in)
		if ok != (len(tc.want) == 1) || (ok && single != tc.want[0]) {
			t.Fatalf("%q: single %q %v", tc.in, single, ok)
		}
	}
}

// A refused tenant ID is quoted whole in the body, as Loki does, but the log
// line keeps a bounded part of it: a long crafted header cannot flood the log.
//
// conformance: tenant-auth-follows-loki
func TestTenantRefusal_LogIsBounded(t *testing.T) {
	var logs lockedBuffer
	p := newTestProxyWithOptions(t, func(c *Config) { c.AuthEnabled = true })
	p.log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	id := strings.Repeat("a", 64<<10) + " " + strings.Repeat("b", 64<<10)
	for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/query_range?query=%7Ba%3D%22b%22%7D", "/loki/api/v1/tail?query=%7Ba%3D%22b%22%7D"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Scope-OrgID", id)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if !strings.Contains(w.Body.String(), "tenant ID '"+id+"' contains unsupported character ' '") {
			t.Fatalf("%s: body does not quote the ID as Loki does (%d, %d bytes)", path, w.Code, w.Body.Len())
		}
	}
	for _, line := range logs.lines() {
		if len(line) > 4096 {
			t.Fatalf("log line of %d bytes: %.300s", len(line), line)
		}
	}
	if len(logs.lines()) == 0 {
		t.Fatal("the refusals were not logged")
	}
}
