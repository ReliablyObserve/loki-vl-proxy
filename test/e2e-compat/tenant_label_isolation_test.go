//go:build e2e

package e2e_compat

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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// tenantLabelIngest writes one row whose stream fields are exactly streamFields.
func tenantLabelIngest(t *testing.T, row map[string]string, streamFields, account string) {
	t.Helper()
	data, _ := json.Marshal(row)
	target := vlURL + "/insert/jsonline?_stream_fields=" + url.QueryEscape(streamFields) + "&_msg_field=_msg&_time_field=_time"
	status, body := hardeningRequest(t, "POST", target, string(data)+"\n", map[string]string{"AccountID": account, "ProjectID": "0", "Content-Type": "application/stream+json"})
	if status < 200 || status >= 300 {
		t.Fatalf("VL ingest %d: %s", status, body)
	}
}

// vlLogsqlQuery runs a LogsQL query on the default tenant with the given
// extra_stream_filters ("" sends none) and returns the status and the _msg values.
func vlLogsqlQuery(t *testing.T, query, extraStreamFilters string) (int, []string) {
	t.Helper()
	form := url.Values{"query": {query}, "limit": {"100"}}
	if extraStreamFilters != "" {
		form.Set("extra_stream_filters", extraStreamFilters)
	}
	status, body := hardeningRequest(t, "POST", vlURL+"/select/logsql/query", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if json.Unmarshal([]byte(line), &row) == nil {
			if msg, ok := row["_msg"].(string); ok {
				msgs = append(msgs, msg)
			}
		}
	}
	return status, msgs
}

// The tenant-label mode relies on what VictoriaLogs does with extra_stream_filters:
// an exact match on the stream field, an empty value that matches streams lacking
// the field (so "" is never used to confine a request), and a value no org ID can
// carry (a newline) that matches nothing without an error. A VictoriaLogs change
// to any of these must fail here, not in production.
//
// conformance: vl-extra-stream-filters-exact-and-empty, tenant-label-isolation
func TestVLCapability_ExtraStreamFiltersTenantSemantics(t *testing.T) {
	unique := fmt.Sprintf("tlcap%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	tenantLabelIngest(t, map[string]string{"_time": stamp, "_msg": unique + "-tagged", "app": unique, "org_id": unique}, "org_id,app", "0")
	tenantLabelIngest(t, map[string]string{"_time": stamp, "_msg": unique + "-untagged", "app": unique}, "app", "0")
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}
	query := `app:="` + unique + `"`

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, msgs := vlLogsqlQuery(t, query, ""); len(msgs) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ingested rows did not become searchable")
		}
		time.Sleep(500 * time.Millisecond)
	}

	marshal := func(value string) string {
		out, _ := json.Marshal(map[string]string{"org_id": value})
		return string(out)
	}
	for _, tc := range []struct {
		name, filter string
		want         []string
	}{
		{"no filter sees both", "", []string{unique + "-tagged", unique + "-untagged"}},
		// The empty value matches a stream that lacks the field: it must never be
		// used to confine a request to "no tenant".
		{"empty value matches the untagged stream", marshal(""), []string{unique + "-untagged"}},
		// proxy tenantFilterNeverMatches: no org ID can carry a newline.
		{"newline matches nothing", marshal("\n"), nil},
		{"exact value matches only that tenant", marshal(unique), []string{unique + "-tagged"}},
	} {
		status, msgs := vlLogsqlQuery(t, query, tc.filter)
		if status != 200 {
			t.Fatalf("%s: status %d", tc.name, status)
		}
		if strings.Join(sortedCopy(msgs), ",") != strings.Join(sortedCopy(tc.want), ",") {
			t.Fatalf("%s: got %v, want %v", tc.name, msgs, tc.want)
		}
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// labelModeServer is an in-process proxy in tenant-label mode against the live
// VictoriaLogs (and, when set, vmalert).
func labelModeServer(t *testing.T, mutate func(*proxy.Config)) *httptest.Server {
	t.Helper()
	cfg := proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error", TenantLabel: "org_id"}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := proxy.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func labelModeGet(t *testing.T, server *httptest.Server, org, path string) (int, string) {
	t.Helper()
	headers := map[string]string{}
	if org != "" {
		headers["X-Scope-OrgID"] = org
	}
	status, body := hardeningRequest(t, "GET", server.URL+path, "", headers)
	return status, string(body)
}

// Tenant-label mode against live data: the default aliases and an empty org are
// refused on every read route unless allow-global is set or the alias is mapped;
// a real org sees only its own streams.
//
// conformance: tenant-label-isolation, vl-extra-stream-filters-exact-and-empty, semantics/tenant-label-default-aliases
func TestHardeningLive_TenantLabelDefaultAliases(t *testing.T) {
	n := time.Now().UnixNano()
	service := fmt.Sprintf("tlalias-%d", n)
	orgA, orgB, orgC := fmt.Sprintf("tla-%d", n), fmt.Sprintf("tlb-%d", n), fmt.Sprintf("tlc-%d", n)
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	hardeningIngest(t, service, orgA, "0", "marker-A-"+service, stamp)
	hardeningIngest(t, service, orgB, "0", "marker-B-"+service, stamp)
	hardeningIngest(t, service, "", "0", "marker-untagged-"+service, stamp)
	const mappedAccount = "540010"
	hardeningIngest(t, service, orgA, mappedAccount, "marker-mapped-"+service, stamp)
	if status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil); status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}

	window := url.Values{"start": {strconv.FormatInt(stamp.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(stamp.Add(time.Minute).UnixNano(), 10)}}
	selector := `{service_name="` + service + `"}`
	withQuery := func(path string, extra url.Values) string {
		q := url.Values{}
		for k, v := range window {
			q[k] = v
		}
		for k, v := range extra {
			q[k] = v
		}
		return path + "?" + q.Encode()
	}
	routes := map[string]string{
		"query_range":  withQuery("/loki/api/v1/query_range", url.Values{"query": {selector}, "direction": {"forward"}, "limit": {"100"}}),
		"labels":       withQuery("/loki/api/v1/labels", nil),
		"label_values": withQuery("/loki/api/v1/label/service_name/values", url.Values{"query": {selector}}),
		"series":       withQuery("/loki/api/v1/series", url.Values{"match[]": {selector}}),
		"volume":       withQuery("/loki/api/v1/index/volume", url.Values{"query": {selector}}),
	}
	eventually := func(name string, check func() error) {
		t.Helper()
		var err error
		for deadline := time.Now().Add(40 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
			if err = check(); err == nil {
				return
			}
		}
		t.Fatalf("%s: %v", name, err)
	}

	plain := labelModeServer(t, nil)
	for _, org := range []string{"", "0", "fake", "default", "*"} {
		for name, path := range routes {
			if status, body := labelModeGet(t, plain, org, path); status != http.StatusForbidden {
				t.Fatalf("org %q %s: status %d, want 403: %s", org, name, status, body)
			}
		}
	}

	// A real org sees only its own streams, on every route.
	eventually("tenant A confined", func() error {
		for name, path := range routes {
			status, body := labelModeGet(t, plain, orgA, path)
			if status != http.StatusOK {
				return fmt.Errorf("%s: status %d: %s", name, status, body)
			}
			// The label names answer does not carry the service value itself.
			if name != "labels" && !strings.Contains(body, service) {
				return fmt.Errorf("%s: tenant A's data missing: %s", name, body)
			}
			for _, foreign := range []string{"marker-B-", "marker-untagged-", "marker-mapped-", orgB} {
				if strings.Contains(body, foreign) {
					return fmt.Errorf("%s: %q leaked to tenant A: %s", name, foreign, body)
				}
			}
		}
		_, body := labelModeGet(t, plain, orgA, routes["query_range"])
		if !strings.Contains(body, "marker-A-"+service) {
			return fmt.Errorf("query_range: tenant A's line missing: %s", body)
		}
		return nil
	})
	for name, path := range routes {
		status, body := labelModeGet(t, plain, orgC, path)
		if status != http.StatusOK || strings.Contains(body, service) {
			t.Fatalf("tenant without data, %s: %d %s", name, status, body)
		}
	}

	// allow-global is the explicit global view: the aliases and "*" see every
	// tenant's lines, including the stream without the tenant label.
	global := labelModeServer(t, func(c *proxy.Config) { c.AllowGlobalTenant = true })
	for _, org := range []string{"", "0", "fake", "default", "*"} {
		eventually("global view "+org, func() error {
			status, body := labelModeGet(t, global, org, routes["query_range"])
			if status != http.StatusOK {
				return fmt.Errorf("status %d: %s", status, body)
			}
			for _, want := range []string{"marker-A-", "marker-B-", "marker-untagged-"} {
				if !strings.Contains(body, want) {
					return fmt.Errorf("%q missing: %s", want, body)
				}
			}
			return nil
		})
	}

	// A mapped alias gets its mapping (account) and no tenant filter; the other
	// aliases stay refused.
	mapped := labelModeServer(t, func(c *proxy.Config) {
		c.TenantMap = map[string]proxy.TenantMapping{"fake": {AccountID: mappedAccount, ProjectID: "0"}}
	})
	eventually("mapped alias", func() error {
		status, body := labelModeGet(t, mapped, "fake", routes["query_range"])
		if status != http.StatusOK || !strings.Contains(body, "marker-mapped-"+service) {
			return fmt.Errorf("status %d: %s", status, body)
		}
		for _, foreign := range []string{"marker-A-", "marker-B-", "marker-untagged-"} {
			if strings.Contains(body, foreign) {
				return fmt.Errorf("%q from the default account in the mapped view: %s", foreign, body)
			}
		}
		return nil
	})
	for _, org := range []string{"", "0", "default"} {
		if status, _ := labelModeGet(t, mapped, org, routes["query_range"]); status != http.StatusForbidden {
			t.Fatalf("unmapped alias %q with another alias mapped: status %d", org, status)
		}
	}
}

// Rules and alerts come from a ruler that knows nothing of the tenant label. The
// stack's vmalert rules carry no org_id label, so a real org sees none of them and
// only allow-global shows them.
//
// conformance: tenant-label-isolation, prometheus_api_v1_rules, prometheus_api_v1_alerts
func TestHardeningLive_TenantLabelRulesAndAlertsConfinement(t *testing.T) {
	direct := &http.Client{Timeout: 5 * time.Second}
	resp, err := direct.Get(vmalertURL + "/api/v1/rules?datasource_type=vlogs")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Skipf("vmalert not reachable at %s (err=%v): rules/alerts confinement not checked", vmalertURL, err)
	}
	_ = resp.Body.Close()
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	if groups := extractArray(extractMap(getJSON(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs"), "data"), "groups"); len(groups) == 0 {
		t.Skip("vmalert has no rule groups loaded: nothing to confine")
	}

	confined := labelModeServer(t, func(c *proxy.Config) { c.RulerBackendURL = vmalertURL; c.AlertsBackendURL = vmalertURL })
	global := labelModeServer(t, func(c *proxy.Config) {
		c.RulerBackendURL, c.AlertsBackendURL, c.AllowGlobalTenant = vmalertURL, vmalertURL, true
	})
	for _, path := range []string{"/prometheus/api/v1/rules?datasource_type=vlogs", "/loki/api/v1/rules", "/api/prom/rules", "/loki/api/v1/alerts?datasource_type=vlogs", "/prometheus/api/v1/alerts?datasource_type=vlogs"} {
		status, body := labelModeGet(t, confined, "tenant-without-rules", path)
		if status != http.StatusOK || strings.Contains(body, "loki-vl-e2e-alerts") || strings.Contains(body, "ApiGatewayErrors") {
			t.Fatalf("confined %s: %d %s", path, status, body)
		}
		if status, _ := labelModeGet(t, confined, "fake", path); status != http.StatusForbidden {
			t.Fatalf("alias without allow-global %s: %d", path, status)
		}
	}
	status, body := labelModeGet(t, global, "*", "/prometheus/api/v1/rules?datasource_type=vlogs")
	if status != http.StatusOK || !strings.Contains(body, "loki-vl-e2e-alerts") {
		t.Fatalf("global view: %d %s", status, body)
	}
}
