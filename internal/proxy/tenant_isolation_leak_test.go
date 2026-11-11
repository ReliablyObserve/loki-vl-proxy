package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/gorilla/websocket"
)

// tenantLeakRows are two tenants' rows whose every value names its tenant, so
// any of them in the other tenant's answer is a leak.
func tenantLeakRows() map[string][]vlemu.Row {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	rows := func(t string) []vlemu.Row {
		return []vlemu.Row{
			{TS: at, Stream: `{app="` + t + `-secret-app",namespace="` + t + `-secret-ns"}`, Msg: t + "-secret-line level=error", Labels: map[string]string{"level": "error", "user": t + "-secret-user"}},
			{TS: at + 1e9, Stream: `{app="` + t + `-secret-app",namespace="` + t + `-secret-ns"}`, Msg: t + "-secret-line-2", Labels: map[string]string{"user": t + "-secret-user"}},
		}
	}
	b := rows("team-b")
	b = append(b, vlemu.Row{TS: at + 2e9, Stream: `{app="team-b-secret-app",namespace="team-b-secret-ns"}`, Msg: "team-b-secret-line-3", Labels: map[string]string{"user": "team-b-secret-user"}})
	// Different row counts: a count that crosses tenants shows as well.
	return map[string][]vlemu.Row{"1:0": rows("team-a"), "2:0": b}
}

// tenantLeakRoutes are the read routes, each asked over the same window for
// every tenant, with what each tenant's own answer must hold ({t} its name,
// {n} its row count): the proof that the answer is its own data, not empty.
func tenantLeakRoutes() []struct{ name, target, want string } {
	win := "&start=1762000000&end=1762007200"
	q := url.QueryEscape(`{app=~".+"}`)
	return []struct{ name, target, want string }{
		{"query_range logs", "/loki/api/v1/query_range?query=" + q + win + "&limit=100", "{t}-secret-line"},
		{"query_range metric", "/loki/api/v1/query_range?query=" + url.QueryEscape(`sum by (app) (count_over_time({app=~".+"}[5m]))`) + win + "&step=300", `"values":[[1762000000,"{n}"]]`},
		{"query metric", "/loki/api/v1/query?query=" + url.QueryEscape(`sum by (app) (count_over_time({app=~".+"}[1h]))`) + "&time=1762007200", `"{n}"]`},
		{"series", "/loki/api/v1/series?match%5B%5D=" + q + win, "{t}-secret-ns"},
		{"labels", "/loki/api/v1/labels?" + win[1:], `"namespace"`},
		{"label values", "/loki/api/v1/label/app/values?" + win[1:], "{t}-secret-app"},
		{"label values with query", "/loki/api/v1/label/namespace/values?query=" + q + win, "{t}-secret-ns"},
		{"index stats", "/loki/api/v1/index/stats?query=" + q + win, `"entries":{n}}`},
		{"volume", "/loki/api/v1/index/volume?query=" + q + win, `"{t}-secret-app"},"value":[1762007200,"{n}"]`},
		{"volume_range", "/loki/api/v1/index/volume_range?query=" + q + win + "&step=300", "{t}-secret-app"},
		{"detected_fields", "/loki/api/v1/detected_fields?query=" + q + win, `"label":"user"`},
		{"detected_field values", "/loki/api/v1/detected_field/user/values?query=" + q + win, "{t}-secret-user"},
		{"detected_labels", "/loki/api/v1/detected_labels?query=" + q + win, `"label":"namespace"`},
		{"patterns", "/loki/api/v1/patterns?query=" + q + win + "&step=300", `"status":"success"`},
	}
}

// tenantLeakFleet is two proxy replicas over one tenant-scoped VictoriaLogs,
// each with a memory cache, a disk cache and the peer tier to the other.
type tenantLeakFleet struct {
	emu      *vlemu.Tenants
	replicas []*httptest.Server
	peerHits atomic.Int64 // authenticated /_cache/* requests between the replicas
}

func newTenantLeakFleet(t *testing.T) *tenantLeakFleet {
	t.Helper()
	emu := &vlemu.Tenants{Rows: tenantLeakRows()}
	vl := httptest.NewServer(emu.Handler())
	t.Cleanup(vl.Close)
	f := &tenantLeakFleet{emu: emu}
	handlers := make([]http.Handler, 2)
	for i := range handlers {
		i := i
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/_cache/") && r.Header.Get("X-Peer-Token") != "" {
				f.peerHits.Add(1)
			}
			handlers[i].ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		f.replicas = append(f.replicas, srv)
	}
	hosts := []string{}
	for _, srv := range f.replicas {
		u, _ := url.Parse(srv.URL)
		hosts = append(hosts, u.Host)
	}
	const peerToken = "fleet-peer-token"
	for i, host := range hosts {
		dc, err := cache.NewDiskCache(cache.DiskCacheConfig{Path: t.TempDir() + "/cache.db", MinTTL: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dc.Close() })
		c := cache.New(5*time.Minute, 10000)
		c.SetL2(dc)
		pc := cache.NewPeerCache(cache.PeerConfig{
			SelfAddr: host, DiscoveryType: "static", StaticPeers: strings.Join(hosts, ","),
			Timeout: 2 * time.Second, AuthToken: peerToken, WriteThrough: true, WriteThroughMinTTL: time.Second,
		})
		t.Cleanup(pc.Close)
		c.SetL3(pc)
		t.Cleanup(c.Close)
		p, err := New(Config{
			BackendURL: vl.URL, Cache: c, CompatCache: cache.New(5*time.Minute, 10000), LogLevel: "error",
			PeerCache: pc, PeerAuthToken: peerToken,
			AuthEnabled: true,
			TenantMap:   map[string]TenantMapping{"team-a": {AccountID: "1", ProjectID: "0"}, "team-b": {AccountID: "2", ProjectID: "0"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		handlers[i] = mux
	}
	return f
}

func (f *tenantLeakFleet) get(t *testing.T, replica int, target, org string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.replicas[replica].URL+target, nil)
	req.Header.Set("X-Scope-OrgID", org)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// No tenant ever sees another tenant's data on any read route, from
// VictoriaLogs or from any cache tier: team-a's answers fill both replicas'
// memory, disk and peer caches first, then team-b asks the same requests on
// both replicas and must get its own rows, read from VictoriaLogs under its
// own account, never team-a's cached ones; and the reverse.
//
// conformance: tenant-auth-follows-loki, tenant-numeric-routing
func TestTenantIsolation_NoLeakOnAnyReadRouteOrCacheTier(t *testing.T) {
	f := newTenantLeakFleet(t)
	tenants := []struct{ org, account, rows string }{{"team-a", "1:0", "2"}, {"team-b", "2:0", "3"}}
	for _, route := range tenantLeakRoutes() {
		t.Run(route.name, func(t *testing.T) {
			for _, tn := range tenants {
				before := f.emu.Calls(tn.account)
				for _, replica := range []int{0, 1, 0, 1} { // miss, then each tier warm
					status, body := f.get(t, replica, route.target, tn.org)
					if status != http.StatusOK {
						t.Fatalf("%s on replica %d: %d %s", tn.org, replica, status, body)
					}
					if leaked := f.emu.LeakedMarkers(tn.account, body); len(leaked) > 0 {
						t.Fatalf("%s on replica %d sees another tenant's data %v: %s", tn.org, replica, leaked, body)
					}
					if want := strings.NewReplacer("{t}", tn.org, "{n}", tn.rows).Replace(route.want); !strings.Contains(body, want) {
						t.Fatalf("%s on replica %d: answer lacks its own %s: %s", tn.org, replica, want, body)
					}
				}
				if f.emu.Calls(tn.account) == before {
					t.Fatalf("%s was answered without a VictoriaLogs call under its own account: served from another tenant's cache entry", tn.org)
				}
			}
		})
	}
	if f.peerHits.Load() == 0 {
		t.Fatal("the replicas never used the peer tier: the run proves nothing about it")
	}
	t.Run("tail", func(t *testing.T) {
		for _, tn := range tenants {
			ws := "ws" + strings.TrimPrefix(f.replicas[1].URL, "http") + "/loki/api/v1/tail?query=" + url.QueryEscape(`{app=~".+"}`)
			conn, _, err := websocket.DefaultDialer.Dial(ws, http.Header{"X-Scope-OrgID": {tn.org}})
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, msg, err := conn.ReadMessage()
			_ = conn.Close()
			if err != nil {
				t.Fatalf("%s: %v", tn.org, err)
			}
			if leaked := f.emu.LeakedMarkers(tn.account, string(msg)); len(leaked) > 0 {
				t.Fatalf("%s tail sees another tenant's data %v: %s", tn.org, leaked, msg)
			}
			if !strings.Contains(string(msg), tn.org+"-secret-line") {
				t.Fatalf("%s tail did not stream its own rows: %s", tn.org, msg)
			}
		}
	})
}
