package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

func snRouteOracleProxy(t *testing.T, backend string, route bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
		BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
		ServiceNameStreamRoute: route, ServiceNameStreamRouteViolationTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// TestServiceNameRouteAnswersOnVictoriaLogs runs the proxy over VictoriaLogs v1.49.0 and v1.54.0 (their own
// storage and query handlers, test/oracle/vl serve) with and without the automatic service_name route
// (-service-name-stream-route, every window narrowed) and holds every answer with it to the answer without it:
// on the tenant whose rows every narrowing keeps, on a multi-tenant query, and on the tenant whose rows break it
// (a regular service_name in another service's stream, a regular service.name beside a service.name stream label
// of another value), where the proof finds the rows and the reads are sent again without the stream filter; for
// every value a stream can derive and every shape of snOracleRequests, and for two identical requests at once on
// the violating tenant. The clean tenants' log reads carry the stream filter.
//
// conformance: semantics/service-name-stream-route, vl-stream-filter-or-groups-and-missing-label
func TestServiceNameRouteAnswersOnVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	t.Cleanup(querycorpus.PauseRecording())
	streams := snOracleStreams()
	sets := []oraclerun.ServeDataSet{
		{Tenant: 0, Rows: snServeRows(streams)},
		{Tenant: 1, Rows: snServeRows(snViolatingStreams())},
		{Tenant: 2, Rows: snServeRows(streams[:9])},
	}
	values := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-x", "none", unknownServiceName, `we"ird\x`, "ünï code", "line\nbreak"}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, sets)
		if err != nil {
			t.Fatal(err)
		}
		offFront, onFront := &snFront{version: v.version, upstream: srv.URL}, &snFront{version: v.version, upstream: srv.URL}
		offSrv, onSrv := httptest.NewServer(offFront), httptest.NewServer(onFront)
		off, on := snRouteOracleProxy(t, offSrv.URL, false), snRouteOracleProxy(t, onSrv.URL, true)
		on.serviceNameRouteMinWindow = 0
		same, nonEmpty, total := 0, 0, 0
		compare := func(tenant, target string) {
			total++
			headers := map[string]string{"X-Scope-OrgID": tenant}
			a, b := doCompatProxyRequest(off, target, headers), doCompatProxyRequest(on, target, headers)
			if a.Code != b.Code || snCanonical(a.Body.Bytes()) != snCanonical(b.Body.Bytes()) {
				t.Errorf("%s tenant %s %s:\n route on  %d %.400s\n route off %d %.400s", v.version, tenant, target, b.Code, b.Body.String(), a.Code, a.Body.String())
				return
			}
			if a.Code != http.StatusOK {
				t.Errorf("%s tenant %s %s: %d %s", v.version, tenant, target, a.Code, a.Body.String())
				return
			}
			same++
			if body := a.Body.String(); len(body) > 0 && containsAny(body, []string{`"values":[[`, `"value":[`, `"label":`}) {
				nonEmpty++
			}
		}
		for _, tenant := range []string{"0", "0|2"} {
			for _, value := range values {
				for _, target := range snOracleRequests(value) {
					compare(tenant, target)
				}
			}
		}
		narrowedClean := onFront.narrowedReads()
		if onFront.proofRowsFound() != 0 {
			t.Errorf("%s: a proof found a row on the clean tenants", v.version)
		}
		// The violating tenant: the first ask of each value proves and falls back, later asks remember it.
		for _, value := range values {
			for _, target := range snOracleRequests(value) {
				compare("1", target)
			}
		}
		if onFront.proofRowsFound() == 0 {
			t.Errorf("%s: no proof found a row on the violating tenant: the check does not reach the fallback", v.version)
		}
		// Two identical requests at once on the violating tenant (a fresh proxy: nothing remembered yet).
		fresh := snRouteOracleProxy(t, onSrv.URL, true)
		fresh.serviceNameRouteMinWindow = 0
		target := snOracleRequests("svc-a")[0]
		want := snCanonical(doCompatProxyRequest(off, target, map[string]string{"X-Scope-OrgID": "1"}).Body.Bytes())
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := doCompatProxyRequest(fresh, target, map[string]string{"X-Scope-OrgID": "1"})
				if got := snCanonical(res.Body.Bytes()); res.Code != http.StatusOK || got != want {
					t.Errorf("%s concurrent violating request: %d %.400s, want %.400s", v.version, res.Code, got, want)
				}
			}()
		}
		wg.Wait()
		if narrowedClean == 0 || offFront.narrowedReads() != 0 {
			t.Errorf("%s: %d narrowed reads with the route on the clean tenants, %d without the route", v.version, narrowedClean, offFront.narrowedReads())
		}
		if nonEmpty < total/3 {
			t.Errorf("%s: only %d of %d answers hold data", v.version, nonEmpty, total)
		}
		t.Logf("VictoriaLogs %s: %d of %d answers equal with and without the route (%d hold data), %d narrowed reads on the clean tenants, %d proofs found rows on the violating tenant",
			v.version, same, total, nonEmpty, narrowedClean, onFront.proofRowsFound())
		offSrv.Close()
		onSrv.Close()
		srv.Stop()
	}
}

func (f *snFront) proofRowsFound() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proofRows
}

func (f *snFront) narrowedReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.narrowed
}
