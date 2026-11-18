//go:build e2e

package e2e_fleet

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFleet_VLRefusalHeldAcrossReplicas: a query VictoriaLogs refuses before reading data (its LogsQL is longer than
// VictoriaLogs' -search.maxQueryLen, 16384 bytes by default, while the LogQL is within the proxy's own limit) is
// asked of VictoriaLogs once by the replica that gets it; the refusal is held (-error-cache-vl-refusal-ttl) and
// pushed to every peer of the same build, VictoriaLogs release and configuration, so the same request on the two
// other replicas answers Loki's error byte for byte without a VictoriaLogs call.
//
// conformance: semantics/error-cache-vl-refusal-peer-sync, vl-query-too-long
func TestFleet_VLRefusalHeldAcrossReplicas(t *testing.T) {
	app := fmt.Sprintf("vl-refusal-%d", time.Now().UnixNano())
	query := `{app="` + app + `"} |= "` + strings.Repeat("x", 17000) + `"`
	// A settled window (the response cache keys it exactly; a window near now bypasses it), the same on every replica.
	end := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)},
		"end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"10"}}
	get := func(addr string) (int, string) {
		t.Helper()
		resp, err := fleetHTTP.Get(addr + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	queryCalls := func(addr string) float64 { return vlRouteCounts(scrapeMetrics(t, addr))["/select/logsql/query"] }
	received := func(addr string) float64 {
		return scrapeMetrics(t, addr).sum("loki_vl_proxy_error_cache_peer_sync_total", map[string]string{"outcome": "received"})
	}

	before := make([]float64, len(proxyAddrs))
	receivedBefore := make([]float64, len(proxyAddrs))
	for i, addr := range proxyAddrs {
		before[i], receivedBefore[i] = queryCalls(addr), received(addr)
	}
	status, body := get(proxyAddrs[0])
	if status != http.StatusInternalServerError {
		t.Fatalf("proxy-a answered %d %q, want Loki's 500 for a refused plan", status, body)
	}
	if calls := queryCalls(proxyAddrs[0]) - before[0]; calls < 1 {
		t.Fatalf("proxy-a sent %v VictoriaLogs calls, want the refused one", calls)
	}
	deadline := time.Now().Add(15 * time.Second)
	for received(proxyAddrs[1]) <= receivedBefore[1] || received(proxyAddrs[2]) <= receivedBefore[2] {
		if time.Now().After(deadline) {
			t.Fatal("the refusal did not reach proxy-b and proxy-c")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 1; i < len(proxyAddrs); i++ {
		gotStatus, gotBody := get(proxyAddrs[i])
		if gotStatus != status || gotBody != body {
			t.Fatalf("%s answered %d %q, proxy-a %d %q", fleetServices[i], gotStatus, gotBody, status, body)
		}
		if calls := queryCalls(proxyAddrs[i]) - before[i]; calls != 0 {
			t.Fatalf("%s sent %v VictoriaLogs query calls for a refusal a peer holds", fleetServices[i], calls)
		}
	}
}
