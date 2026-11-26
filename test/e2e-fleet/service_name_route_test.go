//go:build e2e

package e2e_fleet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestFleet_ServiceNameRouteViolationReachesPeers: with the automatic service_name route enabled on every replica
// (-service-name-stream-route), a row the narrowing would leave out (a regular service_name field in a stream of
// another service.name) that one replica's proof finds is pushed to every peer, so the other replicas' first log
// read of that tenant's service goes to VictoriaLogs once, unnarrowed, with no proof read; every replica answers
// the same lines, which are the rows the derived service_name filter keeps.
//
// conformance: semantics/service-name-stream-route
func TestFleet_ServiceNameRouteViolationReachesPeers(t *testing.T) {
	svc := fmt.Sprintf("sn-route-%d", time.Now().UnixNano())
	// One whole hour: the read is one window of the proxy's 1h split, so it is narrowed (-service-name-stream-route-min-window
	// is 30m) and the rows of both streams spread over it.
	end := time.Now().Add(-5 * time.Minute).Truncate(time.Hour)
	start := end.Add(-time.Hour)
	var rows strings.Builder
	for i := 0; i < 20; i++ {
		for _, r := range []map[string]string{
			{"service.name": svc, "pod": svc + "-0", "_msg": fmt.Sprintf("own %d", i)},
			{"service.name": svc + "-other", "pod": svc + "-1", "service_name": svc, "_msg": fmt.Sprintf("stored %d", i)},
		} {
			r["_time"] = start.Add(time.Duration(3*i+1) * time.Minute).Format(time.RFC3339Nano)
			line, _ := json.Marshal(r)
			rows.Write(line)
			rows.WriteByte('\n')
		}
	}
	resp, err := http.Post(fleetVLURL+"/insert/jsonline?_stream_fields=service.name,pod", "application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("ingest: %d", resp.StatusCode)
	}
	if resp, err := http.Post(fleetVLURL+"/internal/force_flush", "text/plain", nil); err == nil {
		resp.Body.Close()
	}
	// lines answers `{service_name="svc"}` over the window moved back by shift, so each ask is a new window (a shift
	// moves the window off the hour: proxy-b's two split reads both come from the remembered violation).
	lines := func(addr string, shift time.Duration) int {
		t.Helper()
		params := url.Values{"query": {`{service_name="` + svc + `"}`}, "start": {strconv.FormatInt(start.Add(-shift).UnixNano(), 10)},
			"end": {strconv.FormatInt(end.Add(-shift).UnixNano(), 10)}, "limit": {"1000"}}
		resp, err := fleetHTTP.Get(addr + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var out struct {
			Data struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil {
			t.Fatalf("%s: %d %.300s", addr, resp.StatusCode, body)
		}
		n := 0
		for _, r := range out.Data.Result {
			n += len(r.Values)
		}
		return n
	}
	outcome := func(addr, name string) float64 {
		return scrapeMetrics(t, addr).sum("loki_vl_proxy_internal_operation_total", map[string]string{"operation": "service_name_stream_route", "outcome": name})
	}
	queryCalls := func(addr string) float64 { return vlRouteCounts(scrapeMetrics(t, addr))["/select/logsql/query"] }

	// VictoriaLogs holds every row (asked directly: a replica asking would find the violation itself).
	vlCount := func() string {
		form := url.Values{"query": {`"service.name":` + strconv.Quote(svc) + ` OR service_name:=` + strconv.Quote(svc) + ` | stats count() n`},
			"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
		resp, err := http.PostForm(fleetVLURL+"/select/logsql/query", form)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		var row struct {
			N string `json:"n"`
		}
		body, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(body, &row)
		return row.N
	}
	deadline := time.Now().Add(60 * time.Second)
	for vlCount() != "40" {
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaLogs does not hold the 40 rows of %s", svc)
		}
		time.Sleep(500 * time.Millisecond)
	}
	received := make([]float64, len(proxyAddrs))
	for i, addr := range proxyAddrs {
		received[i] = outcome(addr, "peer_violation_received")
	}
	violations := outcome(proxyAddrs[0], "violation")
	if got := lines(proxyAddrs[0], 0); got != 40 {
		t.Fatalf("proxy-a answered %d lines, want the 40 rows the derived filter keeps", got)
	}
	if outcome(proxyAddrs[0], "violation") <= violations {
		t.Fatal("proxy-a's proof found no violation")
	}
	deadline = time.Now().Add(15 * time.Second)
	for outcome(proxyAddrs[1], "peer_violation_received") <= received[1] {
		if time.Now().After(deadline) {
			t.Fatal("the violation did not reach proxy-b")
		}
		time.Sleep(100 * time.Millisecond)
	}
	before := queryCalls(proxyAddrs[1])
	remembered, narrowed, proofs := outcome(proxyAddrs[1], "remembered_violation"), outcome(proxyAddrs[1], "narrowed"), outcome(proxyAddrs[1], "violation")
	if got := lines(proxyAddrs[1], time.Minute); got != 40 {
		t.Fatalf("proxy-b answered %d lines, want 40", got)
	}
	if outcome(proxyAddrs[1], "remembered_violation") <= remembered {
		t.Fatal("proxy-b did not read the violation proxy-a pushed")
	}
	if outcome(proxyAddrs[1], "narrowed") != narrowed || outcome(proxyAddrs[1], "violation") != proofs {
		t.Fatal("proxy-b narrowed or proved a read of a service a peer found a violation of")
	}
	// The window moved off the hour is two reads of the 1h split, both without the stream filter and no proof.
	if calls := queryCalls(proxyAddrs[1]) - before; calls != 2 {
		t.Fatalf("proxy-b sent %v VictoriaLogs query calls, want the 2 split reads (no narrowed read, no proof)", calls)
	}
}
