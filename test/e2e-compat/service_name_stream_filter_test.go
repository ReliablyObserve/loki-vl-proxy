//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// snProxy is an in-process proxy over the stack's VictoriaLogs with -service-name-stream-label-only on or off.
func snProxy(t *testing.T, on bool) string {
	t.Helper()
	p, err := proxy.New(proxy.Config{
		BackendURL:                 vlURL,
		Cache:                      cache.NewDisabled(),
		LogLevel:                   "error",
		LabelStyle:                 proxy.LabelStyleUnderscores,
		MetadataFieldMode:          proxy.MetadataFieldModeTranslated,
		DisableLabelsCacheWarm:     true,
		ServiceNameStreamLabelOnly: on,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

// snRouteProxy is a proxy with the automatic service_name route enabled for every read window
// (-service-name-stream-route, -service-name-stream-route-min-window=0) and its /metrics, as an operator opts in.
func snRouteProxy(t *testing.T) string {
	t.Helper()
	instrumentation := true
	p, err := proxy.New(proxy.Config{
		BackendURL:                         vlURL,
		Cache:                              cache.NewDisabled(),
		LogLevel:                           "error",
		LabelStyle:                         proxy.LabelStyleUnderscores,
		MetadataFieldMode:                  proxy.MetadataFieldModeTranslated,
		DisableLabelsCacheWarm:             true,
		ServiceNameStreamRoute:             true,
		ServiceNameStreamRouteViolationTTL: time.Hour,
		RegisterInstrumentation:            &instrumentation,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

// snRouteOutcomes is the proxy's count of each service_name route outcome, from its /metrics.
func snRouteOutcomes(t *testing.T, base string) map[string]float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := map[string]float64{}
	const prefix = `loki_vl_proxy_internal_operation_total{operation="service_name_stream_route",outcome="`
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			if outcome, value, ok := strings.Cut(rest, `"} `); ok {
				out[outcome], _ = strconv.ParseFloat(value, 64)
			}
		}
	}
	return out
}

// snLines is a query_range log answer as a sorted list of timestamp and line, and the line count.
func snLines(t *testing.T, base string, query string, start, end time.Time) []string {
	t.Helper()
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", url.Values{
		"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)},
		"limit": {"1000"}, "direction": {"backward"},
	}, "0", nil)
	var resp struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Values [][]string
			}
		}
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil || resp.Data.ResultType != "streams" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	var out []string
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			out = append(out, v[0]+" "+v[1])
		}
	}
	sort.Strings(out)
	return out
}

// snCount is the total of an instant count over the window.
func snCount(t *testing.T, base string, query string, at time.Time) string {
	t.Helper()
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}, "0", nil)
	var resp struct {
		Data struct {
			Result []struct {
				Value []json.RawMessage
			}
		}
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Value) != 2 {
		return fmt.Sprintf("%d series", len(resp.Data.Result))
	}
	var v string
	_ = json.Unmarshal(resp.Data.Result[0].Value[1], &v)
	return v
}

// TestCompat_ServiceNameStreamFilterLikeLoki pushes one service in each layout the stream filter of
// -service-name-stream-label-only reads (OpenTelemetry service.name as a VictoriaLogs stream field, Loki's
// service_name label on the Loki push route, a service named only by app, which Loki discovers) to Loki and
// VictoriaLogs, and holds the proxy with the flag to Loki (the lines of the log query, the counts) and to the proxy
// without it (every answer, byte for byte after sorting): the flag changes the VictoriaLogs reads only. The proxy
// with the automatic route enabled (-service-name-stream-route, every window) answers the same lines as Loki and the
// same parser chain as without it, narrows the reads of these clean services, and on a service with rows the
// narrowing would leave out (a regular service_name in another service's stream) finds them and falls back.
//
// conformance: semantics/service-name-stream-filter, semantics/service-name-stream-route, service-name-derivation
func TestCompat_ServiceNameStreamFilterLikeLoki(t *testing.T) { runServiceNameStreamFilterLikeLoki(t) }

// TestVLCapability_ServiceNameStreamFilterLikeLoki is TestCompat_ServiceNameStreamFilterLikeLoki in the VictoriaLogs
// version matrix (compat-vl): the stream filter's or groups, the empty value matching a stream without the label,
// and the stream labels read as the rows' fields, on every supported VictoriaLogs release.
//
// conformance: vl-stream-filter-or-groups-and-missing-label, semantics/service-name-stream-filter, semantics/service-name-stream-route
func TestVLCapability_ServiceNameStreamFilterLikeLoki(t *testing.T) {
	runServiceNameStreamFilterLikeLoki(t)
}

func runServiceNameStreamFilterLikeLoki(t *testing.T) {
	run := fmt.Sprintf("snsf-%d", time.Now().UnixNano())
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	lines := func(kind string) []string {
		out := make([]string, 12)
		for i := range out {
			if kind == "json" {
				out[i] = fmt.Sprintf(`{"level":"%s","msg":"%s line %d"}`, []string{"info", "error"}[i%2], run, i)
			} else {
				out[i] = fmt.Sprintf(`level=%s msg="%s line %d"`, []string{"info", "warn"}[i%2], run, i)
			}
		}
		return out
	}
	services := map[string][]streamDef{
		run + "-otel": {
			{Labels: map[string]string{"service.name": run + "-otel", "k8s.pod.name": run + "-otel-0"}, Lines: lines("json")},
			{Labels: map[string]string{"service.name": run + "-otel", "k8s.pod.name": run + "-otel-1"}, Lines: lines("logfmt")},
		},
		run + "-push": {{Labels: map[string]string{"service_name": run + "-push", "env": "snsf"}, Lines: lines("logfmt"), StructuredMetadata: map[string]string{"trace_id": "abc"}}},
		run + "-app":  {{Labels: map[string]string{"app": run + "-app", "env": "snsf"}, Lines: lines("json")}},
	}
	for _, defs := range services {
		for _, sd := range defs {
			pushStream(t, base, sd)
		}
	}
	forceVLFlush(t)
	end := base.Add(time.Minute)
	start := base.Add(-time.Minute)
	waitForFixtureOnBothBackends(t, fmt.Sprintf(`{"service.name"=%q}`, run+"-otel"), fmt.Sprintf(`{service_name=%q}`, run+"-otel"), start, end, 24)
	waitForFixtureOnBothBackends(t, fmt.Sprintf(`{service_name=%q}`, run+"-push"), fmt.Sprintf(`{service_name=%q}`, run+"-push"), start, end, 12)
	waitForFixtureOnBothBackends(t, fmt.Sprintf(`{app=%q}`, run+"-app"), fmt.Sprintf(`{service_name=%q}`, run+"-app"), start, end, 12)

	off, on, route := snProxy(t, false), snProxy(t, true), snRouteProxy(t)
	for service := range services {
		// The automatic route, enabled: the same lines and counts as Loki and as the proxy without it.
		sel := fmt.Sprintf(`{service_name=%q}`, service)
		if got, want := snLines(t, route, sel, start, end), snLines(t, lokiURL, sel, start, end); strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s: the proxy with the route returns %d lines, Loki %d:\n%v\n%v", service, len(got), len(want), got, want)
		}
		params := url.Values{"query": {sel + " | json | logfmt | drop __error__, __error_details__"}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"1000"}}
		a, ab := rejectedQueryGet(t, route, "/loki/api/v1/query_range", params, "0", nil)
		b, bb := rejectedQueryGet(t, off, "/loki/api/v1/query_range", params, "0", nil)
		if a != http.StatusOK || a != b || snE2ECanonical(ab) != snE2ECanonical(bb) {
			t.Errorf("%s: the parser chain differs with the route:\n on  %d %.300s\n off %d %.300s", service, a, ab, b, bb)
		}
	}
	if outcomes := snRouteOutcomes(t, route); outcomes["narrowed"] == 0 {
		t.Errorf("the route narrowed no read of a clean service: %v", outcomes)
	}
	// A service with rows the narrowing would leave out (a regular service_name field in a stream of another
	// service.name, VictoriaLogs only): the proof finds them and the reads fall back, so the route answers what the
	// proxy without it answers.
	viol := run + "-viol"
	var rows strings.Builder
	for i := 0; i < 12; i++ {
		for _, r := range []map[string]string{
			{"service.name": viol, "k8s.pod.name": viol + "-0", "_msg": fmt.Sprintf("%s own %d", run, i)},
			{"service.name": viol + "-other", "k8s.pod.name": viol + "-1", "service_name": viol, "_msg": fmt.Sprintf("%s stored %d", run, i)},
		} {
			r["_time"] = base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
			line, _ := json.Marshal(r)
			rows.Write(line)
			rows.WriteByte('\n')
		}
	}
	resp, err := http.Post(vlURL+"/insert/jsonline?_stream_fields=service.name,k8s.pod.name", "application/stream+json", strings.NewReader(rows.String()))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	forceVLFlush(t)
	violSel := fmt.Sprintf(`{service_name=%q}`, viol)
	before := snRouteOutcomes(t, route)["violation"]
	deadline := time.Now().Add(30 * time.Second)
	for len(snLines(t, off, violSel, start, end)) != 24 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: VictoriaLogs does not hold the 24 rows", viol)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if got, want := snLines(t, route, violSel, start, end), snLines(t, off, violSel, start, end); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s: the route returns %d lines where the proxy without it returns %d", viol, len(got), len(want))
	}
	if after := snRouteOutcomes(t, route)["violation"]; after <= before {
		t.Errorf("%s: the proof found no violation (%v)", viol, snRouteOutcomes(t, route))
	}
	for service, defs := range services {
		want := 12 * len(defs)
		sel := fmt.Sprintf(`{service_name=%q}`, service)
		lokiLines, onLines, offLines := snLines(t, lokiURL, sel, start, end), snLines(t, on, sel, start, end), snLines(t, off, sel, start, end)
		if len(lokiLines) != want {
			t.Fatalf("%s: Loki returned %d lines, want %d: the fixture is not in place", service, len(lokiLines), want)
		}
		if strings.Join(onLines, "\n") != strings.Join(lokiLines, "\n") {
			t.Errorf("%s: the proxy with the flag returns %d lines, Loki %d:\n%v\n%v", service, len(onLines), len(lokiLines), onLines, lokiLines)
		}
		if strings.Join(onLines, "\n") != strings.Join(offLines, "\n") {
			t.Errorf("%s: the flag changes the lines: %d with, %d without", service, len(onLines), len(offLines))
		}
		for _, q := range []string{
			"sum(count_over_time(" + sel + "[2m]))",
			"sum(count_over_time(" + sel + ` |= "line" [2m]))`,
			"sum(count_over_time(" + sel + ` | logfmt | drop __error__, __error_details__ [2m]))`,
		} {
			loki, withFlag, without := snCount(t, lokiURL, q, end), snCount(t, on, q, end), snCount(t, off, q, end)
			if withFlag != loki || withFlag != without {
				t.Errorf("%s: %s = %s with the flag, %s without, Loki %s", service, q, withFlag, without, loki)
			}
		}
		for _, q := range []string{
			"sum by (detected_level) (count_over_time(" + sel + " [1m]))",
			sel + " | json | logfmt | drop __error__, __error_details__",
		} {
			params := url.Values{"query": {q}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}, "limit": {"1000"}}
			a, ab := rejectedQueryGet(t, on, "/loki/api/v1/query_range", params, "0", nil)
			b, bb := rejectedQueryGet(t, off, "/loki/api/v1/query_range", params, "0", nil)
			if a != http.StatusOK || a != b || snE2ECanonical(ab) != snE2ECanonical(bb) {
				t.Errorf("%s: %s differs with the flag:\n on  %d %.300s\n off %d %.300s", service, q, a, ab, b, bb)
			}
		}
	}
}

// snE2ECanonical is an answer with sorted object keys and sorted results, without its query statistics.
func snE2ECanonical(body []byte) string {
	var v map[string]any
	if json.Unmarshal(body, &v) != nil {
		return string(body)
	}
	if d, ok := v["data"].(map[string]any); ok {
		delete(d, "stats")
		if items, ok := d["result"].([]any); ok {
			sort.Slice(items, func(i, j int) bool {
				a, _ := json.Marshal(items[i])
				b, _ := json.Marshal(items[j])
				return string(a) < string(b)
			})
		}
	}
	out, _ := json.Marshal(v)
	return string(out)
}
