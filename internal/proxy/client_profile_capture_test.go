package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/clientprofile"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// capturedClientRequests are the header sets Grafana sent the proxy on the compose stack
// (internal/clientprofile/testdata/captured-*.json), with the surface each request came from.
func capturedClientRequests(t *testing.T) []struct {
	file, flow, surface string
	header              http.Header
} {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "clientprofile", "testdata", "captured-*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no captured header sets: %v", err)
	}
	var out []struct {
		file, flow, surface string
		header              http.Header
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var set struct {
			Requests []struct {
				Flow    string            `json:"flow"`
				Headers map[string]string `json:"headers"`
				Surface string            `json:"surface"`
			} `json:"requests"`
		}
		if err := json.Unmarshal(b, &set); err != nil {
			t.Fatal(err)
		}
		for _, r := range set.Requests {
			h := http.Header{}
			for k, v := range r.Headers {
				h.Set(k, v)
			}
			out = append(out, struct {
				file, flow, surface string
				header              http.Header
			}{filepath.Base(f), r.Flow, r.Surface, h})
		}
	}
	return out
}

// Every captured Grafana request reaches the request line and the /debug/queries callers view with its typed
// profile: Logs Drilldown recognised on every Drilldown request, Explore never taken for Drilldown, the Grafana and
// Loki datasource versions normalised, and no Drilldown version (no header carries one). The surface the response
// cache key and the Drilldown windowing read (grafana_drilldown) agrees with the typed one on Drilldown.
//
// conformance: drilldown/client-detection-captured-header-sets, observability-slow-query-log
func TestCapturedGrafanaRequestsCarryTheirClientProfile(t *testing.T) {
	for _, req := range capturedClientRequests(t) {
		var buf bytes.Buffer
		p, _ := slowLogProxy(t, 0)
		p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		p.requestSampler = nil
		p.enableQueryAnalytics = true
		serveCallerLog(p, req.header)

		lines := slowLogLines(t, &buf, "request")
		if len(lines) != 1 {
			t.Fatalf("%s %s: %d request lines", req.file, req.flow, len(lines))
		}
		line := lines[0]
		if got, _ := line["client.surface"].(string); got != req.surface {
			t.Errorf("%s %s: client.surface %q, want %q", req.file, req.flow, got, req.surface)
		}
		if _, ok := line["grafana.drilldown.version"]; ok {
			t.Errorf("%s %s: a Drilldown version in the log line: %v", req.file, req.flow, line["grafana.drilldown.version"])
		}
		legacy, _ := line["grafana.client.surface"].(string)
		if (req.surface == "drilldown") != (legacy == "grafana_drilldown") {
			t.Errorf("%s %s: typed surface %s, cache-key surface %q", req.file, req.flow, req.surface, legacy)
		}
		cacheRequest := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
		cacheRequest.Header = req.header.Clone()
		cacheProfile := p.responseProfileCacheKey(cacheRequest, "query_range")
		if strings.Contains(cacheProfile, "/sl-") != (req.surface == "drilldown") {
			t.Errorf("%s %s: cache limit mode %q does not match captured surface %s", req.file, req.flow, cacheProfile, req.surface)
		}

		rec := httptest.NewRecorder()
		p.queryTracker.Handler(rec, httptest.NewRequest(http.MethodGet, "/debug/queries", nil))
		var out struct {
			Callers metrics.CallerView `json:"callers"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Callers.RecentRequests) != 1 {
			t.Fatalf("%s %s: %d recent requests", req.file, req.flow, len(out.Callers.RecentRequests))
		}
		recent := out.Callers.RecentRequests[0]
		want := clientprofile.Detect(req.header)
		if recent.Surface != req.surface || recent.GrafanaVersion != want.Grafana.Label() || recent.DrilldownVersion != "" {
			t.Errorf("%s %s: callers view %+v, want surface %s Grafana %s", req.file, req.flow, recent, req.surface, want.Grafana.Label())
		}
		if strings.Contains(req.header.Get("User-Agent"), " loki/") != (recent.DatasourceVersion != "") {
			t.Errorf("%s %s: datasource version %q from %q", req.file, req.flow, recent.DatasourceVersion, req.header.Get("User-Agent"))
		}
	}
}

// What leaves the proxy about the client is bounded: a forged User-Agent, Source tag or referer gives a closed
// surface name and normalised versions in the log line, the callers view and the VictoriaLogs request headers, never
// the text sent.
//
// conformance: observability-slow-query-log
func TestHostileClientHeadersStayBounded(t *testing.T) {
	hostile := http.Header{
		"User-Agent":        {"Grafana/13.3.1-" + strings.Repeat("x", 4000) + " loki/1.2.3\n\x00evil"},
		"X-Query-Tags":      {"Source=" + strings.Repeat("grafana-lokiexplore-app", 200)},
		"X-Grafana-Referer": {"javascript:alert(1)//a/grafana-lokiexplore-app/" + strings.Repeat("%", 3000)},
	}
	var buf bytes.Buffer
	p, _ := slowLogProxy(t, 0)
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p.requestSampler = nil
	p.enableQueryAnalytics = true
	serveCallerLog(p, hostile)
	line := slowLogLines(t, &buf, "request")[0]
	if v, _ := line["grafana.version"].(string); v != "13.3.1" {
		t.Errorf("grafana.version %q, want the normalised 13.3.1", v)
	}
	if v, ok := line["grafana.datasource.version"]; ok {
		t.Errorf("grafana.datasource.version %v: the loki/ token lies past the bounded part of the User-Agent read", v)
	}
	if v, _ := line["client.surface"].(string); v != "drilldown" {
		t.Errorf("client.surface %q", v)
	}
	gp := detectGrafanaClientProfile(httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil), "labels", "/loki/api/v1/labels")
	if gp.version != "" || gp.client.Surface != clientprofile.SurfaceAPI {
		t.Errorf("a request without Grafana headers: version %q, surface %s", gp.version, gp.client.Surface.Label())
	}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
	r.Header = hostile
	if gp := detectGrafanaClientProfile(r, "labels", "/loki/api/v1/labels"); len(gp.version) > len("9999.9999.9999") {
		t.Errorf("the version sent upstream is %d bytes", len(gp.version))
	}
}
