//go:build e2e

package e2e_compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// jsonVolumeStreamLine is one fixture line of a stream; level is the stream
// label, empty for a stream without one.
type jsonVolumeStreamLine struct {
	ts    time.Time
	level string
	msg   string
}

// vlQueryRecorder forwards to VictoriaLogs and records each request path with
// its LogsQL query.
type vlQueryRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (rec *vlQueryRecorder) reset() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	calls := rec.calls
	rec.calls = nil
	return calls
}

// newJSONVolumeRouteProxy starts an in-process proxy with the
// patterns-autodetect label flags whose backend requests are recorded.
func newJSONVolumeRouteProxy(t *testing.T) (string, *vlQueryRecorder) {
	t.Helper()
	target, err := url.Parse(vlURL)
	if err != nil {
		t.Fatal(err)
	}
	rec := &vlQueryRecorder{}
	forward := httputil.NewSingleHostReverseProxy(target)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		form, _ := url.ParseQuery(string(body))
		query := form.Get("query")
		if query == "" {
			query = r.URL.Query().Get("query")
		}
		rec.mu.Lock()
		rec.calls = append(rec.calls, r.URL.Path+" "+query)
		rec.mu.Unlock()
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)

	p, err := proxy.New(proxy.Config{
		BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error",
		LabelStyle: proxy.LabelStyleUnderscores, MetadataFieldMode: proxy.MetadataFieldModeHybrid, EmitStructuredMetadata: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.ValidateBackendVersionCompatibility(ctx); err != nil {
		t.Fatalf("backend version probe: %v", err)
	}
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL, rec
}

// ingestJSONVolumeFixture writes byte-identical lines into Loki and
// VictoriaLogs, with level as a stream label where the stream has one, and
// proves both backends hold every line of each app.
func ingestJSONVolumeFixture(t *testing.T, apps map[string][]jsonVolumeStreamLine) {
	t.Helper()
	var vlRows strings.Builder
	var streams []any
	for app, lines := range apps {
		byLevel := map[string][][]string{}
		for _, line := range lines {
			fields := map[string]string{"_time": line.ts.UTC().Format(time.RFC3339Nano), "_msg": line.msg, "app": app}
			if line.level != "" {
				fields["level"] = line.level
			}
			row, _ := json.Marshal(fields)
			vlRows.Write(row)
			vlRows.WriteByte('\n')
			byLevel[line.level] = append(byLevel[line.level], []string{strconv.FormatInt(line.ts.UnixNano(), 10), line.msg})
		}
		for level, values := range byLevel {
			labels := map[string]string{"app": app}
			if level != "" {
				labels["level"] = level
			}
			streams = append(streams, map[string]any{"stream": labels, "values": values})
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,level", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	// The fixture is older than query_ingesters_within, so Loki serves it only
	// after the ingester flushes it to the store.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
	deadline := time.Now().Add(180 * time.Second)
	for {
		pending := ""
		for app, lines := range apps {
			fx := slidingLiveFixture{app: app}
			for _, line := range lines {
				fx.lines = append(fx.lines, slidingLiveLine{ts: line.ts, msg: line.msg})
			}
			if lokiCount, vlCount := slidingFixtureCounts(t, fx); lokiCount != len(lines) || vlCount != len(lines) {
				pending += fmt.Sprintf(" %s: fixture=%d loki=%d victorialogs=%d;", app, len(lines), lokiCount, vlCount)
			}
		}
		if pending == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not healthy:%s", pending)
		}
		time.Sleep(time.Second)
	}
}

// Grafana Explore's logs volume for `{...} | json` groups by level and
// detected_level with parser errors dropped. It must match Loki without
// scanning raw rows, including unparseable lines, levels stored only in the
// JSON body and JSON keys that collide with stream labels; a level-less line
// that Loki parses partially keeps the exact raw evaluator.
func TestRangeMetricCompatibilityJSONLogsVolume(t *testing.T) {
	now := time.Now()
	app := fmt.Sprintf("json-volume-%d", now.UnixNano())
	partial := fmt.Sprintf("json-volume-partial-%d", now.UnixNano())
	plain := fmt.Sprintf("level-volume-plain-%d", now.UnixNano())
	logfmt := fmt.Sprintf("level-volume-logfmt-%d", now.UnixNano())
	logfmtRisk := fmt.Sprintf("level-volume-logfmt-risk-%d", now.UnixNano())
	s0 := now.Add(-9 * time.Hour).Truncate(time.Hour)
	fixture := map[string][]jsonVolumeStreamLine{}
	for i := 0; i < 60; i++ { // one tick every 10s for 10 minutes
		tick := s0.Add(time.Duration(i) * 10 * time.Second)
		at := func(k int) time.Time { return tick.Add(time.Duration(k) * time.Second) }
		fixture[app] = append(fixture[app],
			// A JSON key colliding with a stream label becomes level_extracted.
			jsonVolumeStreamLine{at(0), "info", fmt.Sprintf(`{"level":"error","msg":"collision %d"}`, i)},
			jsonVolumeStreamLine{at(1), "warn", fmt.Sprintf(`{"msg": "truncated %d`, i)},
			jsonVolumeStreamLine{at(2), "", fmt.Sprintf(`{"level":"debug","n":%d}`, i)},
			jsonVolumeStreamLine{at(3), "", fmt.Sprintf(`{"counter":%d}`, i)},
			jsonVolumeStreamLine{at(4), "", fmt.Sprintf("plain text line %d", i)},
			jsonVolumeStreamLine{at(5), "info", fmt.Sprintf(`{"app":"json-shadow","level":"info","n":%d}`, i)},
		)
		fixture[partial] = append(fixture[partial],
			jsonVolumeStreamLine{at(0), "info", fmt.Sprintf(`{"msg":"ok %d"}`, i)},
			jsonVolumeStreamLine{at(1), "", fmt.Sprintf(`{"level":"error","msg": truncated %d`, i)},
			jsonVolumeStreamLine{at(2), "", fmt.Sprintf(` {"level":"warn","n":%d}`, i)},
			// Loki trims spaces around keys and skips array values.
			jsonVolumeStreamLine{at(3), "", fmt.Sprintf(`{" level ":"debug","n":%d}`, i)},
			jsonVolumeStreamLine{at(4), "", fmt.Sprintf(`{"level":["error"],"n":%d}`, i)},
		)
		// Level-less lines carry no level in the body, so Loki's ingest-time
		// detected_level is unknown for them.
		fixture[plain] = append(fixture[plain],
			jsonVolumeStreamLine{at(0), "info", fmt.Sprintf("request served %d", i)},
			jsonVolumeStreamLine{at(1), "debug", fmt.Sprintf("cache probe %d", i)},
			jsonVolumeStreamLine{at(2), "", fmt.Sprintf("op=select n=%d", i)},
		)
		fixture[logfmt] = append(fixture[logfmt],
			jsonVolumeStreamLine{at(0), "info", fmt.Sprintf("level=error msg=collision n=%d", i)},
			jsonVolumeStreamLine{at(1), "", fmt.Sprintf(`level=warn op=update msg="slow query" n=%d`, i)},
			jsonVolumeStreamLine{at(2), "", fmt.Sprintf("op=select n=%d", i)},
			jsonVolumeStreamLine{at(3), "", fmt.Sprintf(`{"msg":"json body","n":%d}`, i)},
		)
		fixture[logfmtRisk] = append(fixture[logfmtRisk],
			jsonVolumeStreamLine{at(0), "", fmt.Sprintf("n=%d\tlevel=warn", i)},
		)
	}
	ingestJSONVolumeFixture(t, fixture)

	route, recorder := newJSONVolumeRouteProxy(t)
	targets := map[string]string{"proxy": proxyURL, "patterns-autodetect": patternsAutodetectProxyURL, "in-process": route}
	start, end := s0.Add(-2*time.Minute), s0.Add(12*time.Minute)
	for _, tc := range []struct {
		query   string
		step    time.Duration
		pushed  bool
		minimum int // Loki series expected for the fixture
	}{
		// Grafana's exact supplementary logs volume query.
		{`sum by (level, detected_level) (count_over_time({app="` + app + `"} | json | drop __error__[1m]))`, time.Minute, true, 4},
		{`sum by (level) (count_over_time({app="` + app + `"} | json | drop __error__, __error_details__ [2m]))`, time.Minute, true, 4},
		{`sum by (app) (count_over_time({app=~"` + app + `"} | json | drop __error__[1m]))`, time.Minute, true, 1},
		{`sum by (detected_level) (bytes_over_time({app="` + app + `"} | json | drop __error__[90s]))`, 30 * time.Second, true, 4},
		{`sum by (level) (count_over_time({app="` + partial + `"} | json | drop __error__[1m]))`, time.Minute, false, 3},
		// Grafana's logs volume for plain and logfmt selectors.
		{`sum by (level, detected_level) (count_over_time({app="` + plain + `"} | drop __error__[1m]))`, time.Minute, true, 3},
		{`sum by (level, detected_level) (count_over_time({app="` + logfmt + `"} | logfmt | drop __error__[1m]))`, time.Minute, true, 3},
	} {
		t.Run(tc.query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
			if len(loki) < tc.minimum {
				t.Fatalf("Loki sanity: expected at least %d series, got %d: %v", tc.minimum, len(loki), loki)
			}
			for name, base := range targets {
				recorder.reset()
				assertSlidingParity(t, tc.query+" ["+name+"]", loki, slidingRangeSeries(t, base, tc.query, start, end, tc.step, nil))
				if name != "in-process" {
					continue
				}
				stats, guards, raw := 0, 0, 0
				for _, call := range recorder.reset() {
					switch {
					case strings.HasPrefix(call, "/select/logsql/stats_query_range "):
						stats++
					case strings.HasPrefix(call, "/select/logsql/query ") && strings.HasSuffix(call, " | limit 1"):
						guards++
					case strings.HasPrefix(call, "/select/logsql/query "):
						raw++
					}
				}
				wantGuards := 1
				if !strings.Contains(tc.query, "json") && !strings.Contains(tc.query, "logfmt") {
					wantGuards = 0 // no parser: stored fields only
				}
				if tc.pushed && (stats != 1 || guards != wantGuards || raw != 0) {
					t.Fatalf("expected %d guard and one stats_query_range request without raw rows: stats=%d guards=%d raw=%d", wantGuards, stats, guards, raw)
				}
				// The bucket query runs beside the guard and is cancelled when the
				// guard finds a line the parsers read differently, so a stats
				// request may be observed; the answer must still come from the
				// raw evaluator.
				if !tc.pushed && (guards != 1 || raw != 1) {
					t.Fatalf("expected the guard to keep the raw evaluator: stats=%d guards=%d raw=%d", stats, guards, raw)
				}
			}
		})
	}

	// Loki's logfmt decoder ends a token at any whitespace, VictoriaLogs'
	// unpack_logfmt does not, so the stats pushdown must stay out and the
	// pipeline has to be evaluated over the rows. The proof is the values:
	// they must equal Loki's.
	t.Run("logfmt parse risk", func(t *testing.T) {
		query := `sum by (level, detected_level) (count_over_time({app="` + logfmtRisk + `"} | logfmt | drop __error__[1m]))`
		loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
		if len(loki) == 0 {
			t.Fatal("Loki sanity: expected the tab-separated fixture to produce a series")
		}
		recorder.reset()
		assertSlidingParity(t, query+" [in-process]", loki, slidingRangeSeries(t, route, query, start, end, time.Minute, nil))
		guards, rows := 0, 0
		for _, call := range recorder.reset() {
			switch {
			case strings.HasSuffix(call, " | limit 1"):
				guards++
			case strings.HasPrefix(call, "/select/logsql/query "):
				rows++
			}
		}
		if guards != 1 || rows != 1 {
			t.Fatalf("expected one risk check and one row fetch: guards=%d rows=%d", guards, rows)
		}
	})
}
