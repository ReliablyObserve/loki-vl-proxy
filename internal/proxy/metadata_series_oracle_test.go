package proxy

import (
	"context"
	"encoding/json"
	"io"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// smOracleEnd is the end of the structured-metadata oracle data set (unix seconds).
const smOracleEnd = 1700003600

// smOracleStream is a stream of the data set: its labels, and per line the structured metadata as Loki's push API
// stores it (sanitized names) and as VictoriaLogs stores it (the names as sent).
type smOracleStream struct {
	labels map[string]string
	lines  []smOracleLine
}

type smOracleLine struct {
	ts       int64
	line     string
	metaLoki map[string]string
	metaVL   [][2]string
}

// smOracleStreams is an OpenTelemetry-like data set: services whose lines carry a per-line trace_id and span_id, a
// pod name and service.name in structured metadata (service.name is service_name_extracted in Loki's series, beside
// the service_name stream label), a line without metadata, a metadata field named like a stream label of another
// stream only, and services with no structured metadata at all.
func smOracleStreams() []smOracleStream {
	var out []smOracleStream
	for si, svc := range []string{"checkout", "pricing", "plain-a", "plain-b"} {
		for _, level := range []string{"info", "error"} {
			for pod := 0; pod < 2; pod++ {
				s := smOracleStream{labels: map[string]string{"app": svc, "service_name": svc, "env": "production", "level": level, "pod": fmt.Sprintf("%s-%d", svc, pod)}}
				for i := 0; i < 14; i++ {
					ts := int64(smOracleEnd-3500+si*17+pod*5+i*233)*int64(time.Second) + int64(si*100+pod)
					l := smOracleLine{ts: ts, line: fmt.Sprintf(`{"msg":"m%d","duration_ms":%d}`, i, 10+(i*37+si)%90)}
					if si < 2 && i%5 != 4 {
						trace := fmt.Sprintf("%s%02d%d%s", svc[:2], i, pod, level[:1])
						podName := fmt.Sprintf("k8s-%s-%d", svc, i%2)
						l.metaLoki = map[string]string{"trace_id": trace, "service_name_extracted": svc, "k8s_pod_name": podName}
						l.metaVL = [][2]string{{"trace_id", trace}, {"service.name", svc}, {"k8s.pod.name", podName}}
						if si == 1 && i%3 == 0 {
							l.metaLoki["region"] = "eu"
							l.metaVL = append(l.metaVL, [2]string{"region", "eu"})
						}
					}
					s.lines = append(s.lines, l)
				}
				out = append(out, s)
			}
		}
	}
	return out
}

// smOracleQueries are the metric functions and wrappers whose answer reads a value per series, the controls whose
// answer sums them, and an instant form of each kind.
var smOracleQueries = []string{
	`count_over_time({env="production"}[5m])`,
	`rate({env="production"}[10m])`,
	`bytes_over_time({app="checkout"}[5m])`,
	`count by (app) (count_over_time({env="production"}[5m]))`,
	`avg by (service_name) (rate({env="production"}[5m]))`,
	`max by (app) (count_over_time({env="production"}[10m]))`,
	`min(count_over_time({env="production"}[5m]))`,
	`stddev by (app) (count_over_time({env="production"}[10m]))`,
	`stdvar(count_over_time({env="production"}[10m]))`,
	`topk(3, count_over_time({env="production"}[10m]))`,
	`bottomk(2, count_over_time({app="pricing"}[10m]))`,
	`sort_desc(count_over_time({app="pricing"}[5m]))`,
	`sum without (pod) (count_over_time({env="production"}[5m]))`,
	`count without (level) (count_over_time({env="production"}[5m]))`,
	`sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`,
	`count_over_time({env="production"}[5m]) > 0`,
	`sum by (service_name) (max by (service_name, app) (count_over_time({env="production"}[5m])))`,
	`sum by (app) (max_over_time({app="checkout"} | json | unwrap duration_ms [10m]))`,
	`count by (app) (count_over_time({env="production"} | drop trace_id [5m]))`,
	`sum by (app) (count_over_time({env="production"}[5m]))`,
	`sum(rate({env="production"}[5m]))`,
	`count(count_over_time({app="plain-a"}[5m]))`,
}

// smOracleSeriesLimit is the tenant's max_query_series of the limited run: the data set's series per stream fit it,
// its series per structured-metadata set do not.
const smOracleSeriesLimit = 40

// smOracleLimitQueries are answered under smOracleSeriesLimit.
var smOracleLimitQueries = []string{
	`count_over_time({env="production"}[5m])`,
	`count by (app) (count_over_time({env="production"}[5m]))`,
	`sum without (pod) (count_over_time({app="checkout"}[5m]))`,
	`sum by (app) (rate({env="production"}[5m]))`,
	`count by (trace_id) (count_over_time({env="production"}[5m]))`,
	`avg by (app) (count_over_time({env="production"}[5m]))`,
}

// TestStructuredMetadataSeriesMatchLoki runs the proxy with -structured-metadata-series-identity over VictoriaLogs
// v1.49.0 and v1.54.0 (their own storage and query handlers, test/oracle/vl serve) and holds every answer to the Loki
// engine's (test/oracle/loki engine) over the same rows: the metadata Loki's push API stores (sanitized names) on
// Loki's side, the fields as sent on VictoriaLogs'. Every function and wrapper of smOracleQueries, instant and range
// (a step below, at and above the range), with and without the flag: with it every answer is Loki's; without it the
// series-level answers differ (the stream identity) and the sums do not. The reads the flag rebuilds carry the
// set_stream_fields pipe and survive VictoriaLogs' parser.
//
// conformance: metric-identity/structured-metadata-splits-series
func TestStructuredMetadataSeriesMatchLoki(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs and Loki")
	}
	t.Cleanup(querycorpus.PauseRecording())
	streams := smOracleStreams()
	type request struct {
		id, target, query string
		instant           bool
	}
	var requests, limitRequests []request
	set := oraclerun.EngineDataSet{}
	for _, s := range streams {
		es := oraclerun.EngineStream{Labels: s.labels}
		for _, l := range s.lines {
			meta := map[string]string{"detected_level": s.labels["level"]}
			for k, v := range l.metaLoki {
				meta[k] = v
			}
			es.Entries = append(es.Entries, oraclerun.EngineEntry{TS: l.ts, Line: l.line, Metadata: meta})
		}
		set.Streams = append(set.Streams, es)
	}
	for qi, q := range smOracleQueries {
		instant := url.Values{"query": {q}, "time": {fmt.Sprint(smOracleEnd - 600)}}
		id := fmt.Sprintf("i%d", qi)
		requests = append(requests, request{id: id, target: "/loki/api/v1/query?" + instant.Encode(), query: q, instant: true})
		set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id, Instant: true, Form: instant.Encode()})
		for _, step := range []int{60, 300, 900} {
			// Bounds on every step's grid: Loki's frontend and the proxy evaluate the same steps.
			form := url.Values{"query": {q}, "start": {fmt.Sprint((smOracleEnd - 3600) / 900 * 900)}, "end": {fmt.Sprint((smOracleEnd - 600) / 900 * 900)}, "step": {fmt.Sprint(step)}}
			id := fmt.Sprintf("r%d-%d", qi, step)
			requests = append(requests, request{id: id, target: "/loki/api/v1/query_range?" + form.Encode(), query: q})
			set.Requests = append(set.Requests, oraclerun.EngineRequest{ID: id, Form: form.Encode()})
		}
	}
	// The same rows under a series limit: a query whose Loki series pass it answers Loki's limit error, one whose
	// series fit answers as without the limit.
	limited := oraclerun.EngineDataSet{Streams: set.Streams, MaxSeries: smOracleSeriesLimit}
	for qi, q := range smOracleLimitQueries {
		for _, instant := range []bool{true, false} {
			form := url.Values{"query": {q}, "time": {fmt.Sprint(smOracleEnd - 600)}}
			path := "/loki/api/v1/query?"
			if !instant {
				form = url.Values{"query": {q}, "start": {fmt.Sprint((smOracleEnd - 3600) / 900 * 900)}, "end": {fmt.Sprint((smOracleEnd - 600) / 900 * 900)}, "step": {"60"}}
				path = "/loki/api/v1/query_range?"
			}
			id := fmt.Sprintf("l%d-%v", qi, instant)
			limitRequests = append(limitRequests, request{id: id, target: path + form.Encode(), query: q, instant: instant})
			limited.Requests = append(limited.Requests, oraclerun.EngineRequest{ID: id, Instant: instant, Form: form.Encode()})
		}
	}
	loki, err := oraclerun.LokiEngine(filepath.Join("..", "..", "test", "oracle", "loki"), []oraclerun.EngineDataSet{set, limited})
	if err != nil {
		t.Fatal(err)
	}
	var rows []oraclerun.ServeRow
	for _, s := range streams {
		var stream [][2]string
		for _, k := range smSortedKeys(s.labels) {
			stream = append(stream, [2]string{k, s.labels[k]})
		}
		for _, l := range s.lines {
			fields := append(append([][2]string(nil), l.metaVL...), [2]string{"_msg", l.line})
			rows = append(rows, oraclerun.ServeRow{TS: l.ts, Stream: stream, Fields: fields})
		}
	}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{{Tenant: 0, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		front := &smFront{version: v.version, upstream: srv.URL}
		fs := httptest.NewServer(front)
		on, off := smOracleProxy(t, fs.URL, true), smOracleProxy(t, fs.URL, false)
		matched, offDiffers, series := 0, 0, 0
		for _, r := range requests {
			want := loki[r.id]
			if want.Status != http.StatusOK {
				t.Fatalf("%s %s: Loki answered %d %s", v.version, r.target, want.Status, want.Error)
			}
			wantResult := smResult(t, want.Body)
			got := doCompatProxyRequest(on, r.target, map[string]string{"X-Scope-OrgID": "0"})
			if got.Code != http.StatusOK {
				t.Errorf("%s %s: proxy answered %d %s", v.version, r.target, got.Code, got.Body.String())
				continue
			}
			compare := smCompare
			if strings.HasPrefix(r.query, "topk(") || strings.HasPrefix(r.query, "bottomk(") {
				// Loki keeps its map order among series of equal value at the k-th place, which no reader of the answer can
				// rebuild (semantics/topk-per-step-membership-differs); a split makes such ties common. The values ranked at
				// each step are held to Loki's.
				compare = smCompareRanked
			}
			if diff := compare(wantResult, smResult(t, got.Body.Bytes())); diff != "" {
				t.Errorf("%s %s:\n%s", v.version, r.target, diff)
				continue
			}
			matched++
			series += len(wantResult)
			if base := doCompatProxyRequest(off, r.target, map[string]string{"X-Scope-OrgID": "0"}); compare(wantResult, smResult(t, base.Body.Bytes())) != "" {
				offDiffers++
			}
		}
		capped := smOracleProxyLimited(t, fs.URL)
		refused := 0
		for _, r := range limitRequests {
			want := loki[r.id]
			got := doCompatProxyRequest(capped, r.target, map[string]string{"X-Scope-OrgID": "0"})
			if got.Code != want.Status {
				t.Errorf("%s %s under the series limit: proxy %d %s, Loki %d %s %s", v.version, r.target, got.Code, got.Body.String(), want.Status, want.Body, want.Error)
				continue
			}
			if want.Status != http.StatusOK {
				refused++
				if body := strings.TrimSpace(got.Body.String()); body != strings.TrimSpace(want.Error) {
					t.Errorf("%s %s under the series limit: proxy %q, Loki %q", v.version, r.target, body, want.Error)
				}
				continue
			}
			if diff := smCompare(smResult(t, want.Body), smResult(t, got.Body.Bytes())); diff != "" {
				t.Errorf("%s %s under the series limit:\n%s", v.version, r.target, diff)
			}
		}
		if refused == 0 {
			t.Errorf("%s: no request passed the series limit", v.version)
		}
		rebuilt := front.rebuilt()
		if rebuilt == 0 {
			t.Errorf("%s: no read carried the identity", v.version)
		}
		// Every rebuilt read the proxy sent is one VictoriaLogs' parser reads back (its clone): the serve oracle answered
		// each with 200, or the request above failed.
		t.Logf("VictoriaLogs %s: %d of %d answers equal Loki's (%d series); without the flag %d differ; %d reads rebuilt",
			v.version, matched, len(requests), series, offDiffers, rebuilt)
		if offDiffers == 0 {
			t.Errorf("%s: no answer differs without the flag: the data set does not reach the identity", v.version)
		}
		fs.Close()
		srv.Stop()
	}
}

func smSortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func smOracleProxy(t *testing.T, backend string, on bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
		BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
		StructuredMetadataSeriesIdentity: on,
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

func smOracleProxyLimited(t *testing.T, backend string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
		BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
		StructuredMetadataSeriesIdentity: true, MaxStatsQuerySeries: smOracleSeriesLimit,
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

// smFront passes the proxy's reads to the serve oracle and counts those that carry the identity.
type smFront struct {
	version, upstream string
	mu                sync.Mutex
	withIdentity      int
}

func (f *smFront) rebuilt() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.withIdentity
}

func (f *smFront) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		_, _ = w.Write([]byte("OK"))
		return
	case "/metrics":
		_, _ = w.Write([]byte(`vm_app_version{version="victoria-logs-20260101-000000-tags-` + f.version + `-0-g0000000", short_version="` + f.version + `"} 1` + "\n"))
		return
	}
	body := make([]byte, 0, 1024)
	if r.Body != nil {
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, r.Body)
		body = []byte(buf.String())
	}
	query := r.URL.Query().Get("query")
	if form, err := url.ParseQuery(string(body)); err == nil && form.Get("query") != "" {
		query = form.Get("query")
	}
	if strings.Contains(query, "| set_stream_fields ") && !strings.Contains(query, "_stream |") {
		f.mu.Lock()
		f.withIdentity++
		f.mu.Unlock()
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, f.upstream+r.URL.RequestURI(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// smSeries is an answer's series by their labels in one spelling, each a map of sample time to value.
type smSeries map[string]map[string]string

func smResult(t *testing.T, body []byte) smSeries {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("answer %s: %v", body, err)
	}
	out := smSeries{}
	for _, s := range resp.Data.Result {
		key, _ := json.Marshal(s.Metric)
		points := map[string]string{}
		values := s.Values
		if s.Value != nil {
			values = [][]any{s.Value}
		}
		for _, v := range values {
			points[fmt.Sprint(v[0])] = fmt.Sprint(v[1])
		}
		out[string(key)] = points
	}
	return out
}

// smCompare is "" when got has want's series and values (finite floats to the last bits), else what differs.
func smCompare(want, got smSeries) string {
	var b strings.Builder
	for k, wp := range want {
		gp, ok := got[k]
		if !ok {
			fmt.Fprintf(&b, "  missing %s\n", k)
			continue
		}
		for ts, wv := range wp {
			if !smClose(wv, gp[ts]) {
				fmt.Fprintf(&b, "  %s at %s: Loki %s, proxy %s\n", k, ts, wv, gp[ts])
			}
		}
		if len(gp) != len(wp) {
			fmt.Fprintf(&b, "  %s: Loki %d points, proxy %d\n", k, len(wp), len(gp))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			fmt.Fprintf(&b, "  extra %s\n", k)
		}
	}
	if b.Len() > 2000 {
		return b.String()[:2000]
	}
	return b.String()
}

// smCompareRanked is "" when got ranks the same values as want at every sample time: the series names may differ
// where values tie.
func smCompareRanked(want, got smSeries) string {
	values := func(s smSeries) map[string][]float64 {
		out := map[string][]float64{}
		for _, points := range s {
			for ts, v := range points {
				var f float64
				_, _ = fmt.Sscan(v, &f)
				out[ts] = append(out[ts], f)
			}
		}
		for ts := range out {
			sort.Float64s(out[ts])
		}
		return out
	}
	w, g := values(want), values(got)
	var b strings.Builder
	for ts, wv := range w {
		if fmt.Sprint(wv) != fmt.Sprint(g[ts]) {
			fmt.Fprintf(&b, "  at %s: Loki ranks %v, proxy %v\n", ts, wv, g[ts])
		}
	}
	for ts := range g {
		if _, ok := w[ts]; !ok {
			fmt.Fprintf(&b, "  at %s: proxy ranks %v, Loki nothing\n", ts, g[ts])
		}
	}
	return b.String()
}

func smClose(a, b string) bool {
	if a == b {
		return true
	}
	var x, y float64
	if _, err := fmt.Sscan(a, &x); err != nil {
		return false
	}
	if _, err := fmt.Sscan(b, &y); err != nil {
		return false
	}
	return math.Abs(x-y) <= 1e-9*math.Max(math.Abs(x), math.Abs(y))
}
