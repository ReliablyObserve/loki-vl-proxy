package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
)

// planDefectBackend is a fake VictoriaLogs that refuses the calls refuse picks,
// with refusal's status and body, and answers every other call with an empty
// valid answer of its endpoint. It records each call's path and LogsQL.
type planDefectBackend struct {
	*httptest.Server
	mu    sync.Mutex
	calls [][2]string
}

func newPlanDefectBackend(t *testing.T, refuse func(path, query string) (int, string)) *planDefectBackend {
	t.Helper()
	b := &planDefectBackend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if !strings.HasPrefix(r.URL.Path, "/select/logsql/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		query := r.Form.Get("query")
		b.mu.Lock()
		b.calls = append(b.calls, [2]string{r.URL.Path, query})
		b.mu.Unlock()
		if status, body := refuse(r.URL.Path, query); status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case r.URL.Path == "/select/logsql/query" || r.URL.Path == "/select/logsql/tail":
			w.Header().Set("Content-Type", "application/x-ndjson")
		case r.URL.Path == "/select/logsql/hits":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *planDefectBackend) recorded() [][2]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][2]string(nil), b.calls...)
}

// planDefectProxy is a proxy in front of backend with its routes registered
// and its request error lines written as JSON to logs.
func planDefectProxy(t *testing.T, backendURL string) (*Proxy, http.Handler, *bytes.Buffer) {
	t.Helper()
	p := newTestProxy(t, backendURL)
	var logs bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&planDefectLogWriter{w: &logs}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux, &logs
}

// planDefectLogWriter serialises writes from the handler and background goroutines.
type planDefectLogWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *planDefectLogWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// planDefectCount is loki_vl_proxy_plan_defects_total{endpoint,plan_route} as exported.
func planDefectCount(t *testing.T, m *metrics.Metrics, endpoint, route string) int {
	t.Helper()
	w := httptest.NewRecorder()
	m.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := fmt.Sprintf("loki_vl_proxy_plan_defects_total{endpoint=%q,plan_route=%q} ", endpoint, route)
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.Atoi(rest)
			if err != nil {
				t.Fatalf("metric line %q: %v", line, err)
			}
			return n
		}
	}
	return 0
}

// planDefectLines are the request error lines that name a plan defect.
func planDefectLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if m["plan_defect"] == true {
			out = append(out, m)
		}
	}
	return out
}

// TestPlanDefect_ClientGetsLokisFailedBackendAnswer: when VictoriaLogs refuses
// the LogsQL of a query the proxy validated, the client reads Loki's answer for
// a failed backend call (500 Internal Server Error), never VictoriaLogs' status
// or text; the refusal is counted and logged with its VictoriaLogs route, the
// hash of the LogsQL sent and VictoriaLogs' message. A route that has a
// fallback giving the same answer keeps it, and a query Loki rejects is still
// rejected with Loki's 400 before VictoriaLogs is asked.
//
// conformance: semantics/vl-refusal-is-plan-defect, semantics/error-layer-backend-failure-status, semantics/error-layer-backend-text
func TestPlanDefect_ClientGetsLokisFailedBackendAnswer(t *testing.T) {
	refuseAll := func(status int, body string) func(string, string) (int, string) {
		return func(path, _ string) (int, string) {
			if path == "/select/logsql/tail" {
				return 0, ""
			}
			return status, body
		}
	}
	refusePath := func(want string, status int, body string) func(string, string) (int, string) {
		return func(path, _ string) (int, string) {
			if path == want {
				return status, body
			}
			return 0, ""
		}
	}
	for _, tc := range []struct {
		name     string
		path     string
		params   map[string]string
		refuse   func(path, query string) (int, string)
		endpoint string
		route    string // the refused call's VictoriaLogs route; "" = no plan defect
		status   int
		text     string // "" = a 200 answer
		noVL     bool   // the request is answered before any VictoriaLogs call
	}{
		{
			name: "log query, VictoriaLogs 400", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `{app="api"} |= "x"`, "limit": "10"},
			refuse: refuseAll(http.StatusBadRequest, vlBodyParsePattern), endpoint: "query_range", route: "/select/logsql/query",
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			name: "metric query, VictoriaLogs 422 from stats", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `sum(count_over_time({app="api"}[5m]))`, "step": "60"},
			refuse: refuseAll(http.StatusUnprocessableEntity, vlBodyParseRegexStats), endpoint: "query_range",
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			name: "instant metric query, VictoriaLogs 400 echoing the query", path: "/loki/api/v1/query",
			params: map[string]string{"query": `sum(count_over_time({app="api"}[5m]))`},
			refuse: refuseAll(http.StatusBadRequest, vlBody152ParsePattern), endpoint: "query", route: "/select/logsql/stats_query",
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			name: "metadata route, VictoriaLogs 400", path: "/loki/api/v1/series",
			params: map[string]string{"match[]": `{app="api"}`},
			refuse: refuseAll(http.StatusBadRequest, vlBodyParsePattern), endpoint: "series",
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			name: "argument VictoriaLogs refuses", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `sum(count_over_time({app="api"}[5m]))`, "step": "60"},
			refuse: refuseAll(http.StatusBadRequest, vlBodyBadStepArg), endpoint: "query_range", route: "/select/logsql/stats_query_range",
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			// The unwrap stats fast path is the proxy's own rewrite: its refusal falls back to the raw evaluator,
			// which answers what Loki answers, so the client gets the answer and no failure.
			name: "fallback route answers", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `sum_over_time({app="billing"} | logfmt | unwrap latency [5m])`, "step": "60"},
			refuse: refusePath("/select/logsql/stats_query_range", http.StatusUnprocessableEntity, vlBodyParseRegexStats),
			status: http.StatusOK,
		},
		{
			// The volume's derived-label form is a rewrite too: the plain _stream form answers instead.
			name: "volume fallback answers", path: "/loki/api/v1/index/volume",
			params: map[string]string{"query": `{app="billing"}`, "targetLabels": "service_name"},
			refuse: func(path, query string) (int, string) {
				if strings.HasPrefix(path, "/select/logsql/stats_query") && strings.Contains(query, " as service_name ") {
					return http.StatusBadRequest, vlBodyParsePattern
				}
				return 0, ""
			},
			status: http.StatusOK,
		},
		{
			// A resource limit is a failed call, not a refusal: the same 500, but no plan defect.
			name: "VictoriaLogs memory limit is no plan defect", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `sum(count_over_time({app="api"}[5m]))`, "step": "60"},
			refuse: refuseAll(http.StatusUnprocessableEntity, vlBodyStatsMemory),
			status: http.StatusInternalServerError, text: lokiErrInternal,
		},
		{
			name: "query Loki rejects is 400 before VictoriaLogs", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `{app="api"} |`},
			refuse: refuseAll(http.StatusBadRequest, vlBodyParsePattern),
			status: http.StatusBadRequest, text: "parse error at line 1, col 14: syntax error: unexpected $end", noVL: true,
		},
		{
			name: "metric query Loki rejects is 400 before VictoriaLogs", path: "/loki/api/v1/query_range",
			params: map[string]string{"query": `sum_over_time({app="api"}[5m])`, "step": "60"},
			refuse: refuseAll(http.StatusBadRequest, vlBodyParsePattern),
			status: http.StatusBadRequest, text: "parse error : invalid aggregation sum_over_time without unwrap", noVL: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newPlanDefectBackend(t, tc.refuse)
			p, h, logs := planDefectProxy(t, backend.URL)
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, badRequestURL(tc.path, tc.params), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			h.ServeHTTP(w, req)
			calls := backend.recorded()
			if tc.noVL != (len(calls) == 0) {
				t.Fatalf("VictoriaLogs calls %v, want none: %v", calls, tc.noVL)
			}
			if w.Code != tc.status {
				t.Fatalf("status %d, want %d: %.300q", w.Code, tc.status, w.Body.String())
			}
			if tc.text != "" && w.Body.String() != tc.text {
				t.Fatalf("body %q, want %q", w.Body.String(), tc.text)
			}
			if body := w.Body.String(); strings.Contains(body, "billing-secret-value") || strings.Contains(body, "cannot parse") || strings.Contains(body, "failed to parse") {
				t.Fatalf("the answer carries VictoriaLogs' refusal: %q", body)
			}
			lines := planDefectLines(t, logs)
			if tc.endpoint == "" {
				if len(lines) != 0 {
					t.Fatalf("a plan defect was logged for an answer that is none: %v", lines)
				}
				return
			}
			if len(lines) != 1 {
				t.Fatalf("want one plan defect line, got %d: %s", len(lines), logs.String())
			}
			line := lines[0]
			route, _ := line["plan_route"].(string)
			if tc.route != "" && route != tc.route || !strings.HasPrefix(route, "/select/logsql/") {
				t.Fatalf("plan_route %q, want %q", route, tc.route)
			}
			var sent string
			for _, c := range calls {
				if c[0] == route {
					sent = c[1]
					break
				}
			}
			if want := strconv.FormatUint(logsqlHash(sent), 16); line["logsql_hash"] != want {
				t.Fatalf("logsql_hash %v, want %s (the hash of %q)", line["logsql_hash"], want, sent)
			}
			if line["kind"] != "internal" || line["msg_id"] != "internal" || line["code"] != float64(http.StatusInternalServerError) {
				t.Fatalf("log line class %v %v %v", line["kind"], line["msg_id"], line["code"])
			}
			if up, _ := line["upstream_error"].(string); !strings.Contains(up, "cannot parse") && !strings.Contains(up, "cannot parse duration") {
				t.Fatalf("upstream_error %q does not carry VictoriaLogs' message", up)
			}
			if n := planDefectCount(t, p.metrics, tc.endpoint, route); n != 1 {
				t.Fatalf("plan defects counted %d, want 1", n)
			}
		})
	}
}

// TestPlanDefect_QueriesLokiRejectsThatReachVictoriaLogs pins the queries Loki
// rejects at its lexer that the proxy's parser accepts (blanks Loki does not
// read as white space between tokens): validation lets them through, so
// VictoriaLogs refuses the LogsQL and the client reads the failed-backend 500
// where Loki answers 400. The grammar sweep holds their count
// (victorialogs_rejections_for_queries_loki_rejects), which may only go down.
//
// conformance: semantics/parser-reads-unicode-blank-as-white-space, semantics/vl-refusal-is-plan-defect
func TestPlanDefect_QueriesLokiRejectsThatReachVictoriaLogs(t *testing.T) {
	for _, query := range []string{"sum by (a) (rate({a=\"b\"}[5m]))", "sum\v(rate({a=\"b\"}[5m])) by (a)"} {
		t.Run(strconv.Quote(query), func(t *testing.T) {
			backend := newPlanDefectBackend(t, func(string, string) (int, string) { return http.StatusBadRequest, vlBody152ParsePattern })
			_, h, _ := planDefectProxy(t, backend.URL)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, badRequestURL("/loki/api/v1/query_range", map[string]string{"query": query, "step": "60"}), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			h.ServeHTTP(w, req)
			if len(backend.recorded()) == 0 || w.Code != http.StatusInternalServerError || w.Body.String() != lokiErrInternal {
				t.Fatalf("got %d %q after %d VictoriaLogs calls; once validation rejects it, Loki's 400 belongs here", w.Code, w.Body.String(), len(backend.recorded()))
			}
		})
	}
}

// TestPlanDefect_TypedWhereClassified: a VictoriaLogs refusal is typed as a
// plan defect where it is classified, and every other VictoriaLogs answer is
// not; the typed and the untyped forms of a refusal answer alike, and the
// failure stays final (no retry, no fallback, no stale answer).
//
// conformance: semantics/vl-refusal-is-plan-defect
func TestPlanDefect_TypedWhereClassified(t *testing.T) {
	p := &Proxy{}
	for _, tc := range []struct {
		name   string
		status int
		body   string
		defect bool
	}{
		{"parse 400", http.StatusBadRequest, vlBodyParsePattern, true},
		{"parse 422", http.StatusUnprocessableEntity, vlBodyParseRegexStats, true},
		{"echoed parse 400", http.StatusBadRequest, vlBody152ParsePattern, true},
		{"empty query", http.StatusBadRequest, vlBodyEmptyQuery, true},
		{"bad step argument", http.StatusUnprocessableEntity, vlBodyBadStepArg, true},
		{"memory limit", http.StatusUnprocessableEntity, vlBodyStatsMemory, false},
		{"unknown stats function", http.StatusUnprocessableEntity, vlBodyUnknownStatsFirst, false},
		{"unsupported path", http.StatusBadRequest, vlBodyUnsupportedPath, false},
		{"server error", http.StatusInternalServerError, "boom", false},
		{"queue", http.StatusTooManyRequests, "busy", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := p.redactedBackendStatusError("", tc.status, []byte(tc.body))
			if isPlanDefect(err) != tc.defect || isUpstreamQueryRejected(err) != tc.defect {
				t.Fatalf("plan defect %v, rejected %v, want %v", isPlanDefect(err), isUpstreamQueryRejected(err), tc.defect)
			}
			var statusErr *upstreamStatusError
			if !errors.As(err, &statusErr) || statusErr.status != tc.status {
				t.Fatalf("the VictoriaLogs status is not reachable under the type: %v", err)
			}
			if !tc.defect {
				return
			}
			// The error keeps VictoriaLogs' redacted reason for log lines; the writer answers Loki's 500.
			if err.Error() != statusErr.Error() || err.Error() == lokiErrInternal {
				t.Fatalf("the plan defect's text %q is not VictoriaLogs' reason %q", err.Error(), statusErr.Error())
			}
			var logs bytes.Buffer
			wp := &Proxy{log: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))}
			rec := httptest.NewRecorder()
			if code := wp.writeErr(rec, context.Background(), err); code != http.StatusInternalServerError || rec.Body.String() != lokiErrInternal {
				t.Fatalf("writeErr answered %d %q", code, rec.Body.String())
			}
			lines := planDefectLines(t, &logs)
			if len(lines) != 1 || lines[0]["kind"] != "internal" || lines[0]["msg_id"] != "internal" || lines[0]["upstream_error"] != statusErr.Error() {
				t.Fatalf("log line %s", logs.String())
			}
			// A refusal met by the translation step's refusal mapping is still the proxy's defect.
			if refusal := queryRefusal(fmt.Errorf("translate: %w", err)); refusal.Kind() != lokierr.KindInternal || refusal.Status() != http.StatusInternalServerError || refusal.Text() != lokiErrInternal {
				t.Fatalf("queryRefusal answered %d %q", refusal.Status(), refusal.Text())
			}
			untyped := &upstreamStatusError{status: tc.status, msg: statusErr.msg, class: vlErrorQueryRejected}
			for _, e := range []error{err, untyped, fmt.Errorf("left operand: %w", err)} {
				status, errorType, text := lokiClientError(context.Background(), e)
				if status != http.StatusInternalServerError || errorType != "internal" || text != lokiErrInternal {
					t.Fatalf("%T answered %d %q %q", e, status, errorType, text)
				}
			}
			if shouldFallbackToGenericMetadata(err) {
				t.Fatal("a refused query falls back to the generic metadata route")
			}
		})
	}
}

// TestPlanDefect_FactsWithoutTelemetry: a plan defect of a request whose
// telemetry recorded no refused call (a call made outside the request, or no
// request context at all) is still counted and logged, with "unknown" in
// place of the route and the hash; an error that is no plan defect is neither.
//
// conformance: semantics/vl-refusal-is-plan-defect
func TestPlanDefect_FactsWithoutTelemetry(t *testing.T) {
	m := metrics.NewMetrics()
	p := &Proxy{metrics: m}
	defect := p.redactedBackendStatusError("", http.StatusBadRequest, []byte(vlBodyParsePattern))
	//nolint:staticcheck // a nil context is what writeFailure may be called with
	for _, ctx := range []context.Context{nil, context.Background()} {
		route, hash, ok := p.notePlanDefect(ctx, defect)
		if !ok || route != planDefectUnknown || hash != planDefectUnknown {
			t.Fatalf("notePlanDefect = %q %q %v", route, hash, ok)
		}
	}
	if n := planDefectCount(t, m, planDefectUnknown, planDefectUnknown); n != 2 {
		t.Fatalf("counted %d, want 2", n)
	}
	if _, _, ok := p.notePlanDefect(context.Background(), errors.New("boom")); ok {
		t.Fatal("an error that is no plan defect was counted")
	}
	// The first refused call of a request is the one the line names.
	rt := &requestTelemetry{}
	ctx := context.WithValue(context.Background(), requestTelemetryKey, rt)
	recordVLRefusalCall(ctx, "/select/logsql/query", "a")
	recordVLRefusalCall(ctx, "/select/logsql/hits", "b")
	recordVLRefusalCall(context.Background(), "/select/logsql/hits", "c") // no telemetry: nothing to keep
	if route, hash, _ := p.notePlanDefect(ctx, defect); route != "/select/logsql/query" || hash != strconv.FormatUint(logsqlHash("a"), 16) {
		t.Fatalf("named %q %q, want the first refused call", route, hash)
	}
	if isVLRefusalStatus(http.StatusOK) || isVLRefusalStatus(http.StatusInternalServerError) || !isVLRefusalStatus(http.StatusBadRequest) || !isVLRefusalStatus(http.StatusUnprocessableEntity) {
		t.Fatal("refusal statuses are 400 and 422 only")
	}
}

// TestPlanDefect_TailClose: a tail whose VictoriaLogs call is refused after
// the upgrade is closed with Loki's 1011 and its 500 text; the refusal is
// counted and logged as a plan defect, and any other failure stays a debug line.
//
// conformance: semantics/vl-refusal-is-plan-defect, loki_api_v1_tail
func TestPlanDefect_TailClose(t *testing.T) {
	m := metrics.NewMetrics()
	var logs bytes.Buffer
	p := &Proxy{metrics: m, log: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	defect := p.redactedBackendStatusError("", http.StatusBadRequest, []byte(vlBodyParsePattern))
	if !p.logTailPlanDefect(context.Background(), defect) {
		t.Fatal("a refused tail call is a plan defect")
	}
	if p.logTailPlanDefect(context.Background(), errors.New("stream ended")) {
		t.Fatal("a broken stream is no plan defect")
	}
	if lines := planDefectLines(t, &logs); len(lines) != 1 || lines[0]["msg"] != "tail ended with a plan defect" || !strings.Contains(fmt.Sprint(lines[0]["upstream_error"]), "cannot parse") {
		t.Fatalf("lines %v", logs.String())
	}
	if n := planDefectCount(t, m, planDefectUnknown, planDefectUnknown); n != 1 {
		t.Fatalf("counted %d, want 1", n)
	}
	if _, _, text := lokiClientError(context.Background(), defect); text != lokiErrInternal {
		t.Fatalf("close text %q", text)
	}
}

// BenchmarkPlanDefectAnswer is the error path of a refused query: VictoriaLogs'
// answer typed where it is classified, then written through the error layer and
// its log line (encoded at Warn, the level of the line on either status).
func BenchmarkPlanDefectAnswer(b *testing.B) {
	p := &Proxy{metrics: metrics.NewMetrics(), log: slog.New(slog.NewJSONHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	body := []byte(vlBodyParsePattern)
	ctx := context.WithValue(context.Background(), requestTelemetryKey, &requestTelemetry{})
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		p.writeErr(w, ctx, p.redactedBackendStatusError("", http.StatusBadRequest, body))
	}
}

// BenchmarkQueryRangeLogThroughRoutes is the happy path the refusal record
// sits on: a log query through the registered route (request telemetry
// included) with one VictoriaLogs call answered 200, a new window each time so
// no cache answers it.
func BenchmarkQueryRangeLogThroughRoutes(b *testing.B) {
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2024-01-15T10:30:00Z","_msg":"test","_stream":"{app=\"nginx\"}","app":"nginx"}` + "\n"))
	}))
	b.Cleanup(vl.Close)
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(time.Minute, 10000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		req := httptest.NewRequest(http.MethodGet, `/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D&limit=10&start=`+strconv.Itoa(1700000000+i)+`&end=`+strconv.Itoa(1700000060+i), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}
}

type discardWriter struct{}

func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }
