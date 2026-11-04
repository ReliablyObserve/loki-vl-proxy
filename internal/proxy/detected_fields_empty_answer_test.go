package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A detected_fields query that matches no line answers like Loki: no fields
// and no limit. Loki v3.7.9 lists only the fields of the lines the query
// itself returned (pkg/querier/queryrange/detected_fields.go:42-73,
// parseDetectedFields over re.Data.Result) and sets the limit only when it
// lists a field or a value (:86-89); its JSON omits an empty list and a zero
// limit (pkg/logproto/logproto.pb.go DetectedFieldsResponse, omitempty), so
// the body is `{}`. The proxy keeps its status and data mirror (an
// envelope the visual comparison already explains).

// emptyMatchVL is a recording fake VictoriaLogs whose rows exist only for
// queries matches accepts: a filter that matches no row (a level pill, a
// field filter, a selector without streams) gets an empty scan and an empty
// field_names answer, as VictoriaLogs gives for the same filter.
type emptyMatchVL struct {
	srv     *httptest.Server
	full    *fakeLogfmtVL
	none    *fakeLogfmtVL
	matches func(query string) bool
	scanLag time.Duration
	mu      sync.Mutex
	log     []string
}

func newEmptyMatchVL(t testing.TB, lines []fakeVLLine, matches func(string) bool) *emptyMatchVL {
	t.Helper()
	return newEmptyMatchVLWithScanLag(t, lines, matches, 0)
}

// newEmptyMatchVLWithScanLag delays every scan (/select/logsql/query) by lag.
func newEmptyMatchVLWithScanLag(t testing.TB, lines []fakeVLLine, matches func(string) bool, lag time.Duration) *emptyMatchVL {
	t.Helper()
	e := &emptyMatchVL{full: &fakeLogfmtVL{lines: lines}, none: &fakeLogfmtVL{}, matches: matches, scanLag: lag}
	e.srv = httptest.NewServer(http.HandlerFunc(e.serve))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *emptyMatchVL) serve(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	query := r.Form.Get("query")
	e.mu.Lock()
	e.log = append(e.log, r.URL.Path+" "+query)
	e.mu.Unlock()
	lag := e.scanLag // set before the server starts
	matched := e.matches(query)
	if r.URL.Path == "/select/logsql/streams" {
		var hits []fieldHit
		if matched && len(e.full.lines) > 0 {
			hits = append(hits, fieldHit{e.full.lines[0].streamString(), int64(len(e.full.lines))})
		}
		writeVLFieldNames(w, hits)
		return
	}
	if r.URL.Path == "/select/logsql/query" && lag > 0 {
		select {
		case <-time.After(lag):
		case <-r.Context().Done():
			return
		}
	}
	if matched {
		e.full.serve(w, r)
		return
	}
	e.none.serve(w, r)
}

func (e *emptyMatchVL) calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.log...)
}

func (e *emptyMatchVL) reset() {
	e.mu.Lock()
	e.log = nil
	e.mu.Unlock()
}

// withoutToken is a matcher for a strict query whose filter matches no row:
// VictoriaLogs answers rows only for queries that do not carry token.
func withoutToken(token string) func(string) bool {
	return func(q string) bool { return !strings.Contains(q, token) }
}

type detectedFieldsAnswer struct {
	keys   []string
	labels []string
	limit  interface{}
	raw    string
}

func detectedFieldsRequest(t testing.TB, h http.Handler, orgID, query, start, end string) detectedFieldsAnswer {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start)
	params.Set("end", end)
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?"+params.Encode(), nil)
	if orgID != "" {
		req.Header.Set("X-Scope-OrgID", orgID)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("detected_fields status %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	ans := detectedFieldsAnswer{limit: body["limit"], raw: rec.Body.String()}
	for k := range body {
		ans.keys = append(ans.keys, k)
	}
	sort.Strings(ans.keys)
	fields, _ := body["fields"].([]interface{})
	for _, f := range fields {
		if m, ok := f.(map[string]interface{}); ok {
			ans.labels = append(ans.labels, fmt.Sprint(m["label"]))
		}
	}
	sort.Strings(ans.labels)
	return ans
}

func handlerFunc(p *Proxy) http.Handler { return http.HandlerFunc(p.handleDetectedFields) }

// fieldLookupPath reports the VictoriaLogs endpoints a detected_fields scan or
// field lookup (strict or relaxed) reads.
func fieldLookupPath(path string) bool {
	switch path {
	case "/select/logsql/query", "/select/logsql/field_names", "/select/logsql/field_values", "/select/logsql/streams":
		return true
	}
	return false
}

const (
	emptyAnswerStart = "2026-11-03T13:57:00.000Z"
	emptyAnswerEnd   = "2026-11-03T14:57:00.000Z"
)

// conformance: semantics/detected-fields-empty-answer, loki_api_v1_detected_fields
func TestDetectedFields_EmptyAnswerLikeLoki_EdgeTable(t *testing.T) {
	cases := []struct {
		name       string
		lines      []fakeVLLine
		query      string
		matches    func(string) bool
		wantLabels []string // nil: Loki's empty answer (no fields, no limit)
	}{
		// The Logs Drilldown page of a JSON service with the level pills
		// error and warn: Drilldown escapes the pipe, so the regexp is the
		// literal "error|warn" and matches no line on Loki or the proxy.
		{"escaped pipe level regexp matches no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"} | detected_level=~"error\\|warn"` + drilldownParserTail, withoutToken(`"error|warn"`), nil},
		{"level pill matches no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"} | detected_level="debug"` + drilldownParserTail, withoutToken(`"debug"`), nil},
		{"negated level pill matches no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"} | detected_level!="error"` + drilldownParserTail, withoutToken(`"error"`), nil},
		{"field filter behind the parsers matches no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"}` + drilldownParserTail + `| method="DELETE"`, withoutToken(`DELETE`), nil},
		{"level pill and field filter match no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail + `| method="DELETE"`, withoutToken(`DELETE`), nil},
		{"line filter matches no line", apiGatewayFakeLines(),
			`{service_name="api-gateway"} |= "no-such-text"` + drilldownParserTail, withoutToken(`no-such-text`), nil},
		{"selector without streams", apiGatewayFakeLines(),
			`{service_name="no-such-service"}`, withoutToken(`no-such-service`), nil},
		// The db-postgres page: a plain-text service with an error pill and
		// no error line in the window.
		{"plain-text level pill matches no line", dbPostgresPlainFakeLines(),
			`{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail, withoutToken(`"error"`), nil},
		{"logfmt level pill matches no line", paymentLogfmtFakeLines(),
			`{service_name="payment-service"} | detected_level="error"` + drilldownParserTail, withoutToken(`"error"`), nil},
		// Controls: lines that match keep their fields and Loki's limit.
		{"plain-text lines matched", dbPostgresPlainFakeLines(),
			`{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail, func(string) bool { return true }, []string{"detected_level"}},
		{"json lines matched", apiGatewayFakeLines(),
			`{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail, func(string) bool { return true },
			[]string{"detected_level", "duration_ms", "error", "ip", "method", "path", "region", "request_id", "span_id", "status", "trace_id", "user_id"}},
	}
	for _, mode := range detectedFieldsModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				vl := newEmptyMatchVL(t, tc.lines, tc.matches)
				p := newDetectedFieldsTestProxy(t, vl.srv.URL, mode.opts...)
				got := detectedFieldsRequest(t, handlerFunc(p), "", tc.query, emptyAnswerStart, emptyAnswerEnd)
				if tc.wantLabels == nil {
					if len(got.labels) != 0 || got.limit != nil {
						t.Fatalf("Loki answers {} (no fields, no limit); got %s", got.raw)
					}
					if strings.Join(got.keys, ",") != "data,fields,status" {
						t.Fatalf("keys %v, want the proxy's envelope without limit: %s", got.keys, got.raw)
					}
					// No relaxed lookup: every scan and field lookup carried
					// the query's own filter (the label-name inventory a
					// profile reads over `*` is not a field lookup).
					for _, c := range vl.calls() {
						path, q, _ := strings.Cut(c, " ")
						if fieldLookupPath(path) && tc.matches(q) {
							t.Fatalf("VictoriaLogs call without the query's filter (relaxed lookup): %s", c)
						}
					}
					return
				}
				if strings.Join(got.labels, ",") != strings.Join(tc.wantLabels, ",") {
					t.Fatalf("labels %v, want %v", got.labels, tc.wantLabels)
				}
				if got.limit != float64(1000) {
					t.Fatalf("limit %v, want 1000 with fields: %s", got.limit, got.raw)
				}
			})
		}
	}
}

// The matcher tokens above tell the strict query from its relaxed form: the
// relaxed query the old fallback sent carries none of them, so the "no
// relaxed lookup" assertion is not vacuous.
// conformance: semantics/detected-fields-empty-answer
func TestDetectedFields_EmptyAnswerTokensLeaveRelaxedQuery(t *testing.T) {
	vl := newEmptyMatchVL(t, nil, func(string) bool { return false })
	p := newDetectedFieldsTestProxy(t, vl.srv.URL)
	for _, tc := range []struct{ query, token string }{
		{`{service_name="api-gateway"} | detected_level=~"error\\|warn"` + drilldownParserTail, `"error|warn"`},
		{`{service_name="api-gateway"} | detected_level="debug"` + drilldownParserTail, `"debug"`},
		{`{service_name="api-gateway"}` + drilldownParserTail + `| method="DELETE"`, `DELETE`},
		{`{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail, `"error"`},
	} {
		strict, err := p.translateQueryWithContext(context.Background(), tc.query)
		if err != nil || !strings.Contains(strict, tc.token) {
			t.Fatalf("strict translation of %s lacks %s: %q (%v)", tc.query, tc.token, strict, err)
		}
		relaxed, err := p.translateQueryWithContext(context.Background(), relaxedFieldDetectionQuery(tc.query))
		if err != nil || strings.Contains(relaxed, tc.token) {
			t.Fatalf("relaxed translation of %s carries %s: %q (%v)", tc.query, tc.token, relaxed, err)
		}
	}
}

// A repeated empty answer is served from the endpoint cache with the same
// body and no VictoriaLogs call, as before the change.
// conformance: semantics/detected-fields-empty-answer
func TestDetectedFields_EmptyAnswerCachedRepeat(t *testing.T) {
	vl := newEmptyMatchVL(t, apiGatewayFakeLines(), withoutToken(`"error|warn"`))
	p := newDetectedFieldsTestProxy(t, vl.srv.URL)
	q := `{service_name="api-gateway"} | detected_level=~"error\\|warn"` + drilldownParserTail
	first := detectedFieldsRequest(t, handlerFunc(p), "", q, emptyAnswerStart, emptyAnswerEnd)
	vl.reset()
	second := detectedFieldsRequest(t, handlerFunc(p), "", q, emptyAnswerStart, emptyAnswerEnd)
	if strings.TrimSpace(first.raw) != strings.TrimSpace(second.raw) {
		t.Fatalf("cached answer differs:\n%s\n%s", first.raw, second.raw)
	}
	if n := len(vl.calls()); n != 0 {
		t.Fatalf("cached repeat made %d VictoriaLogs calls: %v", n, vl.calls())
	}
}

// A scan that runs out of the proxy's Drilldown deadline keeps its degraded
// answer: the native field index of the bare selector (out of scope here; a
// Loki query that times out fails instead).
// conformance: semantics/detected-fields-empty-answer
func TestDetectedFields_ScanDeadlineKeepsNativeFallback(t *testing.T) {
	vl := newEmptyMatchVLWithScanLag(t, apiGatewayFakeLines(), withoutToken(`"debug"`), 2*time.Second)
	p := newDetectedFieldsTestProxy(t, vl.srv.URL)
	p.drilldownScanTimeout = 100 * time.Millisecond
	q := `{service_name="api-gateway"} | detected_level="debug"` + drilldownParserTail
	got := detectedFieldsRequest(t, handlerFunc(p), "", q, emptyAnswerStart, emptyAnswerEnd)
	if len(got.labels) == 0 || got.limit != float64(1000) {
		t.Fatalf("deadline fallback should list the native fields with the limit: %s\n%v", got.raw, vl.calls())
	}
	relaxed := false
	for _, c := range vl.calls() {
		if strings.HasPrefix(c, "/select/logsql/field_names ") && !strings.Contains(c, `"debug"`) {
			relaxed = true
		}
	}
	if !relaxed {
		t.Fatalf("no relaxed native lookup after the deadline: %v", vl.calls())
	}
}

// The multi-tenant merge answers Loki's empty body too: no limit when no
// tenant lists a field, the limit when one does.
// conformance: semantics/detected-fields-empty-answer
func TestDetectedFields_MultiTenantEmptyAnswer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		matchesA  bool
		matchesB  bool
		wantEmpty bool
	}{
		{"no tenant matches", false, false, true},
		{"first tenant empty, second matches", false, true, false},
		{"both match", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := paymentLogfmtFakeLines()
			full := &fakeLogfmtVL{lines: lines}
			none := &fakeLogfmtVL{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusOK)
					return
				}
				match := tc.matchesB
				if r.Header.Get("AccountID") == "10" {
					match = tc.matchesA
				}
				if match {
					full.serve(w, r)
					return
				}
				none.serve(w, r)
			}))
			t.Cleanup(srv.Close)
			_, _, mux := newTwoTenantProxy(t, srv.URL)
			q := `{service_name="payment-service"} | detected_level="warn"` + drilldownParserTail
			got := detectedFieldsRequest(t, mux, "tenant-a|tenant-b", q, emptyAnswerStart, emptyAnswerEnd)
			if tc.wantEmpty {
				if len(got.labels) != 0 || got.limit != nil {
					t.Fatalf("want Loki's empty answer, got %s", got.raw)
				}
				return
			}
			if len(got.labels) == 0 || got.limit == nil {
				t.Fatalf("want fields with a limit, got %s", got.raw)
			}
		})
	}
}

// lokiEmptyAnswerReference is Loki's detected_fields answer over the lines
// the query matched: none gives `{}`; otherwise the reference field labels of
// the #1032 differential (lokiDetectedFieldLabels) and the limit.
func lokiEmptyAnswerReference(matched []fakeVLLine) (map[string]struct{}, bool) {
	if len(matched) == 0 {
		return map[string]struct{}{}, false
	}
	return lokiDetectedFieldLabels(matched, true), true
}

// conformance: semantics/detected-fields-empty-answer
func TestDetectedFields_EmptyAnswerDifferentialAgainstLokiReference(t *testing.T) {
	queries := []struct{ query, token string }{
		{`{service_name="svc"} | detected_level="error"` + drilldownParserTail, `"error"`},
		{`{service_name="svc"} | detected_level=~"error\\|warn"` + drilldownParserTail, `"error|warn"`},
		{`{service_name="svc"}` + drilldownParserTail + `| status!="200"`, `"200"`},
		{`{service_name="svc"} |= "needle"` + drilldownParserTail, `needle`},
	}
	rng := rand.New(rand.NewSource(20261104))
	for i := 0; i < 120; i++ {
		lines := randomDetectedFieldsLines(rng)
		q := queries[i%len(queries)]
		strictMatches := rng.Intn(2) == 0
		matcher := withoutToken(q.token)
		if strictMatches {
			matcher = func(string) bool { return true }
		}
		vl := newEmptyMatchVL(t, lines, matcher)
		p := newDetectedFieldsTestProxy(t, vl.srv.URL, withLokiProfileNames())
		got := detectedFieldsRequest(t, handlerFunc(p), "", q.query, emptyAnswerStart, emptyAnswerEnd)
		var matched []fakeVLLine
		if strictMatches {
			matched = lines
		}
		want, wantLimit := lokiEmptyAnswerReference(matched)
		gotSet := map[string]struct{}{}
		for _, l := range got.labels {
			gotSet[l] = struct{}{}
		}
		var extra, missing []string
		for k := range gotSet {
			if _, ok := want[k]; !ok {
				extra = append(extra, k)
			}
		}
		for k := range want {
			if _, ok := gotSet[k]; !ok {
				missing = append(missing, k)
			}
		}
		if len(extra) > 0 || len(missing) > 0 || (got.limit != nil) != wantLimit {
			t.Fatalf("case %d %s matched=%v\nextra %v missing %v limit %v (want limit %v)\n%s", i, q.query, strictMatches, extra, missing, got.limit, wantLimit, got.raw)
		}
	}
}

// FuzzDetectedFieldsEmptyAnswer drives one handler over arbitrary stored lines
// and either outcome of the query's filter: the answer carries a limit
// exactly when it lists a field, and a filter that matches no row never
// lists one or reaches VictoriaLogs without the filter.
func FuzzDetectedFieldsEmptyAnswer(f *testing.F) {
	for _, seed := range []struct {
		msg   string
		match bool
	}{
		{`{"method":"GET","status":500,"level":"error"}`, false},
		{`{"method":"GET","status":500,"level":"error"}`, true},
		{`level=warn msg="retry" attempt=2`, false},
		{`level=warn msg="retry" attempt=2`, true},
		{`2026-11-03 14:00:00 UTC [1] ERROR:  deadlock detected`, true},
		{``, true},
		{`{`, false},
	} {
		f.Add(seed.msg, seed.match)
	}
	var (
		mu      sync.Mutex
		current []fakeVLLine
		match   bool
	)
	stream := generatorStream("svc", "error", nil)
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		lines := current
		m := match || !strings.Contains(r.Form.Get("query"), `"error"`)
		calls = append(calls, r.URL.Path+" "+r.Form.Get("query"))
		mu.Unlock()
		if !m {
			lines = nil
		}
		// A fake per request: the scan and the field lookup run concurrently.
		(&fakeLogfmtVL{lines: lines}).serve(w, r)
	}))
	defer srv.Close()
	p := newDetectedFieldsTestProxy(f, srv.URL)
	var window atomic.Int64
	q := `{service_name="svc"} | detected_level="error"` + drilldownParserTail
	f.Fuzz(func(t *testing.T, msg string, m bool) {
		if len(msg) > 4096 {
			return
		}
		mu.Lock()
		current = []fakeVLLine{newFakeLine(stream, nil, msg)}
		match = m
		calls = nil
		mu.Unlock()
		// A window of its own per input: every request misses the caches.
		end := time.Date(2026, 11, 3, 14, 57, 0, 0, time.UTC).Add(-time.Duration(window.Add(1)) * time.Hour)
		got := detectedFieldsRequest(t, handlerFunc(p), "", q, end.Add(-time.Hour).Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
		if (got.limit != nil) != (len(got.labels) > 0) {
			t.Fatalf("limit %v with %d fields: %s", got.limit, len(got.labels), got.raw)
		}
		if !m {
			if len(got.labels) != 0 {
				t.Fatalf("no row matched, fields listed: %s", got.raw)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, c := range calls {
				if path, q, _ := strings.Cut(c, " "); fieldLookupPath(path) && !strings.Contains(q, `"error"`) {
					t.Fatalf("VictoriaLogs call without the query's filter: %s", c)
				}
			}
		}
	})
}

// BenchmarkDetectedFieldsEmptyAnswerFakeVL measures one uncached
// detected_fields request whose query matches no line (the Drilldown level
// pill pages) against the recording fake: VictoriaLogs calls and bytes per
// request, and the fields answered.
func BenchmarkDetectedFieldsEmptyAnswerFakeVL(b *testing.B) {
	for _, tc := range []struct {
		name  string
		lines []fakeVLLine
		query string
		token string
	}{
		{"json-escaped-pipe", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level=~"error\\|warn"` + drilldownParserTail, `"error|warn"`},
		{"plain-level-pill", dbPostgresPlainFakeLines(), `{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail, `"error"`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			vl := newEmptyMatchVL(b, tc.lines, withoutToken(tc.token))
			p := newDetectedFieldsTestProxy(b, vl.srv.URL, withLokiProfileNames())
			vl.reset()
			fields := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A window of its own per request: every request misses the caches.
				end := time.Date(2026, 11, 3, 14, 57, 0, 0, time.UTC).Add(-time.Duration(i) * time.Hour)
				fields = len(detectedFieldsRequest(b, handlerFunc(p), "", tc.query, end.Add(-time.Hour).Format(time.RFC3339Nano), end.Format(time.RFC3339Nano)).labels)
			}
			b.StopTimer()
			calls := 0
			for _, c := range vl.calls() {
				if path, _, _ := strings.Cut(c, " "); fieldLookupPath(path) {
					calls++
				}
			}
			b.ReportMetric(float64(calls)/float64(b.N), "vlcalls/op")
			b.ReportMetric(float64(fields), "fields")
		})
	}
}
