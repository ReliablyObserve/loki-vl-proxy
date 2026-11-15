package proxy

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// statsQueryRecorder records the LogsQL text of every stats call it passes on.
type statsQueryRecorder struct {
	inner   http.Handler
	mu      sync.Mutex
	queries []string
}

func (s *statsQueryRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query") || r.URL.Path == "/select/logsql/query" {
		_ = r.ParseForm()
		s.mu.Lock()
		s.queries = append(s.queries, r.Form.Get("query"))
		s.mu.Unlock()
	}
	s.inner.ServeHTTP(w, r)
}

// take is the texts recorded since the last take.
func (s *statsQueryRecorder) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.queries
	s.queries = nil
	return out
}

// hashed counts the texts that group by a parser's series hash.
func hashed(queries []string) int {
	n := 0
	for _, q := range queries {
		if strings.Contains(q, dynamicSeriesHashPipe) {
			n++
		}
	}
	return n
}

// Each operand of a binary operation reads the series it reads alone: the binary route evaluates an operand as a
// request of its own, so an identity operand groups its stats by the series hash on an instant query and on a range
// up to the step, and by the stream on a range longer than the step, whatever the other operand is (a number, a
// stream's metric, another identity, a nested operation) and whatever vector matching joins them. An operand whose
// range is the step keeps the hash when the other operand's range is longer.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityBinaryOperandsReadTheHash(t *testing.T) {
	const (
		maxApp = `sum by (app) (max_over_time({app="api"} | json | unwrap n [5m]))`
		minApp = `sum by (app) (min_over_time({app="api"} | json | unwrap n [5m]))`
		avgApp = `sum by (app) (avg_over_time({app="api"} | json | unwrap n [5m]))`
		countA = `sum by (app) (count_over_time({app="api"}[5m]))`
	)
	rec := &statsQueryRecorder{inner: binaryOperandStatsStub()}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.53.0", "v1.53.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	const end = int64(1700011800)
	get := func(path string, form url.Values) []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path+"?"+form.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s %s: status %d: %s", path, form.Get("query"), w.Code, w.Body)
		}
		return w.Body.Bytes()
	}
	instant := func(q string) url.Values {
		return url.Values{"query": {q}, "time": {strconv.FormatInt(end, 10)}}
	}
	rangeOf := func(q string, step time.Duration) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(end-3600, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {strconv.FormatInt(int64(step/time.Second), 10)}}
	}
	for _, tc := range []struct {
		q string
		// identities is the number of identity operands; others the operands read from the stream's series.
		identities, others int
	}{
		{maxApp + ` - ` + minApp, 2, 0},
		{maxApp + ` / on (app) ` + avgApp, 2, 0},
		{maxApp + ` / ignoring (pod) group_left ` + avgApp, 2, 0},
		{maxApp + ` * 2`, 1, 0},
		{`1000 - ` + avgApp, 1, 0},
		{`(` + maxApp + ` - ` + minApp + `) / ` + avgApp, 3, 0},
		{avgApp + ` - ` + countA, 1, 1},
		{maxApp + ` > bool ` + avgApp, 2, 0},
		{`sum(` + maxApp + ` - ` + minApp + `)`, 2, 0},
		{`SUM BY (app) (MAX_OVER_TIME({app="api"} | JSON | UNWRAP n [5m])) - ` + minApp, 2, 0},
	} {
		p.cache = cache.New(time.Minute, 1000)
		rec.take()
		get("/loki/api/v1/query", instant(tc.q))
		if sent := rec.take(); hashed(sent) != tc.identities || len(sent) != tc.identities+tc.others {
			t.Errorf("%s instant: %d of %d stats calls group by the series hash, want %d of %d", tc.q, hashed(sent), len(sent), tc.identities, tc.identities+tc.others)
		}
		// A range whose step is the range: the hash; a repeat asks VictoriaLogs nothing and answers the same series.
		cold := get("/loki/api/v1/query_range", rangeOf(tc.q, 5*time.Minute))
		if sent := rec.take(); hashed(sent) != tc.identities {
			t.Errorf("%s step=range: %d of %d stats calls group by the series hash, want %d", tc.q, hashed(sent), len(sent), tc.identities)
		}
		again := get("/loki/api/v1/query_range", rangeOf(tc.q, 5*time.Minute))
		if sent := rec.take(); len(sent) != 0 || !bytes.Equal(dynamicIdentityResult(t, cold), dynamicIdentityResult(t, again)) {
			t.Errorf("%s step=range repeat: %d VictoriaLogs calls, want 0 and the same answer", tc.q, len(sent))
		}
		// A range longer than the step: each operand keeps the stream's series, as it does alone.
		get("/loki/api/v1/query_range", rangeOf(tc.q, time.Minute))
		if sent := rec.take(); hashed(sent) != 0 || len(sent) == 0 {
			t.Errorf("%s step<range: %d of %d calls group by the series hash, want none", tc.q, hashed(sent), len(sent))
		}
	}
	// The operand whose range is the step keeps the hash though the other operand's range is longer.
	p.cache = cache.New(time.Minute, 1000)
	rec.take()
	get("/loki/api/v1/query_range", rangeOf(`sum by (app) (max_over_time({app="api"} | json | unwrap n [1m])) - `+countA, time.Minute))
	if sent := rec.take(); hashed(sent) != 1 {
		t.Errorf("mixed ranges at step 1m: %d of %d calls group by the series hash, want the identity operand's", hashed(sent), len(sent))
	}
}

// binaryOperandStatsStub answers every stats call with one series of app api: a value at the instant, and a point
// every step of a stats_query_range window, whatever the time format of its bounds.
func binaryOperandStatsStub() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[1700011800,"3"]}]}}`))
		case "/select/logsql/stats_query_range":
			at := func(v string) int64 {
				if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
					return ts.Unix()
				}
				n, _ := strconv.ParseInt(v, 10, 64)
				if n > 1e12 {
					n /= int64(time.Second)
				}
				return n
			}
			start, end := at(r.Form.Get("start")), at(r.Form.Get("end"))
			step, err := time.ParseDuration(r.Form.Get("step"))
			if err != nil || step < time.Second {
				step = time.Minute
			}
			var b strings.Builder
			b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[`)
			for ts := start - start%int64(step/time.Second); ts <= end; ts += int64(step / time.Second) {
				if b.Len() > 0 && !strings.HasSuffix(b.String(), "[") {
					b.WriteByte(',')
				}
				b.WriteString(`[` + strconv.FormatInt(ts, 10) + `,"3"]`)
			}
			b.WriteString(`]}]}}`)
			_, _ = w.Write([]byte(b.String()))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		}
	})
}

// A range request translates each operand of its binary operation with the mark the operand's own request gets, so the
// operand's request reads that translation from the cache: an identity operand over a range longer than the step is
// marked, one whose range is the step is not, a number and a request without the identity's words keep the request's
// context.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityBinaryOperandContext(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	for _, tc := range []struct {
		q, step     string
		left, right bool
	}{
		{`sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])) * 2`, "60", true, false},
		{`sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])) * 2`, "300", false, false},
		{`sum by (app) (max_over_time({app="a"} | json | unwrap n [1m])) - sum by (app) (count_over_time({app="a"}[5m]))`, "60", false, false},
		{`sum by (app) (count_over_time({app="a"}[5m])) / sum by (app) (stddev_over_time({app="a"} | logfmt | unwrap n [5m]))`, "60", false, true},
		{`sum by (app) (rate({app="a"}[5m])) / sum by (app) (count_over_time({app="a"}[5m]))`, "60", false, false},
		{`2 * sum by (app) (stddev_over_time({app="a"} | logfmt | unwrap n [5m]))`, "60", false, true},
	} {
		expr, err := logqlpkg.Parse(tc.q)
		if err != nil {
			t.Fatalf("%s: %v", tc.q, err)
		}
		bin, ok := expr.(*logqlpkg.BinOpExpr)
		if !ok {
			t.Fatalf("%s: not a binary operation", tc.q)
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {tc.q}, "step": {tc.step}}.Encode(), nil)
		left, right := p.binaryOperandContexts(r, bin)
		if streamIdentityOnly(left) != tc.left || streamIdentityOnly(right) != tc.right {
			t.Errorf("%s step %s: operands marked %v, %v; want %v, %v", tc.q, tc.step, streamIdentityOnly(left), streamIdentityOnly(right), tc.left, tc.right)
		}
		if !tc.left && left != r.Context() || !tc.right && right != r.Context() {
			t.Errorf("%s step %s: an unmarked operand got a context of its own", tc.q, tc.step)
		}
	}
}

// FuzzDynamicIdentityFenceMatchesPlan: the response key fence of a parser's dynamic identity fires exactly for the
// queries whose plan holds the identity at any depth (lower.WritesDynamicUnwrapIdentity), read in the spelling the
// handlers read (keywords in lower case) and in the spelling the client wrote: its text screen never turns away a
// query whose answer the identity changed, or an answer cached before would be served for it.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func FuzzDynamicIdentityFenceMatchesPlan(f *testing.F) {
	for _, s := range []string{
		`sum by (app) (max_over_time({app="a"} | json | unwrap n [5m])) - sum by (app) (min_over_time({app="a"} | json | unwrap n [5m]))`,
		`2 * SUM(STDDEV_OVER_TIME({app="a"} | LOGFMT | UNWRAP n [5m]))`,
		`topk(2, sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])))`,
		`label_replace(max by (app) (stdvar_over_time({app="a"} | json | unwrap n [5m])), "x", "$1", "app", "(.*)")`,
		"# c\nsum(avg_over_time({a=\"b\"} | json | unwrap x [5m]))",
		`sum(max_over_time # c` + "\n" + `({app="a"} | json | unwrap n [1m])) / on (app) group_left sum by (app) (count_over_time({app="a"}[5m]))`,
		`sum(quantile_over_time(0.9, {app="a"} | json | unwrap n [5m])) - sum(avg_over_time({app="a"} | json | keep n | unwrap n [5m]))`,
		`sum(rate({app="json"} | logfmt [5m])) * 2`,
		`sum(avg_over_time({app="a"} | unwrap n [5m])) * 2`,
		// Letters that fold to an ASCII keyword letter (U+017F, U+212A) in a keyword without an ASCII capital.
		"sum(avg_over_time({a=\"b\"} | j\u017fon | unwrap x [5m])) - sum(max_over_time({a=\"b\"} | json | unwrap x [5m]))",
		"sum(avg_over_time({a=\"b\"} | logfmt | unwrap x [5m])) / sum(\u017ftddev_over_time({a=\"b\"} | logfmt | unwrap x [5m]))",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 512 {
			return
		}
		fence := dynamicUnwrapIdentityResponseKeyVersion(q) != ""
		holds := func(text string) bool {
			plan := resolvedPlan(text)
			return plan != nil && lower.WritesDynamicUnwrapIdentity(plan)
		}
		if read := holds(dynamicQuery(q)); fence != read {
			t.Fatalf("%q: fence %v, the plan the handlers read holds the identity %v", q, fence, read)
		}
		if written := holds(strings.TrimSpace(q)); written && !fence {
			t.Fatalf("%q: the plan of the client's spelling holds the identity and the answer is not fenced", q)
		}
	})
}
