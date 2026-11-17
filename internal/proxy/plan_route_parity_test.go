package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// Route parity: the real query handlers run every plan-corpus row (the curated rows and the capture sample) under
// each request profile (instant; range with step equal to, under and over the query's range; a 24h chunk) against
// a stub VictoriaLogs answering each endpoint with data or empty (so each probe is seen both ways). A run records
// its status, its VictoriaLogs requests (endpoint and parameters), how many calls it started
// (X-Loki-VL-Backend-Calls) and how many the exact repeat of the request on the same proxy starts, and its route:
// the operations, cache reads and LogQL rewrites of the request's trace. Requests are marked with
// deterministicProbes, so the concurrent probes (the json and logfmt parse-risk probes, the checks beside an
// ordered-JSON stats attempt) all reach the stub on every run. The route a physical plan predicts must equal the
// recorded one; until a construct's lowering lands it is delegated. The route_parity section of query-plan.json
// holds the counts, one digest per profile and construct of every run's status and route decisions (routes), one
// of every run's VictoriaLogs calls (calls), and the calls per profile and construct, cold and on the exact repeat
// (vl_calls), all gated: a change that moves a route, a status or a VictoriaLogs call fails here until
// LVP_QUERY_PLAN_UPDATE=1 records it (and the PR says why), and one that adds calls fails until it is recorded.

// routeEnd is the pinned end of every request window: in the past, so no freshness or recent-tail rule applies.
var routeEnd = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// routeProfile is one request shape.
type routeProfile struct {
	name    string
	instant bool
	// window and step derive the range request from the query's range (5m when it has none).
	window func(r time.Duration) time.Duration
	step   func(r time.Duration) time.Duration
}

func routeProfiles() []routeProfile {
	fixed := func(d time.Duration) func(time.Duration) time.Duration {
		return func(time.Duration) time.Duration { return d }
	}
	return []routeProfile{
		{name: "instant", instant: true},
		{name: "step=range", window: fixed(time.Hour), step: func(r time.Duration) time.Duration { return r }},
		{name: "step<range", window: fixed(time.Hour), step: func(r time.Duration) time.Duration { return max(r/5, time.Second) }},
		{name: "step>range", window: fixed(time.Hour), step: func(r time.Duration) time.Duration { return 3 * r }},
		{name: "24h chunk", window: fixed(24 * time.Hour), step: fixed(time.Minute)},
	}
}

// routeStub is a VictoriaLogs stand-in: data answers each endpoint with one series or row (a level field, a JSON
// body), empty answers with none.
type routeStub struct {
	data bool
	// jitter delays every answer by up to 3ms, so concurrent calls finish in any order (the determinism check).
	jitter bool
	mu     sync.Mutex
	on     bool
	got    []string
}

func (s *routeStub) RoundTrip(r *http.Request) (*http.Response, error) {
	form := url.Values{}
	for k, v := range r.URL.Query() {
		form[k] = v
	}
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if pf, err := url.ParseQuery(string(b)); err == nil {
			for k, v := range pf {
				form[k] = append(form[k], v...)
			}
		}
	}
	s.mu.Lock()
	if s.on {
		s.got = append(s.got, r.URL.Path+"?"+form.Encode())
	}
	s.mu.Unlock()
	if s.jitter {
		time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(s.body(r.URL.Path, form.Get("query")))), Request: r}, nil
}

// routeStatsPipe reads a stats pipe's grouping and result names, for the row a /select/logsql/query stats call
// answers with (the stub only reads what it must answer; the proxy wrote the query).
var (
	routeStatsBy     = regexp.MustCompile(`\| stats by \(([^)]*)\)`)
	routeStatsResult = regexp.MustCompile(`\bas ([A-Za-z_][A-Za-z0-9_.]*)`)
)

func (s *routeStub) body(path, query string) string {
	ts := routeEnd.Add(-30 * time.Second)
	sec := strconv.FormatInt(ts.Unix(), 10)
	switch path {
	case "/select/logsql/query":
		if !s.data {
			return ""
		}
		if i := strings.LastIndex(query, "| stats "); i >= 0 {
			row := map[string]string{}
			if m := routeStatsBy.FindStringSubmatch(query[i:]); m != nil {
				for _, f := range strings.Split(m[1], ",") {
					f = strings.TrimSpace(f)
					if name, _, ok := strings.Cut(f, ":"); ok && name == "_time" {
						row["_time"] = ts.Format(time.RFC3339Nano)
					} else if f != "" {
						row[strings.Trim(f, `"`)] = "a"
					}
				}
			}
			for _, m := range routeStatsResult.FindAllStringSubmatch(query[i:], -1) {
				row[m[1]] = "1"
			}
			b, _ := json.Marshal(row)
			return string(b) + "\n"
		}
		return `{"_time":"` + ts.Format(time.RFC3339Nano) + `","_msg":"{\"level\":\"error\",\"n\":1}","_stream":"{app=\"a\"}","app":"a","level":"error"}` + "\n"
	case "/select/logsql/stats_query_range":
		if !s.data {
			return `{"status":"success","data":{"resultType":"matrix","result":[]}}`
		}
		return `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a","level":"error"},"values":[[` + sec + `,"1"]]}]}}`
	case "/select/logsql/stats_query":
		if !s.data {
			return `{"status":"success","data":{"resultType":"vector","result":[]}}`
		}
		return `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"a","level":"error"},"value":[` + sec + `,"1"]}]}}`
	case "/select/logsql/hits":
		if !s.data {
			return `{"hits":[]}`
		}
		return `{"hits":[{"fields":{"app":"a"},"timestamps":["` + ts.Format(time.RFC3339) + `"],"values":[1],"total":1}]}`
	case "/select/logsql/streams":
		if !s.data {
			return `{"values":[]}`
		}
		return `{"values":[{"value":"{app=\"a\"}","hits":1}]}`
	case "/select/logsql/field_names", "/select/logsql/stream_field_names", "/select/logsql/field_values", "/select/logsql/stream_field_values":
		if !s.data {
			return `{"values":[]}`
		}
		return `{"values":[{"value":"app","hits":1},{"value":"level","hits":1}]}`
	}
	return `{}`
}

// routeRecord is what one run did.
type routeRecord struct {
	Status int
	// Calls are the set of VictoriaLogs requests, sorted (parallel fetches start in any order).
	Calls []string
	// Steps are the set of the route's operations and rewrites, sorted; Cache the set of its cache reads.
	Steps []string
	Cache []string
	// Started is the VictoriaLogs calls the request started (X-Loki-VL-Backend-Calls; in a prediction, the calls
	// the route sends), Sent the requests the stub answered for it: a request sent twice counts twice in both, and
	// the header counts a streamed answer's calls only up to its headers.
	Started, Sent int
	// queryShape marks a prediction whose calls hold each request's route shape (routeCallShape), not its whole
	// query: equal compares the recorded calls' shapes with them.
	queryShape bool
}

// gated is what the route_parity digests hold: the status and the route's decisions. The VictoriaLogs calls and
// the cache reads are not in it: on the concurrent probe routes (the json and logfmt metric risk probes, the
// detected_level derivation) the first answer cancels the other calls, so which of them reach VictoriaLogs, and
// which cache reads run, varies run to run (measured 2026-11-08: 18-28 of 24,300 runs differ between two runs;
// holding each answer or keeping only uncancelled calls moves the tie, not removes it). A prediction compares
// the status, the calls and the steps (equal); a metric prediction holds each call by its route shape
// (routeCallShape), and the concurrent probe routes stay delegated.
func (r routeRecord) gated() string {
	return fmt.Sprintf("status %d\n steps %s", r.Status, strings.Join(r.Steps, ", "))
}

// gatedCalls is what the calls digests hold: the calls the request started and the set of its VictoriaLogs
// requests, each whole. With deterministicProbes every probe the request starts reaches VictoriaLogs, so the set is
// the same on every run (TestPlanRouteParityCallsAreDeterministic).
func (r routeRecord) gatedCalls() string {
	return fmt.Sprintf("started %d\n calls %s", r.Started, strings.Join(r.Calls, "\n       "))
}

func (r routeRecord) String() string {
	return fmt.Sprintf("status %d\n calls %s\n steps %s\n cache %s\n vl started %d sent %d", r.Status, strings.Join(r.Calls, "\n       "), strings.Join(r.Steps, ", "), strings.Join(r.Cache, ", "), r.Started, r.Sent)
}

func (r routeRecord) equal(o routeRecord) bool {
	calls := o.Calls
	if r.queryShape && !o.queryShape {
		calls = routeCallShapes(o.Calls)
	}
	return r.Status == o.Status && r.Started == o.Started && strings.Join(r.Calls, "\n") == strings.Join(calls, "\n") && strings.Join(r.Steps, "\n") == strings.Join(o.Steps, "\n")
}

// newRouteProxy is a proxy whose VictoriaLogs is stub, behind the counter of the calls each request starts
// (X-Loki-VL-Backend-Calls). compat adds the compatibility-edge response cache the proxy binary has on by default
// (-compat-cache-enabled), for the exact repeat; the route runs keep the proxy without it, as their digests were
// recorded.
func newRouteProxy(t *testing.T, stub *routeStub, compat bool) (*Proxy, *http.ServeMux) {
	t.Helper()
	cfg := Config{BackendURL: "http://vl.invalid", Cache: cache.New(time.Minute, 1000), LogLevel: "error", DebugVLCallsHeader: true}
	if compat {
		cfg.CompatCache = cache.New(time.Minute, 1000)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.client.Transport = &vlCallCounter{inner: stub, host: p.backend.Host}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

// serveRoute serves q under the profile, marked with deterministicProbes, and returns the answer and the calls it
// started.
func serveRoute(t *testing.T, mux *http.ServeMux, q string, rangeDur time.Duration, rp routeProfile) (*httptest.ResponseRecorder, int) {
	t.Helper()
	form := url.Values{"query": {q}}
	path := "/loki/api/v1/query"
	if rp.instant {
		form.Set("time", strconv.FormatInt(routeEnd.Unix(), 10))
	} else {
		path = "/loki/api/v1/query_range"
		form.Set("start", strconv.FormatInt(routeEnd.Add(-rp.window(rangeDur)).Unix(), 10))
		form.Set("end", strconv.FormatInt(routeEnd.Unix(), 10))
		form.Set("step", strconv.FormatFloat(rp.step(rangeDur).Seconds(), 'f', -1, 64))
	}
	req := httptest.NewRequest(http.MethodGet, path+"?"+form.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	req = req.WithContext(context.WithValue(req.Context(), deterministicProbesKey{}, true))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	started, err := strconv.Atoi(rec.Header().Get(vlBackendCallsHeader))
	if err != nil {
		t.Fatalf("%q: no %s header: %v", q, vlBackendCallsHeader, err)
	}
	return rec, started
}

// runRoute serves one request on a fresh proxy (every cache cold) and records it.
func runRoute(t *testing.T, q string, rangeDur time.Duration, rp routeProfile, data bool) routeRecord {
	return runRouteOn(t, q, rangeDur, rp, &routeStub{data: data})
}

func runRouteOn(t *testing.T, q string, rangeDur time.Duration, rp routeProfile, stub *routeStub) routeRecord {
	t.Helper()
	p, mux := newRouteProxy(t, stub, false)
	defer func() { _ = p.Shutdown(context.Background()) }()
	// Locked: a call that lost a race may still log after the request.
	var logs lockedBuffer
	p.log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// Every request is slow, so its trace is kept and logged.
	p.slowQueryThreshold, p.slowQueryLogRawQueries = time.Nanosecond, true

	stub.mu.Lock()
	stub.on = true
	stub.mu.Unlock()
	rec, started := serveRoute(t, mux, q, rangeDur, rp)
	stub.mu.Lock()
	stub.on = false
	calls, sent := uniqueSorted(stub.got), len(stub.got)
	stub.mu.Unlock()

	out := routeRecord{Status: rec.Code, Calls: calls, Started: started, Sent: sent}

	for _, line := range logs.lines() {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != "slow_request" {
			continue
		}
		steps, _ := m["proxy.steps"].([]any)
		for _, s := range steps {
			step, _ := s.(map[string]any)
			switch step["kind"] {
			case "operation":
				out.Steps = append(out.Steps, fmt.Sprintf("operation %v %v", step["operation"], step["outcome"]))
			case "cache":
				out.Cache = append(out.Cache, fmt.Sprintf("%v %v", step["tier"], step["result"]))
			case "rewrite":
				out.Steps = append(out.Steps, fmt.Sprintf("rewrite %v", step["to"]))
			}
		}
	}
	out.Steps, out.Cache = uniqueSorted(out.Steps), uniqueSorted(out.Cache)
	return out
}

// runRouteVLCalls serves one request on a fresh proxy with the response caches the proxy binary has by default,
// then its exact repeat, and returns the VictoriaLogs calls each started.
func runRouteVLCalls(t *testing.T, q string, rangeDur time.Duration, rp routeProfile, data bool) (cold, repeat int) {
	t.Helper()
	p, mux := newRouteProxy(t, &routeStub{data: data}, true)
	defer func() { _ = p.Shutdown(context.Background()) }()
	_, cold = serveRoute(t, mux, q, rangeDur, rp)
	_, repeat = serveRoute(t, mux, q, rangeDur, rp)
	return cold, repeat
}

// uniqueSorted is the set of lines, sorted: parallel paths repeat a call or a cache read a varying number of times.
func uniqueSorted(lines []string) []string {
	out := append([]string(nil), lines...)
	sort.Strings(out)
	n := 0
	for i, l := range out {
		if i == 0 || l != out[n-1] {
			out[n] = l
			n++
		}
	}
	return out[:n]
}

// firstRange is the range of the plan's first range aggregation, 5m when it has none.
func firstRange(n queryplan.Node) time.Duration {
	var walk func(queryplan.Node) time.Duration
	walk = func(n queryplan.Node) time.Duration {
		switch v := n.(type) {
		case *queryplan.RangeAgg:
			return v.Range
		case *queryplan.VectorAgg:
			return walk(v.In)
		case *queryplan.Rank:
			return walk(v.In)
		case *queryplan.LabelReplace:
			return walk(v.In)
		case *queryplan.BinOp:
			if d := walk(v.L); d > 0 {
				return d
			}
			return walk(v.R)
		}
		return 0
	}
	if d := walk(n); d > 0 {
		return d
	}
	return 5 * time.Minute
}

// routePrediction is what the harness holds to the handlers: the physical plan's predicted route, or a test's
// stand-in (got is the recorded run, for those).
type routePrediction func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error)

// errRouteRulePending is a lowered plan whose route the physical route rules do not predict yet: it is delegated,
// counted under "<construct> (route rule)". Range and vector aggregations and binary operations over them have a
// route rule (lower.RouteQuery, metricRoute).
var errRouteRulePending = errors.New("route parity: no route rule for the lowered plan yet")

func nativeRoute(q string, p *queryplan.Plan, rp routeProfile, data bool, _ routeRecord) (routeRecord, error) {
	switch p.Root.(type) {
	case *queryplan.RangeAgg, *queryplan.VectorAgg, *queryplan.BinOp:
		return metricRoute(q, p, rp, data)
	}
	if _, err := lower.Plan(p, lower.Profile{}); err != nil {
		return routeRecord{}, err
	}
	return routeRecord{}, errRouteRulePending
}

type routeSection struct {
	// Rows are the plan-corpus rows that validate and build a plan; Runs are rows × profiles × stub answers.
	Rows     int      `json:"rows"`
	Runs     int      `json:"runs"`
	Profiles []string `json:"profiles"`
	// Native are the runs whose predicted route equals the recorded one; Mismatches the runs where it differs.
	Native     int `json:"native"`
	Mismatches int `json:"mismatches"`
	// DelegatedByConstruct counts, by the construct at the top of the plan, the runs no lowering predicts yet.
	DelegatedByConstruct map[string]int `json:"delegated_by_construct"`
	// Statuses counts the runs by HTTP status (reported).
	Statuses map[string]int `json:"statuses"`
	// Routes holds, per "profile/construct", a digest of every recorded run in corpus order.
	Routes map[string]string `json:"routes"`
	// Calls holds, per "profile/construct", a digest of every run's VictoriaLogs calls in corpus order: the number
	// the request started and each request whole (gatedCalls).
	Calls map[string]string `json:"calls"`
	// VLCalls counts, per "profile/construct", the VictoriaLogs calls of the runs, cold and on the exact repeat.
	VLCalls map[string]routeVLCalls `json:"vl_calls"`
}

// routeVLCalls is the VictoriaLogs calls of a group of runs, counted from X-Loki-VL-Backend-Calls: Cold on a fresh
// proxy, Repeat on the exact repeat of the request on the same proxy (a reused answer starts none), RepeatRuns the
// runs whose repeat started any.
type routeVLCalls struct {
	Runs       int `json:"runs"`
	Cold       int `json:"cold"`
	Repeat     int `json:"repeat"`
	RepeatRuns int `json:"repeat_runs"`
}

type routeRow struct {
	q         string
	plan      *queryplan.Plan
	construct string
	rangeDur  time.Duration
}

func routeRows(t *testing.T, root string, limit int) []routeRow {
	t.Helper()
	qs, err := plancorpus.Queries(root)
	if err != nil {
		t.Fatal(err)
	}
	var rows []routeRow
	for _, q := range qs {
		expr, msg := logqlpkg.ValidateLogQLExpr(q)
		if msg != "" || expr == nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(p)
		rows = append(rows, routeRow{q: q, plan: p, construct: lower.Construct(p.Root), rangeDur: firstRange(p.Root)})
		if limit > 0 && len(rows) == limit {
			break
		}
	}
	return rows
}

// runRouteParity runs the rows and returns the section, the mismatches and every recorded run by row, profile and
// stub answer.
func runRouteParity(t *testing.T, rows []routeRow, predict routePrediction) (routeSection, []string) {
	t.Helper()
	return runRouteParityCounting(t, rows, predict, false)
}

// runRouteParityCounting is runRouteParity that, when vlCalls is set, also counts every run's VictoriaLogs calls
// cold and on the exact repeat with the default response caches (runRouteVLCalls) into the section's VLCalls.
func runRouteParityCounting(t *testing.T, rows []routeRow, predict routePrediction, vlCalls bool) (routeSection, []string) {
	t.Helper()
	profiles := routeProfiles()
	sec := routeSection{Rows: len(rows), DelegatedByConstruct: map[string]int{}, Statuses: map[string]int{}, Routes: map[string]string{}, Calls: map[string]string{}, VLCalls: map[string]routeVLCalls{}}
	for _, rp := range profiles {
		sec.Profiles = append(sec.Profiles, rp.name)
	}
	digests, callDigests := map[string]io.Writer{}, map[string]io.Writer{}
	sums, callSums := map[string]interface{ Sum([]byte) []byte }{}, map[string]interface{ Sum([]byte) []byte }{}
	var problems []string
	var dump *os.File
	if f := os.Getenv("LVP_ROUTE_DUMP"); f != "" {
		dump, _ = os.Create(f)
		defer func() { _ = dump.Close() }()
	}
	for _, row := range rows {
		for _, rp := range profiles {
			for _, data := range []bool{true, false} {
				got := runRoute(t, row.q, row.rangeDur, rp, data)
				sec.Runs++
				sec.Statuses[strconv.Itoa(got.Status)]++
				key := rp.name + "/" + row.construct
				if digests[key] == nil {
					h, c := sha256.New(), sha256.New()
					digests[key], sums[key] = h, h
					callDigests[key], callSums[key] = c, c
				}
				fmt.Fprintf(digests[key], "%s\x00%v\x00%s\x01", row.q, data, got.gated())
				fmt.Fprintf(callDigests[key], "%s\x00%v\x00%s\x01", row.q, data, got.gatedCalls())
				cold, repeat := 0, 0
				if vlCalls {
					cold, repeat = runRouteVLCalls(t, row.q, row.rangeDur, rp, data)
					vc := sec.VLCalls[key]
					vc.Runs, vc.Cold, vc.Repeat = vc.Runs+1, vc.Cold+cold, vc.Repeat+repeat
					if repeat > 0 {
						vc.RepeatRuns++
					}
					sec.VLCalls[key] = vc
				}

				want, err := predict(row.q, row.plan, rp, data, got)
				var nl *lower.NotLoweredError
				class := "native"
				switch {
				case errors.As(err, &nl):
					class = nl.Construct
				case errors.Is(err, lower.ErrNotLowered):
					class = "unnamed"
				case errors.Is(err, errRouteRulePending):
					class = row.construct + " (route rule)"
				case err != nil:
					class = "mismatch"
					sec.Mismatches++
					problems = append(problems, fmt.Sprintf("%s data=%v: %q: %v", rp.name, data, row.q, err))
				case !want.equal(got):
					class = "mismatch"
					sec.Mismatches++
					problems = append(problems, fmt.Sprintf("%s data=%v: %q:\n predicted %s\n recorded  %s", rp.name, data, row.q, want, got))
				default:
					sec.Native++
				}
				if class != "native" && class != "mismatch" {
					sec.DelegatedByConstruct[class]++
				}
				if dump != nil {
					fmt.Fprintf(dump, "%s %v %q\n%s\n route %s\n", rp.name, data, row.q, got, class)
					if class == "mismatch" && err == nil {
						fmt.Fprintf(dump, " predicted started %d calls %d\n", want.Started, len(want.Calls))
					}
					if vlCalls {
						fmt.Fprintf(dump, " vl default caches cold %d repeat %d\n", cold, repeat)
					}
				}
			}
		}
	}
	for key, h := range sums {
		sec.Routes[key] = hex.EncodeToString(h.Sum(nil))[:16]
		sec.Calls[key] = hex.EncodeToString(callSums[key].Sum(nil))[:16]
	}
	return sec, problems
}

// TestPlanRouteParity holds the physical plan's routes to the handlers' on the plan corpus, and the handlers'
// routes to the ones recorded in the route_parity section.
//
// conformance: architecture-syntax-package-layering
func TestPlanRouteParity(t *testing.T) {
	if testing.Short() && os.Getenv("LVP_QUERY_PLAN_UPDATE") != "1" && os.Getenv("CI") == "" {
		t.Skip("route parity runs every corpus row on a fresh proxy; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	root := filepath.Join("..", "..")
	if routeParityRace {
		// A sample for the race detector; the digests hold the whole corpus, so they are gated without -race.
		if got, problems := runRouteParity(t, routeRows(t, root, 300), nativeRoute); got.Mismatches != 0 {
			t.Errorf("%d runs route unlike the handlers:\n%s", got.Mismatches, joinFirstRoute(problems, 10))
		}
		return
	}
	got, problems := runRouteParityCounting(t, routeRows(t, root, 0), nativeRoute, true)

	path := filepath.Join(root, plancorpus.PlanPath)
	file := map[string]json.RawMessage{}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &file); err != nil {
			t.Fatal(err)
		}
	}
	var want routeSection
	if raw, ok := file["route_parity"]; ok {
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("route parity: %d rows, %d runs, native %d, mismatches %d, statuses %v", got.Rows, got.Runs, got.Native, got.Mismatches, got.Statuses)
	if got.Mismatches != 0 {
		t.Errorf("%d runs route unlike the handlers:\n%s", got.Mismatches, joinFirstRoute(problems, 10))
	}
	if got.Native < want.Native {
		t.Errorf("native runs fell from %d to %d", want.Native, got.Native)
	}
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		file["route_parity"], _ = json.Marshal(got)
		out, err := json.MarshalIndent(file, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	keys := make([]string, 0, len(got.Routes))
	for k := range got.Routes {
		keys = append(keys, k)
	}
	for k := range want.Routes {
		if _, ok := got.Routes[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Run(k, func(t *testing.T) {
			if got.Routes[k] != want.Routes[k] {
				t.Errorf("the handlers' routes changed (digest %s, recorded %s): a route decision or a status moved (LVP_ROUTE_DUMP=<file> writes every run; compare it with main's); if intended, run LVP_QUERY_PLAN_UPDATE=1 and say why in the PR", got.Routes[k], want.Routes[k])
			}
			if got.Calls[k] != want.Calls[k] {
				t.Errorf("the handlers' VictoriaLogs calls changed (digest %s, recorded %s): a request sent to VictoriaLogs moved (LVP_ROUTE_DUMP=<file> writes every run; compare it with main's); if intended, run LVP_QUERY_PLAN_UPDATE=1 and say why in the PR", got.Calls[k], want.Calls[k])
			}
			g, w := got.VLCalls[k], want.VLCalls[k]
			switch {
			case g.Cold > w.Cold || g.Repeat > w.Repeat:
				t.Errorf("more VictoriaLogs calls: cold %d (recorded %d), exact repeat %d (recorded %d) over %d runs", g.Cold, w.Cold, g.Repeat, w.Repeat, g.Runs)
			case g != w:
				t.Errorf("VictoriaLogs calls moved to %+v (recorded %+v): run LVP_QUERY_PLAN_UPDATE=1 to record it", g, w)
			}
		})
	}
	if got.Native > want.Native {
		t.Errorf("native runs rose from %d to %d: run LVP_QUERY_PLAN_UPDATE=1 to record it", want.Native, got.Native)
	}
}

// routeProbeQueries are queries whose runs send the probes that race in the handlers: the json and logfmt
// parse-risk probes (a `| limit 1` existence check each), the checks beside an ordered-JSON stats attempt, the
// detected_level presence probe and the working-parser probe.
var routeProbeQueries = []string{
	`sum by (level, detected_level) (count_over_time({env="production"} | logfmt | drop __error__ [1m]))`,
	`sum by (service_name) (count_over_time({env="production"} | json | __error__="" [5m]))`,
	`sum by (detected_level) (count_over_time({app="json-level-probe"} | json | drop __error__ [1m]))`,
	`sum(count_over_time({env="production"} | detected_level="info" | json | logfmt | drop __error__, __error_details__ [1h]))`,
}

func routeProbeRows(t *testing.T) []routeRow {
	t.Helper()
	rows := make([]routeRow, len(routeProbeQueries))
	for i, q := range routeProbeQueries {
		rows[i] = routeRowOf(t, q)
	}
	return rows
}

// routeProbeCall reports whether a recorded VictoriaLogs request is a probe: a `| limit 1` existence check (the
// parse-risk and level probes), the detected_level presence probe (one count per level field) or the working-parser
// probe (the 25 newest lines).
func routeProbeCall(call string) bool {
	path, raw, _ := strings.Cut(call, "?")
	form, err := url.ParseQuery(raw)
	if err != nil || path != "/select/logsql/query" {
		return false
	}
	q := form.Get("query")
	return strings.HasSuffix(q, "| limit 1") || strings.Contains(q, " as p0,") || form.Get("limit") == "25"
}

// The gate fails a wrong prediction and passes a right one, on the probe routes too, and a row's run records the
// same route and the same VictoriaLogs calls twice.
//
// conformance: architecture-syntax-package-layering
func TestPlanRouteParityCatchesAWrongRoute(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	probes := routeProbeRows(t)
	rows := append(routeRows(t, filepath.Join("..", ".."), 40), probes...)
	echo := func(_ string, _ *queryplan.Plan, _ routeProfile, _ bool, got routeRecord) (routeRecord, error) {
		return got, nil
	}
	sec, problems := runRouteParity(t, rows, echo)
	t.Run("the recorded route is native", func(t *testing.T) {
		if sec.Runs == 0 || sec.Native != sec.Runs || sec.Mismatches != 0 {
			t.Fatalf("echo: %+v\n%s", sec, joinFirstRoute(problems, 5))
		}
	})
	t.Run("a dropped VictoriaLogs request is a mismatch", func(t *testing.T) {
		drop := func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error) {
			if len(got.Calls) > 0 {
				got.Calls = got.Calls[1:]
			} else {
				got.Calls = []string{"/select/logsql/query?query=%2A"}
			}
			return got, nil
		}
		if bad, _ := runRouteParity(t, rows, drop); bad.Native != 0 || bad.Mismatches != sec.Runs {
			t.Fatalf("drop: %+v", bad)
		}
	})
	t.Run("a different route step is a mismatch", func(t *testing.T) {
		step := func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error) {
			got.Steps = append(append([]string(nil), got.Steps...), "operation raw_rows probe")
			return got, nil
		}
		if bad, _ := runRouteParity(t, rows, step); bad.Native != 0 || bad.Mismatches != sec.Runs {
			t.Fatalf("step: %+v", bad)
		}
	})
	// A probe route is native when its prediction holds every probe the handlers sent, and only then.
	probeRuns := 0
	probeSec, _ := runRouteParity(t, probes, func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error) {
		if slices.ContainsFunc(got.Calls, routeProbeCall) {
			probeRuns++
		}
		return got, nil
	})
	t.Run("a probe route is native", func(t *testing.T) {
		if probeRuns == 0 || probeSec.Native != probeSec.Runs {
			t.Fatalf("%d runs with a probe: %+v", probeRuns, probeSec)
		}
	})
	t.Run("a dropped probe is a mismatch", func(t *testing.T) {
		drop := func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error) {
			if i := slices.IndexFunc(got.Calls, routeProbeCall); i >= 0 {
				got.Calls = slices.Delete(slices.Clone(got.Calls), i, i+1)
				got.Started--
			}
			return got, nil
		}
		if bad, _ := runRouteParity(t, probes, drop); bad.Mismatches != probeRuns || bad.Native != bad.Runs-probeRuns {
			t.Fatalf("drop a probe from %d runs: %+v", probeRuns, bad)
		}
	})
	t.Run("a probe sent once more is a mismatch", func(t *testing.T) {
		again := func(q string, p *queryplan.Plan, rp routeProfile, data bool, got routeRecord) (routeRecord, error) {
			if slices.ContainsFunc(got.Calls, routeProbeCall) {
				got.Started++
			}
			return got, nil
		}
		if bad, _ := runRouteParity(t, probes, again); bad.Mismatches != probeRuns || bad.Native != bad.Runs-probeRuns {
			t.Fatalf("one more probe on %d runs: %+v", probeRuns, bad)
		}
	})
	t.Run("a run records the same route and calls twice", func(t *testing.T) {
		again, _ := runRouteParity(t, rows, echo)
		for k, d := range sec.Routes {
			if again.Routes[k] != d || again.Calls[k] != sec.Calls[k] {
				t.Fatalf("%s: digests %s, %s then %s, %s", k, d, sec.Calls[k], again.Routes[k], again.Calls[k])
			}
		}
	})
}

// Marked with deterministicProbes, a run sends the same VictoriaLogs requests, as many times, and takes the same
// route whatever order the stub's answers come back in: every probe of a race reaches VictoriaLogs, and the verdicts
// are read in the probes' order. The stub delays each answer by up to 3ms, so the probes finish in a different order
// on every repeat.
//
// conformance: architecture-syntax-package-layering
func TestPlanRouteParityCallsAreDeterministic(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, row := range routeProbeRows(t) {
		for _, rp := range routeProfiles() {
			for _, data := range []bool{true, false} {
				var first routeRecord
				for i := range 3 {
					got := runRouteOn(t, row.q, row.rangeDur, rp, &routeStub{data: data, jitter: true})
					if i == 0 {
						first = got
						continue
					}
					if got.gated() != first.gated() || got.gatedCalls() != first.gatedCalls() {
						t.Fatalf("%s data=%v %q: run %d differs:\n first %s\n then  %s", rp.name, data, row.q, i, first, got)
					}
				}
			}
		}
	}
}

// Marked with deterministicProbes, anyRisk runs every probe and reads the verdicts in the probes' order; unmarked,
// the first risky verdict cancels the probes still running.
//
// conformance: architecture-syntax-package-layering
func TestAnyRiskDeterministicProbes(t *testing.T) {
	errSlow := errors.New("slow probe")
	t.Run("unmarked: the first risky verdict cancels the rest", func(t *testing.T) {
		cancelled := make(chan bool, 1)
		risky, err := anyRisk(context.Background(), []func(context.Context) (bool, error){
			func(ctx context.Context) (bool, error) {
				select {
				case <-ctx.Done():
					cancelled <- true
					return false, ctx.Err()
				case <-time.After(time.Minute):
					cancelled <- false
					return false, errSlow
				}
			},
			func(context.Context) (bool, error) { return true, nil },
		})
		if !risky || err != nil || !<-cancelled {
			t.Fatalf("risky %v, err %v: want the fast risky verdict and the slow probe cancelled", risky, err)
		}
	})
	t.Run("marked: every probe runs, read in order", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), deterministicProbesKey{}, true)
		var ran atomic.Int32
		risky, err := anyRisk(ctx, []func(context.Context) (bool, error){
			func(ctx context.Context) (bool, error) { return false, nil },
			func(ctx context.Context) (bool, error) {
				time.Sleep(20 * time.Millisecond)
				ran.Add(1)
				if ctx.Err() != nil {
					return false, ctx.Err()
				}
				return false, errSlow
			},
			func(context.Context) (bool, error) { ran.Add(1); return true, nil },
		})
		if !errors.Is(err, errSlow) || risky || ran.Load() != 2 {
			t.Fatalf("risky %v, err %v, %d probes ran: want the slow probe's error, read before the fast risky verdict", risky, err, ran.Load())
		}
		if risky, err := anyRisk(ctx, []func(context.Context) (bool, error){
			func(context.Context) (bool, error) { return false, nil },
			func(context.Context) (bool, error) { return false, nil },
		}); risky || err != nil {
			t.Fatalf("no risky probe: risky %v, err %v", risky, err)
		}
	})
}

func joinFirstRoute(lines []string, n int) string {
	if len(lines) > n {
		return strings.Join(lines[:n], "\n") + fmt.Sprintf("\n… and %d more", len(lines)-n)
	}
	return strings.Join(lines, "\n")
}
