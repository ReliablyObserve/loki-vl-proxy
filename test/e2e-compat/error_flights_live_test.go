//go:build e2e

package e2e_compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The request flight and error cache tests run against their own proxy
// replica (e2e-proxy-errflight: -debug-vl-calls-header,
// -max-stats-query-series=1, -backend-timeout=3s, shipped error cache
// defaults), so the VictoriaLogs calls they count are their own. The outage
// tests stop and pause the stack's VictoriaLogs container: they run only with
// LVP_E2E_OUTAGE=1, in the error-flights CI group, after the other tests of
// this file.

var (
	errFlightURL = envOr("ERRFLIGHT_PROXY_URL", "http://localhost:13120")
	vlContainer  = envOr("E2E_VL_CONTAINER", "e2e-victorialogs")
)

const (
	efBackendCallsHeader = "X-Loki-VL-Backend-Calls"
	efCacheResultHeader  = "X-Loki-VL-Cache-Result"
)

type efAnswer struct {
	code   int
	body   []byte
	header http.Header
	err    error
}

// calls is the VictoriaLogs calls the request reported (-debug-vl-calls-header).
func (a efAnswer) calls() int {
	n, _ := strconv.Atoi(a.header.Get(efBackendCallsHeader))
	return n
}

// data is a success body without its stats, which every request gets for itself.
func (a efAnswer) data() string {
	body, _, _ := strings.Cut(string(a.body), `,"stats":`)
	return body
}

func efDo(t *testing.T, ctx context.Context, base, method, path string, params url.Values, tenant string) efAnswer {
	t.Helper()
	var req *http.Request
	if method == http.MethodPost {
		req, _ = http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(params.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req, _ = http.NewRequestWithContext(ctx, method, base+path+"?"+params.Encode(), nil)
	}
	req.Header.Set("X-Scope-OrgID", tenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return efAnswer{err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return efAnswer{code: resp.StatusCode, body: body, header: resp.Header, err: err}
}

func efGet(t *testing.T, params url.Values) efAnswer {
	t.Helper()
	return efDo(t, context.Background(), errFlightURL, http.MethodGet, "/loki/api/v1/query_range", params, "0")
}

// efBackendCalls is the VictoriaLogs calls the errflight replica started (its
// upstream select calls), read from its own /metrics.
func efBackendCalls(t *testing.T) int64 {
	t.Helper()
	var n int64
	for _, line := range strings.Split(scrapeProxyMetrics(t, errFlightURL), "\n") {
		if !strings.HasPrefix(line, `loki_vl_proxy_backend_duration_seconds_count{system="vl",direction="upstream",endpoint="select_logsql_`) {
			continue
		}
		if i := strings.LastIndexByte(line, ' '); i > 0 {
			if v, err := strconv.ParseInt(line[i+1:], 10, 64); err == nil {
				n += v
			}
		}
	}
	return n
}

// efIngest writes the same lines of app to Loki and VictoriaLogs, one stream
// per level, starting at at.
func efIngest(t *testing.T, app string, at time.Time, levels []string, perLevel int) {
	t.Helper()
	for _, level := range levels {
		lines := make([]logLine, perLevel)
		for i := range lines {
			lines[i] = logLine{Msg: fmt.Sprintf("errflight %s %s line %d", app, level, i), Level: level}
		}
		pushCustomToLoki(t, at, map[string]string{"app": app, "level": level}, lines)
		pushCustomToVL(t, at, map[string]string{"app": app, "level": level}, lines, []string{"app", "level"})
	}
}

// efWindow is a settled window around at.
func efWindow(at time.Time) (string, string) {
	return strconv.FormatInt(at.Add(-10*time.Minute).Unix(), 10), strconv.FormatInt(at.Add(10*time.Minute).Unix(), 10)
}

func efLogParams(app, start, end string) url.Values {
	return url.Values{"query": {fmt.Sprintf(`{app=%q}`, app)}, "start": {start}, "end": {end}, "limit": {"100"}, "direction": {"backward"}}
}

// efWaitLines waits until the errflight replica answers app's log query with
// want lines.
func efWaitLines(t *testing.T, params url.Values, want int) efAnswer {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	probe := url.Values{}
	for k, v := range params {
		probe[k] = v
	}
	for i := 0; ; i++ {
		probe.Set("limit", strconv.Itoa(1000+i)) // a new key each time: no cached answer
		a := efGet(t, probe)
		if a.code == http.StatusOK && strings.Count(string(a.body), "errflight ") >= want {
			return a
		}
		if time.Now().After(deadline) {
			t.Fatalf("lines not visible through the proxy: %d %.200s", a.code, a.body)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func efBurst(n int, send func(i int) efAnswer) []efAnswer {
	out := make([]efAnswer, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out[i] = send(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return out
}

// Fifty identical successful requests at once send the calls of one request;
// every answer carries the same data, the data the stack's main proxy and
// Loki answer.
//
// conformance: semantics/error-coalescing-request-flights
func TestErrorFlightsLive_ConcurrentIdenticalSuccessesShareOneCall(t *testing.T) {
	waitForReady(t, errFlightURL+"/ready", 60*time.Second)
	app := fmt.Sprintf("errflight-ok-%d", time.Now().UnixNano())
	at := time.Now().Add(-5 * time.Minute)
	efIngest(t, app, at, []string{"info"}, 20)
	start, end := efWindow(at)
	params := efLogParams(app, start, end)
	efWaitLines(t, params, 20)

	before := efBackendCalls(t)
	answers := efBurst(50, func(int) efAnswer { return efGet(t, params) })
	calls := efBackendCalls(t) - before
	one := efGet(t, func() url.Values { p := efLogParams(app, start, end); p.Set("limit", "99"); return p }())
	for _, a := range answers {
		if a.err != nil || a.code != http.StatusOK || a.data() != answers[0].data() {
			t.Fatalf("answer %d %v, want the same 200 for every request", a.code, a.err)
		}
	}
	if calls > int64(one.calls()) {
		t.Fatalf("50 identical requests sent %d VictoriaLogs calls, one request sends %d", calls, one.calls())
	}
	t.Logf("50 identical successes: %d VictoriaLogs calls (one request: %d)", calls, one.calls())
	// The stack's main proxy runs another label profile: the lines, not the
	// stream labels, are compared.
	main := efDo(t, context.Background(), proxyURL, http.MethodGet, "/loki/api/v1/query_range", params, "0")
	if main.code != http.StatusOK || efLines(t, main.body) != efLines(t, answers[0].body) {
		t.Fatalf("the main proxy answers %d with other lines:\nmain  %s\nproxy %s", main.code, efLines(t, main.body), efLines(t, answers[0].body))
	}
	loki := efDo(t, context.Background(), lokiURL, http.MethodGet, "/loki/api/v1/query_range", params, "0")
	if loki.code != http.StatusOK || efLines(t, loki.body) != efLines(t, answers[0].body) {
		t.Fatalf("Loki answers other lines:\nloki  %s\nproxy %s", efLines(t, loki.body), efLines(t, answers[0].body))
	}
}

// efLines is the lines and timestamps of a streams answer, sorted.
func efLines(t *testing.T, body []byte) string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode %.200s: %v", body, err)
	}
	var lines []string
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			lines = append(lines, v[0]+" "+v[1])
		}
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// efSeriesLimit is a metric over three series, over the replica's series
// limit of 1: Loki's data-dependent series-limit error.
func efSeriesLimit(app, start, end string) url.Values {
	return url.Values{"query": {fmt.Sprintf(`sum by (level) (count_over_time({app=%q}[1m]))`, app)}, "start": {start}, "end": {end}, "step": {"60"}}
}

// efWaitSeriesLimit waits until VictoriaLogs' stats see app's series, so the
// metric answers Loki's series limit; each try uses another step from
// firstStep, so no answer of an earlier try is reused.
func efWaitSeriesLimit(t *testing.T, app, start, end string, firstStep int) efAnswer {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for step := firstStep; ; step++ {
		p := efSeriesLimit(app, start, end)
		p.Set("step", strconv.Itoa(step))
		a := efGet(t, p)
		if a.code == http.StatusBadRequest && strings.Contains(string(a.body), "maximum number of series (1) reached for a single query") {
			return a
		}
		if time.Now().After(deadline) {
			t.Fatalf("want Loki's series limit, got %d %.300s", a.code, a.body)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Fifty identical requests over the series limit send the calls of one; every
// answer is the same Loki series-limit error byte for byte; a repeat is the
// held answer with no call until the data TTL (5 s) passes.
//
// conformance: semantics/error-cache-classes-and-ttl, semantics/error-cache-byte-equal-answers
func TestErrorFlightsLive_SeriesLimitHeldForDataTTL(t *testing.T) {
	waitForReady(t, errFlightURL+"/ready", 60*time.Second)
	app := fmt.Sprintf("errflight-limit-%d", time.Now().UnixNano())
	at := time.Now().Add(-5 * time.Minute)
	efIngest(t, app, at, []string{"info", "warn", "error"}, 5)
	start, end := efWindow(at)
	efWaitLines(t, efLogParams(app, start, end), 15)
	params := efSeriesLimit(app, start, end)

	one := efWaitSeriesLimit(t, app, start, end, 117)
	before := efBackendCalls(t)
	answers := efBurst(50, func(int) efAnswer { return efGet(t, params) })
	calls := efBackendCalls(t) - before
	for _, a := range answers {
		if a.code != http.StatusBadRequest || !bytes.Equal(a.body, answers[0].body) ||
			a.header.Get("Content-Type") != one.header.Get("Content-Type") || a.header.Get("X-Content-Type-Options") != one.header.Get("X-Content-Type-Options") {
			t.Fatalf("answer %d %q differs from %q", a.code, a.body, answers[0].body)
		}
	}
	if calls > int64(one.calls()) {
		t.Fatalf("50 identical failing requests sent %d VictoriaLogs calls, one request sends %d", calls, one.calls())
	}
	t.Logf("50 identical requests over the series limit: %d VictoriaLogs calls (one request: %d)", calls, one.calls())
	held := efGet(t, params)
	if held.calls() != 0 || held.header.Get(efCacheResultHeader) != "hit" || !bytes.Equal(held.body, answers[0].body) {
		t.Fatalf("repeat: %d calls, cache result %q", held.calls(), held.header.Get(efCacheResultHeader))
	}
	time.Sleep(5500 * time.Millisecond)
	if again := efGet(t, params); again.calls() == 0 {
		t.Fatal("after the data TTL the request must ask VictoriaLogs again")
	}
}

// A held failure answers only its own request: another tenant, query, step,
// limit, direction, route or method asks VictoriaLogs itself; GET and POST of
// the same request answer the same bytes.
//
// conformance: semantics/error-cache-tenant-isolation
func TestErrorFlightsLive_HeldFailureIsolation(t *testing.T) {
	waitForReady(t, errFlightURL+"/ready", 60*time.Second)
	app := fmt.Sprintf("errflight-iso-%d", time.Now().UnixNano())
	at := time.Now().Add(-5 * time.Minute)
	efIngest(t, app, at, []string{"info", "warn"}, 5)
	start, end := efWindow(at)
	efWaitLines(t, efLogParams(app, start, end), 10)
	efWaitSeriesLimit(t, app, start, end, 217)
	base := efSeriesLimit(app, start, end)
	base.Set("limit", "100")
	base.Set("direction", "backward")
	first := efGet(t, base)
	if first.code != http.StatusBadRequest {
		t.Fatalf("want the series limit, got %d %.200s", first.code, first.body)
	}
	if held := efGet(t, base); held.calls() != 0 {
		t.Fatal("the request itself must be answered by its held failure")
	}
	variant := func(key, value string) url.Values {
		v := url.Values{}
		for k, vs := range base {
			v[k] = vs
		}
		v.Set(key, value)
		return v
	}
	cases := []struct {
		name   string
		method string
		path   string
		params url.Values
		tenant string
	}{
		{"tenant", http.MethodGet, "/loki/api/v1/query_range", base, "1"},
		{"query", http.MethodGet, "/loki/api/v1/query_range", variant("query", fmt.Sprintf(`sum by (level) (count_over_time({app=%q} |= "line" [1m]))`, app)), "0"},
		{"step", http.MethodGet, "/loki/api/v1/query_range", variant("step", "30"), "0"},
		{"limit", http.MethodGet, "/loki/api/v1/query_range", variant("limit", "50"), "0"},
		{"direction", http.MethodGet, "/loki/api/v1/query_range", variant("direction", "forward"), "0"},
		{"route", http.MethodGet, "/loki/api/v1/query", url.Values{"query": base["query"], "time": {end}}, "0"},
		{"method", http.MethodPost, "/loki/api/v1/query_range", base, "0"},
	}
	for _, c := range cases {
		a := efDo(t, context.Background(), errFlightURL, c.method, c.path, c.params, c.tenant)
		if a.err != nil || a.calls() == 0 {
			t.Errorf("another %s was answered without its own VictoriaLogs call (%d, %v)", c.name, a.code, a.err)
		}
		if c.name == "method" && (a.code != first.code || !bytes.Equal(a.body, first.body)) {
			t.Errorf("POST answers %d %q, GET %d %q", a.code, a.body, first.code, first.body)
		}
	}
}

// docker runs a docker command on the stack's VictoriaLogs container.
func efDocker(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", append(args, vlContainer)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

func efOutageEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("LVP_E2E_OUTAGE") != "1" {
		t.Skip("stops the stack's VictoriaLogs: set LVP_E2E_OUTAGE=1 (the error-flights CI group)")
	}
}

func efWaitVL(t *testing.T) {
	t.Helper()
	waitForReady(t, vlURL+"/health", 120*time.Second)
}

// Through a VictoriaLogs outage: 50 identical requests send about one
// request's calls and answer the same failure; 20 clients refreshing one query
// every 100 ms with end=now send about one call per transient TTL (the hold
// covers every window of the query); after VictoriaLogs is back, the query
// answers 200 with the lines written after the recovery.
//
// conformance: semantics/error-coalescing-request-flights, semantics/error-cache-classes-and-ttl
func TestErrorFlightsLive_ZOutageHeldAndRecovers(t *testing.T) {
	efOutageEnabled(t)
	waitForReady(t, errFlightURL+"/ready", 60*time.Second)
	app := fmt.Sprintf("errflight-outage-%d", time.Now().UnixNano())
	at := time.Now().Add(-5 * time.Minute)
	efIngest(t, app, at, []string{"info"}, 5)
	start, end := efWindow(at)
	params := efLogParams(app, start, end)
	efWaitLines(t, params, 5)

	efDocker(t, "stop", "-t", "1")
	t.Cleanup(func() {
		efDocker(t, "start")
		efWaitVL(t)
	})
	before := efBackendCalls(t)
	burst := efBurst(50, func(int) efAnswer { return efGet(t, params) })
	burstCalls := efBackendCalls(t) - before
	for _, a := range burst {
		if a.err != nil || a.code < 500 || !bytes.Equal(a.body, burst[0].body) {
			t.Fatalf("outage answer %d %q, want one failure for all", a.code, a.body)
		}
	}
	// The stack's main proxy, in front of the same stopped VictoriaLogs,
	// answers the same failure.
	if main := efDo(t, context.Background(), proxyURL, http.MethodGet, "/loki/api/v1/query_range", params, "0"); main.code != burst[0].code || !bytes.Equal(main.body, burst[0].body) {
		t.Fatalf("the main proxy answers the outage with %d %q, the flight with %d %q", main.code, main.body, burst[0].code, burst[0].body)
	}
	burstHeader := 0
	for _, a := range burst {
		burstHeader += a.calls()
	}
	// The header counts every attempt handed to the transport, a retry
	// included; /metrics counts each call once however often it was retried,
	// background calls included.
	t.Logf("outage burst of 50: %d VictoriaLogs attempts by the requests (header, retries included), %d calls on /metrics (retries not counted, background included), status %d", burstHeader, burstCalls, burst[0].code)
	if burstHeader > 4 {
		t.Fatalf("50 identical requests during the outage made %d VictoriaLogs attempts, want one request's", burstHeader)
	}

	// Refresh storm: end=now moves on every request.
	before = efBackendCalls(t)
	stopAt := time.Now().Add(3 * time.Second)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		requests int
		byHeader int
		statuses = map[int]int{}
	)
	for c := 0; c < 20; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				now := time.Now()
				p := efLogParams(app, strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10))
				a := efGet(t, p)
				mu.Lock()
				requests++
				byHeader += a.calls()
				statuses[a.code]++
				mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	storm := efBackendCalls(t) - before
	t.Logf("3 s refresh storm, 20 clients, end=now: %d requests, %d VictoriaLogs attempts by the requests (header, retries included), %d calls on /metrics (retries not counted, background included), statuses %v", requests, byHeader, storm, statuses)
	// One request's attempts (its retries included, as the burst's leader made
	// them) per transient TTL: three seconds, plus an expiry on each edge.
	perRequest := max(burstHeader, 1)
	if byHeader > 5*perRequest {
		t.Fatalf("a 3 s refresh storm of 20 clients made %d VictoriaLogs attempts, want about %d per second", byHeader, perRequest)
	}

	efDocker(t, "start")
	efWaitVL(t)
	recovered := time.Now()
	fresh := time.Now()
	efIngest(t, app, fresh, []string{"info"}, 1)
	for {
		now := time.Now()
		p := efLogParams(app, strconv.FormatInt(fresh.Add(-time.Hour).Unix(), 10), strconv.FormatInt(now.Add(time.Minute).Unix(), 10))
		p.Set("limit", "1000")
		a := efGet(t, p)
		if a.code == http.StatusOK && strings.Count(string(a.body), "errflight ") >= 6 {
			t.Logf("first 200 with the new line %v after VictoriaLogs was healthy", time.Since(recovered).Round(time.Millisecond))
			break
		}
		// The circuit breaker stays open up to -cb-open-duration (10 s) after
		// the failures; a held failure adds at most its 1 s TTL.
		if time.Since(recovered) > 20*time.Second {
			t.Fatalf("no 200 with the new data 20 s after recovery: %d %.200s", a.code, a.body)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// With VictoriaLogs paused, a leader whose client gives up hands its followers
// nothing: they start again as a new leader and answer 200 once VictoriaLogs
// resumes, never the leader's cancellation; a follower that gives up gets its
// own 499 while the leader goes on. A timeout is never held: the repeat asks
// VictoriaLogs again.
//
// conformance: semantics/error-coalescing-no-inherited-cancel
func TestErrorFlightsLive_ZPausedLeaderCancelFollowersRetry(t *testing.T) {
	efOutageEnabled(t)
	waitForReady(t, errFlightURL+"/ready", 60*time.Second)
	app := fmt.Sprintf("errflight-pause-%d", time.Now().UnixNano())
	at := time.Now().Add(-5 * time.Minute)
	efIngest(t, app, at, []string{"info"}, 3)
	start, end := efWindow(at)
	efWaitLines(t, efLogParams(app, start, end), 3)
	params := efLogParams(app, start, end)
	params.Set("limit", "77")

	efDocker(t, "pause")
	paused := true
	t.Cleanup(func() {
		if paused {
			efDocker(t, "unpause")
		}
		efWaitVL(t)
	})
	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelLeader()
	leader := make(chan efAnswer, 1)
	go func() {
		leader <- efDo(t, leaderCtx, errFlightURL, http.MethodGet, "/loki/api/v1/query_range", params, "0")
	}()
	time.Sleep(150 * time.Millisecond)
	quitter, quit := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer quit()
	followers := make(chan []efAnswer, 1)
	go func() {
		followers <- efBurst(6, func(i int) efAnswer {
			ctx := context.Background()
			if i == 0 {
				ctx = quitter // a follower whose client gives up
			}
			return efDo(t, ctx, errFlightURL, http.MethodGet, "/loki/api/v1/query_range", params, "0")
		})
	}()
	if l := <-leader; l.err == nil {
		t.Fatalf("the leader's client should have given up, got %d", l.code)
	}
	time.Sleep(500 * time.Millisecond) // the quitting follower is gone too
	efDocker(t, "unpause")
	paused = false
	for i, a := range <-followers {
		if i == 0 {
			if a.err == nil {
				t.Fatalf("the quitting follower should have given up, got %d", a.code)
			}
			continue
		}
		if a.err != nil || a.code != http.StatusOK || strings.Count(string(a.body), "errflight ") != 3 {
			t.Fatalf("follower %d answered %d %v %.200s, want the new leader's 200 with the lines", i, a.code, a.err, a.body)
		}
	}

	// A timeout is never held: VictoriaLogs paused past -backend-timeout (3 s).
	efDocker(t, "pause")
	paused = true
	timeout := efLogParams(app, start, end)
	timeout.Set("limit", "78")
	first := efGet(t, timeout)
	if first.code < 500 {
		t.Fatalf("want a timeout failure, got %d", first.code)
	}
	if repeat := efGet(t, timeout); repeat.calls() == 0 {
		t.Fatal("a timed-out request must not be held: the repeat asks VictoriaLogs")
	}
	efDocker(t, "unpause")
	paused = false
}
