package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This corpus fixes the response contract, not today's evaluator or miss count.
// In particular, fewer raw reads after a future safe cache/pushdown improvement
// must not make the window-parity test fail.
type dlvBaselineShape struct {
	name, pipeline string
	tagged, errors bool
}

var dlvBaselineShapes = []dlvBaselineShape{
	{name: "DLV1", pipeline: "| json", tagged: true},
	{name: "DLV2", pipeline: `| json | __error__!=""`, errors: true},
	{name: "DLV3", pipeline: "| json"},
}

type dlvBaselineBackend struct {
	rows                  []jsonVolumeLine
	calls                 atomic.Int64
	fail                  atomic.Bool
	mu                    sync.Mutex
	accounts, credentials []string
}

func newDLVBaselineBackend(t *testing.T, shape dlvBaselineShape, at time.Time) (*httptest.Server, *dlvBaselineBackend) {
	t.Helper()
	b := &dlvBaselineBackend{}
	for minute := -1; minute <= 6; minute++ {
		for index, second := range []int{0, 1, 59} {
			level := []string{"info", "warn"}[(minute+index+4)%2]
			msg := fmt.Sprintf(`{"level":%q,"message":"request served"}`, level)
			if shape.errors && index == 1 {
				msg = "malformed JSON fixture line"
			}
			b.rows = append(b.rows, jsonVolumeLine{at.Add(time.Duration(minute)*time.Minute + time.Duration(second)*time.Second), level, msg})
		}
	}
	// Retain the existing stats-capable fixture for routes other than the raw
	// collector. The test does not insist that an answer must use raw rows.
	fallbacks := map[[2]bool]http.Handler{}
	for _, otherAccount := range []bool{false, true} {
		for _, otherCredential := range []bool{false, true} {
			account, credential := "10", "alpha"
			if otherAccount {
				account = "20"
			}
			if otherCredential {
				credential = "beta"
			}
			var rows []jsonVolumeLine
			for i, row := range b.rows {
				if dlvBaselineIdentityKeeps(i, account, credential) {
					rows = append(rows, row)
				}
			}
			fallback, _ := newJSONVolumeFakeVL(t, rows)
			fallbacks[[2]bool{otherAccount, otherCredential}] = fallback.Config.Handler
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.calls.Add(1)
		if b.fail.Load() {
			http.Error(w, "controlled upstream failure", http.StatusServiceUnavailable)
			return
		}
		b.mu.Lock()
		b.accounts = append(b.accounts, r.Header.Get("AccountID"))
		b.credentials = append(b.credentials, r.Header.Get("X-Auth-Token"))
		b.mu.Unlock()
		if r.URL.Path != "/select/logsql/query" {
			identity := [2]bool{r.Header.Get("AccountID") == "20", r.Header.Get("X-Auth-Token") == "beta"}
			fallbacks[identity].ServeHTTP(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("backend form: %v", err)
			return
		}
		from, e1 := time.Parse(time.RFC3339Nano, r.Form.Get("start"))
		to, e2 := time.Parse(time.RFC3339Nano, r.Form.Get("end"))
		if e1 != nil || e2 != nil {
			t.Errorf("backend bounds %v: %v/%v", r.Form, e1, e2)
			return
		}
		query := r.Form.Get("query")
		unparsed := strings.Contains(query, " | filter -"+jsonObjectStartFilter)
		limit := len(b.rows)
		if pos := strings.LastIndex(query, " | limit "); pos >= 0 {
			if n, err := strconv.Atoi(query[pos+9:]); err == nil {
				limit = n
			}
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for index, row := range b.rows {
			if row.ts.Before(from) || !row.ts.Before(to) || !dlvBaselineIdentityKeeps(index, r.Header.Get("AccountID"), r.Header.Get("X-Auth-Token")) {
				continue
			}
			if unparsed && json.Valid([]byte(row.msg)) {
				continue
			}
			if limit == 0 {
				break
			}
			limit--
			_ = json.NewEncoder(w).Encode(map[string]string{"_time": row.ts.Format(time.RFC3339Nano), "_msg": row.msg, "_stream": fmt.Sprintf(`{app="api",level=%q}`, row.level)})
		}
	}))
	t.Cleanup(srv.Close)
	return srv, b
}

func dlvBaselineIdentityKeeps(index int, account, credential string) bool {
	return (account != "20" || index%2 == 0) && (credential != "beta" || index%3 != 0)
}

func newDLVBaselineProxy(t *testing.T, backend string, tier0 bool) *Proxy {
	t.Helper()
	c := cache.NewWithMaxBytes(time.Hour, 128, 8<<20)
	var edge *cache.Cache
	if tier0 {
		edge = cache.NewWithMaxBytes(time.Hour, 128, 2<<20)
	}
	p, err := New(Config{BackendURL: backend, Cache: c, CompatCache: edge, LogLevel: "error", ForwardHeaders: []string{"X-Auth-Token"}, TenantMap: map[string]TenantMapping{"team-a": {AccountID: "10", ProjectID: "0"}, "team-b": {AccountID: "20", ProjectID: "0"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		c.Close()
		if edge != nil {
			edge.Close()
		}
	})
	return p
}

func dlvBaselineRequest(shape dlvBaselineShape, from, to time.Time) (string, map[string]string) {
	q := fmt.Sprintf(`sum by (detected_level) (count_over_time({app="api"} %s [1m]))`, shape.pipeline)
	params := url.Values{"query": {q}, "start": {strconv.FormatInt(from.UnixNano(), 10)}, "end": {strconv.FormatInt(to.UnixNano(), 10)}, "step": {"60"}}
	headers := map[string]string{"X-Scope-OrgID": "team-a", "X-Auth-Token": "alpha"}
	if shape.tagged {
		headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
	}
	return "/loki/api/v1/query_range?" + params.Encode(), headers
}

type dlvBaselineMatrix map[string]map[int64]string

// Independently count selected fixture rows over Loki's (t-window,t] interval;
// do not call the production parser, window evaluator, or cache decoder.
func dlvBaselineWant(rows []jsonVolumeLine, shape dlvBaselineShape, from, to time.Time, account, credential string) dlvBaselineMatrix {
	want := dlvBaselineMatrix{}
	for at := from; !at.After(to); at = at.Add(time.Minute) {
		counts := map[string]int{}
		for i, row := range rows {
			if !dlvBaselineIdentityKeeps(i, account, credential) || !row.ts.After(at.Add(-time.Minute)) || row.ts.After(at) {
				continue
			}
			if shape.errors && json.Valid([]byte(row.msg)) {
				continue
			}
			counts[row.level]++
		}
		for level, n := range counts {
			if want[level] == nil {
				want[level] = map[int64]string{}
			}
			want[level][at.Unix()] = strconv.Itoa(n)
		}
	}
	return want
}

func dlvBaselineReadMatrix(t *testing.T, r *httptest.ResponseRecorder) dlvBaselineMatrix {
	t.Helper()
	if r.Code != http.StatusOK || r.Header().Get(drilldownPartialUpstreamStatusHeader) != "" {
		t.Fatalf("not a complete success: %d %s %s", r.Code, r.Header(), r.Body)
	}
	var body struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][]json.RawMessage
			}
		}
		Warnings []string
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil || body.Status != "success" || body.Data.ResultType != "matrix" || len(body.Warnings) != 0 {
		t.Fatalf("invalid complete matrix: %v %s", err, r.Body)
	}
	got := dlvBaselineMatrix{}
	for _, series := range body.Data.Result {
		if len(series.Metric) != 1 || series.Metric["detected_level"] == "" {
			t.Fatalf("unexpected labels: %v", series.Metric)
		}
		level := series.Metric["detected_level"]
		if got[level] != nil {
			t.Fatalf("duplicate level %q", level)
		}
		got[level] = map[int64]string{}
		for _, point := range series.Values {
			if len(point) != 2 {
				t.Fatal("invalid point")
			}
			var ts int64
			var value string
			if json.Unmarshal(point[0], &ts) != nil || json.Unmarshal(point[1], &value) != nil {
				t.Fatalf("invalid sample: %s", point)
			}
			if _, ok := got[level][ts]; ok {
				t.Fatalf("duplicate timestamp %d", ts)
			}
			got[level][ts] = value
		}
	}
	return got
}

func TestDLVCacheBaselineWindowParity(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, shape := range dlvBaselineShapes {
		for _, tier0 := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tier0=%t", shape.name, tier0), func(t *testing.T) {
				backend, fixture := newDLVBaselineBackend(t, shape, base)
				p := newDLVBaselineProxy(t, backend.URL, tier0)
				for _, move := range []time.Duration{0, 0, -time.Minute, time.Minute} {
					from, to := base.Add(time.Minute+move), base.Add(3*time.Minute+move)
					target, headers := dlvBaselineRequest(shape, from, to)
					got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
					want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
					if len(want) == 0 || !reflect.DeepEqual(got, want) {
						t.Fatalf("move=%s\ngot %v\nwant %v", move, got, want)
					}
				}
			})
		}
	}
}

func TestDLVCacheBaselineCompleteTier0Repeat(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, shape := range dlvBaselineShapes {
		t.Run(shape.name, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, true)
			target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
			want := dlvBaselineWant(fixture.rows, shape, base.Add(time.Minute), base.Add(3*time.Minute), "10", "alpha")
			first := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
			before := fixture.calls.Load()
			second := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
			if len(first) == 0 || !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) || fixture.calls.Load() != before {
				t.Fatalf("complete repeat: first=%v second=%v want=%v backend calls %d -> %d", first, second, want, before, fixture.calls.Load())
			}
		})
	}
}

func TestDLVCacheBaselineIsolationAndFailureRecovery(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[2]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, true)
	target, _ := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
	keys := map[string]bool{}
	var firstIdentity dlvBaselineMatrix
	for _, identity := range []struct{ tenant, account, auth, tag string }{{"team-a", "10", "alpha", ""}, {"team-b", "20", "alpha", ""}, {"team-a", "10", "beta", ""}, {"team-a", "10", "alpha", "Source=grafana-lokiexplore-app"}} {
		headers := map[string]string{"X-Scope-OrgID": identity.tenant, "X-Auth-Token": identity.auth, "X-Query-Tags": identity.tag}
		got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
		want := dlvBaselineWant(fixture.rows, shape, base.Add(time.Minute), base.Add(3*time.Minute), identity.account, identity.auth)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("identity %+v: got %v want %v", identity, got, want)
		}
		if firstIdentity == nil {
			firstIdentity = want
		} else if (identity.tenant == "team-b" || identity.auth == "beta") && reflect.DeepEqual(firstIdentity, want) {
			t.Fatal("fixture does not distinguish tenant/auth identities")
		}
		r := httptest.NewRequest("GET", target, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		key, ok := p.compatCacheKey("query_range", r)
		if !ok || keys[key] {
			t.Fatalf("Loki-facing identity/profile key collision: %+v", identity)
		}
		keys[key] = true
	}
	// Start with cold caches: future extent reuse or read-ahead may legitimately
	// cover a new window in the previous proxy.
	p = newDLVBaselineProxy(t, backend.URL, true)
	target, headers := dlvBaselineRequest(shape, base.Add(2*time.Minute), base.Add(4*time.Minute))
	fixture.fail.Store(true)
	failed := doCompatProxyRequest(p, target, headers)
	if failed.Code < 400 {
		t.Fatalf("backend failure became success: %d %s", failed.Code, failed.Body)
	}
	fixture.fail.Store(false)
	p.rangeMetricRowLimit = 1
	limited := doCompatProxyRequest(p, target, headers)
	want := dlvBaselineWant(fixture.rows, shape, base.Add(2*time.Minute), base.Add(4*time.Minute), "10", "alpha")
	if limited.Code == http.StatusOK {
		// A future safe cached/pushdown answer need not fetch raw rows. If it
		// succeeds under this raw-fetch limit, it must still be complete.
		if got := dlvBaselineReadMatrix(t, limited); !reflect.DeepEqual(got, want) {
			t.Fatalf("limited response is incomplete: got %v want %v", got, want)
		}
	} else if limited.Code < 400 || strings.Contains(limited.Body.String(), `"result"`) {
		t.Fatalf("row overflow became partial data: %d %s", limited.Code, limited.Body)
	}
	p.rangeMetricRowLimit = 100
	got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failed response poisoned retry: got %v want %v", got, want)
	}
	fixture.mu.Lock()
	accounts, credentials := append([]string(nil), fixture.accounts...), append([]string(nil), fixture.credentials...)
	fixture.mu.Unlock()
	if !slices.Contains(accounts, "20") || !slices.Contains(credentials, "beta") {
		t.Fatalf("identity not forwarded: account=%v auth=%v", accounts, credentials)
	}

	// Loki matrices and VL bucket payloads may share storage, never identities.
	r := httptest.NewRequest("GET", target, nil)
	r.Header.Set("X-Scope-OrgID", "team-a")
	lokiKey, _ := p.compatCacheKey("query_range", r)
	r = p.injectAuthFingerprint(p.withRequestScope(r))
	params := url.Values{"query": {`app:="api" | stats count() as c`}, "start": {base.Format(time.RFC3339Nano)}, "end": {base.Add(5 * time.Minute).Format(time.RFC3339Nano)}, "step": {"1m"}}
	vlReq, ok := p.statsExtentRequestFor(r.Context(), params)
	if !ok {
		t.Fatal("VL bucket request not cacheable")
	}
	vlKey := vlReq.chunkKey(statsExtentFloorDiv(vlReq.first, vlReq.chunk))
	if lokiKey == "" || lokiKey == vlKey {
		t.Fatal("Loki and VL namespaces collide")
	}
	shared := cache.NewWithMaxBytes(time.Hour, 10, 1<<20)
	defer shared.Close()
	lokiBody := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	vlBody, err := encodeStatsExtentChunk(&statsExtentChunk{from: vlReq.first, to: vlReq.first, readAt: base.UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeStatsExtentChunk(vlBody); err != nil || json.Valid(vlBody) {
		t.Fatalf("VL fixture is not a valid distinct bucket payload: %v", err)
	}
	if _, err := decodeStatsExtentChunk(lokiBody); err == nil {
		t.Fatal("VL bucket decoder accepted a Loki response")
	}
	shared.SetWithTTL(lokiKey, lokiBody, time.Hour)
	shared.SetWithTTL(vlKey, vlBody, time.Hour)
	if got, ok := shared.Get(lokiKey); !ok || string(got) != string(lokiBody) {
		t.Fatal("VL payload replaced Loki payload")
	}
	if got, ok := shared.Get(vlKey); !ok || string(got) != string(vlBody) {
		t.Fatal("Loki payload replaced VL payload")
	}
}
