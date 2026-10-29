package proxy

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// shortPodVL answers stats_query_range like VictoriaLogs does for the series
// rows of `sum without (pod) (count_over_time({env="production"}[1m]))` over
// the UI log generator's workload: short-lived pods (a new pod name per batch),
// so about 13,500 pod streams an hour, each with a few lines inside one step.
// A query with the stream cut (withSlidingWithoutCut) is answered with the
// buckets of the stream texts left once the pod pair is cut out. The answer
// for a request is rendered once and served from memory, so a benchmark
// measures the proxy, not the stub.
type shortPodVL struct {
	pods  []shortPod
	calls atomic.Int64
	rows  atomic.Int64
	mu    sync.Mutex
	cache map[string]shortPodAnswer
}

type shortPodAnswer struct {
	body []byte
	rows int
}

type shortPod struct {
	stream, cut, level string
	lines              []int64 // ns
}

var shortPodCutRE = regexp.MustCompile(`,pod="[^"]*"`)

func newShortPodVL(podsPerHour int, from, to int64, seed int64) *shortPodVL {
	rnd := rand.New(rand.NewSource(seed))
	apps := []string{"api-gateway", "payment-service", "auth-service", "nginx-ingress", "worker-service", "db-postgres", "cache-redis", "frontend-ssr", "batch-etl", "ml-serving"}
	levels := []string{"info", "info", "info", "warn", "error", "debug"}
	n := int(int64(podsPerHour) * (to - from) / int64(time.Hour))
	vl := &shortPodVL{cache: map[string]shortPodAnswer{}}
	for i := 0; i < n; i++ {
		app, level := apps[rnd.Intn(len(apps))], levels[rnd.Intn(len(levels))]
		stream := fmt.Sprintf(`{app=%q,cluster="us-east-1",container=%q,env="production",level=%q,namespace="prod",pod="%s-%05x",service_name=%q}`,
			app, strings.Split(app, "-")[0], level, app, i, app)
		born := from + rnd.Int63n(to-from)
		pod := shortPod{stream: stream, cut: shortPodCutRE.ReplaceAllString(stream, ""), level: level}
		for k := 1 + rnd.Intn(4); k > 0; k-- {
			pod.lines = append(pod.lines, born+rnd.Int63n(int64(8*time.Second)))
		}
		vl.pods = append(vl.pods, pod)
	}
	return vl
}

func (vl *shortPodVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/select/logsql/stats_query_range" {
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	vl.calls.Add(1)
	key := r.Form.Encode()
	vl.mu.Lock()
	answer, ok := vl.cache[key]
	vl.mu.Unlock()
	if !ok {
		answer = vl.render(r.Form)
		vl.mu.Lock()
		vl.cache[key] = answer
		vl.mu.Unlock()
	}
	vl.rows.Add(int64(answer.rows))
	_, _ = w.Write(answer.body)
}

func (vl *shortPodVL) render(form url.Values) shortPodAnswer {
	step, _ := time.ParseDuration(form.Get("step"))
	var off time.Duration
	if o := form.Get("offset"); o != "" {
		off, _ = time.ParseDuration(o)
	}
	startTime, _ := time.Parse(time.RFC3339Nano, form.Get("start"))
	endTime, _ := time.Parse(time.RFC3339Nano, form.Get("end"))
	start, end := startTime.UnixNano(), endTime.UnixNano()
	cut := strings.Contains(form.Get("query"), "replace_regexp")
	type group struct {
		stream, level string
		buckets       map[int64]int
	}
	groups := map[string]*group{}
	var order []string
	for _, pod := range vl.pods {
		stream := pod.stream
		if cut {
			stream = pod.cut
		}
		id := stream + "\x00" + pod.level
		for _, ts := range pod.lines {
			if ts < start || ts >= end {
				continue
			}
			g := groups[id]
			if g == nil {
				g = &group{stream: stream, level: pod.level, buckets: map[int64]int{}}
				groups[id] = g
				order = append(order, id)
			}
			// lib/logstorage/block_result.go truncateTimestamp
			b := ts + int64(off)
			b -= ((b % int64(step)) + int64(step)) % int64(step)
			g.buckets[b-int64(off)]++
		}
	}
	var sb strings.Builder
	sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, id := range order {
		g := groups[id]
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"metric":{"__name__":"c","_stream":%q,"level":%q},"values":[`, g.stream, g.level)
		ts := make([]int64, 0, len(g.buckets))
		for b := range g.buckets {
			ts = append(ts, b)
		}
		sort.Slice(ts, func(a, b int) bool { return ts[a] < ts[b] })
		for j, b := range ts {
			if j > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `[%s,"%d"]`, strconv.FormatFloat(float64(b)/1e9, 'f', -1, 64), g.buckets[b])
		}
		sb.WriteString("]}")
	}
	sb.WriteString("]}}")
	return shortPodAnswer{body: []byte(sb.String()), rows: len(order)}
}

// BenchmarkSlidingSumWithout drives the A/B shape W8, `sum without (pod)
// (count_over_time({env="production", ...}[1m]))`, with the step Grafana picks
// for the range (range != step: sliding windows) through the real handler,
// over the stub's buckets of about 13,500 pod streams an hour.
func BenchmarkSlidingSumWithout(b *testing.B) {
	const query = `sum without (pod) (count_over_time({env="production", instrumentation!="otel", app!="otel-collector"}[1m]))`
	end := int64(1_800_000_000) * int64(time.Second)
	for _, c := range []struct {
		name string
		rng  time.Duration
		step time.Duration
	}{
		{"1h_step5s", time.Hour, 5 * time.Second},
		{"6h_step10s", 6 * time.Hour, 10 * time.Second},
	} {
		b.Run(c.name, func(b *testing.B) {
			vl := newShortPodVL(13500, end-int64(c.rng)-int64(2*time.Minute), end, 1)
			backend := httptest.NewServer(vl)
			defer backend.Close()
			responseCache := cache.New(time.Millisecond, 10)
			defer responseCache.Close()
			// The A/B stack's series limit: Loki's is counted on the folded answer.
			p, err := New(Config{BackendURL: backend.URL, Cache: responseCache, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
				MaxStatsQuerySeries: 1_000_000})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = p.Shutdown(b.Context()) }()
			target := func(i int) string {
				// Trailing blanks make every request a new one for the proxy's
				// caches; the VictoriaLogs call stays the same.
				params := url.Values{"query": {query + strings.Repeat(" ", i)}}
				params.Set("start", strconv.FormatInt((end-int64(c.rng))/1e9, 10))
				params.Set("end", strconv.FormatInt(end/1e9, 10))
				params.Set("step", strconv.FormatInt(int64(c.step/time.Second), 10))
				return "/loki/api/v1/query_range?" + params.Encode()
			}
			res := doCompatProxyRequest(p, target(0), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				b.Fatalf("%d %.300s", res.Code, res.Body)
			}
			before, rowsBefore := vl.calls.Load(), vl.rows.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doCompatProxyRequest(p, target(i+1), map[string]string{"X-Scope-OrgID": "0"})
			}
			b.StopTimer()
			calls := vl.calls.Load() - before
			if calls < int64(b.N) {
				b.Fatalf("%d VictoriaLogs calls for %d requests: answered from a cache", calls, b.N)
			}
			b.ReportMetric(float64(vl.rows.Load()-rowsBefore)/float64(calls), "vl-series/call")
		})
	}
}
