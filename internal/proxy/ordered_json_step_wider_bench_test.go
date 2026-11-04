package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// memoVL answers each distinct request once from the recording fake and
// replays it afterwards, so a benchmark measures the proxy, not the fake. It
// counts the requests by kind and the raw rows the proxy reads.
type memoVL struct {
	srv   *httptest.Server
	mu    sync.Mutex
	memo  map[string][]byte
	calls map[string]int
	rows  int
	bytes int
}

func newMemoVL(b testing.TB, inner string) *memoVL {
	m := &memoVL{memo: map[string][]byte{}, calls: map[string]int{}}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		key := r.URL.Path + "?" + r.Form.Encode()
		m.mu.Lock()
		body, ok := m.memo[key]
		m.mu.Unlock()
		if !ok {
			resp, err := http.PostForm(inner+r.URL.Path, r.Form)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			m.mu.Lock()
			m.memo[key] = body
			m.mu.Unlock()
		}
		kind := strings.TrimPrefix(r.URL.Path, "/select/logsql/")
		if kind == "query" && strings.HasSuffix(r.Form.Get("query"), " | limit 1") {
			kind = "probe"
		}
		m.mu.Lock()
		m.calls[kind]++
		m.bytes += len(body)
		if kind == "query" {
			m.rows += bytes.Count(body, []byte("\n"))
		}
		m.mu.Unlock()
		_, _ = w.Write(body)
	}))
	b.Cleanup(m.srv.Close)
	return m
}

func (m *memoVL) reset() {
	m.mu.Lock()
	m.calls, m.rows, m.bytes = map[string]int{}, 0, 0
	m.mu.Unlock()
}

// BenchmarkOrderedJSONStepWiderThanRange measures DLV3 (`sum by
// (detected_level)` over `| json [1m]`) and a control set of detected_level and
// `| json` metric shapes over 48h and 24h of lines (one every 20 s) at
// Grafana's steps, against the recording fake with uncached proxy responses:
// time and allocations per request, VictoriaLogs calls by kind and raw rows
// read per request.
func BenchmarkOrderedJSONStepWiderThanRange(b *testing.B) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 48*180+30; i++ {
		rows = append(rows, stepWiderRow(s0.Add(time.Duration(i)*20*time.Second), []string{"info", "error", "warn"}[i%3], 2*(i%2), i))
	}
	inner, fake := newPushdownFakeVL(b, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	for _, tc := range []struct {
		name, query string
		hours       int
		step        time.Duration
	}{
		{"DLV3/48h-step120", `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, 48, 2 * time.Minute},
		{"DLV3/24h-step120", `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, 24, 2 * time.Minute},
		{"DLV3/24h-step60", `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`, 24, time.Minute},
		{"DLV2/48h-step120", `sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`, 48, 2 * time.Minute},
		{"DLV2/24h-step60", `sum by (detected_level) (count_over_time({env="prod"} | json | __error__!="" [1m]))`, 24, time.Minute},
		{"valid-by-status/48h-step120", `sum by (status) (count_over_time({env="prod"} | json | __error__="" [1m]))`, 48, 2 * time.Minute},
		{"DLV5/48h-step120", `sum by (detected_level) (count_over_time({env="prod"} | json | drop __error__ [1m]))`, 48, 2 * time.Minute},
		{"DLV1-auto/48h-step120", `sum by (detected_level) (count_over_time({env="prod"} | json [2m]))`, 48, 2 * time.Minute},
	} {
		b.Run(tc.name, func(b *testing.B) {
			vl := newMemoVL(b, inner.URL)
			p, err := New(Config{BackendURL: vl.srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
			fake.stored = p.labelTranslator.ToVL
			end := s0.Add(time.Duration(tc.hours)*time.Hour + 10*time.Minute)
			params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(end.Add(-time.Duration(tc.hours)*time.Hour).Unix(), 10)},
				"end": {strconv.FormatInt(end.Unix(), 10)}, "step": {strconv.FormatInt(int64(tc.step/time.Second), 10)}}
			target := "/loki/api/v1/query_range?" + params.Encode()
			run := func() {
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, target, nil))
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
			}
			run() // fills the memo
			vl.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
			vl.mu.Lock()
			n := float64(b.N)
			b.ReportMetric(float64(vl.calls["stats_query_range"])/n, "stats/op")
			b.ReportMetric(float64(vl.calls["query"])/n, "rawqueries/op")
			b.ReportMetric(float64(vl.calls["probe"])/n, "probes/op")
			b.ReportMetric(float64(vl.rows)/n, "rawrows/op")
			b.ReportMetric(float64(vl.bytes)/n, "vlbytes/op")
			vl.mu.Unlock()
		})
	}
}
