package proxy

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkQuantileCounts24h: quantile_over_time(0.95, ... [1m]) by
// (service_name) over 24h at a 60 s step, VictoriaLogs' value counts held in
// memory (1,441 buckets of about 700 lines over some 1,800 distinct values, a
// 9 MB answer): the proxy's part of the E2 shape's cold request.
func BenchmarkQuantileCounts24h(b *testing.B) {
	end := time.Unix(1_800_000_000, 0).Truncate(time.Minute)
	start := end.Add(-24 * time.Hour)
	rnd := rand.New(rand.NewSource(1))
	type point struct{ ts, n int64 }
	byValue := map[int][]point{}
	var values []int
	for ts := start.Add(-time.Minute).Unix(); ts <= end.Unix(); ts += 60 {
		seen := map[int]int64{}
		for i := 0; i < 700; i++ {
			seen[int(rnd.ExpFloat64()*200)]++
		}
		for v, n := range seen {
			if byValue[v] == nil {
				values = append(values, v)
			}
			byValue[v] = append(byValue[v], point{ts, n})
		}
	}
	var buf bytes.Buffer
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, v := range values {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"metric":{"__name__":"__lvp_c","service_name":"api-gateway","__lvp_v":"%d","__lvp_bad":"0"},"values":[`, v)
		for j, p := range byValue[v] {
			if j > 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, `[%d,"%d"]`, p.ts, p.n)
		}
		buf.WriteString(`]}`)
	}
	buf.WriteString(`]}}`)
	body := buf.Bytes()
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", "v1.52.0", "v1.52.0")
		case "/select/logsql/stats_query_range":
			calls.Add(1)
			_, _ = w.Write(body)
		}
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	q := `quantile_over_time(0.95, {service_name="api-gateway"} | json | unwrap duration_ms [1m]) by (service_name)`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// A window of its own each time: no cache answers.
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(start.Unix()-int64(i%600)*60, 10)}, "end": {strconv.FormatInt(end.Unix()-int64(i%600)*60, 10)}, "step": {"60"}}
		if res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
			b.Fatalf("%d %s", res.Code, res.Body.String())
		}
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "vl-calls/op")
}

// BenchmarkRateCounterUnderSum6h: sum(rate_counter(... | logfmt | unwrap
// amount [5m])) over 6h at a 10 s step on 21,600 lines with a transaction id
// each (the U9 shape), over the emulated VictoriaLogs; vl-rows/op counts the
// rows VictoriaLogs returned to the proxy, vl-calls/op its calls.
func BenchmarkRateCounterUnderSum6h(b *testing.B) {
	end := int64(1_800_000_000) * 1e9
	start := end - int64(6*time.Hour)
	var rows []emuRow
	for i, ts := 0, start-int64(5*time.Minute); ts <= end; i, ts = i+1, ts+int64(time.Second) {
		rows = append(rows, emuRow{ts: ts + int64(i%7)*1e8, stream: fmt.Sprintf(`{app="rc",pod="p%d"}`, i%4), value: float64(i % 50),
			labels: map[string]string{"txn": "t" + strconv.Itoa(i)}, msg: fmt.Sprintf("amount=%d txn=t%d", i%50, i)})
	}
	emu := &vlEmulator{t: b, rows: rows, field: "amount", version: "v1.53.0"}
	backend := httptest.NewServer(rateCounterEmuHandler(emu))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	q := `sum(rate_counter({app="rc"} | logfmt | unwrap amount [5m]))`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(start/1e9-int64(i%60)*10, 10)}, "end": {strconv.FormatInt(end/1e9-int64(i%60)*10, 10)}, "step": {"10"}}
		if res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
			b.Fatalf("%d %.300s", res.Code, res.Body.String())
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(emu.calls))/float64(b.N), "vl-calls/op")
}
