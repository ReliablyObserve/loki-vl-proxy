package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// BenchmarkListedWindowsRequest is the proxy's cost of one cold query_range request (a fresh proxy, every cache cold)
// over an extraction list's series identity on a range longer than the step: an hour at a step of a minute over a
// range of five, against the emulated VictoriaLogs, whose answers are replayed after the first (the op times the
// proxy, not the emulator). The shapes are the ones the window evaluator reads in one chunk.
func BenchmarkListedWindowsRequest(b *testing.B) {
	end := int64(1_800_000_000) * 1e9
	streams := []string{`{app="uw",ns="x",pod="a"}`, `{app="uw",ns="x",pod="b"}`, `{app="uw",ns="y",pod="a"}`, `{app="uw",ns="y",pod="c"}`}
	routes := []string{"/x", "/y", "/z"}
	var rows []vlemu.Row
	for i := 0; i < 20_000; i++ {
		amount := float64((i*7919)%500 + 1)
		user := "u" + strconv.Itoa(i%2000)
		route := routes[(i/4)%3]
		rows = append(rows, vlemu.Row{TS: end - int64(i)*int64(330*time.Millisecond) - 300_000, Stream: streams[i%4],
			Labels: map[string]string{"user": user, "route": route}, Value: amount,
			Msg: "amount=" + strconv.FormatFloat(amount, 'f', -1, 64) + " user=" + user + " route=" + route})
	}
	emu := &vlemu.Emulator{T: b, Rows: rows, Field: "amount", Version: "v1.54.0"}
	inner := emu.Handler()
	var mu sync.Mutex
	replay := map[string][]byte{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		key := r.Method + " " + r.URL.String() + " " + string(body)
		mu.Lock()
		got, ok := replay[key]
		mu.Unlock()
		if !ok {
			r.Body = io.NopCloser(bytes.NewReader(body))
			rec := httptest.NewRecorder()
			inner.ServeHTTP(rec, r)
			if rec.Code != http.StatusOK {
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
				return
			}
			got = rec.Body.Bytes()
			mu.Lock()
			replay[key] = got
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(got)
	}))
	b.Cleanup(backend.Close)
	for _, q := range []string{
		`sum(stdvar_over_time({app="uw"} | logfmt amount | unwrap amount [5m]))`,
		`sum by (ns) (max_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`avg by (ns) (avg_over_time({app="uw"} | logfmt amount, route | unwrap amount [5m]))`,
		`sum(max_over_time({app="uw"} | logfmt amount, user | unwrap amount [5m]))`,
	} {
		target := "/loki/api/v1/query_range?" + url.Values{"query": {q}, "start": {strconv.FormatInt(end/1e9-3600, 10)},
			"end": {strconv.FormatInt(end/1e9, 10)}, "step": {"60"}}.Encode()
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N+1; i++ {
				b.StopTimer()
				p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
				if err != nil {
					b.Fatal(err)
				}
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					b.Fatal(err)
				}
				if i > 0 {
					b.StartTimer()
				}
				res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
				b.StopTimer()
				if res.Code != http.StatusOK {
					b.Fatalf("%d %.200s", res.Code, res.Body.String())
				}
				_ = p.Shutdown(context.Background())
			}
		})
	}
}
