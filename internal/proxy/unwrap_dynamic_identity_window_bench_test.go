package proxy

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// BenchmarkDynamicIdentityWindows measures a parser's series identity over a range longer than the step (a [5m]
// range at a 1m step over 1h) through the real handlers, against VictoriaLogs' own answers: the vlemu emulator
// answers each distinct request once (2,000 json lines over the window), and the benchmark loop serves those answers
// again, so the proxy's work is what is measured (the request, the bucket merge per series, the fold), not the
// emulator's. unique has a series per line (a request id on every line); dup has 40 series of 50 lines each. The
// answers are the ones the proxy reads on each build: the stream's or group's buckets on base, the series hash's
// buckets on this branch.
func BenchmarkDynamicIdentityWindows(b *testing.B) {
	b.Run("unique", func(b *testing.B) { benchDynamicIdentityWindows(b, 2000) })
	b.Run("dup", func(b *testing.B) { benchDynamicIdentityWindows(b, 40) })
}

func benchDynamicIdentityWindows(b *testing.B, series int) {
	const rows = 2000
	end := time.Unix(1_800_000_600, 0)
	var emuRows []vlemu.Row
	for i := 0; i < rows; i++ {
		ts := end.Add(-time.Duration(rows-i) * 1800 * time.Millisecond).Add(250 * time.Millisecond)
		req := "r" + strconv.Itoa(i%series)
		v := float64(i % 500)
		msg := `{"n":` + strconv.Itoa(i%500) + `,"req":"` + req + `"}`
		emuRows = append(emuRows, vlemu.Row{TS: ts.UnixNano(), Stream: `{app="x",pod="p` + strconv.Itoa(i%4) + `"}`,
			Labels: map[string]string{"req": req}, Value: v, Msg: msg})
	}
	emu := &vlemu.Emulator{T: b, Rows: emuRows, Field: "n"}
	inner := emu.Handler()
	selector := regexp.MustCompile(`app:="x[0-9]+"`)
	var mu sync.Mutex
	answers := map[string][]byte{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		form := url.Values{}
		for k, v := range r.Form {
			form[k] = v
		}
		form.Set("query", selector.ReplaceAllString(form.Get("query"), `app:="x"`))
		key := r.URL.Path + "?" + form.Encode()
		mu.Lock()
		body, ok := answers[key]
		mu.Unlock()
		if !ok {
			req := httptest.NewRequest(r.Method, r.URL.Path+"?"+form.Encode(), nil)
			rec := httptest.NewRecorder()
			inner.ServeHTTP(rec, req)
			body = bytes.Clone(rec.Body.Bytes())
			mu.Lock()
			answers[key] = body
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer backend.Close()
	c := cache.New(time.Millisecond, 10)
	defer c.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	for _, tc := range []struct{ name, query string }{
		{"sum_by_avg", `sum by (pod) (avg_over_time({app="x"} | json | unwrap n [5m]))`},
		{"sum_max", `sum(max_over_time({app="x"} | json | unwrap n [5m]))`},
		{"avg_stddev", `avg(stddev_over_time({app="x"} | json | unwrap n [5m]))`},
		{"max_by_max", `max by (pod) (max_over_time({app="x"} | json | unwrap n [5m]))`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// A selector of its own per iteration, so no response or extent cache answers it.
				query := strings.ReplaceAll(tc.query, `{app="x"}`, `{app="x`+strconv.Itoa(i)+`"}`)
				params := url.Values{"query": {query}, "start": {"1799997000"}, "end": {"1800000600"}, "step": {"60"}}
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
			}
		})
	}
}
