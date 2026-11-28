package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestStructuredMetadataSeriesCost times the flag's shapes on a real VictoriaLogs (the serve oracle) over a generated
// day of OpenTelemetry-like rows: per call path, VictoriaLogs time and bytes, cold and shifted, with the flag off and
// on. LVP_SM_COST=1 runs it; LVP_SM_COST_PROFILE=<dir> writes a CPU profile of the proxy per shape.
func TestStructuredMetadataSeriesCost(t *testing.T) {
	if os.Getenv("LVP_SM_COST") != "1" {
		t.Skip("LVP_SM_COST=1 runs the cost table on VictoriaLogs")
	}
	hours := 6
	if v := os.Getenv("LVP_SM_COST_HOURS"); v != "" {
		_, _ = fmt.Sscan(v, &hours)
	}
	perHour := 10_000
	churn := time.Duration(0) // pods rotate this often (new streams), 0: three pods for good
	if v := os.Getenv("LVP_SM_COST_CHURN"); v != "" {
		churn, _ = time.ParseDuration(v)
	}
	if v := os.Getenv("LVP_SM_COST_LINES"); v != "" {
		_, _ = fmt.Sscan(v, &perHour)
	}
	end := time.Unix(1_790_000_000, 0).Truncate(time.Hour)
	start := end.Add(-time.Duration(hours)*time.Hour - 10*time.Minute)
	var rows []oraclerun.ServeRow
	for svc := 0; svc < 15; svc++ {
		name := fmt.Sprintf("svc-%02d", svc)
		lines := perHour / 10
		otel := svc < 5
		if otel {
			lines = perHour
		}
		n := int(float64(lines) * end.Sub(start).Hours())
		gap := end.Sub(start) / time.Duration(n)
		for i := 0; i < n; i++ {
			at := start.Add(time.Duration(i) * gap)
			pod := fmt.Sprintf("%s-%d", name, i%3)
			if churn > 0 {
				pod = fmt.Sprintf("%s-%d-%d", name, at.Sub(start)/churn, i%3)
			}
			stream := [][2]string{{"app", name}, {"env", "production"}, {"pod", pod}, {"service_name", name}}
			fields := [][2]string{{"_msg", fmt.Sprintf(`level=info msg="handled" duration_ms=%d`, i%97)}}
			if otel {
				fields = append(fields, [2]string{"trace_id", fmt.Sprintf("%s%08x", name, i)}, [2]string{"span_id", fmt.Sprintf("%06x", i)},
					[2]string{"service.name", name}, [2]string{"k8s.pod.name", pod})
			}
			rows = append(rows, oraclerun.ServeRow{TS: at.UnixNano(), Stream: stream, Fields: fields})
		}
	}
	t.Logf("%d rows over %d h", len(rows), hours)
	// The serve oracle reads a data set as one line of at most 256 MiB: about a million of these rows.
	srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), "", []oraclerun.ServeDataSet{{Tenant: 0, Rows: rows}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	front := &costFront{upstream: srv.URL}
	fs := httptest.NewServer(front)
	defer fs.Close()
	step := map[int]int{1: 12, 6: 25, 24: 20}[hours]
	if step == 0 {
		step = 60
	}
	shapes := []struct{ name, query string }{
		{"B2", `sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`},
		{"F2", `count by (service_name) (count_over_time({env="production"}[5m]))`},
		{"bare", `count_over_time({service_name="svc-00"}[5m])`},
		{"without", `sum without (pod) (count_over_time({service_name="svc-00"}[5m]))`},
		{"rate", `rate({service_name="svc-00"}[5m])`},
		{"stddev", `stddev by (app) (count_over_time({env="production"}[5m]))`},
		{"control", `sum by (service_name) (count_over_time({env="production"}[5m]))`},
	}
	if only := os.Getenv("LVP_SM_COST_SHAPES"); only != "" {
		var kept = shapes[:0]
		for _, s := range shapes {
			if strings.Contains(","+only+",", ","+s.name+",") {
				kept = append(kept, s)
			}
		}
		shapes = kept
	}
	profiles := os.Getenv("LVP_SM_COST_PROFILE")
	for _, s := range shapes {
		for _, on := range []bool{false, true} {
			p, err := New(Config{BackendURL: fs.URL, Cache: cache.New(time.Hour, 100_000), LogLevel: "error",
				BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, StructuredMetadataSeriesIdentity: on,
				MaxStatsQuerySeries: 500_000})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
				t.Fatal(err)
			}
			from, to := end.Add(-time.Duration(hours)*time.Hour).Unix(), end.Unix()
			for _, run := range []struct {
				label string
				shift int64
			}{{"cold", 0}, {"shift", 60}} {
				target := "/loki/api/v1/query_range?" + url.Values{"query": {s.query}, "start": {fmt.Sprint(from + run.shift)}, "end": {fmt.Sprint(to + run.shift)}, "step": {fmt.Sprint(step)}}.Encode()
				front.reset()
				var prof *os.File
				if profiles != "" && run.label == "cold" && on {
					prof, _ = os.Create(filepath.Join(profiles, fmt.Sprintf("%s-%dh.cpu", s.name, hours)))
					_ = pprof.StartCPUProfile(prof)
				}
				began := time.Now()
				got := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0", "Cache-Control": "no-cache"})
				took := time.Since(began)
				if prof != nil {
					pprof.StopCPUProfile()
					_ = prof.Close()
				}
				t.Logf("%-8s %dh on=%-5v %-5s %3d %7.3fs %9d B  VL: %s", s.name, hours, on, run.label, got.Code, took.Seconds(), got.Body.Len(), front.summary())
			}
			_ = p.Shutdown(context.Background())
		}
	}
}

// costFront passes the proxy's reads to VictoriaLogs and times them by path.
type costFront struct {
	upstream string
	mu       sync.Mutex
	calls    map[string][3]float64 // count, seconds, bytes
}

func (f *costFront) reset() {
	f.mu.Lock()
	f.calls = map[string][3]float64{}
	f.mu.Unlock()
}

func (f *costFront) summary() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.calls))
	for k := range f.calls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		c := f.calls[k]
		fmt.Fprintf(&b, "%s %d %.3fs %.0fkB; ", strings.TrimPrefix(k, "/select/logsql/"), int(c[0]), c[1], c[2]/1024)
	}
	return b.String()
}

func (f *costFront) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		_, _ = w.Write([]byte("OK"))
		return
	case "/metrics":
		_, _ = w.Write([]byte(`vm_app_version{version="victoria-logs-20260101-000000-tags-v1.54.0-0-g0000000", short_version="v1.54.0"} 1` + "\n"))
		return
	}
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	req, err := http.NewRequestWithContext(r.Context(), r.Method, f.upstream+r.URL.RequestURI(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	began := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	took := time.Since(began).Seconds()
	if os.Getenv("LVP_SM_COST_DUMP") != "" {
		q, _ := url.ParseQuery(string(body))
		if q.Get("query") == "" {
			q = r.URL.Query()
		}
		fmt.Fprintf(os.Stderr, "COSTCALL %s %.3fs %dB start=%s end=%s step=%s q=%s\n", r.URL.Path, took, len(out), q.Get("start"), q.Get("end"), q.Get("step"), q.Get("query"))
	}
	key := r.URL.Path
	if key == "/select/logsql/query" && !strings.Contains(r.URL.RawQuery+string(body), "stats") {
		key += "(rows)"
	}
	f.mu.Lock()
	if f.calls != nil {
		c := f.calls[key]
		f.calls[key] = [3]float64{c[0] + 1, c[1] + took, c[2] + float64(len(out))}
	}
	f.mu.Unlock()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(out)
}
