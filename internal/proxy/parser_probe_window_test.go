package proxy

import (
	"context"
	"fmt"
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

// parserProbeBackend answers the parser probe (a `sort by (_time desc)` read
// of 25 rows) with lines chosen by the probe's bounds, records every probe and
// every other read, and answers the metric reads empty.
type parserProbeBackend struct {
	mu     sync.Mutex
	probes []url.Values
	reads  []string
	// lines answers a probe with its start and end ("" when absent).
	lines func(start, end string) []string
}

func (b *parserProbeBackend) handler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.URL.Path {
	case "/health":
		_, _ = w.Write([]byte("ok"))
		return
	case "/metrics":
		_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", "v1.54.0", "victoria-logs-20260101-000000-tags-v1.54.0-0-gabcdef0123")
		return
	}
	query := r.FormValue("query")
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.URL.Path == "/select/logsql/query" && r.FormValue("limit") == "25" && strings.HasSuffix(query, "| sort by (_time desc)") {
		b.probes = append(b.probes, r.Form)
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, line := range b.lines(r.FormValue("start"), r.FormValue("end")) {
			_, _ = fmt.Fprintf(w, `{"_time":"2026-11-27T10:00:00Z","_stream":"{app=\"x\"}","_msg":%q}`+"\n", line)
		}
		return
	}
	if strings.HasPrefix(query, "_time:[2000-01-01T00:00:00Z") || query == "*" {
		// The proxy's capability probes, not reads of the request.
		w.Header().Set("Content-Type", "application/x-ndjson")
		return
	}
	b.reads = append(b.reads, r.URL.Path+" "+query)
	switch r.URL.Path {
	case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	default:
		w.Header().Set("Content-Type", "application/x-ndjson")
	}
}

func newParserProbeProxy(t *testing.T, b *parserProbeBackend) *Proxy {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(b.handler))
	t.Cleanup(backend.Close)
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

const parserProbeAt = int64(1795780800) * 1e9 // 2026-11-27T12:00:00Z

// An instant query carries time, not start and end. The parser probe of a
// query naming both `| json` and `| logfmt` read start and end, so it went to
// VictoriaLogs with no time range and VictoriaLogs scanned the whole retention
// for the 25 newest lines (#1821: 19.3 s for a [3h] Drilldown count). The probe
// now reads the window the query evaluates at its time: (t-offset-range,
// t-offset], sent as VictoriaLogs' [start, end) like the instant stats read,
// the union over every range selector.
//
// conformance: semantics/instant-parser-probe-window, semantics/two-parsers-keep-one, loki_api_v1_query
func TestInstantParserProbeReadsTheQueryWindow(t *testing.T) {
	const pipe = `{app="x"} | json | logfmt | drop __error__, __error_details__`
	at := time.Unix(0, parserProbeAt).UTC()
	window := func(from, to time.Duration) [2]int64 {
		return [2]int64{parserProbeAt - int64(from) + 1, parserProbeAt - int64(to) + 1}
	}
	for _, tc := range []struct {
		name, query, time string
		want              [2]int64
	}{
		{"issue query, unix seconds", `sum(count_over_time(` + pipe + ` [3h]))`, strconv.FormatInt(parserProbeAt/1e9, 10), window(3*time.Hour, 0)},
		{"unix nanoseconds", `sum(count_over_time(` + pipe + ` [3h]))`, strconv.FormatInt(parserProbeAt, 10), window(3*time.Hour, 0)},
		{"float seconds", `sum(count_over_time(` + pipe + ` [3h]))`, strconv.FormatInt(parserProbeAt/1e9, 10) + ".000", window(3*time.Hour, 0)},
		{"RFC3339", `sum(count_over_time(` + pipe + ` [3h]))`, at.Format(time.RFC3339), window(3*time.Hour, 0)},
		{"bare range", `count_over_time(` + pipe + ` [1h])`, strconv.FormatInt(parserProbeAt/1e9, 10), window(time.Hour, 0)},
		{"offset", `sum(count_over_time(` + pipe + ` [1h] offset 2h))`, strconv.FormatInt(parserProbeAt/1e9, 10), window(3*time.Hour, 2*time.Hour)},
		{"label_replace", `label_replace(sum(count_over_time(` + pipe + ` [30m])), "a", "b", "", "")`, strconv.FormatInt(parserProbeAt/1e9, 10), window(30*time.Minute, 0)},
		{"vector operand", `vector(1) + sum(count_over_time(` + pipe + ` [5m]))`, strconv.FormatInt(parserProbeAt/1e9, 10), window(5*time.Minute, 0)},
		{"filter after the parsers", `sum(rate(` + pipe + ` | level="info" [10m]))`, strconv.FormatInt(parserProbeAt/1e9, 10), window(10*time.Minute, 0)},
		{"absent_over_time", `absent_over_time({app="x"} | json | logfmt [5m])`, strconv.FormatInt(parserProbeAt/1e9, 10), window(5*time.Minute, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
			p := newParserProbeProxy(t, b)
			res := doCompatProxyRequest(p, "/loki/api/v1/query?"+url.Values{"query": {tc.query}, "time": {tc.time}}.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if len(b.probes) != 1 {
				t.Fatalf("%d parser probes, want 1", len(b.probes))
			}
			got := [2]int64{parserProbeVLTime(t, b.probes[0].Get("start")), parserProbeVLTime(t, b.probes[0].Get("end"))}
			if got != tc.want {
				t.Fatalf("probe window [%d, %d), want [%d, %d)", got[0], got[1], tc.want[0], tc.want[1])
			}
		})
	}

	t.Run("no time is now", func(t *testing.T) {
		b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
		p := newParserProbeProxy(t, b)
		before := time.Now().UnixNano()
		res := doCompatProxyRequest(p, "/loki/api/v1/query?"+url.Values{"query": {`sum(count_over_time(` + pipe + ` [3h]))`}}.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		after := time.Now().UnixNano()
		if res.Code != http.StatusOK || len(b.probes) != 1 {
			t.Fatalf("%d %s, %d probes", res.Code, res.Body, len(b.probes))
		}
		end := parserProbeVLTime(t, b.probes[0].Get("end"))
		if start := parserProbeVLTime(t, b.probes[0].Get("start")); end-start != int64(3*time.Hour) {
			t.Fatalf("probe window [%d, %d) is not 3h long", start, end)
		}
		if end < before+1 || end > after+1 {
			t.Fatalf("probe end %d outside the request's now [%d, %d]", end, before, after)
		}
	})
}

// A log query reads no lines at an instant: Loki refuses it. The proxy sent the
// unbounded probe before it gave Loki's 400; it now answers the same 400 with
// no VictoriaLogs read.
//
// conformance: semantics/instant-parser-probe-window, semantics/two-parsers-keep-one, loki_api_v1_query
func TestInstantLogQueryWithTwoParsersSendsNoProbe(t *testing.T) {
	b := &parserProbeBackend{lines: func(string, string) []string { return []string{"level=info msg=hi"} }}
	p := newParserProbeProxy(t, b)
	res := doCompatProxyRequest(p, "/loki/api/v1/query?"+url.Values{"query": {`{app="x"} | json | logfmt`}, "time": {strconv.FormatInt(parserProbeAt/1e9, 10)}}.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "log queries are not supported as an instant query type") {
		t.Fatalf("%d %s", res.Code, res.Body)
	}
	if len(b.probes) != 0 || len(b.reads) != 0 {
		t.Fatalf("VictoriaLogs reads %d probes, %v", len(b.probes), b.reads)
	}
}

// The probe decides which parser the query keeps, so where it reads matters,
// not only what it costs. Here the lines the query evaluates (the hour before
// time) are logfmt and the newer lines, after time, are JSON. The unbounded
// probe read the 25 newest lines of the whole retention, preferred json and
// dropped the logfmt stage: the logfmt lines lost their parsed labels, so the
// count grouped by one fell into a single series, where Loki runs both stages
// and groups by the logfmt label. The
// bounded probe reads the evaluated lines and keeps logfmt. A range query was
// bounded already: its probe still reads [start, end].
//
// conformance: semantics/instant-parser-probe-window, semantics/two-parsers-keep-one, loki_api_v1_query, loki_api_v1_query_range
func TestInstantParserProbeChoosesFromTheEvaluatedLines(t *testing.T) {
	lines := func(start, end string) []string {
		if start == "" && end == "" {
			out := make([]string, 25)
			for i := range out {
				out[i] = `{"route":"/c","msg":"newer"}`
			}
			return out
		}
		return []string{"route=/a msg=evaluated", "route=/b msg=evaluated"}
	}
	const query = `max by (route) (count_over_time({app="x"} | json | logfmt | drop __error__, __error_details__ [1h]))`
	for _, tc := range []struct {
		name, target string
	}{
		{"instant", "/loki/api/v1/query?" + url.Values{"query": {query}, "time": {strconv.FormatInt(parserProbeAt/1e9, 10)}}.Encode()},
		{"range", "/loki/api/v1/query_range?" + url.Values{"query": {query}, "start": {strconv.FormatInt(parserProbeAt/1e9-3600, 10)}, "end": {strconv.FormatInt(parserProbeAt/1e9, 10)}, "step": {"60"}}.Encode()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &parserProbeBackend{lines: lines}
			p := newParserProbeProxy(t, b)
			res := doCompatProxyRequest(p, tc.target, map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if len(b.probes) != 1 || b.probes[0].Get("start") == "" || b.probes[0].Get("end") == "" {
				t.Fatalf("probes %v, want one bounded probe", b.probes)
			}
			if len(b.reads) == 0 {
				t.Fatal("no metric read")
			}
			for _, read := range b.reads {
				if !strings.Contains(read, "unpack_logfmt") || strings.Contains(read, "unpack_json") {
					t.Fatalf("metric read %q, want the logfmt stage the evaluated lines need", read)
				}
			}
		})
	}
}

// The window itself, from the parsed query: the union of the range selectors'
// windows (a mixed offset under a binary keeps its own), the selectors of a
// call kept as text, and a short window where the length is not known.
func TestInstantParserProbeWindowUnion(t *testing.T) {
	now := time.Unix(0, parserProbeAt+int64(time.Hour))
	at := strconv.FormatInt(parserProbeAt/1e9, 10)
	for _, tc := range []struct {
		query, time string
		from, to    int64
		probe       bool
	}{
		{`sum(count_over_time({a="b"} | json | logfmt [5m] offset 1h)) / sum(count_over_time({a="b"} | json | logfmt [10m]))`, at, parserProbeAt - int64(65*time.Minute), parserProbeAt, true},
		{`approx_topk(2, sum by (x) (count_over_time({a="b"} | json | logfmt [15m])))`, at, parserProbeAt - int64(15*time.Minute), parserProbeAt, true},
		{`{a="b"} | json | logfmt`, at, 0, 0, false},
		{`not a query`, at, parserProbeAt - int64(parserProbeFallbackWindow), parserProbeAt, true},
		{`sum(count_over_time({a="b"} | json | logfmt [1h]))`, "", now.UnixNano() - int64(time.Hour), now.UnixNano(), true},
		{`sum(count_over_time({a="b"} | json | logfmt [1h]))`, "not a time", now.UnixNano() - int64(time.Hour), now.UnixNano(), true},
	} {
		start, end, probe := instantParserProbeWindow(tc.query, tc.time, now)
		if probe != tc.probe {
			t.Fatalf("%s: probe %v, want %v", tc.query, probe, tc.probe)
		}
		if !probe {
			continue
		}
		if start != strconv.FormatInt(tc.from+1, 10) || end != strconv.FormatInt(tc.to+1, 10) {
			t.Fatalf("%s: [%s, %s), want [%d, %d)", tc.query, start, end, tc.from+1, tc.to+1)
		}
	}
}

// parserProbeVLTime reads a probe bound the proxy sent VictoriaLogs.
func parserProbeVLTime(t *testing.T, raw string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(formatVLTimestamp(raw), 10, 64)
	if err != nil {
		t.Fatalf("probe bound %q: %v", raw, err)
	}
	return n
}

// BenchmarkInstantParserProbeWindow is the proxy's cost of bounding the probe:
// the window read from the parsed query (memoised) at the request's time.
func BenchmarkInstantParserProbeWindow(b *testing.B) {
	const query = `sum(count_over_time({cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__ [3h]))`
	now := time.Unix(0, parserProbeAt)
	at := strconv.FormatInt(parserProbeAt/1e9, 10)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, probe := instantParserProbeWindow(query, at, now); !probe {
			b.Fatal("no window")
		}
	}
}

// FuzzInstantParserProbeWindow: any query text and time give either no probe
// or a window that ends after it starts, without a panic.
func FuzzInstantParserProbeWindow(f *testing.F) {
	for _, seed := range []string{
		`sum(count_over_time({a="b"} | json | logfmt | drop __error__, __error_details__ [3h]))`,
		`sum(count_over_time({a="b"} | json | logfmt [5m] offset 1h)) / sum(count_over_time({a="b"} | json | logfmt [10m]))`,
		`label_replace(sum(count_over_time({a="b"} | json | logfmt [30m])), "a", "b", "", "")`,
		`approx_topk(2, sum by (x) (count_over_time({a="b"} | json | logfmt [15m])))`,
		`{a="b"} | json | logfmt`,
		`vector(1)`,
		`count_over_time({a="b"} | json | logfmt [$__auto])`,
	} {
		f.Add(seed, "1795780800")
	}
	f.Add(`rate({a="b"}[1y] offset 1000w)`, "")
	now := time.Unix(0, parserProbeAt)
	f.Fuzz(func(t *testing.T, query, at string) {
		start, end, probe := instantParserProbeWindow(query, at, now)
		if !probe {
			return
		}
		s, errS := strconv.ParseInt(start, 10, 64)
		e, errE := strconv.ParseInt(end, 10, 64)
		if errS != nil || errE != nil || s >= e {
			t.Fatalf("%q at %q: window [%s, %s)", query, at, start, end)
		}
	})
}
