package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: semantics/detected-level-stream-stats-without-level
func TestStreamDecidesLevelIsTheComplementOfTheLevelLessFilter(t *testing.T) {
	filter := levelLessStreamFilter()
	for _, name := range append([]string{detectedLevelLabel}, levelFieldNames[:]...) {
		if !strings.Contains(filter, name+`=""`) {
			t.Fatalf("filter %s leaves out %s", filter, name)
		}
	}
	for _, tc := range []struct {
		stream  map[string]string
		decides bool
	}{
		{map[string]string{"app": "a"}, false},
		{nil, false},
		{map[string]string{"app": "a", "level": "ERROR"}, true},
		{map[string]string{"app": "a", "LEVEL": "warning"}, true},
		{map[string]string{"app": "a", "log.level": "info"}, true},
		{map[string]string{"app": "a", "severity": "error"}, true},
		{map[string]string{"app": "a", "SEVERITY_TEXT": "Info"}, true},
		{map[string]string{"app": "a", "lvl": "x"}, true},
		{map[string]string{"app": "a", "detected_level": "debug"}, true},
		// An empty label is no label, and severity_number alone decides no level of a stream (streamLevel).
		{map[string]string{"app": "a", "level": ""}, false},
		{map[string]string{"app": "a", "severity_number": "17"}, false},
		// Names the rule does not read, or spell otherwise.
		{map[string]string{"app": "a", "loglevel": "error"}, false},
		{map[string]string{"app": "a", "LeVeL": "error"}, false},
	} {
		if got := streamDecidesLevel(tc.stream); got != tc.decides {
			t.Errorf("streamDecidesLevel(%v) = %v, want %v", tc.stream, got, tc.decides)
		}
	}
}

// conformance: semantics/detected-level-stream-stats-without-level
func TestWithLevelChainAhead(t *testing.T) {
	chain := fullLevelChain()
	for _, tc := range []struct{ in, want string }{
		{`app:="a" | unpack_logfmt`, `app:="a" ` + chain + ` | unpack_logfmt`},
		{`app:="a" "x|y" | unpack_json`, `app:="a" "x|y" ` + chain + ` | unpack_json`},
		{`app:="a"`, `app:="a" ` + chain},
	} {
		if got := withLevelChainAhead(tc.in); got != tc.want {
			t.Errorf("withLevelChainAhead(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}

// levelLessFakeVL answers the proxy's per-stream stats reads: the first read with the streams of streams, each
// with value 1 at every bucket, and the read restricted to the streams without a level label (extra_filters) with
// those streams split by the levels of levels. It records every stats_query_range call.
type levelLessFakeVL struct {
	mu      sync.Mutex
	streams []string
	levels  map[string][]string
	calls   []url.Values
}

func (f *levelLessFakeVL) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = io.WriteString(w, "OK")
			return
		case "/metrics":
			_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20250101-000000-tags-v1.53.0-0-g0", short_version="v1.53.0"} 1`+"\n")
			return
		case "/select/logsql/stats_query_range":
		case "/select/logsql/query":
			// The presence probe of the chain: every field and key present, so the chain stays whole.
			row := map[string]string{}
			for i := range levelPresenceMaskLen() {
				row[fmt.Sprintf("p%d", i)] = "1"
			}
			b, _ := json.Marshal(row)
			_, _ = w.Write(append(b, '\n'))
			return
		default:
			http.Error(w, "not served", http.StatusInternalServerError)
			return
		}
		_ = r.ParseForm()
		f.mu.Lock()
		f.calls = append(f.calls, r.Form)
		f.mu.Unlock()
		start, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
		step, _ := time.ParseDuration(r.Form.Get("step"))
		var values []string
		for ts := start; step > 0 && ts < start+int64(15*time.Minute); ts += int64(step) {
			values = append(values, fmt.Sprintf(`[%d,"1"]`, ts/int64(time.Second)))
		}
		vals := "[" + strings.Join(values, ",") + "]"
		var results []string
		restricted := r.Form.Get("extra_filters") != ""
		for _, s := range f.streams {
			levelLess := !streamDecidesLevel(parseStreamLabels(s))
			switch {
			case restricted && !levelLess:
			case restricted:
				for _, lv := range f.levels[s] {
					metric, _ := json.Marshal(map[string]string{"_stream": s, "detected_level": lv})
					results = append(results, `{"metric":`+string(metric)+`,"values":`+vals+`}`)
				}
			default:
				metric, _ := json.Marshal(map[string]string{"_stream": s})
				results = append(results, `{"metric":`+string(metric)+`,"values":`+vals+`}`)
			}
		}
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[`+strings.Join(results, ",")+`]}}`)
	})
}

func (f *levelLessFakeVL) takeCalls() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.calls
	f.calls = nil
	return out
}

// levelLessSeries is the detected_level of each series of a query_range answer, by its stream's app label.
func levelLessSeries(t *testing.T, body []byte) []string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	var out []string
	for _, s := range resp.Data.Result {
		lv, ok := s.Metric[detectedLevelLabel]
		if !ok {
			lv = "-"
		}
		out = append(out, s.Metric["app"]+"="+lv)
	}
	sort.Strings(out)
	return out
}

// TestLevelLessStreamsReadAgainOnlyWhenNeeded: a per-stream stats read (the bare-parser read of
// count_over_time(... | logfmt), and the lowered rate under sort, both at a tumbling step) whose streams all carry a
// level label sends today's one read and nothing more; one whose answer holds a stream without one sends one more
// read, restricted with extra_filters to those streams, with the detected_level chain ahead of the parser and
// detected_level in every stream group, and that stream's series become one per level its lines carry. A repeat is
// served from the response cache with no VictoriaLogs call.
//
// conformance: semantics/detected-level-stream-stats-without-level
func TestLevelLessStreamsReadAgainOnlyWhenNeeded(t *testing.T) {
	for _, q := range []string{
		`count_over_time({app=~"a|b"} | logfmt [5m])`,
		`sort(rate({app=~"a|b"} | json [1m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			fake := &levelLessFakeVL{levels: map[string][]string{`{app="b"}`: {"error", "info"}}}
			srv := httptest.NewServer(fake.handler(t))
			defer srv.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 1000), CompatCache: cache.New(60*time.Second, 100), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
			step := "300"
			if strings.Contains(q, "[1m]") {
				step = "60"
			}
			form := url.Values{"query": {q}, "start": {"1760000100"}, "end": {"1760000700"}, "step": {step}}
			get := func() *httptest.ResponseRecorder {
				return doCompatProxyRequest(p, "/loki/api/v1/query_range?"+form.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			}

			// Every stream carries a level label: today's one read.
			fake.streams = []string{`{app="a",level="ERROR"}`}
			res := get()
			calls := fake.takeCalls()
			if res.Code != http.StatusOK || len(calls) != 1 || calls[0].Get("extra_filters") != "" || strings.Contains(calls[0].Get("query"), detectedLevelLabel) {
				t.Fatalf("labelled window: %d, calls %v, %s", res.Code, calls, res.Body.String())
			}
			if got := levelLessSeries(t, res.Body.Bytes()); strings.Join(got, " ") != "a=error" {
				t.Fatalf("labelled window series %v", got)
			}

			// A stream without a level label: one more read, restricted to it.
			form.Set("end", "1760000760")
			fake.streams = []string{`{app="a",level="ERROR"}`, `{app="b"}`}
			res = get()
			calls = fake.takeCalls()
			if res.Code != http.StatusOK || len(calls) != 2 {
				t.Fatalf("level-less window: %d, %d calls, %s", res.Code, len(calls), res.Body.String())
			}
			follow := calls[1]
			if follow.Get("extra_filters") != levelLessStreamFilter() || calls[0].Get("extra_filters") != "" {
				t.Fatalf("restriction: first %q, second %q", calls[0].Get("extra_filters"), follow.Get("extra_filters"))
			}
			fq := follow.Get("query")
			if !strings.Contains(fq, strings.TrimSuffix(fullLevelChain(), levelChainDelete)) || strings.Count(fq, "_stream, "+detectedLevelLabel) != strings.Count(calls[0].Get("query"), "by (_stream") {
				t.Fatalf("second read %q\nfirst read %q", fq, calls[0].Get("query"))
			}
			if strings.Index(fq, fullLevelChain()) > strings.Index(fq, "| unpack_") {
				t.Fatalf("the chain must read the row before the parser: %q", fq)
			}
			if got := levelLessSeries(t, res.Body.Bytes()); strings.Join(got, " ") != "a=error b=error b=info" {
				t.Fatalf("level-less window series %v", got)
			}

			// A repeat: no VictoriaLogs call.
			if res = get(); res.Code != http.StatusOK || len(fake.takeCalls()) != 0 {
				t.Fatalf("repeat: %d, calls made", res.Code)
			}
		})
	}
}

// levelLessBenchBody is a stats_query_range answer of n series of 60 points, by stream when byStream.
func levelLessBenchBody(n int, byStream bool) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		if byStream {
			fmt.Fprintf(&b, `{"metric":{"_stream":"{app=\"a\",level=\"error\",pod=\"p%d\"}"},"values":[`, i)
		} else {
			fmt.Fprintf(&b, `{"metric":{"pod":"p%d"},"values":[`, i)
		}
		for j := range 60 {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%d"]`, 1760000000+j*60, j)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// BenchmarkWithLevelLessStreams: what the check costs a read whose streams all carry a level label (the labels are
// read in place, nothing is sent), and a read not grouped by stream (one byte search).
func BenchmarkWithLevelLessStreams(b *testing.B) {
	p := &Proxy{}
	params := url.Values{"query": {`app:="a" | unpack_logfmt | stats by (_stream) count() as c`}}
	for _, bc := range []struct {
		name     string
		byStream bool
	}{{"labelled_streams_50", true}, {"not_by_stream_50", false}} {
		body := levelLessBenchBody(50, bc.byStream)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := p.withLevelLessStreams(context.Background(), "/select/logsql/stats_query_range", params, body, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestLevelLessSecondReadFailureAnswersLikeTheFirst: when the read of the streams without a level label fails
// (VictoriaLogs answers 500 with its own text, or the connection drops), each route that sends it answers as it
// answers a failure of its own read: Loki's status for a failed backend, and none of VictoriaLogs' text.
//
// conformance: semantics/detected-level-stream-stats-without-level
func TestLevelLessSecondReadFailureAnswersLikeTheFirst(t *testing.T) {
	const secret = "vl-internal-detail-7f3a"
	for _, route := range []struct {
		name, path, query, step string
	}{
		{"bare-parser stats read", "/loki/api/v1/query_range", `count_over_time({app="b"} | logfmt [5m])`, "300"},
		{"stats_query_range route", "/loki/api/v1/query_range", `sort(rate({app="b"} | json [1m]))`, "60"},
		{"sliding stats read", "/loki/api/v1/query_range", `sum without (pod) (max_over_time({app="b"} | logfmt amount | unwrap amount [5m]))`, "60"},
		{"stats_query route", "/loki/api/v1/query", `sum without (pod) (max_over_time({app="b"} | logfmt amount | unwrap amount [5m]))`, ""},
	} {
		for _, failure := range []string{"status 500", "connection dropped"} {
			t.Run(route.name+"/"+failure, func(t *testing.T) {
				second := 0
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/health":
						_, _ = io.WriteString(w, "OK")
						return
					case "/metrics":
						_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20250101-000000-tags-v1.53.0-0-g0", short_version="v1.53.0"} 1`+"\n")
						return
					}
					_ = r.ParseForm()
					if r.Form.Get("extra_filters") != "" {
						second++
						if failure == "status 500" {
							http.Error(w, "victorialogs: "+secret, http.StatusInternalServerError)
							return
						}
						conn, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = conn.Close()
						}
						return
					}
					switch r.URL.Path {
					case "/select/logsql/stats_query":
						_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"_stream":"{app=\"b\",pod=\"p\"}","__lvp_bad":"0"},"value":[1760000700,"3"]}]}}`)
					case "/select/logsql/stats_query_range":
						_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"{app=\"b\",pod=\"p\"}","__lvp_bad":"0"},"values":[[1760000400,"3"],[1760000460,"3"],[1760000700,"3"]]}]}}`)
					default:
						// The presence probe and any raw read: no answer, so the chain stays whole.
						http.Error(w, "not served", http.StatusInternalServerError)
					}
				}))
				defer srv.Close()
				p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
				form := url.Values{"query": {route.query}}
				if route.step == "" {
					form.Set("time", "1760000700")
				} else {
					form.Set("start", "1760000100")
					form.Set("end", "1760000700")
					form.Set("step", route.step)
				}
				res := doCompatProxyRequest(p, route.path+"?"+form.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				if second == 0 {
					t.Fatalf("the second read was not sent: %d %s", res.Code, res.Body.String())
				}
				if res.Code != http.StatusInternalServerError {
					t.Fatalf("status %d, want Loki's 500 for a failed backend: %s", res.Code, res.Body.String())
				}
				if body := res.Body.String(); strings.Contains(body, secret) || strings.Contains(strings.ToLower(body), "victorialogs") {
					t.Fatalf("VictoriaLogs text reached the client: %s", body)
				}
			})
		}
	}
}
