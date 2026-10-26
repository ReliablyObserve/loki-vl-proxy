package proxy

import (
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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// withWindowPhaseFilter puts the window filter of a range shorter than the step
// right after the query's filters, before any pipe, and after the derived
// detected_level chain when the query holds one (the pipes before the chain are
// the base its presence probe is remembered under).
// conformance: window-bounds-and-step-alignment, semantics/unwrap-conversion-error
func TestWithWindowPhaseFilterPlacement(t *testing.T) {
	start, step, window := time.Unix(1700000000, 0), 10*time.Minute, 5*time.Minute
	phase := windowPhaseFilter(start, step, window)
	chain := fullLevelChain()
	for _, tc := range []struct{ name, query, want string }{
		{"filters only", `env:="production"`, `env:="production"` + phase},
		{"filters and pipes", `env:="production" | unpack_logfmt | filter latency_ms:*`,
			`env:="production"` + phase + ` | unpack_logfmt | filter latency_ms:*`},
		{"a pipe inside a quoted phrase", `_msg:"a | b" | unpack_json`, `_msg:"a | b"` + phase + ` | unpack_json`},
		{"a pipe inside a regexp", `_msg:~"GET|POST" app:="x" | unpack_logfmt`, `_msg:~"GET|POST" app:="x"` + phase + ` | unpack_logfmt`},
		{"an escaped quote before the pipe", `_msg:"say \"|\"" | unpack_logfmt`, `_msg:"say \"|\""` + phase + ` | unpack_logfmt`},
		{"the detected_level chain and its delete", `app:="x"` + chain + ` | unpack_logfmt | filter n:*`,
			`app:="x"` + strings.TrimSuffix(chain, levelChainDelete) + levelChainDelete + phase + ` | unpack_logfmt | filter n:*`},
		{"the detected_level chain without its delete", `app:="x"` + strings.TrimSuffix(chain, levelChainDelete) + ` | stats by (detected_level) count()`,
			`app:="x"` + strings.TrimSuffix(chain, levelChainDelete) + phase + ` | stats by (detected_level) count()`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withWindowPhaseFilter(context.Background(), tc.query, start, step, window); got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

var (
	phaseMathRE = regexp.MustCompile(` \| math \(\(_time - -?\d+\) % \d+\) as __lvp_window_phase \| filter __lvp_window_phase:>0 __lvp_window_phase:<=\d+`)
	pipeNameRE  = regexp.MustCompile(`^\s*([a-z_]+)`)
)

// rowLocalPipes are the pipes a metric's log pipeline translates to. Each reads
// and writes the fields of one row: none drops a row for another row's sake,
// orders or groups rows, or writes _time, so a filter on the stored _time gives
// the same rows on either side of them.
var rowLocalPipes = map[string]bool{
	"format": true, "unpack_json": true, "unpack_logfmt": true, "unpack_syslog": true, "filter": true,
	"math": true, "copy": true, "replace": true, "replace_regexp": true, "extract": true,
	"extract_regexp": true, "delete": true, "rename": true, "decolorize": true, "drop_empty_fields": true,
	"pack_json": true, "pack_logfmt": true, "len": true,
}

// For every range metric whose range is shorter than its step, the window
// filter leads the pipeline the stats (and the raw evaluator) read: once, right
// after the filters (or the derived detected_level chain), and the pipes it
// moved ahead of are row-local and never write _time. The rows the stats
// group are therefore those they grouped with the filter after the pipeline,
// while the lines outside every window are no longer parsed.
// conformance: window-bounds-and-step-alignment, semantics/unwrap-conversion-error, loki_api_v1_query_range
func TestWindowPhaseFilterLeadsTranslatedPipelines(t *testing.T) {
	end := time.Unix(1761399600, 0)
	for _, q := range []string{
		`sum by (service_name) (rate({env="production"} | logfmt | unwrap latency_ms [5m]))`,
		`quantile_over_time(0.95, {service_name="api-gateway"} | json | unwrap duration_ms [1m]) by (service_name)`,
		`sum by (level) (count_over_time({app="a"} | logfmt | level="error" [1m]))`,
		`sum by (detected_level) (count_over_time({app="a"} [1m]))`,
		`sum by (app) (rate({app="a"} |= "x" | regexp "(?P<n>[0-9]+)" | unwrap n [5m]))`,
		`sum without (pod) (bytes_over_time({app="a"} | line_format "{{.msg}}" [2m]))`,
		`sum(rate({app="a"} | json | unwrap bytes(size) | __error__="" [5m]))`,
		`sum by (app) (count_over_time({app="a"} | pattern "<_> <n>" [1m]))`,
		`sum by (app) (rate({app="a"} | logfmt | unwrap duration(d) [5m]))`,
		`sum by (app) (count_over_time({app="a"} | json | label_format x="{{.y}}" [1m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			var mu sync.Mutex
			var queries []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				mu.Lock()
				queries = append(queries, r.Form.Get("query"))
				mu.Unlock()
				switch r.URL.Path {
				case "/select/logsql/stats_query_range":
					_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
				case "/select/logsql/stats_query":
					_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
				}
			}))
			t.Cleanup(srv.Close)
			p := newTestProxy(t, srv.URL)
			params := url.Values{"query": {q}, "start": {strconv.FormatInt(end.Add(-time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {"600"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body)
			}
			mu.Lock()
			defer mu.Unlock()
			phased := 0
			for _, query := range queries {
				locs := phaseMathRE.FindAllStringIndex(query, -1)
				if len(locs) == 0 {
					continue
				}
				phased++
				if len(locs) != 1 {
					t.Fatalf("the window filter appears %d times: %s", len(locs), query)
				}
				head := query[:locs[0][0]]
				chain := strings.TrimSuffix(fullLevelChain(), levelChainDelete)
				if translator.FirstTopLevelPipe(head) >= 0 && !strings.HasSuffix(head, chain) && !strings.HasSuffix(head, chain+levelChainDelete) {
					t.Fatalf("the window filter does not follow the filters: %s", query)
				}
				for _, pipe := range strings.Split(query[locs[0][1]:], " | ")[1:] {
					m := pipeNameRE.FindStringSubmatch(pipe)
					if m == nil {
						t.Fatalf("unreadable pipe %q in %s", pipe, query)
					}
					if m[1] == "stats" {
						break
					}
					if !rowLocalPipes[m[1]] {
						t.Fatalf("pipe %q after the window filter is not row-local: %s", m[1], query)
					}
					if strings.Contains(pipe, "as _time") || (m[1] == "delete" && strings.Contains(pipe, "_time")) || (m[1] == "rename" && strings.Contains(pipe, "_time")) {
						t.Fatalf("pipe %q writes _time: %s", pipe, query)
					}
				}
			}
			if phased == 0 {
				t.Fatalf("no backend query carried the window filter: %q", queries)
			}
		})
	}
}

// The conversion-error lookup of a range shorter than the step reads only the
// lines inside a window: the metric's base already holds the window filter at
// its head, so the lookup carries it once, ahead of the parser.
// conformance: semantics/unwrap-conversion-error, window-bounds-and-step-alignment
func TestUnwrapConversionLookupKeepsOneWindowFilter(t *testing.T) {
	at := time.Unix(1700000400, 0)
	badTS := at.Add(23 * time.Minute)
	fake := &conversionFakeVL{badBucket: badTS.Truncate(10 * time.Minute).Unix()}
	p := newTestProxy(t, newConversionFakeVL(t, fake).URL)
	params := url.Values{
		"query": {`sum by (service_name) (rate({service_name="pay"} | logfmt | unwrap v [5m]))`},
		"start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(time.Hour).UnixNano(), 10)}, "step": {"600"},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("no row fails the conversion: got %d %s", rec.Code, rec.Body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.lookups) == 0 {
		t.Fatalf("the flagged bucket must be looked up; stats=%q", fake.stats)
	}
	for _, lookup := range fake.lookups {
		query := lookup.Get("query")
		locs := phaseMathRE.FindAllStringIndex(query, -1)
		if len(locs) != 1 || locs[0][0] > strings.Index(query, "| unpack_logfmt") {
			t.Fatalf("want one window filter ahead of the parser: %s", query)
		}
	}
}
