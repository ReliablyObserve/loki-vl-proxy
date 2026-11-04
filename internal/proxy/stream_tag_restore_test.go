package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// VictoriaLogs' Loki push API unpacks the keys of a JSON line into fields after
// the stream labels, and a block keeps the last value written for a name
// (app/vlinsert/loki/loki_json.go parseJSONRequest and addMsgField,
// lib/logstorage/block.go mustInitFromRows): a line whose JSON has a key named
// like a stream label stores the line's value under the label's name, while
// _stream keeps the label. pushdownRow models that with vl overriding stream.
//
// In Loki v3.7.9 a metric's labels are the stream labels, then the structured
// metadata (pkg/logql/log/labels.go LabelsBuilder, a stream label wins), and a
// parser renames a key that collides to name_extracted
// (pkg/logql/log/parser.go duplicateSuffix). So
// `sum by (version) (count_over_time({...} [5s]))` groups by the stream's
// version whatever the line holds.

// shadowedVersionRows mirrors the visual stack's api-gateway: stream label
// version="v2", every line's JSON has a version key of v1 or v2 that
// VictoriaLogs stored over the label. A second service has the label and no
// such key, a third has no version stream label and version as structured
// metadata.
func shadowedVersionRows(s0 time.Time) []pushdownRow {
	var rows []pushdownRow
	for i := 0; i < 240; i++ {
		ts := s0.Add(time.Duration(i)*2500*time.Millisecond + 300*time.Millisecond)
		lvl := []string{"info", "warn", "error"}[i%3]
		body := []string{"v1", "v2"}[(i/3)%2]
		switch i % 4 {
		case 0, 1:
			rows = append(rows, pushdownRow{ts: ts,
				stream: map[string]string{"service_name": "api-gateway", "version": "v2", "level": lvl, "env": "production"},
				vl:     map[string]string{"version": body, "level": lvl},
				msg:    `{"level":"` + lvl + `","version":"` + body + `","msg":"request"}`})
		case 2:
			rows = append(rows, pushdownRow{ts: ts,
				stream: map[string]string{"service_name": "api-gateway", "version": "v3", "level": lvl, "env": "production"},
				msg:    `{"level":"` + lvl + `","msg":"request"}`})
		default:
			rows = append(rows, pushdownRow{ts: ts,
				stream: map[string]string{"service_name": "api-gateway", "level": lvl, "env": "production"},
				vl:     map[string]string{"version": "v7"},
				loki:   map[string]string{"version": "v7"},
				msg:    `level=` + lvl + ` msg="sm"`})
		}
	}
	return rows
}

// lokiVersionReference is Loki's answer: per evaluation time t in [start, end]
// at step, the lines in (t-window, t] that the level filter keeps (op "" for
// none), grouped by the stream label, else the structured metadata, of name by.
func lokiVersionReference(rows []pushdownRow, by, levelOp, levelValue string, start, end time.Time, step, window time.Duration) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for at := start; !at.After(end); at = at.Add(step) {
		counts := map[string]int{}
		for _, row := range rows {
			if !row.ts.After(at.Add(-window)) || row.ts.After(at) {
				continue
			}
			if levelOp != "" && !lokiRefMatches(levelOp, levelValue, lokiRefLevel(row.stream, row.loki, row.msg)) {
				continue
			}
			v, ok := row.stream[by]
			if !ok {
				v = row.loki[by]
			}
			key := "{}"
			if v != "" {
				key = canonicalLabelsKey(map[string]string{by: v})
			}
			counts[key]++
		}
		for key, n := range counts {
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][at.Unix()] = strconv.Itoa(n)
		}
	}
	return out
}

// newStreamTagProxy is the pushdown test proxy in the default (Loki-compatible)
// profile, fresh: the tenant's stream label names are not listed.
func newStreamTagProxy(t *testing.T, srvURL string) *Proxy {
	t.Helper()
	p := newFilterPushdownProxy(t, srvURL)
	p.metadataFieldMode = MetadataFieldModeTranslated // the default: the Loki-compatible profile
	t.Cleanup(p.stopStreamLabelNames)
	return p
}

// runScopedQueryRange runs a query_range as a client request (the tenant's
// stream label names apply) and returns series key -> unix second -> value.
func runScopedQueryRange(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) map[string]map[int64]string {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, scopedRequest(p, "/loki/api/v1/query_range?"+params.Encode()))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, rec.Code, rec.Body)
	}
	return parseMatrixPoints(t, query, rec.Body.Bytes())
}

func parseMatrixPoints(t *testing.T, query string, body []byte) map[string]map[int64]string {
	t.Helper()
	got := map[string]map[int64]string{}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: invalid matrix (%v): %s", query, err, body)
	}
	for _, series := range resp.Data.Result {
		key := canonicalLabelsKey(series.Metric)
		if len(series.Metric) == 0 {
			key = "{}"
		}
		if got[key] == nil {
			got[key] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			got[key][int64(ts)], _ = pair[1].(string)
		}
	}
	return got
}

func diffSeriesPoints(got, want map[string]map[int64]string) string {
	var diffs []string
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	for k := range keys {
		if fmt.Sprint(got[k]) != fmt.Sprint(want[k]) {
			sum := func(m map[int64]string) int {
				n := 0
				for _, v := range m {
					x, _ := strconv.Atoi(v)
					n += x
				}
				return n
			}
			diffs = append(diffs, fmt.Sprintf("%s: got %d points (total %d), want %d points (total %d)", k, len(got[k]), sum(got[k]), len(want[k]), sum(want[k])))
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "; ")
}

// The Logs Drilldown labels tab breaks a service down by a stream label, with
// and without a level pill, and Explore sums by it: the proxy answers Loki's
// series and counts although VictoriaLogs stored a JSON key of the same name
// over the label, keeps a structured metadata value where the stream has no
// such label, and adds no VictoriaLogs call.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key, severity/detected-level-filter-json-level, loki_api_v1_query_range
func TestStreamLabelShadowedByIngestJSONKeyLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := shadowedVersionRows(s0)
	start, end := s0.Add(time.Minute), s0.Add(9*time.Minute)
	cases := []struct {
		name, query, levelOp, levelValue string
		step, window                     time.Duration
	}{
		{"labels tab", `sum(count_over_time({service_name="api-gateway" ,version != ""} [5s])) by (version)`, "", "", 5 * time.Second, 5 * time.Second},
		{"labels tab, level pill", `sum(count_over_time({service_name="api-gateway" ,version != ""}  | detected_level="error"      [5s])) by (version)`, "=", "error", 5 * time.Second, 5 * time.Second},
		{"labels tab, two-level pill", `sum(count_over_time({service_name="api-gateway" ,version != ""} | detected_level=~"error|warn" [5s])) by (version)`, "=~", "error|warn", 5 * time.Second, 5 * time.Second},
		{"explore sum by", `sum by (version) (count_over_time({service_name="api-gateway"} [1m]))`, "", "", time.Minute, time.Minute},
		{"a 30s step", `sum by (version) (count_over_time({service_name="api-gateway"} [30s]))`, "", "", 30 * time.Second, 30 * time.Second},
		{"sliding window", `sum by (version) (count_over_time({service_name="api-gateway"} [1m]))`, "", "", 20 * time.Second, time.Minute},
	}
	// Loki's stream selector reads stream labels only, so `version != ""`
	// leaves out a line whose version is structured metadata; the proxy's
	// selector reads the stored field and keeps it (open:
	// semantics/stream-label-selector-shadowed-by-ingest-json-key). The cases
	// whose selector names the label run without those lines.
	var streamRows []pushdownRow
	for _, row := range rows {
		if _, ok := row.stream["version"]; ok {
			streamRows = append(streamRows, row)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows := rows
			if strings.Contains(c.query, `version != ""`) {
				rows = streamRows
			}
			srv, fake := newPushdownFakeVL(t, rows, nil)
			fake.presence = true
			p := newStreamTagProxy(t, srv.URL)
			got := runScopedQueryRange(t, p, c.query, start, end, c.step)
			want := lokiVersionReference(rows, "version", c.levelOp, c.levelValue, start, end, c.step, c.window)
			if d := diffSeriesPoints(got, want); d != "" {
				t.Fatalf("%s:\n%s", c.query, d)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.raw != 0 {
				t.Fatalf("%s read %d raw-row windows", c.query, fake.raw)
			}
		})
	}
}

// A fresh proxy restores the grouping on the first request, without listing
// the tenant's stream label names: the answer is Loki's, VictoriaLogs gets no
// stream_field_names call, and the second request is a response cache hit.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamTagRestoreFirstRequestWithoutListing(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := shadowedVersionRows(s0)
	srv, fake := newPushdownFakeVL(t, rows, nil)
	p := newStreamTagProxy(t, srv.URL)
	query := `sum by (version) (count_over_time({service_name="api-gateway"} [1m]))`
	start, end := s0.Add(time.Minute), s0.Add(9*time.Minute)
	want := lokiVersionReference(rows, "version", "", "", start, end, time.Minute, time.Minute)
	for i := 0; i < 2; i++ {
		if d := diffSeriesPoints(runScopedQueryRange(t, p, query, start, end, time.Minute), want); d != "" {
			t.Fatalf("request #%d: %s", i, d)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.listings != 0 || len(fake.stats) != 1 {
		t.Fatalf("%d stream_field_names and %d stats calls, want 0 and 1 (the second request is cached)", fake.listings, len(fake.stats))
	}
}

// Two replicas that never listed anything translate a parser-free grouped
// metric identically and key its response identically, so an entry one
// computes is the entry the other looks up (the peer cache tiers).
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamTagRestoreSameOnEveryReplica(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	srv, _ := newPushdownFakeVL(t, shadowedVersionRows(s0), nil)
	a, b := newStreamTagProxy(t, srv.URL), newStreamTagProxy(t, srv.URL)
	warmStreamLabels(t, b, scopedRequest(b, "/").Context()) // B knows the tenant's names, A does not
	for _, q := range []string{
		`sum by (version) (count_over_time({service_name="api-gateway"} [1m]))`,
		`sum(count_over_time({service_name="api-gateway" ,version != ""} | detected_level="error" [5s])) by (version)`,
		`sum by (app) (count_over_time({app="x"} [5m]))`,
	} {
		ra, rb := scopedRequest(a, "/loki/api/v1/query_range"), scopedRequest(b, "/loki/api/v1/query_range")
		ta, errA := a.translateQueryWithContext(ra.Context(), q)
		tb, errB := b.translateQueryWithContext(rb.Context(), q)
		if errA != nil || errB != nil || ta != tb {
			t.Fatalf("%s: translations differ (%v, %v):\nA %s\nB %s", q, errA, errB, ta, tb)
		}
		if ka, kb := a.responseNamesCacheKey(ra, q), b.responseNamesCacheKey(rb, q); ka != kb || ka == "" {
			t.Fatalf("%s: cache key parts %q and %q", q, ka, kb)
		}
	}
}

// A parser-free metric query restores exactly the names it groups by (in a
// vector or range aggregation, nested in label_replace or a binary operand; a
// topk is answered from its inner aggregation's translation), never a derived or level name, a name only its selector or a
// `without` holds, or a log query's.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamTagRestoreNames(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	srv, fake := newPushdownFakeVL(t, shadowedVersionRows(s0), nil)
	p := newStreamTagProxy(t, srv.URL)
	ctx := scopedRequest(p, "/").Context()
	for _, c := range []struct {
		query   string
		restore []string
	}{
		{`sum by (version) (count_over_time({service_name="api-gateway"} [1m]))`, []string{"version"}},
		{`sum by (env, version) (rate({service_name="api-gateway"} [1m]))`, []string{"env", "version"}},
		{`sum by (user) (count_over_time({service_name="api-gateway"} [1m]))`, []string{"user"}},
		{`sum(count_over_time({service_name="api-gateway", version="v2"} [1m]))`, nil},
		{`sum without (pod) (count_over_time({service_name="api-gateway"} [1m]))`, nil},
		{`sum by (level) (count_over_time({service_name="api-gateway"} [1m]))`, nil},
		{`sum by (detected_level, service_name) (count_over_time({service_name="api-gateway"} [1m]))`, nil},
		{`label_replace(sum by (version) (count_over_time({service_name="api-gateway"} [1m])), "v", "$1", "version", "(.*)")`, []string{"version"}},
		{`sum by (env) (count_over_time({service_name="api-gateway"} [1m])) / sum by (env) (count_over_time({service_name="api-gateway"} |= "by (x)" [1m]))`, []string{"env"}},
		{`{service_name="api-gateway"} | version="v2"`, nil},
	} {
		got, err := p.translateQueryWithContext(ctx, c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		var restored []string
		for _, m := range regexp.MustCompile(`extract if \(__lxst_\d+:\*\) "<([^>]+)>" from __lxst_\d+`).FindAllStringSubmatch(got, -1) {
			if !slicesContains(restored, m[1]) {
				restored = append(restored, m[1])
			}
		}
		sort.Strings(restored)
		if fmt.Sprint(restored) != fmt.Sprint(c.restore) {
			t.Errorf("%s: restores %v, want %v\n%s", c.query, restored, c.restore, got)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.listings != 0 {
		t.Errorf("parser-free metric queries listed the tenant's stream label names %d times", fake.listings)
	}
}

func slicesContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Differential: generated stream labels, JSON keys that shadow them or not,
// structured metadata, level pills and windows; the proxy's grouped counts
// equal Loki's.
//
// conformance: semantics/stream-label-shadowed-by-ingest-json-key
func TestStreamLabelShadowedByIngestJSONKeyDifferential(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261104))
	for i := 0; i < 40; i++ {
		s0 := time.Unix(1700000400, 0).UTC()
		rows := randomShadowedRows(rnd, s0)
		step := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute}[rnd.Intn(4)]
		window := step
		pill := []struct{ op, value string }{{"", ""}, {"=", "error"}, {"!=", "info"}, {"=~", "warn|error"}}[rnd.Intn(4)]
		stage := ""
		if pill.op != "" {
			stage = " | detected_level" + pill.op + strconv.Quote(pill.value)
		}
		query := fmt.Sprintf(`sum by (version) (count_over_time({service_name="svc"}%s [%s]))`, stage, uwWindowText(window))
		start := s0.Add(time.Duration(rnd.Intn(60)) * time.Second).Truncate(step)
		end := start.Add(time.Duration(4+rnd.Intn(12)) * step)
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.presence = true
		p := newStreamTagProxy(t, srv.URL)
		got := runScopedQueryRange(t, p, query, start, end, step)
		want := lokiVersionReference(rows, "version", pill.op, pill.value, start, end, step, window)
		if d := diffSeriesPoints(got, want); d != "" {
			t.Fatalf("#%d %s [%s..%s step %s]:\n%s", i, query, start.Format(time.TimeOnly), end.Format(time.TimeOnly), step, d)
		}
	}
}

// randomShadowedRows generates lines of one service over three streams:
// version a stream label (shadowed by a JSON key or not), or structured
// metadata, or absent; values with quotes, backslashes and commas.
func randomShadowedRows(rnd *rand.Rand, s0 time.Time) []pushdownRow {
	values := []string{"v1", "v2", `a"b`, `c\d`, "e,version=f", ""}
	var rows []pushdownRow
	for i := 0; i < 20+rnd.Intn(120); i++ {
		ts := s0.Add(time.Duration(rnd.Int63n(int64(10 * time.Minute))))
		lvl := []string{"info", "warn", "error", "debug"}[rnd.Intn(4)]
		row := pushdownRow{ts: ts, stream: map[string]string{"service_name": "svc", "level": lvl}}
		if rnd.Intn(3) == 0 {
			row.stream["app_version"] = values[rnd.Intn(len(values)-1)]
		}
		switch rnd.Intn(3) {
		case 0: // a stream label, maybe shadowed by the line's key
			row.stream["version"] = values[rnd.Intn(len(values)-1)]
			if body := values[rnd.Intn(len(values))]; body != "" {
				row.vl = map[string]string{"version": body}
				row.msg = `{"level":"` + lvl + `","version":` + strconv.Quote(body) + `}`
			} else {
				row.msg = `{"level":"` + lvl + `"}`
			}
		case 1: // structured metadata
			if v := values[rnd.Intn(len(values))]; v != "" {
				row.vl, row.loki = map[string]string{"version": v}, map[string]string{"version": v}
			}
			row.msg = "level=" + lvl + " msg=sm"
		default: // no version anywhere
			row.msg = "plain " + lvl + " line"
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ts.Before(rows[j].ts) })
	return rows
}

// BenchmarkStreamTagRestore measures, through the real handler against the
// fake VictoriaLogs (uncached), the shapes whose translation the restore
// changes (a stream label grouping: shadowed and not, with and without a
// level pill, after logfmt) and a control without a stream label grouping.
// vl-stats/op, vl-raw/op and vl-probes/op show that no shape changes path or
// call count. The backend replays the fake's first answer per endpoint, so
// ns/op and allocs/op measure the proxy; VictoriaLogs' own cost of the restore
// pipes is measured by bench/ab on a stack.
func BenchmarkStreamTagRestore(b *testing.B) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := shadowedVersionRows(s0)
	start, end := s0.Add(time.Minute), s0.Add(9*time.Minute)
	type shape struct {
		name, query string
		cached      bool // a response cache, and each request one step later (the extent cache)
	}
	shapes := []shape{
		{"labels_tab_version_pill", `sum(count_over_time({service_name="api-gateway" ,version != ""}  | detected_level="error"      [5s])) by (version)`, false},
		{"labels_tab_version", `sum(count_over_time({service_name="api-gateway" ,version != ""} [5s])) by (version)`, false},
		{"labels_tab_env", `sum(count_over_time({service_name="api-gateway" ,env != ""} [5s])) by (env)`, false},
		{"labels_tab_env_pill", `sum(count_over_time({service_name="api-gateway" ,env != ""} | detected_level="error" [5s])) by (env)`, false},
		{"explore_sum_by_env_logfmt", `sum by (env) (count_over_time({service_name="api-gateway"} | logfmt [5s]))`, false},
		{"sliding_by_env", `sum by (env) (count_over_time({service_name="api-gateway"} [1m]))`, false},
		{"binary_by_env", `sum by (env) (count_over_time({service_name="api-gateway"} | detected_level="error" [5s])) / sum by (env) (count_over_time({service_name="api-gateway"} [5s]))`, false},
		{"label_replace_by_env", `label_replace(sum by (env) (count_over_time({service_name="api-gateway"} [5s])), "e", "$1", "env", "(.*)")`, false},
		{"cached_shifting_by_version", `sum by (version) (count_over_time({service_name="api-gateway"} [5s]))`, true},
		{"control_by_detected_level", `sum by (detected_level) (count_over_time({service_name="api-gateway"} [5s]))`, false},
	}
	for _, sh := range shapes {
		b.Run(sh.name, func(b *testing.B) {
			fakeSrv, fake := newPushdownFakeVL(b, rows, nil)
			// The fake answers each endpoint once; later calls replay that answer,
			// so ns/op and allocs/op are the proxy's, not the fake's evaluation.
			var replayMu sync.Mutex
			replay := map[string][]byte{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				key := r.URL.Path
				if strings.Contains(r.Form.Get("query"), "| limit 1") || strings.Contains(r.Form.Get("query"), "count() if (") {
					key += "#probe"
				}
				replayMu.Lock()
				body, ok := replay[key]
				replayMu.Unlock()
				if !ok {
					req, _ := http.NewRequest(r.Method, fakeSrv.URL+r.URL.Path, strings.NewReader(r.Form.Encode()))
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadGateway)
						return
					}
					body, _ = io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					replayMu.Lock()
					replay[key] = body
					replayMu.Unlock()
				}
				fake.mu.Lock()
				switch {
				case strings.HasSuffix(key, "#probe"):
					fake.guards = append(fake.guards, "")
				case r.URL.Path == "/select/logsql/stats_query_range" || r.URL.Path == "/select/logsql/stats_query":
					fake.stats = append(fake.stats, r.Form.Get("query"))
				case r.URL.Path == "/select/logsql/query":
					fake.raw++
				}
				fake.mu.Unlock()
				_, _ = w.Write(body)
			}))
			b.Cleanup(srv.Close)
			cfg := Config{BackendURL: srv.URL, LogLevel: "error", LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated}
			if sh.cached {
				cfg.Cache = cache.New(time.Minute, 10000)
			}
			p, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(p.stopStreamLabelNames)
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			targetAt := func(i int) string {
				shift := time.Duration(0)
				if sh.cached {
					shift = time.Duration(i%60) * 5 * time.Second
				}
				params := url.Values{"query": {sh.query}, "start": {strconv.FormatInt(start.Add(shift).UnixNano(), 10)}, "end": {strconv.FormatInt(end.Add(shift).UnixNano(), 10)}, "step": {"5"}}
				return "/loki/api/v1/query_range?" + params.Encode()
			}
			target := targetAt(0)
			// One request fills the replay before counting (translation and
			// response caching are off: Cache is nil).
			p.handleQueryRange(httptest.NewRecorder(), scopedRequest(p, target))
			fake.mu.Lock()
			fake.stats, fake.raw, fake.guards, fake.presenceCalls = nil, 0, nil, nil
			fake.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, scopedRequest(p, targetAt(i+1)))
				if rec.Code != http.StatusOK {
					b.Fatalf("%s: %d %s", sh.query, rec.Code, rec.Body)
				}
			}
			b.StopTimer()
			fake.mu.Lock()
			b.ReportMetric(float64(len(fake.stats))/float64(b.N), "vl-stats/op")
			b.ReportMetric(float64(fake.raw)/float64(b.N), "vl-raw/op")
			b.ReportMetric(float64(len(fake.guards))/float64(b.N), "vl-probes/op")
			fake.mu.Unlock()
		})
	}
}
