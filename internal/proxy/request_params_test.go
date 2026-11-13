package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// requestParamAnswersPath is the file of Loki's recorded answers to
// request-parameter probes: every parameter of every read route with valid and
// invalid values, and pairs of invalid inputs that show the order Loki checks
// them in.
var requestParamAnswersPath = filepath.Join("testdata", "loki_request_param_answers.jsonl")

func readRequestParamAnswers(t *testing.T) []lokiErrorAnswer {
	t.Helper()
	f, err := os.Open(requestParamAnswersPath)
	if err != nil {
		t.Fatalf("open %s: %v", requestParamAnswersPath, err)
	}
	defer f.Close()
	var rows []lokiErrorAnswer
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<22)
	for sc.Scan() {
		var row lokiErrorAnswer
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("%s: %v", requestParamAnswersPath, err)
		}
		if row.ID != "" {
			rows = append(rows, row)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// requestParamKnownDifferences are the recorded rows whose proxy answer is not
// Loki's yet, with why. The list may only shrink: a row that newly differs
// fails, and a listed row that now matches fails until it is removed.
var requestParamKnownDifferences = map[string]string{
	"r0610": "the replay proxy keeps max_entries_limit_per_query at its default of 10000; the recorded Loki has no such limit",
	"r0673": "the replay proxy keeps max_entries_limit_per_query at its default of 10000; the recorded Loki has no such limit",
	"r0691": "the proxy reads an integer time of 11 to 18 digits by its magnitude (seconds, milliseconds, microseconds) and refuses a year 3000 time; Loki reads it as nanoseconds",
	"r1597": "a stage that does not compile is reported with the parse error before the parameters; Loki reports it after them (needs the parser's phase of a rejection)",
	"r1598": "a stage that does not compile is reported with the parse error before the parameters; Loki reports it after them (needs the parser's phase of a rejection)",
	"r1600": "a stage that does not compile is reported with the parse error before the parameters; Loki reports it after them (needs the parser's phase of a rejection)",
	"r1601": "a stage that does not compile is reported with the parse error before the parameters; Loki reports it after them (needs the parser's phase of a rejection)",
}

// requestParamKnownDifferenceCeiling is the length of requestParamKnownDifferences:
// a fix removes a row and lowers it, a new difference cannot be added.
const requestParamKnownDifferenceCeiling = 7

// TestRequestParamAnswers replays Loki's recorded answers to request-parameter
// probes against the proxy: a request Loki rejects gets Loki's status,
// Content-Type, X-Content-Type-Options and text, byte for byte, and a request
// Loki accepts is not rejected. The rows cover every route x parameter x
// invalid value and the pairs that fix the order of two checks.
// LVP_REQUEST_PARAM_UPDATE=1 prints the rows that differ.
// conformance: semantics/error-request-validation-parity
func TestRequestParamAnswers(t *testing.T) {
	rows := readRequestParamAnswers(t)
	addr := newLokiErrorReplayProxy(t)
	var diffs, fixed []string
	compared := 0
	for _, row := range rows {
		reply := row.replay(t, addr)
		if row.NotComparable != "" {
			continue
		}
		compared++
		want := row.Loki
		ok := reply == want
		if want.Status < 400 {
			// A success body is data the stand-in backend does not hold.
			ok = reply.Status == want.Status
		}
		_, known := requestParamKnownDifferences[row.ID]
		switch {
		case !ok && !known:
			diffs = append(diffs, row.ID+" ("+row.Route+"): Loki "+summarizeReply(want)+", proxy "+summarizeReply(reply))
		case ok && known:
			fixed = append(fixed, row.ID)
		}
	}
	sort.Strings(diffs)
	t.Logf("%d rows, %d compared, %d listed as known differences", len(rows), compared, len(requestParamKnownDifferences))
	if len(requestParamKnownDifferences) != requestParamKnownDifferenceCeiling {
		t.Errorf("requestParamKnownDifferences lists %d rows, its ceiling is %d: the list may only shrink, and a fix lowers the ceiling with it", len(requestParamKnownDifferences), requestParamKnownDifferenceCeiling)
	}
	if len(diffs) > 0 {
		t.Errorf("%d rows differ from Loki:\n%s", len(diffs), strings.Join(diffs, "\n"))
	}
	if len(fixed) > 0 {
		t.Errorf("rows now answer as Loki does, remove them from requestParamKnownDifferences: %s", strings.Join(fixed, ", "))
	}
}

// TestQueryRangeSinceSetsTheWindow: a query_range with since and no start or
// end covers the hour before now, as Loki reads it, so VictoriaLogs is asked
// for that window and the query stats report the lines and bytes read from it,
// the same as the request that names the window.
// conformance: query-stats-envelope, semantics/error-request-validation-parity
func TestQueryRangeSinceSetsTheWindow(t *testing.T) {
	stub := &statsStubVL{rows: 20}
	var mu sync.Mutex
	var sentStart, sentEnd []string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path == "/select/logsql/query" {
			mu.Lock()
			sentStart = append(sentStart, r.Form.Get("start"))
			sentEnd = append(sentEnd, r.Form.Get("end"))
			mu.Unlock()
		}
		stub.ServeHTTP(w, r)
	}))
	defer vl.Close()
	newProxy := func() http.Handler {
		p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 100), LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		return mux
	}
	now := statsTestNow()
	since := url.Values{"query": {`{app="api"}`}, "limit": {"100"}, "since": {"1h"}}
	named := url.Values{"query": {`{app="api"}`}, "limit": {"100"},
		"start": {fmt.Sprint(now.Add(-time.Hour).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	got := statsView(t, serveQueryStats(t, newProxy(), "/loki/api/v1/query_range", since, nil))
	mu.Lock()
	sinceStarts := append([]string(nil), sentStart...)
	mu.Unlock()
	want := statsView(t, serveQueryStats(t, newProxy(), "/loki/api/v1/query_range", named, nil))
	if got.Summary.Lines == 0 || got.Summary.Bytes == 0 {
		t.Fatalf("since=1h: summary %+v, want the lines and bytes of the hour read", got.Summary)
	}
	if got.Summary.Lines != want.Summary.Lines || got.Summary.Bytes != want.Summary.Bytes {
		t.Errorf("since=1h: lines %d bytes %d; the same window named: lines %d bytes %d", got.Summary.Lines, got.Summary.Bytes, want.Summary.Lines, want.Summary.Bytes)
	}
	// The log query of the since request is the call that carries the window.
	var asked []time.Duration
	for _, raw := range sinceStarts {
		if ns, ok := parseLokiTimeToUnixNano(raw); ok {
			asked = append(asked, time.Since(time.Unix(0, ns)))
		}
	}
	if len(asked) == 0 {
		t.Fatalf("since=1h: VictoriaLogs was asked for starts %q, want the start of the hour", sinceStarts)
	}
	for _, age := range asked {
		if age < time.Hour-5*time.Minute || age > time.Hour+5*time.Minute {
			t.Errorf("since=1h: VictoriaLogs start is %v old, want an hour", age)
		}
	}
}

// TestFormBodyOverTenMegabytesIsRefusedOnEveryRoute: a form body over 10 MB is
// answered Loki's `http: POST too large` on every read route (a recorded row on
// query_range and series: r1401, r1402), the read stops at the bound, and a
// body of another content type is not read at all.
// conformance: semantics/error-request-validation-parity
func TestFormBodyOverTenMegabytesIsRefusedOnEveryRoute(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	big := "query=" + strings.Repeat("a", 10<<20)
	paths := map[string]string{
		"query_range": "/loki/api/v1/query_range", "query": "/loki/api/v1/query", "labels": "/loki/api/v1/labels",
		"label_values": "/loki/api/v1/label/app/values", "series": "/loki/api/v1/series", "index_stats": "/loki/api/v1/index/stats",
		"volume": "/loki/api/v1/index/volume", "volume_range": "/loki/api/v1/index/volume_range", "patterns": "/loki/api/v1/patterns",
		"detected_fields": "/loki/api/v1/detected_fields", "detected_field_values": "/loki/api/v1/detected_field/app/values",
		"detected_labels": "/loki/api/v1/detected_labels",
	}
	for route, path := range paths {
		t.Run(route, func(t *testing.T) {
			read := &countingReader{r: strings.NewReader(big)}
			req := httptest.NewRequest(http.MethodPost, path, read)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || rec.Body.String() != "http: POST too large" {
				t.Fatalf("got %d %q, want 400 %q", rec.Code, rec.Body.String(), "http: POST too large")
			}
			if read.n > 10<<20+4096 {
				t.Errorf("read %d bytes of the body, want it to stop at 10 MB", read.n)
			}
			// The same bytes under another content type are not a form.
			req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(big))
			req.Header.Set("Content-Type", "text/plain")
			rec = httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Body.String() == "http: POST too large" {
				t.Errorf("a text/plain body was read as a form: %d %q", rec.Code, rec.Body.String())
			}
		})
	}
}

// countingReader counts the bytes read from it.
type countingReader struct {
	r interface{ Read([]byte) (int, error) }
	n int
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += n
	return n, err
}
