package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// keepMetricQuery is a `| json` metric the strict split answers: a keep after
// the parser leaves a line Loki's parser rejects to decide the answer.
const keepMetricQuery = `count_over_time({A="0"} | json | keep a [1m])`

// unparsedLineStub is a fake VictoriaLogs whose every row lookup finds a row
// (a line without a brace in the window, for the unparsed-line lookup) and
// whose stats answer one series; each answer waits delay first. It records
// every request it receives as path and sorted parameters.
type unparsedLineStub struct {
	delay time.Duration
	mu    sync.Mutex
	seen  []string
}

func (s *unparsedLineStub) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(body))
		for k, v := range r.URL.Query() {
			form[k] = v
		}
		keys := make([]string, 0, len(form))
		for k := range form {
			if k != "timeout" {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		var b strings.Builder
		b.WriteString(r.URL.Path)
		for _, k := range keys {
			b.WriteString(" " + k + "=" + strings.Join(form[k], ","))
		}
		s.mu.Lock()
		s.seen = append(s.seen, b.String())
		s.mu.Unlock()
		if s.delay > 0 {
			select {
			case <-time.After(s.delay):
			case <-r.Context().Done():
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/query") {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:20:00Z","_stream":"{app=\"a\"}","_msg":"{\"a\":\"1\",\"b\":\"2\"}","a":"1"}` + "\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"{app=\"a\"}"},"values":[[1700000000,"1"]]}]}}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// take returns the requests received since the last take, sorted.
func (s *unparsedLineStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.seen
	s.seen = nil
	slices.Sort(out)
	return out
}

func unparsedLineRequest(t *testing.T, p *Proxy, start int64) string {
	t.Helper()
	params := url.Values{"query": {keepMetricQuery}, "start": {strings.TrimSpace(time.Unix(start, 0).UTC().Format(time.RFC3339))}, "end": {time.Unix(start+600, 0).UTC().Format(time.RFC3339)}, "step": {"60"}}
	rec := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var answer any
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
		t.Fatalf("answer %q: %v", rec.Body.String(), err)
	}
	canon, _ := json.Marshal(answer)
	return string(canon)
}

func lookupsOf(requests []string) int {
	n := 0
	for _, r := range requests {
		if strings.Contains(r, `| filter -_msg:~"^[ \\t\\n\\r]*\\{" | limit 1`) && !strings.Contains(r, "| filter (") {
			n++
		}
	}
	return n
}

func quietProxyLog(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
}

// The strict split looks up a line Loki's parser rejects before it trusts its
// stats buckets; when it finds one, the row evaluator builds the answer from
// the same row instead of asking VictoriaLogs the same question again.
//
// conformance: json-metric-unparsed-line-lookup-once
func TestOrderedJSONUnparsedLine_RowEvaluatorReusesTheRiskChecksRow(t *testing.T) {
	quietProxyLog(t)
	stub := &unparsedLineStub{delay: 20 * time.Millisecond}
	p := newCompatTestProxy(t, stub.server(t).URL)
	_ = unparsedLineRequest(t, p, 1700000000)
	got := stub.take()
	if n := lookupsOf(got); n != 1 {
		t.Fatalf("the unparsed-line lookup went to VictoriaLogs %d times, want once:\n%s", n, strings.Join(got, "\n"))
	}
}

// One spelling sends one set of VictoriaLogs requests. A cold request starts
// the stats query and the parse checks beside the unparsed-line lookup and
// cancels them once the lookup finds a line Loki's parser rejects: with
// VictoriaLogs answering after a round trip (here 20 ms) every one of them
// has been sent by then, and 60 cold requests send one set. Once a row is
// found, a refresh of the window (the next step) starts nothing beside the row
// evaluator: no stats, no checks, only its own lookup of the refreshed window
// (its error names the first row of exactly that window) and its read, the
// same on every refresh. Every request gets a success answer.
//
// conformance: json-metric-unparsed-line-lookup-once
func TestOrderedJSONUnparsedLine_OneSpellingSendsOneRequestSet(t *testing.T) {
	quietProxyLog(t)
	coldSets, answers := map[string]int{}, map[string]int{}
	for i := 0; i < 60; i++ {
		stub := &unparsedLineStub{delay: 20 * time.Millisecond}
		p := newCompatTestProxy(t, stub.server(t).URL)
		answers[unparsedLineRequest(t, p, 1700000000)]++
		coldSets[strings.Join(stub.take(), "\n")]++
		if i%20 == 0 {
			// Refreshes on the same proxy: the window moves by one step.
			var refresh []string
			for j := 1; j <= 5; j++ {
				answers[unparsedLineRequest(t, p, 1700000000+int64(60*j))]++
				got := stub.take()
				if slices.ContainsFunc(got, func(r string) bool { return strings.Contains(r, "stats_query_range") }) || lookupsOf(got) != 1 {
					t.Fatalf("refresh %d started the stats or the checks, or looked up more than once:\n%s", j, strings.Join(got, "\n"))
				}
				if refresh == nil {
					refresh = got
				} else if len(got) != len(refresh) {
					t.Fatalf("refresh %d sent %d requests, the first refresh %d", j, len(got), len(refresh))
				}
			}
			if len(refresh) != 2 {
				t.Fatalf("a refresh sent %d requests, want the row evaluator's lookup and read:\n%s", len(refresh), strings.Join(refresh, "\n"))
			}
		}
	}
	if len(coldSets) != 1 {
		for set, n := range coldSets {
			t.Logf("%d runs:\n%s", n, set)
		}
		t.Fatalf("60 cold requests sent %d different request sets", len(coldSets))
	}
	for answer := range answers {
		if !strings.Contains(answer, `"status":"success"`) {
			t.Fatalf("answer %s", answer)
		}
	}
}

// A remembered row decides only a window that holds its timestamp: a window
// that ends before it looks the line up again.
//
// conformance: json-metric-unparsed-line-lookup-once
func TestOrderedJSONUnparsedLine_RememberedRowOnlyDecidesItsWindow(t *testing.T) {
	quietProxyLog(t)
	stub := &unparsedLineStub{}
	p := newCompatTestProxy(t, stub.server(t).URL)
	ctx := context.WithValue(context.Background(), orgIDKey, "team-a")
	lookup := unparsedLineLookup(`A:="0"`)
	at := time.Date(2023, 11, 14, 22, 20, 0, 0, time.UTC)
	row, err := p.readUnparsedLineRow(ctx, lookup, at.Add(-time.Minute), at.Add(time.Minute))
	if err != nil || row == nil {
		t.Fatalf("row %q, %v", row, err)
	}
	for _, tc := range []struct {
		name     string
		from, to time.Time
		want     bool
	}{
		{"window holding the row", at.Add(-time.Hour), at.Add(time.Hour), true},
		{"row at the window end (inclusive)", at.Add(-time.Minute), at, true},
		{"row at the window start (inclusive bound)", at, at.Add(time.Minute), true},
		{"window ending before the row", at.Add(-time.Hour), at.Add(-time.Nanosecond), false},
		{"window starting after the row", at.Add(time.Nanosecond), at.Add(time.Hour), false},
	} {
		if _, ok := p.cachedUnparsedLineRow(ctx, lookup, tc.from, tc.to, false); ok != tc.want {
			t.Errorf("%s: remembered %v, want %v", tc.name, ok, tc.want)
		}
	}
	// The row evaluator reuses only the row of exactly its own lookup.
	if _, ok := p.cachedUnparsedLineRow(ctx, lookup, at.Add(-time.Minute), at.Add(time.Minute), true); !ok {
		t.Error("the row of the same window is not reused")
	}
	if _, ok := p.cachedUnparsedLineRow(ctx, lookup, at.Add(-time.Hour), at.Add(time.Hour), true); ok {
		t.Error("the row of another window is reused as the first row of this one")
	}
	other := context.WithValue(context.Background(), orgIDKey, "team-b")
	if _, ok := p.cachedUnparsedLineRow(other, lookup, at.Add(-time.Hour), at.Add(time.Hour), false); ok {
		t.Error("another tenant reads the remembered row")
	}
	if _, ok := p.cachedUnparsedLineRow(ctx, unparsedLineLookup(`A:="1"`), at.Add(-time.Hour), at.Add(time.Hour), false); ok {
		t.Error("another lookup reads the remembered row")
	}
}

// A check cancelled by the early-out remembers nothing: the next request runs
// it again instead of reading a verdict the cancelled call never reached.
//
// conformance: json-metric-unparsed-line-lookup-once
func TestCoveredWindowProbe_CancelledCheckRemembersNothing(t *testing.T) {
	stub := &unparsedLineStub{}
	p := newCompatTestProxy(t, stub.server(t).URL)
	ctx := context.WithValue(context.Background(), orgIDKey, "team-a")
	start, end := time.Unix(1700000000, 0), time.Unix(1700000600, 0)
	calls := 0
	check := func(err error) func(from, to time.Time, acc string) (string, bool, error) {
		return func(from, to time.Time, acc string) (string, bool, error) {
			calls++
			return "", false, err
		}
	}
	if _, _, err := p.coveredWindowProbe(ctx, "cancel-test", start, end, check(context.Canceled)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if _, _, err := p.coveredWindowProbe(ctx, "cancel-test", start, end, check(nil)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("%d checks, want the cancelled one and a new one", calls)
	}
	if _, _, err := p.coveredWindowProbe(ctx, "cancel-test", start, end, check(nil)); err != nil || calls != 2 {
		t.Fatalf("a completed clean check was not remembered (%d checks, %v)", calls, err)
	}
}

// BenchmarkUnparsedLineRowMiss is what the strict split adds to a request on
// clean data: one remembered-row lookup that misses, before it starts.
func BenchmarkUnparsedLineRowMiss(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	ctx := context.WithValue(context.Background(), orgIDKey, "team-a")
	lookup := unparsedLineLookup(`app:="api" | unpack_json fields (a)`)
	from, to := time.Unix(1700000000, 0), time.Unix(1700003600, 0)
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := p.cachedUnparsedLineRow(ctx, lookup, from, to, false); ok {
			b.Fatal("hit")
		}
	}
}

// With -json-line-fields-as-line-keys a placeholder row holds a JSON line, so
// the unparsed-line lookup leaves placeholder rows out (extra_filters), as the
// parse-risk probes do; the one shared lookup of the risk check and the row
// evaluator carries the filter.
//
// conformance: json-metric-unparsed-line-lookup-once
func TestOrderedJSONUnparsedLine_LookupLeavesPlaceholderRowsOut(t *testing.T) {
	quietProxyLog(t)
	stub := &unparsedLineStub{}
	p, err := New(Config{BackendURL: stub.server(t).URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error",
		LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, JSONLineFieldsAsLineKeys: true,
		TenantMap: map[string]TenantMapping{"team-a": {AccountID: "10", ProjectID: "0"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if p.placeholderRiskFilter() == "" {
		t.Fatal("the test proxy does not read placeholder rows as JSON lines")
	}
	_ = unparsedLineRequest(t, p, 1700000000)
	got := stub.take()
	lookups := 0
	for _, r := range got {
		if strings.Contains(r, `| filter -_msg:~"^[ \\t\\n\\r]*\\{" | limit 1`) && !strings.Contains(r, "| filter (") {
			lookups++
			if !strings.Contains(r, " extra_filters=") {
				t.Errorf("a lookup without the placeholder filter: %s", r)
			}
		}
	}
	if lookups != 1 {
		t.Fatalf("%d lookups, want 1:\n%s", lookups, strings.Join(got, "\n"))
	}
}
