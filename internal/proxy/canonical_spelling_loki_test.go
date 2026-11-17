package proxy

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// canonicalSpellingCases are metric queries spelled with blanks and parentheses Loki reads away, each with the
// spelling the request rewrite writes for it.
var canonicalSpellingCases = []struct{ spelled, canonical string }{
	{`rate ({app="api"}[1m])`, `rate({app="api"}[1m])`},
	{"count_over_time\t({app=\"api\"} | json [1m])", `count_over_time({app="api"} | json [1m])`},
	{`(count_over_time({app="api"}[1m]))`, `count_over_time({app="api"}[1m])`},
	{`((sum by (pod) (rate({app="api"}[1m]))))`, `sum by (pod) (rate({app="api"}[1m]))`},
	{`sum by (pod) ( count_over_time({app="api"}[1m]) )`, `sum by (pod) (count_over_time({app="api"}[1m]))`},
	{`max (count_over_time({app="api"}[1m]))`, `max(count_over_time({app="api"}[1m]))`},
	{`sum(count_over_time({app="api"}[1m])) + vector (1)`, `sum(count_over_time({app="api"}[1m])) + vector(1)`},
	{`vector ( 2 ) * sum by (pod) (rate({app="api"}[1m]))`, `vector(2) * sum by (pod) (rate({app="api"}[1m]))`},
	{`(vector (2) + sum by (pod) (count_over_time({app="api"}[1m]))) * 2`, `(vector(2) + sum by (pod) (count_over_time({app="api"}[1m]))) * 2`},
	{`sum((sum by (pod) (count_over_time({app="api"}[1m])) + 1))`, `sum(sum by (pod) (count_over_time({app="api"}[1m])) + 1)`},
	{`label_replace( sum by (pod) (rate({app="api"}[1m])), "p", "$1", "pod", "(.*)" )`, `label_replace(sum by (pod) (rate({app="api"}[1m])), "p", "$1", "pod", "(.*)")`},
	{`sort_desc (sum by (pod) (count_over_time({app="api"}[1m])))`, `sort_desc(sum by (pod) (count_over_time({app="api"}[1m])))`},
}

// A metric query spelled with blanks or parentheses Loki reads away answers what Loki answers for it (Loki's
// engine over the same lines, recorded with the engine oracle; LVP_RUN_ORACLES=1 records them again), through the
// proxy's routes: a range function with a blank before its parenthesis, a query wrapped in parentheses, a vector(n)
// with blanks, and calls with blanks inside their parentheses. On main the proxy answered some of them with a
// phrase search, a 400 or a 500. The respelled query is also the canonical one to every cache: after the canonical
// spelling, it is answered from the response cache with no VictoriaLogs call and the same series.
//
// conformance: semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-parenthesized-metric-as-text, semantics/vector-call-blank-before-parenthesis-as-query, loki_api_v1_query_range
func TestCanonicalSpelling_AnswersMatchLokiEngine(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	rows := keepRows(rand.New(rand.NewSource(11)), s0, 220, false)
	srv, fake, counts := newKeepFakeVL(t, rows)
	p := newFilterPushdownProxy(t, srv.URL)
	fake.stored = p.labelTranslator.ToVL
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	run := func(query string) *httptest.ResponseRecorder {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	calls := func() int32 { return counts.stats.Load() + counts.raw.Load() }
	cached := 0
	for _, tc := range canonicalSpellingCases {
		if got := requestQueryText(tc.spelled, 0); got != tc.canonical {
			t.Errorf("%q is respelled %q, want %q", tc.spelled, got, tc.canonical)
			continue
		}
		want := lokiPushdownMatrix(t, "canonical_spelling", rows, tc.spelled, start, end, time.Minute)
		canon := run(tc.canonical)
		if canon.Code != http.StatusOK {
			t.Errorf("%q: status %d: %s", tc.canonical, canon.Code, canon.Body)
			continue
		}
		// The canonical spelling's exact repeat, then the respelled query: the same VictoriaLogs calls (none where the
		// response cache answers the repeat).
		before := calls()
		run(tc.canonical)
		repeat := calls() - before
		before = calls()
		rec := run(tc.spelled)
		if rec.Code != http.StatusOK {
			t.Errorf("%q: status %d: %s", tc.spelled, rec.Code, rec.Body)
			continue
		}
		if n := calls() - before; n != repeat {
			t.Errorf("%q made %d VictoriaLogs calls after %q, whose exact repeat makes %d", tc.spelled, n, tc.canonical, repeat)
		}
		if repeat == 0 {
			cached++
		}
		got := decodeExtractedMatrix(t, rec.Body.Bytes())
		if c := decodeExtractedMatrix(t, canon.Body.Bytes()); !sameMatrixValues(got, c) {
			t.Errorf("%q answers\n %v\nwhere %q answers\n %v", tc.spelled, got, tc.canonical, c)
		}
		// Loki's engine oracle derives no detected_level (Loki derives it at ingest, which the oracle's in-memory
		// store skips); the proxy's detected_level of a range aggregation is held to Loki by its own differentials.
		if !sameMatrixValues(withoutMatrixLabel(t, rec.Body.Bytes(), "detected_level"), want) {
			t.Errorf("%q\n got: %v\nwant: %v", tc.spelled, got, want)
		}
	}
	if cached < len(canonicalSpellingCases)-1 {
		t.Errorf("%d of %d respelled queries answered from the cache", cached, len(canonicalSpellingCases))
	}
}

// withoutMatrixLabel is decodeExtractedMatrix of body with the label name left out of every series.
func withoutMatrixLabel(t *testing.T, body []byte, name string) map[string]map[int64]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("invalid matrix: %v: %s", err, body)
	}
	out := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		delete(series.Metric, name)
		key := canonicalLabelsKey(series.Metric)
		if out[key] == nil {
			out[key] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			out[key][int64(ts)], _ = pair[1].(string)
		}
	}
	return out
}

// A respelled query reaches VictoriaLogs as its canonical spelling does: served cold on a fresh proxy, the two make
// the same requests, which the emulated VictoriaLogs records.
//
// conformance: semantics/translator-reads-blank-before-range-call-as-text, semantics/translator-reads-parenthesized-metric-as-text, semantics/vector-call-blank-before-parenthesis-as-query
func TestCanonicalSpelling_SameVictoriaLogsRequests(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	s0 := time.Unix(1700000400, 0).UTC()
	start, end := s0.Add(2*time.Minute), s0.Add(10*time.Minute)
	rows := keepRows(rand.New(rand.NewSource(11)), s0, 220, false)
	requests := func(query string) (int, []string) {
		srv, fake, _ := newKeepFakeVL(t, rows)
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		var mu sync.Mutex
		var sent []string
		inner := p.client.Transport
		p.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			sent = append(sent, r.URL.Path+"?"+r.URL.RawQuery)
			mu.Unlock()
			return inner.RoundTrip(r)
		})
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		mu.Lock()
		defer mu.Unlock()
		return rec.Code, uniqueSorted(sent)
	}
	for _, tc := range canonicalSpellingCases {
		wantCode, want := requests(tc.canonical)
		gotCode, got := requests(tc.spelled)
		if gotCode != wantCode || len(got) != len(want) {
			t.Errorf("%q: %d %v\nwhere %q: %d %v", tc.spelled, gotCode, got, tc.canonical, wantCode, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%q sends %s\nwhere %q sends %s", tc.spelled, got[i], tc.canonical, want[i])
			}
		}
	}
}
