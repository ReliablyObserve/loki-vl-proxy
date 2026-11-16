package proxy

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

// keywordFoldAnswer is a row of the Loki answers internal/logql records for keywords written with a fold rune
// (TestKeywordFoldRunesMatchLoki): the query, its lower case spelling and whether Loki accepts it, with its error.
type keywordFoldAnswer struct {
	Q      string `json:"q"`
	Ref    string `json:"ref"`
	Accept bool   `json:"accept"`
	Error  string `json:"error"`
}

func readKeywordFoldAnswers(t *testing.T) []keywordFoldAnswer {
	t.Helper()
	return readLexerEdgeAnswerFile(t, "../logql/testdata/keyword-fold-runes-loki.jsonl.gz")
}

// readLexerEdgeAnswerFile reads one of the Loki answer files internal/logql records for its lexer edge grids.
func readLexerEdgeAnswerFile(t *testing.T, path string) []keywordFoldAnswer {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []keywordFoldAnswer
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var a keywordFoldAnswer
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if len(out) < 300 {
		t.Fatalf("%d recorded answers: the grid is too small", len(out))
	}
	return out
}

// A keyword written with the Kelvin sign (U+212A) for a k or U+0130 for an i (`topK(`, `bottomK(`, `| Keep`,
// `avg_over_tİme(`, `wİthout`) is the lower case query to Loki (recorded Loki answers, internal/logql
// TestKeywordFoldRunesMatchLoki), so the proxy sends VictoriaLogs the requests of the lower case spelling and answers
// the client the same, on query_range and on query for a metric query: before, the string translator read `topK(` as
// no ranking and sent the whole query as a phrase. A spelling Loki rejects is Loki's 400 with Loki's text, before any
// VictoriaLogs request. A letter Loki reads as a name letter (U+017F, ſ) is left to the grid of names.
//
// conformance: semantics/keyword-fold-runes-read-as-keyword
func TestKeywordFoldRunesRequestsEqualLowerCase(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vl.Close()
	rejector := newCompatTestProxy(t, vl.URL)
	served, rejected := 0, 0
	for _, a := range readKeywordFoldAnswers(t) {
		if a.Ref == "" || strings.Contains(a.Q, "ſ") || strings.Contains(a.Q, "é") {
			continue
		}
		endpoints := []string{"query_range", "query"}
		if strings.HasPrefix(a.Q, "{") {
			endpoints = endpoints[:1] // a log query is no instant query
		}
		for _, endpoint := range endpoints {
			if !a.Accept {
				params := url.Values{"query": {a.Q}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
				before := upstream.Load()
				rec := doCompatProxyRequest(rejector, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
				if body := rec.Body.String(); rec.Code != http.StatusBadRequest || !strings.Contains(body, a.Error) && !strings.Contains(body, jsonQuoted(a.Error)) {
					t.Errorf("%s %q: %d %s, want Loki's 400 %q", endpoint, a.Q, rec.Code, body, a.Error)
				}
				if calls := upstream.Load() - before; calls != 0 {
					t.Errorf("%s %q: %d requests reached VictoriaLogs", endpoint, a.Q, calls)
				}
				rejected++
				continue
			}
			// The lower case spelling of a few rows reaches no VictoriaLogs request (approx_topk on query_range is the
			// proxy's error answer): the folded spelling must give the same answer.
			wantVL, wantBody := blankAfterKeywordRequests(t, endpoint, a.Ref)
			gotVL, gotBody := blankAfterKeywordRequests(t, endpoint, a.Q)
			racy := requestSetVaries(a.Ref)
			if !racy && !slices.Equal(gotVL, wantVL) {
				t.Errorf("%s %q differs from %q at %s", endpoint, a.Q, a.Ref, firstDifference(strings.Join(gotVL, "\n"), strings.Join(wantVL, "\n")))
			}
			if gotBody != wantBody {
				t.Errorf("%s %q: answer %s, want %s", endpoint, a.Q, gotBody, wantBody)
			}
			if len(wantVL) > 0 {
				served++
			}
		}
	}
	if served < 80 || rejected < 80 {
		t.Fatalf("%d served and %d rejected spellings: the grid is too small", served, rejected)
	}
	t.Logf("%d spellings answered as the lower case query, %d rejected with Loki's text", served, rejected)
}

// jsonQuoted is s as the JSON error body writes it, without the quotes.
func jsonQuoted(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

// A keyword written with a fold rune reads the response cache entry of the lower case spelling: the request path
// writes the query in lower case before the cache key is built (lowercaseQueryKeywords), so a repeat in either
// spelling costs no VictoriaLogs request, and the key of the lower case spelling, which every client sends, does not
// change. The answers a build before this one cached under the fold rune's own text (a phrase search) are never read
// again: no key holds that text any more, so the fences of these constructs keep their versions.
//
// conformance: semantics/keyword-fold-runes-read-as-keyword, cache-response-key-versions
func TestKeywordFoldRunesShareTheLowerCaseCacheEntry(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"{app=\"a\"}","level":"info"},"values":[[1700000000,"1"]]}]}}`))
	}))
	defer vl.Close()
	for _, tc := range []struct{ lower, folded string }{
		{`topk(1, rate({app="a"}[1m]))`, "topK(1, rate({app=\"a\"}[1m]))"},
		{`bottomk(1, rate({app="a"}[1m]))`, "bottomK(1, rate({app=\"a\"}[1m]))"},
		{`sum by (app) (avg_over_time({app="a"} | json | unwrap n [1m]))`, "sum by (app) (avg_over_tİme({app=\"a\"} | json | unwrap n [1m]))"},
		{`sum without (app) (rate({app="a"}[1m]))`, "sum wİthout (app) (rate({app=\"a\"}[1m]))"},
	} {
		p := newCompatTestProxy(t, vl.URL)
		ask := func(q string) (int64, string) {
			params := url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}}
			before := upstream.Load()
			rec := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
			if rec.Code != http.StatusOK {
				t.Fatalf("%q: %d %s", q, rec.Code, rec.Body.String())
			}
			return upstream.Load() - before, rec.Body.String()
		}
		cold, want := ask(tc.lower)
		if cold == 0 {
			t.Fatalf("%q: no VictoriaLogs request on a cold cache", tc.lower)
		}
		if calls, got := ask(tc.folded); calls != 0 || got != want {
			t.Errorf("%q after %q: %d VictoriaLogs requests, answer %s, want 0 and %s", tc.folded, tc.lower, calls, got, want)
		}
		if calls, _ := ask(tc.lower); calls != 0 {
			t.Errorf("%q repeated: %d VictoriaLogs requests, want 0", tc.lower, calls)
		}
	}
}

// A tab, a line feed, a carriage return or a run of them after any keyword or before a call's parenthesis is the
// spaced query to Loki (recorded Loki answers, internal/logql TestKeywordBlankMatchesLoki): the proxy sends
// VictoriaLogs the requests of the spaced spelling and answers the same, on query_range and on query for a metric
// query. A character Loki does not skip there is Loki's 400 with Loki's text before any VictoriaLogs request. The
// spellings the string translator still reads unlike Loki are known differences of their own, pinned here so a
// change shows: a blank before the parenthesis of a range function or of vector()
// (semantics/translator-reads-blank-before-range-call-as-text, semantics/vector-call-blank-before-parenthesis-as-query).
//
// conformance: semantics/translator-reads-only-space-between-tokens, semantics/parser-reads-unicode-blank-as-white-space, semantics/blank-before-call-parenthesis-read-unlike-loki, semantics/translator-reads-blank-before-range-call-as-text, semantics/vector-call-blank-before-parenthesis-as-query
func TestKeywordBlankRequestsEqualSpaced(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	var upstream atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vl.Close()
	rejector := newCompatTestProxy(t, vl.URL)
	served, rejected, known := 0, 0, 0
	knownCases := map[string]int{}
	for _, a := range readLexerEdgeAnswerFile(t, "../logql/testdata/keyword-blank-loki.jsonl.gz") {
		endpoints := []string{"query_range", "query"}
		if strings.HasPrefix(a.Q, "{") {
			endpoints = endpoints[:1]
		}
		for _, endpoint := range endpoints {
			if !a.Accept {
				if strings.HasPrefix(a.Q, "approx_topk") {
					continue // semantics/approx-topk-errors-differ
				}
				params := url.Values{"query": {a.Q}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
				before := upstream.Load()
				rec := doCompatProxyRequest(rejector, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
				if body := rec.Body.String(); rec.Code != http.StatusBadRequest || !strings.Contains(body, a.Error) && !strings.Contains(body, jsonQuoted(a.Error)) {
					t.Errorf("%s %q: %d %s, want Loki's 400 %q", endpoint, a.Q, rec.Code, rec.Body.String(), a.Error)
				}
				if calls := upstream.Load() - before; calls != 0 {
					t.Errorf("%s %q: %d requests reached VictoriaLogs", endpoint, a.Q, calls)
				}
				rejected++
				continue
			}
			wantVL, wantBody := blankAfterKeywordRequests(t, endpoint, a.Ref)
			gotVL, gotBody := blankAfterKeywordRequests(t, endpoint, a.Q)
			racy := requestSetVaries(a.Ref)
			same := (racy || slices.Equal(gotVL, wantVL)) && gotBody == wantBody
			if caseID := blankKnownDifference(a.Q); caseID != "" && !same {
				known++
				knownCases[caseID]++
				continue
			}
			if !same {
				t.Errorf("%s %q: %d requests, answer %s;\n want the spaced spelling's %d requests, answer %s", endpoint, a.Q, len(gotVL), gotBody, len(wantVL), wantBody)
			}
			served++
		}
	}
	if served < 300 || rejected < 300 {
		t.Fatalf("%d served and %d rejected spellings: the grid is too small", served, rejected)
	}
	t.Logf("%d spellings answered as the spaced query, %d rejected with Loki's text, %d known differences %v", served, rejected, known, knownCases)
}

// blankKnownDifference names the registry case of a blank spelling the proxy still answers unlike the spaced one, or
// "": a tab, a line feed or a carriage return between a call's name and its parenthesis.
func blankKnownDifference(q string) string {
	m := blankBeforeCall.FindStringSubmatch(q)
	if m == nil {
		return ""
	}
	switch name := strings.ToLower(m[1]); {
	case name == "vector":
		return "semantics/vector-call-blank-before-parenthesis-as-query"
	case strings.HasSuffix(name, "_over_time") || strings.HasPrefix(name, "rate") || name == "bytes_rate":
		return "semantics/translator-reads-blank-before-range-call-as-text"
	}
	return "semantics/blank-before-call-parenthesis-read-unlike-loki"
}

// blankBeforeCall finds a name followed by blanks with a tab, a line feed or a carriage return in them, and a
// parenthesis.
var blankBeforeCall = regexp.MustCompile(`([A-Za-z_]+)[ ]*[\t\r\n][ \t\r\n]*\(`)

// requestSetVaries reports a metric query whose set of VictoriaLogs requests varies from run to run for one spelling,
// so a test compares its answer and not its requests: a keep under a range function
// (TestBlankAfterKeywordRequestsEqualSpaced), and a json or logfmt parser in a metric query, whose evaluator races a
// raw-row probe against the stats read (the same lower case query sends one set or another, -race shows it within a
// few dozen runs).
func requestSetVaries(query string) bool {
	return !strings.HasPrefix(query, "{") &&
		(strings.Contains(query, "| keep") || strings.Contains(query, "| json") || strings.Contains(query, "| logfmt"))
}
