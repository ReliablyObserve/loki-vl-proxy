package proxy

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
)

// blankAfterKeywordRequests answers query on the proxy as shipped, with VictoriaLogs answering each endpoint in its own
// format (an empty set of rows for a query, a matrix for a stats query), and returns what reached VictoriaLogs, sorted
// (a query that reads VictoriaLogs more than once, a keep or a ranking, sends its requests in no fixed order), and the
// client's answer.
func blankAfterKeywordRequests(t *testing.T, endpoint, query string) ([]string, string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(body))
		for k, v := range r.URL.Query() {
			form[k] = v
		}
		keys := make([]string, 0, len(form))
		for k := range form {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		var b strings.Builder
		b.WriteString(r.URL.Path)
		for _, k := range keys {
			b.WriteString(" " + k + "=" + strings.Join(form[k], ","))
		}
		mu.Lock()
		seen = append(seen, b.String())
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/query") {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_stream":"{app=\"a\"}","_msg":"{\"a\":\"1\",\"b\":\"2\"}","a":"1"}` + "\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"{app=\"a\"}","level":"info"},"values":[[1700000000,"1"]]},{"metric":{"_stream":"{app=\"b\"}","level":"info"},"values":[[1700000000,"3"]]}]}}`))
	}))
	defer vl.Close()
	p := newCompatTestProxy(t, vl.URL)
	params := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
	rec := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
	var answer any
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
		slices.Sort(seen)
		return seen, rec.Body.String()
	}
	canon, _ := json.Marshal(answer)
	slices.Sort(seen)
	return seen, string(canon)
}

// A tab, a line feed, a carriage return or a run of them where a space stands after a parser keyword, a stage name, an
// unwrap conversion, a grouping keyword or a binary operator is the same query to Loki (internal/logql
// TestBlankAfterKeywordMatchesLoki, recorded Loki answers), so the proxy sends VictoriaLogs the same requests and answers
// the client the same for every spelling, on query_range and query, for a log query and a metric query. The string
// translator reads only the space and copies the rest into a pipe VictoriaLogs rejects (`| json<CR>A0`: unexpected pipe
// name "json"); the query plan, which serves these constructs, reads the tree.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestBlankAfterKeywordRequestsEqualSpaced(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	rows := 0
	for _, endpoint := range []string{"query_range", "query"} {
		for _, tpl := range blankspace.Templates() {
			if strings.HasPrefix(tpl, "{") && endpoint == "query" {
				continue // a log query is no instant query
			}
			if !strings.HasPrefix(tpl, "{") && strings.Contains(tpl, "| keep") {
				// A keep under a range function reads VictoriaLogs through a route whose set of requests varies from run
				// to run for one spelling (60 runs of `keep a` sent five different sets under -race): nothing to compare.
				continue
			}
			ref := strings.ReplaceAll(tpl, blankspace.Mark, " ")
			wantVL, wantBody := blankAfterKeywordRequests(t, endpoint, ref)
			if len(wantVL) == 0 {
				t.Fatalf("%s %s: the reference reaches no VictoriaLogs request (%s)", endpoint, ref, wantBody)
			}
			for _, blank := range blankspace.Skipped {
				q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
				gotVL, gotBody := blankAfterKeywordRequests(t, endpoint, q)
				rows++
				if !slices.Equal(gotVL, wantVL) {
					t.Errorf("%s %q differs from %q at %s", endpoint, q, ref, firstDifference(strings.Join(gotVL, "\n"), strings.Join(wantVL, "\n")))
				}
				if gotBody != wantBody {
					t.Errorf("%s %q: answer %s, want %s", endpoint, q, gotBody, wantBody)
				}
			}
		}
	}
	if rows < 500 {
		t.Fatalf("%d rows: the grid is too small", rows)
	}
}

// A character Loki's lexer does not skip (a vertical tab, a form feed, U+0085, U+00A0, a control byte) where a blank
// stands is Loki's parse error: a 400 on every route, answered before VictoriaLogs is asked.
//
// conformance: semantics/parser-reads-unicode-blank-as-white-space
func TestBlankAfterKeywordRejectedNeverReachesVictoriaLogs(t *testing.T) {
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
	p := newCompatTestProxy(t, vl.URL)
	rows := 0
	for _, tpl := range blankspace.Templates() {
		for _, blank := range blankspace.Rejected {
			q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
			endpoint := "query_range"
			params := url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
			before := upstream.Load()
			rec := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
			rows++
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%q: %d %s, want Loki's 400", q, rec.Code, rec.Body.String())
			}
			if calls := upstream.Load() - before; calls != 0 {
				t.Errorf("%q: %d requests reached VictoriaLogs", q, calls)
			}
		}
	}
	if rows < 250 {
		t.Fatalf("%d rows: the grid is too small", rows)
	}
}
