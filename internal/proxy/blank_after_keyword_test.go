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
	return blankAfterKeywordRequestsLowering(t, true, endpoint, query)
}

// blankAfterKeywordRequestsLowering is blankAfterKeywordRequests with the query plan serving (the default) or not
// (-query-plan-lowering=false, the string translator).
func blankAfterKeywordRequestsLowering(t *testing.T, lowering bool, endpoint, query string) ([]string, string) {
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
	p.queryPlanLowering = lowering
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
// name "json"), or finds no `| unwrap ` and fails an unwrap with no parser before it with Loki's fixed 500
// (`max_over_time({A="0"} | unwrap<CR>A [1m])`); the query plan, which serves these constructs, reads the tree.
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
			// A keep under a range function is answered by the strict JSON split, which starts its stats query and parse
			// checks beside the unparsed-line lookup and cancels them once the lookup finds a line Loki's parser rejects
			// (this fake's lines); with a fake that answers at once, whether a cancelled call reached it first is a race
			// (3 sets in 60 runs under -race). Its answer is compared, its requests are not; with a round trip of latency
			// the set is fixed (TestOrderedJSONUnparsedLine_OneSpellingSendsOneRequestSet).
			racy := !strings.HasPrefix(tpl, "{") && strings.Contains(tpl, "| keep")
			ref := strings.ReplaceAll(tpl, blankspace.Mark, " ")
			wantVL, wantBody := blankAfterKeywordRequests(t, endpoint, ref)
			if len(wantVL) == 0 {
				t.Fatalf("%s %s: the reference reaches no VictoriaLogs request (%s)", endpoint, ref, wantBody)
			}
			for _, blank := range blankspace.Skipped {
				q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
				gotVL, gotBody := blankAfterKeywordRequests(t, endpoint, q)
				rows++
				if !racy && !slices.Equal(gotVL, wantVL) {
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
	t.Logf("%d blank spellings compared with the spaced spelling", rows)
}

// With the query plan off (-query-plan-lowering=false) the string translator answers a log query with a tab, a line
// feed or a carriage return after json, logfmt, regexp, pattern, label_format, line_format, drop, keep, unpack or
// decolorize, or in a label filter, in a text VictoriaLogs rejects: the rows of the table that
// TestBlankAfterKeywordRequestsEqualSpaced holds equal to the spaced spelling differ here. The test pins that the
// difference is the translator's and that the plan removes it (semantics/translator-reads-only-space-between-tokens, open
// until the translator is deleted): when it stops failing here, update the case.
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestBlankAfterKeywordRollbackFlagIsTheTranslatorsMisreading(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	for _, tpl := range []string{`{A="0"} | json` + blankspace.Mark + `A0`, `{A="0"} | logfmt` + blankspace.Mark + `a`, "{A=\"0\"} | regexp" + blankspace.Mark + "`(?P<a>x)`",
		`{A="0"} | pattern` + blankspace.Mark + `"<a> x"`, `{A="0"} | json | line_format` + blankspace.Mark + `"x"`} {
		ref := strings.ReplaceAll(tpl, blankspace.Mark, " ")
		want, _ := blankAfterKeywordRequestsLowering(t, false, "query_range", ref)
		for _, blank := range []string{"\t", "\n", "\r", "\r\n"} {
			q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
			if got, _ := blankAfterKeywordRequestsLowering(t, false, "query_range", q); slices.Equal(got, want) {
				t.Errorf("%q: the string translator sends VictoriaLogs what it does for %q; update semantics/translator-reads-only-space-between-tokens", q, ref)
			}
			if got, _ := blankAfterKeywordRequestsLowering(t, true, "query_range", q); !slices.Equal(got, mustRequests(t, ref)) {
				t.Errorf("%q: the query plan sends VictoriaLogs something other than it does for %q", q, ref)
			}
		}
	}
}

// mustRequests is what reaches VictoriaLogs for a query on the proxy as shipped.
func mustRequests(t *testing.T, query string) []string {
	t.Helper()
	got, _ := blankAfterKeywordRequests(t, "query_range", query)
	return got
}

// A character Loki's lexer does not skip (a vertical tab, a form feed, U+0085, U+00A0, a control byte) where a blank
// stands is Loki's parse error: a 400 on every route that takes the query (query_range and query for a metric query,
// query_range and tail for a log query), answered before VictoriaLogs is asked.
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
		routes := []string{"query_range", "query"}
		if strings.HasPrefix(tpl, "{") {
			routes = []string{"query_range", "tail"}
		}
		for _, blank := range blankspace.Rejected {
			q := strings.ReplaceAll(tpl, blankspace.Mark, blank)
			for _, route := range routes {
				params := url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
				before := upstream.Load()
				rec := doCompatProxyRequest(p, "/loki/api/v1/"+route+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
				rows++
				if rec.Code != http.StatusBadRequest {
					t.Errorf("%s %q: %d %s, want Loki's 400", route, q, rec.Code, rec.Body.String())
				}
				if calls := upstream.Load() - before; calls != 0 {
					t.Errorf("%s %q: %d requests reached VictoriaLogs", route, q, calls)
				}
			}
		}
	}
	if rows < 500 {
		t.Fatalf("%d rows: the grid is too small", rows)
	}
}

// An unwrap with a blank the string translator does not read as a space, under every wrapper the proxy evaluates
// itself or Loki keeps the query through (a ranking, a sort, label_replace, a binary operation, an aggregation, an
// offset), gets the requests and the answer of the spaced spelling: the proxy translates the operand, which the plan
// serves (semantics/translator-reads-only-space-between-tokens).
//
// conformance: semantics/translator-reads-only-space-between-tokens
func TestUnwrapBlankUnderWrappersEqualsSpaced(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	for _, tpl := range []string{
		`topk(3, sum_over_time({A="0"} | json |¤unwrap¤a [5m]))`, `bottomk(1, max_over_time({A="0"} |¤unwrap¤a [5m]))`,
		`sort(max_over_time({A="0"} |¤unwrap¤a [5m]))`, `sort_desc(sum by (A) (sum_over_time({A="0"} |¤unwrap¤a [5m])))`,
		`label_replace(max_over_time({A="0"} |¤unwrap¤a [5m]), "b", "$1", "A", "(.*)")`,
		`sum(max_over_time({A="0"} |¤unwrap¤a [5m])) / sum(count_over_time({A="0"} [5m]))`,
		`max_over_time({A="0"} |¤unwrap¤a [5m]) > 1`, `max_over_time({A="0"} |¤unwrap¤a [5m] offset 5m)`,
		`avg by (A) (avg_over_time({A="0"} | logfmt |¤unwrap¤duration(a) [5m]))`, `rate({A="0"} |¤unwrap¤a [5m])`,
		`quantile_over_time(0.5, {A="0"} |¤unwrap¤c [5m]) by (d)`, `topk(1, rate({A="0"} |¤unwrap¤a [5m]))`,
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			wantVL, wantBody := blankAfterKeywordRequests(t, endpoint, strings.ReplaceAll(tpl, "¤", " "))
			for _, blank := range []string{"\t", "\r\n", "  "} {
				q := strings.ReplaceAll(tpl, "¤", blank)
				gotVL, gotBody := blankAfterKeywordRequests(t, endpoint, q)
				if !requestSetVaries(tpl) && !slices.Equal(gotVL, wantVL) || gotBody != wantBody {
					t.Errorf("%s %q: %d requests, answer %s;\n want the spaced spelling's %d requests, answer %s", endpoint, q, len(gotVL), gotBody, len(wantVL), wantBody)
				}
			}
		}
	}
}
