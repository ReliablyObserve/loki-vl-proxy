package proxy

import (
	"context"
	"encoding/json"
	"fmt"
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
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// blankGroupingTemplates are metric queries with a grouping clause %s (each `%s` is the clause): the clause in front of
// its operand or after it, a rate and a count over time under it, a ranking, an unwrapped range, a level over a level
// and a binary operation.
var blankGroupingTemplates = []string{
	`sum %s (rate({A="0"}[1m]))`,
	`sum(rate({A="0"}[1m])) %s`,
	`count %s (rate({A="0"}[1m]))`,
	`avg %s (rate({A="0"}[1m]))`,
	`min %s (bytes_rate({A="0"}[1m]))`,
	`max %s (rate({A="0"}[1m]))`,
	`sum %s (count_over_time({A="0"}[1m]))`,
	`sum %s (count_over_time({A="0"} | json [1m]))`,
	`sum %s (bytes_over_time({A="0"} | logfmt | keep app [1m]))`,
	`topk(1, rate({A="0"}[1m])) %s`,
	`topk %s (1, rate({A="0"}[1m]))`,
	`bottomk(2, sum %s (rate({A="0"}[1m]))) %s`,
	`stddev %s (rate({A="0"}[1m]))`,
	`sum %s (sum %s (rate({A="0"}[1m])))`,
	`max_over_time({A="0"} | logfmt | unwrap n [1m]) %s`,
	`sum %s (max_over_time({A="0"} | logfmt | unwrap n [1m]))`,
	`sum %s (rate({A="0"}[1m])) / sum %s (rate({A="0"}[1m]))`,
}

// blankGroupings are the spellings of an empty grouping that hold only blanks Loki's lexer skips.
var blankGroupings = []string{"( )", "(  )", "(\t)", "(\n)", "(\r\n)", "( \t )", "( # a comment\n)"}

// blankGroupingRequests answers query on the proxy as shipped and returns what reached VictoriaLogs, in order, and the
// client's answer.
func blankGroupingRequests(t *testing.T, endpoint, query string) ([]string, string) {
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
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"_stream":"{app=\"a\"}","level":"info"},"values":[[1700000000,"1"]]},{"metric":{"_stream":"{app=\"b\"}","level":"info"},"values":[[1700000000,"3"]]}]}}`))
	}))
	defer vl.Close()
	p := newCompatTestProxy(t, vl.URL)
	params := url.Values{"query": {query}, "start": {"1700000000"}, "end": {"1700000600"}, "step": {"60"}, "time": {"1700000600"}}
	rec := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "team-a"})
	// The labels of a series come in no fixed order: compare the answers as values.
	var answer any
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
		return seen, rec.Body.String()
	}
	canon, _ := json.Marshal(answer)
	return seen, string(canon)
}

// A grouping with blanks and no label (`by( )`) is an empty grouping, as Loki's tree holds it (test/oracle/loki facts,
// semantics/translator-reads-blank-grouping-as-none): the proxy sends VictoriaLogs and answers the client exactly what
// it does for `by ()`, whichever aggregation, operand and route the query takes. The string translator reads the blank
// grouping as no grouping at all and writes a different text for a rate under it, which the query plan does not copy.
//
// conformance: semantics/translator-reads-blank-grouping-as-none
func TestBlankGroupingRequestsEqualEmptyGrouping(t *testing.T) {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
	for _, endpoint := range []string{"query_range", "query"} {
		for _, tmpl := range blankGroupingTemplates {
			for _, kw := range []string{"by"} { // `without ( )` is a clause with a label for the translator, see the case
				ref := strings.ReplaceAll(tmpl, "%s", kw+" ()")
				wantVL, wantBody := blankGroupingRequests(t, endpoint, ref)
				if len(wantVL) == 0 || !strings.Contains(wantBody, `"status":"success"`) {
					t.Fatalf("%s %s: the reference is not an answer (%d requests to VictoriaLogs): %s", endpoint, ref, len(wantVL), wantBody)
				}
				for _, blank := range blankGroupings {
					q := strings.ReplaceAll(tmpl, "%s", kw+blank)
					gotVL, gotBody := blankGroupingRequests(t, endpoint, q)
					if !slices.Equal(gotVL, wantVL) {
						t.Errorf("%s %q differs from %q at %s", endpoint, q, ref, firstDifference(strings.Join(gotVL, "\n"), strings.Join(wantVL, "\n")))
					}
					if gotBody != wantBody {
						t.Errorf("%s %q: answer %s, want %s", endpoint, q, gotBody, wantBody)
					}
				}
			}
		}
	}
}

// firstDifference says where a and b first differ, with the text around it.
func firstDifference(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	around := func(s string) string { return s[max(0, i-60):min(len(s), i+100)] }
	return fmt.Sprintf("byte %d: %q, want %q", i, around(a), around(b))
}

// BenchmarkBlankGroupingTranslate is the translation of a rate under `by ()` and under `by( )` on the plan path the
// proxy serves: the two spellings cost the same, because the plan reads the clause from the tree.
func BenchmarkBlankGroupingTranslate(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.translationCache = nil
	withFlipped(p, metricFlipCandidates...)
	for _, q := range []string{`sum by () (rate({A="0"}[1y]))`, `sum by( ) (rate({A="0"}[1y]))`, `sum(rate({A="0"}[1y]))`} {
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := p.translateQueryOpts(context.Background(), q, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A grouping on a range aggregation that takes none is Loki's error whether it holds labels, nothing or blanks, with
// Loki's text (recorded with the Loki oracle, facts command): `rate(..) by( )` is rejected as `by ()` is, before any
// translation.
//
// conformance: semantics/translator-reads-blank-grouping-as-none
func TestBlankGroupingOnRangeAggregationIsLokisError(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`rate({A="0"}[1m]) by ()`, "parse error : grouping not allowed for rate aggregation"},
		{`rate({A="0"}[1m]) by( )`, "parse error : grouping not allowed for rate aggregation"},
		{"rate({A=\"0\"}[1m]) by(\t)", "parse error : grouping not allowed for rate aggregation"},
		{`rate({A="0"}[1m]) without( )`, "parse error : grouping not allowed for rate aggregation"},
		{`count_over_time({A="0"}[1m]) by ( )`, "parse error : grouping not allowed for count_over_time aggregation"},
	} {
		if err := logqlpkg.ParseAndValidate(tc.query); err == nil || err.Error() != tc.want {
			t.Errorf("%q: %v, want %q", tc.query, err, tc.want)
		}
	}
}
