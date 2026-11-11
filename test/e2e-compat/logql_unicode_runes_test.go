//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// Loki's lexer (Go's text/scanner) starts a number only with an ASCII digit: a decimal digit of another script, a
// superscript, a vulgar fraction and a Roman numeral start no token, and where the grammar has no place for a name
// the query is a 400 with Loki's text. The proxy answers the same status and the same bytes for each, and returns:
// a decimal digit after a label name once made its parser loop without end. The service name is unique to the run,
// and no query reaches a backend: all of them are rejected by the parser.
//
// conformance: semantics/logql-unicode-runes, loki_api_v1_query_range
func TestCompat_LogQLUnicodeRunesLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	svc := fmt.Sprintf("runes-%d", time.Now().UnixNano())
	sel := fmt.Sprintf(`{service_name=%q}`, svc)
	end := time.Now()
	start := end.Add(-5 * time.Minute)
	for _, query := range []string{
		sel + " | x ٣",
		sel + " | x > ٣",
		sel + " | x=٣",
		sel + " | json | x == ٣1",
		sel + " | x >= 1٣",
		sel + " | x ०",
		sel + " | x ３",
		sel + " | x \U0001d7d1",
		sel + " | x ²",
		sel + " | x ⅓",
		sel + " | x Ⅳ",
		fmt.Sprintf("sum(rate(%s[5m])) > ٣", sel),
	} {
		t.Run(strconv.Quote(query), func(t *testing.T) {
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			ls, lb := hardeningRequest(t, http.MethodGet, lokiURL+"/loki/api/v1/query_range?"+params.Encode(), "", headers)
			ps, pb := hardeningRequest(t, http.MethodGet, proxyURL+"/loki/api/v1/query_range?"+params.Encode(), "", headers)
			if ls != http.StatusBadRequest {
				t.Fatalf("Loki answered %d %s, the test pins 400", ls, lb)
			}
			if ps != ls || string(pb) != string(lb) {
				t.Errorf("proxy %d %q\n Loki  %d %q", ps, pb, ls, lb)
			}
		})
	}
}
