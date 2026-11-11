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

// Loki's lexer skips four characters between tokens: a space, a tab, a line feed and a carriage return. Any other
// character that Go calls white space (a vertical tab, a form feed, U+0085, U+00A0, U+2003) is a token of its own that
// no grammar rule accepts; a [range] is one token up to its ']', so a blank in it is an error of the lexer; a name
// list holds names separated by commas; a double-quoted string ends at a line break. The proxy answers Loki's 400
// with Loki's text for each, byte for byte, where it answered the query before. The service name is unique to the
// run, and no query reaches a backend: all of them are rejected by the parser.
//
// conformance: semantics/logql-loki-whitespace, loki_api_v1_query_range
func TestCompat_LogQLWhitespaceLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	svc := fmt.Sprintf("ws-%d", time.Now().UnixNano())
	sel := fmt.Sprintf(`{service_name=%q}`, svc)
	end := time.Now()
	start := end.Add(-5 * time.Minute)
	for _, query := range []string{
		sel + "\v|= \"x\"",
		sel + " |\vjson",
		sel + " | json",
		sel + " |= \"x\" ",
		sel + " | x=\"a\nb\"",
		fmt.Sprintf("rate(%s[ 5m])", sel),
		fmt.Sprintf("rate(%s[5m ])", sel),
		fmt.Sprintf("rate\v(%s[5m])", sel),
		fmt.Sprintf("sum by (a b) (rate(%s[5m]))", sel),
		fmt.Sprintf("sum by (a,) (rate(%s[5m]))", sel),
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
