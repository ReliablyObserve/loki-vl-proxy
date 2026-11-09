//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// An ip() label filter on a value with an IPv4 address inside other text ("10.0.0.5:443", "client 10.0.0.5")
// keeps the lines Loki keeps, under = and !=: the address is scanned out of the value as Loki scans it. A
// missing label is dropped under both operators.
// conformance: semantics/ip-label-filter-embedded-address, loki_api_v1_query_range
func TestCompat_IPLabelFilterEmbeddedAddressLikeLoki(t *testing.T) {
	app := fmt.Sprintf("ip-embedded-%d", time.Now().UnixNano())
	values := []string{"10.0.0.5", "10.0.0.5:443", "client 10.0.0.5", "[10.0.0.5]:80", "192.168.1.1", "192.168.1.1:80", "010.0.0.5", "10.0.0.256", "no address"}
	rows := make([]map[string]string, len(values))
	lines := make([]string, len(values))
	for i, v := range values {
		rows[i] = map[string]string{"addr": v}
		lines[i] = fmt.Sprintf("line %d", i)
	}
	start, end := pushLabelFixture(t, app, rows, lines)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	for _, stage := range []string{
		`| addr=ip("10.0.0.0/8")`, `| addr!=ip("10.0.0.0/8")`,
		`| addr=ip("10.0.0.5")`, `| addr!=ip("10.0.0.5")`,
		`| addr=ip("192.168.1.1-192.168.1.9")`, `| addr!=ip("192.168.1.1-192.168.1.9")`,
		`| addr=ip("10.0.0.0/8") | addr!=ip("10.0.0.5")`,
	} {
		q := fmt.Sprintf(`{app=%q} %s`, app, stage)
		ls, lb, lresp := queryRangeGET(t, lokiURL, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		ps, pb, presp := queryRangeGET(t, proxyURL, url.Values{"query": {q}, "start": {start}, "end": {end}, "limit": {"100"}}, headers)
		if ls != http.StatusOK || ps != http.StatusOK {
			t.Errorf("%s: Loki %d %s, proxy %d %s", q, ls, lb, ps, pb)
			continue
		}
		l, p := resultLines(lresp), resultLines(presp)
		if strings.Join(p, "|") != strings.Join(l, "|") {
			t.Errorf("%s:\n proxy %v\n Loki  %v", q, p, l)
		}
	}
}
