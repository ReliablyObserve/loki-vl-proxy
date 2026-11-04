package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeVLIPInRange is VictoriaLogs' ipv4_range / ipv6_range on one value: a
// whole address (lib/logstorage values_encoder.go tryParseIPv4, which takes
// leading zeros and up to nine characters in the last octet and reads a
// two-character octet without checking its second character; parser.go
// tryParseIPv6 = netip.ParseAddr of 2 to 45 characters, IPv4 text included)
// between lo and hi.
func fakeVLIPInRange(value, lo, hi string, v6 bool) bool {
	if !v6 {
		n, ok := fakeVLParseIPv4(value)
		l, _ := fakeVLParseIPv4(lo)
		h, _ := fakeVLParseIPv4(hi)
		return ok && n >= l && n <= h
	}
	if len(value) < 2 || len(value) > 45 {
		return false
	}
	a, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	b, l, h := a.As16(), netip.MustParseAddr(lo).As16(), netip.MustParseAddr(hi).As16()
	return string(b[:]) >= string(l[:]) && string(b[:]) <= string(h[:])
}

func fakeVLParseIPv4(s string) (uint32, bool) {
	if len(s) < 7 || len(s) > 15 || strings.Count(s, ".") != 3 {
		return 0, false
	}
	var out uint32
	for i, p := range strings.Split(s, ".") {
		if len(p) == 0 || len(p) > 9 || i < 3 && len(p) > 3 {
			return 0, false
		}
		var n uint64
		if len(p) == 2 && p[0] >= '0' && p[0] <= '9' {
			n = 10*uint64(p[0]-'0') + uint64(p[1]-'0')
		} else {
			for j := 0; j < len(p); j++ {
				if p[j] < '0' || p[j] > '9' {
					return 0, false
				}
				n = n*10 + uint64(p[j]-'0')
			}
		}
		if n > 255 {
			return 0, false
		}
		out = out<<8 | uint32(n)
	}
	return out, true
}

// ipFilterRows are payment-service logfmt lines: most without an addr key
// (the 2026-11-04 run: Loki 0 lines for addr=ip("1.2.3.4"), the proxy 99),
// some with a whole address in or out of 1.2.3.0/24, an IPv6 address and a
// leading-zero address.
func ipFilterRows(s0 time.Time) []pushdownRow {
	addrs := []string{"", "", "", "1.2.3.4", "", "1.2.3.200", "", "5.6.7.8", "", "2001:db8::1", "", "01.2.3.4"}
	rows := make([]pushdownRow, 0, 120)
	for i := 0; i < 120; i++ {
		msg := fmt.Sprintf(`level=info msg="charge" n=%d`, i)
		if a := addrs[i%len(addrs)]; a != "" {
			msg += " addr=" + a
		}
		rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(i) * 5 * time.Second), stream: map[string]string{"service_name": "payment-service"}, msg: msg})
	}
	return rows
}

// lokiIPLineKept is Loki's answer for a whole-address or absent addr
// (pkg/logql/log/ip.go:123-145): an absent label never matches, = keeps a
// line whose address netip parses and the pattern contains, != the others.
func lokiIPLineKept(msg, pattern string, negate bool) bool {
	_, after, found := strings.Cut(msg, " addr=")
	if !found {
		return false
	}
	in := false
	if a, err := netip.ParseAddr(after); err == nil {
		if p, perr := netip.ParsePrefix(pattern); perr == nil {
			in = p.Contains(a)
		} else {
			in = netip.MustParseAddr(pattern) == a
		}
	}
	return in != negate
}

// The ip() label filter answers like Loki on log and metric queries: a line
// without the label is dropped under = and !=, a whole address in the
// pattern is kept by = and dropped by !=, a leading-zero address holds no
// address. The queries are those of the 2026-11-04 run and their != and CIDR
// forms.
// conformance: semantics/ip-label-filter-missing-label, loki_api_v1_query_range
func TestIPLabelFilter_LogsAndMetricsLikeLoki(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	rows := ipFilterRows(s0)
	srv, _ := newPushdownFakeVL(t, rows, nil)
	defer srv.Close()
	p := newFilterPushdownProxy(t, srv.URL)
	start, end := s0, s0.Add(10*time.Minute)
	for _, tc := range []struct {
		pattern string
		negate  bool
	}{{"1.2.3.4", false}, {"1.2.3.4", true}, {"1.2.3.0/24", false}, {"1.2.3.0/24", true}, {"2001:db8::/32", false}, {"2001:db8::/32", true}} {
		op := "="
		if tc.negate {
			op = "!="
		}
		want := 0
		for _, r := range rows {
			if lokiIPLineKept(r.msg, tc.pattern, tc.negate) {
				want++
			}
		}
		logQuery := fmt.Sprintf(`{service_name="payment-service"} | logfmt | addr%sip(%q)`, op, tc.pattern)
		params := url.Values{"query": {logQuery}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"1000"}}
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
		var logs struct {
			Data struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &logs); err != nil || rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", logQuery, rec.Code, rec.Body)
		}
		got := 0
		for _, s := range logs.Data.Result {
			got += len(s.Values)
		}
		if got != want {
			t.Errorf("%s: %d lines, Loki %d", logQuery, got, want)
		}
		metric := fmt.Sprintf(`sum(count_over_time({service_name="payment-service"} | logfmt | addr%sip(%q) [10m]))`, op, tc.pattern)
		params = url.Values{"query": {metric}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}
		rec = httptest.NewRecorder()
		p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
		var vec struct {
			Data struct {
				Result []struct {
					Value []any `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &vec); err != nil || rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", metric, rec.Code, rec.Body)
		}
		gotMetric := "0"
		if len(vec.Data.Result) == 1 {
			gotMetric, _ = vec.Data.Result[0].Value[1].(string)
		}
		if wantMetric := strconv.Itoa(want); gotMetric != wantMetric && (want != 0 || len(vec.Data.Result) != 0) {
			t.Errorf("%s: %s, Loki %s", metric, gotMetric, wantMetric)
		}
	}
}
