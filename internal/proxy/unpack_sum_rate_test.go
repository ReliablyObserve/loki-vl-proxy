package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// An ungrouped sum of a count or a rate over an unpack takes VictoriaLogs' stats at an instant, as at a range: the
// evaluator over raw rows grouped a rate by the stream. Every other shape keeps its route.
//
// conformance: semantics/unpack-instant-sum-rate-per-stream, semantics/unpack-instant-grouped-sum-reads-every-line
func TestIsUnpackSumAll(t *testing.T) {
	for q, want := range map[string]bool{
		`sum by (x) (rate({a="b"} | unpack [1m]))`:                     true,
		`sum(count_over_time({a="b"} | unpack [1m])) by (x, y)`:        true,
		`sum by () (rate({a="b"} | unpack [1m]))`:                      true,
		`sum without (x) (rate({a="b"} | unpack [1m]))`:                false,
		`max by (x) (rate({a="b"} | unpack [1m]))`:                     false,
		`sum by (x) (bytes_rate({a="b"} | unpack [1m]))`:               false,
		`sum by (x) (sum_over_time({a="b"} | unpack | unwrap y [1m]))`: false,
		`sum by (x) (rate({a="b"} | json [1m]))`:                       false,
		`rate({a="b"} | unpack [1m])`:                                  false,
	} {
		if got := isUnpackSum(RequestQueryText(q)); got != want {
			t.Errorf("isUnpackSum %s: %v, want %v", q, got, want)
		}
	}
	for q, want := range map[string]bool{
		`sum(rate({a="b"} | unpack [1m]))`:                                     true,
		`sum(count_over_time({a="b"} | unpack [1m]))`:                          true,
		`sum(rate({a="b"} |= "k" | unpack | drop x [1m] offset 5m))`:           true,
		`SUM(RATE({a="b"} | UNPACK [1m]))`:                                     true,
		`sum(rate({a="b"} | unpack | x="1" [1m]))`:                             true,
		`label_replace(sum(rate({a="b"} | unpack [1m])), "d", "x", "d", ".*")`: true,
		`sum by (x) (rate({a="b"} | unpack [1m]))`:                             false,
		`sum without (x) (rate({a="b"} | unpack [1m]))`:                        false,
		`max(rate({a="b"} | unpack [1m]))`:                                     false,
		`rate({a="b"} | unpack [1m])`:                                          false,
		`sum(bytes_rate({a="b"} | unpack [1m]))`:                               false,
		`sum(rate({a="b"} | json [1m]))`:                                       false,
		`sum(rate({a="b"} |= "unpack" [1m]))`:                                  false,
		`sum(sum_over_time({a="b"} | unpack | unwrap x [1m]))`:                 false,
		`{a="b"} | unpack`:                                                     false,
	} {
		if got := isUnpackSumAll(RequestQueryText(q)); got != want {
			t.Errorf("%s (as the handlers read it): %v, want %v", q, got, want)
		}
	}
}

// The fence moves the key of a metric query that names unpack, on every response key, and of no other.
//
// conformance: semantics/unpack-instant-sum-rate-per-stream, semantics/ungrouped-sum-skips-unpack
func TestUnpackMetricFence(t *testing.T) {
	key := func(q string, site responseKeySite) string {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {q}}.Encode(), nil)
		return responseKeyFences(r, q, site)
	}
	for q, want := range map[string]bool{
		`sum(rate({a="b"} | unpack [1m]))`:       true,
		`sum(RATE({a="b"} | UnPack [1m]))`:       true,
		`count_over_time({a="b"} | unpack [1m])`: true,
		`sum(rate({a="b"} | json [1m]))`:         false,
		`sum(rate({a="b"}[1m]))`:                 false,
		`{a="b"} | unpack`:                       false,
		`count_over_time({a="b"} |= "k" [1m])`:   false,
		// The answers a topk over a sum behind a keep or with an empty by clause gave before
		// semantics/ungrouped-sum-skips-unpack (v2).
		`topk(1, sum(rate({a="b"} | unpack | keep c [1m])))`:          true,
		`topk(1, sum by () (count_over_time({a="b"} | unpack [1m])))`: true,
	} {
		for _, site := range []responseKeySite{responseKeyCompatQuery, responseKeyCompatQueryRange, responseKeyInnerQueryRange} {
			if got := strings.Contains(key(q, site), unpackMetricResponseVersion); got != want {
				t.Errorf("%s at site %d: fenced %v, want %v", q, site, got, want)
			}
			// A key written before v2 is not read back.
			if want && strings.Contains(key(q, site), "@unpack-metric-v1:") {
				t.Errorf("%s at site %d: keyed under the v1 fence", q, site)
			}
		}
	}
}
