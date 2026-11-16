package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// A ranking, stddev or stdvar over a binary operation on query_range is evaluated by the binary route, operand by
// operand, as the instant handler evaluates it: every VictoriaLogs request is one operand's query (never the
// translator's marker of the whole operation), a repeat of the request is served from the response cache with no
// VictoriaLogs call, and the cached answer is the first one. The Loki answers are held by the differential
// (internal/querycorpus TestRankOverBinaryMatchesLoki).
//
// conformance: semantics/rank-over-binary-operation
func TestRangeRankOverBinaryEvaluatesOperands(t *testing.T) {
	for _, tc := range []struct {
		query string
		// operands is how many VictoriaLogs stats requests the operands make.
		operands int
	}{
		{`topk(2, sum by (app) (rate({app="a"}[1m])) / sum by (app) (rate({app="b"}[1m])))`, 2},
		{`bottomk(1, sum by (app) (count_over_time({app="a"}[1m])) > bool sum by (app) (count_over_time({app="b"}[1m])))`, 2},
		{`sort(sum by (app) (count_over_time({app="a"}[1m])) and sum by (app) (count_over_time({app="b"}[1m])))`, 2},
		{`sort_desc(sum by (app) (count_over_time({app="a"}[1m])) unless on (app) sum by (app) (count_over_time({app="b"}[1m])))`, 2},
		{`topk by (app) (1, sum by (app, pod) (count_over_time({app="a"}[1m])) * 2)`, 1},
		{`stddev(sum by (app) (count_over_time({app="a"}[1m])) / sum by (app) (count_over_time({app="b"}[1m])) * 100)`, 2},
		{`sum(topk(2, sum by (app) (count_over_time({app="a"}[1m])) - sum by (app) (count_over_time({app="b"}[1m]))))`, 2},
	} {
		t.Run(tc.query, func(t *testing.T) {
			up := &upstreamRecorder{}
			vl := httptest.NewServer(http.HandlerFunc(up.handler))
			t.Cleanup(vl.Close)
			p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			up.take()
			target := "/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&query=" + url.QueryEscape(tc.query)
			headers := map[string]string{"X-Scope-OrgID": "0"}
			first := doCompatProxyRequest(p, target, headers)
			if first.Code != http.StatusOK {
				t.Fatalf("status %d: %s", first.Code, first.Body)
			}
			stats := 0
			for _, sent := range up.take() {
				if strings.Contains(sent, "__binary__") || strings.Contains(sent, "%7C%7C%7C") {
					t.Errorf("the binary marker reached VictoriaLogs: %s", sent)
				}
				if strings.Contains(sent, "/select/logsql/stats_query_range") {
					stats++
				}
			}
			if stats != tc.operands {
				t.Errorf("%d stats requests, want one per operand (%d)", stats, tc.operands)
			}
			again := doCompatProxyRequest(p, target, headers)
			if again.Code != http.StatusOK || again.Body.String() != first.Body.String() {
				t.Fatalf("the repeat answers another body\nfirst  %d %s\nrepeat %d %s", first.Code, first.Body, again.Code, again.Body)
			}
			if reqs := up.take(); len(reqs) != 0 {
				t.Errorf("the repeat asked VictoriaLogs again: %q", reqs)
			}
		})
	}
}

// The operands of a ranking over a binary operation are queries of their own, cached under their own keys: the same
// binary operation at the root, and another ranking over it, are answered from those operand answers with no
// VictoriaLogs call.
//
// conformance: semantics/rank-over-binary-operation
func TestRangeRankOverBinaryReusesOperandAnswers(t *testing.T) {
	up := &upstreamRecorder{}
	vl := httptest.NewServer(http.HandlerFunc(up.handler))
	t.Cleanup(vl.Close)
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	up.take()
	binary := `sum by (app) (rate({app="a"}[1m])) / sum by (app) (rate({app="b"}[1m]))`
	headers := map[string]string{"X-Scope-OrgID": "0"}
	target := func(q string) string {
		return "/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&query=" + url.QueryEscape(q)
	}
	if rec := doCompatProxyRequest(p, target("topk(2, "+binary+")"), headers); rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if len(up.take()) == 0 {
		t.Fatal("the first request asked VictoriaLogs nothing")
	}
	for _, q := range []string{binary, "sort_desc(" + binary + ")", "bottomk(1, " + binary + ")"} {
		if rec := doCompatProxyRequest(p, target(q), headers); rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", q, rec.Code, rec.Body)
		}
		if reqs := up.take(); len(reqs) != 0 {
			t.Errorf("%s asked VictoriaLogs again for operands already answered: %q", q, reqs)
		}
	}
}

// The answers cached before the binary route served these queries on query_range are fenced: the range keys of a
// ranking, stddev or stdvar over a binary operation carry the fence, an instant key and every other query's keys do
// not.
//
// conformance: semantics/rank-over-binary-operation, cache-response-key-versions
func TestRankOverBinaryFencesRangeKeysOnly(t *testing.T) {
	rank := `topk(2, sum by (app) (rate({app="a"}[1m])) / sum by (app) (rate({app="b"}[1m])))`
	r := fenceShapeRequest(rank)
	for _, site := range []responseKeySite{responseKeyCompatQueryRange, responseKeyInnerQueryRange} {
		if !strings.Contains(responseKeyFences(r, rank, site), rankOverBinaryResponseVersion) {
			t.Errorf("site %d: the range key of %q carries no fence", site, rank)
		}
	}
	if strings.Contains(responseKeyFences(r, rank, responseKeyCompatQuery), rankOverBinaryResponseVersion) {
		t.Errorf("the instant key of %q is fenced: its answers did not change", rank)
	}
	plain := `topk(2, sum by (app) (rate({app="a"}[1m])))`
	if strings.Contains(responseKeyFences(fenceShapeRequest(plain), plain, responseKeyCompatQueryRange), rankOverBinaryResponseVersion) {
		t.Errorf("the range key of %q is fenced: it has no binary operand", plain)
	}
}

// BenchmarkRangeRankOverBinary measures a query_range request of a ranking over a binary operation that misses the
// response cache (every iteration asks another step): the proxy's work around its VictoriaLogs calls, which a fake
// VictoriaLogs answers at once, and a repeat served from the cache.
func BenchmarkRangeRankOverBinary(b *testing.B) {
	up := &upstreamRecorder{}
	vl := httptest.NewServer(http.HandlerFunc(up.handler))
	b.Cleanup(vl.Close)
	p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 100000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	headers := map[string]string{"X-Scope-OrgID": "0"}
	query := url.QueryEscape(`topk(2, sum by (app) (rate({app="a"}[1m])) / sum by (app) (rate({app="b"}[1m])))`)
	target := func(step int) string {
		return "/loki/api/v1/query_range?start=1609459200&end=1609462800&step=" + strconv.Itoa(step) + "&query=" + query
	}
	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if rec := doCompatProxyRequest(p, target(60+i%50000), headers); rec.Code != http.StatusOK {
				b.Fatalf("status %d", rec.Code)
			}
			up.take()
		}
	})
	b.Run("repeat", func(b *testing.B) {
		_ = doCompatProxyRequest(p, target(59), headers)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if rec := doCompatProxyRequest(p, target(59), headers); rec.Code != http.StatusOK {
				b.Fatalf("status %d", rec.Code)
			}
		}
	})
}
