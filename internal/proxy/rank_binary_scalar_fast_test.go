package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// rankScalarGate is rangeRankScalarOperand of query (a ranking, stddev or stdvar) on a query_range request over
// [start, end] every step, with the operand's translation as the range handler reads it.
func rankScalarGate(t *testing.T, p *Proxy, query string, start, end int64, step string) bool {
	t.Helper()
	postAgg, ok := parseInstantMetricPostAggQuery(query)
	if !ok {
		t.Fatalf("%s: not a post-aggregation", query)
	}
	translated, err := p.translatePostAggOperand(context.Background(), postAgg)
	if err != nil {
		return false
	}
	translated, _, withoutLabels, _ := splitPostProcessing(translated)
	translated = preserveMetricStreamIdentity(postAgg.inner, translated, withoutLabels)
	stats, isStats := translator.DecodeStats(translated)
	operand, _ := parseLogQLExpr(context.Background(), postAgg.inner)
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{
		"query": {postAgg.inner}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {step},
	}.Encode(), nil)
	_, gated := rangeRankScalarOperand(r, operand, stats, isStats)
	return gated
}

// The scalar fast path of a ranking's binary operand takes exactly the shapes it answers as the binary route does: a
// range function of lines with line filters only and a number, joined by an arithmetic operator, over a range equal to
// the step, on or off the step's grid. Everything else is left to the binary route.
//
// conformance: semantics/rank-binary-operand-label-replace-once
func TestRankScalarFastPathGate(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1")
	const at = int64(1_800_000_000)
	x := `count_over_time({app="uw"} != "x" [1m])`
	for _, c := range []struct {
		query  string
		start  int64
		step   string
		gated  bool
		reason string
	}{
		{`topk(1, ` + x + ` * 2)`, at - 600, "60", true, "count and a number, tumbling, on the grid"},
		{`topk(1, 2 * ` + x + `)`, at - 600, "60", false, "the number on the left: the translator's text is no scalar stats binary"},
		{`stddev(rate({app="uw"}[1m]) / 60)`, at - 600, "60", true, "rate"},
		{`sort_desc(bytes_over_time({app="uw"} |= "line" [1m]) - 1)`, at - 600, "60", true, "bytes and a line filter"},
		{`stdvar(bytes_rate({app="uw"}[15s]) ^ 2)`, at - 600, "15", true, "bytes_rate, 15s"},
		{`topk(1, ` + x + ` * 2)`, at - 600, "15", false, "range longer than the step"},
		{`topk(1, ` + x + ` * 2)`, at - 590, "60", true, "start off the step's grid"},
		{`topk(1, count_over_time({app="uw"}[5m]) * 2)`, at - 600, "60", false, "sliding window"},
		{`topk(1, ` + x + ` > 2)`, at - 600, "60", false, "comparison"},
		{`topk(1, ` + x + ` > bool 2)`, at - 600, "60", false, "bool"},
		{`topk(1, ` + x + ` * 2 + 1)`, at - 600, "60", false, "chain"},
		{`topk(1, ` + x + ` + vector(1))`, at - 600, "60", false, "vector(n)"},
		{`topk(1, sum by (pod) (` + x + `) * 2)`, at - 600, "60", false, "vector aggregation"},
		{`topk(1, ` + x + ` / ` + x + `)`, at - 600, "60", false, "two queries"},
		{`topk(1, count_over_time({app="uw"} | logfmt [1m]) * 2)`, at - 600, "60", false, "parser"},
		{`topk(1, count_over_time({app="uw"} | pod="p1" [1m]) * 2)`, at - 600, "60", false, "label filter"},
		{`topk(1, count_over_time({app="uw"} != "(" [1m]) * 2)`, at - 600, "60", false, "the translator drops the operation"},
		{`topk(1, sum_over_time({app="uw"} | unwrap n [1m]) * 2)`, at - 600, "60", false, "unwrap"},
		{`topk(1, count_over_time({app="uw"}[1m] offset 1m) * 2)`, at - 600, "60", false, "offset"},
		{`topk(1, label_replace(` + x + `, "pod", "x-$1", "pod", "(.*)") * 2)`, at - 600, "60", false, "label_replace"},
	} {
		if got := rankScalarGate(t, p, c.query, c.start*1e9, at*1e9, c.step); got != c.gated {
			t.Errorf("%s (step %s, %s): gated %v, want %v", c.query, c.step, c.reason, got, c.gated)
		}
	}
}

// Over the shapes the gate takes, the scalar fast path answers what the binary route answers: the same series,
// timestamps and values, for every range function of lines, every arithmetic operator, with and without a line filter, tumbling windows of 15s, 1m and 5m, under topk, bottomk, sort_desc, stddev and
// stdvar, on VictoriaLogs v1.49.0 and v1.53.0 (emulated). Loki's own answers for the gated shapes are held by
// TestRankingOverBinaryOperandMatchesRecordedLoki.
//
// conformance: semantics/rank-binary-operand-label-replace-once, loki_api_v1_query_range
func TestRankScalarFastPathMatchesBinaryRoute(t *testing.T) {
	const at = int64(1_800_000_000)
	rows := rankingBinaryOperandRows(at)
	run := func(p *Proxy, query string, start, end int64, step string) map[string]map[int64]float64 {
		t.Helper()
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {step}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %.300s", query, res.Code, res.Body.String())
		}
		s, err := decodeLokiSeries(res.Body.Bytes())
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return rankingProxySeries(t, query, s)
	}
	gate := newTestProxy(t, "http://127.0.0.1:1")
	compared := 0
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		emu := &vlemu.Emulator{T: t, Rows: rows, Version: version}
		backend := httptest.NewServer(emu.Handler())
		t.Cleanup(backend.Close)
		for _, window := range []struct {
			text string
			step int64
		}{{"15s", 15}, {"1m", 60}, {"5m", 300}} {
			for _, bounds := range [][2]int64{{at - 10*60, at}, {at - 10*60 + 7, at - 3}} {
				start, end := bounds[0], bounds[1]
				for _, fn := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
					for _, filter := range []string{"", ` != "x"`} {
						operand := fn + `({app="uw"}` + filter + ` [` + window.text + `])`
						for _, op := range []string{"+", "-", "*", "/", "%", "^"} {
							{
								bin := operand + " " + op + " 3"
								for _, wrap := range []string{"topk(3, %s)", "bottomk(3, %s)", "sort_desc(%s)", "stddev(%s)", "stdvar(%s)"} {
									query := fmt.Sprintf(wrap, bin)
									step := strconv.FormatInt(window.step, 10)
									if !rankScalarGate(t, gate, query, start*1e9, end*1e9, step) {
										t.Fatalf("%s at step %s: the gate does not take it", query, step)
									}
									answers := [2]map[string]map[int64]float64{}
									for i, fast := range []bool{true, false} {
										rankScalarFastPath = fast
										p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
										if err != nil {
											t.Fatal(err)
										}
										if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
											t.Fatal(err)
										}
										answers[i] = run(p, query, start, end, step)
										_ = p.Shutdown(context.Background())
									}
									rankScalarFastPath = true
									if len(answers[1]) == 0 {
										t.Fatalf("%s %s: the binary route answers no series", version, query)
									}
									compareRankingSeries(t, version+" fast path vs binary route "+query, answers[0], answers[1])
									compared++
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d gated requests answer as the binary route", compared)
	if compared < 2000 {
		t.Fatalf("only %d gated requests compared", compared)
	}
}
