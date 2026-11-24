package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// emptyWithoutOperand takes off the outer sum, avg, min and max levels with a `without ()` clause, in every
// spelling the parser reads, and nothing else: a count, stddev, stdvar or ranking, a clause with a label, `by ()`, a
// level under another level, and an operand with no aggregation are sent as written.
//
// conformance: semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutOperandTakesOffOnlyTheLevelsThatKeepEverySeries(t *testing.T) {
	const x = `count_over_time({app="a"}[5m])`
	for in, want := range map[string]string{
		`sum without () (` + x + `)`:                                x,
		`sum(` + x + `) without ()`:                                 x,
		`SUM WITHOUT ( ) (` + x + `)`:                               x,
		`avg without () (` + x + `)`:                                x,
		`min without () (` + x + `)`:                                x,
		`max without () (` + x + `)`:                                x,
		`max without () (sum without () (` + x + `))`:               x,
		`sum without () (sum by (app) (` + x + `))`:                 `sum by (app) (` + x + `)`,
		`sum by (app) (sum without () (` + x + `))`:                 `sum by (app) (sum without () (` + x + `))`,
		`count without () (` + x + `)`:                              `count without () (` + x + `)`,
		`stddev without () (` + x + `)`:                             `stddev without () (` + x + `)`,
		`stdvar without () (` + x + `)`:                             `stdvar without () (` + x + `)`,
		`topk without () (1, ` + x + `)`:                            `topk without () (1, ` + x + `)`,
		`sum without (pod) (` + x + `)`:                             `sum without (pod) (` + x + `)`,
		`sum by () (` + x + `)`:                                     `sum by () (` + x + `)`,
		`sum(` + x + `)`:                                            `sum(` + x + `)`,
		x:                                                           x,
		`sum without () (max_over_time({app="a"} | unwrap n [5m]))`: `max_over_time({app="a"} | unwrap n [5m])`,
	} {
		expr, err := logqlpkg.Parse(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		wantExpr, err := logqlpkg.Parse(want)
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		if got := emptyWithoutOperand(expr).String(); got != wantExpr.String() {
			t.Errorf("emptyWithoutOperand(%s) = %s, want %s", in, got, wantExpr.String())
		}
	}
}

// emptyWithoutRun is one proxy over a counting emulated VictoriaLogs: run answers a query_range request and the
// VictoriaLogs calls it cost.
type emptyWithoutRun struct {
	p     *Proxy
	calls *atomic.Int64
}

func newEmptyWithoutRun(t *testing.T, disableLowering bool) emptyWithoutRun {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: withoutScopeRows(), Field: "amount", Version: "v1.52.0"}
	var calls atomic.Int64
	h := emu.Handler()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
		BackendVersionCheckTimeout: time.Second, DisableQueryPlanLowering: disableLowering})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	return emptyWithoutRun{p: p, calls: &calls}
}

// run answers query on query_range over two minutes with a 60s step, and the VictoriaLogs calls it made.
func (r emptyWithoutRun) run(t *testing.T, query string) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	base := int64(1_800_000_000)
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(base-120, 10)}, "end": {strconv.FormatInt(base, 10)}, "step": {"60"}}
	before := r.calls.Load()
	res := doCompatProxyRequest(r.p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	return res, r.calls.Load() - before
}

// On query_range with the step equal to the range, a sum `without ()` operand of a binary operation answers every
// series of its operand, as Loki does (a clause of no label drops none): the answer is the one of the same operation
// over the operand alone, series and values. A repeat is answered from the cache with no VictoriaLogs call. With
// -query-plan-lowering=false the handlers read the clause as before (one ungrouped series), the rollback.
//
// conformance: semantics/binary-operand-without-empty-collapses, cache-response-key-versions
func TestEmptyWithoutOperandAnswersEverySeries(t *testing.T) {
	const (
		empty = `sum without () (count_over_time({app="uw"}[1m])) / 2`
		bare  = `count_over_time({app="uw"}[1m]) / 2`
	)
	on := newEmptyWithoutRun(t, false)
	got, cold := on.run(t, empty)
	want, _ := newEmptyWithoutRun(t, false).run(t, bare)
	gotSeries, wantSeries := openShapeCanon(binarySeriesOf(t, got.Body.Bytes())), openShapeCanon(binarySeriesOf(t, want.Body.Bytes()))
	if gotSeries != wantSeries {
		t.Fatalf("%s answers %s, the operation over its operand %s", empty, gotSeries, wantSeries)
	}
	if n := len(binarySeriesOf(t, got.Body.Bytes())); n < 2 {
		t.Fatalf("%s answers %d series, want one per stream", empty, n)
	}
	if cold == 0 {
		t.Fatal("the cold request made no VictoriaLogs call")
	}
	if _, repeat := on.run(t, empty); repeat != 0 {
		t.Errorf("the repeat made %d VictoriaLogs calls, want 0", repeat)
	}
	rollback, _ := newEmptyWithoutRun(t, true).run(t, empty)
	if n := len(binarySeriesOf(t, rollback.Body.Bytes())); n != 1 {
		t.Errorf("with the lowering off the clause is read as before (one series), got %d", n)
	}
}

// binarySeriesOf reads the series of a query_range answer: each label set, written sorted, with its points.
func binarySeriesOf(t *testing.T, body []byte) map[string]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	out := map[string]string{}
	for _, s := range resp.Data.Result {
		keys := make([]string, 0, len(s.Metric))
		for k, v := range s.Metric {
			keys = append(keys, k+"="+v)
		}
		sort.Strings(keys)
		out[strings.Join(keys, ",")] = fmt.Sprint(s.Values)
	}
	return out
}

// The binary empty without fence fires for a query whose parsed tree holds a sum, avg, min or max `without ()`
// operand, in any spelling the parser reads, and stays off for one the request validation rejects (Loki answers its
// parse error, so the tree holds no such operation): an empty-compatible selector, the fuzz finding of
// FuzzResponseKeyFencesMatchParsedTree, kept as a seed.
//
// conformance: cache-response-key-versions, semantics/binary-operand-without-empty-collapses
func TestEmptyWithoutFenceFollowsTheValidatedTree(t *testing.T) {
	for _, tc := range []struct {
		query string
		fires bool
	}{
		{`sum without () (count_over_time({app="a"}[5m])) % 0`, true},
		{"Sum\nwiTHOUT\n()\t(COUNT_OVER_TIME\n({A0=\"a\"}[0y]))%0", true},
		{`2 * max without () (rate({app="a"}[1m]))`, true},
		{"Sum\nwiTHOUT\n()\t(COUNT_OVER_TIME\n({A0=\"\"}[0y]))%0", false},
		{`sum without () (count_over_time({app=""}[5m])) / 2`, false},
		{`sum without () (count_over_time({app=~".*"}[5m])) / 2`, false},
		{`sum without () (count_over_time({app="a"}[5m])) / `, false},
		{`count without () (count_over_time({app="a"}[5m])) / 2`, false},
		{"lABel_replACe(sum\rwithout\t() # \"\n(Count_over_t\u0130me({}[$__auto]))%00,\r\"\",\"\",\"\",\"\")\r", false},
		{`label_replace(sum without () (count_over_time({app="a"}[$__auto])) % 0, "", "", "", "")`, true},
	} {
		if _, msg := logqlpkg.ValidateLogQLExpr(tc.query); tc.fires && msg != "" {
			t.Fatalf("%q: the validation rejects a row that must fire: %s", tc.query, msg)
		}
		if got := emptyWithoutOperandResponseKeyVersion(tc.query) != ""; got != tc.fires {
			resolved := fenceVariablesResolved(tc.query)
			t.Logf("original validation: %+v; resolved validation: %+v; resolved query: %q; plan: %+v",
				cachedQueryValidation(tc.query), cachedQueryValidation(resolved), resolved, resolvedPlan(dynamicQuery(resolved)))
			t.Errorf("%q: the fence fires %v, want %v", tc.query, got, tc.fires)
		}
	}
}

// BenchmarkEmptyWithoutFence times the binary empty without fence on the queries it turns away with its text screen
// alone (a labelled without clause, a log query, a by clause) and on one it fires for (the plan read from the memo).
func BenchmarkEmptyWithoutFence(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"labelled", `sum without (pod) (rate({service_name="payment-service"} | logfmt amount | unwrap amount [5m]))`},
		{"log", `{app="a",env=~"p.*"} | json | status="500" |= "error"`},
		{"by", `sum by (service_name) (count_over_time({service_name="payment-service"} |= "error" [5m])) / 2`},
		{"fires", `sum without () (count_over_time({service_name="payment-service"}[5m])) / 2`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = emptyWithoutOperandResponseKeyVersion(tc.query)
			}
		})
	}
}
