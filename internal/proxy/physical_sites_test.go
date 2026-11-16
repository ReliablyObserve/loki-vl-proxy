package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// The handlers that strip the translator's post-processing markers read them through the physical-plan codec.
// These tests pin what that must not change: for each marker shape, the LogsQL the handler sends to VictoriaLogs
// and the series it answers after the fold and the label_replace transforms, on both endpoints. The wants were
// recorded from the handlers before they moved onto the codec.

// siteBackend answers every stats query with three series and records each VictoriaLogs request as
// "<endpoint> <query>".
type siteBackend struct {
	mu      sync.Mutex
	reqs    []string // "<endpoint> <sha256 of the query>/<its length>", the form the wants hold
	queries []string // the queries, for the failure message
}

func (b *siteBackend) record(r *http.Request) {
	_ = r.ParseForm()
	b.mu.Lock()
	defer b.mu.Unlock()
	q := r.FormValue("query")
	b.reqs = append(b.reqs, fmt.Sprintf("%s %x/%d", strings.TrimPrefix(r.URL.Path, "/select/logsql/"), sha256.Sum256([]byte(q)), len(q)))
	b.queries = append(b.queries, q)
}

func (b *siteBackend) requests() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]string(nil), b.reqs...)
	sort.Strings(out)
	return out
}

func newSiteBackend(t *testing.T) (*httptest.Server, *siteBackend) {
	t.Helper()
	rec := &siteBackend{}
	sets := []map[string]string{
		{"app": "api-gateway", "pod": "p1", "instance": "i1"},
		{"app": "api-gateway", "pod": "p2", "instance": "i1"},
		{"app": "web", "pod": "p1", "instance": "i2"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		var out []string
		for i, m := range sets {
			mb, _ := json.Marshal(m)
			switch r.URL.Path {
			case "/select/logsql/stats_query_range":
				out = append(out, fmt.Sprintf(`{"metric":%s,"values":[[1704067200,"%d"],[1704067260,"%d"]]}`, mb, i+1, i+2))
			case "/select/logsql/stats_query":
				out = append(out, fmt.Sprintf(`{"metric":%s,"value":[1704067200,"%d"]}`, mb, i+1))
			}
		}
		resultType := "vector"
		if r.URL.Path == "/select/logsql/stats_query_range" {
			resultType = "matrix"
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":%q,"result":[%s]}}`, resultType, strings.Join(out, ","))
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// seriesKeys is the label sets of an answer with their points, sorted.
func seriesKeys(a lrAnswer) []string {
	var out []string
	for _, s := range a.series {
		out = append(out, fmt.Sprintf("%s %v%v", lrKey(s.Metric), s.Value, s.Values))
	}
	sort.Strings(out)
	return out
}

// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses, loki_api_v1_query, loki_api_v1_query_range
func TestMarkerSitesKeepTheirVictoriaLogsQueryAndPostProcessing(t *testing.T) {
	const rate = `rate({app="x"}[1m])`
	const rateY = `rate({app="y"}[1m])`
	const lr = `"short", "$1", "app", "(.*)-.*"`
	cases := []struct{ name, logql string }{
		{"plain", rate},
		{"without", `sum without (pod) (` + rate + `)`},
		{"without nested", `sum without (instance) (sum without (pod) (` + rate + `))`},
		{"without with op", `max without (pod, instance) (` + rate + `)`},
		{"label_replace once", `label_replace(` + rate + `, ` + lr + `)`},
		{"label_replace three", `label_replace(label_replace(label_replace(` + rate + `, "a", "$1", "app", "(.*)-.*"), "b", "x", "a", ".*"), "c", "$1", "b", "(.*)")`},
		{"label_replace over without", `label_replace(sum without (pod) (` + rate + `), ` + lr + `)`},
		{"binary", rate + ` / ` + rateY},
		{"binary with matching", `sum by (app, pod, instance) (` + rate + `) / on (app, pod, instance) sum by (app, pod, instance) (` + rateY + `)`},
		{"binary over without", `sum without (pod) (` + rate + `) / sum without (pod) (` + rateY + `)`},
		{"scalar", rate + ` * 100`},
		{"scalar left", `2 * ` + rate},
		{"topk over without", `topk(2, sum without (pod) (` + rate + `))`},
		{"topk over label_replace", `topk(2, label_replace(` + rate + `, ` + lr + `))`},
		{"sort over label_replace of without", `sort(label_replace(sum without (pod) (` + rate + `), ` + lr + `))`},

		// Marker shapes a client can reach: arguments at the edges, invalid ones, and text of the query that looks
		// like a marker (a line filter or a label value that holds `__without__:` or a whole `__lvp_lr:` marker),
		// which is read as text: VictoriaLogs gets the whole query, the string included.
		{"label_replace four", `label_replace(label_replace(label_replace(label_replace(` + rate + `, "a", "$1", "app", "(.*)-.*"), "b", "${1}", "a", "(.*)"), "c", "x", "b", ".*"), "d", "y", "c", ".*")`},
		{"label_replace empty arguments", `label_replace(` + rate + `, "", "", "", "")`},
		{"label_replace empty replacement", `label_replace(` + rate + `, "short", "", "app", "(.*)-.*")`},
		{"label_replace braces", `label_replace(` + rate + `, "short", "${1}-x", "app", "(?P<h>.*)-.*")`},
		{"label_replace invalid regexp", `label_replace(` + rate + `, "short", "$1", "app", "(")`},
		{"label_replace invalid regexp under without", `label_replace(sum without (pod) (` + rate + `), "short", "$1", "app", "[a-")`},
		{"label_replace one side of a binary", `label_replace(` + rate + `, ` + lr + `) / ` + rateY},
		{"label_replace both sides of a binary", `label_replace(` + rate + `, ` + lr + `) / label_replace(` + rateY + `, ` + lr + `)`},
		{"without over label_replace", `sum without (pod) (label_replace(` + rate + `, ` + lr + `))`},
		{"without empty labels", `sum without () (` + rate + `)`},
		{"without repeated label", `sum without (pod, pod) (` + rate + `)`},
		{"without unicode label", `sum without (é) (` + rate + `)`},
		{"line filter holding a without marker", `{app="x"} |= "__without__:pod"`},
		{"line filter holding a without marker and an op", `{app="x"} |= "__without__:sum:pod,node"`},
		{"metric over a line filter holding a without marker", `sum(count_over_time({app="x"} |= "__without__:pod" [1m]))`},
		{"label value holding a without marker", `sum(count_over_time({app="__without__:pod"} [1m]))`},
		{"line filter holding a half label_replace marker", `count_over_time({app="x"} |= "a__lvp_lr:YQ==__" [1m])`},
		{"line filter holding a label_replace marker", `sum(count_over_time({app="x"} |= "__lvp_lr:c2hvcnQAJDEAYXBwACguKikAYQ==__" [1m]))`},
		{"line filter holding a binary marker", `rate({app="x"} |= "__binary__:/:A|||B" [1m])`},
		{"line filters holding separators", `rate({app="x"} |= "|||" [1m]) / rate({app="y"} |= "@@@on:app" [1m])`},
	}
	for _, tc := range cases {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				srv, rec := newSiteBackend(t)
				p := newTestProxy(t, srv.URL)
				a := lrAsk(t, p, endpoint, tc.logql)
				got := siteAnswer{code: a.code, vl: rec.requests(), series: seriesKeys(a)}
				if a.code >= 400 {
					got.body = a.body // Loki's error text, which must not change
				}
				want, ok := siteWants[tc.name+"/"+endpoint]
				if !ok || fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", want) {
					t.Errorf("%s %s: got\n\t%q: %#v,\nwant %#v\nqueries sent:\n%s", endpoint, tc.logql, tc.name+"/"+endpoint, got, want, strings.Join(rec.queryList(), "\n"))
				}
			})
		}
	}
}

func (b *siteBackend) queryList() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.queries...)
}

// siteAnswer is what a handler did for one request: its status, the VictoriaLogs requests it made and the series
// it answered.
type siteAnswer struct {
	code   int
	vl     []string
	series []string
	body   string // the error text of an answer of 400 or more
}

// siteWants holds the recorded answers, by "<case>/<endpoint>".
var siteWants = map[string]siteAnswer{
	"plain/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"plain/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"without/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"without/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range fe6a7cff2d58863e65b27d7d9be0e77300fc06b199049bd4f1a9966468375862/8037",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"without nested/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\"} [1.70406726e+09 3][]",
			"{app=\"web\"} [1.70406726e+09 3][]",
		},
	},
	"without nested/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\"} [][[1.70406726e+09 3]]",
			"{app=\"web\"} [][[1.70406726e+09 3]]",
		},
	},
	"without with op/query": {
		code: 200,
		vl: []string{
			"stats_query 32da04749a41154b4bbfeb364330cb373d9147fe0af0221a3c2d88a3cbfec9b5/7938",
		},
		series: []string{
			"{app=\"api-gateway\"} [1.70406726e+09 2][]",
			"{app=\"web\"} [1.70406726e+09 3][]",
		},
	},
	"without with op/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range 9b903e7f9a9bfdc89801eceb26044ffeb7ec039468b411c13d03c8149d7ca7c2/8182",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace once/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace once/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace three/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{a=\"api\",app=\"api-gateway\",b=\"x\",c=\"x\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{a=\"api\",app=\"api-gateway\",b=\"x\",c=\"x\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",b=\"x\",c=\"x\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace three/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{a=\"api\",app=\"api-gateway\",b=\"x\",c=\"x\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{a=\"api\",app=\"api-gateway\",b=\"x\",c=\"x\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",b=\"x\",c=\"x\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace over without/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace over without/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [][[1.70406726e+09 3]]",
			"{app=\"web\",instance=\"i2\"} [][[1.70406726e+09 3]]",
		},
	},
	"binary/query": {
		code: 200,
		vl: []string{
			"stats_query a98d07e6aac0c7ead0ed03539c8ce8414fca0f1a42201353065f4a1005874879/7938",
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 1][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 1][]",
		},
	},
	"binary/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"query c7ccd74cc4e9722ac757e820afe0a628a264b7cab5b38e3e9d0e5ff4a39f672e/1423",
			"stats_query_range ccd27f1b1a1c263e53297b2ede67b10a5b088c9473800b97382cddbb627d4d77/7902",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 1]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
	"binary with matching/query": {
		code: 200,
		vl: []string{
			"stats_query c47c36460eb7892374fd67bc40a76a63fc8779968b2e745bc22c7cd5113b0e29/147",
			"stats_query d6603ddc843963fee69890a0329c7808a3704774d2fb89305f269a9505802cb1/147",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 1][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 1][]",
		},
	},
	"binary with matching/query_range": {
		code: 200,
		vl: []string{
			"stats_query_range c47c36460eb7892374fd67bc40a76a63fc8779968b2e745bc22c7cd5113b0e29/147",
			"stats_query_range d6603ddc843963fee69890a0329c7808a3704774d2fb89305f269a9505802cb1/147",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 1]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
	"binary over without/query": {
		code: 200,
		vl: []string{
			"stats_query a98d07e6aac0c7ead0ed03539c8ce8414fca0f1a42201353065f4a1005874879/7938",
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\"} [1.70406726e+09 1][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 1][]",
		},
	},
	"binary over without/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"query c7ccd74cc4e9722ac757e820afe0a628a264b7cab5b38e3e9d0e5ff4a39f672e/1423",
			"stats_query_range ccc3b29bc4931110297d0c985493db606dc7798adcdf9e65071ef6e344ce8050/8037",
			"stats_query_range fe6a7cff2d58863e65b27d7d9be0e77300fc06b199049bd4f1a9966468375862/8037",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 1]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
	"scalar/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 100][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 200][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 300][]",
		},
	},
	"scalar/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 100]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 200]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 300]]",
		},
	},
	"scalar left/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 2][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 4][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 6][]",
		},
	},
	"scalar left/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 2]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 4]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 6]]",
		},
	},
	"topk over without/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"topk over without/query_range": {
		code: 200,
		vl: []string{
			"query a09c510987f831b278685d50cd9f9ef55cbf48fa06d464dc1e0cc0a415fe8666/7791",
		},
		series: nil,
	},
	"topk over label_replace/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"topk over label_replace/query_range": {
		code: 200,
		vl: []string{
			"query a09c510987f831b278685d50cd9f9ef55cbf48fa06d464dc1e0cc0a415fe8666/7791",
		},
		series: nil,
	},
	"sort over label_replace of without/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"sort over label_replace of without/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [][[1.70406726e+09 3]]",
			"{app=\"web\",instance=\"i2\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace four/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{a=\"api\",app=\"api-gateway\",b=\"api\",c=\"x\",d=\"y\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{a=\"api\",app=\"api-gateway\",b=\"api\",c=\"x\",d=\"y\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",c=\"x\",d=\"y\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace four/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{a=\"api\",app=\"api-gateway\",b=\"api\",c=\"x\",d=\"y\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{a=\"api\",app=\"api-gateway\",b=\"api\",c=\"x\",d=\"y\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",c=\"x\",d=\"y\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace empty arguments/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace empty arguments/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace empty replacement/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace empty replacement/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace braces/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api-x\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api-x\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"label_replace braces/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api-x\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api-x\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"label_replace invalid regexp/query": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "parse error : invalid regex in label_replace: error parsing regexp: missing closing ): `^(?:()$`",
	},
	"label_replace invalid regexp/query_range": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "parse error : invalid regex in label_replace: error parsing regexp: missing closing ): `^(?:()$`",
	},
	"label_replace invalid regexp under without/query": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "parse error : invalid regex in label_replace: error parsing regexp: invalid character class range: `a-)`",
	},
	"label_replace invalid regexp under without/query_range": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "parse error : invalid regex in label_replace: error parsing regexp: invalid character class range: `a-)`",
	},
	"label_replace one side of a binary/query": {
		code: 200,
		vl: []string{
			"stats_query a98d07e6aac0c7ead0ed03539c8ce8414fca0f1a42201353065f4a1005874879/7938",
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 1][]",
		},
	},
	"label_replace one side of a binary/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"query c7ccd74cc4e9722ac757e820afe0a628a264b7cab5b38e3e9d0e5ff4a39f672e/1423",
			"stats_query_range ccd27f1b1a1c263e53297b2ede67b10a5b088c9473800b97382cddbb627d4d77/7902",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
	"label_replace both sides of a binary/query": {
		code: 200,
		vl: []string{
			"stats_query a98d07e6aac0c7ead0ed03539c8ce8414fca0f1a42201353065f4a1005874879/7938",
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [1.70406726e+09 1][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 1][]",
		},
	},
	"label_replace both sides of a binary/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"query c7ccd74cc4e9722ac757e820afe0a628a264b7cab5b38e3e9d0e5ff4a39f672e/1423",
			"stats_query_range ccd27f1b1a1c263e53297b2ede67b10a5b088c9473800b97382cddbb627d4d77/7902",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\",short=\"api\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [][[1.70406726e+09 1]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
	"without over label_replace/query": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "log queries are not supported as an instant query type, please change your query to a range query type",
	},
	"without over label_replace/query_range": {
		code: 200,
		vl: []string{
			"query 7dd75bb9ec45c202214374b17493b592b04ca859aa739a13e7a910bbfb1d564c/98",
		},
		series: nil,
	},
	"without empty labels/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"without empty labels/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range 02f029cc3d48dec6cc305c1abbdecf6aa574c8bbedb7f31f931b4233bcbcc33c/7866",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"without repeated label/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"without repeated label/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range 71dcbf9b769d22d85a872c8fd31781a3a1c93a27e04579e7229a2253378c3dfb/8172",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"without unicode label/query": {
		code: 200,
		vl: []string{
			"stats_query edc2cf183c2c0832a94cb41c018958da8da8eb28636cdc39a3a3f4cd8a350a17/7938",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"without unicode label/query_range": {
		code: 200,
		vl: []string{
			"query 9b836af0f04650e1c7ab62a2c0a683057e48e55a553788661023cd4758c007ee/1423",
			"stats_query_range d1cdbae61c5a582fb133162762805d7025c7407672a240d816102e919f4ae488/7902",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"line filter holding a without marker/query": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "log queries are not supported as an instant query type, please change your query to a range query type",
	},
	"line filter holding a without marker/query_range": {code: 200, vl: []string{"query 1d9a16652b64fcd24709237c216b2467c69869c0b69cd5a29110c698cad5eb14/50"}, series: []string(nil), body: ""},
	"line filter holding a without marker and an op/query": {
		code:   400,
		vl:     nil,
		series: nil,
		body:   "log queries are not supported as an instant query type, please change your query to a range query type",
	},
	"line filter holding a without marker and an op/query_range":     {code: 200, vl: []string{"query 97d919f4cb24709f0459d1561316762997dbe5234b33ba1163e395c340dc657e/59"}, series: []string(nil), body: ""},
	"metric over a line filter holding a without marker/query":       {code: 200, vl: []string{"stats_query f83f5838e7421bc3b9a966ad3f1174c25c00e20c4c464a421f6234e08bd84f63/63"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]"}, body: ""},
	"metric over a line filter holding a without marker/query_range": {code: 200, vl: []string{"stats_query_range 6f84431cab138a4f6b1c5b756c8417bd45fda69da3b92d2b0b3923d9f17b71d6/43"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]"}, body: ""},
	"label value holding a without marker/query":                     {code: 200, vl: []string{"stats_query cf220dc93f77cca9f006c5e1ba181210ce1fa5b7fb981c1b8f3c469aa8212aa9/58"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]"}, body: ""},
	"label value holding a without marker/query_range":               {code: 200, vl: []string{"stats_query_range 43a31f4b4686e4a4c3e7b307e2be82c8998628937adc14b4ae0cac196454e792/38"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]"}, body: ""},
	"line filter holding a half label_replace marker/query": {
		code: 200,
		vl: []string{
			"stats_query 4c2eec464d8c1d1cfc2772b1457504bebdbcb65a8a2f11e4f6a748bdeda610d4/7847",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"line filter holding a half label_replace marker/query_range": {
		code: 200,
		vl: []string{
			"query 51e4b11b7d71d3b2469d20f9dddc76d40a886c51d1b45d99fa56eea2c5c72be2/1443",
			"stats_query_range c7af58179e679530bf92e2deab3b3ec0fabfa88cf9820d8ba364fc69eb2f8f05/7811",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"line filter holding a label_replace marker/query":       {code: 200, vl: []string{"stats_query 31e37fdf1b8904f5dae30c2f4a8ac6181e94be28eb6306718c0f6ae91ff14934/87"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]"}, body: ""},
	"line filter holding a label_replace marker/query_range": {code: 200, vl: []string{"stats_query_range db10bfd2168737c8b87bd33d59f042da21f727b0dcbdecacce17bc49b3d0dbb5/67"}, series: []string{"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]", "{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]", "{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]"}, body: ""},
	"line filter holding a binary marker/query": {
		code: 200,
		vl: []string{
			"stats_query 751d7a0f631a50f7d7273d0bcee4eeecec8db04fe86b4dda79501dba50fef024/7966",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"line filter holding a binary marker/query_range": {
		code: 200,
		vl: []string{
			"query 440d207d4f5c4d5e0673410a852f49e4a5b00a6b4c1bbfecf4682a7dd6b67792/1451",
			"stats_query_range 268b334ec6a4e6d34d745da882c74f3d01ac4a7f33b5c96fa388b234a806e609/7930",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 2]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 3]]",
		},
	},
	"line filters holding separators/query": {
		code: 200,
		vl: []string{
			"stats_query 0a31cd752892f53ceb9417920b74bf14b9a997330925178c098969114cc2b478/7951",
			"stats_query 95fb4deb256c770ba2d6f38582bc4bf50b178b8c798a3c00adab112ebed62278/7951",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [1.70406726e+09 1][]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [1.70406726e+09 1][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 1][]",
		},
	},
	"line filters holding separators/query_range": {
		code: 200,
		vl: []string{
			"query 1df733015b2cab6a6f526d1dbf47804e7de39394c4e93c745ac7241f49386d76/1436",
			"query 3b34a419494892116ea6f83537ccb18be3b195bf968977c9b5859c5d82f467ea/1436",
			"stats_query_range da3a0ca45ceeef193828322aef97d8fbee988cff131864af428bd6cf072a928d/7915",
			"stats_query_range f2d412480b287b56732dc739ee6e14ec688af4218231350a370ed48d315683c1/7915",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p1\"} [][[1.70406726e+09 1]]",
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\"} [][[1.70406726e+09 1]]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [][[1.70406726e+09 1]]",
		},
	},
}
