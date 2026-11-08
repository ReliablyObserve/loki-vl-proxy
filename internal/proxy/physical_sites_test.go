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
	}
	for _, tc := range cases {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				srv, rec := newSiteBackend(t)
				p := newTestProxy(t, srv.URL)
				a := lrAsk(t, p, endpoint, tc.logql)
				got := siteAnswer{code: a.code, vl: rec.requests(), series: seriesKeys(a)}
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
}

// siteWants holds the recorded answers, by "<case>/<endpoint>".
var siteWants = map[string]siteAnswer{
	"plain/query": {
		code: 200,
		vl: []string{
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 5b0ecb4bef8593b5af54c47ec76a3cfbbc4ae31acfc585bf9ea24c44b3754a6b/7348",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
		},
		series: []string{
			"{app=\"api-gateway\"} [][[1.70406726e+09 3]]",
			"{app=\"web\"} [][[1.70406726e+09 3]]",
		},
	},
	"without with op/query": {
		code: 200,
		vl: []string{
			"stats_query bdc2cd6ed1c1b542019e9609ab19f2b3e5dcd3792395ab03b8d9ab30de5e8953/7249",
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
			"stats_query_range ccd61f84b204942f481a2d403176098974b5b36970f0b41468b9a305c6c4137d/7493",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [][[1.70406726e+09 3]]",
			"{app=\"web\",instance=\"i2\"} [][[1.70406726e+09 3]]",
		},
	},
	"binary/query": {
		code: 200,
		vl: []string{
			"stats_query 799a59ff206658b1168d9ed9a2eeeb39c91a2b3fc661c47d1befe431d2efa057/7249",
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
			"stats_query_range ab586bbcf57365d604ba3b81aff364d85ccad47b15871b67937e805cd2b87990/7213",
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
			"stats_query 799a59ff206658b1168d9ed9a2eeeb39c91a2b3fc661c47d1befe431d2efa057/7249",
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 5b0ecb4bef8593b5af54c47ec76a3cfbbc4ae31acfc585bf9ea24c44b3754a6b/7348",
			"stats_query_range ffc93f267cbf12d2e8c63f92824ab5bfa32e269ca624ab544e34f53a62371c40/7348",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
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
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\"} [1.70406726e+09 3][]",
			"{app=\"web\",instance=\"i2\"} [1.70406726e+09 3][]",
		},
	},
	"topk over without/query_range": {
		code: 200,
		vl: []string{
			"query 9c052c7f7fc34abb989703c1006e13388e8aebe0137649b4adead955e797d512/7102",
		},
		series: nil,
	},
	"topk over label_replace/query": {
		code: 200,
		vl: []string{
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",pod=\"p2\",short=\"api\"} [1.70406726e+09 2][]",
			"{app=\"web\",instance=\"i2\",pod=\"p1\"} [1.70406726e+09 3][]",
		},
	},
	"topk over label_replace/query_range": {
		code: 200,
		vl: []string{
			"query 9c052c7f7fc34abb989703c1006e13388e8aebe0137649b4adead955e797d512/7102",
		},
		series: nil,
	},
	"sort over label_replace of without/query": {
		code: 200,
		vl: []string{
			"stats_query d241cc425be74c9dc306a09723738329c808909a995d869b77380a4b13b02c65/7249",
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
			"stats_query_range 7605dc4f9dd070aa1b7c223c2f1f5a113071b2ad8b83297497852929c4abbaa8/7213",
		},
		series: []string{
			"{app=\"api-gateway\",instance=\"i1\",short=\"api\"} [][[1.70406726e+09 3]]",
			"{app=\"web\",instance=\"i2\"} [][[1.70406726e+09 3]]",
		},
	},
}
