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

// Loki answers every metric instant query with `resultType: vector` and a
// scalar literal with `scalar` (pkg/logql/engine.go evalSample, evalLiteral;
// pkg/querier/queryrange/marshal.go ResultToResponse), and its own decoder
// fails on a missing or unknown type (pkg/loghttp/query.go UnmarshalJSON). The
// Grafana Loki datasource parses the answer by that type. The proxy answered
// `stdvar without (...)` and a `label_replace` around a binary expression with
// a 200 and no resultType: the failure of the post-processed query was sent
// with its status dropped, and the translator's binary marker was sent to
// VictoriaLogs as a query (semantics/instant-result-type-missing).
//
// Each shape is asked of Loki and of the proxy. Both must answer, and answer
// the same status and the same resultType; the series of the answers are
// compared by the other registry items, not here.
// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestCompat_InstantResultTypeAlwaysSet(t *testing.T) {
	ensureDataIngested(t)
	waitForLokiMetricData(t)

	sel := `{app="api-gateway"}`
	count := `count_over_time(` + sel + `[1m])`
	rate := `rate(` + sel + `[1m])`
	shapes := []struct {
		name  string
		query string
	}{
		// the request of the registry row and the queries of its clusters
		{"row stdvar without", `stdvar without (app) (count_over_time({namespace="prod"}[1m]))`},
		{"cluster ce65447e", `stdvar without (app) (count_over_time(({namespace="prod"} |~".+bar")[1m]))`},
		{"cluster 307bbdd1 avg binary", `label_replace( avg by (app) ( sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) + sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) / sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) ) * 2, "new", "$1", "app", "f(.*)" )`},
		{"cluster 307bbdd1 bytes_over_time", `label_replace( bytes_over_time(({namespace="prod"} |= "baz" |~ "blip" != "flip" !~ "flap")[5m]), "buzz", "$2", "bar", "(.*):(.*)" )`},
		// the same shapes over data
		{"stdvar without", `stdvar without (app) (` + count + `)`},
		{"stddev without", `stddev without (app) (` + count + `)`},
		{"sum without", `sum without (app) (` + count + `)`},
		{"avg without", `avg without (app) (` + count + `)`},
		{"label_replace of avg binary", `label_replace(avg by (app) (sum by (app) (` + rate + `) + sum by (app) (` + rate + `)), "x", "$1", "app", "(.*)")`},
		{"label_replace of binary", `label_replace(sum by (app) (` + rate + `) + sum by (app) (` + rate + `), "x", "$1", "app", "(.*)")`},
		{"label_replace of binary scalar", `label_replace(sum by (app) (` + rate + `) * 2, "x", "$1", "app", "(.*)")`},
		{"label_replace of nested binary scalar", `label_replace(avg by (app) (sum by (app) (` + rate + `) + sum by (app) (` + rate + `) / sum by (app) (` + rate + `)) * 2, "new", "$1", "app", "f(.*)")`},
		{"label_join of binary", `label_join(sum by (app) (` + count + `) * 2, "z", "-", "app", "app")`},
		{"label_replace", `label_replace(sum by (app) (` + count + `), "x", "$1", "app", "(.*)")`},
		{"label_replace of bytes_over_time", `label_replace(bytes_over_time(` + sel + `[5m]), "buzz", "$2", "app", "(.*)-(.*)")`},
		// the other shapes
		{"range aggregation", count},
		{"sum by", `sum by (app) (` + rate + `)`},
		{"binary", `sum by (app) (` + rate + `) / sum by (app) (` + count + `)`},
		{"binary scalar", `sum(` + count + `) * 60`},
		{"comparison", `sum by (app) (` + count + `) > 0`},
		{"or vector", `sum(` + count + `) or vector(0)`},
		{"topk", `topk(2, sum by (app) (` + count + `))`},
		{"sort", `sort(sum by (app) (` + count + `))`},
		{"absent_over_time", `absent_over_time({app="no-such-app-for-resulttype"}[5m])`},
		{"vector()", `vector(1)`},
		{"vector binary", `vector(1) + vector(2)`},
		{"scalar literal", `1`},
		{"negative literal", `-1`},
		{"scalar binary", `1 + 1`},
		{"log selector", sel},
	}

	ask := func(t *testing.T, base, query, orgID string, at time.Time) (int, string, map[string]interface{}) {
		params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
		return doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{
			"X-Scope-OrgID":                  orgID,
			"X-Loki-Response-Encoding-Flags": "categorize-labels",
		})
	}
	resultType := func(payload map[string]interface{}) string {
		data, _ := payload["data"].(map[string]interface{})
		rt, _ := data["resultType"].(string)
		return rt
	}
	check := func(t *testing.T, query, orgID string) {
		t.Helper()
		at := time.Now()
		lokiStatus, lokiBody, lokiPayload := ask(t, lokiURL, query, orgID, at)
		proxyStatus, proxyBody, proxyPayload := ask(t, proxyURL, query, orgID, at)
		if proxyStatus == http.StatusOK && resultType(proxyPayload) == "" {
			t.Errorf("%s: the proxy answers 200 without a resultType: %s", query, proxyBody)
		}
		if lokiStatus != proxyStatus {
			t.Errorf("%s: HTTP status loki=%d proxy=%d\nloki:  %s\nproxy: %s", query, lokiStatus, proxyStatus, lokiBody, proxyBody)
			return
		}
		if lokiStatus != http.StatusOK {
			return // both reject the query alike
		}
		if lt, pt := resultType(lokiPayload), resultType(proxyPayload); lt == "" || lt != pt {
			t.Errorf("%s: resultType loki=%q proxy=%q\nloki:  %s\nproxy: %s", query, lt, pt, lokiBody, proxyBody)
		}
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) { check(t, shape.query, "0") })
	}

	// Several tenants merge their answers: a vector stays a vector, a log
	// query stays rejected, and a constant needs no tenant.
	t.Run("multi-tenant", func(t *testing.T) {
		for _, query := range []string{
			`sum by (app) (` + count + `)`,
			`label_replace(sum by (app) (` + rate + `) * 2, "x", "$1", "app", "(.*)")`,
			`stdvar without (app) (` + count + `)`,
			`vector(1)`,
			`1`,
			`1 + 1`,
			sel,
		} {
			check(t, query, "0|fake")
		}
	})

	// A vector sample is stamped in seconds (pkg/util/marshal NewVector), not
	// in the nanoseconds of the request.
	t.Run("sample time is seconds", func(t *testing.T) {
		at := time.Now()
		for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
			_, body, payload := ask(t, base, `vector(1)`, "0", at)
			data, _ := payload["data"].(map[string]interface{})
			result, _ := data["result"].([]interface{})
			if len(result) != 1 {
				t.Fatalf("%s: vector(1) holds %d samples: %s", name, len(result), body)
			}
			sample, _ := result[0].(map[string]interface{})
			value, _ := sample["value"].([]interface{})
			ts, _ := value[0].(float64)
			if diff := ts - float64(at.UnixMilli())/1e3; diff < -1 || diff > 1 {
				t.Errorf("%s: vector(1) is stamped %v, want the evaluation time %s in seconds", name, value[0], fmt.Sprint(float64(at.UnixMilli())/1e3))
			}
		}
	})

	// An aggregation over a binary expression (`sum by (job) (a / b)`) is not
	// evaluated by the proxy yet (semantics/aggregation-over-binary-dropped); it
	// may be rejected, but never with a 200 that has no resultType.
	t.Run("aggregation over binary is never typeless", func(t *testing.T) {
		for _, query := range []string{
			`label_replace( sum by (job) ( sum_over_time( {service_name="api-gateway"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] ) / count_over_time({service_name="api-gateway"} | logfmt | label_format foo=bar[5m]) ), "foo", "$1", "service", "(.*):.*" )`,
			`label_replace( sum by (job) ( sum_over_time( {service_name="api-gateway"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] offset 1h ) / count_over_time({service_name="api-gateway"} | logfmt | label_format foo=bar[5m] offset 1h) ), "foo", "$1", "service", "(.*):.*" )`,
		} {
			status, body, payload := ask(t, proxyURL, query, "0", time.Now())
			if status == http.StatusOK && resultType(payload) == "" {
				t.Errorf("%s: the proxy answers 200 without a resultType: %s", query, body)
			}
		}
	})
}
