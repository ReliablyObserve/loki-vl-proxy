package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// An instant query answers with the result type of its expression, never
// without one. Loki v3.7.7:
//
//   - every metric expression is a vector: pkg/logql/engine.go evalSample ends
//     in JoinSampleVector (instant: `return vec, nil`), and vector(n) is a
//     one-sample vector with no labels (evalVector); pkg/querier/queryrange
//     /marshal.go ResultToResponse labels promql.Vector loghttp.ResultTypeVector;
//   - a scalar literal, or a binary operation of two literals which the parser
//     folds into one (syntax/ast.go mustNewBinOpExpr), is a scalar (evalLiteral);
//   - a log selector is no instant query: pkg/querier/http.go
//     InstantQueryHandler fails it with ErrUnsupportedSyntaxForInstantQuery
//     (HTTP 400, pkg/util/server/error.go), so an instant answer is never
//     `streams`;
//   - a request wholly before max_query_lookback is answered with an empty
//     vector, whatever the expression (queryrange/codec.go NewEmptyResponse,
//     LokiInstantRequest).
//
// The sample time of a vector or scalar is seconds (model.Time, millisecond
// precision), not nanoseconds (pkg/util/marshal/query.go NewVector, NewScalar).

type instantBackendMode string

const (
	instantBackendData    instantBackendMode = "data"
	instantBackendEmpty   instantBackendMode = "empty"
	instantBackendNoType  instantBackendMode = "no-result-type"
	instantBackendBad400  instantBackendMode = "backend-400"
	instantBackendDown500 instantBackendMode = "backend-500"
)

// instantBackend is a VictoriaLogs that answers every stats_query and every raw
// query in one mode: with series, with none, without the resultType field,
// or with an error status.
func instantBackend(t *testing.T, mode instantBackendMode) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if q := r.FormValue("query"); strings.Contains(q, "__binary__") || strings.Contains(q, "|||") {
			// VictoriaLogs cannot parse an internal marker: a marker that
			// reaches it is a proxy bug, answered as VictoriaLogs would.
			t.Errorf("an internal marker reached VictoriaLogs: %s", q)
			http.Error(w, "cannot parse query: unexpected token", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			switch mode {
			case instantBackendEmpty:
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
			case instantBackendNoType:
				_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"metric":{"app":"a"},"value":[1704067200,"5"]}]}}`))
			case instantBackendBad400:
				http.Error(w, "cannot parse stats", http.StatusBadRequest)
			case instantBackendDown500:
				http.Error(w, "storage unavailable", http.StatusInternalServerError)
			default:
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[` +
					`{"metric":{"app":"a","_stream":"{app=\"a\"}","level":"info"},"value":[1704067200,"5"]},` +
					`{"metric":{"app":"b","_stream":"{app=\"b\"}","level":"info"},"value":[1704067200,"7"]}]}}`))
			}
		case "/select/logsql/query":
			switch mode {
			case instantBackendBad400:
				http.Error(w, "cannot parse query", http.StatusBadRequest)
			case instantBackendDown500:
				http.Error(w, "storage unavailable", http.StatusInternalServerError)
			case instantBackendEmpty:
			default:
				a, b := "level=info latency=12 bytes=5", "level=info latency=30 bytes=9"
				if strings.Contains(r.FormValue("query"), "unpack_json") {
					a, b = `{"level":"info","latency":12}`, `{"level":"info","latency":30}`
				}
				_, _ = w.Write([]byte(`{"_time":"2024-01-01T00:00:00Z","_msg":` + strconv.Quote(a) + `,"_stream":"{app=\"a\"}","app":"a"}` + "\n" +
					`{"_time":"2024-01-01T00:00:10Z","_msg":` + strconv.Quote(b) + `,"_stream":"{app=\"b\"}","app":"b"}` + "\n"))
			}
		case "/health":
			_, _ = w.Write([]byte("ok"))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type instantShape struct {
	name  string
	query string
	// want is the resultType of a successful answer; "error" is a rejected
	// query (HTTP 400) on every backend.
	want string
}

// instantRejectedShapes are the shapes the proxy does not evaluate yet, an
// aggregation over a binary operation: it may answer with an error status, but
// never with a 200 that has no resultType.
var instantRejectedShapes = map[string]bool{
	"parity f2066991 unwrap ratio": true,
	"parity f2066991 offset":       true,
}

const instantNS = `{namespace="prod"}`

func instantShapes() []instantShape {
	rate := `rate(` + instantNS + `[1m])`
	count := `count_over_time(` + instantNS + `[1m])`
	return []instantShape{
		// native stats
		{"range aggregation", count, "vector"},
		{"range aggregation line filter", `count_over_time(` + instantNS + ` |~ ".+bar" [1m])`, "vector"},
		{"parenthesized log range", `count_over_time((` + instantNS + ` |~ ".+bar")[1m])`, "vector"},
		{"bytes_over_time", `bytes_over_time(` + instantNS + `[5m])`, "vector"},
		{"sum", `sum(` + count + `)`, "vector"},
		{"sum by", `sum by (app) (` + rate + `)`, "vector"},
		{"avg by", `avg by (app) (` + count + `)`, "vector"},
		{"max", `max(` + count + `)`, "vector"},
		{"count by", `count by (app) (` + count + `)`, "vector"},
		{"stddev", `stddev(` + count + `)`, "vector"},
		{"stdvar by", `stdvar by (app) (` + count + `)`, "vector"},
		{"topk", `topk(2, sum by (app) (` + count + `))`, "vector"},
		{"bottomk", `bottomk(1, sum by (app) (` + count + `))`, "vector"},
		{"sort", `sort(sum by (app) (` + count + `))`, "vector"},
		{"quantile_over_time", `quantile_over_time(0.9, ` + instantNS + ` | logfmt | unwrap latency [5m])`, "vector"},
		{"sum over unwrap", `sum(sum_over_time(` + instantNS + ` | logfmt | unwrap latency [5m]))`, "vector"},
		{"bare parser", `count_over_time(` + instantNS + ` | logfmt [5m])`, "vector"},
		{"absent_over_time", `absent_over_time(` + instantNS + `[5m])`, "vector"},
		{"offset", `sum(count_over_time(` + instantNS + `[1m] offset 5m))`, "vector"},
		// without grouping (post-processed)
		{"stdvar without", `stdvar without (app) (` + count + `)`, "vector"},
		{"stdvar without line filter", `stdvar without (app) (count_over_time((` + instantNS + ` |~ ".+bar")[1m]))`, "vector"},
		{"sum without", `sum without (app) (` + count + `)`, "vector"},
		{"avg without", `avg without (app) (` + count + `)`, "vector"},
		{"sum without nothing", `sum without () (` + count + `)`, "vector"},
		// label_replace and label_join
		{"label_replace", `label_replace(sum by (app) (` + count + `), "x", "$1", "app", "(.*)")`, "vector"},
		{"label_replace line filter", `label_replace(bytes_over_time((` + instantNS + ` |= "baz" |~ "blip" != "flip" !~ "flap")[5m]), "buzz", "$2", "bar", "(.*):(.*)")`, "vector"},
		{"label_replace nested", `label_replace(label_replace(sum by (app) (` + count + `), "x", "$1", "app", "(.*)"), "y", "$1", "x", "(.*)")`, "vector"},
		{"label_join", `label_join(sum by (app, level) (` + count + `), "z", "-", "app", "level")`, "vector"},
		{"label_replace of avg without", `label_replace(avg without (app) (` + count + `), "x", "$1", "level", "(.*)")`, "vector"},
		// binary operations
		{"vector add", `sum by (app) (` + rate + `) + sum by (app) (` + rate + `)`, "vector"},
		{"vector divide scalar", `sum by (app) (` + rate + `) / 60`, "vector"},
		{"scalar multiply vector", `2 * sum by (app) (` + count + `)`, "vector"},
		{"comparison filter", `sum by (app) (` + count + `) > 5`, "vector"},
		{"comparison bool", `sum by (app) (` + count + `) > bool 5`, "vector"},
		{"or vector zero", `sum(` + count + `) or vector(0)`, "vector"},
		{"and", `sum by (app) (` + count + `) and sum by (app) (` + rate + `)`, "vector"},
		{"on matching", `sum by (app) (` + count + `) / on (app) sum by (app) (` + rate + `)`, "vector"},
		{"nested binary", `sum(` + count + `) + sum(` + count + `) / sum(` + count + `)`, "vector"},
		{"label_replace of binary", `label_replace(sum by (app) (` + rate + `) + sum by (app) (` + rate + `), "x", "$1", "app", "(.*)")`, "vector"},
		{"label_replace of binary scalar", `label_replace(sum by (app) (` + rate + `) * 2, "x", "$1", "app", "(.*)")`, "vector"},
		{"label_replace of avg over nested binary", `label_replace(avg by (app) (sum by (app) (` + rate + `) + sum by (app) (` + rate + `) / sum by (app) (` + rate + `)) * 2, "new", "$1", "app", "f(.*)")`, "vector"},
		{"label_replace of unwrap binary", `label_replace(sum by (job) (sum_over_time(` + instantNS + ` | logfmt | unwrap latency | __error__!~".*" [5m]) / count_over_time(` + instantNS + ` | logfmt | label_format foo=bar [5m])), "foo", "$1", "service", "(.*):.*")`, "vector"},
		// the queries of the parity run that answered 200 without a resultType
		// (clusters 307bbdd1, f2066991, ce65447e), verbatim
		{"parity 307bbdd1 avg binary", `label_replace( avg by (app) ( sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) + sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) / sum by (app) (rate({namespace="prod"} |~".+bar" [1m])) ) * 2, "new", "$1", "app", "f(.*)" )`, "vector"},
		{"parity 307bbdd1 bytes_over_time", `label_replace( bytes_over_time(({namespace="prod"} |= "baz" |~ "blip" != "flip" !~ "flap")[5m]), "buzz", "$2", "bar", "(.*):(.*)" )`, "vector"},
		{"parity f2066991 unwrap ratio", `label_replace( sum by (job) ( sum_over_time( {service_name="api-gateway"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] ) / count_over_time({service_name="api-gateway"} | logfmt | label_format foo=bar[5m]) ), "foo", "$1", "service", "(.*):.*" )`, "vector"},
		{"parity f2066991 offset", `label_replace( sum by (job) ( sum_over_time( {service_name="api-gateway"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) | __error__!~".*" [5m] offset 1h ) / count_over_time({service_name="api-gateway"} | logfmt | label_format foo=bar[5m] offset 1h) ), "foo", "$1", "service", "(.*):.*" )`, "vector"},
		{"parity ce65447e stdvar without", `stdvar without (app) (count_over_time(({namespace="prod"} |~".+bar")[1m]))`, "vector"},
		// constants
		{"vector()", `vector(1)`, "vector"},
		{"vector binary", `vector(1) + vector(2)`, "vector"},
		{"vector compared", `vector(1) > bool 0`, "vector"},
		{"scalar literal", `1`, "scalar"},
		{"negative literal", `-1`, "scalar"},
		{"scalar binary", `1 + 1`, "scalar"},
		{"scalar nested binary", `2 * (3 + 4)`, "scalar"},
		// a log query is no instant query
		{"log selector", instantNS, "error"},
		{"log selector with filter", instantNS + ` |= "x" | json`, "error"},
	}
}

type instantAnswer struct {
	code int
	body []byte
	resp struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Data      *struct {
			ResultType string            `json:"resultType"`
			Result     []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	raw map[string]json.RawMessage
}

// instantClients are the header sets of the clients that read an instant
// answer: a bare client, Grafana's Explore and Logs Drilldown. Grafana-sourced
// requests take their own failure and series-limit branches.
var instantClients = map[string]map[string]string{
	"curl":      nil,
	"explore":   {"User-Agent": "Grafana/13.0.1", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana"},
	"drilldown": {"User-Agent": "Grafana/13.0.1", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana-lokiexplore-app"},
}

func askInstant(t *testing.T, p *Proxy, q, orgID string, extra url.Values) instantAnswer {
	return askInstantAs(t, p, q, orgID, extra, nil)
}

func askInstantAs(t *testing.T, p *Proxy, q, orgID string, extra url.Values, headers map[string]string) instantAnswer {
	t.Helper()
	params := url.Values{"query": {q}, "time": {"1704067200"}}
	for k, v := range extra {
		params[k] = v
	}
	req := httptest.NewRequest("GET", "/loki/api/v1/query?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", orgID)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)
	a := instantAnswer{code: rec.Code, body: rec.Body.Bytes()}
	if err := json.Unmarshal(a.body, &a.resp); err != nil {
		t.Fatalf("%s: body is not JSON (HTTP %d): %v\n%s", q, a.code, err, a.body)
	}
	_ = json.Unmarshal(a.body, &a.raw)
	return a
}

// requireInstantAnswer holds an answer to Loki's contract: a 2xx carries
// status success, the resultType want and a result of that type's shape; a
// failure is an error status with an error body and no data. The reverse of
// either, a 200 that holds an error or a success without a type, is what a
// client cannot decode.
func requireInstantAnswer(t *testing.T, label string, a instantAnswer, want string, mayFail bool) {
	t.Helper()
	if a.code >= 400 {
		if !mayFail {
			t.Errorf("%s: HTTP %d, want a %s answer\n%s", label, a.code, want, a.body)
		}
		if a.resp.Status != "error" {
			t.Errorf("%s: HTTP %d with status %q, want error\n%s", label, a.code, a.resp.Status, a.body)
		}
		if a.resp.Data != nil {
			t.Errorf("%s: an error answer holds data\n%s", label, a.body)
		}
		return
	}
	if want == "error" {
		t.Errorf("%s: HTTP %d, want Loki's 400 for a log query as an instant query\n%s", label, a.code, a.body)
		return
	}
	if a.resp.Status != "success" || a.resp.Data == nil {
		t.Errorf("%s: HTTP %d holds status %q and no data, want a %s answer (an error under a 200)\n%s", label, a.code, a.resp.Status, want, a.body)
		return
	}
	if got := a.resp.Data.ResultType; got != want {
		t.Errorf("%s: resultType %q, want %q\n%s", label, got, want, a.body)
		return
	}
	requireInstantResultShape(t, label, a, want)
}

// requireInstantResultShape checks the result against loghttp's types: a vector
// sample is {metric, value:[seconds, "v"]}, a scalar is [seconds, "v"].
func requireInstantResultShape(t *testing.T, label string, a instantAnswer, want string) {
	t.Helper()
	checkPoint := func(raw json.RawMessage) {
		var point []any
		if err := json.Unmarshal(raw, &point); err != nil || len(point) != 2 {
			t.Errorf("%s: sample point %s is not [time, value]\n%s", label, raw, a.body)
			return
		}
		ts, ok := point[0].(float64)
		if !ok || ts < 1e9 || ts > 1e11 {
			t.Errorf("%s: sample time %v is not seconds since the epoch\n%s", label, point[0], a.body)
		}
		if _, ok := point[1].(string); !ok {
			t.Errorf("%s: sample value %v is not a string\n%s", label, point[1], a.body)
		}
	}
	switch want {
	case "scalar":
		var data struct {
			Result json.RawMessage `json:"result"`
		}
		_ = json.Unmarshal(a.raw["data"], &data)
		checkPoint(data.Result)
	case "vector":
		for _, item := range a.resp.Data.Result {
			var sample struct {
				Metric map[string]string `json:"metric"`
				Value  json.RawMessage   `json:"value"`
				Values json.RawMessage   `json:"values"`
			}
			if err := json.Unmarshal(item, &sample); err != nil || sample.Metric == nil || sample.Value == nil || sample.Values != nil {
				t.Errorf("%s: %s is not a vector sample {metric, value}\n%s", label, item, a.body)
				continue
			}
			checkPoint(sample.Value)
		}
	}
}

// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestInstantQueryAlwaysHasLokiResultType(t *testing.T) {
	modes := []instantBackendMode{instantBackendData, instantBackendEmpty, instantBackendNoType, instantBackendBad400, instantBackendDown500}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			p := newTestProxy(t, instantBackend(t, mode).URL)
			for client, headers := range instantClients {
				t.Run(client, func(t *testing.T) {
					for _, shape := range instantShapes() {
						t.Run(shape.name, func(t *testing.T) {
							a := askInstantAs(t, p, shape.query, "0", nil, headers)
							// A backend that fails fails the query; a literal or vector()
							// has no backend to fail. A log selector is rejected whatever
							// the backend says.
							mayFail := mode == instantBackendBad400 || mode == instantBackendDown500 || shape.want == "error" || instantRejectedShapes[shape.name]
							if shape.want == "scalar" || strings.HasPrefix(shape.query, "vector(") {
								mayFail = false
							}
							requireInstantAnswer(t, string(mode)+" "+client+" "+shape.query, a, shape.want, mayFail)
							if shape.want == "error" && a.code != http.StatusBadRequest {
								t.Errorf("%s: HTTP %d, want 400", shape.query, a.code)
							}
						})
					}
				})
			}
		})
	}
}

// A backend failure under a post-processed query (without, label_replace,
// label_join) keeps the failing status: the buffered answer used to be sent
// with 200, an error body under a success status.
// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestInstantQueryPostProcessedBackendFailureKeepsStatus(t *testing.T) {
	for _, tc := range []struct {
		mode instantBackendMode
		code int
	}{{instantBackendBad400, http.StatusBadRequest}, {instantBackendDown500, http.StatusInternalServerError}} {
		p := newTestProxy(t, instantBackend(t, tc.mode).URL)
		for _, q := range []string{
			`stdvar without (app) (count_over_time(` + instantNS + `[1m]))`,
			`label_replace(sum by (app) (count_over_time(` + instantNS + `[1m])), "x", "$1", "app", "(.*)")`,
			`label_join(sum by (app, level) (count_over_time(` + instantNS + `[1m])), "z", "-", "app", "level")`,
		} {
			a := askInstant(t, p, q, "0", nil)
			if a.code != tc.code || a.resp.Status != "error" {
				t.Errorf("%s with %s: HTTP %d status %q, want HTTP %d with status error\n%s", q, tc.mode, a.code, a.resp.Status, tc.code, a.body)
			}
		}
	}
}

// Sample times of vector() are seconds, like every other sample, and the
// scalar of a literal follows pkg/util/marshal NewScalar.
// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestInstantConstantsCarrySecondsTimestamps(t *testing.T) {
	p := newTestProxy(t, instantBackend(t, instantBackendData).URL)
	for _, tc := range []struct{ query, time string }{
		{`vector(1)`, "1704067200"},
		{`vector(1)`, "1704067200000000000"},
		{`vector(1)`, "2024-01-01T00:00:00Z"},
		{`vector(1) + vector(2)`, "1704067200.5"},
		{`1`, "1704067200"},
		{`1 + 1`, "1704067200000000000"},
	} {
		a := askInstant(t, p, tc.query, "0", url.Values{"time": {tc.time}})
		want := "vector"
		if !strings.Contains(tc.query, "vector") {
			want = "scalar"
		}
		requireInstantAnswer(t, tc.query+" at "+tc.time, a, want, false)
	}
	// without a time, the evaluation time is now
	req := httptest.NewRequest("GET", "/loki/api/v1/query?query="+url.QueryEscape(`1`), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)
	var got struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []any  `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Data.ResultType != "scalar" || len(got.Data.Result) != 2 {
		t.Fatalf("scalar literal without time: HTTP %d %s", rec.Code, rec.Body.String())
	}
	if ts, _ := got.Data.Result[0].(float64); time.Since(time.Unix(int64(ts), 0)) > time.Minute {
		t.Fatalf("scalar literal without time is stamped %v, want now", got.Data.Result[0])
	}
}

// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestInstantQueryMultiTenantResultTypes(t *testing.T) {
	p := newTestProxy(t, instantBackend(t, instantBackendData).URL)
	for _, shape := range instantShapes() {
		t.Run(shape.name, func(t *testing.T) {
			a := askInstant(t, p, shape.query, "a|b", nil)
			requireInstantAnswer(t, "a|b "+shape.query, a, shape.want, shape.want == "error" || instantRejectedShapes[shape.name])
			if shape.want == "error" && a.code != http.StatusBadRequest {
				t.Errorf("a|b %s: HTTP %d, want 400", shape.query, a.code)
			}
		})
	}
	// Every tenant filtered out by __tenant_id__ leaves nothing to ask; a log
	// query is still no instant query, whatever tenants are left.
	a := askInstant(t, p, `{__tenant_id__="nobody"}`, "a|b", nil)
	requireInstantAnswer(t, "a|b nobody", a, "error", true)
	if a.code != http.StatusBadRequest {
		t.Errorf("log query for no tenant: HTTP %d, want 400\n%s", a.code, a.body)
	}
}

// A label transform around a binary expression evaluates the expression: its
// operands run as instant queries and combine, and the transform applies to
// the result. The marker the translator builds for the expression used to be
// sent to VictoriaLogs as a query.
// conformance: semantics/instant-result-type-missing, loki_api_v1_query
func TestInstantLabelTransformOfBinaryEvaluatesTheBinary(t *testing.T) {
	p := newTestProxy(t, instantBackend(t, instantBackendData).URL)
	count := `count_over_time(` + instantNS + `[1m])`
	for _, q := range []string{
		`label_replace(sum by (app) (` + count + `) * 2, "x", "$1", "app", "(.*)")`,
		`label_replace(2 * sum by (app) (` + count + `), "x", "$1", "app", "(.*)")`,
		`label_replace(label_replace(sum by (app) (` + count + `) * 2, "x", "$1", "app", "(.*)"), "y", "$1", "app", "(.*)")`,
		`label_join(sum by (app) (` + count + `) * 2, "z", "-", "app", "app")`,
	} {
		a := askInstant(t, p, q, "0", nil)
		requireInstantAnswer(t, q, a, "vector", false)
		got := map[string]string{}
		for _, item := range a.resp.Data.Result {
			var sample struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			}
			_ = json.Unmarshal(item, &sample)
			got[sample.Metric["app"]], _ = sample.Value[1].(string)
		}
		if got["a"] != "10" || got["b"] != "14" || len(got) != 2 {
			t.Errorf("%s: values by app %v, want a=10 and b=14 (the backend's 5 and 7, doubled)\n%s", q, got, a.body)
		}
	}
}

func TestStripLabelTransforms(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`rate({a="b"}[1m])`, `rate({a="b"}[1m])`},
		{`label_replace(rate({a="b"}[1m]), "x", "$1", "a", "(.*)")`, `rate({a="b"}[1m])`},
		{`label_join(sum by (a, b) (rate({a="b"}[1m])), "z", "-", "a", "b")`, `sum by (a, b) (rate({a="b"}[1m]))`},
		{`label_join(label_replace( x + y , "x", "$1", "a", "(.*)"), "z", "-", "a")`, `x + y`},
		{`label_replace(rate({a="b"}[1m])`, `label_replace(rate({a="b"}[1m])`}, // no argument list: left as it is
		{`label_replace(`, `label_replace(`},
	} {
		if got := stripLabelTransforms(tc.in); got != tc.want {
			t.Errorf("stripLabelTransforms(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
