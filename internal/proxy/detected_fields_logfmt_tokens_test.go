package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Detected fields of a query that keeps an unpack_logfmt (a detected_level
// pill, or a field filter behind `| json | logfmt`) must be the fields Loki
// lists: VictoriaLogs' logfmt parser turns every word of a JSON or plain-text
// line into an empty field, and field_names lists all of them.
//
// Loki v3.7.9 builds the list from the sampled lines only
// (pkg/querier/queryrange/detected_fields.go:282-363 parseDetectedFields):
// structured metadata, the labels the query's own parsers extracted
// (entry.Parsed), and a json re-parse of the line, logfmt when json fails
// (parseEntry, :387-460). Its logfmt parser skips a key without a value
// (pkg/logql/log/parser.go:423) and rejects a key holding a quote
// (pkg/logql/log/logfmt/decode.go:78-81), so a JSON or plain-text line gives
// it no logfmt keys.

// fakeLogfmtVL is a recording fake VictoriaLogs for detected_fields. A line's
// stored fields are its stream labels, its structured metadata and, for a JSON
// line, its keys flattened with dots. `| unpack_logfmt` without a fields list
// adds VictoriaLogs' logfmt fields of _msg (lib/logstorage/logfmt_parser.go:
// a token without '=' is a field with an empty value), and field_names
// counts each column of the result on every row (pipe_field_names.go).
type fakeLogfmtVL struct {
	srv   *httptest.Server
	lines []fakeVLLine
	mu    sync.Mutex
	log   []string
	bytes int
}

type fakeVLLine struct {
	stream map[string]string
	msg    string
	fields map[string]string
}

func (l fakeVLLine) streamString() string {
	keys := make([]string, 0, len(l.stream))
	for k := range l.stream {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Quote(l.stream[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// unrestrictedUnpackLogfmtRE matches an unpack_logfmt pipe that reads every
// key. The detected_level derivation's unpack_logfmt pipes carry a fields list.
var unrestrictedUnpackLogfmtRE = regexp.MustCompile(`\| unpack_logfmt(?:\s*\||\s*$)`)

// vlLogfmtFields is VictoriaLogs' logfmtParser.parse (v1.51.1).
func vlLogfmtFields(s string) map[string]string {
	out := map[string]string{}
	for {
		n := strings.IndexAny(s, "= ")
		if n < 0 {
			out[s] = ""
			return out
		}
		name, ch := s[:n], s[n]
		s = s[n+1:]
		if ch == ' ' {
			out[name] = ""
			continue
		}
		if len(s) == 0 {
			out[name] = ""
			return out
		}
		if s[0] == '"' || s[0] == '`' {
			if qp, err := strconv.QuotedPrefix(s); err == nil {
				if v, err := strconv.Unquote(qp); err == nil {
					out[name] = v
					s = s[len(qp):]
					if len(s) == 0 || s[0] != ' ' {
						return out
					}
					s = s[1:]
					continue
				}
			}
		}
		m := strings.IndexByte(s, ' ')
		if m < 0 {
			out[name] = s
			return out
		}
		out[name] = s[:m]
		s = s[m+1:]
	}
}

func flattenJSONFields(prefix string, v interface{}, out map[string]string) {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, vv := range x {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flattenJSONFields(key, vv, out)
		}
	case string:
		out[prefix] = x
	case nil:
	default:
		b, _ := json.Marshal(x)
		out[prefix] = string(b)
	}
}

func newFakeLogfmtVL(t testing.TB, lines []fakeVLLine) *fakeLogfmtVL {
	t.Helper()
	f := &fakeLogfmtVL{lines: lines}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeLogfmtVL) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func (f *fakeLogfmtVL) reset() {
	f.mu.Lock()
	f.log, f.bytes = nil, 0
	f.mu.Unlock()
}

func (f *fakeLogfmtVL) rowFields(line fakeVLLine, query string) map[string]string {
	row := map[string]string{}
	for k, v := range line.fields {
		row[k] = v
	}
	if unrestrictedUnpackLogfmtRE.MatchString(query) {
		for k, v := range vlLogfmtFields(line.msg) {
			if k != "" {
				row[k] = v
			}
		}
	}
	return row
}

func (f *fakeLogfmtVL) serve(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	query := r.Form.Get("query")
	var body []byte
	switch r.URL.Path {
	case "/select/logsql/field_names":
		hits := map[string]int{}
		for _, line := range f.lines {
			for k := range f.rowFields(line, query) {
				hits[k]++
			}
			hits["_msg"]++
			hits["_stream"]++
			hits["_time"]++
		}
		names := make([]string, 0, len(hits))
		for k := range hits {
			names = append(names, k)
		}
		sort.Strings(names)
		values := make([]map[string]interface{}, 0, len(names))
		for _, k := range names {
			values = append(values, map[string]interface{}{"value": k, "hits": hits[k]})
		}
		body, _ = json.Marshal(map[string]interface{}{"values": values})
		w.Header().Set("Content-Type", "application/json")
	case "/select/logsql/field_values":
		field := r.Form.Get("field")
		hits := map[string]int{}
		for _, line := range f.lines {
			if v := f.rowFields(line, query)[field]; v != "" {
				hits[v]++
			}
		}
		values := make([]map[string]interface{}, 0, len(hits))
		for v, n := range hits {
			values = append(values, map[string]interface{}{"value": v, "hits": n})
		}
		body, _ = json.Marshal(map[string]interface{}{"values": values})
		w.Header().Set("Content-Type", "application/json")
	case "/select/logsql/query":
		limit, _ := strconv.Atoi(r.Form.Get("limit"))
		var b strings.Builder
		for i, line := range f.lines {
			if limit > 0 && i >= limit {
				break
			}
			row := map[string]string{
				"_time":   fmt.Sprintf("2026-11-03T14:%02d:%02dZ", (i/60)%60, i%60),
				"_msg":    line.msg,
				"_stream": line.streamString(),
			}
			for k, v := range f.rowFields(line, query) {
				row[k] = v
			}
			rb, _ := json.Marshal(row)
			b.Write(rb)
			b.WriteByte('\n')
		}
		body = []byte(b.String())
		w.Header().Set("Content-Type", "application/x-ndjson")
	default:
		body = []byte(`{"values":[]}`)
		w.Header().Set("Content-Type", "application/json")
	}
	f.mu.Lock()
	f.log = append(f.log, r.URL.Path+" "+query)
	f.bytes += len(body)
	f.mu.Unlock()
	_, _ = w.Write(body)
}

// pythonJSONLine renders an object the way the e2e generator's json.dumps
// does: ", " and ": " separators.
func pythonJSONLine(keys []string, values map[string]interface{}) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteString(": ")
		if v, ok := values[k].(map[string]interface{}); ok {
			inner := make([]string, 0, len(v))
			for ik := range v {
				inner = append(inner, ik)
			}
			sort.Strings(inner)
			b.WriteString(pythonJSONLine(inner, v))
			continue
		}
		vb, _ := json.Marshal(values[k])
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.String()
}

func newFakeLine(stream, metadata map[string]string, msg string) fakeVLLine {
	fields := map[string]string{}
	for k, v := range stream {
		fields[k] = v
	}
	for k, v := range metadata {
		fields[k] = v
	}
	var obj map[string]interface{}
	if json.Unmarshal([]byte(msg), &obj) == nil {
		flattenJSONFields("", obj, fields)
	}
	return fakeVLLine{stream: stream, msg: msg, fields: fields}
}

func generatorStream(service, level string, extra map[string]string) map[string]string {
	s := map[string]string{
		"service_name": service, "app": service, "namespace": "prod", "cluster": "us-east-1",
		"env": "production", "level": level, "pod": service + "-7d9f-x1", "container": strings.Split(service, "-")[0],
	}
	for k, v := range extra {
		s[k] = v
	}
	return s
}

// apiGatewayFakeLines mirrors the e2e generator's api-gateway error lines.
func apiGatewayFakeLines() []fakeVLLine {
	keys := []string{"service", "method", "path", "status", "duration_ms", "trace_id", "span_id", "request_id", "ip", "user_id", "level", "region", "version", "error"}
	stream := generatorStream("api-gateway", "error", map[string]string{"version": "v2"})
	out := make([]fakeVLLine, 0, 40)
	for i := 0; i < 40; i++ {
		out = append(out, newFakeLine(stream, nil, pythonJSONLine(keys, map[string]interface{}{
			"service":     map[string]interface{}{"name": "api-gateway"},
			"method":      []string{"GET", "POST", "PUT"}[i%3],
			"path":        fmt.Sprintf("/api/v1/users/%d", i),
			"status":      []int{500, 502, 503}[i%3],
			"duration_ms": 100 + i,
			"trace_id":    fmt.Sprintf("trace%04d", i),
			"span_id":     fmt.Sprintf("span%04d", i),
			"request_id":  fmt.Sprintf("req%04d", i),
			"ip":          fmt.Sprintf("10.0.0.%d", i),
			"user_id":     fmt.Sprintf("user%d", i%10),
			"level":       "error",
			"region":      []string{"us-east-1", "us-west-2", "eu-west-1"}[i%3],
			"version":     []string{"v1", "v2"}[i%2],
			"error":       []string{"gateway timeout", "service unavailable"}[i%2],
		})))
	}
	return out
}

// otelCollectorFakeLines mirrors the generator's otel-collector warn lines:
// a JSON body plus OTel structured metadata.
func otelCollectorFakeLines() []fakeVLLine {
	keys := []string{"message", "level", "pipeline", "received", "dropped", "export_ms"}
	stream := generatorStream("otel-collector", "warn", map[string]string{"namespace": "monitoring", "component": "collector"})
	pipelines := []string{"traces/otlp", "metrics/prometheus", "logs/loki"}
	out := make([]fakeVLLine, 0, 30)
	for i := 0; i < 30; i++ {
		meta := map[string]string{
			"trace_id": fmt.Sprintf("t%04d", i), "span_id": fmt.Sprintf("s%04d", i),
			"service.name": "otel-collector", "service.version": "0.96.0",
			"deployment.environment": "production", "k8s.pod.name": "otel-collector-5c-x",
			"k8s.namespace.name": "monitoring", "k8s.cluster.name": "prod-eu",
		}
		out = append(out, newFakeLine(stream, meta, pythonJSONLine(keys, map[string]interface{}{
			"message": "pipeline " + pipelines[i%3] + " flushed", "level": "warn",
			"pipeline": pipelines[i%3], "received": 1000 + i, "dropped": 1 + i%7, "export_ms": 10 + i,
		})))
	}
	return out
}

func paymentLogfmtFakeLines() []fakeVLLine {
	stream := generatorStream("payment-service", "warn", nil)
	out := make([]fakeVLLine, 0, 30)
	for i := 0; i < 30; i++ {
		out = append(out, newFakeLine(stream, nil, fmt.Sprintf(`level=warn msg="payment retry scheduled" tx_id=tx%d provider=stripe attempt=%d latency_ms=%d`, i, i%3, 100+i)))
	}
	return out
}

func dbPostgresPlainFakeLines() []fakeVLLine {
	stream := generatorStream("db-postgres", "error", nil)
	out := make([]fakeVLLine, 0, 30)
	for i := 0; i < 30; i++ {
		out = append(out, newFakeLine(stream, nil, fmt.Sprintf("2026-11-03 14:00:%02d.123 UTC [%d] ERROR:  deadlock detected on relation \"orders\", 500, {retry}", i, 1000+i)))
	}
	return out
}

type detectedFieldsMode struct {
	name string
	opts []testProxyOption
}

func withLokiProfileNames() testProxyOption {
	return func(cfg *Config) {
		cfg.LabelStyle = LabelStyleUnderscores
		cfg.MetadataFieldMode = MetadataFieldModeTranslated
	}
}

var detectedFieldsModes = []detectedFieldsMode{
	{name: "default"},
	{name: "loki-profile", opts: []testProxyOption{withLokiProfileNames()}},
}

func newDetectedFieldsTestProxy(t testing.TB, backend string, opts ...testProxyOption) *Proxy {
	t.Helper()
	cfg := Config{BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"}
	for _, o := range opts {
		o(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// detectedFieldLabels runs one detected_fields request and returns its field
// labels with their parsers.
func detectedFieldLabels(t testing.TB, p *Proxy, query, end string) map[string][]string {
	t.Helper()
	return detectedFieldLabelsWindow(t, p, query, "2026-11-03T13:57:00.000Z", end)
}

func detectedFieldLabelsWindow(t testing.TB, p *Proxy, query, start, end string) map[string][]string {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start)
	params.Set("end", end)
	rec := httptest.NewRecorder()
	p.handleDetectedFields(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("detected_fields status %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Fields []struct {
			Label   string   `json:"label"`
			Parsers []string `json:"parsers"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v: %s", err, rec.Body.String())
	}
	out := make(map[string][]string, len(resp.Fields))
	for _, f := range resp.Fields {
		out[f.Label] = f.Parsers
	}
	return out
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

const drilldownParserTail = `    | json  | logfmt | drop __error__, __error_details__ `

// conformance: semantics/detected-fields-no-logfmt-token-fields
func TestDetectedFields_NoLogfmtTokenFields_EdgeTable(t *testing.T) {
	apiJSON := []string{"detected_level", "duration_ms", "error", "ip", "method", "path", "region", "request_id", "span_id", "status", "trace_id", "user_id"}
	otelJSON := []string{"detected_level", "dropped", "export_ms", "message", "pipeline", "received"}
	payment := []string{"attempt", "detected_level", "latency_ms", "msg", "provider", "tx_id"}
	cases := []struct {
		name    string
		lines   []fakeVLLine
		query   string
		want    []string // exact label set, every mode
		wantAny []string // must also be present (mode-dependent naming excluded from want)
	}{
		// The reported page: Fields tab of a JSON service with a level pill.
		{"json level pill", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail, apiJSON, nil},
		{"json no filter", apiGatewayFakeLines(), `{service_name="api-gateway"}` + drilldownParserTail, apiJSON, nil},
		{"json level regexp pill", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level=~"error|warn"` + drilldownParserTail, apiJSON, nil},
		{"json level negated pill", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level!="info"` + drilldownParserTail, apiJSON, nil},
		// A field filter behind the parsers keeps | logfmt in the scan.
		{"json field filter", apiGatewayFakeLines(), `{service_name="api-gateway"}` + drilldownParserTail + `| method="GET"`, apiJSON, nil},
		{"json negative field filter", apiGatewayFakeLines(), `{service_name="api-gateway"}` + drilldownParserTail + `| status!="200"`, apiJSON, nil},
		{"json level pill and field filter", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail + `| method="GET"`, apiJSON, nil},
		// OTel structured metadata stays listed (mode decides dotted or underscored names).
		{"otel level pill", otelCollectorFakeLines(), `{service_name="otel-collector"} | detected_level="warn"` + drilldownParserTail, nil, append([]string{"trace_id", "span_id"}, otelJSON...)},
		{"otel no filter", otelCollectorFakeLines(), `{service_name="otel-collector"}` + drilldownParserTail, nil, append([]string{"trace_id", "span_id"}, otelJSON...)},
		// logfmt keys of logfmt lines stay listed.
		{"logfmt level pill", paymentLogfmtFakeLines(), `{service_name="payment-service"} | detected_level="warn"` + drilldownParserTail, payment, nil},
		{"logfmt no filter", paymentLogfmtFakeLines(), `{service_name="payment-service"}` + drilldownParserTail, payment, nil},
		{"logfmt field filter", paymentLogfmtFakeLines(), `{service_name="payment-service"}` + drilldownParserTail + `| provider="stripe"`, payment, nil},
		// A plain-text line has no fields beside detected_level.
		{"plain level pill", dbPostgresPlainFakeLines(), `{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail, []string{"detected_level"}, nil},
		{"plain no filter", dbPostgresPlainFakeLines(), `{service_name="db-postgres"}` + drilldownParserTail, []string{"detected_level"}, nil},
		{"plain explore level filter", dbPostgresPlainFakeLines(), `{service_name="db-postgres"} | detected_level="error"`, []string{"detected_level"}, nil},
		{"plain logfmt filter", dbPostgresPlainFakeLines(), `{service_name="db-postgres"} | logfmt | level="error"`, []string{"detected_level"}, nil},
	}
	for _, mode := range detectedFieldsModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				vl := newFakeLogfmtVL(t, tc.lines)
				p := newDetectedFieldsTestProxy(t, vl.srv.URL, mode.opts...)
				got := detectedFieldLabels(t, p, tc.query, "2026-11-03T14:57:00.000Z")
				tokens := map[string]struct{}{}
				for _, line := range tc.lines {
					for k, v := range vlLogfmtFields(line.msg) {
						if v == "" {
							tokens[k] = struct{}{}
							tokens[lokiJSONKeyLabel(k)] = struct{}{}
						}
					}
				}
				for label := range got {
					if _, invented := tokens[label]; invented {
						t.Errorf("label %q is a logfmt token of the line, not a field (got %v)", label, sortedKeys(got))
					}
				}
				if tc.want != nil {
					if g := sortedKeys(got); strings.Join(g, ",") != strings.Join(tc.want, ",") {
						t.Errorf("labels\n got %v\nwant %v", g, tc.want)
					}
				}
				for _, w := range tc.wantAny {
					if _, ok := got[w]; !ok {
						t.Errorf("missing %q in %v", w, sortedKeys(got))
					}
				}
			})
		}
	}
}

// A field's values under a level pill still come back once the field_names
// lookup no longer lists the keys of logfmt lines: the values path falls back
// to the sampled lines, as Loki reads them (parseDetectedFieldValues,
// pkg/querier/queryrange/detected_fields.go:134-176).
// conformance: semantics/detected-fields-no-logfmt-token-fields
func TestDetectedFieldValues_LevelPillKeepsLineKeys(t *testing.T) {
	txIDs := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		txIDs = append(txIDs, fmt.Sprintf("tx%d", i))
	}
	sort.Strings(txIDs)
	cases := []struct {
		name, field, query string
		lines              []fakeVLLine
		want               []string
	}{
		{"logfmt key", "tx_id", `{service_name="payment-service"} | detected_level="warn"` + drilldownParserTail, paymentLogfmtFakeLines(), txIDs},
		{"logfmt key, field filter", "provider", `{service_name="payment-service"}` + drilldownParserTail + `| provider="stripe"`, paymentLogfmtFakeLines(), []string{"stripe"}},
		{"json key", "method", `{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail, apiGatewayFakeLines(), []string{"GET", "POST", "PUT"}},
	}
	for _, mode := range detectedFieldsModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				vl := newFakeLogfmtVL(t, tc.lines)
				p := newDetectedFieldsTestProxy(t, vl.srv.URL, mode.opts...)
				params := url.Values{}
				params.Set("query", tc.query)
				params.Set("start", "2026-11-03T13:57:00.000Z")
				params.Set("end", "2026-11-03T14:57:00.000Z")
				rec := httptest.NewRecorder()
				p.handleDetectedFieldValues(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_field/"+tc.field+"/values?"+params.Encode(), nil))
				var resp struct {
					Values []string `json:"values"`
				}
				if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
					t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
				sort.Strings(resp.Values)
				if strings.Join(resp.Values, ",") != strings.Join(tc.want, ",") {
					t.Errorf("values\n got %v\nwant %v", resp.Values, tc.want)
				}
			})
		}
	}
}

// The field_names request carries no unpack that reads every key, while the
// scan still runs the query's own pipes.
// conformance: semantics/detected-fields-no-logfmt-token-fields
func TestDetectedFields_LevelPillFieldNamesWithoutUnrestrictedUnpack(t *testing.T) {
	vl := newFakeLogfmtVL(t, apiGatewayFakeLines())
	p := newDetectedFieldsTestProxy(t, vl.srv.URL)
	detectedFieldLabels(t, p, `{service_name="api-gateway"} | detected_level="error"`+drilldownParserTail, "2026-11-03T14:57:00.000Z")
	var sawNames, sawScan bool
	for _, c := range vl.calls() {
		path, query, _ := strings.Cut(c, " ")
		switch path {
		case "/select/logsql/field_names":
			sawNames = true
			if unrestrictedUnpackLogfmtRE.MatchString(query) {
				t.Errorf("field_names query keeps an unrestricted unpack_logfmt: %s", query)
			}
			// The pill compares the derived detected_level (the chain's __dl).
			if !strings.Contains(query, `| filter __dl:="error"`) {
				t.Errorf("field_names query lost the level filter: %s", query)
			}
		case "/select/logsql/query":
			sawScan = true
			if !strings.Contains(query, "unpack_logfmt") {
				t.Errorf("scan query must keep the query's pipes: %s", query)
			}
		}
	}
	if !sawNames || !sawScan {
		t.Fatalf("expected one field_names and one scan call, got %v", vl.calls())
	}
}

// conformance: semantics/detected-fields-no-logfmt-token-fields
func TestStripUnrestrictedLogfmtUnpacks_Table(t *testing.T) {
	cases := []struct{ in, want string }{
		{`a:=b | unpack_logfmt | filter level:="error"`, `a:=b | filter level:="error"`},
		{`a:=b | unpack_logfmt | filter level:="error" | unpack_logfmt`, `a:=b | filter level:="error"`},
		{`a:=b | unpack_logfmt`, `a:=b`},
		{`a:=b | unpack_logfmt keep_original_fields | filter x:=1`, `a:=b | filter x:=1`},
		{`a:=b | unpack_logfmt skip_empty_results`, `a:=b`},
		{`a:=b | unpack_logfmt from _msg result_prefix "p_" | filter x:=1`, `a:=b | filter x:=1`},
		{`a:=b | unpack_logfmt if (x:y) from _msg`, `a:=b`},
		// Restricted unpacks (the detected_level derivation) stay.
		{`a:=b | unpack_logfmt if (-_msg:~"^{") from _msg fields (level, lvl) result_prefix "__l_" | filter x:=1`, `a:=b | unpack_logfmt if (-_msg:~"^{") from _msg fields (level, lvl) result_prefix "__l_" | filter x:=1`},
		{`a:=b | unpack_logfmt fields(level)`, `a:=b | unpack_logfmt fields(level)`},
		// Text that only looks like the pipe is data.
		{`"| unpack_logfmt" | filter x:=1`, `"| unpack_logfmt" | filter x:=1`},
		{`a:=b | filter _msg:="x | unpack_logfmt"`, `a:=b | filter _msg:="x | unpack_logfmt"`},
		{`a:=b | unpack_logfmt result_prefix "fields (x)"`, `a:=b`},
		{`a:=b | unpack_json | filter x:=1`, `a:=b | unpack_json | filter x:=1`},
		{`a:=b | unpack_logfmtx`, `a:=b | unpack_logfmtx`},
		{`*`, `*`},
		{``, ``},
	}
	for _, tc := range cases {
		if got := stripUnrestrictedLogfmtUnpacks(tc.in); got != tc.want {
			t.Errorf("stripUnrestrictedLogfmtUnpacks(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}

func FuzzStripUnrestrictedLogfmtUnpacks(f *testing.F) {
	for _, seed := range []string{
		`a:=b | unpack_logfmt | filter level:="error" | unpack_logfmt`,
		`a:=b | unpack_logfmt if (-_msg:~"^{") from _msg fields (level) result_prefix "__l_"`,
		`"| unpack_logfmt" | unpack_logfmt keep_original_fields`,
		"a | unpack_logfmt `x` | unpack_logfmt fields(a)",
		`a | filter x:="\" | unpack_logfmt"`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, q string) {
		out := stripUnrestrictedLogfmtUnpacks(q)
		if again := stripUnrestrictedLogfmtUnpacks(out); again != out {
			t.Fatalf("not idempotent: %q -> %q -> %q", q, out, again)
		}
		if len(out) > len(q) {
			t.Fatalf("output grew: %q -> %q", q, out)
		}
		restricted := func(s string) int {
			n := 0
			for _, pipe := range topLevelPipes(s) {
				if strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(pipe, "|")), "unpack_logfmt") && !isUnrestrictedLogfmtUnpack(pipe) {
					n++
				}
			}
			return n
		}
		for _, pipe := range topLevelPipes(out) {
			if isUnrestrictedLogfmtUnpack(pipe) {
				t.Fatalf("unrestricted unpack left in %q (from %q)", out, q)
			}
		}
		if restricted(out) != restricted(q) {
			t.Fatalf("restricted unpack dropped: %q -> %q", q, out)
		}
		if out == q {
			return
		}
		// Only whole pipes are removed: what is left is the input's head
		// followed by a subsequence of its pipes.
		kept := topLevelPipes(q)
		for _, pipe := range topLevelPipes(out) {
			found := false
			for len(kept) > 0 {
				cand := kept[0]
				kept = kept[1:]
				if strings.TrimRight(cand, " ") == strings.TrimRight(pipe, " ") {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("pipe %q of %q not taken from %q", pipe, out, q)
			}
		}
	})
}

// topLevelPipes splits a LogsQL query into its `| ...` stages (the head
// before the first pipe is not returned).
func topLevelPipes(q string) []string {
	var out []string
	for {
		i := firstPipeOutsideQuotes(q)
		if i < 0 {
			return out
		}
		q = q[i:]
		j := firstPipeOutsideQuotes(q[1:])
		if j < 0 {
			return append(out, q)
		}
		out = append(out, q[:j+1])
		q = q[j+1:]
	}
}

func firstPipeOutsideQuotes(q string) int {
	var quote byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '|':
			return i
		}
	}
	return -1
}

// lokiDetectedFieldLabels is a Loki-semantics reference of detected_fields
// over the lines a query matched, for lines without structured metadata:
// parseDetectedFields + parseEntry (pkg/querier/queryrange/detected_fields.go
// :282-460) with the query's own `| json | logfmt` stages (entry.Parsed) and
// Loki's logfmt decoder rules (pkg/logql/log/logfmt/decode.go:43-140,
// parser.go:379-447). Keys clashing with a stream label are named
// <key>_extracted by Loki (parser.go:409-410); the proxy omits them by an
// owner decision, so the reference leaves them out. detected_level is
// structured metadata Loki adds at ingest.
func lokiDetectedFieldLabels(lines []fakeVLLine, queryHasParsers bool) map[string]struct{} {
	out := map[string]struct{}{"detected_level": {}}
	add := func(stream map[string]string, key string) {
		if key == "" {
			return
		}
		if _, clash := stream[key]; clash {
			return // Loki: key_extracted
		}
		out[key] = struct{}{}
	}
	for _, line := range lines {
		var obj map[string]interface{}
		isJSON := json.Unmarshal([]byte(line.msg), &obj) == nil
		if isJSON {
			for _, k := range lokiJSONLeafLabels("", obj) {
				add(line.stream, k)
			}
			// The query's logfmt stage gives nothing for these lines (no '=').
			continue
		}
		for k := range lokiLogfmtKeys(line.msg) {
			add(line.stream, k)
		}
		_ = queryHasParsers
	}
	return out
}

func lokiJSONLeafLabels(prefix string, obj map[string]interface{}) []string {
	var out []string
	for k, v := range obj {
		name := k
		if prefix != "" {
			name = prefix + "_" + k
		}
		switch x := v.(type) {
		case map[string]interface{}:
			out = append(out, lokiJSONLeafLabels(name, x)...)
		case []interface{}:
			// Loki's json parser skips arrays.
		case nil:
		case string:
			if x != "" {
				out = append(out, name)
			}
		default:
			out = append(out, name)
		}
	}
	return out
}

// lokiLogfmtKeys is Loki's non-strict logfmt parser for keys made of
// [a-z0-9_]: a token without '=' or with an empty value gives nothing, a key
// or unquoted value holding a quote is a syntax error that skips the token.
func lokiLogfmtKeys(line string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, tok := range splitLokiLogfmtTokens(line) {
		eq := strings.IndexByte(tok, '=')
		if eq <= 0 {
			continue
		}
		key, val := tok[:eq], tok[eq+1:]
		if strings.ContainsAny(key, "\"=") {
			continue
		}
		if strings.HasPrefix(val, `"`) {
			uq, err := strconv.Unquote(val)
			if err != nil || uq == "" {
				continue
			}
		} else if val == "" || strings.ContainsAny(val, "\"=") {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func splitLokiLogfmtTokens(line string) []string {
	var out []string
	start, inQuote := 0, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inQuote {
			switch c {
			case '\\':
				i++
			case '"':
				inQuote = false
			}
			continue
		}
		if c == '"' && i > 0 && line[i-1] == '=' {
			inQuote = true
			continue
		}
		if c <= ' ' {
			if i > start {
				out = append(out, line[start:i])
			}
			start = i + 1
		}
	}
	if start < len(line) {
		out = append(out, line[start:])
	}
	return out
}

func randomDetectedFieldsLines(rng *rand.Rand) []fakeVLLine {
	stream := map[string]string{"service_name": "svc", "level": "error", "env": "prod"}
	keyPool := []string{"method", "path", "status", "user_id", "trace_id", "region", "msg", "latency_ms", "tx_id", "attempt", "code", "op", "level", "env"}
	words := []string{"GET", "error", "timeout", "500,", "[1001]", "ERROR:", "us-east-1", "{retry}", `"quoted"`, "a/b", "x:y", "--", "42", "deadlock", "", "ok"}
	word := func() string { return words[rng.Intn(len(words))] }
	n := 30 + rng.Intn(50)
	lines := make([]fakeVLLine, 0, n)
	for i := 0; i < n; i++ {
		switch rng.Intn(3) {
		case 0: // JSON, spaced or compact, no '=' in values
			keys := []string{}
			vals := map[string]interface{}{}
			for j := 0; j < 1+rng.Intn(6); j++ {
				k := keyPool[rng.Intn(len(keyPool))]
				if _, dup := vals[k]; dup {
					continue
				}
				keys = append(keys, k)
				switch rng.Intn(4) {
				case 0:
					vals[k] = rng.Intn(1000)
				case 1:
					vals[k] = map[string]interface{}{"name": word() + "x"}
				default:
					vals[k] = strings.TrimSpace(word() + " " + word() + "x")
				}
			}
			msg := pythonJSONLine(keys, vals)
			if rng.Intn(2) == 0 {
				var obj map[string]interface{}
				_ = json.Unmarshal([]byte(msg), &obj)
				b, _ := json.Marshal(obj)
				msg = string(b)
			}
			lines = append(lines, newFakeLine(stream, nil, msg))
		case 1: // logfmt with stray words
			parts := []string{}
			for j := 0; j < 1+rng.Intn(5); j++ {
				k := keyPool[rng.Intn(len(keyPool))]
				switch rng.Intn(4) {
				case 0:
					// A quoted value without inner quotes: the proxy's scan parser
					// and Loki's decoder differ on malformed quoting, which is
					// not what this test pins.
					parts = append(parts, k+`="`+strings.ReplaceAll(word()+" "+word(), `"`, "")+`x"`)
				case 1:
					parts = append(parts, word()+"x")
				default:
					parts = append(parts, k+"="+strconv.Itoa(rng.Intn(100)))
				}
			}
			lines = append(lines, newFakeLine(stream, nil, nonJSONLine(parts)))
		default: // plain text
			parts := []string{}
			for j := 0; j < 2+rng.Intn(6); j++ {
				parts = append(parts, word()+"x")
			}
			lines = append(lines, newFakeLine(stream, nil, nonJSONLine(parts)))
		}
	}
	return lines
}

// nonJSONLine joins tokens into a line that does not start with '{': the
// proxy's scan reads a line starting with '{' as JSON, a separate known
// difference this test does not pin.
func nonJSONLine(parts []string) string {
	line := strings.Join(parts, " ")
	if strings.HasPrefix(line, "{") {
		line = "x " + line
	}
	return line
}

// conformance: semantics/detected-fields-no-logfmt-token-fields
func TestDetectedFields_DifferentialAgainstLokiReference(t *testing.T) {
	queries := []string{
		`{service_name="svc"} | detected_level="error"` + drilldownParserTail,
		`{service_name="svc"}` + drilldownParserTail,
		`{service_name="svc"} | detected_level=~"error|warn"` + drilldownParserTail,
		`{service_name="svc"}` + drilldownParserTail + `| status!="200"`,
	}
	rng := rand.New(rand.NewSource(20261103))
	for i := 0; i < 120; i++ {
		lines := randomDetectedFieldsLines(rng)
		query := queries[i%len(queries)]
		vl := newFakeLogfmtVL(t, lines)
		p := newDetectedFieldsTestProxy(t, vl.srv.URL, withLokiProfileNames())
		got := detectedFieldLabels(t, p, query, "2026-11-03T14:57:00.000Z")
		want := lokiDetectedFieldLabels(lines, true)
		var extra, missing []string
		for k := range got {
			if _, ok := want[k]; !ok {
				extra = append(extra, k)
			}
		}
		for k := range want {
			if _, ok := got[k]; !ok {
				missing = append(missing, k)
			}
		}
		sort.Strings(extra)
		sort.Strings(missing)
		if len(extra) > 0 || len(missing) > 0 {
			var msgs []string
			for _, l := range lines {
				msgs = append(msgs, l.msg)
			}
			t.Fatalf("case %d %s\nextra (not Loki) %v\nmissing (Loki lists) %v\nlines:\n%s", i, query, extra, missing, strings.Join(msgs, "\n"))
		}
	}
}

// BenchmarkDetectedFieldsLevelPillFakeVL measures one uncached detected_fields
// request of the Drilldown Fields tab with a level pill against the recording
// fake: VictoriaLogs calls and bytes per request, and the fields answered.
func BenchmarkDetectedFieldsLevelPillFakeVL(b *testing.B) {
	for _, tc := range []struct {
		name  string
		lines []fakeVLLine
		query string
	}{
		{"json", apiGatewayFakeLines(), `{service_name="api-gateway"} | detected_level="error"` + drilldownParserTail},
		{"otel", otelCollectorFakeLines(), `{service_name="otel-collector"} | detected_level="warn"` + drilldownParserTail},
		{"plain", dbPostgresPlainFakeLines(), `{service_name="db-postgres"} | detected_level="error"` + drilldownParserTail},
	} {
		b.Run(tc.name, func(b *testing.B) {
			vl := newFakeLogfmtVL(b, tc.lines)
			p := newDetectedFieldsTestProxy(b, vl.srv.URL, withLokiProfileNames())
			vl.reset()
			fields := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A window of its own per request: every request misses the caches.
				end := time.Date(2026, 11, 3, 14, 57, 0, 0, time.UTC).Add(-time.Duration(i) * time.Hour)
				fields = len(detectedFieldLabelsWindow(b, p, tc.query, end.Add(-time.Hour).Format(time.RFC3339Nano), end.Format(time.RFC3339Nano)))
			}
			b.StopTimer()
			vl.mu.Lock()
			b.ReportMetric(float64(len(vl.log))/float64(b.N), "vlcalls/op")
			b.ReportMetric(float64(vl.bytes)/float64(b.N), "vlbytes/op")
			vl.mu.Unlock()
			b.ReportMetric(float64(fields), "fields")
		})
	}
}
