//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// logLineBodyRoute is how a line reaches VictoriaLogs. Loki always receives
// the line unchanged through its push API.
type logLineBodyRoute string

const (
	// routeGenerator mirrors test/e2e-compat/log-generator.py: JSON lines are
	// pushed through VictoriaLogs' Loki endpoint with _msg set to the original
	// line, so VictoriaLogs keeps the line in _msg and also stores every JSON
	// key as a field. Other lines are pushed unchanged.
	routeGenerator logLineBodyRoute = "generator"
	// routeLokiPush pushes the line unchanged through VictoriaLogs' Loki
	// endpoint, as Promtail or Alloy do. A JSON line without _msg is unpacked
	// into fields and _msg holds VictoriaLogs' -defaultMsgValue placeholder, as
	// does an empty line.
	routeLokiPush logLineBodyRoute = "loki-push"
	// routeJSONLine sends the JSON line's keys as an /insert/jsonline row
	// without _msg.
	routeJSONLine logLineBodyRoute = "jsonline"
)

type logLineBodyEntry struct {
	route    logLineBodyRoute
	line     string
	metadata map[string]string
}

// rebuilt reports whether VictoriaLogs stores no line for the entry, so the
// proxy rebuilds it from the row's fields.
func (e logLineBodyEntry) rebuilt() bool {
	return e.route != routeGenerator
}

// logLineBodyVLLine mirrors _inject_vl_msg in log-generator.py for a line
// already written with Python's json.dumps separators.
func logLineBodyVLLine(t *testing.T, line string) string {
	t.Helper()
	if !strings.HasPrefix(line, "{") {
		return line
	}
	var probe map[string]any
	if json.Unmarshal([]byte(line), &probe) != nil {
		return line
	}
	quoted, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(line, "}") + `, "_msg": ` + string(quoted) + "}"
}

func logLineBodyPush(t *testing.T, target string, labels map[string]string, stamps []time.Time, entries []logLineBodyEntry, forVL bool) {
	t.Helper()
	values := make([][]any, 0, len(entries))
	for i, entry := range entries {
		line := entry.line
		if forVL && entry.route == routeGenerator {
			line = logLineBodyVLLine(t, line)
		}
		value := []any{strconv.FormatInt(stamps[i].UnixNano(), 10), line}
		if entry.metadata != nil {
			value = append(value, entry.metadata)
		}
		values = append(values, value)
	}
	payload, err := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": values}}})
	if err != nil {
		t.Fatal(err)
	}
	status, body := hardeningRequest(t, http.MethodPost, target, string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent && status != http.StatusOK {
		t.Fatalf("push to %s: %d %s", target, status, body)
	}
}

// logLineBodyPushJSONLine writes each JSON line's keys, the stream labels and
// _time as one /insert/jsonline row without _msg.
func logLineBodyPushJSONLine(t *testing.T, labels map[string]string, stamps []time.Time, entries []logLineBodyEntry) {
	t.Helper()
	streamFields := make([]string, 0, len(labels))
	var rows strings.Builder
	for i, entry := range entries {
		row := map[string]any{}
		if err := json.Unmarshal([]byte(entry.line), &row); err != nil {
			t.Fatalf("jsonline entry %q: %v", entry.line, err)
		}
		for name, value := range labels {
			row[name] = value
		}
		row["_time"] = stamps[i].UTC().Format(time.RFC3339Nano)
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		rows.Write(encoded)
		rows.WriteByte('\n')
	}
	for name := range labels {
		streamFields = append(streamFields, name)
	}
	sort.Strings(streamFields)
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(streamFields, ",")), rows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("jsonline push: %d %s", status, body)
	}
}

// logLineBodyLines returns the lines of a Loki streams response keyed by
// timestamp, failing on errors, warnings or partial responses.
func logLineBodyLines(t *testing.T, base string, params url.Values, headers map[string]string) map[string]string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s: %v", base, err)
	}
	defer resp.Body.Close()
	var body struct {
		Status   string   `json:"status"`
		Warnings []string `json:"warnings"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&body) != nil {
		t.Fatalf("%s: status %d", base, resp.StatusCode)
	}
	if body.Status != "success" || body.Data.ResultType != "streams" || len(body.Warnings) != 0 || resp.Header.Get("X-Loki-VL-Partial-Response") != "" {
		t.Fatalf("%s: unhealthy response status=%q type=%q warnings=%v partial=%q", base, body.Status, body.Data.ResultType, body.Warnings, resp.Header.Get("X-Loki-VL-Partial-Response"))
	}
	lines := map[string]string{}
	for _, stream := range body.Data.Result {
		for _, value := range stream.Values {
			var ts, line string
			if len(value) < 2 || json.Unmarshal(value[0], &ts) != nil || json.Unmarshal(value[1], &line) != nil {
				t.Fatalf("%s: malformed value", base)
			}
			lines[ts] = line
		}
	}
	return lines
}

func logLineBodyTail(t *testing.T, base string, params url.Values) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial("ws"+strings.TrimPrefix(base, "http")+"/loki/api/v1/tail?"+params.Encode(), http.Header{"X-Scope-OrgID": {"0"}})
	if err != nil {
		t.Fatalf("%s tail dial: %v (resp=%v)", base, err, resp)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func logLineBodyReadTail(t *testing.T, name string, conn *websocket.Conn, want int) map[string]string {
	t.Helper()
	lines := map[string]string{}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	for len(lines) < want {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("%s tail read after %d/%d lines: %v", name, len(lines), want, err)
		}
		var frame struct {
			Streams []struct {
				Values [][]string `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			t.Fatalf("%s tail frame: %v", name, err)
		}
		for _, stream := range frame.Streams {
			for _, value := range stream.Values {
				if len(value) >= 2 {
					lines[value[0]] = value[1]
				}
			}
		}
	}
	return lines
}

func logLineBodyInProcessProxy(t *testing.T, style proxy.LabelStyle, mode proxy.MetadataFieldMode) string {
	t.Helper()
	p, err := proxy.New(proxy.Config{
		BackendURL:                 vlURL,
		Cache:                      cache.NewDisabled(),
		LogLevel:                   "error",
		LabelStyle:                 style,
		MetadataFieldMode:          mode,
		EmitStructuredMetadata:     true,
		QueryRangeWindowingEnabled: true,
		QueryRangeSplitInterval:    time.Hour,
		QueryRangeMaxParallel:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

// logLineBodyFlatten flattens a JSON log line the way VictoriaLogs unpacks it
// at ingestion (lib/logstorage/json_parser.go): nested object keys are joined
// with dots, numbers, booleans and arrays keep their JSON text, nulls and empty
// strings are dropped.
func logLineBodyFlatten(line string) (map[string]string, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	flat := map[string]string{}
	var walk func(prefix string, value any) error
	walk = func(prefix string, value any) error {
		switch v := value.(type) {
		case nil:
		case map[string]any:
			for key, nested := range v {
				if err := walk(prefix+key+".", nested); err != nil {
					return err
				}
			}
		case string:
			if v != "" {
				flat[strings.TrimSuffix(prefix, ".")] = v
			}
		case json.Number:
			flat[strings.TrimSuffix(prefix, ".")] = v.String()
		case bool:
			flat[strings.TrimSuffix(prefix, ".")] = strconv.FormatBool(v)
		default:
			encoded, err := json.Marshal(v)
			if err != nil {
				return err
			}
			flat[strings.TrimSuffix(prefix, ".")] = string(encoded)
		}
		return nil
	}
	return flat, walk("", object)
}

// logLineBodyDiff compares proxy lines with Loki's. Lines VictoriaLogs stored
// verbatim must be byte-identical. Lines it stored without a message are
// rebuilt by the proxy from typeless, flattened fields, so they cannot equal
// Loki's bytes: they must be a JSON object whose string key/value set equals
// Loki's line after flattening, and an empty Loki line must stay empty.
func logLineBodyDiff(want, got map[string]string, rebuilt map[string]bool) string {
	keys := make([]string, 0, len(want)+len(got))
	for ts := range want {
		keys = append(keys, ts)
	}
	for ts := range got {
		if _, ok := want[ts]; !ok {
			keys = append(keys, ts)
		}
	}
	sort.Strings(keys)
	var diff strings.Builder
	for _, ts := range keys {
		wantLine, wantOK := want[ts]
		gotLine, gotOK := got[ts]
		if wantOK != gotOK {
			fmt.Fprintf(&diff, "\n  ts=%s present in loki=%t proxy=%t\n    loki:  %q\n    proxy: %q", ts, wantOK, gotOK, wantLine, gotLine)
			continue
		}
		if !rebuilt[ts] || wantLine == "" {
			if wantLine != gotLine {
				fmt.Fprintf(&diff, "\n  ts=%s\n    loki:  %q\n    proxy: %q", ts, wantLine, gotLine)
			}
			continue
		}
		wantFields, err := logLineBodyFlatten(wantLine)
		if err != nil {
			fmt.Fprintf(&diff, "\n  ts=%s loki line is not a JSON object: %v", ts, err)
			continue
		}
		var gotFields map[string]string
		if err := json.Unmarshal([]byte(gotLine), &gotFields); err != nil || !reflect.DeepEqual(wantFields, gotFields) {
			fmt.Fprintf(&diff, "\n  ts=%s rebuilt line differs (err=%v)\n    loki flattened: %v\n    proxy:          %q", ts, err, wantFields, gotLine)
		}
	}
	return diff.String()
}

// TestCompat_LogLineBodyMatchesLoki pushes the log shapes the UI log generator
// writes (JSON lines with extracted fields, lines with structured metadata,
// logfmt) and the shapes real shippers send without _msg (Loki push of JSON
// lines, an empty line, /insert/jsonline rows) to Loki and to VictoriaLogs.
// Every proxy must return Loki's lines on single-request and windowed
// query_range, with and without categorize-labels, and on tail; see
// logLineBodyDiff for the rows VictoriaLogs stores without a line.
func TestCompat_LogLineBodyMatchesLoki(t *testing.T) {
	app := fmt.Sprintf("log-line-body-%d", time.Now().UnixNano())
	entries := []logLineBodyEntry{
		{route: routeGenerator, line: `{"service": {"name": "` + app + `"}, "method": "POST", "path": "/api/v1/cart", "status": 200, "duration_ms": 70, "trace_id": "b09df04f98497226c5a6a00bf1798b7b", "level": "info", "version": "v1"}`},
		{route: routeGenerator, line: `{"service": {"name": "` + app + `"}, "method": "GET", "path": "/api/v1/users", "status": 401, "duration_ms": 233, "trace_id": "98e1d9db09af2c2af0aed875926240ca", "level": "warn", "version": "v1"}`},
		{route: routeGenerator, line: `{"message": "db_query completed", "level": "info", "duration_ms": 12, "operation": "db_query"}`, metadata: map[string]string{"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "service.name": app, "k8s.pod.name": app + "-0"}},
		{route: routeGenerator, line: "payment failed: gateway timeout", metadata: map[string]string{"trace_id": "a1b2c3", "span_id": "d4e5f6"}},
		{route: routeGenerator, line: `level=warn msg="job retry" attempt=3 queue=billing`},
		{route: routeLokiPush, line: `{"method":"GET","status":200,"msg":"hello json","ok":true,"none":null,"empty":"","tags":["a","b"],"http":{"route":"/a<b","latency_ms":1.5}}`},
		{route: routeLokiPush, line: `{"level":"error","error":"gateway \"timeout\"\nretrying","attempt":3}`},
		{route: routeLokiPush, line: ""},
		{route: routeJSONLine, line: `{"event":"login","user":{"id":"u1","roles":["admin"]},"success":false}`},
	}
	baseLabels := map[string]string{"app": app, "service_name": app, "namespace": "prod", "cluster": "us-east-1"}
	selector := fmt.Sprintf(`{app=%q}`, app)

	type tailTarget struct {
		name string
		conn *websocket.Conn
	}
	// Loki closes a tail whose start is ahead of its own clock ("past now -
	// max_query_lookback"), so start a few seconds back to absorb clock skew
	// between the test host and the Loki container. The app is unique, so the
	// replayed history holds no lines.
	tailStart := time.Now().Add(-5 * time.Second)
	tailParams := url.Values{"query": {selector}, "start": {strconv.FormatInt(tailStart.UnixNano(), 10)}, "limit": {"100"}}
	tails := []tailTarget{
		{name: "loki", conn: logLineBodyTail(t, lokiURL, tailParams)},
		{name: "proxy", conn: logLineBodyTail(t, proxyURL, tailParams)},
	}
	// Real Loki can miss the first frame when a push lands before the tail
	// subscription is registered.
	time.Sleep(750 * time.Millisecond)

	base := time.Now().Truncate(time.Millisecond)
	rebuilt := map[string]bool{}
	byRoute := map[logLineBodyRoute][]logLineBodyEntry{}
	stampsByRoute := map[logLineBodyRoute][]time.Time{}
	for i, entry := range entries {
		stamp := base.Add(time.Duration(i) * time.Millisecond)
		rebuilt[strconv.FormatInt(stamp.UnixNano(), 10)] = entry.rebuilt()
		byRoute[entry.route] = append(byRoute[entry.route], entry)
		stampsByRoute[entry.route] = append(stampsByRoute[entry.route], stamp)
	}
	for _, route := range []logLineBodyRoute{routeGenerator, routeLokiPush, routeJSONLine} {
		labels := map[string]string{"route": string(route)}
		for name, value := range baseLabels {
			labels[name] = value
		}
		logLineBodyPush(t, lokiURL+"/loki/api/v1/push", labels, stampsByRoute[route], byRoute[route], false)
		if route == routeJSONLine {
			logLineBodyPushJSONLine(t, labels, stampsByRoute[route], byRoute[route])
			continue
		}
		logLineBodyPush(t, vlURL+"/insert/loki/api/v1/push", labels, stampsByRoute[route], byRoute[route], true)
	}
	hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)

	// Whole seconds: the fixture wait sends second-precision bounds.
	fixtureEnd := base.Truncate(time.Second).Add(2 * time.Second)
	waitForFixtureOnBothBackends(t, fmt.Sprintf("app:=%q", app), selector, fixtureEnd.Add(-time.Minute), fixtureEnd, len(entries))

	// VictoriaLogs must hold each generator line in _msg and the placeholder
	// for every other row; otherwise the comparison below proves nothing.
	vlParams := url.Values{"query": {fmt.Sprintf("app:=%q | fields _time, _msg", app)}, "start": {strconv.FormatInt(fixtureEnd.Add(-time.Minute).Unix(), 10)}, "end": {strconv.FormatInt(fixtureEnd.Add(time.Minute).Unix(), 10)}}
	status, vlBody := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+vlParams.Encode(), "", nil)
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs query: %d %s", status, vlBody)
	}
	stored := map[string]int{}
	for _, row := range strings.Split(strings.TrimSpace(string(vlBody)), "\n") {
		var fields map[string]string
		if json.Unmarshal([]byte(row), &fields) != nil {
			t.Fatalf("VictoriaLogs row: %s", row)
		}
		stored[fields["_msg"]]++
	}
	placeholders := 0
	for _, entry := range entries {
		if entry.rebuilt() {
			placeholders++
			continue
		}
		if stored[entry.line] == 0 {
			t.Fatalf("VictoriaLogs _msg does not hold pushed line %q: %s", entry.line, vlBody)
		}
	}
	if got := stored["missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"]; got != placeholders {
		t.Fatalf("VictoriaLogs stored %d placeholder rows, want %d: %s", got, placeholders, vlBody)
	}

	targets := []struct{ name, url string }{
		{"proxy", proxyURL},
		{"loki-profile", proxyUnderscoreURL},
		{"otel-hybrid", proxyOTelHybridURL},
		{"native-metadata", proxyNativeMetadataURL},
		{"translated-metadata", proxyTranslatedMetadataURL},
		{"no-metadata", proxyNoStructuredMetadataURL},
	}
	for _, style := range []proxy.LabelStyle{proxy.LabelStylePassthrough, proxy.LabelStyleUnderscores} {
		for _, mode := range []proxy.MetadataFieldMode{proxy.MetadataFieldModeNative, proxy.MetadataFieldModeTranslated, proxy.MetadataFieldModeHybrid} {
			targets = append(targets, struct{ name, url string }{fmt.Sprintf("in-process-%s-%s", style, mode), logLineBodyInProcessProxy(t, style, mode)})
		}
	}

	ranges := []struct {
		name       string
		start, end time.Time
	}{
		{"single", fixtureEnd.Add(-time.Minute), fixtureEnd.Add(time.Minute)},
		// Longer than the one-hour split interval, so windowing proxies split it.
		{"windowed", fixtureEnd.Add(-2 * time.Hour), fixtureEnd.Add(time.Minute)},
	}
	// Line filters run on VictoriaLogs' _msg, which holds the placeholder for
	// rebuilt rows, so only a filter that keeps every row is comparable here.
	// label_format writes a field in VictoriaLogs that must not reach a
	// rebuilt line.
	queries := []string{selector, selector + ` | drop __error__`, selector + ` != "no-such-token"`, selector + ` | label_format line_body_extra="x"`}
	encodings := map[string]map[string]string{
		"plain":      nil,
		"categorize": {"X-Loki-Response-Encoding-Flags": "categorize-labels"},
	}
	for _, rng := range ranges {
		for _, query := range queries {
			params := url.Values{
				"query": {query},
				"start": {strconv.FormatInt(rng.start.UnixNano(), 10)},
				"end":   {strconv.FormatInt(rng.end.UnixNano(), 10)},
				"limit": {"100"},
			}
			for encName, headers := range encodings {
				loki := logLineBodyLines(t, lokiURL, params, headers)
				if len(loki) != len(entries) {
					t.Fatalf("%s %s %s: Loki returned %d lines, want %d", rng.name, query, encName, len(loki), len(entries))
				}
				for _, target := range targets {
					t.Run(fmt.Sprintf("query_range/%s/%s/%s/%s", rng.name, encName, target.name, query), func(t *testing.T) {
						got := logLineBodyLines(t, target.url, params, headers)
						if diff := logLineBodyDiff(loki, got, rebuilt); diff != "" {
							t.Fatalf("line bodies differ from Loki:%s", diff)
						}
					})
				}
			}
		}
	}

	t.Run("tail", func(t *testing.T) {
		lines := map[string]map[string]string{}
		for _, tail := range tails {
			lines[tail.name] = logLineBodyReadTail(t, tail.name, tail.conn, len(entries))
		}
		if diff := logLineBodyDiff(lines["loki"], lines["proxy"], rebuilt); diff != "" {
			t.Fatalf("tail line bodies differ from Loki:%s", diff)
		}
	})
}
