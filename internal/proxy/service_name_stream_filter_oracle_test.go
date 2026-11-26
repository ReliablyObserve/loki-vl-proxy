package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// snOracleEnd is the end of the window the service_name oracle's rows are in.
const snOracleEnd = 1795132800 // 2026-11-20T00:00:00Z

// snStream is a stream of the service_name oracle: its stream labels and the regular fields of its rows.
type snStream struct {
	stream, fields [][2]string
}

// snOracleStreams are tenant 0's streams: every way VictoriaLogs holds the derivation's sources for which the
// statement of -service-name-stream-label-only is true (OpenTelemetry service.name as a stream field, the Loki push
// route's service_name label, both on one stream, service.name as a regular field of a stream without that label,
// later sources, no source, values that need quoting), and random streams over the same space.
func snOracleStreams() []snStream {
	tag := func(kv ...string) [][2]string {
		var out [][2]string
		for i := 0; i+1 < len(kv); i += 2 {
			out = append(out, [2]string{kv[i], kv[i+1]})
		}
		return out
	}
	out := []snStream{
		{stream: tag("service.name", "svc-a", "k8s.pod.name", "a-0")},
		{stream: tag("service.name", "svc-a", "k8s.pod.name", "a-1")},
		{stream: tag("service.name", "svc-b", "k8s.pod.name", "b-0")},
		{stream: tag("service_name", "svc-a", "job", "push-1")},
		{stream: tag("service_name", "svc-c", "job", "push-2")},
		{stream: tag("service_name", "svc-a", "service.name", "svc-b")},
		{stream: tag("k8s.pod.name", "raw-0"), fields: tag("service.name", "svc-a")},
		{stream: tag("k8s.pod.name", "raw-1"), fields: tag("service.name", "svc-b")},
		{stream: tag("app", "svc-a")},
		{stream: tag("app", "svc-d", "k8s.container.name", "svc-a")},
		{stream: tag("host", "h-1")},
		{stream: tag("service.name", `we"ird\x`, "k8s.pod.name", "w-0")},
		{stream: tag("service.name", "ünï code")},
		{stream: tag("service.name", "line\nbreak")},
		{stream: tag("service.name", unknownServiceName)},
	}
	r := rand.New(rand.NewSource(1763))
	values := []string{"svc-a", "svc-b", "svc-e"}
	for i := 0; i < 40; i++ {
		s := snStream{stream: tag("k8s.pod.name", fmt.Sprintf("rnd-%d", i))}
		if r.Intn(3) == 0 {
			s.stream = append(s.stream, [2]string{"service_name", values[r.Intn(3)]})
		}
		switch r.Intn(3) {
		case 1:
			s.stream = append(s.stream, [2]string{"service.name", values[r.Intn(3)]})
		case 2:
			s.fields = append(s.fields, [2]string{"service.name", values[r.Intn(3)]})
		}
		switch r.Intn(3) {
		case 1:
			s.stream = append(s.stream, [2]string{"app", values[r.Intn(3)]})
		case 2:
			s.fields = append(s.fields, [2]string{"app", values[r.Intn(3)]})
		}
		out = append(out, s)
	}
	return out
}

// snViolatingStreams are tenant 1's streams, for which the statement is false: a regular service_name field in a
// stream of another service, and a regular service.name beside a service.name stream label of another value (an
// OpenTelemetry log attribute beside the resource attribute), which VictoriaLogs reads as the row's service.name.
func snViolatingStreams() []snStream {
	return []snStream{
		{stream: [][2]string{{"service.name", "svc-b"}, {"k8s.pod.name", "v-0"}}, fields: [][2]string{{"service_name", "svc-a"}}},
		{stream: [][2]string{{"service.name", "svc-x"}, {"k8s.pod.name", "v-1"}}, fields: [][2]string{{"service.name", "svc-a"}}},
		{stream: [][2]string{{"service.name", "svc-a"}, {"k8s.pod.name", "v-2"}}},
	}
}

func snServeRows(streams []snStream) []oraclerun.ServeRow {
	var rows []oraclerun.ServeRow
	for si, s := range streams {
		for i := 0; i < 6; i++ {
			level := []string{"info", "error", "warn"}[(si+i)%3]
			line := fmt.Sprintf(`{"msg":"m%d","status":%d}`, i, 200+i)
			if i%2 == 1 {
				line = fmt.Sprintf(`msg=m%d status=%d`, i, 200+i)
			}
			fields := append(append([][2]string(nil), s.fields...), [2]string{"level", level}, [2]string{"org.component", fmt.Sprintf("c%d", i%2)}, [2]string{"_msg", line})
			ts := (int64(snOracleEnd)-int64(60+si*7+i*300))*int64(time.Second) + int64(si)
			rows = append(rows, oraclerun.ServeRow{TS: ts, Stream: s.stream, Fields: fields})
		}
	}
	return rows
}

// snCanonical is a Loki answer in one spelling: objects with sorted keys (the order of a stream's labels is not
// part of the answer) and the results, streams or series, in sorted order.
func snCanonical(body []byte) string {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return string(body)
	}
	if m, ok := v.(map[string]any); ok {
		for _, list := range []any{m["data"], m["fields"]} {
			var items []any
			switch d := list.(type) {
			case map[string]any:
				items, _ = d["result"].([]any)
			case []any:
				items = d
			}
			sort.Slice(items, func(i, j int) bool {
				a, _ := json.Marshal(items[i])
				b, _ := json.Marshal(items[j])
				return string(a) < string(b)
			})
		}
	}
	out, _ := json.Marshal(v)
	return string(out)
}

// snFront forwards the proxy's requests to the VictoriaLogs server with the tenant headers the proxy set, answers
// the version probe as release version, and records the query of every data read.
type snFront struct {
	version, upstream string
	mu                sync.Mutex
	reads             []string
}

func (f *snFront) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		_, _ = io.WriteString(w, "OK")
		return
	case "/metrics":
		_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20260101-000000-tags-`+f.version+`-0-g0000000", short_version="`+f.version+`"} 1`+"\n")
		return
	}
	body, _ := io.ReadAll(r.Body)
	if form, err := url.ParseQuery(string(body)); err == nil && form.Get("query") != "" {
		f.mu.Lock()
		f.reads = append(f.reads, form.Get("query"))
		f.mu.Unlock()
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, f.upstream+r.URL.RequestURI(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for _, h := range []string{"Content-Type", "AccountID", "ProjectID"} {
		req.Header.Set(h, r.Header.Get(h))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (f *snFront) streamFilterReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, q := range f.reads {
		if strings.HasPrefix(q, "{service_name=") && strings.Contains(q, " or service_name=") {
			n++
		}
	}
	return n
}

// snOracleRequests are the requests the oracle compares for a service_name value: the Drilldown and Explore shapes
// of issue #1763 (log query, a parser chain, grouped and ungrouped counts, a field matcher beside the selector, an
// instant query) and the Drilldown routes that read the selector (detected fields, volume).
func snOracleRequests(value string) []string {
	sel := "service_name=" + strconv.Quote(value)
	window := url.Values{"start": {strconv.FormatInt((snOracleEnd-3600)*1e9, 10)}, "end": {strconv.FormatInt(snOracleEnd*1e9, 10)}}
	rng := func(q string, extra ...string) string {
		v := url.Values{"query": {q}, "start": window["start"], "end": window["end"], "limit": {"1000"}}
		for i := 0; i+1 < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		return "/loki/api/v1/query_range?" + v.Encode()
	}
	return []string{
		rng("{" + sel + "}"),
		rng("{" + sel + "}" + ` | json | logfmt | drop __error__, __error_details__`),
		rng("sum by (level) (count_over_time({"+sel+"}[1m]))", "step", "60"),
		rng("sum by (detected_level) (count_over_time({"+sel+"} | drop __error__ [1m]))", "step", "60"),
		rng("sum(count_over_time({"+sel+`, level="info"}[5m]))`, "step", "300"),
		rng("sum by (org_component) (count_over_time({"+sel+"} | logfmt | drop __error__ [1m]))", "step", "60"),
		"/loki/api/v1/query?" + url.Values{"query": {"sum(count_over_time({" + sel + "}[1h]))"}, "time": window["end"]}.Encode(),
		"/loki/api/v1/detected_fields?" + url.Values{"query": {"{" + sel + "}"}, "start": window["start"], "end": window["end"]}.Encode(),
		"/loki/api/v1/index/volume?" + url.Values{"query": {"{" + sel + "}"}, "start": window["start"], "end": window["end"]}.Encode(),
	}
}

func snOracleProxy(t *testing.T, backend string, on bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL: backend, Cache: cache.New(60*time.Second, 1000), LogLevel: "error",
		BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second,
		ServiceNameStreamLabelOnly: on,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// TestServiceNameStreamFilterAnswersOnVictoriaLogs runs the proxy over VictoriaLogs v1.49.0 and v1.54.0 (their own
// storage and query handlers, test/oracle/vl serve) with and without -service-name-stream-label-only, and holds
// every answer with the flag to the answer without it, byte for byte: on one tenant, on a multi-tenant query, for
// every value a stream can derive (each source, a value no row has, unknown_service, values that need quoting),
// for log, parser, grouped and instant metric queries and the Drilldown routes. The reads with the flag carry the
// stream filter. On a tenant whose rows break the statement the answers differ, which is the boundary the flag
// documents.
//
// conformance: semantics/service-name-stream-filter, vl-stream-filter-or-groups-and-missing-label
func TestServiceNameStreamFilterAnswersOnVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	t.Cleanup(querycorpus.PauseRecording())
	streams := snOracleStreams()
	sets := []oraclerun.ServeDataSet{
		{Tenant: 0, Rows: snServeRows(streams)},
		{Tenant: 1, Rows: snServeRows(snViolatingStreams())},
		{Tenant: 2, Rows: snServeRows(streams[:9])},
	}
	values := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "none", unknownServiceName, `we"ird\x`, "ünï code", "line\nbreak"}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, sets)
		if err != nil {
			t.Fatal(err)
		}
		offFront, onFront := &snFront{version: v.version, upstream: srv.URL}, &snFront{version: v.version, upstream: srv.URL}
		offSrv, onSrv := httptest.NewServer(offFront), httptest.NewServer(onFront)
		off, on := snOracleProxy(t, offSrv.URL, false), snOracleProxy(t, onSrv.URL, true)
		same, nonEmpty, total := 0, 0, 0
		for _, tenant := range []string{"0", "0|2"} {
			for _, value := range values {
				for _, target := range snOracleRequests(value) {
					total++
					headers := map[string]string{"X-Scope-OrgID": tenant}
					a, b := doCompatProxyRequest(off, target, headers), doCompatProxyRequest(on, target, headers)
					if a.Code != b.Code || snCanonical(a.Body.Bytes()) != snCanonical(b.Body.Bytes()) {
						t.Errorf("%s tenant %s %s:\n flag on  %d %.400s\n flag off %d %.400s", v.version, tenant, target, b.Code, b.Body.String(), a.Code, a.Body.String())
						continue
					}
					if a.Code != http.StatusOK {
						t.Errorf("%s tenant %s %s: %d %s", v.version, tenant, target, a.Code, a.Body.String())
						continue
					}
					same++
					if strings.Contains(a.Body.String(), `"values":[[`) || strings.Contains(a.Body.String(), `"value":[`) || strings.Contains(a.Body.String(), `"label":`) {
						nonEmpty++
					}
				}
			}
		}
		if reads := onFront.streamFilterReads(); reads == 0 || offFront.streamFilterReads() != 0 {
			t.Errorf("%s: %d reads with the stream filter with the flag, %d without", v.version, reads, offFront.streamFilterReads())
		}
		if nonEmpty < total/3 {
			t.Errorf("%s: only %d of %d answers hold data", v.version, nonEmpty, total)
		}
		differs := 0
		for _, target := range snOracleRequests("svc-a") {
			headers := map[string]string{"X-Scope-OrgID": "1"}
			if snCanonical(doCompatProxyRequest(off, target, headers).Body.Bytes()) != snCanonical(doCompatProxyRequest(on, target, headers).Body.Bytes()) {
				differs++
			}
		}
		if differs == 0 {
			t.Errorf("%s: no answer differs on the tenant that breaks the statement: the check does not reach the boundary", v.version)
		}
		t.Logf("VictoriaLogs %s: %d of %d answers equal with and without the flag (%d hold data); %d of %d differ where the statement is false",
			v.version, same, total, nonEmpty, differs, len(snOracleRequests("svc-a")))
		offSrv.Close()
		onSrv.Close()
		srv.Stop()
	}
}

// TestServiceNameStreamFilterRoundTripsOnVictoriaLogs: the text the proxy serves with -service-name-stream-label-only
// (the query plan's, for log, parser, metric and instant shapes and values that need quoting) is accepted by
// VictoriaLogs v1.49.0 and v1.54.0 and survives its print and re-parse (Query.Clone, which every limit, tail,
// hits and cluster query takes): no corpus or shadow row reaches this text, as no profile there sets the flag.
//
// conformance: semantics/service-name-stream-filter, vl-stream-filter-or-groups-and-missing-label
func TestServiceNameStreamFilterRoundTripsOnVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs' parser")
	}
	p := serviceNameFlagProxy(t, "http://127.0.0.1:1", true)
	prof := loweringProfile(nil, nil, p.logsqlCapabilities(), false)
	prof.ServiceNameStreamLabelOnly = true
	var texts []string
	for _, value := range []string{"svc-a", unknownServiceName, `we"ird\x`, "ünï code", "line\nbreak", "tab\there", "{brace} or x"} {
		for _, target := range snOracleRequests(value)[:7] {
			u, _ := url.Parse(target)
			text, ok := p.PlanServedText(RequestQueryText(u.Query().Get("query")), prof)
			if !ok {
				t.Fatalf("not served from the plan: %s", u.Query().Get("query"))
			}
			if !strings.HasPrefix(text, "{service_name=") {
				t.Fatalf("no stream filter: %s", text)
			}
			texts = append(texts, text)
		}
	}
	work := t.TempDir()
	in := filepath.Join(work, "in.jsonl")
	var lines []string
	for i, q := range texts {
		b, _ := json.Marshal(map[string]string{"id": strconv.Itoa(i), "q": q})
		lines = append(lines, string(b))
	}
	if err := os.WriteFile(in, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		bin, err := oraclerun.Build(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, work, "vl-"+v.version)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(work, v.version+".jsonl")
		if err := oraclerun.Run(bin, "roundtrip", "-in", in, "-out", out); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var r struct {
				ID, Error, Panic string
				Accept, OK       bool
			}
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				t.Fatal(err)
			}
			i, _ := strconv.Atoi(r.ID)
			if !r.Accept || !r.OK {
				t.Errorf("%s rejects or does not round-trip %s: %s%s", v.version, texts[i], r.Error, r.Panic)
			}
			n++
		}
		if n != len(texts) {
			t.Fatalf("%s answered %d of %d texts", v.version, n, len(texts))
		}
		t.Logf("VictoriaLogs %s: %d texts accepted and round-tripped", v.version, n)
	}
}
