package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Rows as VictoriaLogs stores the e2e generator's Loki pushes: it unpacks a
// JSON line into stored fields (nested objects as dotted names) next to the
// structured metadata the push carried.
const (
	// OTel collector: JSON line plus OTel structured metadata.
	otelCollectorRow = `{"_time":"2026-10-01T17:21:10.500613Z","_msg":"{\"message\": \"pipeline logs/loki flushed\", \"level\": \"info\", \"pipeline\": \"logs/loki\", \"received\": 145, \"dropped\": 0, \"export_ms\": 175}",` +
		`"_stream":"{app=\"otel-collector\",cluster=\"us-east-1\",level=\"info\",service_name=\"otel-collector\"}","app":"otel-collector","cluster":"us-east-1","level":"info","service_name":"otel-collector",` +
		`"message":"pipeline logs/loki flushed","pipeline":"logs/loki","received":"145","dropped":"0","export_ms":"175",` +
		`"k8s.cluster.name":"k8s-prod-us-east-1","k8s.pod.name":"otel-collector-8000a-4a54","trace_id":"d534db2c2ecff990a520f21acb46f5a8"}`
	// API gateway: JSON line with a nested object, no structured metadata.
	apiGatewayRow = `{"_time":"2026-10-01T17:21:50.500138Z","_msg":"{\"service\": {\"name\": \"api-gateway\"}, \"method\": \"PUT\", \"path\": \"/api/v1/payments\", \"status\": 503}",` +
		`"_stream":"{app=\"api-gateway\",level=\"error\",service_name=\"api-gateway\"}","app":"api-gateway","level":"error","service_name":"api-gateway",` +
		`"service.name":"api-gateway","method":"PUT","path":"/api/v1/payments","status":"503"}`
)

func lineFieldsProxy(t *testing.T, backendURL string, path string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                 backendURL,
		Cache:                      cache.New(30*time.Second, 100),
		LogLevel:                   "error",
		EmitStructuredMetadata:     true,
		StreamResponse:             path == "streamed",
		QueryRangeWindowingEnabled: path == "windowed",
		QueryRangeSplitInterval:    15 * time.Minute,
		LabelStyle:                 LabelStyleUnderscores,
		MetadataFieldMode:          MetadataFieldModeTranslated,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

// lineFieldCategories runs query_range with categorize-labels and returns,
// per stream label set, the sorted structuredMetadata and parsed keys.
func lineFieldCategories(t *testing.T, p *Proxy, query string) (sm, parsed []string, labelTypes map[string]string) {
	t.Helper()
	q := url.Values{}
	q.Set("query", query)
	q.Set("start", "1790871720000000000")
	q.Set("end", "1790875320000000000")
	q.Set("limit", "10")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, w.Code, w.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || len(resp.Data.Result) == 0 {
		t.Fatalf("%s: decode: %v %s", query, err, w.Body.String())
	}
	smSet, pSet := map[string]bool{}, map[string]bool{}
	labelTypes = map[string]string{}
	for _, r := range resp.Data.Result {
		for k := range r.Stream {
			labelTypes[k] = "I"
		}
		for _, v := range r.Values {
			if len(v) < 3 {
				continue
			}
			var meta struct {
				StructuredMetadata map[string]string `json:"structuredMetadata"`
				Parsed             map[string]string `json:"parsed"`
			}
			_ = json.Unmarshal(v[2], &meta)
			for k := range meta.StructuredMetadata {
				smSet[k] = true
				labelTypes[k] = "S"
			}
			for k := range meta.Parsed {
				pSet[k] = true
				labelTypes[k] = "P"
			}
		}
	}
	for k := range smSet {
		sm = append(sm, k)
	}
	for k := range pSet {
		parsed = append(parsed, k)
	}
	sort.Strings(sm)
	sort.Strings(parsed)
	return sm, parsed, labelTypes
}

// TestLogQuery_LineFieldsNeedAParserStage: Loki returns labels from a log line
// only when the query runs a stage that adds them. VictoriaLogs unpacks a JSON
// line at ingest, so the Loki-compatible profile leaves those stored fields
// out of a query without such a stage, on the buffered, streamed and
// windowed response paths, and reports them as parsed with | json. Structured metadata
// (fields the line does not hold) stays structured metadata either way, and a
// nested line object (service.name from {"service":{"name":...}}) is line
// content too, so service_name keeps its indexed stream-label type.
//
// conformance: profiles/parsed-fields-without-parser, loki_api_v1_query_range, loki-compatible-profile
func TestLogQuery_LineFieldsNeedAParserStage(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = r.ParseForm()
		if strings.Contains(r.Form.Get("query"), "otel-collector") {
			_, _ = w.Write([]byte(otelCollectorRow + "\n"))
			return
		}
		_, _ = w.Write([]byte(apiGatewayRow + "\n"))
	}))
	defer backend.Close()

	for _, name := range []string{"buffered", "streamed", "windowed"} {
		p := lineFieldsProxy(t, backend.URL, name)

		sm, parsed, _ := lineFieldCategories(t, p, `{service_name="otel-collector"}`)
		if len(parsed) != 0 {
			t.Fatalf("%s: plain selector returned parsed labels %v; Loki returns none without a parser stage", name, parsed)
		}
		if want := "[detected_level k8s_cluster_name k8s_pod_name trace_id]"; strings.Join([]string{"[", strings.Join(sm, " "), "]"}, "") != want {
			t.Fatalf("%s: structured metadata %v, want %s", name, sm, want)
		}

		sm, parsed, _ = lineFieldCategories(t, p, `{service_name="otel-collector"} | json`)
		if want := "dropped export_ms message pipeline received"; strings.Join(parsed, " ") != want {
			t.Fatalf("%s: | json parsed %v, want %s", name, parsed, want)
		}
		if want := "detected_level k8s_cluster_name k8s_pod_name trace_id"; strings.Join(sm, " ") != want {
			t.Fatalf("%s: | json structured metadata %v, want %s", name, sm, want)
		}

		sm, parsed, types := lineFieldCategories(t, p, `{service_name="api-gateway"}`)
		if len(parsed) != 0 || strings.Join(sm, " ") != "detected_level" {
			t.Fatalf("%s: api-gateway plain selector: structured metadata %v parsed %v, want only detected_level", name, sm, parsed)
		}
		if types["service_name"] != "I" {
			t.Fatalf("%s: service_name typed %q, want the indexed stream label (I)", name, types["service_name"])
		}
	}
}

// TestHidesLineFields_StagesThatAddLabels: only a parser stage or label_format
// adds labels to a Loki entry; line filters, label filters, drop, keep,
// decolorize and line_format do not.
//
// conformance: profiles/parsed-fields-without-parser
func TestHidesLineFields_StagesThatAddLabels(t *testing.T) {
	p := lineFieldsProxy(t, "http://127.0.0.1:1", "buffered")
	for query, want := range map[string]bool{
		`{app="a"}`: true,
		`{app="a"} |= "x" | level="info" | drop pod`:    true,
		`{app="a"} | decolorize | line_format "{{.x}}"`: true,
		`{app="a"} | json`:                 false,
		`{app="a"} | logfmt`:               false,
		`{app="a"} | regexp "(?P<m>\\w+)"`: false,
		`{app="a"} | pattern "<m> <_>"`:    false,
		`{app="a"} | unpack`:               false,
		`{app="a"} | label_format x="y"`:   false,
	} {
		if got := p.hidesLineFields(query); got != want {
			t.Errorf("hidesLineFields(%s) = %v, want %v", query, got, want)
		}
	}
	other, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Second, 10), LogLevel: "error",
		LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeHybrid})
	if err != nil {
		t.Fatal(err)
	}
	if other.hidesLineFields(`{app="a"}`) {
		t.Fatal("hybrid metadata mode is not the Loki-compatible profile and keeps line fields")
	}
}
