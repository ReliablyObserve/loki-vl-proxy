package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestHardening_TemplateProductionTuples(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"_time":"2026-01-01T00:00:01Z","_msg":"original","_stream":"{app=\"web\"}","trace_id":"trace-visible"}`)
	}))
	defer backend.Close()
	for _, streaming := range []bool{false, true} {
		for _, categorized := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/categorized=%v", streaming, categorized), func(t *testing.T) {
				p := newTestProxy(t, backend.URL)
				p.streamResponse = streaming
				p.emitStructuredMetadata = true
				headers := map[string]string{}
				if categorized {
					headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
				}
				for _, tc := range []struct {
					template string
					status   int
					want     string
				}{
					{`{{printf "%s" .app}}`, 200, "web"},
					{`{{printf "%100000000s" .app}}`, 400, "limit"},
				} {
					q := url.Values{"query": {`{app="web"} | line_format ` + "`" + tc.template + "`"}, "start": {"1767225600"}, "end": {"1767225602"}}
					result := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+q.Encode(), headers)
					if result.Code != tc.status || !strings.Contains(result.Body.String(), tc.want) {
						t.Fatalf("status=%d body=%s", result.Code, result.Body)
					}
					if result.Code == 200 && strings.Contains(result.Body.String(), "original") {
						t.Fatalf("formatting skipped: %s", result.Body)
					}
				}
			})
		}
	}
}

func TestHardening_TemplateCategorizedFieldsPreserved(t *testing.T) {
	metadata := map[string]interface{}{"parsed": map[string]string{"method": "GET"}, "structuredMetadata": map[string]string{"trace_id": "trace-visible"}}
	tuple := []interface{}{"1000", "original", metadata}
	streams := []map[string]interface{}{{"stream": map[string]string{"app": "web"}, "values": []interface{}{tuple}}}
	if err := applyLineFormatTemplate(streams, `{{.app}} {{.method}} {{.trace_id}}`); err != nil {
		t.Fatal(err)
	}
	if tuple[1] != "web GET trace-visible" {
		t.Fatalf("formatted line: %v", tuple[1])
	}
	if tuple[2].(map[string]interface{})["structuredMetadata"].(map[string]string)["trace_id"] != "trace-visible" {
		t.Fatal("metadata lost")
	}
}
