package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestRegexpCaptureVisibilityAcrossResponseModes(t *testing.T) {
	const row = `{"_time":"2026-01-01T00:00:01Z","_msg":"GET /capture","_stream":"{app=\"web\"}","http_method":"GET","trace_id":"trace-visible"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, row) }))
	defer backend.Close()
	const query = `{app="web"} | regexp "(?P<http_method>[A-Z]+)"`
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
				q := url.Values{"query": {query}, "start": {"1767225600"}, "end": {"1767225602"}}
				result := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+q.Encode(), headers)
				if result.Code != 200 {
					t.Fatalf("query: %d %s", result.Code, result.Body)
				}
				var response struct {
					Data struct {
						Result []struct {
							Stream map[string]string `json:"stream"`
							Values [][]any           `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(result.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Data.Result) != 1 {
					t.Fatalf("wrong result: %s", result.Body)
				}
				stream := response.Data.Result[0]
				if stream.Stream["http_method"] != "GET" || stream.Stream["app"] != "web" || stream.Stream["trace_id"] != "" {
					t.Fatalf("wrong stream labels: %s", result.Body)
				}
				if len(stream.Values) != 1 || stream.Values[0][1] != "GET /capture" {
					t.Fatalf("line changed: %s", result.Body)
				}
				if categorized {
					metadata := stream.Values[0][2].(map[string]any)
					parsed := metadata["parsed"].(map[string]any)
					sm := metadata["structuredMetadata"].(map[string]any)
					if parsed["http_method"] != "GET" || parsed["trace_id"] != nil || sm["trace_id"] != "trace-visible" || sm["http_method"] != nil {
						t.Fatalf("wrong field classification: %s", result.Body)
					}
				}
				entries := p.vlLogsToLokiWindowEntries([]byte(row+"\n"), query, categorized, categorized)
				if len(entries) != 1 || entries[0].Stream["http_method"] != "GET" || entries[0].Parsed["http_method"] != "GET" || entries[0].SM["trace_id"] != "trace-visible" {
					t.Fatalf("window reader lost capture: %+v", entries)
				}
			})
		}
	}
}

func TestRegexpCaptureNamesIgnoreLiteralParserText(t *testing.T) {
	if names := regexpCaptureFields(`{app="web"} |= "| regexp (?P<fake>.*)"`); len(names) != 0 {
		t.Fatalf("literal recognized as parser: %v", names)
	}
}
