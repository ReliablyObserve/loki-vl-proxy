package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// A key a | json stage extracts under an alias's name, on a stream that lacks
// the alias's field, stays a parsed label of that name in the log answer in
// every mode, as Loki answers a parsed label no stream label collides with.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_ParsedAliasKeyInLogAnswer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			_, _ = w.Write([]byte(`{"_time":"2026-01-01T00:00:02Z","_stream":"{service.name=\"svc\"}","_stream_id":"2","_msg":"{\"level\":\"warn\",\"pod\":\"json-b\",\"namespace\":\"jns\"}","service.name":"svc","pod":"json-b","namespace":"jns","level":"warn"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	t.Cleanup(srv.Close)
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeUnset, FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		p := newMappingProxy(t, srv.URL, nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: mode, FieldMappings: collisionMappings(), EmitStructuredMetadata: true})
		params := url.Values{"query": {`{service_name="svc"} | json`}, "start": {"1767222000000000000"}, "end": {"1767225601000000000"}, "limit": {"10"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
		mux := http.NewServeMux()
		p.RegisterProxyRoutes(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var out struct {
			Data struct {
				Result []struct {
					Values [][]json.RawMessage `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Data.Result) != 1 {
			t.Fatalf("%s: %v %s", mode, err, rec.Body.String())
		}
		var meta struct {
			Parsed map[string]string `json:"parsed"`
		}
		_ = json.Unmarshal(out.Data.Result[0].Values[0][2], &meta)
		if meta.Parsed["pod"] != "json-b" || meta.Parsed["namespace"] != "jns" {
			t.Errorf("%s: parsed %v, want pod=json-b namespace=jns", mode, meta.Parsed)
		}
	}
}
