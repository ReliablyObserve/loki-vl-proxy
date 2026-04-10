package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newTupleContractProxy(t *testing.T, backendURL string, streamResponse bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backendURL,
		Cache:                  cache.New(30*time.Second, 200),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		StreamResponse:         streamResponse,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStyleUnderscores,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	return p
}

func assertStrictTwoTupleContract(t *testing.T, body []byte) {
	t.Helper()
	var payload struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode response payload: %v\nbody=%s", err, string(body))
	}
	if payload.Status != "success" {
		t.Fatalf("expected success status, got %q body=%s", payload.Status, string(body))
	}

	tupleCount := 0
	for _, stream := range payload.Data.Result {
		for _, rawTuple := range stream.Values {
			tupleCount++
			var tuple []json.RawMessage
			if err := json.Unmarshal(rawTuple, &tuple); err != nil {
				t.Fatalf("failed to decode tuple: %v\nraw=%s", err, string(rawTuple))
			}
			if len(tuple) != 2 {
				t.Fatalf("expected strict 2-tuple [ts,line], got len=%d tuple=%s", len(tuple), string(rawTuple))
			}
			var ts string
			if err := json.Unmarshal(tuple[0], &ts); err != nil {
				t.Fatalf("expected tuple[0] string timestamp, got %s: %v", string(tuple[0]), err)
			}
			var line string
			if err := json.Unmarshal(tuple[1], &line); err != nil {
				t.Fatalf("expected tuple[1] string line, got %s: %v", string(tuple[1]), err)
			}
		}
	}
	if tupleCount == 0 {
		t.Fatalf("expected at least one tuple in response body=%s", string(body))
	}
}

func assertCategorizeLabelsThreeTupleContract(t *testing.T, body []byte) {
	t.Helper()
	var payload struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values []json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode response payload: %v\nbody=%s", err, string(body))
	}
	if payload.Status != "success" {
		t.Fatalf("expected success status, got %q body=%s", payload.Status, string(body))
	}

	tupleCount := 0
	for _, stream := range payload.Data.Result {
		for _, rawTuple := range stream.Values {
			tupleCount++
			var tuple []json.RawMessage
			if err := json.Unmarshal(rawTuple, &tuple); err != nil {
				t.Fatalf("failed to decode tuple: %v\nraw=%s", err, string(rawTuple))
			}
			if len(tuple) != 3 {
				t.Fatalf("expected 3-tuple [ts,line,metadata], got len=%d tuple=%s", len(tuple), string(rawTuple))
			}

			var metadata map[string]json.RawMessage
			if err := json.Unmarshal(tuple[2], &metadata); err != nil {
				t.Fatalf("expected tuple[2] metadata object, got %s: %v", string(tuple[2]), err)
			}
			if len(metadata) == 0 {
				continue
			}
			if _, ok := metadata["structured_metadata"]; ok {
				t.Fatalf("non-Loki structured_metadata alias must not be emitted: %s", string(tuple[2]))
			}
			if raw, ok := metadata["structuredMetadata"]; ok {
				assertMetadataPairArray(t, raw, "structuredMetadata")
			}
			if raw, ok := metadata["parsed"]; ok {
				assertMetadataPairArray(t, raw, "parsed")
			}
			if _, ok := metadata["structuredMetadata"]; !ok {
				if _, ok := metadata["parsed"]; !ok {
					t.Fatalf("expected structuredMetadata and/or parsed metadata keys, got %s", string(tuple[2]))
				}
			}
		}
	}
	if tupleCount == 0 {
		t.Fatalf("expected at least one tuple in response body=%s", string(body))
	}
}

func assertMetadataPairArray(t *testing.T, raw json.RawMessage, key string) {
	t.Helper()
	var pairs []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &pairs); err != nil {
		t.Fatalf("expected %s to be Loki label-pair array, got %s: %v", key, string(raw), err)
	}
	for _, pair := range pairs {
		if pair.Name == "" {
			t.Fatalf("expected non-empty %s pair name in %s", key, string(raw))
		}
	}
}

func makeTupleContractBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		tenant := r.Header.Get("X-Scope-OrgID")
		switch tenant {
		case "tenant-a":
			_, _ = w.Write([]byte(`{"_time":"2026-04-10T10:00:00Z","_msg":"time=\"2026-04-10T10:00:00Z\" level=error msg=\"Drop if no profiles matched -j DROP\" chain.link.env=dev","_stream":"{job=\"tuple-contract\",level=\"error\"}","job":"tuple-contract","level":"error","chain.link.env":"dev"}` + "\n"))
		case "tenant-b":
			_, _ = w.Write([]byte(`{"_time":"2026-04-10T10:00:01Z","_msg":"time=\"2026-04-10T10:00:01Z\" level=error msg=\"fromYaml not defined\" app.label.component=notifications-controller","_stream":"{job=\"tuple-contract\",level=\"error\"}","job":"tuple-contract","level":"error","app.label.component":"notifications-controller"}` + "\n"))
		default:
			_, _ = w.Write([]byte(`{"_time":"2026-04-10T10:00:02Z","_msg":"time=\"2026-04-10T10:00:02Z\" level=error msg=\"api chain check\" resource=argocd/f5-nginx-external","_stream":"{job=\"tuple-contract\",level=\"error\"}","job":"tuple-contract","level":"error","resource":"argocd/f5-nginx-external"}` + "\n"))
		}
	}))
}

func TestTupleContract_DefaultQueryRangeStrictTwoTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "DROP" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "50")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)

	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertStrictTwoTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_DefaultQueryStrictTwoTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "api" | json | logfmt | drop __error__, __error_details__`)
	params.Set("time", "2")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)

	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertStrictTwoTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_StreamResponseModeStrictTwoTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, true)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "fromYaml" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "20")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)

	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertStrictTwoTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_MultiTenantMergeStrictTwoTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "level=error" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "50")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")

	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertStrictTwoTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_CategorizeLabelsQueryRangeThreeTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "DROP" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "50")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")

	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertCategorizeLabelsThreeTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_CategorizeLabelsQueryThreeTuple(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "api" | json | logfmt | drop __error__, __error_details__`)
	params.Set("time", "2")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")

	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertCategorizeLabelsThreeTupleContract(t, rec.Body.Bytes())
}

func TestTupleContract_QueryRangeCacheSegregatesTupleModes(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "api" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "50")

	categorizedReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	categorizedReq.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	categorizedRec := httptest.NewRecorder()
	p.handleQueryRange(categorizedRec, categorizedReq)
	if categorizedRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", categorizedRec.Code, categorizedRec.Body.String())
	}
	assertCategorizeLabelsThreeTupleContract(t, categorizedRec.Body.Bytes())

	defaultReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	defaultRec := httptest.NewRecorder()
	p.handleQueryRange(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", defaultRec.Code, defaultRec.Body.String())
	}
	assertStrictTwoTupleContract(t, defaultRec.Body.Bytes())
}

func TestTupleContract_MultiTenantQueryRangeCacheSegregatesTupleModes(t *testing.T) {
	backend := makeTupleContractBackend(t)
	defer backend.Close()

	p := newTupleContractProxy(t, backend.URL, false)
	params := url.Values{}
	params.Set("query", `{job="tuple-contract"} |= "level=error" | json | logfmt | drop __error__, __error_details__`)
	params.Set("start", "1")
	params.Set("end", "2")
	params.Set("limit", "50")

	categorizedReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	categorizedReq.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	categorizedReq.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	categorizedRec := httptest.NewRecorder()
	p.handleQueryRange(categorizedRec, categorizedReq)
	if categorizedRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", categorizedRec.Code, categorizedRec.Body.String())
	}
	assertCategorizeLabelsThreeTupleContract(t, categorizedRec.Body.Bytes())

	defaultReq := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	defaultReq.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
	defaultRec := httptest.NewRecorder()
	p.handleQueryRange(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", defaultRec.Code, defaultRec.Body.String())
	}
	assertStrictTwoTupleContract(t, defaultRec.Body.Bytes())
}
