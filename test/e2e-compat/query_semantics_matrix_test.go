//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

type querySemanticsMatrix struct {
	Description string               `json:"description"`
	Oracle      string               `json:"oracle"`
	Cases       []querySemanticsCase `json:"cases"`
}

type querySemanticsOperations struct {
	Description string                    `json:"description"`
	Operations  []querySemanticsOperation `json:"operations"`
}

type querySemanticsOperation struct {
	Name     string   `json:"name"`
	Category string   `json:"category"`
	Cases    []string `json:"cases"`
}

type querySemanticsCase struct {
	ID               string `json:"id"`
	Family           string `json:"family"`
	Endpoint         string `json:"endpoint"`
	Query            string `json:"query"`
	Step             string `json:"step"`
	Expectation      string `json:"expectation"`
	ExpectResultType string `json:"expect_result_type"`
	Compare          string `json:"compare"`
	RequireNonEmpty  bool   `json:"require_non_empty"`
}

func loadQuerySemanticsMatrix(t *testing.T) querySemanticsMatrix {
	t.Helper()

	data, err := os.ReadFile("query-semantics-matrix.json")
	if err != nil {
		t.Fatalf("read query semantics matrix: %v", err)
	}
	var matrix querySemanticsMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatalf("decode query semantics matrix: %v", err)
	}
	if len(matrix.Cases) == 0 {
		t.Fatal("query semantics matrix must not be empty")
	}
	for _, tc := range matrix.Cases {
		if tc.ID == "" || tc.Endpoint == "" || tc.Query == "" || tc.Expectation == "" {
			t.Fatalf("query semantics case must define id, endpoint, query, and expectation: %+v", tc)
		}
		switch tc.Endpoint {
		case "query", "query_range":
		default:
			t.Fatalf("unsupported query semantics endpoint %q in case %q", tc.Endpoint, tc.ID)
		}
		switch tc.Expectation {
		case "success", "client_error", "server_error":
		default:
			t.Fatalf("unsupported query semantics expectation %q in case %q", tc.Expectation, tc.ID)
		}
	}
	return matrix
}

func loadQuerySemanticsOperations(t *testing.T) querySemanticsOperations {
	t.Helper()

	data, err := os.ReadFile("query-semantics-operations.json")
	if err != nil {
		t.Fatalf("read query semantics operations inventory: %v", err)
	}
	var inventory querySemanticsOperations
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatalf("decode query semantics operations inventory: %v", err)
	}
	if inventory.Description == "" || len(inventory.Operations) == 0 {
		t.Fatal("query semantics operations inventory must not be empty")
	}
	for _, op := range inventory.Operations {
		if op.Name == "" || op.Category == "" || len(op.Cases) == 0 {
			t.Fatalf("invalid query semantics operation inventory entry: %+v", op)
		}
	}
	return inventory
}

func querySemanticsGET(t *testing.T, baseURL string, tc querySemanticsCase) (int, string, map[string]interface{}) {
	t.Helper()

	params := url.Values{}
	params.Set("query", tc.Query)

	now := time.Now()
	if tc.Endpoint == "query_range" {
		params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
		params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
		params.Set("limit", "1000")
		if tc.Step != "" {
			params.Set("step", tc.Step)
		}
		return doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	}

	params.Set("time", fmt.Sprintf("%d", now.UnixNano()))
	return doJSONGET(t, baseURL+"/loki/api/v1/query?"+params.Encode(), nil)
}

func querySemanticsResultCount(resp map[string]interface{}) int {
	data := extractMap(resp, "data")
	return len(extractArray(data, "result"))
}

func querySemanticsStatusString(resp map[string]interface{}) string {
	if resp == nil {
		return ""
	}
	if status, _ := resp["status"].(string); status != "" {
		return status
	}
	return ""
}

func querySemanticsErrorType(resp map[string]interface{}) string {
	if resp == nil {
		return ""
	}
	if errorType, _ := resp["errorType"].(string); errorType != "" {
		return errorType
	}
	return ""
}

func querySemanticsMetricKeySet(resp map[string]interface{}) []string {
	data := extractMap(resp, "data")
	results := extractArray(data, "result")
	keys := make([]string, 0, len(results))
	for _, item := range results {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		metric := extractMap(entry, "metric")
		parts := make([]string, 0, len(metric))
		for key, value := range metric {
			parts = append(parts, fmt.Sprintf("%s=%v", key, value))
		}
		sort.Strings(parts)
		keys = append(keys, strings.Join(parts, ","))
	}
	sort.Strings(keys)
	return keys
}

func TestQuerySemanticsMatrixManifest(t *testing.T) {
	matrix := loadQuerySemanticsMatrix(t)
	if matrix.Description == "" || matrix.Oracle == "" {
		t.Fatalf("query semantics matrix must describe its purpose and oracle: %+v", matrix)
	}
}

func TestQuerySemanticsOperationsInventory(t *testing.T) {
	matrix := loadQuerySemanticsMatrix(t)
	inventory := loadQuerySemanticsOperations(t)

	caseIDs := make(map[string]struct{}, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		caseIDs[tc.ID] = struct{}{}
	}

	seenCaseRefs := make(map[string]struct{}, len(matrix.Cases))
	for _, op := range inventory.Operations {
		for _, caseID := range op.Cases {
			if _, ok := caseIDs[caseID]; !ok {
				t.Fatalf("operation %q references unknown query semantics case %q", op.Name, caseID)
			}
			seenCaseRefs[caseID] = struct{}{}
		}
	}

	for _, tc := range matrix.Cases {
		if _, ok := seenCaseRefs[tc.ID]; !ok {
			t.Fatalf("query semantics case %q is not tracked by the operations inventory", tc.ID)
		}
	}
}

func TestQuerySemanticsMatrix(t *testing.T) {
	ensureDataIngested(t)
	matrix := loadQuerySemanticsMatrix(t)

	for _, tc := range matrix.Cases {
		tc := tc
		t.Run(tc.ID, func(t *testing.T) {
			proxyStatus, proxyBody, proxyResp := querySemanticsGET(t, proxyURL, tc)
			lokiStatus, lokiBody, lokiResp := querySemanticsGET(t, lokiURL, tc)

			switch tc.Expectation {
			case "success":
				if proxyStatus != http.StatusOK || lokiStatus != http.StatusOK {
					t.Fatalf("expected 200 from both backends, proxy=%d loki=%d proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, proxyBody, lokiBody)
				}
				if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
					t.Fatalf("expected success payload from both backends, proxy=%v loki=%v", proxyResp, lokiResp)
				}

				proxyData := extractMap(proxyResp, "data")
				lokiData := extractMap(lokiResp, "data")
				if proxyData["resultType"] != lokiData["resultType"] {
					t.Fatalf("resultType mismatch for %s: proxy=%v loki=%v", tc.ID, proxyData["resultType"], lokiData["resultType"])
				}
				if tc.ExpectResultType != "" && proxyData["resultType"] != tc.ExpectResultType {
					t.Fatalf("unexpected resultType for %s: got=%v want=%s", tc.ID, proxyData["resultType"], tc.ExpectResultType)
				}

				switch tc.Compare {
				case "line_count":
					proxyCount := countLogLines(proxyResp)
					lokiCount := countLogLines(lokiResp)
					if tc.RequireNonEmpty && (proxyCount == 0 || lokiCount == 0) {
						t.Fatalf("expected non-empty log results for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
					}
					if proxyCount != lokiCount {
						t.Fatalf("line-count mismatch for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
					}
				case "series_count":
					proxyCount := querySemanticsResultCount(proxyResp)
					lokiCount := querySemanticsResultCount(lokiResp)
					if tc.RequireNonEmpty && (proxyCount == 0 || lokiCount == 0) {
						t.Fatalf("expected non-empty result series for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
					}
					if proxyCount != lokiCount {
						t.Fatalf("series-count mismatch for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
					}
				case "metric_keys":
					proxyKeys := querySemanticsMetricKeySet(proxyResp)
					lokiKeys := querySemanticsMetricKeySet(lokiResp)
					if tc.RequireNonEmpty && (len(proxyKeys) == 0 || len(lokiKeys) == 0) {
						t.Fatalf("expected non-empty metric key results for %s, proxy=%v loki=%v", tc.ID, proxyKeys, lokiKeys)
					}
					if len(proxyKeys) != len(lokiKeys) {
						t.Fatalf("metric-key cardinality mismatch for %s, proxy=%v loki=%v", tc.ID, proxyKeys, lokiKeys)
					}
					for i := range proxyKeys {
						if proxyKeys[i] != lokiKeys[i] {
							t.Fatalf("metric-key mismatch for %s, proxy=%v loki=%v", tc.ID, proxyKeys, lokiKeys)
						}
					}
				case "":
				default:
					t.Fatalf("unsupported comparison mode %q in case %q", tc.Compare, tc.ID)
				}

			case "client_error":
				if proxyStatus < 400 || proxyStatus >= 500 || lokiStatus < 400 || lokiStatus >= 500 {
					t.Fatalf("expected 4xx from both backends, proxy=%d loki=%d proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, proxyBody, lokiBody)
				}
				if proxyStatus != lokiStatus {
					t.Fatalf("expected matching 4xx status for %s, proxy=%d loki=%d proxyBody=%s lokiBody=%s", tc.ID, proxyStatus, lokiStatus, proxyBody, lokiBody)
				}
				proxyPayloadStatus := querySemanticsStatusString(proxyResp)
				lokiPayloadStatus := querySemanticsStatusString(lokiResp)
				if proxyPayloadStatus != "" && lokiPayloadStatus != "" && proxyPayloadStatus != lokiPayloadStatus {
					t.Fatalf("expected matching payload status for %s, proxy=%q loki=%q proxyBody=%s lokiBody=%s", tc.ID, querySemanticsStatusString(proxyResp), querySemanticsStatusString(lokiResp), proxyBody, lokiBody)
				}
				proxyErrorType := querySemanticsErrorType(proxyResp)
				lokiErrorType := querySemanticsErrorType(lokiResp)
				if proxyErrorType != "" && lokiErrorType != "" && proxyErrorType != lokiErrorType {
					t.Fatalf("expected matching errorType for %s, proxy=%q loki=%q proxyBody=%s lokiBody=%s", tc.ID, querySemanticsErrorType(proxyResp), querySemanticsErrorType(lokiResp), proxyBody, lokiBody)
				}
			case "server_error":
				if proxyStatus < 500 || proxyStatus >= 600 || lokiStatus < 500 || lokiStatus >= 600 {
					t.Fatalf("expected 5xx from both backends, proxy=%d loki=%d proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, proxyBody, lokiBody)
				}
				if proxyStatus != lokiStatus {
					t.Fatalf("expected matching 5xx status for %s, proxy=%d loki=%d proxyBody=%s lokiBody=%s", tc.ID, proxyStatus, lokiStatus, proxyBody, lokiBody)
				}
			}
		})
	}
}
