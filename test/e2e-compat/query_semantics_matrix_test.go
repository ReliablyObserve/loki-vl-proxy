//go:build e2e

package e2e_compat

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/metricparity"
)

func TestQuerySemanticsMatrixTenantScopedRequestCarriesOrgID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Scope-OrgID"); got != "4200000000" {
			t.Errorf("X-Scope-OrgID = %q, want %q", got, "4200000000")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range tenantRequestHeaders("4200000000") {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestQuerySemanticsMatrixTenantScopedVLRequestUsesAccountAndProjectHeaders(t *testing.T) {
	headers := victoriaLogsTenantHeaders("4200000000")
	if got := headers["AccountID"]; got != "4200000000" {
		t.Errorf("AccountID = %q, want %q", got, "4200000000")
	}
	if got := headers["ProjectID"]; got != "0" {
		t.Errorf("ProjectID = %q, want %q", got, "0")
	}
	if _, ok := headers["X-Scope-OrgID"]; ok {
		t.Error("VictoriaLogs request must not use Loki's X-Scope-OrgID header")
	}
}

func TestQuerySemanticsMatrixScopeQueryAddsFixtureMarker(t *testing.T) {
	for _, tc := range []struct {
		name, query, want string
	}{
		{"existing_matchers", `{app="api-gateway"} | json`, `{app="api-gateway",query_semantics_run_id="run-123"} | json`},
		{"empty_selector", `{} |= "timeout"`, `{query_semantics_run_id="run-123"} |= "timeout"`},
		{"brace_in_matcher_value", `{message="literal } brace"}`, `{message="literal } brace",query_semantics_run_id="run-123"}`},
		{"all_selectors", `{app="api-gateway"} / on(app) {level="error"}`, `{app="api-gateway",query_semantics_run_id="run-123"} / on(app) {level="error",query_semantics_run_id="run-123"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scopeQuerySemanticsQuery(tc.query, "run-123")
			if err != nil {
				t.Fatalf("scope query: %v", err)
			}
			if got != tc.want {
				t.Fatalf("scoped query = %q, want %q", got, tc.want)
			}
		})
	}
	if _, err := scopeQuerySemanticsQuery(`rate(foo[5m])`, "run-123"); err == nil {
		t.Fatal("query without a stream selector was accepted")
	}
}

type querySemanticsMatrix struct {
	Description      string                          `json:"description"`
	Oracle           string                          `json:"oracle"`
	Cases            []querySemanticsCase            `json:"cases"`
	PerformanceCases []querySemanticsPerformanceCase `json:"performance_cases"`
}

type querySemanticsPerformanceCase struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	CaseID   string `json:"case_id,omitempty"`
	Query    string `json:"query,omitempty"`
}

type querySemanticsOperations struct {
	Description string                    `json:"description"`
	Operations  []querySemanticsOperation `json:"operations"`
	DepthPaths  []querySemanticsDepthPath `json:"depth_paths"`
}

type querySemanticsOperation struct {
	Name     string   `json:"name"`
	Category string   `json:"category"`
	Cases    []string `json:"cases"`
}

type querySemanticsDepthPath struct {
	ID               string   `json:"id"`
	Depth            int      `json:"depth"`
	Parser           string   `json:"parser"`
	Stages           []string `json:"stages"`
	MatrixCases      []string `json:"matrix_cases"`
	RegistryItems    []string `json:"registry_items"`
	RegistryCases    []string `json:"registry_cases"`
	LiveTest         string   `json:"live_test"`
	RecordedEvidence string   `json:"recorded_evidence"`
	RelatedTests     []string `json:"related_tests,omitempty"`
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
	KnownDeviation   string `json:"known_deviation,omitempty"`
}

// querySemanticsLogEntries compares the full stream identity and every log
// value, including optional structured metadata. It treats stream and entry
// ordering as immaterial but retains duplicate entries as distinct counts.
func querySemanticsLogEntries(body string) (map[string]int, error) {
	var response struct {
		Data struct {
			Result *[]struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return nil, err
	}
	if response.Data.Result == nil {
		return nil, fmt.Errorf("missing log result")
	}
	entries := make(map[string]int)
	for _, series := range *response.Data.Result {
		if series.Stream == nil || series.Values == nil {
			return nil, fmt.Errorf("log series lacks stream or values")
		}
		for _, value := range series.Values {
			if len(value) < 2 || len(value) > 3 {
				return nil, fmt.Errorf("log value must contain timestamp, line and optional metadata")
			}
			var entry struct {
				Stream    map[string]string  `json:"stream"`
				Timestamp string             `json:"timestamp"`
				Line      string             `json:"line"`
				Metadata  *map[string]string `json:"metadata,omitempty"`
			}
			entry.Stream = series.Stream
			if err := json.Unmarshal(value[0], &entry.Timestamp); err != nil {
				return nil, fmt.Errorf("log timestamp: %w", err)
			}
			if err := json.Unmarshal(value[1], &entry.Line); err != nil {
				return nil, fmt.Errorf("log line: %w", err)
			}
			if len(value) == 3 {
				entry.Metadata = new(map[string]string)
				if err := json.Unmarshal(value[2], entry.Metadata); err != nil || *entry.Metadata == nil {
					return nil, fmt.Errorf("invalid log metadata: %v", err)
				}
			}
			key, err := json.Marshal(entry)
			if err != nil {
				return nil, err
			}
			entries[string(key)]++
		}
	}
	return entries, nil
}

func querySemanticsSameLogEntries(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for entry, count := range left {
		if right[entry] != count {
			return false
		}
	}
	return true
}

func loadQuerySemanticsMatrix(t testing.TB) querySemanticsMatrix {
	t.Helper()

	data, err := os.ReadFile("query-semantics-matrix.json")
	if err != nil {
		t.Fatalf("read query semantics matrix: %v", err)
	}
	var matrix querySemanticsMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatalf("decode query semantics matrix: %v", err)
	}
	if err := validateQuerySemanticsMatrix(matrix); err != nil {
		t.Fatal(err)
	}
	return matrix
}

func querySemanticsCaseQuery(t testing.TB, id string) string {
	t.Helper()
	for _, tc := range loadQuerySemanticsMatrix(t).Cases {
		if tc.ID == id {
			return tc.Query
		}
	}
	t.Fatalf("unknown query semantics case %q", id)
	return ""
}

func validateQuerySemanticsMatrix(matrix querySemanticsMatrix) error {
	if len(matrix.Cases) == 0 {
		return fmt.Errorf("query semantics matrix must not be empty")
	}
	seen := make(map[string]bool, len(matrix.Cases))
	qualityQueries := make(map[string]string, len(matrix.Cases))
	qualitySuccess := make(map[string]bool, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		if seen[tc.ID] {
			return fmt.Errorf("duplicate query semantics case %q", tc.ID)
		}
		seen[tc.ID] = true
		qualityQueries[tc.Query] = tc.ID
		qualitySuccess[tc.ID] = tc.Expectation == "success"
		if tc.ID == "" || tc.Endpoint == "" || tc.Query == "" || tc.Expectation == "" {
			return fmt.Errorf("query semantics case must define id, endpoint, query, and expectation: %+v", tc)
		}
		switch tc.Endpoint {
		case "query", "query_range":
		default:
			return fmt.Errorf("unsupported query semantics endpoint %q in case %q", tc.Endpoint, tc.ID)
		}
		switch tc.Expectation {
		case "success", "client_error", "server_error":
		default:
			return fmt.Errorf("unsupported query semantics expectation %q in case %q", tc.Expectation, tc.ID)
		}
		if tc.Expectation != "success" {
			if tc.Compare != "" || tc.KnownDeviation != "" {
				return fmt.Errorf("error outcome %q cannot declare a success comparison", tc.ID)
			}
			continue
		}
		metric := tc.ExpectResultType == "matrix" || tc.ExpectResultType == "vector"
		switch tc.Compare {
		case "log_entries":
			if tc.ExpectResultType != "streams" {
				return fmt.Errorf("log-entry case %q must expect streams", tc.ID)
			}
		case "series_count", "metric_keys", "metric_points", "any_non_empty":
			if !metric {
				return fmt.Errorf("metric comparison %q must expect a matrix or vector", tc.ID)
			}
			if tc.Endpoint == "query_range" && tc.Step == "" {
				return fmt.Errorf("metric range case %q needs an explicit step", tc.ID)
			}
		default:
			return fmt.Errorf("unsupported comparison %q in case %q", tc.Compare, tc.ID)
		}
		if tc.Compare == "any_non_empty" {
			if tc.KnownDeviation == "" || !tc.RequireNonEmpty {
				return fmt.Errorf("availability case %q needs an accepted registry deviation and nonempty data", tc.ID)
			}
		} else if tc.KnownDeviation != "" {
			return fmt.Errorf("case %q cannot waive a strict comparison with a deviation", tc.ID)
		}
	}
	performanceIDs := make(map[string]bool, len(matrix.PerformanceCases))
	for _, perf := range matrix.PerformanceCases {
		if perf.ID == "" || performanceIDs[perf.ID] || perf.Category == "" || (perf.CaseID == "") == (perf.Query == "") {
			return fmt.Errorf("invalid or duplicate performance case %q", perf.ID)
		}
		performanceIDs[perf.ID] = true
		if perf.CaseID != "" && !qualitySuccess[perf.CaseID] {
			return fmt.Errorf("performance case %q references unknown or unsuccessful quality case %q", perf.ID, perf.CaseID)
		}
		if sharedID, ok := qualityQueries[perf.Query]; perf.Query != "" && ok {
			return fmt.Errorf("performance case %q duplicates quality query %q instead of referencing it", perf.ID, sharedID)
		}
	}
	return nil
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

func querySemanticsParams(tc querySemanticsCase, evaluation time.Time) url.Values {
	params := url.Values{}
	params.Set("query", tc.Query)
	if tc.Endpoint == "query_range" {
		params.Set("start", fmt.Sprintf("%d", evaluation.Add(-10*time.Minute).UnixNano()))
		params.Set("end", fmt.Sprintf("%d", evaluation.UnixNano()))
		params.Set("limit", "1000")
		if tc.Step != "" {
			params.Set("step", tc.Step)
		}
	} else {
		params.Set("time", fmt.Sprintf("%d", evaluation.UnixNano()))
	}
	return params
}

func querySemanticsGET(t *testing.T, baseURL, endpoint string, params url.Values, tenant string) (int, string, map[string]interface{}) {
	t.Helper()
	return doJSONGET(t, baseURL+"/loki/api/v1/"+endpoint+"?"+params.Encode(), tenantRequestHeaders(tenant))
}

func scopeQuerySemanticsQuery(query, runID string) (string, error) {
	if strings.TrimSpace(runID) == "" {
		return "", fmt.Errorf("query semantics fixture marker must not be empty")
	}
	marker := fmt.Sprintf(`query_semantics_run_id=%q`, runID)
	var scoped strings.Builder
	quote := byte(0)
	escaped := false
	selectors := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			scoped.WriteByte(ch)
			if quote == '"' && escaped {
				escaped = false
				continue
			}
			if quote == '"' && ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '"' || ch == '`' {
			quote = ch
			scoped.WriteByte(ch)
			continue
		}
		if ch == '{' {
			selectorEnd := i + 1
			selectorQuote := byte(0)
			selectorEscaped := false
			for selectorEnd < len(query) {
				selectorChar := query[selectorEnd]
				if selectorQuote != 0 {
					if selectorQuote == '"' && selectorEscaped {
						selectorEscaped = false
						selectorEnd++
						continue
					}
					if selectorQuote == '"' && selectorChar == '\\' {
						selectorEscaped = true
						selectorEnd++
						continue
					}
					if selectorChar == selectorQuote {
						selectorQuote = 0
					}
				} else if selectorChar == '"' || selectorChar == '`' {
					selectorQuote = selectorChar
				} else if selectorChar == '}' {
					break
				}
				selectorEnd++
			}
			if selectorEnd == len(query) {
				return "", fmt.Errorf("query semantics query has an unterminated stream selector")
			}
			selector := query[i+1 : selectorEnd]
			scoped.WriteByte('{')
			if strings.TrimSpace(selector) != "" {
				scoped.WriteString(selector)
				scoped.WriteByte(',')
			}
			scoped.WriteString(marker)
			scoped.WriteByte('}')
			i = selectorEnd
			selectors++
			continue
		}
		scoped.WriteByte(ch)
	}
	if selectors == 0 {
		return "", fmt.Errorf("query semantics query has no stream selector")
	}
	return scoped.String(), nil
}

func isolatedNumericTenantID(t *testing.T) string {
	t.Helper()
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("generate isolated numeric tenant ID: %v", err)
	}
	id := binary.BigEndian.Uint32(raw[:])
	if id == 0 {
		id = 1
	}
	return strconv.FormatUint(uint64(id), 10)
}

func TestQuerySemanticsMatrixIsolatedNumericTenantIDIsCanonicalAndUnique(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for range 64 {
		id := isolatedNumericTenantID(t)
		parsed, err := strconv.ParseUint(id, 10, 32)
		if err != nil || strconv.FormatUint(parsed, 10) != id {
			t.Fatalf("tenant ID %q is not a canonical uint32: parse error %v", id, err)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("generated duplicate tenant ID %q", id)
		}
		seen[id] = struct{}{}
	}
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

func TestQuerySemanticsMatrixLogPerformanceCasesHaveQualityContracts(t *testing.T) {
	matrix := loadQuerySemanticsMatrix(t)
	logCategories := map[string]bool{
		"parser": true, "field_filter": true, "format": true,
		"drop_keep": true, "multi_stage": true,
	}
	for _, perf := range matrix.PerformanceCases {
		if logCategories[perf.Category] && perf.CaseID == "" {
			t.Errorf("log performance case %q lacks a quality case", perf.ID)
		}
	}
}

func TestQuerySemanticsMatrixContractValidation(t *testing.T) {
	metric := querySemanticsCase{ID: "metric", Family: "aggregation", Endpoint: "query_range", Query: `count_over_time({app="test"}[1m])`, Step: "60", Expectation: "success", ExpectResultType: "matrix", Compare: "metric_points", RequireNonEmpty: true}
	for _, tc := range []struct {
		name string
		edit func(*querySemanticsCase)
	}{
		{"unknown_comparison", func(c *querySemanticsCase) { c.Compare = "misspelled" }},
		{"missing_comparison", func(c *querySemanticsCase) { c.Compare = "" }},
		{"metric_comparison_on_logs", func(c *querySemanticsCase) { c.ExpectResultType = "streams" }},
		{"line_comparison_on_metrics", func(c *querySemanticsCase) { c.Compare = "line_count" }},
		{"log_entry_comparison_on_metrics", func(c *querySemanticsCase) { c.Compare = "log_entries" }},
		{"missing_metric_step", func(c *querySemanticsCase) { c.Step = "" }},
		{"comparison_on_error", func(c *querySemanticsCase) { c.Expectation = "client_error" }},
		{"availability_without_owner", func(c *querySemanticsCase) { c.Compare = "any_non_empty" }},
		{"deviation_on_strict_contract", func(c *querySemanticsCase) { c.KnownDeviation = "parsed-identity/dynamic-key-parsers" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := metric
			tc.edit(&candidate)
			if err := validateQuerySemanticsMatrix(querySemanticsMatrix{Cases: []querySemanticsCase{candidate}}); err == nil {
				t.Fatal("invalid comparison contract accepted")
			}
		})
	}
	if err := validateQuerySemanticsMatrix(querySemanticsMatrix{Cases: []querySemanticsCase{metric, metric}}); err == nil {
		t.Error("duplicate case identity accepted")
	}
	if err := validateQuerySemanticsMatrix(querySemanticsMatrix{Cases: []querySemanticsCase{metric}}); err != nil {
		t.Errorf("valid metric contract rejected: %v", err)
	}
	logs := querySemanticsCase{ID: "logs", Family: "selector", Endpoint: "query_range", Query: `{app="test"}`, Expectation: "success", ExpectResultType: "streams", Compare: "log_entries"}
	if err := validateQuerySemanticsMatrix(querySemanticsMatrix{Cases: []querySemanticsCase{logs}}); err != nil {
		t.Errorf("valid exact log-entry contract rejected: %v", err)
	}
	logs.Compare = "line_count"
	if err := validateQuerySemanticsMatrix(querySemanticsMatrix{Cases: []querySemanticsCase{logs}}); err == nil {
		t.Error("weak line-count comparison accepted for a log case")
	}
}

func TestQuerySemanticsLogEntries(t *testing.T) {
	response := func(entries ...string) string {
		return `{"data":{"result":[` + strings.Join(entries, ",") + `]}}`
	}
	first := `{"stream":{"app":"api","level":"info"},"values":[["1700000000000000001","same",{"trace_id":"one"}],["1700000000000000001","same",{"trace_id":"one"}]]}`
	reordered := `{"stream":{"level":"info","app":"api"},"values":[["1700000000000000001","same",{"trace_id":"one"}],["1700000000000000001","same",{"trace_id":"one"}]]}`
	want, err := querySemanticsLogEntries(response(first))
	if err != nil {
		t.Fatal(err)
	}
	got, err := querySemanticsLogEntries(response(reordered))
	if err != nil || !querySemanticsSameLogEntries(want, got) {
		t.Fatalf("reordered equivalent streams differ: %v, %v", got, err)
	}
	for _, changed := range []string{
		strings.Replace(first, `"level":"info"`, `"level":"error"`, 1),
		strings.Replace(first, `1700000000000000001`, `1700000000000000002`, 1),
		strings.Replace(first, `"same"`, `"changed"`, 1),
		strings.Replace(first, `"one"`, `"two"`, 1),
		strings.Replace(first, `,["1700000000000000001","same",{"trace_id":"one"}]`, ``, 1),
	} {
		other, err := querySemanticsLogEntries(response(changed))
		if err != nil {
			t.Fatal(err)
		}
		if querySemanticsSameLogEntries(want, other) {
			t.Fatalf("different log entry accepted: %s", changed)
		}
	}
	for _, malformed := range []string{`{"data":{"result":[{"stream":{},"values":[[123,"line"]]}]}}`, `{"data":{"result":[{"stream":{},"values":[["123"]]}]}}`} {
		if _, err := querySemanticsLogEntries(malformed); err == nil {
			t.Fatalf("malformed log entry accepted: %s", malformed)
		}
	}
}

func TestQuerySemanticsMatrixRequestAxis(t *testing.T) {
	evaluation := time.Unix(1700000000, 123456789)
	for _, endpoint := range []string{"query", "query_range"} {
		t.Run(endpoint, func(t *testing.T) {
			tc := querySemanticsCase{Endpoint: endpoint, Query: `rate({app="test"}[5m])`, Step: "60"}
			params := querySemanticsParams(tc, evaluation)
			if params.Get("query") != tc.Query {
				t.Fatalf("query changed: %v", params)
			}
			if endpoint == "query_range" {
				if params.Get("start") != "1699999400123456789" || params.Get("end") != "1700000000123456789" || params.Get("step") != "60" || params.Get("limit") != "1000" || params.Get("time") != "" {
					t.Fatalf("wrong shared range coordinates: %v", params)
				}
			} else if params.Get("time") != "1700000000123456789" || params.Get("start") != "" || params.Get("end") != "" {
				t.Fatalf("wrong shared instant coordinate: %v", params)
			}
		})
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

func TestQuerySemanticsPerformanceCatalog(t *testing.T) {
	matrix := loadQuerySemanticsMatrix(t)
	cases := validParityCases(t)
	if len(cases) != len(matrix.PerformanceCases) {
		t.Fatalf("resolved %d performance cases, want %d", len(cases), len(matrix.PerformanceCases))
	}
	seen := make(map[string]bool, len(cases))
	for _, tc := range cases {
		if tc.name == "" || tc.category == "" || tc.query == "" || seen[tc.name] {
			t.Fatalf("invalid resolved performance case %+v", tc)
		}
		seen[tc.name] = true
	}
	for _, id := range []string{"exact_match", "sum_by", "avg_over_time_unwrap", "vector_div", "json_two_field_filters"} {
		if !seen[id] {
			t.Errorf("focused benchmark case %q is missing", id)
		}
	}
}

func TestQuerySemanticsPerformanceLogCasesHaveStrictQualityContracts(t *testing.T) {
	matrix := loadQuerySemanticsMatrix(t)
	quality := make(map[string]querySemanticsCase, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		quality[tc.ID] = tc
	}
	performance := make(map[string]querySemanticsPerformanceCase, len(matrix.PerformanceCases))
	for _, tc := range matrix.PerformanceCases {
		performance[tc.ID] = tc
	}
	for _, id := range []string{"exact_match", "regex_match", "not_equal", "regex_not_match", "line_contains", "line_not_contains", "line_regex", "line_not_regex"} {
		perf := performance[id]
		contract, ok := quality[perf.CaseID]
		if !ok || perf.Query != "" || contract.Query == "" || contract.Compare != "log_entries" {
			t.Errorf("performance scenario %q lacks a shared strict log-entry contract", id)
		}
	}
}

func TestQuerySemanticsMatrixDepthPaths(t *testing.T) {
	inventory := loadQuerySemanticsOperations(t)
	matrix := loadQuerySemanticsMatrix(t)
	caseIDs := make(map[string]struct{}, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		caseIDs[tc.ID] = struct{}{}
	}
	matrixTests, err := os.ReadFile("query_semantics_matrix_test.go")
	if err != nil {
		t.Fatalf("read live query semantics test source: %v", err)
	}
	parserErrorTests, err := os.ReadFile("parser_error_matrix_test.go")
	if err != nil {
		t.Fatalf("read nested parser test source: %v", err)
	}
	liveTests := string(matrixTests) + string(parserErrorTests)
	seenDepths := make(map[int]bool, 3)
	seenPaths := make(map[string]bool, len(inventory.DepthPaths))
	seenParserDepths := make(map[string]bool, len(inventory.DepthPaths))
	wantStages := map[int]string{
		1: "selector,parser,parsed_field_filter",
		2: "selector,parser,parsed_field_filter,range_function",
		3: "selector,parser,parsed_field_filter,range_function,aggregation",
	}
	for _, path := range inventory.DepthPaths {
		if path.ID == "" || seenPaths[path.ID] {
			t.Fatalf("missing or duplicate depth path id %q", path.ID)
		}
		seenPaths[path.ID] = true
		if path.Depth < 1 || path.Depth > 3 {
			t.Fatalf("depth path %q has unsupported depth %d", path.ID, path.Depth)
		}
		seenDepths[path.Depth] = true
		parserDepth := fmt.Sprintf("%s:%d", path.Parser, path.Depth)
		if seenParserDepths[parserDepth] {
			t.Fatalf("duplicate parser/depth path %q", parserDepth)
		}
		seenParserDepths[parserDepth] = true
		if strings.Join(path.Stages, ",") != wantStages[path.Depth] || len(path.MatrixCases) == 0 || (len(path.RegistryItems) == 0 && len(path.RegistryCases) == 0) {
			t.Fatalf("depth path %q must declare its stages, matrix cases, and registry proof", path.ID)
		}
		for _, id := range path.MatrixCases {
			if _, ok := caseIDs[id]; !ok {
				t.Fatalf("depth path %q references unknown query semantics case %q", path.ID, id)
			}
		}
		for _, id := range path.RegistryItems {
			registryFile := "../../conformance/registry/" + id + ".yaml"
			contents, err := os.ReadFile(registryFile)
			if err != nil {
				t.Fatalf("depth path %q references missing registry item %q: %v", path.ID, id, err)
			}
			registryID := id[strings.LastIndex(id, "/")+1:]
			if !strings.Contains(string(contents), "id: "+registryID) {
				t.Fatalf("depth path %q registry file %q declares another id", path.ID, registryFile)
			}
		}
		for _, id := range path.RegistryCases {
			registryFile := "../../conformance/registry/cases/" + id + ".yaml"
			contents, err := os.ReadFile(registryFile)
			if err != nil {
				t.Fatalf("depth path %q references missing registry case %q: %v", path.ID, id, err)
			}
			if !strings.Contains(string(contents), "id: "+id) {
				t.Fatalf("depth path %q registry file %q declares another id", path.ID, id)
			}
		}
		if path.LiveTest == "" || !strings.Contains(liveTests, "func "+path.LiveTest+"(") {
			t.Fatalf("depth path %q references missing live Loki/proxy test %q", path.ID, path.LiveTest)
		}
		if path.RecordedEvidence != "" {
			if _, err := os.Stat(path.RecordedEvidence); err != nil {
				t.Fatalf("depth path %q references missing retained evidence %q: %v", path.ID, path.RecordedEvidence, err)
			}
		}
		for _, name := range path.RelatedTests {
			if name == path.LiveTest || !strings.HasPrefix(name, "Test") {
				t.Fatalf("depth path %q has redundant or invalid related test %q", path.ID, name)
			}
			matches, err := filepath.Glob("*_test.go")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, file := range matches {
				contents, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(contents), "func "+name+"(") {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("depth path %q references missing related test %q", path.ID, name)
			}
		}
	}
	for depth := 1; depth <= 3; depth++ {
		if !seenDepths[depth] {
			t.Errorf("depth inventory does not cover depth %d", depth)
		}
	}
}

func TestQuerySemanticsMatrixCoversParserMetricDepths(t *testing.T) {
	inventory := loadQuerySemanticsOperations(t)
	matrix := loadQuerySemanticsMatrix(t)
	cases := make(map[string]querySemanticsCase, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		cases[tc.ID] = tc
	}

	wantParsers := []string{"json", "logfmt", "pattern", "regexp"}
	covered := make(map[string]bool, len(wantParsers)*3)
	for _, path := range inventory.DepthPaths {
		if path.Parser == "" {
			t.Errorf("depth path %q does not identify its parser", path.ID)
			continue
		}
		key := fmt.Sprintf("%s:%d", path.Parser, path.Depth)
		if covered[key] {
			t.Errorf("duplicate parser/depth path %q", key)
		}
		covered[key] = true
		for _, caseID := range path.MatrixCases {
			tc, ok := cases[caseID]
			if !ok {
				t.Errorf("depth path %q references unknown matrix case %q", path.ID, caseID)
				continue
			}
			if !strings.Contains(tc.Query, "| "+path.Parser) {
				t.Errorf("depth path %q case %q does not exercise the declared %s parser", path.ID, caseID, path.Parser)
			}
			if path.Depth >= 2 && (tc.ExpectResultType != "matrix" || !strings.Contains(tc.Query, "[5m]")) {
				t.Errorf("depth %d path %q case %q must exercise a range metric", path.Depth, path.ID, caseID)
			}
			if path.Depth == 3 && !strings.Contains(tc.Query, "sum by (") {
				t.Errorf("depth 3 path %q case %q must exercise an outer aggregation", path.ID, caseID)
			}
		}
	}
	for _, parser := range wantParsers {
		for depth := 1; depth <= 3; depth++ {
			if !covered[fmt.Sprintf("%s:%d", parser, depth)] {
				t.Errorf("query semantics inventory does not cover parser %q at depth %d", parser, depth)
			}
		}
	}
}

// conformance: loki_api_v1_query, loki_api_v1_query_range, range_function-count-over-time, range_function-rate, parser-json, parser-logfmt, parser-pattern, parser-regexp
func TestQuerySemanticsMatrix(t *testing.T) {
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, lokiURL+"/ready", 30*time.Second)
	// Use a unique canonical numeric tenant. The local stack enables numeric
	// passthrough (AccountID=<OrgID>, ProjectID=0), so this isolates the matrix
	// without changing its tenant-map configuration or shared A/B data.
	tenant := isolatedNumericTenantID(t)
	t.Logf("using isolated numeric tenant %s for query semantics fixtures", tenant)
	ingestionAnchor = time.Now().Add(-3 * time.Minute)
	ingestRichTestDataForTenant(t, tenant, ingestionAnchor)
	forceVLFlush(t)
	waitForLokiMetricDataSelectorForTenant(t, `{app="api-gateway"}`, tenant)
	matrix := loadQuerySemanticsMatrix(t)
	// Keep fixtures inside their five-minute metric windows even if setup or
	// preceding shard tests took a long time. Minute alignment also matches
	// the reference stack's align_queries_with_step profile.
	evaluation := ingestionAnchor.Add(2 * time.Minute).Truncate(time.Minute)

	for _, tc := range matrix.Cases {
		tc := tc
		t.Run(tc.ID, func(t *testing.T) {
			params := querySemanticsParams(tc, evaluation)
			query, err := scopeQuerySemanticsQuery(params.Get("query"), tenant)
			if err != nil && tc.Expectation == "success" {
				t.Fatalf("scope query to matrix fixtures: %v", err)
			}
			if err == nil {
				params.Set("query", query)
			}
			t.Logf("matrix_case id=%s comparison=%s known_deviation=%s", tc.ID, tc.Compare, tc.KnownDeviation)
			proxyStatus, proxyBody, proxyResp := querySemanticsGET(t, proxyURL, tc.Endpoint, params, tenant)
			lokiStatus, lokiBody, lokiResp := querySemanticsGET(t, lokiURL, tc.Endpoint, params, tenant)
			if tc.ExpectResultType != "streams" {
				t.Log("matrix_backend proxy")
				recordParserMatrixResponse(t, params, proxyStatus, []byte(proxyBody))
				t.Log("matrix_backend loki")
				recordParserMatrixResponse(t, params, lokiStatus, []byte(lokiBody))
			}

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
				case "metric_points":
					responses := make([]map[string]map[string]string, 0, 2)
					for _, backend := range []struct{ name, body string }{{"Loki", lokiBody}, {"proxy", proxyBody}} {
						var response map[string]interface{}
						decoder := json.NewDecoder(strings.NewReader(backend.body))
						decoder.UseNumber()
						if err := decoder.Decode(&response); err != nil {
							t.Fatalf("%s metric JSON: %v", backend.name, err)
						}
						if warnings, ok := response["warnings"].([]interface{}); ok && len(warnings) != 0 {
							t.Fatalf("%s returned partial metric data with warnings: %v", backend.name, warnings)
						}
						samples, err := metricparity.DecodeSamples(response)
						if err != nil {
							t.Fatalf("%s metric samples: %v", backend.name, err)
						}
						responses = append(responses, samples)
					}
					if err := metricparity.Difference(responses[0], responses[1], tc.RequireNonEmpty); err != nil {
						t.Fatalf("%s: %v", tc.ID, err)
					}
				case "log_entries":
					proxyEntries, err := querySemanticsLogEntries(proxyBody)
					if err != nil {
						t.Fatalf("proxy log entries for %s: %v", tc.ID, err)
					}
					lokiEntries, err := querySemanticsLogEntries(lokiBody)
					if err != nil {
						t.Fatalf("Loki log entries for %s: %v", tc.ID, err)
					}
					if tc.RequireNonEmpty && (len(proxyEntries) == 0 || len(lokiEntries) == 0) {
						t.Fatalf("expected non-empty logs for %s", tc.ID)
					}
					if !querySemanticsSameLogEntries(proxyEntries, lokiEntries) {
						t.Fatalf("log-entry mismatch for %s: proxy distinct=%d Loki distinct=%d", tc.ID, len(proxyEntries), len(lokiEntries))
					}
				case "series_count":
					proxyCount := querySemanticsResultCount(proxyResp)
					lokiCount := querySemanticsResultCount(lokiResp)
					if tc.RequireNonEmpty && (proxyCount == 0 || lokiCount == 0) {
						t.Fatalf("expected non-empty result series for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
					}
					if proxyCount != lokiCount {
						proxyKeys := querySemanticsMetricKeySet(proxyResp)
						lokiKeys := querySemanticsMetricKeySet(lokiResp)
						t.Fatalf("series-count mismatch for %s, proxy=%d loki=%d\n  proxy series: %v\n  loki  series: %v", tc.ID, proxyCount, lokiCount, proxyKeys, lokiKeys)
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
				case "any_non_empty":
					// Verifies both backends return results without comparing cardinality.
					// Used for queries where VL groups by stream labels only (stats by
					// (_stream)) while Loki extracts JSON fields as additional label
					// dimensions — an inherent architectural difference, not a bug.
					proxyCount := querySemanticsResultCount(proxyResp)
					lokiCount := querySemanticsResultCount(lokiResp)
					if tc.RequireNonEmpty && (proxyCount == 0 || lokiCount == 0) {
						t.Fatalf("expected non-empty results for %s, proxy=%d loki=%d", tc.ID, proxyCount, lokiCount)
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
