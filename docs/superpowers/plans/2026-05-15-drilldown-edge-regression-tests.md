# Drilldown Multi-Tenant Metadata & Edge Case Regression Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add missing Go e2e-compat tests for (1) multi-tenant `volume_range` and field cardinality accuracy in Drilldown, and (2) regression tests for edge cases including the `| drop __error__` instant-query path and multi-stage pipeline `detected_fields` parity.

**Architecture:** All new tests live in the existing `test/e2e-compat/` package alongside their related files. Multi-tenant Drilldown tests extend `TestDrilldown_GrafanaResourceContracts` as additional `t.Run` subtests. Edge case regressions extend `TestSetup_IngestEdgeCaseData` (data setup) and add new `TestEdge_*` functions. No new files needed — follow existing patterns.

**Tech Stack:** Go `testing` package, build tag `e2e`, HTTP test helpers (`getJSON`, `getJSONWithHeaders`, `pushStream`) already defined in `test/e2e-compat/compat_test.go` and `testdata.go`. Run with `go test -v -tags=e2e -timeout=180s ./test/e2e-compat/ -run <TestName>`.

---

## Context for the implementer

**Key URLs (from `test/e2e-compat/compat_test.go`):**
- `proxyURL` = `http://localhost:13100`
- `lokiURL` = `http://localhost:13101`
- `grafanaURL` = `http://localhost:3002` (defined in `drilldown_compat_test.go:16`)
- `vlURL` = `http://localhost:19428`

**Multi-tenant setup:** The `"Loki (via VL proxy multi-tenant)"` datasource is pre-configured in Grafana. The multi-tenant orgID used in tests is `"0|fake"` — two tenants: `"0"` (default VL tenant 0:0) and `"fake"` (also maps to VL 0:0 in the default config, sharing all data). The `__tenant_id__` synthetic label is injected by the proxy into multi-tenant responses.

**Key helpers:**
```go
// Get datasource UID by name
multiUID := grafanaDatasourceUID(t, "Loki (via VL proxy multi-tenant)")

// GET JSON via Grafana datasource resource proxy
resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume_range?"+params.Encode())

// GET JSON with custom headers
resp := getJSONWithHeaders(t, proxyURL+"/loki/api/v1/detected_fields?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0|fake"})

// Extract helpers
data := extractMap(resp, "data")          // resp["data"].(map[string]interface{})
result := extractArray(data, "result")    // data["result"].([]interface{})
fields, _ := resp["fields"].([]interface{})
```

**Existing test to extend:** `TestDrilldown_GrafanaResourceContracts` in `test/e2e-compat/drilldown_compat_test.go` — all new multi-tenant subtests go here as additional `t.Run(...)` blocks, using the already-declared `multiUID` variable.

---

## Task 1: Multi-tenant `volume_range` returns time-series data with `__tenant_id__` labels

The `index/volume` endpoint is tested for multi-tenant but `index/volume_range` (time-series graph) has zero multi-tenant coverage. Drilldown uses `volume_range` for the log volume graph. Regression: the `volume_range` multi-tenant path must inject `__tenant_id__` labels and return non-empty matrix results.

**Files:**
- Modify: `test/e2e-compat/drilldown_compat_test.go` — add `t.Run` block inside `TestDrilldown_GrafanaResourceContracts`, after the existing `multi_tenant_missing_tenant_keeps_empty_success_shape` subtest (~line 955)

- [ ] **Step 1: Locate insertion point**

Open `test/e2e-compat/drilldown_compat_test.go`. Find the end of the `multi_tenant_missing_tenant_keeps_empty_success_shape` subtest (around line 950). The new subtest goes immediately after its closing `})`.

- [ ] **Step 2: Add the `volume_range` multi-tenant subtest**

Insert after `multi_tenant_missing_tenant_keeps_empty_success_shape`:

```go
	t.Run("multi_tenant_volume_range_returns_level_series_with_tenant_labels", func(t *testing.T) {
		params := url.Values{}
		params.Set("query", `{app="api-gateway",__tenant_id__="fake"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60")
		params.Set("targetLabels", "detected_level")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume_range?"+params.Encode())
		data := extractMap(resp, "data")
		if data == nil {
			t.Fatalf("expected data envelope in multi-tenant volume_range, got %v", resp)
		}
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected non-empty result array for multi-tenant volume_range, got %v", resp)
		}
		// Every series must have a non-empty values array (matrix type)
		for i, item := range result {
			obj, ok := item.(map[string]interface{})
			if !ok {
				t.Fatalf("result[%d] is not an object: %v", i, item)
			}
			values, _ := obj["values"].([]interface{})
			if len(values) == 0 {
				t.Fatalf("result[%d] has no values (expected matrix data points): %v", i, obj)
			}
		}
	})

	t.Run("multi_tenant_volume_range_without_tenant_filter_injects_tenant_id_label", func(t *testing.T) {
		// Without __tenant_id__ filter, merged multi-tenant volume_range must
		// include __tenant_id__ in each series metric so Drilldown can split by tenant.
		params := url.Values{}
		params.Set("query", `{app="api-gateway"}`)
		params.Set("start", start)
		params.Set("end", end)
		params.Set("step", "60")
		params.Set("targetLabels", "__tenant_id__")

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/index/volume_range?"+params.Encode())
		data := extractMap(resp, "data")
		if data == nil {
			t.Fatalf("expected data envelope in multi-tenant volume_range without tenant filter, got %v", resp)
		}
		result := extractArray(data, "result")
		if len(result) == 0 {
			t.Fatalf("expected non-empty result for multi-tenant volume_range without filter, got %v", resp)
		}
		seenTenants := map[string]bool{}
		for _, item := range result {
			metric := item.(map[string]interface{})["metric"].(map[string]interface{})
			if tid, ok := metric["__tenant_id__"].(string); ok && tid != "" {
				seenTenants[tid] = true
			}
		}
		if len(seenTenants) == 0 {
			t.Fatalf("expected __tenant_id__ label in multi-tenant volume_range metric labels, got series: %v", result)
		}
	})
```

- [ ] **Step 3: Run only the new subtests**

```bash
cd test/e2e-compat
go test -v -tags=e2e -timeout=120s ./... -run 'TestDrilldown_GrafanaResourceContracts/multi_tenant_volume_range'
```

Expected: Both subtests PASS. If `volume_range` multi-tenant fanout is not yet implemented in the proxy, they will fail with "expected non-empty result" — that failure is itself the regression catch.

- [ ] **Step 4: Commit**

```bash
git add test/e2e-compat/drilldown_compat_test.go
git commit -m "test(e2e): add multi-tenant volume_range cardinality regression tests"
```

---

## Task 2: Multi-tenant `detected_fields` cardinality value accuracy

Current test only checks cardinality is non-zero. Add a test that verifies the cardinality value is ≥ the known number of distinct values for a field in the test dataset. For `method` in `api-gateway` logs (see `testdata.go`): GET, POST, DELETE = 3 distinct values.

**Files:**
- Modify: `test/e2e-compat/drilldown_compat_test.go` — add subtest after `multi_tenant_volume_range_*` subtests from Task 1

- [ ] **Step 1: Add the cardinality accuracy subtest**

Insert after the two subtests from Task 1:

```go
	t.Run("multi_tenant_detected_fields_cardinality_matches_distinct_values", func(t *testing.T) {
		// api-gateway logs contain GET, POST, DELETE methods (3 distinct values).
		// The cardinality value in detected_fields must be >= 3.
		params := url.Values{}
		params.Set("query", `{app="api-gateway",__tenant_id__="fake"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/detected_fields?"+params.Encode())
		fields, _ := resp["fields"].([]interface{})
		if len(fields) == 0 {
			t.Fatalf("expected fields in multi-tenant detected_fields cardinality test, got %v", resp)
		}

		var methodCard int
		for _, item := range fields {
			field := item.(map[string]interface{})
			if field["label"] != "method" {
				continue
			}
			if card, ok := field["cardinality"].(float64); ok {
				methodCard = int(card)
			}
		}
		// GET, POST, DELETE = at least 3 distinct HTTP methods in api-gateway test data
		if methodCard < 3 {
			t.Fatalf("expected method cardinality >= 3 (GET/POST/DELETE), got %d in response: %v", methodCard, resp)
		}
	})

	t.Run("multi_tenant_detected_labels_cardinality_field_is_present_and_accurate", func(t *testing.T) {
		// detected_labels response includes cardinality per label. Verify
		// the "app" label has cardinality >= 1 (at least api-gateway).
		params := url.Values{}
		params.Set("query", `{__tenant_id__="fake"}`)
		params.Set("start", start)
		params.Set("end", end)

		resp := getJSON(t, grafanaURL+"/api/datasources/uid/"+multiUID+"/resources/detected_labels?"+params.Encode())
		detectedLabels, _ := resp["detectedLabels"].([]interface{})
		if len(detectedLabels) == 0 {
			t.Fatalf("expected detectedLabels array, got %v", resp)
		}
		var appCard int
		for _, item := range detectedLabels {
			obj := item.(map[string]interface{})
			if obj["label"] != "app" {
				continue
			}
			if card, ok := obj["cardinality"].(float64); ok {
				appCard = int(card)
			}
		}
		if appCard < 1 {
			t.Fatalf("expected app label cardinality >= 1 in multi-tenant detected_labels, got %d in: %v", appCard, resp)
		}
	})
```

- [ ] **Step 2: Run the new subtests**

```bash
go test -v -tags=e2e -timeout=120s ./... -run 'TestDrilldown_GrafanaResourceContracts/multi_tenant_detected_fields_cardinality|multi_tenant_detected_labels_cardinality'
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/e2e-compat/drilldown_compat_test.go
git commit -m "test(e2e): verify multi-tenant detected_fields/labels cardinality values are accurate"
```

---

## Task 3: Regression test for `| drop __error__` instant-query path (#370 fix)

PR #370 fixed a bug where `sum(count_over_time({...} | json | logfmt | drop __error__, __error_details__ [interval]))` as an **instant** query was routed to the manual log-fetch path (per-stream) instead of VL native `stats_query` (single aggregated result). Without the fix the proxy returns multiple per-stream series; with the fix it returns a single `{metric:{}}` result.

**Files:**
- Modify: `test/e2e-compat/edgecase_test.go` — add a new `TestEdge_*` function

- [ ] **Step 1: Add ingestion for the drop-error test data**

In `TestSetup_IngestEdgeCaseData` (around line 140 in `edgecase_test.go`), add a new stream push at the end of the function:

```go
	// 14. Drop-error instant-query path (regression for #370)
	pushStream(t, now, streamDef{
		Labels: map[string]string{
			"app": "edge-drop-error", "namespace": "edge-tests", "level": "info",
		},
		Lines: []string{
			`{"msg":"request processed","method":"GET","status":200}`,
			`{"msg":"request processed","method":"POST","status":201}`,
			`{"msg":"request failed","method":"POST","status":500,"error":"timeout"}`,
		},
	})
```

- [ ] **Step 2: Run setup to confirm data ingests**

```bash
go test -v -tags=e2e -timeout=60s ./... -run 'TestSetup_IngestEdgeCaseData'
```

Expected: PASS (no ingestion errors logged).

- [ ] **Step 3: Add the regression test function**

Add after the last `TestEdge_*` function in `test/e2e-compat/edgecase_test.go`:

```go
// TestEdge_DropErrorInstantQueryReturnsAggregatedResult is a regression test for
// the fix in #370 where sum(count_over_time(... | drop __error__)) as an instant
// query was routed to the manual log-fetch path (per-stream) instead of VL native
// stats_query (single aggregated result). The proxy must return exactly one result
// series with an empty metric label set, not one per log stream.
func TestEdge_DropErrorInstantQueryReturnsAggregatedResult(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()

	expr := `sum(count_over_time({app="edge-drop-error"} | json | logfmt | drop __error__, __error_details__ [5m]))`
	params := url.Values{}
	params.Set("query", expr)
	params.Set("time", fmt.Sprintf("%d", now.UnixNano()))

	resp, err := http.Get(proxyURL + "/loki/api/v1/query?" + params.Encode())
	if err != nil {
		t.Fatalf("instant query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var decoded struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if decoded.Status != "success" {
		t.Fatalf("expected status=success, got %q", decoded.Status)
	}
	if decoded.Data.ResultType != "vector" {
		t.Fatalf("expected resultType=vector for instant query, got %q", decoded.Data.ResultType)
	}
	// sum() must aggregate to exactly one result with an empty metric
	if len(decoded.Data.Result) != 1 {
		t.Fatalf("expected exactly 1 aggregated result from sum(count_over_time(...)), got %d results — proxy is returning per-stream series instead of aggregating", len(decoded.Data.Result))
	}
	if len(decoded.Data.Result[0].Metric) != 0 {
		t.Fatalf("expected empty metric label set for sum() result, got %v", decoded.Data.Result[0].Metric)
	}
	// Value must be numeric and > 0
	if len(decoded.Data.Result[0].Value) < 2 {
		t.Fatalf("expected [timestamp, value] in result, got %v", decoded.Data.Result[0].Value)
	}
}
```

- [ ] **Step 4: Run the regression test**

```bash
go test -v -tags=e2e -timeout=120s ./... -run 'TestEdge_DropErrorInstantQueryReturnsAggregatedResult'
```

Expected: PASS. If it fails with "got 3 results", the #370 regression has re-appeared.

- [ ] **Step 5: Commit**

```bash
git add test/e2e-compat/edgecase_test.go
git commit -m "test(e2e): add regression test for | drop __error__ instant-query aggregation (#370)"
```

---

## Task 4: `detected_fields` parity after `| json | drop __error__` pipeline

After a multi-stage pipeline that parses JSON and drops `__error__`/`__error_details__`, `detected_fields` must still return parsed JSON field names (not return empty or miss fields). This tests the interaction between pipeline stages and the detected_fields metadata endpoint.

**Files:**
- Modify: `test/e2e-compat/edgecase_test.go` — add new `TestEdge_*` function

- [ ] **Step 1: Add the test**

Add after `TestEdge_DropErrorInstantQueryReturnsAggregatedResult`:

```go
// TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields verifies that
// detected_fields returns parsed JSON field names even when the query selector
// includes a multi-stage pipeline (| json | drop __error__, __error_details__).
// Drilldown sends detected_fields requests with the current pipeline expression;
// the proxy must strip the pipeline for the metadata call but still use the
// stream selector to scope results.
func TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields(t *testing.T) {
	ensureDataIngested(t)
	now := time.Now()

	// Query with full pipeline — proxy must strip pipeline for detected_fields
	params := url.Values{}
	params.Set("query", `{app="edge-drop-error"} | json | drop __error__, __error_details__`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-15*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	resp, err := http.Get(proxyURL + "/loki/api/v1/detected_fields?" + params.Encode())
	if err != nil {
		t.Fatalf("detected_fields request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var decoded struct {
		Fields []struct {
			Label       string  `json:"label"`
			Type        string  `json:"type"`
			Cardinality float64 `json:"cardinality"`
		} `json:"fields"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("failed to decode detected_fields response: %v", err)
	}

	seenFields := map[string]bool{}
	for _, f := range decoded.Fields {
		seenFields[f.Label] = true
	}

	// The edge-drop-error stream pushed JSON with "msg", "method", "status" fields.
	// All three must appear in detected_fields despite the | json | drop pipeline.
	for _, want := range []string{"msg", "method", "status"} {
		if !seenFields[want] {
			t.Fatalf("expected detected_fields to include %q after | json | drop pipeline, got: %v", want, seenFields)
		}
	}
	// __error__ and __error_details__ must NOT appear (they are internal VL fields)
	for _, forbidden := range []string{"__error__", "__error_details__"} {
		if seenFields[forbidden] {
			t.Fatalf("detected_fields must not expose %q (internal VL field), got: %v", forbidden, seenFields)
		}
	}
}
```

- [ ] **Step 2: Run the test**

```bash
go test -v -tags=e2e -timeout=120s ./... -run 'TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields'
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/e2e-compat/edgecase_test.go
git commit -m "test(e2e): verify detected_fields returns json fields after | json | drop pipeline"
```

---

## Task 5: Wire new tests into CI semantics group

The two new `TestEdge_*` functions must appear in the `semantics` CI group pattern so they run in the `e2e-compat (semantics)` CI job.

**Files:**
- Modify: `.github/workflows/ci.yaml`

- [ ] **Step 1: Locate the semantics group pattern**

In `.github/workflows/ci.yaml`, search for `semantics` in the `e2e-compat` matrix. The pattern looks like:

```yaml
- name: semantics
  pattern: '^(TestSetup_IngestLogs|...|TestLogQL_Exhaustive_.*|TestPipeline_.*)'
```

- [ ] **Step 2: Extend the pattern**

Add `TestEdge_DropErrorInstantQueryReturnsAggregatedResult|TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields` to the regex OR list in the semantics pattern. Keep alphabetical order within the group. The result should include:

```
TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields|TestEdge_DropErrorInstantQueryReturnsAggregatedResult
```

Add these before or after the existing `TestLogQL_Exhaustive_.*|TestPipeline_.*` entries.

- [ ] **Step 3: Verify the YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yaml'))" && echo "YAML valid"
```

Expected: `YAML valid`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yaml
git commit -m "ci: wire new edge-case regression tests into semantics e2e-compat group"
```

---

## Task 6: Add CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add entry under `[Unreleased]`**

```markdown
### Tests

- **e2e: multi-tenant `volume_range` regression tests**: verify `index/volume_range` returns time-series data with `__tenant_id__` labels in multi-tenant Drilldown context; cover both tenant-filtered and unfiltered cases.
- **e2e: multi-tenant field/label cardinality accuracy**: verify `detected_fields` cardinality ≥ known distinct values (GET/POST/DELETE for `method`); verify `detected_labels` includes `cardinality` field with value ≥ 1.
- **e2e: `| drop __error__` instant-query regression (#370)**: `sum(count_over_time(... | drop __error__))` as instant query must return exactly one aggregated result, not one per stream.
- **e2e: `detected_fields` after multi-stage pipeline**: `| json | drop __error__` pipeline must not suppress parsed JSON field names in `detected_fields` response.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: add changelog entries for drilldown and edge case regression tests"
```

---

## Self-Review

**Spec coverage:**
- ✅ `volume_range` multi-tenant: Tasks 1
- ✅ Cardinality value accuracy: Task 2
- ✅ `| drop __error__` instant-query regression: Task 3
- ✅ `detected_fields` after multi-stage pipeline: Task 4
- ✅ CI wiring: Task 5
- ✅ CHANGELOG: Task 6

**No placeholders:** All code is complete and runnable.

**Type consistency:** `getJSON`, `grafanaDatasourceUID`, `extractMap`, `extractArray`, `pushStream`, `ensureDataIngested` all match existing function signatures in the codebase. `json.NewDecoder` decode patterns match `edgecase_test.go:379` (PostQueryRange test).
