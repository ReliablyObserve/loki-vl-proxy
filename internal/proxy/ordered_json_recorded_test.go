package proxy

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/metricparity"
)

const recordedParserMatrixDir = "../../test/e2e-compat/testdata/parser-error-matrix/2026-11-01-exact-fence"

type recordedParserCase struct {
	Case     string          `json:"case"`
	Params   url.Values      `json:"params"`
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"`
}

type recordedParserManifest struct {
	SchemaVersion int               `json:"schema_version"`
	CaseCount     int               `json:"case_count_per_source"`
	SHA256        map[string]string `json:"sha256"`
	Fixture       struct {
		Rows []struct{ Cluster, Kind, Line string } `json:"rows"`
	} `json:"fixture"`
}

func recordedParserInputs(t *testing.T) ([]byte, []byte) {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(recordedParserMatrixDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(filepath.Join(recordedParserMatrixDir, "pr.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	return manifest, archive
}

func decodeRecordedParserMatrix(manifest, archive []byte) (recordedParserManifest, []recordedParserCase, error) {
	var metadata recordedParserManifest
	if err := json.Unmarshal(manifest, &metadata); err != nil {
		return metadata, nil, err
	}
	if metadata.SchemaVersion != 1 || metadata.CaseCount != 132 || len(metadata.Fixture.Rows) != 5 {
		return metadata, nil, fmt.Errorf("unsupported or incomplete parser matrix manifest")
	}
	if fmt.Sprintf("%x", sha256.Sum256(archive)) != metadata.SHA256["pr.json.gz"] {
		return metadata, nil, fmt.Errorf("parser matrix checksum mismatch")
	}
	reader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return metadata, nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil || len(raw) == 1<<20 {
		return metadata, nil, fmt.Errorf("invalid or oversized parser matrix: %v", err)
	}
	var records []recordedParserCase
	if err := json.Unmarshal(raw, &records); err != nil {
		return metadata, nil, err
	}
	if len(records) != 2*metadata.CaseCount {
		return metadata, nil, fmt.Errorf("incomplete parser matrix records: %d", len(records))
	}
	seen := map[string]recordedParserCase{}
	var references []recordedParserCase
	var evaluation, service string
	for _, record := range records {
		parts := strings.Split(record.Case, "/")
		if len(parts) < 4 || parts[0] != "TestChaining_ParserErrorNestedAggregations" {
			return metadata, nil, fmt.Errorf("invalid recorded case: %s", record.Case)
		}
		mode, backend := parts[len(parts)-2], parts[len(parts)-1]
		if (mode != "query" && mode != "query_range") || (backend != "loki" && backend != "proxy") {
			return metadata, nil, fmt.Errorf("invalid recorded endpoint/backend: %s", record.Case)
		}
		if _, exists := seen[record.Case]; exists {
			return metadata, nil, fmt.Errorf("duplicate recorded case: %s", record.Case)
		}
		seen[record.Case] = record
		axis, err := recordedParserAxis(record.Params, mode)
		if err != nil {
			return metadata, nil, fmt.Errorf("%s: %w", record.Case, err)
		}
		if evaluation != "" && axis != evaluation {
			return metadata, nil, fmt.Errorf("recorded fixture axis changed: %s", record.Case)
		}
		evaluation = axis
		name := recordedParserService(record.Params.Get("query"))
		if name == "" || (service != "" && name != service) {
			return metadata, nil, fmt.Errorf("recorded fixture service changed: %s", record.Case)
		}
		service = name
		if record.Status != 200 && record.Status != 400 {
			return metadata, nil, fmt.Errorf("unusable reference status: %s", record.Case)
		}
		if backend == "loki" {
			references = append(references, record)
		}
	}
	for _, reference := range references {
		peer, exists := seen[strings.TrimSuffix(reference.Case, "loki")+"proxy"]
		if !exists || !reflect.DeepEqual(reference.Params, peer.Params) {
			return metadata, nil, fmt.Errorf("missing or mismatched recorded pair: %s", reference.Case)
		}
	}
	if len(references) != metadata.CaseCount {
		return metadata, nil, fmt.Errorf("incomplete Loki reference set: %d", len(references))
	}
	for _, row := range metadata.Fixture.Rows {
		if row.Cluster == "" || row.Kind == "" || row.Line == "" {
			return metadata, nil, fmt.Errorf("incomplete fixture row")
		}
	}
	return metadata, references, nil
}

var recordedParserServiceRE = regexp.MustCompile(`service_name="(parser-matrix-[0-9]+)"`)

func recordedParserService(query string) string {
	match := recordedParserServiceRE.FindStringSubmatch(query)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func recordedParserAxis(params url.Values, mode string) (string, error) {
	keys := []string{"query", "time"}
	axis := params.Get("time")
	if mode == "query_range" {
		keys, axis = []string{"query", "start", "end", "step"}, params.Get("start")
	}
	if len(params) != len(keys) {
		return "", fmt.Errorf("unexpected request parameters")
	}
	for _, key := range keys {
		if len(params[key]) != 1 || params.Get(key) == "" {
			return "", fmt.Errorf("missing or ambiguous request parameter %s", key)
		}
	}
	at, err := time.Parse(time.RFC3339Nano, axis)
	if err != nil || !at.Equal(at.Truncate(time.Minute)) {
		return "", fmt.Errorf("invalid recorded evaluation coordinate %q", axis)
	}
	if mode == "query_range" && (params.Get("step") != "60" || params.Get("end") != at.Add(time.Minute).Format(time.RFC3339)) {
		return "", fmt.Errorf("invalid recorded range axis")
	}
	return axis, nil
}

func TestOrderedJSONRecordedLokiArchive(t *testing.T) {
	manifest, archive := recordedParserInputs(t)
	metadata, records, err := decodeRecordedParserMatrix(manifest, archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 132 || metadata.CaseCount != 132 || len(metadata.Fixture.Rows) != 5 {
		t.Fatalf("incomplete recorded input: cases=%d declared=%d rows=%d", len(records), metadata.CaseCount, len(metadata.Fixture.Rows))
	}
	for _, record := range records {
		if record.Status != 200 && record.Status != 400 {
			t.Fatalf("unusable reference status: %s: %d", record.Case, record.Status)
		}
	}
}

func TestOrderedJSONRecordedLokiArchiveRejectsBrokenInputs(t *testing.T) {
	manifest, archive := recordedParserInputs(t)
	if _, _, err := decodeRecordedParserMatrix(manifest, append(append([]byte{}, archive...), 0)); err == nil {
		t.Fatal("modified compressed archive accepted")
	}
	reader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	for _, tc := range []struct {
		name string
		edit func(*recordedParserManifest, *[]recordedParserCase)
	}{
		{"duplicate", func(_ *recordedParserManifest, r *[]recordedParserCase) { (*r)[1] = (*r)[0] }},
		{"missing", func(_ *recordedParserManifest, r *[]recordedParserCase) { *r = (*r)[1:] }},
		{"wrong_backend", func(_ *recordedParserManifest, r *[]recordedParserCase) { (*r)[0].Case += "-other" }},
		{"missing_pair", func(_ *recordedParserManifest, r *[]recordedParserCase) {
			(*r)[1].Case = strings.Replace((*r)[1].Case, "/sum_by_cluster/", "/different_case/", 1)
		}},
		{"mismatched_query", func(_ *recordedParserManifest, r *[]recordedParserCase) {
			(*r)[1].Params.Set("query", "max("+(*r)[1].Params.Get("query")+")")
		}},
		{"changed_axis", func(_ *recordedParserManifest, r *[]recordedParserCase) {
			(*r)[0].Params.Set("time", "2026-11-01T11:35:00Z")
		}},
		{"fractional_axis", func(_ *recordedParserManifest, r *[]recordedParserCase) {
			(*r)[0].Params.Set("time", "2026-11-01T11:36:00.5Z")
		}},
		{"duplicate_parameter", func(_ *recordedParserManifest, r *[]recordedParserCase) { (*r)[0].Params.Add("query", "other") }},
		{"extra_parameter", func(_ *recordedParserManifest, r *[]recordedParserCase) { (*r)[0].Params.Set("limit", "1") }},
		{"wrong_status", func(_ *recordedParserManifest, r *[]recordedParserCase) { (*r)[0].Status = 500 }},
		{"missing_rows", func(m *recordedParserManifest, _ *[]recordedParserCase) { m.Fixture.Rows = m.Fixture.Rows[1:] }},
		{"empty_row", func(m *recordedParserManifest, _ *[]recordedParserCase) { m.Fixture.Rows[0].Line = "" }},
		{"wrong_schema", func(m *recordedParserManifest, _ *[]recordedParserCase) { m.SchemaVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var metadata recordedParserManifest
			var records []recordedParserCase
			if err := json.Unmarshal(manifest, &metadata); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &records); err != nil {
				t.Fatal(err)
			}
			tc.edit(&metadata, &records)
			encoded, err := json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			var packed bytes.Buffer
			writer := gzip.NewWriter(&packed)
			if _, err := writer.Write(encoded); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			metadata.SHA256["pr.json.gz"] = fmt.Sprintf("%x", sha256.Sum256(packed.Bytes()))
			updated, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := decodeRecordedParserMatrix(updated, packed.Bytes()); err == nil {
				t.Fatal("broken recording accepted despite valid compressed checksum")
			}
		})
	}
}

func recordedParserDifference(reference recordedParserCase, status int, body []byte) error {
	if status != reference.Status {
		return fmt.Errorf("status %d, recorded Loki %d: %s", status, reference.Status, body)
	}
	if reference.Status == http.StatusBadRequest {
		var expected string
		if err := json.Unmarshal(reference.Response, &expected); err != nil {
			return fmt.Errorf("invalid recorded error: %w", err)
		}
		var actual struct{ Status, ErrorType, Error string }
		if err := json.Unmarshal(body, &actual); err != nil {
			return fmt.Errorf("invalid proxy error envelope: %w", err)
		}
		if actual.Status != "error" || actual.ErrorType != "bad_data" || actual.Error == "" {
			return fmt.Errorf("missing Loki-shaped bad_data error: %s", body)
		}
		if recordedParserDiagnostic(expected) != recordedParserDiagnostic(actual.Error) {
			return fmt.Errorf("error diagnostic differs:\nLoki: %s\nproxy: %s", expected, actual.Error)
		}
		return nil
	}
	responses := make([]map[string]map[string]string, 0, 2)
	resultType := ""
	for _, raw := range [][]byte{reference.Response, body} {
		var response map[string]interface{}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&response); err != nil {
			return err
		}
		if err := decoder.Decode(new(interface{})); err != io.EOF {
			return fmt.Errorf("trailing or malformed metric JSON")
		}
		if response["status"] != "success" {
			return fmt.Errorf("missing success envelope")
		}
		if warnings, exists := response["warnings"]; exists {
			values, ok := warnings.([]interface{})
			if !ok || len(values) != 0 {
				return fmt.Errorf("partial or malformed metric warnings: %v", warnings)
			}
		}
		samples, err := metricparity.DecodeSamples(response)
		if err != nil {
			return err
		}
		kind := response["data"].(map[string]interface{})["resultType"].(string)
		if resultType != "" && kind != resultType {
			return fmt.Errorf("result type %s, recorded Loki %s", kind, resultType)
		}
		resultType = kind
		responses = append(responses, samples)
	}
	return metricparity.Difference(responses[0], responses[1], true)
}

// Loki shards need not choose the same failing stream first. Preserve the
// recorded category and complete diagnostic outside that series; neither the
// series' label values nor parser-detail wording is claimed as exact parity.
func recordedParserDiagnostic(text string) string {
	const prefix = " for series: '"
	const suffix = "'.\nUse a label filter"
	if start, end := strings.Index(text, prefix), strings.LastIndex(text, suffix); start >= 0 && end > start+len(prefix) {
		text = text[:start+len(prefix)] + "<failing-series>" + text[end:]
	}
	return strings.TrimSpace(text)
}

// conformance: semantics/pipeline-error-text-parity, semantics/nested-aggregation-outer-by-native-routes, semantics/non-sum-vector-aggregation-unreduced, semantics/json-error-filter-metric-split-pushdown, semantics/parse-error-text-parity
func TestOrderedJSONRecordedLokiContracts(t *testing.T) {
	manifest, archive := recordedParserInputs(t)
	metadata, records, err := decodeRecordedParserMatrix(manifest, archive)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		name := strings.TrimSuffix(strings.TrimPrefix(record.Case, "TestChaining_ParserErrorNestedAggregations/"), "/loki")
		t.Run(name, func(t *testing.T) {
			mode := name[strings.LastIndex(name, "/")+1:]
			axis, err := recordedParserAxis(record.Params, mode)
			if err != nil {
				t.Fatal(err)
			}
			at, err := time.Parse(time.RFC3339Nano, axis)
			if err != nil {
				t.Fatal(err)
			}
			service := recordedParserService(record.Params.Get("query"))
			var rows []pushdownRow
			for index, row := range metadata.Fixture.Rows {
				rows = append(rows, pushdownRow{
					ts:     at.Add(-30*time.Second + time.Duration(index)*time.Millisecond),
					stream: map[string]string{"service_name": service, "cluster": row.Cluster, "kind": row.Kind},
					msg:    row.Line,
				})
			}
			backend, fake := newPushdownFakeVL(t, rows, func(name string) string { return name })
			p := newTestProxy(t, backend.URL)
			actual := doCompatProxyRequest(p, "/loki/api/v1/"+mode+"?"+record.Params.Encode(), nil)
			difference := recordedParserDifference(record, actual.Code, actual.Body.Bytes())
			var expectedError string
			if record.Status == http.StatusBadRequest && json.Unmarshal(record.Response, &expectedError) == nil && strings.HasPrefix(expectedError, "parse error") {
				// Keep the existing syntax-category contract. The owned open gap
				// is visible, never counted as exact error-text compatibility.
				var reply struct{ Status, ErrorType, Error string }
				if err := json.Unmarshal(actual.Body.Bytes(), &reply); err != nil || actual.Code != 400 || reply.Status != "error" || reply.ErrorType != "bad_data" || !strings.HasPrefix(reply.Error, "parse error") {
					t.Fatalf("syntax-category contract: %v", difference)
				}
				t.Logf("comparison=status_category known_gap=semantics/parse-error-text-parity exact_error_text=%t Loki=%q proxy=%q", difference == nil, expectedError, reply.Error)
			} else if difference != nil {
				t.Fatal(difference)
			}
			if record.Status == http.StatusOK || strings.Contains(string(record.Response), "JSONParserErr") {
				fake.mu.Lock()
				calls := len(fake.stats) + len(fake.guards) + fake.raw
				fake.mu.Unlock()
				if calls == 0 {
					t.Fatal("current handler did not read fixture backend")
				}
			}
		})
	}
}

func TestOrderedJSONRecordedOracleRejectsMutations(t *testing.T) {
	manifest, archive := recordedParserInputs(t)
	_, records, err := decodeRecordedParserMatrix(manifest, archive)
	if err != nil {
		t.Fatal(err)
	}
	var reference recordedParserCase
	for _, record := range records {
		if record.Status == 200 {
			reference = record
			break
		}
	}
	for _, tc := range []struct {
		name string
		edit func(map[string]interface{})
	}{
		{"value", func(r map[string]interface{}) {
			r["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[1] = "9"
		}},
		{"timestamp", func(r map[string]interface{}) {
			r["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[0] = json.Number("1.25")
		}},
		{"label", func(r map[string]interface{}) {
			r["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["metric"].(map[string]interface{})["extra"] = "label"
		}},
		{"missing_series", func(r map[string]interface{}) { r["data"].(map[string]interface{})["result"] = []interface{}{} }},
		{"duplicate_series", func(r map[string]interface{}) {
			d := r["data"].(map[string]interface{})
			v := d["result"].([]interface{})
			d["result"] = append(v, v[0])
		}},
		{"warning", func(r map[string]interface{}) { r["warnings"] = []interface{}{"partial response"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var response map[string]interface{}
			decoder := json.NewDecoder(bytes.NewReader(reference.Response))
			decoder.UseNumber()
			if err := decoder.Decode(&response); err != nil {
				t.Fatal(err)
			}
			tc.edit(response)
			body, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if err := recordedParserDifference(reference, 200, body); err == nil {
				t.Fatal("changed current-handler answer accepted")
			}
		})
	}
	if err := recordedParserDifference(reference, 500, []byte("backend unavailable")); err == nil {
		t.Fatal("reference success accepted a backend failure")
	}
	if err := recordedParserDifference(reference, 200, append(append([]byte{}, reference.Response...), []byte("\n{}")...)); err == nil {
		t.Fatal("multiple JSON responses accepted")
	}
}

func TestOrderedJSONRecordedPipelineDiagnostic(t *testing.T) {
	manifest, archive := recordedParserInputs(t)
	_, records, err := decodeRecordedParserMatrix(manifest, archive)
	if err != nil {
		t.Fatal(err)
	}
	var reference recordedParserCase
	var expected string
	for _, record := range records {
		if record.Status == 400 && strings.Contains(string(record.Response), "JSONParserErr") {
			reference = record
			if err := json.Unmarshal(record.Response, &expected); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	for _, tc := range []struct {
		name, message, kind string
		wantError           bool
	}{
		{"reference", expected, "bad_data", false},
		{"other_first_series", strings.Replace(expected, "cluster=", "other_cluster=", 1), "bad_data", false},
		{"category_changed", strings.ReplaceAll(expected, "JSONParserErr", "SampleExtractionErr"), "bad_data", true},
		{"guidance_removed", strings.Split(expected, "\nUse a label filter")[0], "bad_data", true},
		{"guidance_changed", strings.Replace(expected, "skip all potential errors", "ignore everything", 1), "bad_data", true},
		{"wrong_envelope_type", expected, "internal", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]string{"status": "error", "errorType": tc.kind, "error": tc.message})
			if err != nil {
				t.Fatal(err)
			}
			if err := recordedParserDifference(reference, 400, body); (err != nil) != tc.wantError {
				t.Fatalf("diagnostic difference=%v, want error=%v", err, tc.wantError)
			}
		})
	}
}
