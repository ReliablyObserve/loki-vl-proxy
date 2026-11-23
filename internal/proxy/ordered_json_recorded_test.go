package proxy

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/test/casefixtures"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/metricparity"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
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
		// Loki writes the message alone as text/plain.
		actual := errBodyView{Error: string(body)}
		if bytes.HasPrefix(bytes.TrimSpace(body), []byte("{")) || actual.Error == "" {
			return fmt.Errorf("missing Loki-shaped bad_data error text: %s", body)
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
				reply := readErrorRec(t, actual)
				if actual.Code != 400 || !strings.HasPrefix(reply.Error, "parse error") {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.message) // Loki writes the message alone as text/plain
			if err := recordedParserDifference(reference, 400, body); (err != nil) != tc.wantError {
				t.Fatalf("diagnostic difference=%v, want error=%v", err, tc.wantError)
			}
		})
	}
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedParserRawComplementRecordedMetrics(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	// The central quality matrix owns the expressions. Preserve the existing
	// engine request IDs and time axes so the recorded protocol stays unchanged.
	var matrix struct {
		Cases []struct {
			ID                 string `json:"id"`
			Query              string `json:"query"`
			Endpoint           string `json:"endpoint"`
			Step               string `json:"step"`
			SourceControl      string `json:"source_control"`
			RequireEmptyResult bool   `json:"require_empty_result"`
		} `json:"cases"`
	}
	data, err := os.ReadFile("../../test/e2e-compat/query-semantics-matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &matrix); err != nil {
		t.Fatal(err)
	}
	emptyContracts := make(map[string]bool)
	var sets []oraclerun.EngineDataSet
	var fixtures [][]pushdownRow
	for _, witness := range casefixtures.OrderedMixedParserRawComplement() {
		rows := []pushdownRow{{ts: at, msg: witness.Stream.Entries[0].Line, stream: witness.Stream.Labels}}
		set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows)}
		prefix := "mixed_raw_" + strings.ReplaceAll(witness.Name, "-", "_") + "__"
		for _, tc := range matrix.Cases {
			if !strings.HasPrefix(tc.ID, prefix) {
				continue
			}
			suffix := strings.TrimPrefix(tc.ID, prefix)
			id := witness.Name + "/" + strings.ReplaceAll(strings.ReplaceAll(suffix, "__", "/"), "group_away", "group-away")
			var step time.Duration
			end := at.Add(30 * time.Second)
			if suffix == "source" {
				end = at
			} else if tc.SourceControl != prefix+"source" {
				t.Fatalf("matrix case %s lost its own source control", tc.ID)
			}
			if tc.Endpoint == "query_range" {
				step, err = time.ParseDuration(tc.Step + "s")
				if err != nil || step <= 0 {
					t.Fatalf("invalid matrix step in %s: %q", tc.ID, tc.Step)
				}
			} else if tc.Endpoint != "query" {
				t.Fatalf("invalid matrix endpoint in %s: %q", tc.ID, tc.Endpoint)
			}
			set.Requests = append(set.Requests, lokiMetricRequest(id, tc.Query, at.UnixNano(), end.UnixNano(), step))
			emptyContracts[id] = tc.RequireEmptyResult
		}
		if len(set.Requests) != 13 || set.Requests[0].ID != witness.Name+"/source" {
			t.Fatalf("missing matrix raw facets for %s", witness.Name)
		}
		sets, fixtures = append(sets, set), append(fixtures, rows)
	}
	answers := lokiEngineAnswers(t, "ordered_mixed_parser_raw_complement", sets)
	for i, set := range sets {
		backend, fake := newPushdownFakeVL(t, fixtures[i], func(s string) string { return s })
		fake.projectRaw = true
		control := set.Requests[0]
		var source struct {
			Data struct {
				Result []struct {
					Value []json.RawMessage `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(answers[control.ID].Body, &source); err != nil || answers[control.ID].Status != http.StatusOK || len(source.Data.Result) != 1 || len(source.Data.Result[0].Value) != 2 || string(source.Data.Result[0].Value[1]) != `"1"` {
			t.Fatalf("recorded Loki source control is not one row: %s", answers[control.ID].Body)
		}
		for _, request := range set.Requests {
			t.Run(request.ID, func(t *testing.T) {
				route := "query_range"
				if request.Instant {
					route = "query"
				}
				p := newTestProxy(t, backend.URL)
				got := doCompatProxyRequest(p, "/loki/api/v1/"+route+"?"+request.Form, nil)
				want := answers[request.ID]
				if want.Status != http.StatusOK {
					t.Fatalf("unusable actual Loki oracle %d: %s", want.Status, want.Error)
				}
				// A recorded empty result is meaningful only after this exact
				// dataset's positive source control. In particular bare JSON
				// intentionally ignores object/array values in the reverse arm.
				var expected, actual struct {
					Status   string   `json:"status"`
					Warnings []string `json:"warnings"`
					Data     struct {
						ResultType string            `json:"resultType"`
						Result     []json.RawMessage `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(want.Body, &expected); err != nil {
					t.Fatal(err)
				}
				if expected.Status != "success" || expected.Data.Result == nil || expected.Data.ResultType != "vector" && expected.Data.ResultType != "matrix" {
					t.Fatalf("invalid recorded Loki metric answer: %s", want.Body)
				}
				if (len(expected.Data.Result) == 0) != emptyContracts[request.ID] {
					t.Fatalf("central matrix empty-result contract differs from recorded Loki for %s", request.ID)
				}
				if len(expected.Data.Result) == 0 {
					if err := json.Unmarshal(got.Body.Bytes(), &actual); err != nil || got.Code != want.Status || actual.Status != expected.Status || actual.Data.ResultType != expected.Data.ResultType || actual.Data.Result == nil || len(actual.Data.Result) != 0 || len(actual.Warnings) != 0 || len(expected.Warnings) != 0 {
						t.Fatalf("proxy differs from source-controlled recorded empty answer: proxy=%s Loki=%s", got.Body.Bytes(), want.Body)
					}
					return
				}
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// This is the proxy's resource policy, not a claim about Loki's own limit.
func TestOrderedMixedExplicitProjectionValueBudgetPrecedesNormalization(t *testing.T) {
	const limit = 64 << 10
	for _, size := range []int{limit, limit + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			plan, ok := compileOrderedJSONMetric(`sum by (k) (count_over_time({app="api"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`)
			if !ok {
				t.Fatal("explicit mixed plan was not compiled")
			}
			value := strings.Repeat("\ufffd", size/len("\ufffd")) + strings.Repeat("x", size%len("\ufffd"))
			labels, keep, err := plan.process(`{"k":"`+value+`"}`, map[string]string{"app": "api"})
			if size > limit {
				if !errors.Is(err, errOrderedJSONComplexity) {
					t.Fatalf("decoded value over the original byte budget was accepted: keep=%v err=%v", keep, err)
				}
				return
			}
			if err != nil || !keep || labels["k"] != lokiLabelValue(value) {
				t.Fatalf("value at the byte budget was not normalized: keep=%v err=%v", keep, err)
			}
		})
	}
}

// conformance: semantics/two-parsers-keep-one
func TestOrderedMixedParserRecordedMetrics(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i, line := range []string{
		`{"tenant_id":"blue","tenant_id":"duplicate","note":" tenant_id=shadow "}`,
		`tenant_id=green tenant_id=other`, `tenant_id=""`, `{"other":"missing"}`,
		`{"tenant_id":"before","truncated":`, `{"tenant\u005fid":"escaped"}`,
		`{"tenant_id":"meta-collision"}`, `tenant_id=meta-collision`, `tenant_id=stream-collision`,
	} {
		row := pushdownRow{ts: at.Add(time.Duration(i) * time.Millisecond), msg: line, stream: map[string]string{"app": "api"}}
		if i == 6 || i == 7 {
			row.vl = map[string]string{"tenant_id": "meta"}
			row.loki = row.vl
		}
		if i == 8 {
			row.stream["tenant_id"] = "stream"
		}
		rows = append(rows, row)
	}
	base := func(group, pipe string) string {
		return "sum by (" + group + ") (count_over_time({app=\"api\"} " + pipe + " [1m]))"
	}
	explicit := `| json tenant_id="[\"tenant_id\"]" | logfmt | drop __error__, __error_details__ | tenant_id!=""`
	var queries []string
	for _, pipe := range []string{explicit, `| json | logfmt | drop __error__, __error_details__`, `| logfmt | json | drop __error__, __error_details__`,
		`| json | __error__="" | logfmt`, `| logfmt | keep tenant_id | json | __error__=""`,
		`| json | drop tenant_id, __error__ | logfmt`, `| json | logfmt | __error__!="" | drop __error__`,
	} {
		queries = append(queries, base("tenant_id", pipe))
	}
	queries = append(queries, base("tenant_id_extracted", explicit))
	child := queries[0]
	// The unfiltered grouping includes both the empty set and named groups.
	withEmptyGroup := queries[1]
	transformedBinary := `label_replace(%s + 1,"copy","$1","tenant_id","(.+)")`
	for _, rank := range []string{"sort", "sort_desc"} {
		queries = append(queries, rank+"("+withEmptyGroup+")", rank+"("+fmt.Sprintf(transformedBinary, child)+")")
	}
	for _, fn := range []string{"rate", "bytes_over_time", "bytes_rate"} {
		queries = append(queries, strings.Replace(child, "count_over_time", fn, 1))
	}
	for _, filter := range []string{`|= "no-such-value" or "green"`, `!= "no-such-value" or "green"`,
		`|~ "no-such-value" or "gr[e]+n"`, `!~ "no-such-value" or "gr[e]+n"`} {
		queries = append(queries, base("tenant_id", filter+` | json | logfmt | drop __error__, __error_details__`))
	}
	queries = append(queries, child+" + "+queries[2], queries[2]+" + "+child)
	for _, wrapper := range []string{"sort(%s)", "sort_desc(%s)", "max(%s)", "topk(10,%s)", `label_replace(%s,"copy","$1","tenant_id","(.+)")`, "%s + 1", "1 + %s"} {
		queries = append(queries, fmt.Sprintf(wrapper, child))
	}
	set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows)}
	// A single non-JSON row makes the complete failing-series diagnostic deterministic.
	metadata := map[string]string{"detected_level": "error", "region": "west"}
	errorRows := []pushdownRow{{ts: at, msg: "tenant_id=amber", stream: map[string]string{"app": "api"}, loki: metadata, vl: metadata}}
	// Loki's ingester attaches this level. The engine reads the ingest metadata
	// while the VL evaluator receives only the original message and stream.
	bodyLevel := map[string]string{"detected_level": "error"}
	levelRows := []pushdownRow{{ts: at, msg: `level=error msg="r1" amount=1`, stream: map[string]string{"app": "lv"}, loki: bodyLevel}}
	collisionRows := []pushdownRow{{ts: at, msg: `level=error detected_level=shadow`, stream: map[string]string{"app": "lv"}, loki: bodyLevel}}
	levelSet := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(levelRows)}
	collisionSet := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(collisionRows)}
	// A common valid JSON/logfmt line distinguishes rewrite and fallback ownership
	// without depending on the open strict-logfmt malformed-input behavior.
	keepRows := []pushdownRow{
		{ts: at, msg: "{}", stream: map[string]string{"app": "api", "x": "stored", "pod": "a"}},
		{ts: at.Add(time.Millisecond), msg: "{}", stream: map[string]string{"app": "api", "x": "stored", "pod": "b"}},
	}
	keepSet := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(keepRows)}
	errorsSet := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(errorRows)}
	bad := base("tenant_id", `| logfmt | drop __error__ | json`)
	for _, step := range []time.Duration{0, 30 * time.Second} {
		add := func(set *oraclerun.EngineDataSet, query string) {
			set.Requests = append(set.Requests, lokiMetricRequest(fmt.Sprintf("%s/%v", query, step), query, at.UnixNano(), at.Add(30*time.Second).UnixNano(), step))
		}
		for _, pipe := range []string{`| logfmt | json`, `| logfmt | keep detected_level | json`,
			`| logfmt | keep app | json`, `| logfmt | drop detected_level | json`,
			`| logfmt | json | drop __error__, __error_details__`} {
			add(&levelSet, `sum by (missing) (count_over_time({app="lv"} `+pipe+` [1m]))`)
		}
		add(&collisionSet, `sum by (detected_level_extracted, missing) (count_over_time({app="lv"} | logfmt | json [1m]))`)
		add(&collisionSet, `sum by (missing) (count_over_time({app="lv"} | logfmt | detected_level_extracted="shadow" | json [1m]))`)
		for _, q := range queries {
			add(&set, q)
		}
		for _, flag := range []string{"", " --strict", " --keep-empty"} {
			add(&keepSet, `count_over_time({app="api"} | logfmt`+flag+` | json | keep x [1m])`)
		}
		bareKeep := `count_over_time({app="api"} | logfmt | json | keep x [1m])`
		for _, fn := range []string{"rate", "bytes_over_time", "bytes_rate"} {
			add(&keepSet, strings.Replace(bareKeep, "count_over_time", fn, 1))
		}
		for _, wrap := range []string{"sort(%s)", "sort_desc(%s)", "max(%s)", "topk(2,%s)", `label_replace(%s,"copy","$1","x","(.+)")`, "%s + 1", "1 + %s"} {
			add(&keepSet, fmt.Sprintf(wrap, bareKeep))
			add(&errorsSet, fmt.Sprintf(wrap, strings.Replace(bareKeep, "keep x", "keep tenant_id", 1)))
		}
		for _, rank := range []string{"sort", "sort_desc"} {
			badInner := base("tenant_id, missing", `| logfmt | drop __error__ | json`)
			add(&errorsSet, rank+"("+fmt.Sprintf(transformedBinary, badInner)+")")
		}
		for _, kept := range []string{"tenant_id", "missing"} {
			add(&errorsSet, `count_over_time({app="api"} | logfmt | json | keep `+kept+` [1m])`)
		}
		for _, wrap := range []string{"%s", "sort(%s)", "sort_desc(%s)", "max(%s)", "topk(2,%s)", `label_replace(%s,"copy","$1","tenant_id","(.+)")`, "%s + 1", "1 + %s"} {
			add(&errorsSet, fmt.Sprintf(wrap, bad))
			add(&errorsSet, fmt.Sprintf(wrap, base("tenant_id, missing", `| logfmt | drop __error__ | json`)))
		}
	}
	sets := []oraclerun.EngineDataSet{set, errorsSet, keepSet, levelSet, collisionSet}
	answers := lokiEngineAnswers(t, "ordered_mixed_parser_metrics", sets)
	for i, set := range sets {
		fixture := rows
		switch i {
		case 1:
			fixture = errorRows
		case 2:
			fixture = keepRows
		case 3:
			fixture = levelRows
		case 4:
			fixture = collisionRows
		}
		backend, fake := newPushdownFakeVL(t, fixture, func(s string) string { return s })
		fake.projectRaw = true
		p := newTestProxy(t, backend.URL)
		for _, req := range set.Requests {
			t.Run(req.ID, func(t *testing.T) {
				route := "query_range"
				if req.Instant {
					route = "query"
				}
				want := answers[req.ID]
				requestProxy := p
				if strings.Contains(req.ID, "(label_replace(") {
					// Exercise the cold parent and its recursive operand even when a
					// previous fixture query warmed the exact inner expression.
					requestProxy = newTestProxy(t, backend.URL)
				}
				sorted := strings.HasPrefix(req.ID, "sort")
				fake.mu.Lock()
				raw, stats := fake.raw, len(fake.stats)
				fake.mu.Unlock()
				got := doCompatProxyRequest(requestProxy, "/loki/api/v1/"+route+"?"+req.Form, nil)
				if sorted {
					fake.mu.Lock()
					reads, buckets := fake.raw-raw, len(fake.stats)-stats
					var probes int
					for _, read := range fake.rawQueries[raw:] {
						if strings.HasSuffix(read.query, "| sort by (_time desc)") {
							probes++
						}
					}
					fake.mu.Unlock()
					if reads == 0 || buckets != 0 || probes != 0 {
						t.Fatalf("sorted operand used %d raw reads, %d stats queries, %d parser probes", reads, buckets, probes)
					}
				}
				if got.Code != want.Status {
					t.Fatalf("status %d, Loki %d: %s / %s", got.Code, want.Status, got.Body, want.Error)
				}
				if want.Status == 400 {
					if strings.TrimSpace(got.Body.String()) != strings.TrimSpace(want.Error) {
						t.Fatalf("diagnostic %q, Loki %q", got.Body.String(), want.Error)
					}
					return
				}
				if want.Status != 200 {
					t.Fatalf("unusable Loki answer %d: %s", want.Status, want.Error)
				}
				if err := recordedParserDifference(recordedParserCase{Status: 200, Response: want.Body}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatal(err)
				}
				// Instant ranks order values and leave equal-value ties unordered.
				// Other vectors and every range matrix serialize in label order.
				ranked := sorted || strings.HasPrefix(req.ID, "topk(") || strings.HasPrefix(req.ID, "bottomk(")
				byValue := req.Instant && ranked
				if actual, loki := recordedMixedOrder(t, got.Body.Bytes(), byValue), recordedMixedOrder(t, want.Body, byValue); !reflect.DeepEqual(actual, loki) {
					t.Fatalf("series order %v, Loki %v", actual, loki)
				}
			})
		}
	}
}

// A mixed count/rate grouped only by detected_level may use stored-level
// buckets when every selected row's source and precedence are unambiguous.
// conformance: semantics/two-parsers-keep-one, loki_api_v1_query, loki_api_v1_query_range
func TestOrderedMixedStoredLevelRecorded(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	// The first and last buckets read by VL may have no Loki point. Two
	// stored groups fit the intermediate stats guard with a tenant limit of one.
	makeRow := func(ts time.Time, level string) pushdownRow {
		return pushdownRow{ts: ts, msg: "{}", stream: map[string]string{"app": "api", "level": level}, loki: map[string]string{"detected_level": level}}
	}
	edge := []pushdownRow{makeRow(at.Add(-time.Minute), "error"), makeRow(at.Add(-time.Minute+time.Millisecond), "info"),
		makeRow(at, "info"), makeRow(at.Add(30*time.Second), "info"), makeRow(at.Add(30*time.Second+time.Millisecond), "error"), makeRow(at.Add(45*time.Second), "error")}
	makeRank := func(instant bool) []pushdownRow {
		rows := []pushdownRow{makeRow(at, "info"), makeRow(at.Add(-time.Second), "info")}
		second := at
		if !instant {
			second = at.Add(20 * time.Second)
		}
		rows = append(rows, makeRow(second, "error"))
		for range 5 {
			rows = append(rows, makeRow(at.Add(-time.Minute), "error"))
		}
		return rows
	}
	fixtures := []struct {
		name  string
		stats bool
		rows  []pushdownRow
	}{
		{"stored", true, []pushdownRow{
			{ts: at, msg: "not-json", stream: map[string]string{"app": "api", "level": "INFO"}, loki: map[string]string{"detected_level": "info"}},
			{ts: at.Add(time.Millisecond), msg: `{"detected_level":"debug"}`, stream: map[string]string{"app": "api"}, vl: map[string]string{"level": "err"}, loki: map[string]string{"level": "err", "detected_level": "error"}},
		}},
		{"stream", false, []pushdownRow{{ts: at, msg: "{}", stream: map[string]string{"app": "api", "detected_level": "INFO", "level": "warn"}}}},
		{"metadata", false, []pushdownRow{{ts: at, msg: "{}", stream: map[string]string{"app": "api"}, vl: map[string]string{"level": "warn", "detected_level": "error"}, loki: map[string]string{"level": "warn", "detected_level": "error"}}}},
		{"foreign", false, []pushdownRow{{ts: at, msg: "{}", stream: map[string]string{"app": "api", "severity": "error"}, vl: map[string]string{"level": "info"}, loki: map[string]string{"level": "info", "detected_level": "error"}}}},
		{"body", false, []pushdownRow{{ts: at, msg: `{"level":"error"}`, stream: map[string]string{"app": "api"}, loki: map[string]string{"detected_level": "error"}}}},
		{"unknown", false, []pushdownRow{{ts: at, msg: "error", stream: map[string]string{"app": "api"}, vl: map[string]string{"level": "notice"}, loki: map[string]string{"level": "notice", "detected_level": "notice"}}}},
		{"edge-limit", true, edge}, {"rank-range-limit", true, makeRank(false)},
		{"rank-instant-limit", true, makeRank(true)}, {"rank-instant-full", true, makeRank(true)},
	}
	var sets []oraclerun.EngineDataSet
	for _, f := range fixtures {
		set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(f.rows)}
		limited := strings.HasSuffix(f.name, "-limit")
		if limited {
			set.MaxSeries = 1
		}
		for _, fn := range []string{"count_over_time", "rate"} {
			for _, step := range []time.Duration{0, 30 * time.Second} {
				if step == 0 && f.name == "rank-range-limit" || step != 0 && strings.HasPrefix(f.name, "rank-instant") {
					continue
				}
				end := at.Add(30 * time.Second)
				if limited || f.name == "rank-instant-full" {
					end = at
					if step != 0 {
						end = at.Add(45 * time.Second)
					}
				}
				q := `sum by (detected_level) (` + fn + `({app="api"} | json | logfmt | drop __error__, __error_details__ [1m]))`
				req := lokiMetricRequest(fmt.Sprintf("%s/%s/%v", f.name, fn, step), q, at.UnixNano(), end.UnixNano(), step)
				set.Requests = append(set.Requests, req)
				if limited {
					req.ID += "/drilldown"
					req.QueryTags = drilldownSeriesLimitTags
					set.Requests = append(set.Requests, req)
				}
			}
		}
		sets = append(sets, set)
	}
	answers := lokiEngineAnswers(t, "mixed_stored_level", sets)
	for i, set := range sets {
		f := fixtures[i]
		backend, fake := newPushdownFakeVL(t, f.rows, func(s string) string { return s })
		for _, req := range set.Requests {
			t.Run(req.ID, func(t *testing.T) {
				p := newTestProxy(t, backend.URL)
				if set.MaxSeries > 0 {
					p.maxStatsQuerySeries = set.MaxSeries
				}
				fake.mu.Lock()
				raw, stats := fake.raw, len(fake.stats)
				fake.mu.Unlock()
				endpoint := "query_range"
				if req.Instant {
					endpoint = "query"
				}
				headers := map[string]string{"X-Query-Tags": req.QueryTags}
				got := doCompatProxyRequest(p, "/loki/api/v1/"+endpoint+"?"+req.Form, headers)
				want := answers[req.ID]
				if set.MaxSeries == 1 {
					if f.name == "rank-instant-limit" && req.Instant && req.QueryTags != "" {
						// Both groups are first met at this instant. Loki's retained
						// subset is unspecified, but every kept point must match its
						// unlimited answer on exactly the same data and request.
						fullID := strings.TrimSuffix(strings.Replace(req.ID, "rank-instant-limit", "rank-instant-full", 1), "/drilldown")
						matched := false
						for _, fullSet := range sets {
							if fullSet.MaxSeries != 0 || !reflect.DeepEqual(fullSet.Streams, set.Streams) {
								continue
							}
							for _, fullReq := range fullSet.Requests {
								matched = matched || fullReq.ID == fullID && fullReq.Instant && fullReq.Form == req.Form
							}
						}
						full, found := answers[fullID]
						if !matched || !found {
							t.Fatal("limited instant lacks its identical-input unlimited Loki reference")
						}
						if err := mixedStoredLevelInstantLimitDifference(got.Code, got.Body.Bytes(), want, full); err != nil {
							t.Fatal(err)
						}
					} else {
						checkMixedStoredLevelLimit(t, got.Code, got.Body.Bytes(), want)
					}
					if f.name == "rank-range-limit" && req.QueryTags != "" {
						// The first visible group is also the busiest. The five error
						// rows outside the left boundary must not change that ranking.
						busiest := newTestProxy(t, backend.URL)
						busiest.maxStatsQuerySeries = 1
						busiest.drilldownSeriesLimitMode = drilldownSeriesLimitBusiest
						partial := doCompatProxyRequest(busiest, "/loki/api/v1/"+endpoint+"?"+req.Form, headers)
						checkMixedStoredLevelLimit(t, partial.Code, partial.Body.Bytes(), want)
					}
				} else if want.Status != 200 {
					t.Fatalf("unexpected reference status %d: %s", want.Status, want.Error)
				} else if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatal(err)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if f.stats && (len(fake.stats) == stats || fake.raw != raw) || !f.stats && fake.raw == raw {
					t.Fatalf("expected stored stats %v, raw reads %d, stats %d", f.stats, fake.raw-raw, len(fake.stats)-stats)
				}
			})
		}
	}
}

func mixedStoredLevelInstantLimitDifference(status int, body []byte, want, full lokiEngineAnswer) error {
	if status != http.StatusOK || want.Status != http.StatusOK || full.Status != http.StatusOK {
		return fmt.Errorf("limited instant or its unlimited reference failed: %d/%d/%d", status, want.Status, full.Status)
	}
	read := func(body []byte, partial bool) (lokiSeries, error) {
		var head struct {
			Status   string   `json:"status"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(body, &head); err != nil {
			return lokiSeries{}, err
		}
		var warnings []string
		if partial {
			warnings = []string{"maximum number of series (1) reached for a single query; returning partial results"}
		}
		if head.Status != "success" || len(head.Warnings) != len(warnings) || !slices.Equal(head.Warnings, warnings) {
			return lokiSeries{}, fmt.Errorf("unexpected instant status/warnings: %s", body)
		}
		series, err := decodeLokiSeries(body)
		if err != nil {
			return lokiSeries{}, err
		}
		if series.ResultType != "vector" || series.Scalar != nil || len(series.Labels) != len(series.Points) {
			return lokiSeries{}, fmt.Errorf("invalid instant vector: %s", body)
		}
		for key, points := range series.Points {
			if len(series.Labels[key]) == 0 || len(points) != 1 {
				return lokiSeries{}, fmt.Errorf("invalid labelled instant point: %s", body)
			}
		}
		return series, nil
	}
	unlimited, err := read(full.Body, false)
	if err != nil {
		return err
	}
	if len(unlimited.Points) != 2 {
		return fmt.Errorf("unlimited reference must prove both groups")
	}
	for _, partial := range [][]byte{want.Body, body} {
		series, err := read(partial, true)
		if err != nil {
			return err
		}
		if len(series.Points) != 1 {
			return fmt.Errorf("limited instant must retain exactly one series")
		}
		for key, points := range series.Points {
			if !reflect.DeepEqual(series.Labels[key], unlimited.Labels[key]) || !reflect.DeepEqual(points, unlimited.Points[key]) {
				return fmt.Errorf("limited instant series differs from unlimited Loki: %s", partial)
			}
		}
	}
	return nil
}

// conformance: limits/drilldown-partial-with-warning, limits/drilldown-breakdown-keeps-busiest
func TestOrderedMixedStoredLevelInstantLimitComparison(t *testing.T) {
	const warning = `"maximum number of series (1) reached for a single query; returning partial results"`
	const info = `{"metric":{"detected_level":"info"},"value":[1700000400,"2"]}`
	const other = `{"metric":{"detected_level":"error"},"value":[1700000400,"1"]}`
	reply := func(rows, warnings string) []byte {
		return []byte(`{"status":"success","warnings":[` + warnings + `],"data":{"resultType":"vector","result":[` + rows + `]}}`)
	}
	full := lokiEngineAnswer{Status: 200, Body: reply(info+","+other, "")}
	want := lokiEngineAnswer{Status: 200, Body: reply(other, warning)}
	for _, row := range []string{info, other} {
		if err := mixedStoredLevelInstantLimitDifference(200, reply(row, warning), want, full); err != nil {
			t.Fatal(err)
		}
	}
	valid := reply(info, warning)
	for name, body := range map[string][]byte{
		"wrong_value":      []byte(strings.Replace(string(valid), `"2"`, `"3"`, 1)),
		"wrong_time":       []byte(strings.Replace(string(valid), `1700000400`, `1700000401`, 1)),
		"unknown_identity": []byte(strings.Replace(string(valid), `"info"`, `"debug"`, 1)),
		"unlabelled":       []byte(strings.Replace(string(valid), `{"detected_level":"info"}`, `{}`, 1)),
		"extra_label":      []byte(strings.Replace(string(valid), `"info"}`, `"info","extra":"bad"}`, 1)),
		"extra_series":     reply(info+","+other, warning),
		"missing_series":   reply("", warning),
		"duplicate_series": reply(info+","+info, warning),
		"missing_warning":  reply(info, ""),
		"wrong_warning":    reply(info, `"partial"`),
		"extra_warning":    reply(info, warning+`,"partial"`),
		"wrong_type":       []byte(strings.Replace(string(valid), `"vector"`, `"matrix"`, 1)),
		"wrong_status":     []byte(strings.Replace(string(valid), `"success"`, `"error"`, 1)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := mixedStoredLevelInstantLimitDifference(200, body, want, full); err == nil {
				t.Fatal("invalid limited instant accepted")
			}
			badReference := lokiEngineAnswer{Status: 200, Body: body}
			if err := mixedStoredLevelInstantLimitDifference(200, valid, badReference, full); err == nil {
				t.Fatal("invalid limited Loki reference accepted")
			}
		})
	}
	for _, bad := range []lokiEngineAnswer{
		{Status: 400, Body: full.Body},
		{Status: 200, Body: reply(info, "")},
		{Status: 200, Body: reply(info+","+other, warning)},
	} {
		if err := mixedStoredLevelInstantLimitDifference(200, valid, want, bad); err == nil {
			t.Fatal("invalid unlimited reference accepted")
		}
	}
	if err := mixedStoredLevelInstantLimitDifference(400, valid, want, full); err == nil {
		t.Fatal("failed proxy status accepted")
	}
}

func checkMixedStoredLevelLimit(t *testing.T, status int, body []byte, want lokiEngineAnswer) {
	t.Helper()
	if status != want.Status {
		t.Fatalf("status %d, Loki %d: %s / %s", status, want.Status, body, want.Error)
	}
	switch status {
	case http.StatusBadRequest:
		if strings.TrimSpace(string(body)) != strings.TrimSpace(want.Error) {
			t.Fatalf("limit error: %q, Loki %q", body, want.Error)
		}
	case http.StatusOK:
		got := decodeDrilldownSeriesLimitAnswer(t, "proxy", body)
		loki := decodeDrilldownSeriesLimitAnswer(t, "Loki", want.Body)
		if !reflect.DeepEqual(got, loki) {
			t.Fatalf("limit response: proxy %+v, Loki %+v", got, loki)
		}
	default:
		t.Fatalf("unexpected reference status %d", status)
	}
}

func recordedMixedSeriesOrder(t *testing.T, body []byte) []string {
	t.Helper()
	return recordedMixedOrder(t, body, true)
}

func recordedMixedOrder(t *testing.T, body []byte, byValue bool) []string {
	t.Helper()
	var reply struct {
		Data struct {
			Result []struct {
				Metric map[string]string
				Value  []json.RawMessage
			}
		}
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, s := range reply.Data.Result {
		if byValue && len(s.Value) == 2 {
			out = append(out, string(s.Value[1]))
		} else {
			out = append(out, canonicalLabelText(s.Metric))
		}
	}
	return out
}
