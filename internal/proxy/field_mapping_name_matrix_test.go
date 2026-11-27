package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The name matrix: every -label-style x -field-mapping-label-mode x
// -metadata-field-mode on every surface that names a field, for a stream field
// with one alias (k8s.pod.name: pod), a stream field with two (k8s.node.name:
// node, nodename), a structured metadata field with one alias
// (k8s.namespace.name: namespace, the owner's example), one with two
// (custom.team: team, squad), and unmapped fields (service.name on the stream,
// host.name in structured metadata). The answers are held to
// testdata/field_mapping_name_matrix.golden (LVP_UPDATE_FIELD_MAPPING_MATRIX=1
// rewrites it); every name a surface shows is also asked back.

func nameMatrixVL() *mappingVL {
	return &mappingVL{
		fields: map[string]string{"service.name": "api", "k8s.pod.name": "api-1", "k8s.node.name": "node-a"},
		meta:   map[string]string{"k8s.namespace.name": "prod", "custom.team": "core", "host.name": "host-a"},
	}
}

func nameMatrixMappings() []FieldMapping {
	return []FieldMapping{
		{VLField: "k8s.pod.name", LokiLabel: "pod"},
		{VLField: "k8s.node.name", LokiLabel: "node"},
		{VLField: "k8s.node.name", LokiLabel: "nodename"},
		{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
		{VLField: "custom.team", LokiLabel: "team"},
		{VLField: "custom.team", LokiLabel: "squad"},
	}
}

// nameMatrixRow is what one configuration shows.
type nameMatrixRow struct {
	labels, detectedLabels, series, stream, flatStream, metadata, detectedFields []string
}

// sortedNames returns the sorted keys of m without detected_level, as name=value.
func sortedPairs(m map[string]string) []string {
	var out []string
	for k, v := range m {
		if k == "detected_level" {
			continue
		}
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// nameMatrixLog returns the stream labels and structured metadata of the one
// entry a log query answers, with or without categorize-labels.
func nameMatrixLog(t testing.TB, p *Proxy, query string, categorized bool) (stream, metadata map[string]string) {
	t.Helper()
	params := url.Values{"query": {query}, "start": {"1767222000000000000"}, "end": {"1767225601000000000"}, "limit": {"10"}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("query_range %s: %d %s", query, rec.Code, rec.Body.String())
	}
	var out struct {
		Data struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Data.Result) != 1 || len(out.Data.Result[0].Values) != 1 {
		t.Fatalf("query_range %s: %v %s", query, err, rec.Body.String())
	}
	stream = out.Data.Result[0].Stream
	metadata = map[string]string{}
	if v := out.Data.Result[0].Values[0]; len(v) == 3 {
		var meta struct {
			StructuredMetadata map[string]string `json:"structuredMetadata"`
			Parsed             map[string]string `json:"parsed"`
		}
		if err := json.Unmarshal(v[2], &meta); err != nil {
			t.Fatal(err)
		}
		metadata = meta.StructuredMetadata
		if len(meta.Parsed) > 0 {
			t.Fatalf("parsed labels on a plain line: %v", meta.Parsed)
		}
	}
	return stream, metadata
}

func nameMatrixDetectedFields(t testing.TB, p *Proxy) []string {
	t.Helper()
	var out struct {
		Fields []struct {
			Label string `json:"label"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(mappingGet(t, p, "/loki/api/v1/detected_fields", url.Values{"query": {`{pod="api-1"}`}}), &out); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range out.Fields {
		names = append(names, f.Label)
	}
	sort.Strings(names)
	return names
}

func nameMatrixFieldValues(t testing.TB, p *Proxy, name string) []string {
	t.Helper()
	var out struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(mappingGet(t, p, "/loki/api/v1/detected_field/"+name+"/values", url.Values{"query": {`{pod="api-1"}`}}), &out); err != nil {
		t.Fatal(err)
	}
	return out.Values
}

// nameMatrixValue is the stored value behind a name the matrix shows.
func nameMatrixValue(name string) string {
	switch name {
	case "pod", "k8s_pod_name", "k8s.pod.name":
		return "api-1"
	case "node", "nodename", "k8s_node_name", "k8s.node.name":
		return "node-a"
	case "namespace", "k8s_namespace_name", "k8s.namespace.name":
		return "prod"
	case "team", "squad", "custom_team", "custom.team":
		return "core"
	case "host_name", "host.name":
		return "host-a"
	case "service_name", "service.name":
		return "api"
	}
	return ""
}

func nameMatrixShow(t testing.TB, style LabelStyle, mode FieldMappingLabelMode, metadata MetadataFieldMode) nameMatrixRow {
	t.Helper()
	vl := nameMatrixVL()
	backend := vl.server(t).URL
	newProxy := func() *Proxy {
		return newMappingProxy(t, backend, nil, Config{LabelStyle: style, MetadataFieldMode: metadata,
			FieldMappingLabelMode: mode, FieldMappings: nameMatrixMappings(), EmitStructuredMetadata: true})
	}
	p := newProxy()
	var row nameMatrixRow
	row.labels = mappingStrings(t, p, "/loki/api/v1/labels")
	row.detectedLabels = mappingDetectedLabels(t, p)
	var series struct {
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(mappingGet(t, p, "/loki/api/v1/series", url.Values{"match[]": {`{pod="api-1"}`}}), &series); err != nil || len(series.Data) != 1 {
		t.Fatalf("series: %v %v", err, series)
	}
	row.series = sortedPairs(series.Data[0])
	stream, meta := nameMatrixLog(t, p, `{pod="api-1"}`, true)
	row.stream, row.metadata = sortedPairs(stream), sortedPairs(meta)
	flat, _ := nameMatrixLog(t, newProxy(), `{pod="api-1"}`, false)
	row.flatStream = sortedPairs(flat)
	row.detectedFields = nameMatrixDetectedFields(t, p)

	// Every name a surface shows is asked back.
	for _, name := range row.labels {
		if want := nameMatrixValue(name); want != "" {
			if got := mappingStrings(t, newProxy(), "/loki/api/v1/label/"+name+"/values"); !slices.Equal(got, []string{want}) {
				t.Errorf("label values of %s = %v, want %s", name, got, want)
			}
		}
	}
	for _, name := range row.detectedLabels {
		// The synthetic service_name of passthrough is no stored field the
		// stand-in groups by.
		if want := nameMatrixValue(name); want != "" && !(style == LabelStylePassthrough && name == "service_name") {
			got := mappingVolume(t, newProxy(), url.Values{"query": {`{pod="api-1"}`}, "targetLabels": {name}})
			if len(got) != 1 || got[0][name] != want {
				t.Errorf("volume by %s = %v, want %s", name, got, want)
			}
		}
	}
	for _, name := range row.detectedFields {
		want := nameMatrixValue(name)
		if want == "" {
			continue
		}
		if got := nameMatrixFieldValues(t, newProxy(), name); !slices.Equal(got, []string{want}) {
			t.Errorf("detected_field values of %s = %v, want %s", name, got, want)
		}
	}
	// A grouping by any shown name answers under that name.
	for _, name := range slices.Concat(row.labels, row.metadata) {
		name, _, _ = strings.Cut(name, "=")
		want := nameMatrixValue(name)
		// A dotted name under underscores (native metadata names) groups under
		// its underscore spelling, as an unmapped dotted field (host.name) does.
		if want == "" || name == "service_name" || style == LabelStyleUnderscores && strings.Contains(name, ".") {
			continue
		}
		// Without the flag an instant grouping by another name of a mapped field
		// answers under the field's answer name, as on main.
		if mode == FieldMappingLabelModeOriginal {
			continue
		}
		q := fmt.Sprintf(`sum by (%s) (count_over_time({pod="api-1"}[5m]))`, name)
		for _, path := range []string{"/loki/api/v1/query", "/loki/api/v1/query_range"} {
			var out struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
					} `json:"result"`
				} `json:"data"`
			}
			params := url.Values{"query": {q}, "time": {"1767225600"}, "step": {"60"}, "start": {"1767225300"}, "end": {"1767225600"}}
			if err := json.Unmarshal(mappingGet(t, newProxy(), path, params), &out); err != nil {
				t.Fatal(err)
			}
			if len(out.Data.Result) != 1 || out.Data.Result[0].Metric[name] != want || len(out.Data.Result[0].Metric) != 1 {
				t.Errorf("%s %s = %+v, want {%s=%q}", path, q, out.Data.Result, name, want)
			}
		}
	}
	for _, pair := range row.metadata {
		name := pair[:strings.IndexByte(pair, '=')]
		field := nameMatrixField(name)
		// A label filter on the name reads the stored field.
		fp := newProxy()
		vl.mu.Lock()
		vl.queries = nil
		vl.mu.Unlock()
		nameMatrixLog(t, fp, fmt.Sprintf(`{pod="api-1"} | %s="%s"`, name, nameMatrixValue(name)), true)
		vl.mu.Lock()
		queries := slices.Clone(vl.queries)
		vl.mu.Unlock()
		if len(queries) == 0 || !strings.Contains(queries[len(queries)-1], field) {
			t.Errorf("filter on %s: LogsQL %q does not read %s", name, queries, field)
		}
	}
	return row
}

// nameMatrixField is the stored field behind a name.
func nameMatrixField(name string) string {
	switch nameMatrixValue(name) {
	case "api-1":
		return "k8s.pod.name"
	case "node-a":
		return "k8s.node.name"
	case "prod":
		return "k8s.namespace.name"
	case "core":
		return "custom.team"
	case "host-a":
		return "host.name"
	}
	return "service.name"
}

// conformance: field-mapping-label-names, semantics/field-mapping-label-modes, loki_api_v1_labels, loki_api_v1_label_name_values, loki_api_v1_detected_fields, loki_api_v1_query_range
func TestFieldMappingLabelMode_NameMatrix(t *testing.T) {
	var b strings.Builder
	b.WriteString("# -label-style x -field-mapping-label-mode x -metadata-field-mode\n")
	b.WriteString("# stream: service.name=api (unmapped), k8s.pod.name=api-1 (pod), k8s.node.name=node-a (node, nodename)\n")
	b.WriteString("# structured metadata: k8s.namespace.name=prod (namespace), custom.team=core (team, squad), host.name=host-a (unmapped)\n")
	for _, style := range []LabelStyle{LabelStyleUnderscores, LabelStylePassthrough} {
		for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
			for _, metadata := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeNative, MetadataFieldModeHybrid} {
				name := string(style) + "/" + string(mode) + "/" + string(metadata)
				t.Run(name, func(t *testing.T) {
					row := nameMatrixShow(t, style, mode, metadata)
					fmt.Fprintf(&b, "\n## %s\n", name)
					for _, line := range []struct {
						surface string
						names   []string
					}{
						{"labels", row.labels},
						{"detected_labels", row.detectedLabels},
						{"series", row.series},
						{"stream (categorize-labels)", row.stream},
						{"structured metadata", row.metadata},
						{"stream (default encoding)", row.flatStream},
						{"detected_fields", row.detectedFields},
					} {
						fmt.Fprintf(&b, "%s: %s\n", line.surface, strings.Join(line.names, " "))
					}
				})
			}
		}
	}
	golden := filepath.Join("testdata", "field_mapping_name_matrix.golden")
	if os.Getenv("LVP_UPDATE_FIELD_MAPPING_MATRIX") == "1" {
		if err := os.WriteFile(golden, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != string(want) {
		t.Errorf("name matrix differs from %s (LVP_UPDATE_FIELD_MAPPING_MATRIX=1 rewrites it):\n%s", golden, got)
	}
}
