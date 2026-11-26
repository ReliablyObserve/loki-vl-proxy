package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"
)

// collisionVL answers the detected_fields scan with one logfmt row whose
// stream holds k8s.pod.name and one JSON row whose stream holds no mapped
// field; both lines carry keys named like the aliases pod and namespace.
func collisionVL(t *testing.T) string { return collisionVLFor(t) }

// collisionVLFor is collisionVL for a benchmark too.
func collisionVLFor(t testing.TB) string {
	t.Helper()
	rows := []map[string]string{
		{"_time": "2026-01-01T00:00:01Z", "_stream": `{k8s.pod.name="pod-a",service.name="svc"}`, "_stream_id": "1",
			"_msg": "level=info pod=line-a namespace=lns", "k8s.pod.name": "pod-a", "service.name": "svc"},
		{"_time": "2026-01-01T00:00:02Z", "_stream": `{service.name="svc"}`, "_stream_id": "2",
			"_msg": `{"level":"warn","pod":"json-b","namespace":"jns"}`, "service.name": "svc"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			enc := json.NewEncoder(w)
			for _, row := range rows {
				_ = enc.Encode(row)
			}
		case "/select/logsql/streams":
			_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{
				{"value": rows[0]["_stream"], "hits": 1}, {"value": rows[1]["_stream"], "hits": 1}}})
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// collisionMappings give k8s.pod.name the alias pod and k8s.namespace.name the
// alias namespace beside its -label-style name, as a migration from Loki does.
func collisionMappings() []FieldMapping {
	return []FieldMapping{
		{VLField: "k8s.pod.name", LokiLabel: "pod"},
		{VLField: "k8s.namespace.name", LokiLabel: "namespace"},
		{VLField: "k8s.namespace.name", LokiLabel: "k8s_namespace_name"},
	}
}

type collisionField struct {
	Label    string   `json:"label"`
	Parsers  []string `json:"parsers"`
	JSONPath []string `json:"jsonPath"`
}

func collisionFields(t *testing.T, mode FieldMappingLabelMode) map[string]collisionField {
	t.Helper()
	p := newMappingProxy(t, collisionVL(t), nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
		FieldMappingLabelMode: mode, FieldMappings: collisionMappings()})
	var out struct {
		Fields []collisionField `json:"fields"`
	}
	body := mappingGet(t, p, "/loki/api/v1/detected_fields", url.Values{"query": {`{service_name="svc"}`}})
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	fields := map[string]collisionField{}
	for _, f := range out.Fields {
		sort.Strings(f.Parsers)
		fields[f.Label] = f
	}
	return fields
}

// A parsed key named like an alias (-field-mapping-label-mode alias and both)
// is listed as Loki lists it beside a stream that holds the alias:
// name_extracted with its parser where the row's stream holds the alias's
// field, the plain name where it does not; the stream field itself is no
// detected field. The default mode lists what it listed before.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_ParsedAliasCollisionDetectedFields(t *testing.T) {
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		fields := collisionFields(t, mode)
		want := map[string][]string{
			"pod_extracted": {"logfmt"},         // row 1: the stream holds k8s.pod.name
			"pod":           {"json"},           // row 2: no pod stream label
			"namespace":     {"json", "logfmt"}, // neither stream holds k8s.namespace.name
			"level":         {"json", "logfmt"},
		}
		for label, parsers := range want {
			f, ok := fields[label]
			if !ok || !slices.Equal(f.Parsers, parsers) {
				t.Errorf("%s: %s = %+v (listed %v), want parsers %v", mode, label, f, ok, parsers)
			}
		}
		if f := fields["pod"]; !slices.Equal(f.JSONPath, []string{"pod"}) {
			t.Errorf("%s: pod jsonPath %v", mode, f.JSONPath)
		}
		for _, absent := range []string{"namespace_extracted", "k8s_pod_name", "k8s.pod.name"} {
			if _, ok := fields[absent]; ok {
				t.Errorf("%s: %s listed: %v", mode, absent, fields)
			}
		}
	}
	for _, label := range []string{"pod_extracted", "namespace_extracted", "namespace"} {
		if _, ok := collisionFields(t, FieldMappingLabelModeOriginal)[label]; ok {
			t.Errorf("default mode lists %s", label)
		}
	}
}

// The LogsQL of an alias read after a parser: the collision check of
// name_extracted also matches the alias's field in _stream, a filter on the
// alias reads the field where the stream holds it and the parsed key where it
// does not, and a grouping fills the field from the parsed key where the stream
// lacks it. The default mode writes the text it wrote before.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_ParsedAliasCollisionLogsQL(t *testing.T) {
	queries := map[string][]string{
		`{service_name="s"} | logfmt | pod_extracted=~".+-1"`:                                 {`[{,](?:pod|k8s\\.pod\\.name)=\"`},
		`sum by (pod_extracted) (count_over_time({service_name="s"} | json [5m]))`:            {`[{,](?:pod|k8s\\.pod\\.name)=\"`},
		`{service_name="s"} | logfmt | pod="x"`:                                               {`(k8s.pod.name:* "k8s.pod.name":="x") OR (NOT k8s.pod.name:* pod:="x")`},
		`sum by (pod) (count_over_time({service_name="s"} | logfmt [5m]))`:                    {`format if (NOT k8s.pod.name:*) "<pod>" as k8s.pod.name | stats by (k8s.pod.name)`},
		`sum by (namespace) (rate({service_name="s"} | json [5m]))`:                           {`format if (NOT k8s.namespace.name:*) "<namespace>" as k8s.namespace.name`},
		`sum by (pod) (count_over_time({service_name="s"} [5m]))`:                             nil,                                                        // no parser: the stream label alone
		`sum by (k8s_pod_name) (count_over_time({service_name="s"} | logfmt [5m]))`:           {`format if (NOT k8s.pod.name:*) "<pod>" as k8s.pod.name`}, // the field, answered as pod
		`{service_name="s"} | logfmt | k8s_pod_name="x"`:                                      nil,                                                        // the base name is no stream label name: the field
		`sum by (k8s_pod_name_extracted) (count_over_time({service_name="s"} | logfmt [5m]))`: nil,
	}
	translate := func(mode FieldMappingLabelMode, q string) string {
		p := newMappingProxy(t, "http://127.0.0.1:1", nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: mode, FieldMappings: collisionMappings()})
		s, err := p.translateQuery(q)
		if err != nil {
			t.Fatalf("%s %s: %v", mode, q, err)
		}
		return s
	}
	for q, wants := range queries {
		original := translate(FieldMappingLabelModeOriginal, q)
		unset := translate("", q)
		if original != unset {
			t.Errorf("%s: original and unset differ:\n%s\n%s", q, original, unset)
		}
		for _, artifact := range []string{"(?:pod|", "(?:namespace|", "format if (NOT k8s.", "OR (NOT k8s."} {
			if strings.Contains(original, artifact) {
				t.Errorf("%s: the default mode writes %q: %s", q, artifact, original)
			}
		}
		for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
			got := translate(mode, q)
			for _, want := range wants {
				if !strings.Contains(got, want) {
					t.Errorf("%s %s: missing %s in\n%s", mode, q, want, got)
				}
			}
			if wants == nil && got != original {
				t.Errorf("%s %s: changed without an alias read after a parser:\n%s\n%s", mode, q, got, original)
			}
		}
	}
}

// The grouping of an instant metric keeps no underscore fallback named like an
// alias: after a parser that name is the parsed key, which would overwrite the
// stream label in the answer.
//
// conformance: field-mapping-label-names
func TestFieldMappingLabelMode_NoUnderscoreFallbackForAlias(t *testing.T) {
	const query = "* | unpack_logfmt | stats by (k8s.pod.name, k8s.namespace.name) count()"
	for mode, want := range map[FieldMappingLabelMode]string{
		// The default mode keeps its fallback: pod is the last mapping of k8s.pod.name.
		FieldMappingLabelModeOriginal: "stats by (k8s.pod.name, k8s.namespace.name, pod, k8s_namespace_name) count()",
		FieldMappingLabelModeAlias:    "stats by (k8s.pod.name, k8s.namespace.name) count()",
		FieldMappingLabelModeBoth:     "stats by (k8s.pod.name, k8s.namespace.name) count()",
	} {
		p := newMappingProxy(t, "http://127.0.0.1:1", nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: mode, FieldMappings: collisionMappings()})
		got := p.addUnderscorefallbackByLabels(query, []string{"pod", "namespace", "k8s_namespace_name"})
		if !strings.HasSuffix(got, want) {
			t.Errorf("%s: %s, want suffix %s", mode, got, want)
		}
	}
}
