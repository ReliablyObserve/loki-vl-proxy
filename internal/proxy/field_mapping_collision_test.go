package proxy

import (
	"encoding/json"
	"maps"
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
		if _, ok := collisionFields(t, FieldMappingLabelModeUnset)[label]; ok {
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
		`{service_name="s"} | logfmt | pod_extracted=~".+-1"`:                      {`[{,](?:pod|k8s\\.pod\\.name)=\"`},
		`sum by (pod_extracted) (count_over_time({service_name="s"} | json [5m]))`: {`[{,](?:pod|k8s\\.pod\\.name)=\"`},
		// A stream that stores pod itself (never renamed) holds the stream label in that field, which the parser
		// overwrites: its value is copied aside before the parser and read where the stream lacks k8s.pod.name.
		`{service_name="s"} | logfmt | pod="x"`: {`copy pod as __lxsv_pod | unpack_logfmt | format if (NOT __lxsv_pod:* "pod":*) "<pod>" as __lxsv_pod`,
			`(k8s.pod.name:* "k8s.pod.name":="x") OR (NOT k8s.pod.name:* __lxsv_pod:="x")`, `| delete __lxsv_*`},
		`sum by (pod) (count_over_time({service_name="s"} | logfmt [5m]))`: {`copy pod as __lxsv_pod | unpack_logfmt | format if (__lxsv_pod:*) "<__lxsv_pod>" as pod`, `format if (NOT k8s.pod.name:*) "<pod>" as k8s.pod.name | stats by (k8s.pod.name)`},
		`sum by (namespace) (rate({service_name="s"} | json [5m]))`:        {`format if (NOT k8s.namespace.name:*) "<namespace>" as k8s.namespace.name`},
		// The Logs Drilldown field breakdown of a renamed key (count and rate) and a second base beside it: served
		// from the plan in alias and both, where the default mode leaves them to the string translator.
		`sum by (pod_extracted) (count_over_time({service_name="s"} | logfmt | pod_extracted!="" [5m]))`:            {`| delete __lxp* | format if ("pod.extracted":*)`, `format if (__lxp_stream:~"[{,](?:pod|k8s\\.pod\\.name)=\"" __lxp_pod:*) "<__lxp_pod>" as pod_extracted | delete __lxp* | stats by (pod_extracted) count()`},
		`sum by (pod_extracted) (rate({service_name="s"} | json | pod_extracted!="" [5m]))`:                         {`format if (__lxp_stream:~"[{,](?:pod|k8s\\.pod\\.name)=\"" __lxp_pod:*) "<__lxp_pod>" as pod_extracted | delete __lxp* | stats by (pod_extracted) count()`},
		`sum by (pod, namespace_extracted) (count_over_time({service_name="s"} | logfmt | pod_extracted!="" [5m]))`: {`format if (NOT k8s.pod.name:*) "<pod>" as k8s.pod.name`, `(?:namespace|k8s\\.namespace\\.name)=\"" __lxp_namespace:*) "<__lxp_namespace>" as namespace_extracted`},
		`sum by (pod_extracted) (count_over_time({service_name="s"} | logfmt | pod_extracted="x" | json [5m]))`:     {`(?:pod|k8s\\.pod\\.name)=\"" __lxp_pod:*) "<__lxp_pod>" as pod_extracted`},
		`sum by (pod) (count_over_time({service_name="s"} [5m]))`:                                                   nil,                                                        // no parser: the stream label alone
		`sum by (k8s_pod_name) (count_over_time({service_name="s"} | logfmt [5m]))`:                                 {`format if (NOT k8s.pod.name:*) "<pod>" as k8s.pod.name`}, // the field, answered as pod
		`{service_name="s"} | logfmt | k8s_pod_name="x"`:                                                            nil,                                                        // the base name is no stream label name: the field
		`sum by (k8s_pod_name_extracted) (count_over_time({service_name="s"} | logfmt [5m]))`:                       nil,
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
	// In the both mode the -label-style name of a mapped field is a stream label too (it is added to the stream
	// labels beside the alias): read after a parser as an alias is, and its _extracted rename collides with it.
	bothWants := map[string][]string{
		`{service_name="s"} | logfmt | k8s_pod_name="x"`:                                      {`(k8s.pod.name:* "k8s.pod.name":="x") OR (NOT k8s.pod.name:* __lxsv_k8s_pod_name:="x")`},
		`sum by (k8s_pod_name_extracted) (count_over_time({service_name="s"} | logfmt [5m]))`: {`[{,](?:k8s[._]pod[._]name|k8s\\.pod\\.name)=\"`},
	}
	for q, wants := range queries {
		original := translate(FieldMappingLabelModeUnset, q)
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
			wants := wants
			if both, ok := bothWants[q]; ok && mode == FieldMappingLabelModeBoth {
				wants = both
			}
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
		FieldMappingLabelModeUnset: "stats by (k8s.pod.name, k8s.namespace.name, pod, k8s_namespace_name) count()",
		FieldMappingLabelModeAlias: "stats by (k8s.pod.name, k8s.namespace.name) count()",
		FieldMappingLabelModeBoth:  "stats by (k8s.pod.name, k8s.namespace.name) count()",
	} {
		p := newMappingProxy(t, "http://127.0.0.1:1", nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: mode, FieldMappings: collisionMappings()})
		got := p.addUnderscorefallbackByLabels(query, []string{"pod", "namespace", "k8s_namespace_name"})
		if !strings.HasSuffix(got, want) {
			t.Errorf("%s: %s, want suffix %s", mode, got, want)
		}
	}
}

// mappedOnlyVL answers every read with one logfmt row whose stream holds the
// mapped field k8s.pod.name; its line carries the keys pod and k8s_pod_name.
func mappedOnlyVL(t *testing.T) string {
	t.Helper()
	row := map[string]string{"_time": "2026-01-01T00:00:01Z", "_stream": `{k8s.pod.name="pod-a",service.name="svc"}`, "_stream_id": "1",
		"_msg": "level=info pod=line-a k8s_pod_name=kl-a", "k8s.pod.name": "pod-a", "service.name": "svc"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			_ = json.NewEncoder(w).Encode(row)
		case "/select/logsql/streams":
			_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{{"value": row["_stream"], "hits": 1}}})
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			_ = json.NewEncoder(w).Encode(map[string]any{"values": []map[string]any{{"value": "pod-a", "hits": 1}}})
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// A detected field named like a stream label of a mapped field has no values
// where every line's stream holds the field (-field-mapping-label-mode alias
// and both): Loki lists the line's key as name_extracted there, and no stream
// label is a detected field. In the both mode the field's -label-style name is
// such a stream label too, so a parsed k8s_pod_name is k8s_pod_name_extracted.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_StreamNameValuesAndBaseNameCollision(t *testing.T) {
	mappings := []FieldMapping{{VLField: "k8s.pod.name", LokiLabel: "pod"}, {VLField: "k8s.pod.name", LokiLabel: "k8s_pod_name"}}
	for _, tc := range []struct {
		mode              FieldMappingLabelMode
		empty             []string
		fields, notListed []string
	}{
		{FieldMappingLabelModeAlias, []string{"pod"}, []string{"pod_extracted", "k8s_pod_name"}, []string{"pod", "k8s_pod_name_extracted"}},
		{FieldMappingLabelModeBoth, []string{"pod", "k8s_pod_name"}, []string{"pod_extracted", "k8s_pod_name_extracted"}, []string{"pod", "k8s_pod_name"}},
	} {
		p := newMappingProxy(t, mappedOnlyVL(t), nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: tc.mode, FieldMappings: mappings})
		for _, name := range tc.empty {
			var out struct {
				Values []string `json:"values"`
			}
			body := mappingGet(t, p, "/loki/api/v1/detected_field/"+name+"/values", url.Values{"query": {`{service_name="svc"}`}})
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("%s %s: %v: %s", tc.mode, name, err, body)
			}
			if len(out.Values) != 0 {
				t.Errorf("%s: /detected_field/%s/values = %v, want none (Loki: the key is %s_extracted)", tc.mode, name, out.Values, name)
			}
		}
		var fields struct {
			Fields []collisionField `json:"fields"`
		}
		body := mappingGet(t, p, "/loki/api/v1/detected_fields", url.Values{"query": {`{service_name="svc"}`}})
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal(err)
		}
		listed := map[string]bool{}
		for _, f := range fields.Fields {
			listed[f.Label] = true
		}
		for _, name := range tc.fields {
			if !listed[name] {
				t.Errorf("%s: detected_fields lacks %s: %s", tc.mode, name, body)
			}
		}
		for _, name := range tc.notListed {
			if listed[name] {
				t.Errorf("%s: detected_fields lists %s: %s", tc.mode, name, body)
			}
		}
	}
}

// In the both mode a log answer renames a parsed key named like a mapped
// field's -label-style name to name_extracted beside the stream label of that
// name, with and without categorized labels (the uncategorized answer merges
// the parsed labels into the stream labels).
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes, loki_api_v1_query_range
func TestFieldMappingLabelMode_BothBaseNameParsedKeyInLogAnswer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			_, _ = w.Write([]byte(`{"_time":"2026-01-01T00:00:01Z","_stream":"{k8s.pod.name=\"pod-a\",service.name=\"svc\"}","_stream_id":"1","_msg":"level=info k8s_pod_name=kl-a","k8s.pod.name":"pod-a","service.name":"svc","k8s_pod_name":"kl-a","level":"info"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	t.Cleanup(srv.Close)
	mappings := []FieldMapping{{VLField: "k8s.pod.name", LokiLabel: "pod"}, {VLField: "k8s.pod.name", LokiLabel: "k8s_pod_name"}}
	for _, categorized := range []bool{true, false} {
		p := newMappingProxy(t, srv.URL, nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated,
			FieldMappingLabelMode: FieldMappingLabelModeBoth, FieldMappings: mappings, EmitStructuredMetadata: true})
		params := url.Values{"query": {`{service_name="svc"} | logfmt`}, "start": {"1767222000000000000"}, "end": {"1767225601000000000"}, "limit": {"10"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		if categorized {
			req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
		}
		mux := http.NewServeMux()
		p.RegisterProxyRoutes(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var out struct {
			Data struct {
				Result []struct {
					Stream map[string]string   `json:"stream"`
					Values [][]json.RawMessage `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Data.Result) != 1 {
			t.Fatalf("categorized %v: %v %s", categorized, err, rec.Body.String())
		}
		labels := maps.Clone(out.Data.Result[0].Stream)
		if categorized {
			var meta struct {
				Parsed map[string]string `json:"parsed"`
			}
			_ = json.Unmarshal(out.Data.Result[0].Values[0][2], &meta)
			maps.Copy(labels, meta.Parsed)
		}
		if labels["k8s_pod_name"] != "pod-a" || labels["pod"] != "pod-a" || labels["k8s_pod_name_extracted"] != "kl-a" {
			t.Errorf("categorized %v: labels %v, want k8s_pod_name=pod-a pod=pod-a k8s_pod_name_extracted=kl-a", categorized, labels)
		}
	}
}
