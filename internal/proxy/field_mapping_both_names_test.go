package proxy

import (
	"slices"
	"testing"
)

// A stored field named like a name the both mode shows for a mapped field (an
// alias, or the -label-style name) does not take that name from it: the entry
// shows the name once, with the mapped field's value, on every run.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_BothStoredFieldNamedLikeMappingName(t *testing.T) {
	for _, metadata := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeNative, MetadataFieldModeHybrid} {
		for run := 0; run < 8; run++ {
			vl := &mappingVL{
				fields: map[string]string{"k8s.pod.name": "api-1"},
				meta:   map[string]string{"k8s.namespace.name": "prod", "namespace": "stored", "k8s_namespace_name": "stored"},
			}
			p := newMappingProxy(t, vl.server(t).URL, nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: metadata,
				FieldMappingLabelMode: FieldMappingLabelModeBoth, EmitStructuredMetadata: true,
				FieldMappings: []FieldMapping{{VLField: "k8s.pod.name", LokiLabel: "pod"}, {VLField: "k8s.namespace.name", LokiLabel: "namespace"}}})
			_, meta := nameMatrixLog(t, p, `{pod="api-1"}`, true)
			if meta["namespace"] != "prod" {
				t.Fatalf("%s: namespace = %q, want the mapped field's prod (%v)", metadata, meta["namespace"], meta)
			}
			// native shows no k8s_namespace_name for the mapped field: the
			// stored field of that name shows its own value there.
			if v, ok := meta["k8s_namespace_name"]; ok && v != "prod" && metadata != MetadataFieldModeNative {
				t.Fatalf("%s: k8s_namespace_name = %q, want the mapped field's prod (%v)", metadata, v, meta)
			}
		}
	}
}

// The cache identity of the both mode carries the metadata field mode, whose
// names it shows, and differs from the alias mode and from the unset flag.
//
// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelMode_BothNamesIdentity(t *testing.T) {
	identity := func(mode FieldMappingLabelMode, metadata MetadataFieldMode) string {
		p := newMappingProxy(t, "http://127.0.0.1:1", nil, Config{LabelStyle: LabelStyleUnderscores, MetadataFieldMode: metadata,
			FieldMappingLabelMode: mode, FieldMappings: nameMatrixMappings()})
		return p.routingNamespace
	}
	var seen []string
	for _, mode := range []FieldMappingLabelMode{FieldMappingLabelModeAlias, FieldMappingLabelModeBoth} {
		for _, metadata := range []MetadataFieldMode{MetadataFieldModeTranslated, MetadataFieldModeNative, MetadataFieldModeHybrid} {
			id := identity(mode, metadata)
			if slices.Contains(seen, id) {
				t.Fatalf("%s/%s shares a cache identity", mode, metadata)
			}
			seen = append(seen, id)
		}
	}
	if unset := identity("", MetadataFieldModeTranslated); slices.Contains(seen, unset) {
		t.Fatal("the unset flag shares a cache identity with a listing mode")
	}
}
