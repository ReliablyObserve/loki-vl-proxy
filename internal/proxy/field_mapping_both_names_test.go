package proxy

import (
	"slices"
	"testing"
)

// A stored field named like a name the both mode gives a mapped field (an
// alias, or the -label-style name) is not shown under that name: queries for
// it read the mapped field, so the entry shows it once, with the mapped
// field's value, on every run.
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
			if v, ok := meta["k8s_namespace_name"]; ok && v != "prod" {
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
