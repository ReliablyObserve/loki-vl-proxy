package main

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// conformance: field-mapping-label-names, semantics/field-mapping-label-modes
func TestFieldMappingLabelModeConfig(t *testing.T) {
	env := func(key string) string {
		if key == "FIELD_MAPPING_LABEL_MODE" {
			return "both"
		}
		return ""
	}
	if got := applyEnvOverrides(envConfig{}, env); got.labelMappingMode != "both" {
		t.Fatalf("env not applied: %q", got.labelMappingMode)
	}
	if got := applyEnvOverrides(envConfig{labelMappingMode: "alias"}, env); got.labelMappingMode != "alias" {
		t.Fatalf("the flag must win over the environment: %q", got.labelMappingMode)
	}
	for value, want := range map[string]proxy.FieldMappingLabelMode{
		"":         proxy.FieldMappingLabelModeOriginal,
		"original": proxy.FieldMappingLabelModeOriginal,
		"alias":    proxy.FieldMappingLabelModeAlias,
		"both":     proxy.FieldMappingLabelModeBoth,
	} {
		got, err := buildProxyConfig(proxyRuntimeConfig{labelStyle: "underscores", metadataFieldMode: "translated", fieldMappingLabelMode: value})
		if err != nil || got.FieldMappingLabelMode != want {
			t.Fatalf("%q: mode %q, error %v", value, got.FieldMappingLabelMode, err)
		}
	}
	_, err := buildProxyConfig(proxyRuntimeConfig{labelStyle: "underscores", metadataFieldMode: "translated", fieldMappingLabelMode: "aliases"})
	if err == nil || !strings.Contains(err.Error(), "-field-mapping-label-mode") {
		t.Fatalf("invalid mode: %v", err)
	}
}
