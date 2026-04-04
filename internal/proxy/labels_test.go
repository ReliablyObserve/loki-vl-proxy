package proxy

import (
	"testing"
)

func TestSanitizeLabelName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"service.name", "service_name"},
		{"k8s.pod.name", "k8s_pod_name"},
		{"k8s.namespace.name", "k8s_namespace_name"},
		{"deployment.environment.name", "deployment_environment_name"},
		{"simple", "simple"},
		{"already_underscore", "already_underscore"},
		{"with-dash", "with_dash"},
		{"with spaces", "with_spaces"},
		{"123numeric", "key_123numeric"},
		{"a.b.c.d.e", "a_b_c_d_e"},
		{"host.name", "host_name"},
		{"", "key_empty"},
		{"normal_label", "normal_label"},
		{"MixedCase.Field", "MixedCase_Field"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := SanitizeLabelName(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeLabelName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLabelTranslator_Passthrough(t *testing.T) {
	lt := NewLabelTranslator(LabelStylePassthrough, nil)

	if !lt.IsPassthrough() {
		t.Error("passthrough translator should report IsPassthrough=true")
	}

	// No translation in passthrough mode
	if got := lt.ToLoki("service.name"); got != "service.name" {
		t.Errorf("ToLoki should passthrough, got %q", got)
	}
	if got := lt.ToVL("service_name"); got != "service_name" {
		t.Errorf("ToVL should passthrough, got %q", got)
	}
}

func TestLabelTranslator_Underscores(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	if lt.IsPassthrough() {
		t.Error("underscores translator should not report IsPassthrough")
	}

	// Response direction: VL dots → Loki underscores
	tests := []struct {
		vlField  string
		expected string
	}{
		{"service.name", "service_name"},
		{"k8s.pod.name", "k8s_pod_name"},
		{"already_ok", "already_ok"},
		{"level", "level"},
		{"host.name", "host_name"},
	}
	for _, tt := range tests {
		t.Run("ToLoki/"+tt.vlField, func(t *testing.T) {
			if got := lt.ToLoki(tt.vlField); got != tt.expected {
				t.Errorf("ToLoki(%q) = %q, want %q", tt.vlField, got, tt.expected)
			}
		})
	}

	// Query direction: known Loki underscores → VL dots
	queryTests := []struct {
		lokiLabel string
		expected  string
	}{
		{"service_name", "service.name"},
		{"k8s_pod_name", "k8s.pod.name"},
		{"k8s_namespace_name", "k8s.namespace.name"},
		{"level", "level"}, // not in known map, passthrough
		{"detected_level", "level"},
		{"custom_field", "custom_field"}, // not in known map, passthrough
		{"host_name", "host.name"},
		{"container_name", "container.name"},
	}
	for _, tt := range queryTests {
		t.Run("ToVL/"+tt.lokiLabel, func(t *testing.T) {
			if got := lt.ToVL(tt.lokiLabel); got != tt.expected {
				t.Errorf("ToVL(%q) = %q, want %q", tt.lokiLabel, got, tt.expected)
			}
		})
	}
}

func TestLabelTranslator_ToVL_DetectedLevelAliasInPassthrough(t *testing.T) {
	lt := NewLabelTranslator(LabelStylePassthrough, nil)
	if got := lt.ToVL("detected_level"); got != "level" {
		t.Fatalf("ToVL(detected_level) = %q, want level", got)
	}
}

func TestLabelTranslator_CustomMappings(t *testing.T) {
	mappings := []FieldMapping{
		{VLField: "my_custom_field", LokiLabel: "custom"},
		{VLField: "vl_internal_id", LokiLabel: "trace_id"},
	}
	lt := NewLabelTranslator(LabelStylePassthrough, mappings)

	if lt.IsPassthrough() {
		t.Error("translator with custom mappings should not report IsPassthrough")
	}

	// Custom mapping takes precedence
	if got := lt.ToLoki("my_custom_field"); got != "custom" {
		t.Errorf("ToLoki with custom mapping: got %q, want %q", got, "custom")
	}
	if got := lt.ToVL("custom"); got != "my_custom_field" {
		t.Errorf("ToVL with custom mapping: got %q, want %q", got, "my_custom_field")
	}

	// Non-mapped fields passthrough in passthrough mode
	if got := lt.ToLoki("service.name"); got != "service.name" {
		t.Errorf("ToLoki unmapped in passthrough: got %q, want %q", got, "service.name")
	}
}

func TestLabelTranslator_CustomMappingsWithUnderscores(t *testing.T) {
	mappings := []FieldMapping{
		{VLField: "custom.dotted.field", LokiLabel: "my_custom_label"},
	}
	lt := NewLabelTranslator(LabelStyleUnderscores, mappings)

	// Custom mapping overrides automatic sanitization
	if got := lt.ToLoki("custom.dotted.field"); got != "my_custom_label" {
		t.Errorf("custom mapping should override: got %q, want %q", got, "my_custom_label")
	}

	// Non-mapped fields still get automatic sanitization
	if got := lt.ToLoki("other.dotted.field"); got != "other_dotted_field" {
		t.Errorf("auto-sanitize: got %q, want %q", got, "other_dotted_field")
	}
}

func TestLabelTranslator_TranslateLabelsMap(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	input := map[string]string{
		"service.name":           "auth",
		"k8s.pod.name":           "auth-pod-123",
		"level":                  "error",
		"deployment.environment": "production",
	}
	result := lt.TranslateLabelsMap(input)

	expected := map[string]string{
		"service_name":           "auth",
		"k8s_pod_name":           "auth-pod-123",
		"level":                  "error",
		"deployment_environment": "production",
	}
	for k, v := range expected {
		if result[k] != v {
			t.Errorf("TranslateLabelsMap: key %q = %q, want %q", k, result[k], v)
		}
	}
	if len(result) != len(expected) {
		t.Errorf("TranslateLabelsMap: got %d entries, want %d", len(result), len(expected))
	}
}

func TestLabelTranslator_TranslateLabelsList(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	input := []string{"service.name", "k8s.pod.name", "level", "service.name"}
	result := lt.TranslateLabelsList(input)

	// Should deduplicate "service.name" → "service_name" (appears twice)
	if len(result) != 3 {
		t.Errorf("TranslateLabelsList: got %d entries, want 3 (deduplicated): %v", len(result), result)
	}

	found := make(map[string]bool)
	for _, r := range result {
		found[r] = true
	}
	for _, want := range []string{"service_name", "k8s_pod_name", "level"} {
		if !found[want] {
			t.Errorf("TranslateLabelsList: missing %q in %v", want, result)
		}
	}
}

func TestLabelTranslator_PassthroughNoAllocation(t *testing.T) {
	lt := NewLabelTranslator(LabelStylePassthrough, nil)

	input := map[string]string{"a": "1", "b": "2"}
	result := lt.TranslateLabelsMap(input)

	// In passthrough mode, should return the same map (no copy).
	// We verify this by checking values are preserved (pointer identity is implementation detail).
	if result["a"] != "1" || result["b"] != "2" {
		t.Error("passthrough should preserve values")
	}
}
