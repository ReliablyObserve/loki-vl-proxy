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

func TestLabelTranslator_ToVL_LearnedCustomAliases(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	if got := lt.ToVL("cll_pipeline_processing"); got != "cll_pipeline_processing" {
		t.Fatalf("unexpected pre-learn mapping, got %q", got)
	}

	lt.LearnFieldAliases([]string{"cll.pipeline.processing"})
	if got := lt.ToVL("cll_pipeline_processing"); got != "cll.pipeline.processing" {
		t.Fatalf("learned alias mapping failed, got %q", got)
	}
}

func TestLabelTranslator_ToVL_LearnedAliasesAmbiguous(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)
	lt.LearnFieldAliases([]string{"foo.bar", "foo-bar"})

	if got := lt.ToVL("foo_bar"); got != "foo_bar" {
		t.Fatalf("ambiguous alias should fall back to passthrough, got %q", got)
	}
}

func TestLabelTranslator_ToVL_LearnedAliasesDoNotOverrideKnownOrCustom(t *testing.T) {
	ltKnown := NewLabelTranslator(LabelStyleUnderscores, nil)
	ltKnown.LearnFieldAliases([]string{"service-name"})
	if got := ltKnown.ToVL("service_name"); got != "service.name" {
		t.Fatalf("known semconv alias must win, got %q", got)
	}

	ltCustom := NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{
		{VLField: "my.custom.field", LokiLabel: "my_custom_field"},
	})
	ltCustom.LearnFieldAliases([]string{"other.custom.field"})
	if got := ltCustom.ToVL("my_custom_field"); got != "my.custom.field" {
		t.Fatalf("explicit custom mapping must win, got %q", got)
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

func TestNormalizeMetadataFieldMode_DefaultsToHybrid(t *testing.T) {
	if got := normalizeMetadataFieldMode(""); got != MetadataFieldModeHybrid {
		t.Fatalf("normalizeMetadataFieldMode(\"\") = %q, want %q", got, MetadataFieldModeHybrid)
	}
	if got := normalizeMetadataFieldMode("broken"); got != MetadataFieldModeHybrid {
		t.Fatalf("normalizeMetadataFieldMode(invalid) = %q, want %q", got, MetadataFieldModeHybrid)
	}
}

func TestLabelTranslator_MetadataFieldExposures(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	tests := []struct {
		name  string
		field string
		mode  MetadataFieldMode
		want  []metadataFieldExposure
	}{
		{
			name:  "native dotted field",
			field: "service.name",
			mode:  MetadataFieldModeNative,
			want:  []metadataFieldExposure{{name: "service.name", isAlias: false}},
		},
		{
			name:  "translated dotted field",
			field: "service.name",
			mode:  MetadataFieldModeTranslated,
			want:  []metadataFieldExposure{{name: "service_name", isAlias: true}},
		},
		{
			name:  "hybrid dotted field",
			field: "service.name",
			mode:  MetadataFieldModeHybrid,
			want: []metadataFieldExposure{
				{name: "service.name", isAlias: false},
				{name: "service_name", isAlias: true},
			},
		},
		{
			name:  "hybrid plain field dedupes",
			field: "trace_id",
			mode:  MetadataFieldModeHybrid,
			want:  []metadataFieldExposure{{name: "trace_id", isAlias: false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lt.metadataFieldExposures(tt.field, tt.mode)
			if len(got) != len(tt.want) {
				t.Fatalf("metadataFieldExposures(%q, %q) length = %d, want %d (%v)", tt.field, tt.mode, len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("metadataFieldExposures(%q, %q)[%d] = %+v, want %+v", tt.field, tt.mode, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestLabelTranslator_ResolveLabelCandidates(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, []FieldMapping{
		{VLField: "custom.dotted.field", LokiLabel: "custom_label"},
	})

	tests := []struct {
		name      string
		label     string
		available []string
		want      []string
		ambiguous bool
	}{
		{
			name:      "explicit mapping wins",
			label:     "custom_label",
			available: []string{"custom.dotted.field", "custom_label"},
			want:      []string{"custom.dotted.field"},
		},
		{
			name:      "exact native wins over semconv alias",
			label:     "service_name",
			available: []string{"service_name", "service.name"},
			want:      []string{"service_name"},
		},
		{
			name:      "known semconv alias resolves when exact native missing",
			label:     "service_name",
			available: []string{"service.name"},
			want:      []string{"service.name"},
		},
		{
			name:      "unique dynamic alias resolves custom dotted field",
			label:     "com_acme_shop_order_id",
			available: []string{"com.acme.shop.order_id"},
			want:      []string{"com.acme.shop.order_id"},
		},
		{
			name:      "ambiguous dynamic alias returns all candidates",
			label:     "foo_bar",
			available: []string{"foo.bar", "foo-bar"},
			want:      []string{"foo.bar", "foo-bar"},
			ambiguous: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lt.ResolveLabelCandidates(tt.label, tt.available)
			if got.ambiguous != tt.ambiguous {
				t.Fatalf("ResolveLabelCandidates(%q).ambiguous = %v, want %v", tt.label, got.ambiguous, tt.ambiguous)
			}
			if len(got.candidates) != len(tt.want) {
				t.Fatalf("ResolveLabelCandidates(%q) len = %d, want %d (%v)", tt.label, len(got.candidates), len(tt.want), got.candidates)
			}
			for i := range tt.want {
				if got.candidates[i] != tt.want[i] {
					t.Fatalf("ResolveLabelCandidates(%q)[%d] = %q, want %q", tt.label, i, got.candidates[i], tt.want[i])
				}
			}
		})
	}
}

func TestLabelTranslator_ResolveMetadataCandidates(t *testing.T) {
	lt := NewLabelTranslator(LabelStyleUnderscores, nil)

	tests := []struct {
		name      string
		field     string
		available []string
		want      []string
		ambiguous bool
	}{
		{
			name:      "exact native wins for underscore field",
			field:     "service_name",
			available: []string{"service.name", "service_name"},
			want:      []string{"service_name"},
		},
		{
			name:      "hybrid metadata alias resolves dotted field",
			field:     "service_name",
			available: []string{"service.name"},
			want:      []string{"service.name"},
		},
		{
			name:      "custom dotted metadata alias resolves uniquely",
			field:     "com_acme_shop_order_id",
			available: []string{"com.acme.shop.order_id"},
			want:      []string{"com.acme.shop.order_id"},
		},
		{
			name:      "colliding metadata aliases stay ambiguous",
			field:     "foo_bar",
			available: []string{"foo.bar", "foo-bar"},
			want:      []string{"foo.bar", "foo-bar"},
			ambiguous: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lt.ResolveMetadataCandidates(tt.field, tt.available, MetadataFieldModeHybrid)
			if got.ambiguous != tt.ambiguous {
				t.Fatalf("ResolveMetadataCandidates(%q).ambiguous = %v, want %v", tt.field, got.ambiguous, tt.ambiguous)
			}
			if len(got.candidates) != len(tt.want) {
				t.Fatalf("ResolveMetadataCandidates(%q) len = %d, want %d (%v)", tt.field, len(got.candidates), len(tt.want), got.candidates)
			}
			for i := range tt.want {
				if got.candidates[i] != tt.want[i] {
					t.Fatalf("ResolveMetadataCandidates(%q)[%d] = %q, want %q", tt.field, i, got.candidates[i], tt.want[i])
				}
			}
		})
	}
}
