package proxy

import (
	"testing"
)

// TestShouldFilterTranslatedLabel_VLInternal verifies that only VL-internal fields are filtered,
// not legitimate user fields that happen to match naming patterns.
func TestShouldFilterTranslatedLabel_VLInternal(t *testing.T) {
	p := &Proxy{
		declaredLabelFields: []string{},
	}

	tests := []struct {
		name       string
		label      string
		wantFilter bool
	}{
		// VL internal fields SHOULD be filtered
		{"_time internal field", "_time", true},
		{"_msg internal field", "_msg", true},
		{"_stream internal field", "_stream", true},
		{"_stream_id internal field", "_stream_id", true},
		{"detected_level special field", "detected_level", true},

		// OTel-style field names should NOT be filtered - they're valid user/system fields
		{"cloud provider field", "cloud_provider", false},
		{"container id field", "container_id", false},
		{"k8s namespace field", "k8s_namespace_name", false},
		{"service name field", "service_name", false},
		{"service namespace field", "service_namespace", false},

		// Regular custom fields should NOT be filtered
		{"custom field", "custom_field", false},
		{"app label", "app", false},
		{"namespace label", "namespace", false},
		{"my_field", "my_field", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.shouldFilterTranslatedLabel(tt.label)
			if got != tt.wantFilter {
				t.Errorf("shouldFilterTranslatedLabel(%q) = %v, want %v", tt.label, got, tt.wantFilter)
			}
		})
	}
}

// TestShouldFilterTranslatedLabel_DeclaredFields verifies that declared fields are never
// filtered, even if they look like VL internal fields.
func TestShouldFilterTranslatedLabel_DeclaredFields(t *testing.T) {
	tests := []struct {
		name           string
		declaredFields []string
		label          string
		wantFilter     bool
		description    string
	}{
		{
			name:           "declared_exact_match",
			declaredFields: []string{"_time"},
			label:          "_time",
			wantFilter:     false,
			description:    "Declared field should NOT be filtered even if it's a VL internal field",
		},
		{
			name:           "declared_dot_version",
			declaredFields: []string{"_time"},
			label:          "_time",
			wantFilter:     false,
			description:    "Declared internal field should never be filtered",
		},
		{
			name:           "multiple_declared",
			declaredFields: []string{"app", "_msg", "custom_field"},
			label:          "_msg",
			wantFilter:     false,
			description:    "Declared internal field in list should NOT be filtered",
		},
		{
			name:           "not_in_declared_list",
			declaredFields: []string{"app", "custom_field"},
			label:          "_time",
			wantFilter:     true,
			description:    "VL internal field NOT in declared list should be filtered",
		},
		{
			name:           "custom_field_not_declared",
			declaredFields: []string{},
			label:          "custom_app",
			wantFilter:     false,
			description:    "Custom field should NOT be filtered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Proxy{
				declaredLabelFields: tt.declaredFields,
			}
			got := p.shouldFilterTranslatedLabel(tt.label)
			if got != tt.wantFilter {
				t.Errorf("shouldFilterTranslatedLabel(%q) with declared fields %v = %v, want %v\nDescription: %s",
					tt.label, tt.declaredFields, got, tt.wantFilter, tt.description)
			}
		})
	}
}

// TestShouldFilterTranslatedLabel_EdgeCases tests edge case inputs for label filtering.
func TestShouldFilterTranslatedLabel_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		declaredFields []string
		label          string
		wantFilter     bool
	}{
		// Empty label should not crash
		{"empty_string", []string{}, "", false},

		// Single character labels
		{"single_char", []string{}, "a", false},

		// Very long field names (not internal VL fields, so not filtered)
		{"long_custom", []string{}, "very_long_custom_field_name_that_is_not_otel", false},
		{"long_otel_like", []string{}, "cloud_provider_that_is_very_long_and_descriptive", false},

		// Mixed case (Go is case-sensitive - won't match "_time" if wrong case)
		{"uppercase_prefix", []string{}, "Cloud_provider", false}, // Won't match internal field
		{"mixed_case", []string{}, "CONTAINER_ID", false},         // Won't match internal field

		// Multiple underscores (not internal fields, so not filtered)
		{"double_underscore", []string{}, "container__id", false},
		{"trailing_underscore", []string{}, "container_", false},

		// Declared field with complex dot pattern
		{"complex_dots_declared", []string{"service.instance.id"}, "service_instance_id", false},
		{"complex_dots_not_declared", []string{"service.name"}, "service_instance_id", false},

		// Fields that look like internal fields but aren't exact matches
		{"almost_time", []string{}, "_timex", false},
		{"almost_msg", []string{}, "_msgx", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Proxy{
				declaredLabelFields: tt.declaredFields,
			}
			got := p.shouldFilterTranslatedLabel(tt.label)
			if got != tt.wantFilter {
				t.Errorf("shouldFilterTranslatedLabel(%q) with declared %v = %v, want %v",
					tt.label, tt.declaredFields, got, tt.wantFilter)
			}
		})
	}
}

func TestIsVLNonLokiLabelField(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		wantVL bool
	}{
		// Internal VL fields (only _time, _msg, _stream, _stream_id are filtered)
		{"_time", "_time", true},
		{"_msg", "_msg", true},
		{"_stream", "_stream", true},
		{"_stream_id", "_stream_id", true},

		// NOT internal VL fields
		{"_stream_fields", "_stream_fields", false},
		{"_stream_values", "_stream_values", false},

		// Special VL field
		{"detected_level", "detected_level", true},

		// OTel fields (after this change, these should NOT be filtered by isVLNonLokiLabelField)
		{"cloud_provider", "cloud_provider", false},
		{"container_id", "container_id", false},

		// Custom fields
		{"app", "app", false},
		{"custom_field", "custom_field", false},
		{"namespace", "namespace", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isVLNonLokiLabelField(tt.field)
			if got != tt.wantVL {
				t.Errorf("isVLNonLokiLabelField(%q) = %v, want %v", tt.field, got, tt.wantVL)
			}
		})
	}
}
