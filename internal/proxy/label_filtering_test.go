package proxy

import (
	"testing"
)

func TestShouldFilterTranslatedLabel_OTelPrefixes(t *testing.T) {
	p := &Proxy{
		declaredLabelFields: []string{},
	}

	tests := []struct {
		name      string
		label     string
		wantFilter bool
	}{
		// Standard OTel prefixes should be filtered
		{"cloud_ prefix", "cloud_provider", true},
		{"container_ prefix", "container_id", true},
		{"k8s_ prefix", "k8s_namespace_name", true},
		{"telemetry_ prefix", "telemetry_sdk_version", true},
		{"process_ prefix", "process_runtime_name", true},
		{"host_ prefix", "host_name", true},
		{"service_ prefix", "service_name", true},
		{"net_ prefix", "net_peer_name", true},
		{"os_ prefix", "os_type", true},
		{"deployment_ prefix", "deployment_environment", true},
		{"log_ prefix", "log_file_path", true},
		{"db_ prefix", "db_name", true},
		{"messaging_ prefix", "messaging_system", true},
		{"rpc_ prefix", "rpc_method", true},
		{"url_ prefix", "url_full", true},
		{"user_ prefix", "user_id", true},
		{"event_ prefix", "event_name", true},
		{"faas_ prefix", "faas_function_name", true},
		{"http_ prefix", "http_method", true},
		{"aws_ prefix", "aws_region", true},
		{"azure_ prefix", "azure_resource_id", true},
		{"gcp_ prefix", "gcp_project_id", true},

		// Non-OTel prefixes should NOT be filtered
		{"custom field", "custom_field", false},
		{"app label", "app", false},
		{"namespace label", "namespace", false},
		{"custom_app field", "custom_app", false},
		{"my_field", "my_field", false},

		// Edge case: similar to OTel but not exact prefix
		{"cloudprovider (no underscore)", "cloudprovider", false},
		{"containment (not container)", "containment", false},
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

func TestShouldFilterTranslatedLabel_DeclaredFields(t *testing.T) {
	tests := []struct {
		name              string
		declaredFields    []string
		label             string
		wantFilter        bool
		description       string
	}{
		{
			name:           "declared_exact_match",
			declaredFields: []string{"container_id"},
			label:          "container_id",
			wantFilter:     false,
			description:    "Declared field should NOT be filtered even if it matches OTel prefix",
		},
		{
			name:           "declared_dot_version",
			declaredFields: []string{"container.id"},
			label:          "container_id",
			wantFilter:     false,
			description:    "Declared field with dots should match translated underscore version",
		},
		{
			name:           "declared_underscore_version",
			declaredFields: []string{"container_id"},
			label:          "container_id",
			wantFilter:     false,
			description:    "Declared field with underscores should match exactly",
		},
		{
			name:           "multiple_declared",
			declaredFields: []string{"app", "container_id", "custom_field"},
			label:          "container_id",
			wantFilter:     false,
			description:    "Declared field in list should NOT be filtered",
		},
		{
			name:           "not_in_declared_list",
			declaredFields: []string{"app", "custom_field"},
			label:          "container_id",
			wantFilter:     true,
			description:    "OTel field NOT in declared list should be filtered",
		},
		{
			name:           "custom_field_not_declared",
			declaredFields: []string{},
			label:          "custom_app",
			wantFilter:     false,
			description:    "Custom field not matching OTel prefixes should NOT be filtered",
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

		// Very long field names
		{"long_custom", []string{}, "very_long_custom_field_name_that_is_not_otel", false},
		{"long_otel", []string{}, "cloud_provider_that_is_very_long_and_descriptive", true},

		// Mixed case (Go is case-sensitive)
		{"uppercase_prefix", []string{}, "Cloud_provider", false}, // Won't match lowercase prefix
		{"mixed_case", []string{}, "CONTAINER_ID", false},         // Won't match lowercase prefix

		// Multiple underscores (still match OTel prefix)
		{"double_underscore", []string{}, "container__id", true},
		{"trailing_underscore", []string{}, "container_", true},

		// Declared field with complex dot pattern
		{"complex_dots", []string{"service.instance.id"}, "service_instance_id", false},
		{"partial_match_not_declared", []string{"service.name"}, "service_instance_id", true},
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
