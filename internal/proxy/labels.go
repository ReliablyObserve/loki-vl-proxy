package proxy

import (
	"regexp"
	"strings"
)

// LabelStyle controls how VL field names are translated to Loki label names in responses,
// and how Loki label names are translated back to VL field names in queries.
type LabelStyle string

const (
	// LabelStylePassthrough passes VL field names through unchanged.
	// Use when VL already stores labels in Loki-compatible format (underscores).
	LabelStylePassthrough LabelStyle = "passthrough"

	// LabelStyleUnderscores converts dots to underscores in label names (response direction)
	// and underscores back to dots for known OTel fields (query direction).
	// Use when VL stores OTel-style dotted names (e.g., "service.name") and you want
	// Loki-compatible underscore names (e.g., "service_name") in Grafana.
	LabelStyleUnderscores LabelStyle = "underscores"
)

// FieldMapping defines a custom field name mapping between VL and Loki.
type FieldMapping struct {
	VLField   string `json:"vl_field" yaml:"vl_field"`     // field name as stored in VictoriaLogs
	LokiLabel string `json:"loki_label" yaml:"loki_label"` // label name exposed via Loki API
}

// labelSanitizeRe matches characters not allowed in Prometheus/Loki label names.
var labelSanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// LabelTranslator handles bidirectional label name translation between VL and Loki.
type LabelTranslator struct {
	style    LabelStyle
	vlToLoki map[string]string // VL field name → Loki label name
	lokiToVL map[string]string // Loki label name → VL field name
}

// NewLabelTranslator creates a label translator with the given style and custom mappings.
// Custom mappings take precedence over automatic translation.
func NewLabelTranslator(style LabelStyle, mappings []FieldMapping) *LabelTranslator {
	lt := &LabelTranslator{
		style:    style,
		vlToLoki: make(map[string]string),
		lokiToVL: make(map[string]string),
	}

	// Register custom mappings (bidirectional)
	for _, m := range mappings {
		if m.VLField != "" && m.LokiLabel != "" {
			lt.vlToLoki[m.VLField] = m.LokiLabel
			lt.lokiToVL[m.LokiLabel] = m.VLField
		}
	}

	return lt
}

// ToLoki translates a VL field name to a Loki-compatible label name (response direction).
func (lt *LabelTranslator) ToLoki(vlField string) string {
	// Custom mapping takes precedence
	if mapped, ok := lt.vlToLoki[vlField]; ok {
		return mapped
	}

	switch lt.style {
	case LabelStyleUnderscores:
		return SanitizeLabelName(vlField)
	default:
		return vlField
	}
}

// ToVL translates a Loki label name to a VL field name (query direction).
func (lt *LabelTranslator) ToVL(lokiLabel string) string {
	// Custom mapping takes precedence
	if mapped, ok := lt.lokiToVL[lokiLabel]; ok {
		return mapped
	}
	if lokiLabel == "detected_level" {
		return "level"
	}

	switch lt.style {
	case LabelStyleUnderscores:
		// For query direction, we check known OTel semantic conventions.
		// If the underscore label matches a known OTel dotted field, use the dotted form.
		if dotted, ok := knownUnderscoreToDot[lokiLabel]; ok {
			return dotted
		}
		// For unknown labels, pass through as-is — the VL field might already be underscore-based.
		return lokiLabel
	default:
		return lokiLabel
	}
}

// TranslateLabelsMap translates all keys in a labels map (response direction).
func (lt *LabelTranslator) TranslateLabelsMap(labels map[string]string) map[string]string {
	if lt.style == LabelStylePassthrough && len(lt.vlToLoki) == 0 {
		return labels
	}
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[lt.ToLoki(k)] = v
	}
	return result
}

// TranslateLabelsList translates a list of label names (response direction).
func (lt *LabelTranslator) TranslateLabelsList(labels []string) []string {
	if lt.style == LabelStylePassthrough && len(lt.vlToLoki) == 0 {
		return labels
	}
	seen := make(map[string]bool, len(labels))
	result := make([]string, 0, len(labels))
	for _, l := range labels {
		translated := lt.ToLoki(l)
		if !seen[translated] {
			seen[translated] = true
			result = append(result, translated)
		}
	}
	return result
}

// IsPassthrough returns true if no translation is needed.
func (lt *LabelTranslator) IsPassthrough() bool {
	return lt.style == LabelStylePassthrough && len(lt.vlToLoki) == 0
}

// SanitizeLabelName converts a field name to a valid Prometheus/Loki label name.
// Rules: replace [^a-zA-Z0-9_] with _, prefix "key_" if starts with digit.
func SanitizeLabelName(name string) string {
	sanitized := labelSanitizeRe.ReplaceAllString(name, "_")
	// Collapse multiple consecutive underscores from sanitization
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}
	// Trim trailing underscores that result from sanitization
	sanitized = strings.TrimRight(sanitized, "_")
	if len(sanitized) == 0 {
		return "key_empty"
	}
	if sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "key_" + sanitized
	}
	return sanitized
}

// knownUnderscoreToDot maps well-known Loki/Prometheus underscore labels back to
// OTel semantic convention dotted names. Used for query-direction translation when
// label-style=underscores and VL stores OTel-style dots.
var knownUnderscoreToDot = map[string]string{
	// OTel resource attributes
	"service_name":                "service.name",
	"service_namespace":           "service.namespace",
	"service_version":             "service.version",
	"service_instance_id":         "service.instance.id",
	"deployment_environment":      "deployment.environment",
	"deployment_environment_name": "deployment.environment.name",
	"telemetry_sdk_name":          "telemetry.sdk.name",
	"telemetry_sdk_language":      "telemetry.sdk.language",
	"telemetry_sdk_version":       "telemetry.sdk.version",

	// Kubernetes attributes
	"k8s_pod_name":         "k8s.pod.name",
	"k8s_pod_uid":          "k8s.pod.uid",
	"k8s_namespace_name":   "k8s.namespace.name",
	"k8s_node_name":        "k8s.node.name",
	"k8s_container_name":   "k8s.container.name",
	"k8s_deployment_name":  "k8s.deployment.name",
	"k8s_daemonset_name":   "k8s.daemonset.name",
	"k8s_statefulset_name": "k8s.statefulset.name",
	"k8s_replicaset_name":  "k8s.replicaset.name",
	"k8s_job_name":         "k8s.job.name",
	"k8s_cronjob_name":     "k8s.cronjob.name",
	"k8s_cluster_name":     "k8s.cluster.name",

	// Cloud attributes
	"cloud_provider":          "cloud.provider",
	"cloud_platform":          "cloud.platform",
	"cloud_region":            "cloud.region",
	"cloud_availability_zone": "cloud.availability_zone",
	"cloud_account_id":        "cloud.account.id",

	// Host attributes
	"host_name": "host.name",
	"host_id":   "host.id",
	"host_type": "host.type",
	"host_arch": "host.arch",

	// Process attributes
	"process_pid":             "process.pid",
	"process_executable_name": "process.executable.name",
	"process_executable_path": "process.executable.path",
	"process_command":         "process.command",
	"process_runtime_name":    "process.runtime.name",
	"process_runtime_version": "process.runtime.version",

	// OS attributes
	"os_type":    "os.type",
	"os_version": "os.version",

	// Log-specific
	"log_file_path": "log.file.path",
	"log_file_name": "log.file.name",
	"log_iostream":  "log.iostream",

	// Network
	"net_host_name": "net.host.name",
	"net_host_port": "net.host.port",
	"net_peer_name": "net.peer.name",
	"net_peer_port": "net.peer.port",

	// Container
	"container_id":         "container.id",
	"container_name":       "container.name",
	"container_runtime":    "container.runtime",
	"container_image_name": "container.image.name",
	"container_image_tag":  "container.image.tag",
}
