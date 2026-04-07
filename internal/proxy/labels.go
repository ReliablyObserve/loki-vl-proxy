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

// MetadataFieldMode controls how VictoriaLogs field-oriented APIs expose
// non-stream fields such as parsed values and structured metadata.
type MetadataFieldMode string

const (
	// MetadataFieldModeNative keeps VictoriaLogs field names as stored.
	MetadataFieldModeNative MetadataFieldMode = "native"
	// MetadataFieldModeTranslated exposes only Loki-compatible translated aliases.
	MetadataFieldModeTranslated MetadataFieldMode = "translated"
	// MetadataFieldModeHybrid exposes both the native VL field name and the
	// translated Loki-compatible alias when they differ.
	MetadataFieldModeHybrid MetadataFieldMode = "hybrid"
)

// FieldMapping defines a custom field name mapping between VL and Loki.
type FieldMapping struct {
	VLField   string `json:"vl_field" yaml:"vl_field"`     // field name as stored in VictoriaLogs
	LokiLabel string `json:"loki_label" yaml:"loki_label"` // label name exposed via Loki API
}

type metadataFieldExposure struct {
	name    string
	isAlias bool
}

type fieldResolution struct {
	candidates []string
	ambiguous  bool
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

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func containsString(values []string, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
}

// ResolveLabelCandidates resolves a Loki label name to one or more VL native field names
// using runtime field inventory when available. Exact native matches win for backward
// compatibility; translated aliases are only used automatically when they are unique.
func (lt *LabelTranslator) ResolveLabelCandidates(lokiLabel string, available []string) fieldResolution {
	label := strings.TrimSpace(lokiLabel)
	if label == "" {
		return fieldResolution{}
	}

	if len(available) == 0 {
		if lt == nil {
			return fieldResolution{candidates: []string{label}}
		}
		return fieldResolution{candidates: []string{lt.ToVL(label)}}
	}

	var candidates []string
	addIfAvailable := func(name string) bool {
		if !containsString(available, name) {
			return false
		}
		candidates = appendUniqueString(candidates, name)
		return true
	}

	if lt != nil {
		if mapped, ok := lt.lokiToVL[label]; ok && addIfAvailable(mapped) {
			return fieldResolution{candidates: candidates}
		}
	}
	if label == "detected_level" && addIfAvailable("level") {
		return fieldResolution{candidates: candidates}
	}
	if addIfAvailable(label) {
		return fieldResolution{candidates: candidates}
	}
	if lt != nil {
		if dotted, ok := knownUnderscoreToDot[label]; ok && addIfAvailable(dotted) {
			return fieldResolution{candidates: candidates}
		}
	}

	for _, field := range available {
		translated := field
		if lt != nil {
			translated = lt.ToLoki(field)
		}
		if translated == label {
			candidates = appendUniqueString(candidates, field)
		}
	}
	return fieldResolution{
		candidates: candidates,
		ambiguous:  len(candidates) > 1,
	}
}

// ResolveMetadataCandidates resolves a detected field name to one or more native VL field
// names while preserving the current metadata exposure mode. Exact native matches win;
// otherwise translated metadata aliases are only used when uniquely resolvable.
func (lt *LabelTranslator) ResolveMetadataCandidates(fieldName string, available []string, mode MetadataFieldMode) fieldResolution {
	name := strings.TrimSpace(fieldName)
	if name == "" {
		return fieldResolution{}
	}
	if len(available) == 0 {
		return fieldResolution{}
	}
	if containsString(available, name) {
		return fieldResolution{candidates: []string{name}}
	}
	if lt == nil {
		return fieldResolution{}
	}

	var candidates []string
	for _, field := range available {
		for _, exposure := range lt.metadataFieldExposures(field, mode) {
			if exposure.name == name {
				candidates = appendUniqueString(candidates, field)
				break
			}
		}
	}
	return fieldResolution{
		candidates: candidates,
		ambiguous:  len(candidates) > 1,
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

func normalizeMetadataFieldMode(mode MetadataFieldMode) MetadataFieldMode {
	switch mode {
	case MetadataFieldModeNative, MetadataFieldModeTranslated, MetadataFieldModeHybrid:
		return mode
	default:
		return MetadataFieldModeHybrid
	}
}

func (lt *LabelTranslator) metadataFieldExposures(vlField string, mode MetadataFieldMode) []metadataFieldExposure {
	vlField = strings.TrimSpace(vlField)
	if vlField == "" {
		return nil
	}

	mode = normalizeMetadataFieldMode(mode)
	translated := vlField
	if lt != nil {
		translated = lt.ToLoki(vlField)
	}

	seen := make(map[string]struct{}, 2)
	result := make([]metadataFieldExposure, 0, 2)
	add := func(name string, isAlias bool) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		result = append(result, metadataFieldExposure{name: name, isAlias: isAlias})
	}

	switch mode {
	case MetadataFieldModeNative:
		add(vlField, false)
	case MetadataFieldModeTranslated:
		add(translated, translated != vlField)
	default:
		add(vlField, false)
		add(translated, translated != vlField)
	}

	return result
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
