package translate

import (
	"slices"
	"strings"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// ServiceNameGroupingPipes are the pipes that set the service_name field of every row to the service name Loki
// assigns: the first non-empty field of SyntheticServiceNameFields, else UnknownServiceNameValue. VictoriaLogs
// from v1.51 (caps.PipeCoalesce) has the coalesce pipe; older versions get the same value from format pipes that
// keep a non-empty destination. The text takes no argument but the capability, so both are built once.
func ServiceNameGroupingPipes(caps logsql.Capabilities) string {
	if caps.PipeCoalesce {
		return serviceNameCoalesce()
	}
	return serviceNameFormats()
}

var serviceNameCoalesce = sync.OnceValue(func() string {
	names := make([]string, len(SyntheticServiceNameFields))
	for i, field := range SyntheticServiceNameFields {
		names[i] = field
		if hasDot(field) {
			names[i] = `"` + field + `"`
		}
	}
	return "| coalesce(" + strings.Join(names, ", ") + ") default " + logsql.QuoteValue(UnknownServiceNameValue) + " as service_name"
})

var serviceNameFormats = sync.OnceValue(func() string {
	var b strings.Builder
	for _, field := range SyntheticServiceNameFields[1:] {
		b.WriteString(`| format "<` + field + `>" as service_name keep_original_fields `)
	}
	b.WriteString(`| format ` + logsql.QuoteValue(UnknownServiceNameValue) + ` as service_name keep_original_fields`)
	return b.String()
})

// NormalizeByLabels is the grouping labels of a vector aggregation as VictoriaLogs names them: each label is
// translated by the label function (nil keeps the names), except service_name and detected_level, which are
// derived per row and never a translated source field; empty names and repeats are dropped, in order.
func NormalizeByLabels(labels []string, label LabelTranslateFunc) []string {
	return AppendNormalizedLabels(make([]string, 0, len(labels)), labels, label)
}

// AppendNormalizedLabels appends NormalizeByLabels(labels, label) to dst and returns it; a name already among the
// ones it appended is not appended again. A caller with a small array for dst allocates nothing for the list.
func AppendNormalizedLabels(dst, labels []string, label LabelTranslateFunc) []string {
	start := len(dst)
	for _, part := range labels {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if label != nil && part != "service_name" && part != DetectedLevelLabel {
			part = strings.TrimSpace(label(part))
		}
		if part == "" || slices.Contains(dst[start:], part) {
			continue
		}
		dst = append(dst, part)
	}
	return dst
}

// JoinByLabels concatenates two grouping lists, dropping repeats and empty names.
func JoinByLabels(first, second []string) []string {
	return AppendJoinedLabels(make([]string, 0, len(first)+len(second)), first, second)
}

// AppendJoinedLabels appends JoinByLabels(first, second) to dst and returns it.
func AppendJoinedLabels(dst, first, second []string) []string {
	start := len(dst)
	for _, list := range [2][]string{first, second} {
		for _, part := range list {
			part = strings.TrimSpace(part)
			if part == "" || slices.Contains(dst[start:], part) {
				continue
			}
			dst = append(dst, part)
		}
	}
	return dst
}

func hasDot(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return true
		}
	}
	return false
}
