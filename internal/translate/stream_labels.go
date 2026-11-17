package translate

import "strings"

// StreamLabelsProbe is the label name the proxy's translation function answers
// with the stream label names to read as stream values (see WithStreamLabels).
const StreamLabelsProbe = "\x03"

// WithStreamLabels wraps a label translation function so the translator learns
// the names (Loki label names) that are stream labels of the tenant.
func WithStreamLabels(labelFn LabelTranslateFunc, names []string) LabelTranslateFunc {
	if len(names) == 0 {
		return labelFn
	}
	joined := StreamLabelsProbe + "=" + strings.Join(names, ",")
	return func(label string) string {
		if label == StreamLabelsProbe {
			return joined
		}
		if labelFn == nil {
			return label
		}
		return labelFn(label)
	}
}
