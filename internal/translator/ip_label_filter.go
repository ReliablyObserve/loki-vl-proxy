package translator

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"

// ipLabelFilter is translate.IPLabelFilter as LogsQL text.
func ipLabelFilter(field, pattern string, negate bool) (string, bool) {
	f, ok := translate.IPLabelFilter(field, pattern, negate)
	if !ok {
		return "", false
	}
	return f.String(), true
}
