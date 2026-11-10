package translator

import (
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// regexpCaptureLabels returns the field names created by this pipeline stage.
// These names are query-local aliases, not normalized backend field names.
func regexpCaptureLabels(stage string) []string {
	if !strings.HasPrefix(stage, "regexp ") {
		return nil
	}
	quoted, _ := extractQuotedValue(stage[len("regexp "):])
	pattern, err := strconv.Unquote(quoted)
	if err != nil {
		return nil
	}
	return translate.RegexpCaptureNames(pattern)
}
