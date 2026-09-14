package translator

import (
	"regexp"
	"strconv"
	"strings"
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
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	return re.SubexpNames()
}
