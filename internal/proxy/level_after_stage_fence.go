package proxy

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// levelAfterStageResponseVersion fences the answers of a filter on detected_level after a stage, cached while the
// filter read the field a parser, a drop or a formatting stage leaves (semantics/level-filter-after-stage). It is on
// log and metric keys alike, in the exact, compatibility and window caches and their copies on cache peers.
const levelAfterStageResponseVersion = "@level-filter-after-stage-v1:"

// levelAfterStageWords are the words of the stages a filter on detected_level changed its answer behind.
var levelAfterStageWords = [...]string{"json", "logfmt", "regexp", "pattern", "drop", "keep", "decolorize", "line_format", "label_format"}

// levelAfterStageResponseKeyVersion is the fence of a query whose plan writes a filter on detected_level after a
// stage (lower.WritesLevelAfterStage), read from the query's resolved plan after a screen of the text: the label name
// (case-sensitive), a pipe and the word of one of those stages in any spelling (containsWord). A query without the
// name costs one substring search.
func levelAfterStageResponseKeyVersion(query string) string {
	if !strings.Contains(query, detectedLevelLabel) || strings.IndexByte(query, '|') < 0 {
		return ""
	}
	other := spelledOtherwise(query)
	for _, w := range levelAfterStageWords {
		if !containsWord(query, w, other) {
			continue
		}
		if plan := resolvedPlan(fenceVariablesResolved(query)); plan != nil && lower.WritesLevelAfterStage(plan) {
			return levelAfterStageResponseVersion
		}
		return ""
	}
	return ""
}
