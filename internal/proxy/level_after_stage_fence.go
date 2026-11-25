package proxy

import "strings"

// levelAfterStageResponseVersion fences the answers of a filter on detected_level after a stage, cached while the
// filter read the field a parser, a drop or a formatting stage leaves (semantics/level-filter-after-stage). It is on
// log and metric keys alike, in the exact, compatibility and window caches and their copies on cache peers.
const levelAfterStageResponseVersion = "@level-filter-after-stage-v1:"

// levelAfterStageWords are the words of the stages a filter on detected_level changed its answer behind.
var levelAfterStageWords = [...]string{"json", "logfmt", "regexp", "pattern", "drop", "keep", "decolorize", "line_format", "label_format"}

// levelAfterStageResponseKeyVersion is the fence of a query with the label name where a label filter starts (after a
// pipe and blanks) and the word of one of those stages ahead of it, in any spelling (containsWord). It reads the text
// alone, with no parse and no allocation: a query without the name costs one substring search. A comment or a
// character outside ASCII before the name counts as a filter's start. It fires on more than the changed answers,
// which costs such a query one cold miss.
func levelAfterStageResponseKeyVersion(query string) string {
	for from := 0; ; {
		i := strings.Index(query[from:], detectedLevelLabel)
		if i < 0 {
			return ""
		}
		i += from
		from = i + len(detectedLevelLabel)
		j := i - 1
		for j >= 0 && (query[j] == ' ' || query[j] >= '\t' && query[j] <= '\r') {
			j--
		}
		if j < 0 || query[j] != '|' && query[j] < 0x80 && strings.IndexByte(query[:i], '#') < 0 {
			continue
		}
		head := query[:i]
		other := spelledOtherwise(head)
		for _, w := range levelAfterStageWords {
			if containsWord(head, w, other) {
				return levelAfterStageResponseVersion
			}
		}
	}
}
