package querycorpus_test

import (
	"encoding/base64"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The reference for the codec's marker reading: the two helpers the proxy called on a translated query before the
// physical-plan codec, copied unchanged from translator.go at e4a743f67 (v2.93.0, the base of the codec's pull
// request). The translator's own ParseWithoutMarker has since been made faster; these keep the old behaviour as the
// oracle, so a change to the translator cannot move the reference with it.

const (
	legacyWithoutMarker = "__without__:"
	legacyLabelReplace  = "__lvp_lr:"
)

func legacyParseWithoutMarker(query string) (cleanQuery, op string, excludeLabels []string) {
	idx := strings.LastIndex(query, legacyWithoutMarker)
	if idx < 0 {
		return query, "", nil
	}
	cleanQuery = query[:idx]
	labelStr := query[idx+len(legacyWithoutMarker):]
	// A label_replace marker appended after this one is
	// not part of the label list: leave it in the clean query.
	end := len(labelStr)
	for _, other := range []string{legacyLabelReplace} {
		if i := strings.Index(labelStr, other); i >= 0 && i < end {
			end = i
		}
	}
	cleanQuery += labelStr[end:]
	labelStr = labelStr[:end]
	if opEnd := strings.Index(labelStr, ":"); opEnd >= 0 {
		op, labelStr = labelStr[:opEnd], labelStr[opEnd+1:]
	}
	for _, l := range strings.Split(labelStr, ",") {
		l = strings.TrimSpace(l)
		if l != "" {
			excludeLabels = append(excludeLabels, l)
		}
	}
	return cleanQuery, op, excludeLabels
}

func legacyParseAllLabelReplaceMarkers(query string) (string, []translator.LabelReplaceSpec) {
	var specs []translator.LabelReplaceSpec
	for {
		idx := strings.Index(query, legacyLabelReplace)
		if idx < 0 {
			break
		}
		tail := query[idx+len(legacyLabelReplace):]
		// The marker ends at the next "__" that closes it. Base64 chars are
		// [A-Za-z0-9+/=], so "__" cannot appear inside the payload.
		endIdx := strings.Index(tail, "__")
		if endIdx < 0 {
			break
		}
		payload := tail[:endIdx]
		b, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			break
		}
		parts := strings.SplitN(string(b), "\x00", 4)
		if len(parts) != 4 {
			break
		}
		specs = append(specs, translator.LabelReplaceSpec{
			DstLabel:    parts[0],
			Replacement: parts[1],
			SrcLabel:    parts[2],
			Regex:       parts[3],
		})
		// Strip this marker from the query and continue looking for more.
		query = query[:idx] + query[idx+len(legacyLabelReplace)+endIdx+len("__"):]
	}
	return query, specs
}

// legacySplit is the proxy's two-step strip as it ran before the codec: the without marker, then the label_replace
// markers of what is left.
func legacySplit(s string) (clean, op string, labels []string, specs []translator.LabelReplaceSpec) {
	clean, op, labels = legacyParseWithoutMarker(s)
	clean, specs = legacyParseAllLabelReplaceMarkers(clean)
	return clean, op, labels, specs
}
