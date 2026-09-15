package proxy

import (
	"regexp"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func regexpCaptureFields(query string) map[string]bool {
	if !regexpParserStageRE.MatchString(query) {
		return nil
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return nil
	}
	return pipelineRegexpCaptureFields(lq.Pipeline)
}

func pipelineRegexpCaptureFields(pipeline []logqlpkg.Stage) map[string]bool {
	var names map[string]bool
	for _, stage := range pipeline {
		parser, ok := stage.(*logqlpkg.ParserStage)
		if !ok || parser.Type != logqlpkg.ParserRegexp {
			continue
		}
		re, err := regexp.Compile(parser.Param)
		if err != nil {
			continue
		}
		for _, name := range re.SubexpNames() {
			if name == "" {
				continue
			}
			if names == nil {
				names = make(map[string]bool)
			}
			names[name] = true
		}
	}
	return names
}

// Promote only named captures, keeping unrelated backend fields in metadata.
func promoteRegexpCaptureFields(names map[string]bool, sm, parsed map[string]string) map[string]string {
	for name := range names {
		if value, ok := sm[name]; ok {
			if parsed == nil {
				parsed = make(map[string]string)
			}
			parsed[name] = value
			delete(sm, name)
		}
	}
	return parsed
}

func mergeRegexpCaptureLabels(labels, parsed map[string]string, names map[string]bool) map[string]string {
	var result map[string]string
	for name := range names {
		value, ok := parsed[name]
		if !ok {
			continue
		}
		if result == nil {
			result = make(map[string]string, len(labels))
			for k, v := range labels {
				result[k] = v
			}
		}
		result[name] = value
	}
	if result != nil {
		return result
	}
	return labels
}
