package translator

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var (
	extractionItemRE = regexp.MustCompile("^([A-Za-z_][A-Za-z0-9_]*)\\s*(?:=\\s*(?:\"([^\"]*)\"|`([^`]*)`))?$")
	jsonPlainPathRE  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

// parsedSeriesLabels returns the labels the parser stages of a metric query's
// log pipeline add to a Loki series, less the unwrapped one, and whether that
// set is known. A Loki series is the stream labels plus the labels its parsers
// extracted (pkg/logql/log/metrics_extraction.go, which drops the unwrapped
// label from the identity), so when every parser names the keys it extracts
// (`| logfmt amount, route`, a regexp with named groups, a pattern's captures)
// the series of a stream is told apart by those labels alone. A bare `| json`
// or `| logfmt` extracts every key of every line, a stage that creates, renames
// or removes labels (label_format, drop, keep) changes the identity in ways
// this does not follow, and a name that collides with a label the proxy derives
// (level, service_name) or with a label of the stream selector (which Loki
// renames `<key>_extracted`) is read differently: for those the set is not
// known. A stream label the selector does not name cannot be seen here.
func parsedSeriesLabels(query, unwrapField string) ([]string, bool) {
	rest := strings.TrimSpace(query)
	var selector []string
	if strings.HasPrefix(rest, "{") {
		end := findMatchingBrace(rest)
		if end < 0 {
			return nil, false
		}
		selector = selectorLabelNames(rest[1:end])
		rest = rest[end+1:]
	}
	var labels []string
	// A pipeline stage starts at an unquoted `|`; a line filter is the stage
	// `|= ...`, `|~ ...` or `|> ...`, and `!=`, `!~`, `!>` ones share the stage
	// before them.
	for rest = strings.TrimSpace(rest); rest != ""; {
		stage, tail := extractPipelineStage(strings.TrimPrefix(rest, "|"))
		if tail = strings.TrimSpace(tail); len(tail) >= len(rest) {
			return nil, false // a pattern filter (!>) or text this does not follow
		}
		rest = tail
		if stage == "" {
			continue
		}
		names, kind := stageSeriesLabels(stage)
		switch kind {
		case stageUnwrap:
			return labels, true
		case stageUnknown:
			return nil, false
		}
		for _, name := range names {
			switch {
			case name == "" || name == unwrapField:
			case name == "level" || name == detectedLevelLabelName || name == "service_name":
				return nil, false
			case slices.Contains(selector, name):
				// A parsed key that is also a stream label becomes
				// `<key>_extracted` on Loki (pkg/logql/log/labels.go): the name the
				// grouping would give the series is not the one Loki gives it.
				return nil, false
			case !containsStr(labels, name):
				labels = append(labels, name)
			}
		}
	}
	return labels, true
}

type seriesStageKind int

const (
	stageNoLabels seriesStageKind = iota // adds no label to the series: a filter, line_format
	stageLabels                          // names the labels it extracts
	stageUnwrap                          // the unwrap stage, which ends the pipeline of a series
	stageUnknown                         // extracts every key, or changes labels in a way not followed
)

// stageSeriesLabels classifies a pipeline stage and returns the labels it
// extracts when it names them.
func stageSeriesLabels(stage string) ([]string, seriesStageKind) {
	name, args, _ := strings.Cut(stage, " ")
	switch {
	case stage[0] == '=' || stage[0] == '~' || stage[0] == '>' || stage[0] == '!':
		return nil, stageNoLabels // a line filter
	case name == "unwrap":
		return nil, stageUnwrap
	case name == "json" || name == "logfmt":
		return extractionListLabels(name, args)
	case name == "regexp" || name == "pattern":
		return capturedLabels(name, args)
	case name == "line_format" || name == "decolorize":
		return nil, stageNoLabels
	case name == "unpack" || name == "label_format" || name == "drop" || name == "keep" || name == "distinct":
		return nil, stageUnknown
	case strings.ContainsAny(stage, "=<>!~"):
		return nil, stageNoLabels // a label filter
	}
	return nil, stageUnknown
}

// extractionListLabels is the labels of `| json a, b="x.y"` / `| logfmt a, b`;
// a bare parser, which extracts every key, is not known. An alias
// (`label="key"`) is known for a json path of plain keys only, which the
// translation copies into a field of the label's name; a logfmt alias and a json
// path with an index or bracket create no such field.
func extractionListLabels(parser, args string) ([]string, seriesStageKind) {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil, stageUnknown
	}
	var names []string
	for _, item := range splitCSV(args) {
		m := extractionItemRE.FindStringSubmatch(strings.TrimSpace(item))
		if m == nil {
			return nil, stageUnknown
		}
		if expr := m[2] + m[3]; strings.TrimSpace(item) != m[1] && (parser != "json" || !jsonPlainPathRE.MatchString(expr)) {
			return nil, stageUnknown
		}
		names = append(names, m[1])
	}
	return names, stageLabels
}

// capturedLabels is the named groups of a regexp stage and the captures of a
// pattern stage.
func capturedLabels(name, args string) ([]string, seriesStageKind) {
	quoted, after := extractQuotedValue(args)
	pattern, err := strconv.Unquote(quoted)
	if err != nil || strings.TrimSpace(after) != "" {
		return nil, stageUnknown
	}
	if name == "regexp" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, stageUnknown
		}
		return re.SubexpNames(), stageLabels
	}
	var names []string
	for _, m := range patternCaptureRE.FindAllStringSubmatch(pattern, -1) {
		names = append(names, m[1])
	}
	return names, stageLabels
}

// selectorLabelNames is the label names of a stream selector's matchers.
func selectorLabelNames(content string) []string {
	var names []string
	for _, matcher := range splitStreamMatchers(content) {
		for _, op := range streamMatcherOps {
			if idx := matcherOperatorIndex(matcher, op.logql); idx > 0 {
				names = append(names, strings.Trim(strings.TrimSpace(matcher[:idx]), "`\""))
				break
			}
		}
	}
	return names
}
