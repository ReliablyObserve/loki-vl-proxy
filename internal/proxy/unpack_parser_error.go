package proxy

import (
	"errors"
	"strconv"
	"strings"

	"github.com/grafana/jsonparser"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// errUnpackNotObject is Loki's __error_details__ of a line an unpack rejects because it opens with no brace.
var errUnpackNotObject = errors.New("expecting json object(6), but it is not")

// unpackLineError is the error Loki's unpack sets on a line, nil when it sets none: an empty line goes on as it is,
// a line that does not open with a brace is no JSON object, and an object fails where reading its members or one of
// its string values does (recorded answers: testdata/unpack_error_lines.json).
func unpackLineError(line string) error {
	if line == "" {
		return nil
	}
	if line[0] != '{' {
		return errUnpackNotObject
	}
	return jsonparser.ObjectEach([]byte(line), func(_, value []byte, kind jsonparser.ValueType, _ int) error {
		if kind == jsonparser.String {
			_, err := jsonparser.ParseString(value)
			return err
		}
		return nil
	})
}

// unwrapPackedLabels adds the labels an unpack reads from a line: the string members of a line a packing agent
// wrote, an object whose top-level `_entry` is a string, and none of any other line.
func unwrapPackedLabels(line string, add func(name, value string, last bool)) {
	if _, kind, _, err := jsonparser.Get([]byte(line), "_entry"); err != nil || kind != jsonparser.String {
		return
	}
	_ = jsonparser.ObjectEach([]byte(line), func(key, value []byte, kind jsonparser.ValueType, _ int) error {
		if kind == jsonparser.String && string(key) != "_entry" {
			add(orderedJSONLabelName(string(key), true), orderedJSONScalar(value, kind), true)
		}
		return nil
	})
}

// unpackErrorProbeFor describes a count, a rate or a byte count over a log query whose only parser is an unpack and
// whose parser errors reach the result (semantics/unpack-parser-error-not-raised); ok is false when they cannot:
// a sum with no grouping (Loki parses no line for it), a label filter on __error__ or a drop of it after the
// parser, and, left to the shapes the proxy does not answer as Loki yet, a label filter or another stage that reads
// what the unpack made.
func unpackErrorProbeFor(ra *logqlpkg.RangeAggregation, parent *logqlpkg.VectorAggregation) (jsonErrorProbe, bool) {
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok || ra.Grouping != nil || ra.RangeDuration <= 0 {
		return jsonErrorProbe{}, false
	}
	at := -1
	for i, stage := range lq.Pipeline {
		if p, isParser := stage.(*logqlpkg.ParserStage); isParser {
			if p.Type != logqlpkg.ParserUnpack || at >= 0 {
				return jsonErrorProbe{}, false
			}
			at = i
		}
	}
	if at < 0 {
		return jsonErrorProbe{}, false
	}
	switch ra.Op {
	case logqlpkg.RangeRate, logqlpkg.RangeCountOverTime, logqlpkg.RangeBytesRate, logqlpkg.RangeBytesOverTime:
	default:
		return jsonErrorProbe{}, false
	}
	plan := &orderedJSONMetricPlan{parser: "json", function: string(ra.Op), window: ra.RangeDuration}
	if parent != nil && parent.Op == logqlpkg.VectorSum {
		if g := parent.Grouping; g == nil || !g.Without && len(g.Labels) == 0 {
			return jsonErrorProbe{}, false
		}
		plan.aggregated, plan.grouping = true, parent.Grouping
	}
	matchers := make([]string, 0, len(lq.Selector.Matchers))
	for _, m := range lq.Selector.Matchers {
		op := []string{"=", "!=", "=~", "!~"}[int(m.Op)]
		matchers = append(matchers, m.Name+op+strconv.Quote(m.Value))
	}
	plan.selector = "{" + strings.Join(matchers, ",") + "}"
	retained, _, _, ok := plan.compilePipeline(lq.Pipeline[:at])
	if !ok {
		return jsonErrorProbe{}, false
	}
	plan.fetchQuery = plan.selector + retained
	plan.stages = append(plan.stages, orderedJSONStage{unpack: true, parser: true})
	for _, stage := range lq.Pipeline[at+1:] {
		switch stage.(type) {
		case *logqlpkg.LineFilterStage, *logqlpkg.DropStage, *logqlpkg.KeepStage:
		default:
			return jsonErrorProbe{}, false
		}
		compiled, ok := compileOrderedJSONStage(stage)
		if !ok {
			return jsonErrorProbe{}, false
		}
		plan.stages = append(plan.stages, compiled)
	}
	plan.setDetectedLevel()
	if plan.dropsParserErrors() {
		return jsonErrorProbe{}, false
	}
	return jsonErrorProbe{plan: plan, window: ra.RangeDuration, offset: ra.OffsetDuration}, true
}

// unpacksLine reports a plan of the unpack error check (unpackErrorProbeFor), whose only parser is an unpack.
func (plan *orderedJSONMetricPlan) unpacksLine() bool {
	for _, stage := range plan.stages {
		if stage.unpack {
			return true
		}
	}
	return false
}
