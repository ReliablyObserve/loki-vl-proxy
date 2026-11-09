package logql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// Line filters are checked the way Loki answers a pipeline it cannot build, as recorded with the Loki oracle
// (test/oracle/loki pipeline; TestLineFilterRunValidationMatchesLoki):
//
//   - A label filter, line_format and unpack split a pipeline into segments. The line filters of one segment are a
//     run, also across the stages a segment holds between them (json, logfmt, regexp, pattern, label_format, drop,
//     keep, decolorize).
//   - Segments are checked left to right: first the run, then the segment's other stages in order, then the stage
//     that ends the segment (a line_format template, an ip() label filter).
//   - A run is checked from its last filter. A negated `or` chain is one filter per alternative; a positive chain is
//     one filter whose alternatives are text (an ip() among them included), checked in order.
//   - The error quotes the whole run, each filter printed as flatString prints it.

// pipelineBuildError is the error for a pipeline whose stages Loki parses but refuses to build, or "". With
// lineIndependent, a line_format no later stage reads is skipped (see validateLogQuerySemantics).
func pipelineBuildError(stages []Stage, lineIndependent bool) string {
	for start := 0; start <= len(stages); {
		end := start
		for end < len(stages) && !endsLineFilterRun(stages[end]) {
			end++
		}
		segment := stages[start:end]
		if msg := lineFilterRunError(segment); msg != "" {
			return msg
		}
		for _, s := range segment {
			if _, ok := s.(*LineFilterStage); ok {
				continue
			}
			if msg := validateStageSemantics(s); msg != "" {
				return msg
			}
		}
		if end == len(stages) {
			break
		}
		if _, ok := stages[end].(*UnwrapStage); ok {
			break // the label filters after an unwrap are not built as pipeline stages (an ip() one is not checked)
		}
		switch s := stages[end].(type) {
		case *LabelFilterStage:
			if msg := ipLabelFilterError(s); msg != "" {
				return msg
			}
		case *LineFormatStage:
			if !lineIndependent || lineConsumedLater(stages[end+1:]) {
				if msg := validateStageSemantics(s); msg != "" {
					return msg
				}
			}
		}
		start = end + 1
	}
	return ""
}

// endsLineFilterRun reports whether a stage ends the run of line filters before it.
func endsLineFilterRun(s Stage) bool {
	switch st := s.(type) {
	case *LabelFilterStage, *LineFormatStage, *UnwrapStage:
		return true
	case *ParserStage:
		return st.Type == ParserUnpack
	}
	return false
}

// lineFilterRunError is the error for the run of line filters among stages, or "".
func lineFilterRunError(stages []Stage) string {
	for i := len(stages) - 1; i >= 0; i-- {
		lf, ok := stages[i].(*LineFilterStage)
		if !ok {
			continue
		}
		if err := lineFilterError(lf); err != nil {
			return lineFilterRunErrorText(stages, err)
		}
	}
	return ""
}

// lineFilterRunErrorText is stageError for the run of line filters among stages, written into one buffer.
func lineFilterRunErrorText(stages []Stage, err error) string {
	msg := err.Error()
	buf := make([]byte, 0, len("parse error : stage '' : ")+len(msg)+64)
	buf = append(buf, "parse error : stage '"...)
	buf = appendLineFilterRun(buf, stages)
	buf = append(buf, "' : "...)
	buf = append(buf, msg...)
	return string(buf)
}

// appendLineFilterRun appends the run of line filters among stages, each as flatString prints it.
func appendLineFilterRun(buf []byte, stages []Stage) []byte {
	start := len(buf)
	for _, s := range stages {
		if lf, ok := s.(*LineFilterStage); ok {
			if len(buf) > start {
				buf = append(buf, ' ')
			}
			buf = lf.appendFlat(buf)
		}
	}
	return buf
}

// appendFlat appends what flatString returns.
func (s *LineFilterStage) appendFlat(buf []byte) []byte {
	op := lineFilterOpText(s.Op)
	operand := func(buf []byte, value string, ip bool) []byte {
		if ip {
			buf = append(buf, "ip("...)
			buf = strconv.AppendQuote(buf, value)
			return append(buf, ')')
		}
		return strconv.AppendQuote(buf, value)
	}
	buf = append(buf, op...)
	buf = append(buf, ' ')
	buf = operand(buf, s.Value, s.IP)
	for _, alt := range s.Or {
		if s.negated() {
			buf = append(buf, ' ')
			buf = append(buf, op...)
			buf = append(buf, ' ')
		} else {
			buf = append(buf, " or "...)
		}
		buf = operand(buf, alt.Value, alt.IP)
	}
	return buf
}

// lineFilterError is the error for one line filter stage, or nil.
func lineFilterError(s *LineFilterStage) error {
	operand := func(i int) LineFilterAlt {
		if i == 0 {
			return LineFilterAlt{Value: s.Value, IP: s.IP}
		}
		return s.Or[i-1]
	}
	switch {
	case s.negated():
		for i := len(s.Or); i >= 0; i-- {
			alt := operand(i)
			if err := lineFilterOperandError(s.Op, alt.Value, alt.IP); err != nil {
				return err
			}
		}
	case len(s.Or) == 0:
		return lineFilterOperandError(s.Op, s.Value, s.IP)
	default:
		for i := 0; i <= len(s.Or); i++ {
			if err := lineFilterOperandError(s.Op, operand(i).Value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

var errIPOperation = errors.New("ip: invalid operation")

// lineFilterOperandError is the error for one operand of a line filter, or nil: an ip() operand takes |= and !=
// and a valid pattern, a regexp must compile, and a pattern may hold no captures in a row and only `<_>`.
func lineFilterOperandError(op LineFilterOp, value string, ip bool) error {
	if ip {
		if op != LineFilterContains && op != LineFilterExcludes {
			return errIPOperation
		}
		if !validIPPattern(value) {
			return fmt.Errorf("ip: invalid pattern: %q", value)
		}
		return nil
	}
	switch op {
	case LineFilterMatchRe, LineFilterExcludeRe:
		_, err := regexp.Compile(value)
		return err
	case LineFilterContainsPat, LineFilterExcludePat:
		return linePatternError(value)
	}
	return nil
}
