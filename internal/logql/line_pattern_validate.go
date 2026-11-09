package logql

import (
	"errors"
	"fmt"
)

// patternLineFilterError is the rejection Loki answers for the patterns of a pattern line filter stage (|> or !>),
// or "". Recorded with the Loki oracle (test/oracle/loki pipeline, Loki's exported pipeline build): a pattern may not
// hold two captures in a row, and its only capture is `<_>`. Within one pattern the captures in a row are reported
// first; a `|>` chain is read in order, and a negated chain, which Loki builds as one `!>` per alternative, from its
// last alternative.
func patternLineFilterError(s *LineFilterStage) string {
	if s.Op != LineFilterContainsPat && s.Op != LineFilterExcludePat {
		return ""
	}
	n := len(s.Or) + 1
	value := func(i int) string {
		if i == 0 {
			return s.Value
		}
		return s.Or[i-1].Value
	}
	for k := 0; k < n; k++ {
		i := k
		if s.negated() {
			i = n - 1 - k
		}
		if err := linePatternError(value(i)); err != nil {
			return stageError(s.flatString(), err)
		}
	}
	return ""
}

// linePatternError is Loki's error for a pattern of a pattern line filter, or nil. It reads the pattern as
// LinePatternNodes does, without building the nodes.
func linePatternError(p string) error {
	named := ""
	prev, prevEnd := "", -1
	for i := 0; i < len(p); i++ {
		n := linePatternCaptureLen(p[i:])
		if n == 0 {
			continue
		}
		name := p[i+1 : i+n-1]
		if prevEnd == i {
			return fmt.Errorf("found consecutive capture '<%s><%s>': invalid expression", prev, name)
		}
		if named == "" && name != "_" {
			named = name
		}
		prev, prevEnd = name, i+n
		i += n - 1
	}
	if named != "" {
		return errors.New("named captures are not allowed: found '<" + named + ">'")
	}
	return nil
}
