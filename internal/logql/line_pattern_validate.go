package logql

import (
	"errors"
	"fmt"
)

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
