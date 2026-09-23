package logql

import (
	"regexp/syntax"
	"strings"
)

// ParserCaptureLabels returns the labels the `| regexp` and `| pattern` stages
// of a query extract, in pipeline order and without duplicates.
//
// Loki names the series of a metric query with the stream labels plus every
// label the pipeline extracted, so a metric over such a pipeline has one series
// per distinct capture value. Unlike `| json` and `| logfmt`, whose key set is
// known only once a line is read, these two stages name their captures in the
// query itself, so a backend can be asked to group by them.
func ParserCaptureLabels(query string) []string {
	expr, err := Parse(strings.TrimSpace(query))
	if err != nil {
		return nil
	}
	var labels []string
	seen := map[string]struct{}{}
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		labels = append(labels, name)
	}
	var visit func(Expr)
	visit = func(node Expr) {
		switch n := node.(type) {
		case *LogQuery:
			for _, stage := range n.Pipeline {
				parser, ok := stage.(*ParserStage)
				if !ok {
					continue
				}
				switch parser.Type {
				case ParserRegexp:
					for _, name := range regexpCaptureNames(parser.Param) {
						add(name)
					}
				case ParserPattern:
					for _, name := range patternCaptureNames(parser.Param) {
						add(name)
					}
				}
			}
		case *RangeAggregation:
			visit(n.Inner)
		case *VectorAggregation:
			visit(n.Inner)
		}
	}
	visit(expr)
	return labels
}

// regexpCaptureNames returns the named capture groups of a Go regexp, the
// labels Loki's `| regexp` stage extracts (an unnamed group extracts nothing).
func regexpCaptureNames(pattern string) []string {
	reg, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	var names []string
	var walk func(*syntax.Regexp)
	walk = func(r *syntax.Regexp) {
		if r == nil {
			return
		}
		if r.Op == syntax.OpCapture && r.Name != "" {
			names = append(names, r.Name)
		}
		for _, sub := range r.Sub {
			walk(sub)
		}
	}
	walk(reg)
	return names
}

// patternCaptureNames returns the named placeholders of a Loki pattern
// expression: `<name>` extracts a label, `<_>` skips one.
func patternCaptureNames(pattern string) []string {
	var names []string
	rest := pattern
	for {
		open := strings.IndexByte(rest, '<')
		if open < 0 {
			return names
		}
		rest = rest[open+1:]
		closeIdx := strings.IndexByte(rest, '>')
		if closeIdx < 0 {
			return names
		}
		name := rest[:closeIdx]
		rest = rest[closeIdx+1:]
		if name == "_" || !isPatternCaptureName(name) {
			continue
		}
		names = append(names, name)
	}
}

func isPatternCaptureName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
