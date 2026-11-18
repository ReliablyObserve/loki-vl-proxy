package logql

import "sort"

// ArgumentReader reads the first arguments of the calls nested in one text (FirstArgumentLen) in time linear in the
// text, whatever the number of calls: it records once where each bracket closes, and a read jumps over the brackets
// instead of scanning them again for every call around them.
type ArgumentReader struct {
	s string
	// opens are the offsets of the brackets outside strings, in order, and closes the offset of the bracket that
	// closes each (any closing bracket closes the last one open, as FirstArgumentLen counts them), or -1.
	opens, closes []int32
}

// NewArgumentReader records the brackets of s.
func NewArgumentReader(s string) *ArgumentReader {
	r := &ArgumentReader{s: s}
	var open []int32
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			open = append(open, int32(len(r.opens)))
			r.opens = append(r.opens, int32(i))
			r.closes = append(r.closes, -1)
		case c == ')' || c == ']' || c == '}':
			if len(open) > 0 {
				r.closes[open[len(open)-1]] = int32(i)
				open = open[:len(open)-1]
			}
		}
	}
	return r
}

// FirstArgumentLen is FirstArgumentLen(s[from:]) for an offset from outside the strings of s.
func (r *ArgumentReader) FirstArgumentLen(from int) int {
	s := r.s
	var quote byte
	for i := from; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			k := sort.Search(len(r.opens), func(k int) bool { return r.opens[k] >= int32(i) })
			if r.closes[k] < 0 {
				return len(s) - from
			}
			i = int(r.closes[k])
		case c == ')' || c == ']' || c == '}', c == ',':
			return i - from
		}
	}
	return len(s) - from
}
