// Package blankgrouping is test support: it writes a query's `by` clauses that hold only blanks in the spelling the
// string translator reads as Loki does. Tests of several packages (proxy, querycorpus) use it; nothing in the proxy
// imports it.
package blankgrouping

import "strings"

// Cut is q with the blanks cut out of every `by` clause that holds nothing else, outside quoted strings:
// the spelling `by ()` the string translator reads as Loki does (semantics/translator-reads-blank-grouping-as-none).
// The metric flip gate and the plan shadow hold the translator, which the rollback flag serves, to the plan's text of
// that spelling.
func Cut(q string) string {
	var out strings.Builder
	for i := 0; i < len(q); i++ {
		switch c := q[i]; {
		case c == '"' || c == '`':
			j := quoteEnd(q, i)
			out.WriteString(q[i : j+1])
			i = j
		case c == 'b' && blankByEnd(q, i) > 0:
			end := blankByEnd(q, i)
			open := strings.IndexByte(q[i:end], '(') + i
			out.WriteString(q[i:open+1] + ")")
			i = end - 1
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// quoteEnd is the index of the byte that closes the string q[i] opens, or the last byte of q when it is not closed.
func quoteEnd(q string, i int) int {
	quote := q[i]
	j := i + 1
	for j < len(q) && q[j] != quote {
		if quote == '"' && q[j] == '\\' {
			j++
		}
		j++
	}
	return min(j, len(q)-1)
}

// blankByEnd is the index after `)` when q[i:] is a `by` clause of at least one blank and nothing else between its
// parentheses (a word of its own: not the end of a longer name), and 0 when it is not.
func blankByEnd(q string, i int) int {
	isBlank := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	isName := func(c byte) bool {
		return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
	}
	if !strings.HasPrefix(q[i:], "by") || i > 0 && isName(q[i-1]) || i+2 < len(q) && isName(q[i+2]) {
		return 0
	}
	j := i + 2
	for j < len(q) && isBlank(q[j]) {
		j++
	}
	if j >= len(q) || q[j] != '(' {
		return 0
	}
	k := j + 1
	for k < len(q) && isBlank(q[k]) {
		k++
	}
	if k == j+1 || k >= len(q) || q[k] != ')' {
		return 0
	}
	return k + 1
}
