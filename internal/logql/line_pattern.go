package logql

// LinePatternNode is a literal or a capture of the pattern of a pattern line filter (|> or !>).
type LinePatternNode struct {
	// Text is the literal; empty for a capture.
	Text    string
	Capture bool
	// Name is the name of a capture: `_` for <_>.
	Name string
}

// LinePatternNodes splits the pattern of a pattern line filter as Loki reads it: <name>, a letter or underscore
// then letters, digits and underscores, is a capture, and every other rune is literal.
func LinePatternNodes(p string) []LinePatternNode {
	var nodes []LinePatternNode
	lit := 0
	for i := 0; i < len(p); i++ {
		n := linePatternCaptureLen(p[i:])
		if n == 0 {
			continue
		}
		if lit < i {
			nodes = append(nodes, LinePatternNode{Text: p[lit:i]})
		}
		nodes = append(nodes, LinePatternNode{Capture: true, Name: p[i+1 : i+n-1]})
		i += n - 1
		lit = i + 1
	}
	if lit < len(p) {
		nodes = append(nodes, LinePatternNode{Text: p[lit:]})
	}
	return nodes
}

func linePatternCaptureLen(s string) int {
	word := func(c byte, digits bool) bool {
		return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (digits && c >= '0' && c <= '9')
	}
	if len(s) < 3 || s[0] != '<' || !word(s[1], false) {
		return 0
	}
	for j := 2; j < len(s); j++ {
		if s[j] == '>' {
			return j + 1
		}
		if !word(s[j], true) {
			return 0
		}
	}
	return 0
}
