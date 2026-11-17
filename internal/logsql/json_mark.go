package logsql

import (
	"sort"
	"strings"
)

// JSONMark marks, in a query that unpacks JSON lines, the lines VictoriaLogs cannot parse as one JSON object, in
// the same pass as the query's own reads (the proxy's JSON parser error check rides on them).
//
// A sentinel member is spliced into the line after its first brace, so the query's own unpack_json sets the sentinel
// exactly on a line it parses as one JSON object: a line that opens with a brace (after JSON whitespace) has its
// opening brace first, and a line that does not stays no JSON object whatever the splice did. The empty object `{}`
// gets the member without the comma that would break it. The splice and the restore are literal replacements of the
// first occurrence: no regular expression runs on the lines.
//
// The mark goes around the query's first plain unpack_json (not one read from another field, under a prefix or a
// condition, such as a level derivation's). A stats query groups by the sentinel in every stats pipe after it: a
// group whose sentinel is empty holds the marked lines, and the other groups aggregate exactly the lines they did.
// The line is restored after the unpack when a later pipe reads it. A raw-row read sets Flag to "1" on a marked line
// and deletes the sentinel, the line restored.
//
// The query's own text is kept as written: the mark is inserted at the offsets of the tokens the scanner finds, so
// every reader of the text (a prune of a level chain, a cache key) sees the pipes it sent before.
type JSONMark struct {
	Sentinel string // the member the splice adds, unpacked as a field
	Flag     string // set to "1" on a marked raw row
}

// splice is the pipes that add the sentinel member to field.
func (m JSONMark) splice(field string) []Pipe {
	member := `{"` + m.Sentinel + `":1`
	return []Pipe{
		PipeReplace{Field: field, Old: "{", New: member + ",", Limit: 1},
		PipeReplace{Field: field, Old: member + ",}", New: member + "}", Limit: 1},
	}
}

// restore is the pipes that take the member out of _msg again.
func (m JSONMark) restore() []Pipe {
	member := `{"` + m.Sentinel + `":1`
	return []Pipe{
		PipeReplace{Field: "_msg", Old: member + ",", New: "{", Limit: 1},
		PipeReplace{Field: "_msg", Old: member + "}", New: "{}", Limit: 1},
	}
}

// pipesText is the text of pipes, each after a space.
func pipesText(pipes []Pipe) string {
	var b []byte
	for _, p := range pipes {
		b = AppendPipe(append(b, ' '), p)
	}
	return string(b)
}

// textScan walks src outside its quoted strings (double, single and back quotes, with backslash escapes in the first
// two): visit gets each byte offset and the parenthesis depth there; it returns false at an unterminated quote.
func textScan(src string, visit func(i, depth int) bool) bool {
	depth := 0
	for i := 0; i < len(src); i++ {
		switch c := src[i]; c {
		case '"', '\'', '`':
			j := i + 1
			for ; j < len(src) && src[j] != c; j++ {
				if src[j] == '\\' && c != '`' {
					j++
				}
			}
			if j >= len(src) {
				return false
			}
			i = j
			continue
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		}
		if !visit(i, depth) {
			return true
		}
	}
	return true
}

// markedQuery is a query the mark can go around: its text, its pipes' offsets and the index of the unpack.
type markedQuery struct {
	src     string
	q       *Query
	pipes   []int // the offset of each top-level pipe's "|"
	at      int   // the index of the unpack in q.Pipes
	inserts []insert
}

type insert struct {
	at   int
	text string
}

// parseForMark reads query for the mark; ok is false when it does not parse, when the top-level pipes of its text
// are not the parser's, when no plain unpack_json is in it, or when it names the sentinel already.
func (m JSONMark) parseForMark(query string) (*markedQuery, string) {
	if strings.Contains(query, m.Sentinel) {
		return nil, RefusedMarked
	}
	q, err := Parse(query)
	if err != nil {
		return nil, RefusedParse
	}
	k := &markedQuery{src: query, q: q, at: -1}
	ok := textScan(query, func(i, depth int) bool {
		if query[i] == '|' && depth == 0 {
			k.pipes = append(k.pipes, i)
		}
		return true
	})
	if !ok || len(k.pipes) != len(q.Pipes) {
		return nil, RefusedPipes
	}
	for i, p := range q.Pipes {
		if u, isUnpack := p.(PipeUnpackJSON); isUnpack && u.From == "" && u.ResultPrefix == "" && u.If == nil {
			k.at = i
			break
		}
	}
	if k.at < 0 {
		return nil, RefusedNoUnpack
	}
	return k, ""
}

// The reasons a query is left unmarked (StatsRefusal, RowsRefusal).
const (
	RefusedMarked   = "the query names the sentinel"
	RefusedParse    = "the query does not parse"
	RefusedPipes    = "the text's pipes are not the parser's"
	RefusedNoUnpack = "no plain unpack_json"
	RefusedFields   = "the unpack's field list or a stats grouping cannot take the sentinel"
	RefusedNoStats  = "no stats pipe after the unpack"
	RefusedAfter    = "a pipe after the stats could mix or drop the marked groups"
	RefusedDropped  = "a stats pipe after the unpack drops the raw rows' mark"
	RefusedResult   = "the marked text does not parse"
)

// pipeText is the offsets [lo, hi) of pipe i's text, its trailing space left out.
func (k *markedQuery) pipeText(i int) (lo, hi int) {
	lo, hi = k.pipes[i], len(k.src)
	if i+1 < len(k.pipes) {
		hi = k.pipes[i+1]
	}
	for hi > lo && strings.ContainsRune(" \t\n\r", rune(k.src[hi-1])) {
		hi--
	}
	return lo, hi
}

// addToList inserts name into the parenthesised list whose "(" is at offset open.
func (k *markedQuery) addToList(open int, name string) bool {
	closing, empty := -1, true
	textScan(k.src[open:], func(i, depth int) bool {
		switch {
		case depth == 0 && k.src[open+i] == ')':
			closing = open + i
			return false
		case i > 0 && !strings.ContainsRune(" \t\n\r", rune(k.src[open+i])):
			empty = false
		}
		return true
	})
	if closing < 0 {
		return false
	}
	if empty {
		k.inserts = append(k.inserts, insert{closing, name})
	} else {
		k.inserts = append(k.inserts, insert{closing, ", " + name})
	}
	return true
}

// word reports whether the text at offset i is word followed by a space or "(", and the offset after the spaces
// that follow it.
func (k *markedQuery) word(i int, word string) (int, bool) {
	if !strings.HasPrefix(k.src[i:], word) {
		return i, false
	}
	j := i + len(word)
	if j < len(k.src) && k.src[j] != ' ' && k.src[j] != '(' {
		return i, false
	}
	for j < len(k.src) && k.src[j] == ' ' {
		j++
	}
	return j, true
}

// markUnpack adds the splice before the unpack and the sentinel to its field list.
func (k *markedQuery) markUnpack(m JSONMark) bool {
	k.inserts = append(k.inserts, insert{k.pipes[k.at], strings.TrimPrefix(pipesText(m.splice("_msg")), " ") + " "})
	if len(k.q.Pipes[k.at].(PipeUnpackJSON).Fields) == 0 {
		return true
	}
	lo, hi := k.pipeText(k.at)
	at := -1
	textScan(k.src[lo:hi], func(i, depth int) bool {
		if depth == 0 && i > 0 && k.src[lo+i-1] == ' ' {
			if j, ok := k.word(lo+i, "fields"); ok && j < hi && k.src[j] == '(' {
				at = j
				return false
			}
		}
		return true
	})
	return at >= 0 && k.addToList(at, m.Sentinel)
}

// addToGrouping adds name to the grouping of stats pipe i, writing one when it has none.
func (k *markedQuery) addToGrouping(i int, name string) bool {
	lo, _ := k.pipeText(i)
	j := lo + 1
	for j < len(k.src) && k.src[j] == ' ' {
		j++
	}
	j, ok := k.word(j, "stats")
	if !ok {
		return false
	}
	if by, isBy := k.word(j, "by"); isBy && by < len(k.src) && k.src[by] == '(' {
		return k.addToList(by, name)
	}
	if j < len(k.src) && k.src[j] == '(' {
		return k.addToList(j, name)
	}
	k.inserts = append(k.inserts, insert{j, "by (" + name + ") "})
	return true
}

// checked is the marked text, or the refusal when the text does not parse with the inserts.
func (k *markedQuery) checked() (string, string) {
	out := k.text()
	if _, err := Parse(out); err != nil {
		return "", RefusedResult
	}
	return out, ""
}

// keepField adds name to the field list of fields pipe i.
func (k *markedQuery) keepField(i int, name string) {
	_, hi := k.pipeText(i)
	k.inserts = append(k.inserts, insert{hi, ", " + name})
}

// text is the source with the inserts.
func (k *markedQuery) text() string {
	sort.SliceStable(k.inserts, func(a, b int) bool { return k.inserts[a].at > k.inserts[b].at })
	out := k.src
	for _, in := range k.inserts {
		out = out[:in.at] + in.text + out[in.at:]
	}
	return out
}

// MarkStats is a stats query grouped by the sentinel in every stats pipe after the unpack; ok is false when the
// query has no stats pipe after the unpack, a pipe after the stats that could mix or drop the marked groups (only
// stats, math and filter pipes may follow; StatsRefusal names the reason). A fields pipe between the unpack and the
// stats (a keep stage) keeps the sentinel.
func (m JSONMark) MarkStats(query string) (string, bool) {
	out, why := m.markStats(query)
	if why != "" {
		return query, false
	}
	return out, true
}

// StatsRefusal is why MarkStats leaves query unmarked, "" when it marks it.
func (m JSONMark) StatsRefusal(query string) string {
	_, why := m.markStats(query)
	return why
}

func (m JSONMark) markStats(query string) (string, string) {
	k, why := m.parseForMark(query)
	if why != "" {
		return "", why
	}
	if !k.markUnpack(m) {
		return "", RefusedFields
	}
	stats, readsLine := false, false
	for i := k.at + 1; i < len(k.q.Pipes); i++ {
		switch k.q.Pipes[i].(type) {
		case PipeStats:
			if !k.addToGrouping(i, m.Sentinel) {
				return "", RefusedFields
			}
			stats = true
		case PipeMath, PipeFilter:
		case PipeFields:
			if stats {
				return "", RefusedAfter
			}
			k.keepField(i, m.Sentinel) // a keep stage: the sentinel stays for the stats
		default:
			if stats {
				return "", RefusedAfter
			}
		}
		lo, hi := k.pipeText(i)
		readsLine = readsLine || strings.Contains(query[lo:hi], "_msg")
	}
	if !stats {
		return "", RefusedNoStats
	}
	if readsLine {
		// The line is restored only when a pipe after the unpack reads it (a line filter, the bytes of the line).
		_, end := k.pipeText(k.at)
		k.inserts = append(k.inserts, insert{end, pipesText(m.restore())})
	}
	return k.checked()
}

// MarkRows is a raw-row query whose marked rows carry Flag="1", the line restored and the sentinel deleted (a fields
// pipe after the unpack keeps the flag); ok is false when a stats pipe follows the unpack (RowsRefusal names the
// reason).
func (m JSONMark) MarkRows(query string) (string, bool) {
	out, why := m.markRows(query)
	if why != "" {
		return query, false
	}
	return out, true
}

// RowsRefusal is why MarkRows leaves query unmarked, "" when it marks it.
func (m JSONMark) RowsRefusal(query string) string {
	_, why := m.markRows(query)
	return why
}

func (m JSONMark) markRows(query string) (string, string) {
	k, why := m.parseForMark(query)
	if why != "" {
		return "", why
	}
	if !k.markUnpack(m) {
		return "", RefusedFields
	}
	for i := k.at + 1; i < len(k.q.Pipes); i++ {
		switch k.q.Pipes[i].(type) {
		case PipeStats:
			return "", RefusedDropped
		case PipeFields:
			k.keepField(i, m.Flag) // a keep stage: the mark stays on the row
		}
	}
	tail := append(m.restore(),
		PipeFormat{Template: "1", ResultField: m.Flag, If: Not(FieldAny(m.Sentinel))},
		PipeDelete{Labels: []string{m.Sentinel}},
	)
	_, end := k.pipeText(k.at)
	k.inserts = append(k.inserts, insert{end, pipesText(tail)})
	return k.checked()
}

// Lookup is the query reading at most limit rows of base, oldest first, that VictoriaLogs cannot parse as one JSON
// object, as stored: the validity is read on a copy of the line, and the copy, the sentinel and drop fields are
// deleted. ok is false when base does not parse.
func (m JSONMark) Lookup(base string, limit int, drop ...string) (string, bool) {
	if _, err := Parse(base); err != nil {
		return base, false
	}
	const copied = "__vlm"
	pipes := []Pipe{PipeCopy{Pairs: [][2]string{{"_msg", copied}}}}
	pipes = append(pipes, m.splice(copied)...)
	pipes = append(pipes,
		PipeUnpackJSON{From: copied, Fields: []string{m.Sentinel}},
		PipeFilter{Expr: Not(FieldAny(m.Sentinel))},
		PipeDelete{Labels: append([]string{copied, m.Sentinel}, drop...)},
		PipeSort{By: []SortField{{Field: "_time"}}},
		PipeLimit{N: limit},
	)
	out := base + pipesText(pipes)
	if _, err := Parse(out); err != nil {
		return base, false // base's text does not take pipes after it as written (an open quote in a raw pipe)
	}
	return out, true
}
