package logsql

// WithGroupKeyAfterStream returns query with key added, right after _stream, to
// the group of every top-level stats pipe that groups by _stream (unbucketed) and
// not yet by key: `| stats by (_stream, a) f` becomes
// `| stats by (_stream, key, a) f`. Every other byte of the query is kept as it
// is. ok is false, and query is returned unchanged, when the scanner cannot read
// the query or no stats pipe groups by _stream.
func WithGroupKeyAfterStream(query, key string) (string, bool) {
	s := NewScanner(query)
	var inserts []int // offsets right after a group's _stream key
	depth := 0
	// state follows `| stats by (`: 0 nothing, 1 after the pipe, 2 after stats, 3 after by.
	state := 0
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF:
			return insertAfter(query, inserts, ", "+key)
		case tok.Typ == TokError:
			return query, false
		case tok.Typ == TokLParen && state == 3:
			at, ok := streamGroupInsert(s, key)
			if !ok {
				return query, false
			}
			if at >= 0 {
				inserts = append(inserts, at)
			}
			state = 0
		case tok.Typ == TokLParen:
			depth++
			state = 0
		case tok.Typ == TokRParen:
			depth--
			state = 0
		case tok.Typ == TokPipe && depth == 0:
			state = 1
		case tok.Typ == TokIdent && state == 1 && tok.Val == "stats":
			state = 2
		case tok.Typ == TokIdent && state == 2 && tok.Val == "by":
			state = 3
		default:
			state = 0
		}
	}
}

// streamGroupInsert reads a stats group after its opening parenthesis up to its
// closing one and returns the offset right after its _stream key when the group
// holds an unbucketed _stream and not key, else -1. ok is false when the group
// does not close.
func streamGroupInsert(s *Scanner, key string) (at int, ok bool) {
	depth, streamEnd, hasKey, prevStream := 0, -1, false, false
	for {
		tok := s.Next()
		switch {
		case tok.Typ == TokEOF || tok.Typ == TokError:
			return -1, false
		case tok.Typ == TokRParen && depth == 0:
			if streamEnd >= 0 && !hasKey {
				return streamEnd, true
			}
			return -1, true
		case tok.Typ == TokLParen:
			depth++
		case tok.Typ == TokRParen:
			depth--
		case tok.Typ == TokIdent && tok.Val == key:
			hasKey = true
		case tok.Typ == TokIdent && tok.Val == "_stream":
			prevStream, streamEnd = true, s.end
			continue
		case (tok.Typ == TokColon || tok.Typ == TokColonEq) && prevStream:
			streamEnd = -1 // a bucketed or odd key, not the stream itself
		}
		prevStream = false
	}
}

// insertAfter returns query with text inserted at each offset of at (ascending),
// and whether there was any.
func insertAfter(query string, at []int, text string) (string, bool) {
	if len(at) == 0 {
		return query, false
	}
	out := make([]byte, 0, len(query)+len(at)*len(text))
	last := 0
	for _, i := range at {
		out = append(out, query[last:i]...)
		out = append(out, text...)
		last = i
	}
	return string(append(out, query[last:]...)), true
}
