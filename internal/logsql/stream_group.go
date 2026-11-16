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
	// state follows `| stats by (`: 0 nothing, 1 after the pipe, 2 after stats,
	// 3 after by, 4 inside the group.
	state, groupDepth := 0, 0
	streamEnd, hasKey := -1, false
	prevStream := false
	for {
		tok := s.Next()
		switch tok.Typ {
		case TokEOF:
			if len(inserts) == 0 {
				return query, false
			}
			out := make([]byte, 0, len(query)+len(inserts)*(len(key)+2))
			last := 0
			for _, at := range inserts {
				out = append(out, query[last:at]...)
				out = append(out, ", "...)
				out = append(out, key...)
				last = at
			}
			return string(append(out, query[last:]...)), true
		case TokError:
			return query, false
		}
		if state == 4 {
			switch {
			case tok.Typ == TokRParen && depth == groupDepth:
				if streamEnd >= 0 && !hasKey {
					inserts = append(inserts, streamEnd)
				}
				state, streamEnd, hasKey, prevStream = 0, -1, false, false
			case tok.Typ == TokLParen:
				depth++
			case tok.Typ == TokRParen:
				depth--
			case tok.Typ == TokIdent && tok.Val == key:
				hasKey = true
			case tok.Typ == TokIdent && tok.Val == "_stream":
				prevStream = true
				streamEnd = s.end
				continue
			case tok.Typ == TokColon || tok.Typ == TokColonEq:
				if prevStream {
					streamEnd = -1 // a bucketed or odd key, not the stream itself
				}
			}
			prevStream = false
			continue
		}
		switch {
		case tok.Typ == TokLParen:
			if state == 3 {
				state, groupDepth = 4, depth
				continue
			}
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
