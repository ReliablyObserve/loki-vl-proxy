package logsql

import "strings"

// ParserKeepsFields is query with its one parser pipe (unpack_json, unpack_logfmt, extract or extract_regexp) told to
// keep the fields a row already has (keep_original_fields), the rest of the text as written: a field the parser would
// set that a stream label or another stored field already holds keeps the row's own value, as Loki keeps the label
// and names the parsed one name_extracted. ok is false when the query does not parse, holds no parser pipe or more
// than one, or its parser pipe skips empty results (VictoriaLogs takes one of the two options). With logfmtOnly the
// parser pipe must be unpack_logfmt.
func ParserKeepsFields(query string, logfmtOnly bool) (string, bool) {
	q, err := Parse(query)
	if err != nil {
		return query, false
	}
	var pipes []int
	if !textScan(query, func(i, depth int) bool {
		if query[i] == '|' && depth == 0 {
			pipes = append(pipes, i)
		}
		return true
	}) || len(pipes) != len(q.Pipes) {
		return query, false
	}
	at, keep, skip := -1, false, false
	for i, p := range q.Pipes {
		var k, s bool
		switch v := p.(type) {
		case PipeUnpackJSON:
			k, s = v.KeepOriginalFields, v.SkipEmptyResults
		case PipeUnpackLogfmt:
			k, s = v.KeepOriginalFields, v.SkipEmptyResults
		case PipeExtract:
			k, s = v.KeepOriginalFields, v.SkipEmptyResults
		case PipeExtractRegexp:
			k, s = v.KeepOriginalFields, v.SkipEmptyResults
		default:
			continue
		}
		if at >= 0 {
			return query, false
		}
		at, keep, skip = i, k, s
	}
	if at >= 0 && logfmtOnly {
		if _, logfmt := q.Pipes[at].(PipeUnpackLogfmt); !logfmt {
			return query, false
		}
	}
	switch {
	case at < 0 || skip:
		return query, false
	case keep:
		return query, true
	}
	hi := len(query)
	if at+1 < len(pipes) {
		hi = pipes[at+1]
	}
	for hi > pipes[at] && strings.ContainsRune(" \t\n\r", rune(query[hi-1])) {
		hi--
	}
	out := query[:hi] + " keep_original_fields" + query[hi:]
	// The option must land on the parser pipe: the text read back holds it there and nothing else changed.
	back, err := Parse(out)
	if err != nil || len(back.Pipes) != len(q.Pipes) || !keepsOriginal(back.Pipes[at]) {
		return query, false
	}
	return out, true
}

func keepsOriginal(p Pipe) bool {
	switch v := p.(type) {
	case PipeUnpackJSON:
		return v.KeepOriginalFields
	case PipeUnpackLogfmt:
		return v.KeepOriginalFields
	case PipeExtract:
		return v.KeepOriginalFields
	case PipeExtractRegexp:
		return v.KeepOriginalFields
	}
	return false
}
