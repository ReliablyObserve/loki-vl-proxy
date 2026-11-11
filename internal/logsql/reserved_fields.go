package logsql

import "strings"

// ReservedFieldNameOtherCase reports _time, _stream or _stream_id spelled in another case than lower case (_TIME,
// _Stream, _STREAM_ID): a field like any other to VictoriaLogs' storage and pipes, but no field filter can name it.
// Bare, VictoriaLogs reads it, in any case, as its time, stream or stream id filter (`_TIME:="x"` is rejected,
// `_TIME:>=5s` is a time range). Quoted, its parser reads the field, but VictoriaLogs prints the field bare when it
// clones a query (lib/logstorage Query.Clone: print, then parse again), as its last-N optimization of a query with
// a limit does (app/vlstorage RunQuery, GetLastNResultsQuery), and so do the live tail and a cluster's remote query;
// the reprinted `_TIME:="x"` fails the second parse, Clone panics and the VictoriaLogs process restarts
// (VictoriaMetrics/VictoriaLogs#1942). So such a name is only ever written in a stream filter or a pipe, where
// VictoriaLogs prints and reads it back as the field it is.
//
// The test is VictoriaLogs' own: its lexer lowercases a bare token with strings.ToLower, which also maps U+0130 to
// 'i' (`_tİme` is read as _time), where strings.EqualFold does not.
func ReservedFieldNameOtherCase(name string) bool {
	if len(name) < len("_time") || len(name) > 3*len("_stream_id") || name[0] != '_' {
		return false
	}
	switch name {
	case "_time", "_stream", "_stream_id":
		return false
	}
	if isASCII(name) {
		// strings.ToLower of ASCII is the byte-wise lowering EqualFold compares by, without the allocation.
		return strings.EqualFold(name, "_time") || strings.EqualFold(name, "_stream") || strings.EqualFold(name, "_stream_id")
	}
	switch lower := strings.ToLower(name); lower {
	case "_time", "_stream", "_stream_id":
		return true
	}
	return false
}

// isASCII reports a name of ASCII bytes only.
func isASCII(name string) bool {
	for i := 0; i < len(name); i++ {
		if name[i] >= 0x80 {
			return false
		}
	}
	return true
}

// ReservedFieldCaseScratch is the scratch field a filter on a name in another case (ReservedFieldNameOtherCase)
// reads: the name copied into it by a copy pipe, which VictoriaLogs prints and reads back as written.
func ReservedFieldCaseScratch(name string) string { return "__lxrf" + name }

// mayNameReservedOtherCase reports text that holds, anywhere, _time, _stream or _stream_id in another case than
// lower case as a whole name: the cheap screen of GuardReservedFieldCase, which reads no other text. It allocates
// nothing.
func mayNameReservedOtherCase(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] != '_' || i > 0 && isNameByte(text[i-1]) {
			continue
		}
		j := i + 1
		for j < len(text) && isNameByte(text[j]) {
			j++
		}
		if ReservedFieldNameOtherCase(text[i:j]) {
			return true
		}
		i = j - 1
	}
	return false
}

// isNameByte is a byte of a bare LogsQL field name, or of a non-ASCII character in one (`_tİme`).
func isNameByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}
