package translator

// ParseError is returned when the input is not valid LogQL syntax.
// The proxy HTTP layer maps this to errorType "parse error".
type ParseError struct {
	Msg string
	Pos int // byte offset in input; -1 if unknown
}

func (e *ParseError) Error() string { return e.Msg }

// UnsupportedError is returned when a valid LogQL construct has no
// equivalent in LogsQL and cannot be translated.
// The proxy HTTP layer maps this to errorType "bad_data".
type UnsupportedError struct {
	Msg  string
	Func string // the LogQL function or construct that is unsupported
}

func (e *UnsupportedError) Error() string { return e.Msg }
