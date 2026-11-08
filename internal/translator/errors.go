package translator

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"

// ParseError is returned when the input is not valid LogQL syntax.
// The proxy HTTP layer maps this to errorType "parse error".
type ParseError = translate.ParseError

// UnsupportedError is returned when a valid LogQL construct has no
// equivalent in LogsQL and cannot be translated.
// The proxy HTTP layer maps this to errorType "bad_data".
type UnsupportedError struct {
	Msg  string
	Func string // the LogQL function or construct that is unsupported
}

func (e *UnsupportedError) Error() string { return e.Msg }

// LabelTranslateFunc translates a Loki label name to a VictoriaLogs field name.
type LabelTranslateFunc = translate.LabelTranslateFunc
