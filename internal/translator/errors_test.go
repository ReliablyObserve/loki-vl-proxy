package translator_test

import (
	"errors"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestParseError_ErrorInterface(t *testing.T) {
	e := &translator.ParseError{Msg: "unexpected token", Pos: 5}
	if e.Error() != "unexpected token" {
		t.Errorf("got %q", e.Error())
	}
	var pe *translator.ParseError
	if !errors.As(e, &pe) {
		t.Error("errors.As failed")
	}
}

func TestUnsupportedError_ErrorInterface(t *testing.T) {
	e := &translator.UnsupportedError{Msg: "count_values is not supported", Func: "count_values"}
	if e.Error() != "count_values is not supported" {
		t.Errorf("got %q", e.Error())
	}
	var ue *translator.UnsupportedError
	if !errors.As(e, &ue) {
		t.Error("errors.As failed")
	}
	if ue.Func != "count_values" {
		t.Errorf("got Func %q", ue.Func)
	}
}
