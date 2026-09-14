package logql

import (
	"fmt"
	"net/netip"
	"strings"
)

// validIPPattern follows Loki's getMatcher: an address, prefix, or ordered
// same-family address range. Range zones are stripped, as in netipx.ParseIPRange.
func validIPPattern(pattern string) bool {
	if _, err := netip.ParseAddr(pattern); err == nil {
		return true
	}
	if _, err := netip.ParsePrefix(pattern); err == nil {
		return true
	}
	from, to, ok := strings.Cut(pattern, "-")
	if !ok {
		return false
	}
	lo, loErr := netip.ParseAddr(from)
	hi, hiErr := netip.ParseAddr(to)
	return loErr == nil && hiErr == nil && lo.BitLen() == hi.BitLen() &&
		lo.WithZone("").Compare(hi.WithZone("")) <= 0
}

func isLineFilterOperator(op TokType) bool {
	switch op {
	case TokPipeEq, TokBangEq, TokPipeTilde, TokBangTilde, TokPipeGt, TokBangGt:
		return true
	}
	return false
}

func (p *parser) parseLineFilterStage() (Stage, error) {
	tok := p.advance()
	isIP := p.cur.Typ == TokIdent && p.cur.Val == "ip"
	if isIP && tok.Typ != TokPipeEq && tok.Typ != TokBangEq {
		return nil, fmt.Errorf("ip: invalid operation")
	}
	value, err := p.expectStringOrRaw()
	if err != nil {
		return nil, err
	}
	op := LineFilterContains
	switch tok.Typ {
	case TokBangEq:
		op = LineFilterExcludes
	case TokPipeTilde:
		op = LineFilterMatchRe
	case TokBangTilde:
		op = LineFilterExcludeRe
	case TokPipeGt:
		op = LineFilterContainsPat
	case TokBangGt:
		op = LineFilterExcludePat
	}
	return &LineFilterStage{Op: op, Value: value}, nil
}
