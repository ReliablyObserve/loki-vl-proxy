package logql

import (
	"net/netip"
	"strings"
	"unicode"
)

// validIPPattern follows Loki's getMatcher: an address, prefix, or ordered
// same-family address range. Range zones are stripped, as in netipx.ParseIPRange.
func validIPPattern(pattern string) bool {
	if strings.ContainsRune(pattern, '/') {
		// Only a prefix holds a slash; reading it first spares the failed address parse its error.
		_, err := netip.ParsePrefix(pattern)
		return err == nil
	}
	if _, err := netip.ParseAddr(pattern); err == nil {
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
	// An ip() operand is checked with the run of line filters it belongs to (line_filter_runs.go).
	head, err := p.parseLineFilterAlt()
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
	stage := &LineFilterStage{Op: op, Value: head.Value, IP: head.IP}
	for p.cur.Typ == TokOr || p.cur.Typ == TokIdent && strings.EqualFold(p.cur.Val, "or") {
		p.advance()
		alt, err := p.parseLineFilterAlt()
		if err != nil {
			return nil, err
		}
		stage.Or = append(stage.Or, alt)
	}
	if !stage.negated() {
		stage.Or = lastOrGroup(stage.Or)
	}
	if stage.IP && op == LineFilterMatchRe && len(stage.Or) > 0 && lineFilterError(stage) == nil {
		// Loki reads the ip() head of a positive |~ chain as a regexp, which the proxy does not translate yet: a chain
		// Loki accepts is answered as before (semantics/regexp-chain-led-by-ip); one it rejects is left to the
		// validation, which answers Loki's error.
		return nil, errIPOperation
	}
	return stage, nil
}

// lastOrGroup mirrors how Loki's grammar builds a positive chain: an ip(...)
// alternative ends an orFilter, and the next `or` attaches its alternatives to
// the head again (newOrLineFilterExpr: left.Or = right), dropping every
// alternative attached before. `|= "a" or "b" or ip("x") or "c"` is `a or c`.
func lastOrGroup(alts []LineFilterAlt) []LineFilterAlt {
	start := 0
	for i, alt := range alts {
		if alt.IP && i+1 < len(alts) {
			start = i + 1
		}
	}
	return alts[start:]
}

// parseLineFilterAlt parses a line filter operand, what follows the operator
// or `or` (syntax.y orFilter): a string or ip("..."). The ip pattern is not
// validated here.
func (p *parser) parseLineFilterAlt() (LineFilterAlt, error) {
	switch {
	case p.cur.Typ == TokString || p.cur.Typ == TokRawString:
		return LineFilterAlt{Value: p.advance().Val}, nil
	case p.cur.Typ == TokIdent && p.cur.Val == "ip":
		if !strings.HasPrefix(strings.TrimLeftFunc(p.sc.src[p.sc.pos:], unicode.IsSpace), "(") {
			// Loki reads ip as a function only before "(": a bare ip is an identifier it does not expect.
			return LineFilterAlt{}, p.errorAt(p.pos(), "IDENTIFIER", "STRING or ip", false)
		}
		p.advance()
		if p.cur.Typ != TokLParen {
			return LineFilterAlt{}, p.syntaxErrorAt("(")
		}
		p.advance()
		if p.cur.Typ != TokString && p.cur.Typ != TokRawString {
			return LineFilterAlt{}, p.syntaxErrorAt("STRING")
		}
		alt := LineFilterAlt{Value: p.advance().Val, IP: true}
		if p.cur.Typ != TokRParen {
			return LineFilterAlt{}, p.syntaxErrorAt(")")
		}
		p.advance()
		return alt, nil
	}
	return LineFilterAlt{}, p.syntaxErrorAt("STRING or ip")
}
