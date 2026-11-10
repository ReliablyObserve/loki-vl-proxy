package translate

// BinaryOperatorTokens are the operators a binary metric expression is split at, each with the blank it needs on
// either side, in the order they are tried at a position: a two-byte comparison before the one-byte comparison it
// starts with. A set operator (and, or, unless) is none of them.
var BinaryOperatorTokens = [...]string{" / ", " * ", " + ", " - ", " % ", " ^ ", " == ", " != ", " >= ", " <= ", " > ", " < "}

// BinaryOperatorToken is the token of the arithmetic or comparison operator op (`/`, `>=`), "" for any other.
func BinaryOperatorToken(op string) string {
	for _, tok := range BinaryOperatorTokens {
		if len(tok) == len(op)+2 && tok[1:len(tok)-1] == op {
			return tok
		}
	}
	return ""
}
