package proxy

// vlNotInRun is a regexp that matches a run of characters outside set, as [^set]* does, written so that VictoriaLogs
// parses it cheaply. set is the inside of a character class, without ASCII letters.
//
// A negated class spans almost all of Unicode. When it also holds the ASCII letters, Go's regexp/syntax spends about
// a millisecond printing it (String). VictoriaLogs prints every regexp several times while it simplifies it on each
// query parse, so one such class costs it 4-10 ms per query (v1.49.0 and v1.53.0). The proxy's risk probes pay that
// on every request whose window is not covered yet. Here the letters stay out of the wide class: a run of
// non-letters then a run of letters, repeated, is any run of [^set]. Neither class is wide and holds letters, and
// unlike an alternation of the two classes, the parser cannot merge them back into one. A set holding an ASCII letter
// would let [A-Za-z]* match it, so such a set is not written in this form.
func vlNotInRun(set string) string {
	return `(?:[^` + set + `A-Za-z]*[A-Za-z]*)*`
}
