package lower

import (
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// escapedSpellingConstruct is the construct a log query is delegated under when the string translator copies the
// client's spelling of a value where the lowering, which holds the value only, would write its own.
const escapedSpellingConstruct = "LogSelect escaped spelling"

// keepsSpelling reports whether the string translator keeps the client's spelling of a value the lowering would
// write differently. It copies the double-quoted literal of a regexp line filter (|~ and !~, unless an `or` chain
// rebuilds it from the values) and of a matcher that goes into the stream filter; every other position quotes the
// value, as the lowering does. A literal that is not spelled the way strconv.Quote spells its value (logql
// Escaped) differs from the lowering's quoting of the regexp. The stream filter quotes with logsql.QuoteValue,
// which writes a control character as it is where strconv.Quote escapes it, so a matcher whose literal is
// Quote's spelling differs too when the two quotings of its value differ.
func keepsSpelling(sel *queryplan.LogSelect, prof Profile) bool {
	for _, m := range sel.Matchers {
		if m.Op == logql.MatchEq && translate.StreamFilterMatcher(m, prof.Label, prof.StreamFields) && (m.Escaped || quotingsDiffer(m.Value)) {
			return true
		}
	}
	for _, st := range sel.Stages {
		if lf, ok := st.(*queryplan.LineFilter); ok && lf.Escaped && len(lf.Or) == 0 && (lf.Op == logql.LineFilterMatchRe || lf.Op == logql.LineFilterExcludeRe) {
			return true
		}
	}
	return false
}

// quotingsDiffer reports whether logsql.QuoteValue and strconv.Quote write s differently. Printable ASCII, which is
// nearly every value, is written alike.
func quotingsDiffer(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x7f {
			return logsql.QuoteValue(s) != strconv.Quote(s)
		}
	}
	return false
}
