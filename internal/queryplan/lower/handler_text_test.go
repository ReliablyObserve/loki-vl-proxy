package lower

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"

// handlerText is the text the proxy's handlers plan and translate for a client query q: the request's rewrite
// (internal/proxy requestQueryText) writes keywords in lower case, drops comments and logfmt flags, and writes the
// metric calls and parentheses of a query the validation accepts in one spelling (logql.CanonicalSpelling). The
// shadows of this package hold the lowering to the string translator's text of that query, as the proxy serves it.
func handlerText(q string) string {
	return logql.CanonicalSpelling(logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(q))))
}
