// Package logsql provides a typed AST and builder for VictoriaLogs LogsQL queries.
// It targets VictoriaLogs v1.40 and later (v1.4x and v1.5x families).
package logsql

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// quoteLogsQL wraps s in double-quotes and escapes any embedded backslashes or
// double-quote characters so the result is always a valid LogsQL quoted string.
func quoteLogsQL(s string) string {
	// A line break cannot sit raw in a quoted string: VictoriaLogs reads the string the way Go does, and rejects
	// one, or reads a raw carriage return as another character. Bytes that are not UTF-8 would not come back as
	// they are either. Other control characters pass as they are.
	if strings.ContainsAny(s, "\n\r") || !utf8.ValidString(s) {
		return strconv.Quote(s)
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

// quoteLogsQLPattern renders a regexp as a LogsQL string literal. VictoriaLogs
// unquotes double-quoted strings with Go's rules, so every backslash the
// pattern carries has to survive as an escape: `\d` must reach the backend as
// `\\d`, or the query is rejected with "compound token cannot start with". A
// pattern is therefore quoted exactly as Go would quote it, and control
// characters are escaped rather than sent raw.
func quoteLogsQLPattern(s string) string {
	return strconv.Quote(s)
}

// reservedWords are the words VictoriaLogs reads as keywords or filter functions where a filter can begin, so a
// word that is one of them (or starts with one, before a . or -) has to be quoted.
var reservedWords = map[string]bool{
	"and": true, "or": true, "not": true, "offset": true, "in": true, "i": true, "exact": true, "re": true, "seq": true,
	"range": true, "ipv4_range": true, "ipv6_range": true, "eq_field": true, "le_field": true, "lt_field": true,
	"len_range": true, "string_range": true, "value_type": true, "contains_all": true, "contains_any": true,
	"contains_common_case": true, "equals_common_case": true, "json_array_contains_any": true,
	"pattern_match": true, "pattern_match_full": true, "pattern_match_prefix": true, "pattern_match_suffix": true,
	"_time": true, "_stream": true, "_stream_id": true, "options": true,
}

// isWordRune reports whether r is a letter, a digit or _: what a LogsQL word is made of.
func isWordRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }

// isReserved reports whether s starts with a reserved word.
func isReserved(s string) bool {
	end := strings.IndexFunc(s, func(r rune) bool { return !isWordRune(r) })
	if end < 0 {
		end = len(s)
	}
	return reservedWords[strings.ToLower(s[:end])]
}

// wordOrQuoted is s as a bare word when LogsQL reads it as one, else quoted: a word is made of letters, digits and
// _, with . - / + $ inside, has a letter, digit or _, and does not start with a reserved word.
func wordOrQuoted(s string) string {
	plain := false
	for i, r := range s {
		switch {
		case isWordRune(r):
			plain = true
		case r == '/' || (i > 0 && strings.ContainsRune(".-+$", r)):
		default:
			return quoteLogsQL(s)
		}
	}
	if !plain || isReserved(s) {
		return quoteLogsQL(s)
	}
	return s
}

// prefixValue is the value before the * of a prefix filter: empty for the empty prefix.
func prefixValue(s string) string {
	if s == "" {
		return ""
	}
	return wordOrQuoted(s)
}

// fieldPrefix is "field:" for a field filter, empty for a filter on the message.
func fieldPrefix(field string) string {
	if field == "" {
		return ""
	}
	return field + ":"
}

// QuoteValue wraps s in double-quotes and escapes embedded backslashes and
// double-quote characters so the result is always a valid LogsQL quoted string.
func QuoteValue(s string) string { return quoteLogsQL(s) }

// QuotePattern is like QuoteValue but for regexp/pattern strings that carry
// their own backslash escape semantics. Only double-quotes are escaped;
// backslashes pass through verbatim so regex escapes (\d, \w, etc.) are preserved.
func QuotePattern(s string) string { return quoteLogsQLPattern(s) }

// Expr is the top-level LogsQL expression interface.
type Expr interface {
	String() string
	expr()
}

// appender is a node that writes itself onto a buffer: the nodes a query is mostly made of print through it, so a
// whole query is written into one buffer instead of one string per node concatenated by its parent. A node
// without it prints through String.
type appender interface{ appendTo(b []byte) []byte }

// appendFilter appends the text of f.
func appendFilter(b []byte, f FilterExpr) []byte {
	if a, ok := f.(appender); ok {
		return a.appendTo(b)
	}
	return append(b, f.String()...)
}

// appendOperand is operand, appended: f inside a filter that binds with precedence prec.
func appendOperand(b []byte, f FilterExpr, prec int) []byte {
	if filterPrec(f) < prec {
		b = append(b, '(')
		b = appendFilter(b, f)
		return append(b, ')')
	}
	return appendFilter(b, f)
}

// appendQuoteLogsQL is quoteLogsQL, appended.
func appendQuoteLogsQL(b []byte, s string) []byte {
	if strings.ContainsAny(s, "\n\r") || !utf8.ValidString(s) {
		return strconv.AppendQuote(b, s)
	}
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == '\\' || c == '"' {
			b = append(b, '\\')
		}
		b = append(b, s[i])
	}
	return append(b, '"')
}

// FilterExpr is a filter expression node in a LogsQL query.
type FilterExpr interface {
	String() string
	filterExpr()
}

// Pipe is a single pipe stage. String() returns the full pipe token including
// the leading "| " separator (e.g. "| unpack_json", "| filter status:>=500").
// Query.String() inserts a space before calling p.String(), so the result is
// "filter | unpack_json" not "filter| unpack_json".
type Pipe interface {
	String() string
	pipe()
}

// StatsFunc is a function used inside PipeStats (e.g. count(), sum(field)).
// Concrete implementations are defined alongside PipeStats in the pipe types section.
type StatsFunc interface {
	String() string
	statsFunc()
}

// Query is a complete LogsQL query: an optional filter followed by zero or more pipes.
type Query struct {
	Filter FilterExpr
	Pipes  []Pipe
}

func (q Query) String() string {
	bp := queryBufPool.Get().(*[]byte)
	b := (*bp)[:0]
	if q.Filter == nil {
		b = append(b, '*')
	} else {
		b = appendFilter(b, q.Filter)
	}
	for _, p := range q.Pipes {
		b = append(b, ' ')
		if a, ok := p.(appender); ok {
			b = a.appendTo(b)
		} else {
			b = append(b, p.String()...)
		}
	}
	s := string(b)
	// A buffer grown past what a query needs is dropped, so the pool never holds a large one.
	if cap(b) <= maxPooledQueryBuf {
		*bp = b
		queryBufPool.Put(bp)
	}
	return s
}

// queryBufPool holds the buffers Query.String writes into, so a query costs the allocation of its text alone.
var queryBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

const maxPooledQueryBuf = 16 << 10

func (q Query) expr() {}

// --- Simple filter nodes ---

// Word matches a single word anywhere in the log line.
type Word struct{ Value string }

func (w Word) String() string {
	if isReserved(w.Value) {
		return quoteLogsQL(w.Value)
	}
	return w.Value
}
func (w Word) filterExpr() {}

// Phrase matches an exact phrase (including spaces).
type Phrase struct{ Value string }

func (p Phrase) String() string { return quoteLogsQL(p.Value) }
func (p Phrase) filterExpr()    {}

// Prefix matches log lines containing a word with the given prefix.
type Prefix struct{ Value string }

func (p Prefix) String() string { return prefixValue(p.Value) + "*" }
func (p Prefix) filterExpr()    {}

// Substring matches log lines containing the given substring.
type Substring struct{ Value string }

func (s Substring) String() string { return "*" + wordOrQuoted(s.Value) + "*" }
func (s Substring) filterExpr()    {}

// Exact matches an exact value using the `=` operator.
type Exact struct{ Value string }

func (e Exact) String() string { return "=" + quoteLogsQL(e.Value) }
func (e Exact) filterExpr()    {}

// Regexp matches using a regular expression.
type Regexp struct{ Pattern string }

func (r Regexp) String() string { return string(r.appendTo(make([]byte, 0, len(r.Pattern)+8))) }
func (r Regexp) appendTo(b []byte) []byte {
	return strconv.AppendQuote(append(b, '~'), r.Pattern)
}
func (r Regexp) filterExpr() {}

// Sequence matches a sequence of words in order.
type Sequence struct {
	Field string // empty is the message
	Parts []string
}

func (s Sequence) String() string {
	quoted := make([]string, len(s.Parts))
	for i, p := range s.Parts {
		quoted[i] = quoteLogsQL(p)
	}
	return fieldPrefix(s.Field) + "seq(" + strings.Join(quoted, ",") + ")"
}
func (s Sequence) filterExpr() {}

// CaseInsensitive wraps a value for case-insensitive matching.
type CaseInsensitive struct {
	Field string // empty is the message
	Value string
}

func (c CaseInsensitive) String() string {
	return fieldPrefix(c.Field) + "i(" + quoteLogsQL(c.Value) + ")"
}
func (c CaseInsensitive) filterExpr() {}

// Wildcard matches any log line.
type Wildcard struct{}

func (w Wildcard) String() string { return "*" }
func (w Wildcard) filterExpr()    {}

// --- FieldFilter ---

// FieldOp is the operator used in a field filter.
type FieldOp int

const (
	FieldOpExact                FieldOp = iota // :="val"
	FieldOpRegexp                              // :~"pat"
	FieldOpPrefix                              // :prefix*
	FieldOpSubstring                           // :*text*
	FieldOpEmpty                               // :""
	FieldOpAny                                 // :*
	FieldOpGT                                  // :>val
	FieldOpGTE                                 // :>=val
	FieldOpLT                                  // :<val
	FieldOpLTE                                 // :<=val
	FieldOpRange                               // :range(min,max)
	FieldOpIn                                  // :in(a,b,c)
	FieldOpIPv4Range                           // :ipv4_range(first, last)  requires v1.45+
	FieldOpIPv6Range                           // :ipv6_range(first, last)
	FieldOpExactPrefix                         // :="prefix"*
	FieldOpEqField                             // :eq_field(field2)
	FieldOpLeField                             // :le_field(field2)
	FieldOpStringRange                         // :string_range(lo,hi)
	FieldOpValueType                           // :value_type(type)
	FieldOpJSONArrayContainsAny                // :json_array_contains_any("v1","v2")
	FieldOpContainsCommonCase                  // :contains_common_case("p1","p2")
	FieldOpEqualsCommonCase                    // :equals_common_case("p1","p2")
	FieldOpNotExact                            // :!"val" (the field has a value other than val)
	FieldOpPhrase                              // :word or :"phrase" (the field has the word or phrase)
)

// FieldFilter matches a named field using the given operator and value.
type FieldFilter struct {
	Field  string
	Op     FieldOp
	Value  string
	Negate bool
	// GoQuoted quotes an exact value as Go does (control characters escaped) rather than escaping only
	// backslashes and double quotes.
	GoQuoted bool
}

func (f FieldFilter) String() string { return string(f.appendTo(make([]byte, 0, 64))) }

func (f FieldFilter) appendTo(b []byte) []byte {
	if f.Negate {
		b = append(b, "NOT "...)
	}
	if f.Field != "" {
		b = append(b, f.Field...)
		b = append(b, ':')
	}
	switch f.Op {
	case FieldOpExact:
		b = append(b, '=')
		if f.GoQuoted {
			return strconv.AppendQuote(b, f.Value)
		}
		return appendQuoteLogsQL(b, f.Value)
	case FieldOpRegexp:
		b = append(b, '~')
		return strconv.AppendQuote(b, f.Value)
	case FieldOpEmpty:
		return append(b, `""`...)
	case FieldOpAny:
		return append(b, '*')
	case FieldOpGT:
		return append(append(b, '>'), f.Value...)
	case FieldOpGTE:
		return append(append(b, ">="...), f.Value...)
	case FieldOpLT:
		return append(append(b, '<'), f.Value...)
	case FieldOpLTE:
		return append(append(b, "<="...), f.Value...)
	case FieldOpExactPrefix:
		b = append(b, '=')
		b = appendQuoteLogsQL(b, f.Value)
		return append(b, '*')
	case FieldOpNotExact:
		b = append(b, '!')
		return appendQuoteLogsQL(b, f.Value)
	case FieldOpPrefix:
		b = append(b, prefixValue(f.Value)...)
		return append(b, '*')
	case FieldOpSubstring:
		b = append(b, '*')
		b = append(b, wordOrQuoted(f.Value)...)
		return append(b, '*')
	case FieldOpPhrase:
		return append(b, wordOrQuoted(f.Value)...)
	}
	var fn string
	switch f.Op {
	case FieldOpRange:
		fn = "range("
	case FieldOpIn:
		fn = "in("
	case FieldOpIPv4Range:
		fn = "ipv4_range("
	case FieldOpIPv6Range:
		fn = "ipv6_range("
	case FieldOpEqField:
		fn = "eq_field("
	case FieldOpLeField:
		fn = "le_field("
	case FieldOpStringRange:
		fn = "string_range("
	case FieldOpValueType:
		fn = "value_type("
	case FieldOpJSONArrayContainsAny:
		fn = "json_array_contains_any("
	case FieldOpContainsCommonCase:
		fn = "contains_common_case("
	case FieldOpEqualsCommonCase:
		fn = "equals_common_case("
	default:
		panic(fmt.Sprintf("logsql: unknown FieldOp %d", f.Op))
	}
	b = append(b, fn...)
	b = append(b, f.Value...)
	return append(b, ')')
}

func (f FieldFilter) filterExpr() {}

// --- Stream and Time filters ---

// LabelMatcher is a single name op "value" matcher inside a StreamFilter.
type LabelMatcher struct {
	Name  string
	Op    string
	Value string
}

func (m LabelMatcher) String() string {
	return m.Name + m.Op + quoteLogsQL(m.Value)
}

// StreamFilter matches log streams by label selectors.
type StreamFilter struct {
	Matchers []LabelMatcher
}

func (s StreamFilter) String() string {
	parts := make([]string, len(s.Matchers))
	for i, m := range s.Matchers {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (s StreamFilter) filterExpr() {}

// TimeFilter constrains the query to a time range.
type TimeFilter struct {
	Range string // e.g. "5m" or "[2024-01-01,2024-01-02]"
}

func (t TimeFilter) String() string { return "_time:" + t.Range }
func (t TimeFilter) filterExpr()    {}

// --- Logic combinators ---

// filterPrec is how tightly a filter binds when printed beside others: OR loosest, then AND, then NOT and the rest.
func filterPrec(f FilterExpr) int {
	switch f.(type) {
	case OrExpr, AnyOf, AnyOfLower:
		return 1
	case AndExpr, AllOf:
		return 2
	case NotExpr, Exclude:
		return 3
	}
	return 4
}

// operand prints f inside a filter that binds with precedence prec, in parentheses when f binds more loosely.
func operand(f FilterExpr, prec int) string {
	if filterPrec(f) < prec {
		return "(" + f.String() + ")"
	}
	return f.String()
}

// AndExpr is a logical AND of two filter expressions.
type AndExpr struct {
	Left, Right FilterExpr
	// Lower writes the operator in lowercase (`a and b`), which LogsQL reads like `AND`: the spelling the string
	// translator uses to join the label filters of a query. The parser never sets it.
	Lower bool
}

func (a AndExpr) String() string { return string(a.appendTo(make([]byte, 0, 64))) }

func (a AndExpr) appendTo(b []byte) []byte {
	b = appendOperand(b, a.Left, 2)
	if a.Lower {
		b = append(b, " and "...)
	} else {
		b = append(b, " AND "...)
	}
	return appendOperand(b, a.Right, 2)
}

func (a AndExpr) filterExpr() {}

// OrExpr is a logical OR of two filter expressions.
type OrExpr struct {
	Left, Right FilterExpr
	// Lower writes the operator in lowercase (`a or b`), as AndExpr.Lower does.
	Lower bool
}

func (o OrExpr) String() string { return string(o.appendTo(make([]byte, 0, 64))) }

func (o OrExpr) appendTo(b []byte) []byte {
	b = appendFilter(b, o.Left)
	if o.Lower {
		b = append(b, " or "...)
	} else {
		b = append(b, " OR "...)
	}
	return appendFilter(b, o.Right)
}

func (o OrExpr) filterExpr() {}

// Exclude is LogsQL's "-" negation of a filter: -level:*, -(a OR b).
type Exclude struct{ Expr FilterExpr }

func (e Exclude) String() string {
	// "-NOT x" would read as one word, "-NOT"
	if _, ok := e.Expr.(NotExpr); ok {
		return "-(" + e.Expr.String() + ")"
	}
	return "-" + operand(e.Expr, 3)
}
func (e Exclude) filterExpr() {}

// Group is a filter in parentheses.
type Group struct{ Expr FilterExpr }

func (g Group) String() string { return string(g.appendTo(make([]byte, 0, 64))) }
func (g Group) appendTo(b []byte) []byte {
	return append(appendFilter(append(b, '('), g.Expr), ')')
}
func (g Group) filterExpr() {}

// AllOf is filters written one after another, which LogsQL reads as their AND.
type AllOf []FilterExpr

func (a AllOf) String() string { return string(a.appendTo(make([]byte, 0, 128))) }

func (a AllOf) appendTo(b []byte) []byte {
	for i, f := range a {
		if i > 0 {
			b = append(b, ' ')
		}
		b = appendOperand(b, f, 2)
	}
	return b
}
func (a AllOf) filterExpr() {}

// AnyOf is the OR of filters.
type AnyOf []FilterExpr

func (a AnyOf) String() string { return string(a.appendTo(make([]byte, 0, 128))) }

func (a AnyOf) appendTo(b []byte) []byte { return appendJoined(b, a, " OR ") }

// appendJoined appends the filters separated by sep.
func appendJoined(b []byte, fs []FilterExpr, sep string) []byte {
	for i, f := range fs {
		if i > 0 {
			b = append(b, sep...)
		}
		b = appendFilter(b, f)
	}
	return b
}
func (a AnyOf) filterExpr() {}

// AnyOfLower is AnyOf written with the lowercase operator (`a or b`). LogsQL reads both spellings alike; the
// alternatives of a pattern line filter are written this way, the text the string translator wrote for them.
type AnyOfLower []FilterExpr

func (a AnyOfLower) String() string { return string(a.appendTo(make([]byte, 0, 128))) }

func (a AnyOfLower) appendTo(b []byte) []byte { return appendJoined(b, a, " or ") }
func (a AnyOfLower) filterExpr()              {}

// NotExpr negates a filter expression.
type NotExpr struct {
	Expr FilterExpr
}

func (n NotExpr) String() string { return string(n.appendTo(make([]byte, 0, 64))) }
func (n NotExpr) appendTo(b []byte) []byte {
	return appendOperand(append(b, "NOT "...), n.Expr, 3)
}

func (n NotExpr) filterExpr() {}

// --- Additional filter types ---

// AnyCasePhrase matches a phrase case-insensitively using i("phrase") syntax.
// Corresponds to filter_any_case_phrase.go in VL upstream.
type AnyCasePhrase struct{ Value string }

func (a AnyCasePhrase) String() string { return "i(" + quoteLogsQL(a.Value) + ")" }
func (a AnyCasePhrase) filterExpr()    {}

// AnyCasePrefix matches a prefix case-insensitively using i(prefix*) syntax.
// Corresponds to filter_any_case_prefix.go in VL upstream.
type AnyCasePrefix struct {
	Field string // empty is the message
	Value string
}

func (a AnyCasePrefix) String() string {
	return fieldPrefix(a.Field) + "i(" + prefixValue(a.Value) + "*)"
}
func (a AnyCasePrefix) filterExpr() {}

// ExactPrefix matches log lines where the message starts with the exact prefix.
// Syntax: ="prefix"*
type ExactPrefix struct{ Value string }

func (e ExactPrefix) String() string { return "=" + quoteLogsQL(e.Value) + "*" }
func (e ExactPrefix) filterExpr()    {}

// ContainsAll matches log lines containing all of the given words or phrases.
// Syntax: contains_all("w1","w2",...)
type ContainsAll struct {
	Field string // empty is the message
	Parts []string
}

func (c ContainsAll) String() string {
	quoted := make([]string, len(c.Parts))
	for i, p := range c.Parts {
		quoted[i] = quoteLogsQL(p)
	}
	return fieldPrefix(c.Field) + "contains_all(" + strings.Join(quoted, ",") + ")"
}
func (c ContainsAll) filterExpr() {}

// ContainsAny matches log lines containing any of the given words or phrases.
// Syntax: contains_any("w1","w2",...)
type ContainsAny struct {
	Field string // empty is the message
	Parts []string
}

func (c ContainsAny) String() string {
	quoted := make([]string, len(c.Parts))
	for i, p := range c.Parts {
		quoted[i] = quoteLogsQL(p)
	}
	return fieldPrefix(c.Field) + "contains_any(" + strings.Join(quoted, ",") + ")"
}
func (c ContainsAny) filterExpr() {}

// DayRange restricts matches to a time-of-day range within each day.
// Syntax: _time:day_range[08:00, 18:00] [offset 2h]
type DayRange struct {
	Bracket string // e.g. "[08:00, 18:00]"
	Offset  string // optional, e.g. "2h"
}

func (d DayRange) String() string {
	s := "_time:day_range" + d.Bracket
	if d.Offset != "" {
		s += " offset " + d.Offset
	}
	return s
}
func (d DayRange) filterExpr() {}

// WeekRange restricts matches to a day-of-week range within each week.
// Syntax: _time:week_range[Mon, Fri] [offset 2h]
type WeekRange struct {
	Bracket string // e.g. "[Mon, Fri]"
	Offset  string // optional
}

func (w WeekRange) String() string {
	s := "_time:week_range" + w.Bracket
	if w.Offset != "" {
		s += " offset " + w.Offset
	}
	return s
}
func (w WeekRange) filterExpr() {}

// LenRange matches log lines where the field byte length is within [Min, Max].
// An empty Field targets the _msg field. Min/Max can be integers or "inf".
// Syntax (message): len_range(0,100) — (field): field:len_range(0,100)
type LenRange struct {
	Field string // empty = _msg
	Min   string
	Max   string
}

func (l LenRange) String() string {
	inner := l.Min + ", " + l.Max
	if l.Field == "" {
		return "len_range(" + inner + ")"
	}
	return l.Field + ":len_range(" + inner + ")"
}
func (l LenRange) filterExpr() {}

// PatternMatch matches log lines using a pattern with <*> wildcards.
// An empty Field targets the _msg field.
// Syntax (message): pattern("a <*> b") — (field): field:pattern("a <*> b")
type PatternMatch struct {
	Field   string
	Pattern string
}

func (p PatternMatch) String() string {
	inner := `pattern("` + p.Pattern + `")`
	if p.Field == "" {
		return inner
	}
	return p.Field + ":" + inner
}
func (p PatternMatch) filterExpr() {}

// FieldScope applies the filters in parentheses to a field: _msg:(i(error) OR i(warn)).
type FieldScope struct {
	Field string
	Expr  FilterExpr
}

func (f FieldScope) String() string { return f.Field + ":(" + f.Expr.String() + ")" }
func (f FieldScope) filterExpr()    {}

// FilterFunc is a function-style filter whose arguments are kept as written: exact(...), re(...), lt_field(...),
// pattern_match(...) and the like. The arguments are not inspected anywhere in the proxy.
type FilterFunc struct {
	Field string // empty is the message
	Name  string
	Args  string
}

func (f FilterFunc) String() string { return fieldPrefix(f.Field) + f.Name + "(" + f.Args + ")" }
func (f FilterFunc) filterExpr()    {}

// --- DeferredExpr ---

// DeferredExpr holds a raw expression string for deferred/opaque nodes.
// It implements both Expr and FilterExpr.
type DeferredExpr struct {
	Raw string
}

func (d DeferredExpr) String() string { return d.Raw }
func (d DeferredExpr) expr()          {}
func (d DeferredExpr) filterExpr()    {}
func (d DeferredExpr) statsFunc()     {}

// ---------------------------------------------------------------------------
// Core pipe types
// ---------------------------------------------------------------------------

// unpackString prints an unpack_json or unpack_logfmt pipe, name being "| unpack_json" or "| unpack_logfmt" (the
// literal stays in each String method, where conformance/scripts/sync_vl_surface.py looks for it):
// | unpack_json [if (cond)] [from f] [fields (a, b)] [preserve_keys (k)] [result_prefix "p"] [keep_original_fields | skip_empty_results]
// Names are as written, quotes included.
func unpackString(name string, cond FilterExpr, from string, fields, preserveKeys []string, resultPrefix string, keepOriginal, skipEmpty bool) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(ifString(cond))
	if from != "" {
		b.WriteString(" from " + from)
	}
	if len(fields) > 0 {
		b.WriteString(" fields (" + strings.Join(fields, ", ") + ")")
	}
	if len(preserveKeys) > 0 {
		b.WriteString(" preserve_keys (" + strings.Join(preserveKeys, ", ") + ")")
	}
	if resultPrefix != "" {
		b.WriteString(" result_prefix " + quoteLogsQL(resultPrefix))
	}
	b.WriteString(flagsString(keepOriginal, skipEmpty))
	return b.String()
}

// ifString is the " if (cond)" argument of a pipe, empty without a condition.
func ifString(cond FilterExpr) string {
	if cond == nil {
		return ""
	}
	return " if (" + cond.String() + ")"
}

// flagsString is the keep_original_fields or skip_empty_results flag a pipe was given.
func flagsString(keepOriginal, skipEmpty bool) string {
	switch {
	case keepOriginal:
		return " keep_original_fields"
	case skipEmpty:
		return " skip_empty_results"
	}
	return ""
}

// PipeUnpackJSON unpacks JSON fields from log lines.
// When From is non-empty the syntax is "| unpack_json from <From>".
type PipeUnpackJSON struct {
	From         string // empty is _msg
	If           FilterExpr
	Fields       []string // the fields to unpack, empty for all
	PreserveKeys []string // the keys whose nested objects stay JSON text
	ResultPrefix string
	// KeepOriginalFields keeps the fields already set; SkipEmptyResults skips empty results.
	KeepOriginalFields bool
	SkipEmptyResults   bool
}

func (p PipeUnpackJSON) String() string {
	return unpackString("| unpack_json", p.If, p.From, p.Fields, p.PreserveKeys, p.ResultPrefix, p.KeepOriginalFields, p.SkipEmptyResults)
}
func (p PipeUnpackJSON) pipe() {}

// PipeFilter applies an additional filter expression in the pipeline.
type PipeFilter struct {
	Expr FilterExpr
}

func (p PipeFilter) String() string { return string(p.appendTo(make([]byte, 0, 128))) }
func (p PipeFilter) appendTo(b []byte) []byte {
	return appendFilter(append(b, "| filter "...), p.Expr)
}
func (p PipeFilter) pipe() {}

// PipeLimit limits the number of log entries returned.
type PipeLimit struct {
	N int
}

func (p PipeLimit) String() string { return fmt.Sprintf("| limit %d", p.N) }
func (p PipeLimit) pipe()          {}

// --- Helper types for PipeStats and PipeSort ---

// GroupKey is a single field used in a "by (f1, f2)" grouping clause.
type GroupKey struct {
	Field string
	// Bucket groups by the field's value rounded down to a bucket, such as a step for _time ("_time:60s").
	Bucket string
	// Offset shifts the buckets ("_time:5m offset 1h"); it needs a Bucket.
	Offset string
}

func (k GroupKey) String() string {
	s := k.Field
	if k.Bucket != "" {
		s += ":" + k.Bucket
		if k.Offset != "" {
			s += " offset " + k.Offset
		}
	}
	return s
}

// StatsFuncAlias pairs a StatsFunc with its output alias.
type StatsFuncAlias struct {
	Func  StatsFunc
	Alias string
	// If limits the function to the rows that match: count() if (level:error) as errors.
	If FilterExpr
}

// SortField is a single field in a sort expression, with optional descending flag.
type SortField struct {
	Field string
	Desc  bool
}

// --- Parser pipe stages ---

// PipeUnpackLogfmt unpacks logfmt key=value pairs from log lines.
// When From is non-empty the syntax is "| unpack_logfmt from <From>".
type PipeUnpackLogfmt struct {
	From         string // empty is _msg
	If           FilterExpr
	Fields       []string // the fields to unpack, empty for all
	ResultPrefix string
	// KeepOriginalFields keeps the fields already set; SkipEmptyResults skips empty results.
	KeepOriginalFields bool
	SkipEmptyResults   bool
}

func (p PipeUnpackLogfmt) String() string {
	return unpackString("| unpack_logfmt", p.If, p.From, p.Fields, nil, p.ResultPrefix, p.KeepOriginalFields, p.SkipEmptyResults)
}
func (p PipeUnpackLogfmt) pipe() {}

// PipeExtract extracts fields from a log line using a pattern:
// | extract [if (cond)] "pattern" [from f] [keep_original_fields | skip_empty_results]
type PipeExtract struct {
	Pattern string
	From    string     // source field as written, e.g. "_msg"; empty is _msg
	If      FilterExpr // optional condition
	// KeepOriginalFields keeps the fields already set; SkipEmptyResults skips empty results.
	KeepOriginalFields bool
	SkipEmptyResults   bool
}

func (p PipeExtract) String() string { return string(p.appendTo(make([]byte, 0, 128))) }

func (p PipeExtract) appendTo(b []byte) []byte {
	b = append(b, "| extract"...)
	if p.If != nil {
		b = appendFilter(append(b, " if ("...), p.If)
		b = append(b, ')')
	}
	b = strconv.AppendQuote(append(b, ' '), p.Pattern)
	if p.From != "" {
		b = append(append(b, " from "...), p.From...)
	}
	return append(b, flagsString(p.KeepOriginalFields, p.SkipEmptyResults)...)
}
func (p PipeExtract) pipe() {}

// PipeExtractRegexp extracts fields using a named-capture regular expression:
// | extract_regexp [if (cond)] "pattern" [from f] [keep_original_fields | skip_empty_results]
type PipeExtractRegexp struct {
	Pattern string
	From    string // empty is _msg
	If      FilterExpr
	// KeepOriginalFields keeps the fields already set; SkipEmptyResults skips empty results.
	KeepOriginalFields bool
	SkipEmptyResults   bool
	// Quoted writes the regular expression Go-quoted, as the string translator does, instead of backquoted.
	Quoted bool
}

func (p PipeExtractRegexp) String() string {
	pattern := rawOrQuoted(p.Pattern)
	if p.Quoted {
		pattern = strconv.Quote(p.Pattern)
	}
	s := "| extract_regexp" + ifString(p.If) + " " + pattern
	if p.From != "" {
		s += " from " + p.From
	}
	return s + flagsString(p.KeepOriginalFields, p.SkipEmptyResults)
}
func (p PipeExtractRegexp) pipe() {}

// rawOrQuoted writes a regular expression as a backquoted string, which keeps its backslashes as they are, or, when
// it has a backquote or a control character, as a Go-quoted one.
func rawOrQuoted(s string) string {
	if strings.ContainsAny(s, "`") || strings.ContainsFunc(s, unicode.IsControl) || !utf8.ValidString(s) {
		return strconv.Quote(s)
	}
	return "`" + s + "`"
}

// --- Fields pipe stages ---

// PipeFields keeps only the listed fields.
type PipeFields struct{ Labels []string }

func (p PipeFields) String() string { return "| fields " + strings.Join(p.Labels, ", ") }
func (p PipeFields) pipe()          {}

// PipeDelete removes the listed fields.
type PipeDelete struct{ Labels []string }

func (p PipeDelete) String() string { return string(p.appendTo(make([]byte, 0, 64))) }
func (p PipeDelete) appendTo(b []byte) []byte {
	b = append(b, "| delete "...)
	for i, l := range p.Labels {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, l...)
	}
	return b
}
func (p PipeDelete) pipe() {}

// --- Transform pipe stages ---

// PipeFormat formats log fields into a new field using a template:
// | format [if (cond)] "template" [as f] [keep_original_fields | skip_empty_results]
type PipeFormat struct {
	Template    string
	ResultField string // empty is _msg
	If          FilterExpr
	// KeepOriginalFields keeps the value already set; SkipEmptyResults skips an empty result.
	KeepOriginalFields bool
	SkipEmptyResults   bool
}

func (p PipeFormat) String() string {
	s := "| format" + ifString(p.If) + " " + strconv.Quote(p.Template)
	if p.ResultField != "" {
		s += " as " + p.ResultField
	}
	return s + flagsString(p.KeepOriginalFields, p.SkipEmptyResults)
}
func (p PipeFormat) pipe() {}

// PipeRename renames fields using pairs of [old, new] names.
type PipeRename struct{ Pairs [][2]string }

func (p PipeRename) String() string {
	parts := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		parts[i] = pair[0] + " as " + pair[1]
	}
	return "| rename " + strings.Join(parts, ", ")
}
func (p PipeRename) pipe() {}

// PipeReplace replaces occurrences of a substring in a field:
// | replace [if (cond)] ("old", "new") [at f] [limit N]
type PipeReplace struct {
	Field string // empty is _msg
	Old   string
	New   string
	If    FilterExpr
	Limit int // 0 replaces every occurrence
}

func (p PipeReplace) String() string {
	s := fmt.Sprintf(`| replace%s (%s, %s)`, ifString(p.If), strconv.Quote(p.Old), strconv.Quote(p.New))
	return s + atLimitString(p.Field, p.Limit)
}
func (p PipeReplace) pipe() {}

// atLimitString is the " at f" and " limit N" arguments of replace and replace_regexp.
func atLimitString(field string, limit int) string {
	s := ""
	if field != "" {
		s += " at " + field
	}
	if limit > 0 {
		s += " limit " + strconv.Itoa(limit)
	}
	return s
}

// PipeReplaceRegexp replaces regex matches in a field with a replacement string:
// | replace_regexp [if (cond)] ("regexp", "replacement") [at f] [limit N]
type PipeReplaceRegexp struct {
	Field       string // empty is _msg
	Regex       string
	Replacement string
	If          FilterExpr
	Limit       int // 0 replaces every match
}

func (p PipeReplaceRegexp) String() string {
	// VictoriaLogs' syntax: replace_regexp ("regexp", "replacement") at field. The regexp is a raw (backquoted)
	// string, so its backslashes and Unicode classes reach VictoriaLogs as written.
	s := fmt.Sprintf("| replace_regexp%s (%s, %s)", ifString(p.If), rawOrQuoted(p.Regex), strconv.Quote(p.Replacement))
	return s + atLimitString(p.Field, p.Limit)
}
func (p PipeReplaceRegexp) pipe() {}

// PipePackJSON packs the listed fields into a JSON value stored in ResultField:
// | pack_json [fields (a, b)] [as f]
type PipePackJSON struct {
	Fields      []string
	ResultField string // empty is _msg
}

func (p PipePackJSON) String() string { return packString("| pack_json", p.Fields, p.ResultField) }
func (p PipePackJSON) pipe()          {}

// PipePackLogfmt packs the listed fields into a logfmt value stored in ResultField:
// | pack_logfmt [fields (a, b)] [as f]
type PipePackLogfmt struct {
	Fields      []string
	ResultField string // empty is _msg
}

func (p PipePackLogfmt) String() string {
	return packString("| pack_logfmt", p.Fields, p.ResultField)
}
func (p PipePackLogfmt) pipe() {}

func packString(name string, fields []string, result string) string {
	s := name
	if len(fields) > 0 {
		s += " fields (" + strings.Join(fields, ", ") + ")"
	}
	if result != "" {
		s += " as " + result
	}
	return s
}

// --- Aggregation pipe stages ---

// PipeStats groups log entries and computes statistics.
type PipeStats struct {
	By    []GroupKey
	Funcs []StatsFuncAlias
}

func (p PipeStats) String() string { return pipeStatsString("stats", p.By, p.Funcs) }
func (p PipeStats) pipe()          {}

// PipeMath evaluates a math expression and stores the result in Alias.
type PipeMath struct {
	Expr  string
	Alias string
}

func (p PipeMath) String() string { return fmt.Sprintf("| math %s as %s", p.Expr, p.Alias) }
func (p PipeMath) pipe()          {}

// PipeSort sorts log entries by one or more fields.
type PipeSort struct {
	By    []SortField
	Limit int // 0 = no limit
}

func (p PipeSort) String() string {
	parts := make([]string, len(p.By))
	for i, sf := range p.By {
		if sf.Desc {
			parts[i] = sf.Field + " desc"
		} else {
			parts[i] = sf.Field
		}
	}
	s := "| sort by (" + strings.Join(parts, ", ") + ")"
	if p.Limit > 0 {
		s += fmt.Sprintf(" limit %d", p.Limit)
	}
	return s
}
func (p PipeSort) pipe() {}

// PipeTop aggregates top N entries grouped by the given fields.
// Syntax: | top N by (f1, f2)
type PipeTop struct {
	N  int
	By []string
}

func (p PipeTop) String() string {
	s := fmt.Sprintf("| top %d", p.N)
	if len(p.By) > 0 {
		s += " by (" + strings.Join(p.By, ", ") + ")"
	}
	return s
}
func (p PipeTop) pipe() {}

// PipeFirst returns the first N log entries, optionally grouped by fields.
// Syntax: | first [N] [by (f1, f2)]
type PipeFirst struct {
	N  int      // 0 = default (1)
	By []string // empty = no grouping
}

func (p PipeFirst) String() string {
	var b strings.Builder
	b.WriteString("| first")
	if p.N > 0 {
		fmt.Fprintf(&b, " %d", p.N)
	}
	if len(p.By) > 0 {
		b.WriteString(" by (" + strings.Join(p.By, ", ") + ")")
	}
	return b.String()
}
func (p PipeFirst) pipe() {}

// PipeLast returns the last N log entries, optionally grouped by fields.
// Syntax: | last [N] [by (f1, f2)]
type PipeLast struct {
	N  int      // 0 = default (1)
	By []string // empty = no grouping
}

func (p PipeLast) String() string {
	var b strings.Builder
	b.WriteString("| last")
	if p.N > 0 {
		fmt.Fprintf(&b, " %d", p.N)
	}
	if len(p.By) > 0 {
		b.WriteString(" by (" + strings.Join(p.By, ", ") + ")")
	}
	return b.String()
}
func (p PipeLast) pipe() {}

// PipeSample returns a random sample of N log entries.
// Syntax: | sample N
type PipeSample struct{ N int }

func (p PipeSample) String() string { return fmt.Sprintf("| sample %d", p.N) }
func (p PipeSample) pipe()          {}

// PipeOffset skips the first N log entries.
// Syntax: | offset N
type PipeOffset struct{ N int }

func (p PipeOffset) String() string { return fmt.Sprintf("| offset %d", p.N) }
func (p PipeOffset) pipe()          {}

// PipeUniq returns unique rows, optionally considering only given fields.
// Syntax: | uniq [by (f1, f2)]
type PipeUniq struct{ By []string }

func (p PipeUniq) String() string {
	if len(p.By) == 0 {
		return "| uniq"
	}
	return "| uniq by (" + strings.Join(p.By, ", ") + ")"
}
func (p PipeUniq) pipe() {}

// PipeFieldNames returns the names of all fields present in the matching log entries.
// Syntax: | field_names
type PipeFieldNames struct{}

func (p PipeFieldNames) String() string { return "| field_names" }
func (p PipeFieldNames) pipe()          {}

// PipeDropEmptyFields removes fields with empty values from each log entry.
// Syntax: | drop_empty_fields
type PipeDropEmptyFields struct{}

func (p PipeDropEmptyFields) String() string { return "| drop_empty_fields" }
func (p PipeDropEmptyFields) pipe()          {}

// PipeCopy duplicates fields under new names.
// Syntax: | copy src1 as dst1, src2 as dst2
type PipeCopy struct{ Pairs [][2]string }

func (p PipeCopy) String() string {
	parts := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		parts[i] = pair[0] + " as " + pair[1]
	}
	return "| copy " + strings.Join(parts, ", ")
}
func (p PipeCopy) pipe() {}

// PipeCoalesce returns the first non-empty value across the listed fields.
// Syntax: | coalesce(f1, f2, ...) [default "val"] as result
type PipeCoalesce struct {
	Fields  []string
	Default string // optional
	Result  string
}

func (p PipeCoalesce) String() string {
	s := "| coalesce(" + strings.Join(p.Fields, ", ") + ")"
	if p.Default != "" {
		s += " default " + quoteLogsQL(p.Default)
	}
	if p.Result != "" {
		s += " as " + p.Result
	}
	return s
}
func (p PipeCoalesce) pipe() {}

// PipeCollapseNums collapses consecutive numeric runs in a field.
// Syntax: | collapse_nums [at field]
type PipeCollapseNums struct{ Field string }

func (p PipeCollapseNums) String() string {
	if p.Field == "" {
		return "| collapse_nums"
	}
	return "| collapse_nums at " + p.Field
}
func (p PipeCollapseNums) pipe() {}

// PipeDecolorize removes ANSI color escape codes from a field.
// Syntax: | decolorize [field]
type PipeDecolorize struct{ Field string }

func (p PipeDecolorize) String() string {
	if p.Field == "" {
		return "| decolorize"
	}
	return "| decolorize " + p.Field
}
func (p PipeDecolorize) pipe() {}

// PipeFacets extracts unique field name + value combinations.
// Syntax: | facets [limit N]
type PipeFacets struct{ Limit int }

func (p PipeFacets) String() string {
	if p.Limit == 0 {
		return "| facets"
	}
	return fmt.Sprintf("| facets limit %d", p.Limit)
}
func (p PipeFacets) pipe() {}

// PipeFieldValues returns distinct values of a field.
// Syntax: | field_values field [limit N]
type PipeFieldValues struct {
	Field string
	Limit int
}

func (p PipeFieldValues) String() string {
	s := "| field_values " + p.Field
	if p.Limit > 0 {
		s += fmt.Sprintf(" limit %d", p.Limit)
	}
	return s
}
func (p PipeFieldValues) pipe() {}

// PipeGenerateSequence generates a sequence of integers.
// Syntax: | generate_sequence limit N
type PipeGenerateSequence struct{ Limit int }

func (p PipeGenerateSequence) String() string {
	return fmt.Sprintf("| generate_sequence %d", p.Limit)
}
func (p PipeGenerateSequence) pipe() {}

// PipeHash computes a hash of the given fields and stores it in Result.
// Syntax: | hash(f1, f2) as result
type PipeHash struct {
	Fields []string
	Result string
}

func (p PipeHash) String() string {
	return "| hash(" + strings.Join(p.Fields, ", ") + ") as " + p.Result
}
func (p PipeHash) pipe() {}

// PipeJoin joins log entries with the results of an inner query.
// Syntax: | join by (f1, f2) (inner_query) [prefix pfx]
type PipeJoin struct {
	By     []string
	Inner  string // raw inner query string
	Prefix string // optional field-name prefix for joined fields
}

func (p PipeJoin) String() string {
	s := "| join by (" + strings.Join(p.By, ", ") + ") (" + p.Inner + ")"
	if p.Prefix != "" {
		s += " prefix " + p.Prefix
	}
	return s
}
func (p PipeJoin) pipe() {}

// PipeJSONArrayLen returns the length of a JSON array stored in a field.
// Syntax: | json_array_len(field) as result
type PipeJSONArrayLen struct {
	Field  string
	Result string
}

func (p PipeJSONArrayLen) String() string {
	return "| json_array_len(" + p.Field + ") as " + p.Result
}
func (p PipeJSONArrayLen) pipe() {}

// PipeJSONArrayConcat joins the items of a JSON array field into a single
// delimited string.
// Syntax: | json_array_concat [delimiter] [from <src_field>] [as <result_field>]
// All parts are optional; VictoriaLogs defaults the delimiter to "," and the
// source/result fields to _msg when they are omitted.
type PipeJSONArrayConcat struct {
	Delimiter    string // join separator; only emitted when HasDelimiter is set
	HasDelimiter bool   // distinguishes an explicit "" delimiter from an unset one
	From         string // optional source field (default _msg)
	As           string // optional result field
}

func (p PipeJSONArrayConcat) String() string {
	s := "| json_array_concat"
	if p.HasDelimiter {
		s += ` "` + p.Delimiter + `"`
	}
	if p.From != "" {
		s += " from " + p.From
	}
	if p.As != "" {
		s += " as " + p.As
	}
	return s
}
func (p PipeJSONArrayConcat) pipe() {}

// PipeLen returns the byte length of a field as a new field.
// Syntax: | len(field) as result
type PipeLen struct {
	Field  string
	Result string
}

func (p PipeLen) String() string { return "| len(" + p.Field + ") as " + p.Result }
func (p PipeLen) pipe()          {}

// PipeQueryStats returns statistics about the current query execution.
// Syntax: | query_stats
type PipeQueryStats struct{}

func (p PipeQueryStats) String() string { return "| query_stats" }
func (p PipeQueryStats) pipe()          {}

// PipeRunningStats computes cumulative running statistics over a time window.
// Syntax: | running_stats [by (f1, f2)] func() as alias [, ...]
type PipeRunningStats struct {
	By    []GroupKey
	Funcs []StatsFuncAlias
}

func (p PipeRunningStats) String() string { return pipeStatsString("running_stats", p.By, p.Funcs) }
func (p PipeRunningStats) pipe()          {}

// PipeSetStreamFields marks the listed fields as stream-identifying metadata.
// Syntax: | set_stream_fields f1, f2, ...
type PipeSetStreamFields struct{ Fields []string }

func (p PipeSetStreamFields) String() string {
	return "| set_stream_fields " + strings.Join(p.Fields, ", ")
}
func (p PipeSetStreamFields) pipe() {}

// PipeSplit splits the Separator-delimited value of From into individual tokens.
// Syntax: | split "sep" [from field] [as result]
type PipeSplit struct {
	Separator string
	From      string // optional; defaults to _msg
	As        string // optional result field
}

func (p PipeSplit) String() string {
	s := `| split "` + p.Separator + `"`
	if p.From != "" {
		s += " from " + p.From
	}
	if p.As != "" {
		s += " as " + p.As
	}
	return s
}
func (p PipeSplit) pipe() {}

// PipeStreamContext adds surrounding context lines from the same log stream.
// Syntax: | stream_context [before N] [after N]
type PipeStreamContext struct {
	Before int
	After  int
}

func (p PipeStreamContext) String() string {
	s := "| stream_context"
	if p.Before > 0 {
		s += fmt.Sprintf(" before %d", p.Before)
	}
	if p.After > 0 {
		s += fmt.Sprintf(" after %d", p.After)
	}
	return s
}
func (p PipeStreamContext) pipe() {}

// PipeTimeAdd adds a duration to all timestamps in the results.
// Syntax: | time_add duration
type PipeTimeAdd struct{ Duration string }

func (p PipeTimeAdd) String() string { return "| time_add " + p.Duration }
func (p PipeTimeAdd) pipe()          {}

// PipeTotalStats computes aggregate statistics across all matching log entries.
// Syntax: | total_stats [by (f1, f2)] func() as alias [, ...]
type PipeTotalStats struct {
	By    []GroupKey
	Funcs []StatsFuncAlias
}

func (p PipeTotalStats) String() string { return pipeStatsString("total_stats", p.By, p.Funcs) }
func (p PipeTotalStats) pipe()          {}

// PipeUnion combines results from two queries.
// Syntax: | union (inner_query)
type PipeUnion struct{ Inner string }

func (p PipeUnion) String() string { return "| union (" + p.Inner + ")" }
func (p PipeUnion) pipe()          {}

// PipeUnpackSyslog parses RFC-3164/RFC-5424 syslog entries into structured fields.
// Syntax: | unpack_syslog
type PipeUnpackSyslog struct{}

func (p PipeUnpackSyslog) String() string { return "| unpack_syslog" }
func (p PipeUnpackSyslog) pipe()          {}

// PipeUnpackWords splits a field into individual words.
// Syntax: | unpack_words [from field] [as result]
type PipeUnpackWords struct {
	From string // optional; defaults to _msg
	As   string // optional result field
}

func (p PipeUnpackWords) String() string {
	s := "| unpack_words"
	if p.From != "" {
		s += " from " + p.From
	}
	if p.As != "" {
		s += " as " + p.As
	}
	return s
}
func (p PipeUnpackWords) pipe() {}

// PipeUnroll expands JSON array values in the listed fields into separate log entries.
// Syntax: | unroll (f1, f2, ...)
type PipeUnroll struct{ Fields []string }

func (p PipeUnroll) String() string {
	return "| unroll (" + strings.Join(p.Fields, ", ") + ")"
}
func (p PipeUnroll) pipe() {}

// PipeUpdate assigns a new value to a field using an expression.
// Syntax: | update field = expr
type PipeUpdate struct {
	Field string
	Expr  string
}

func (p PipeUpdate) String() string { return "| update " + p.Field + " = " + p.Expr }
func (p PipeUpdate) pipe()          {}

// pipeStatsString is a shared formatter for stats-like pipe stages (stats/running_stats/total_stats).
func pipeStatsString(keyword string, by []GroupKey, funcs []StatsFuncAlias) string {
	var b strings.Builder
	b.WriteString("| ")
	b.WriteString(keyword)
	if len(by) > 0 {
		keys := make([]string, len(by))
		for i, k := range by {
			keys[i] = k.String()
		}
		b.WriteString(" by (")
		b.WriteString(strings.Join(keys, ", "))
		b.WriteString(")")
	}
	if len(funcs) == 0 {
		return "| " + keyword
	}
	for i, fa := range funcs {
		if i == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteString(", ")
		}
		b.WriteString(fa.Func.String())
		b.WriteString(ifString(fa.If))
		if fa.Alias != "" {
			b.WriteString(" as ")
			b.WriteString(fa.Alias)
		}
	}
	return b.String()
}

// --- Stats functions (26 total) ---

// Count counts the number of log entries.
type Count struct {
	// Fields are the fields to count rows with a value in; none counts every row.
	Fields []string
}

func (c Count) String() string { return "count(" + strings.Join(c.Fields, ", ") + ")" }
func (Count) statsFunc()       {}

// Sum computes the sum of a numeric field.
type Sum struct{ Field string }

func (s Sum) String() string { return "sum(" + s.Field + ")" }
func (s Sum) statsFunc()     {}

// Min computes the minimum value of a field.
type Min struct{ Field string }

func (m Min) String() string { return "min(" + m.Field + ")" }
func (m Min) statsFunc()     {}

// Max computes the maximum value of a field.
type Max struct{ Field string }

func (m Max) String() string { return "max(" + m.Field + ")" }
func (m Max) statsFunc()     {}

// Avg computes the average value of a field.
type Avg struct{ Field string }

func (a Avg) String() string { return "avg(" + a.Field + ")" }
func (a Avg) statsFunc()     {}

// Median computes the median value of a field.
type Median struct{ Field string }

func (m Median) String() string { return "median(" + m.Field + ")" }
func (m Median) statsFunc()     {}

// Quantile computes the given quantile (phi) of a field.
type Quantile struct {
	Phi   float64
	Field string
}

func (q Quantile) String() string { return fmt.Sprintf("quantile(%g, %s)", q.Phi, q.Field) }
func (q Quantile) statsFunc()     {}

// Stddev computes the standard deviation of a field.
type Stddev struct{ Field string }

func (s Stddev) String() string { return "stddev(" + s.Field + ")" }
func (s Stddev) statsFunc()     {}

// Stdvar computes the variance of a field.
type Stdvar struct{ Field string }

func (s Stdvar) String() string { return "stdvar(" + s.Field + ")" }
func (s Stdvar) statsFunc()     {}

// Rate computes the ingestion rate.
type Rate struct{}

func (Rate) String() string { return "rate()" }
func (Rate) statsFunc()     {}

// RateSum computes the per-second sum rate of a numeric field (requires VL v1.44+).
type RateSum struct{ Field string }

func (r RateSum) String() string { return "rate_sum(" + r.Field + ")" }
func (r RateSum) statsFunc()     {}

// CountUniq counts the number of unique values of a field.
type CountUniq struct{ Field string }

func (c CountUniq) String() string { return "count_uniq(" + c.Field + ")" }
func (c CountUniq) statsFunc()     {}

// CountUniqHash counts unique values using a hash (approximate, memory-efficient).
type CountUniqHash struct{ Field string }

func (c CountUniqHash) String() string { return "count_uniq_hash(" + c.Field + ")" }
func (c CountUniqHash) statsFunc()     {}

// UniqValues collects unique values of a field up to Limit entries.
type UniqValues struct {
	Field string
	Limit int
}

func (u UniqValues) String() string { return fmt.Sprintf("uniq_values(%s, %d)", u.Field, u.Limit) }
func (u UniqValues) statsFunc()     {}

// FieldMax returns the log entry with the maximum value of a field.
type FieldMax struct{ Field string }

func (f FieldMax) String() string { return "field_max(" + f.Field + ")" }
func (f FieldMax) statsFunc()     {}

// FieldMin returns the log entry with the minimum value of a field.
type FieldMin struct{ Field string }

func (f FieldMin) String() string { return "field_min(" + f.Field + ")" }
func (f FieldMin) statsFunc()     {}

// JSONValues collects field values as a JSON array.
type JSONValues struct{ Field string }

func (j JSONValues) String() string { return "json_values(" + j.Field + ")" }
func (j JSONValues) statsFunc()     {}

// Any returns an arbitrary value of a field.
type Any struct{ Field string }

func (a Any) String() string { return "any(" + a.Field + ")" }
func (a Any) statsFunc()     {}

// CountEmpty counts log entries where the field is empty.
type CountEmpty struct{ Field string }

func (c CountEmpty) String() string { return "count_empty(" + c.Field + ")" }
func (c CountEmpty) statsFunc()     {}

// SumLen computes the total byte length of a field across all log entries.
type SumLen struct{ Field string }

func (s SumLen) String() string { return "sum_len(" + s.Field + ")" }
func (s SumLen) statsFunc()     {}

// Values collects field values up to Limit entries.
type Values struct {
	Field string
	Limit int
}

func (v Values) String() string { return fmt.Sprintf("values(%s, %d)", v.Field, v.Limit) }
func (v Values) statsFunc()     {}

// Histogram computes a histogram of a field (available on every supported version).
type Histogram struct{ Field string }

func (h Histogram) String() string { return "histogram(" + h.Field + ")" }
func (h Histogram) statsFunc()     {}

// Last returns the last value of a field in the time range.
type Last struct{ Field string }

func (l Last) String() string { return "last(" + l.Field + ")" }
func (l Last) statsFunc()     {}

// First returns the first value of a field in the time range.
type First struct{ Field string }

func (f First) String() string { return "first(" + f.Field + ")" }
func (f First) statsFunc()     {}

// RowAny returns an arbitrary log entry with all requested fields.
type RowAny struct{ Fields []string }

func (r RowAny) String() string { return "row_any(" + strings.Join(r.Fields, ", ") + ")" }
func (r RowAny) statsFunc()     {}

// RowMax returns the log entry with the maximum value of By, including the listed fields.
type RowMax struct {
	By     string
	Fields []string
}

func (r RowMax) String() string {
	all := append([]string{r.By}, r.Fields...)
	return "row_max(" + strings.Join(all, ", ") + ")"
}
func (r RowMax) statsFunc() {}

// RowMin returns the log entry with the minimum value of By, including the listed fields.
type RowMin struct {
	By     string
	Fields []string
}

func (r RowMin) String() string {
	all := append([]string{r.By}, r.Fields...)
	return "row_min(" + strings.Join(all, ", ") + ")"
}
func (r RowMin) statsFunc() {}

// JSONValuesSorted collects field values as a sorted JSON array.
type JSONValuesSorted struct{ Field string }

func (j JSONValuesSorted) String() string { return "json_values_sorted(" + j.Field + ")" }
func (j JSONValuesSorted) statsFunc()     {}

// JSONValuesTopK collects the top K most frequent field values as a JSON array.
type JSONValuesTopK struct {
	Field string
	Limit int
}

func (j JSONValuesTopK) String() string {
	return fmt.Sprintf("json_values_topk(%s, %d)", j.Field, j.Limit)
}
func (j JSONValuesTopK) statsFunc() {}
