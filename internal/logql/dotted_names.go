package logql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// DottedNameError returns the parse error Loki v3.7.7 reports for the first
// "." token of query, or "" when the query has none.
//
// Loki's lexer emits "." as its own DOT token, and no grammar rule accepts
// DOT, so a dotted name such as k8s.namespace.name is a syntax error in every
// LogQL position (stream matchers, label filters, by/without/on/ignoring
// lists, keep/drop, label_format, json/logfmt parameters, unwrap). Dots
// inside strings, raw strings, comments, numbers, durations and [range]
// literals are not DOT tokens. The message and position follow Loki: the
// 1-based line and character column of the dot, then goyacc's
// "syntax error: unexpected ." with the expected-token list Loki's parser
// prints in that position (at most four tokens, none when more are possible).
//
// The proxy's own grammar accepts dotted names for the hybrid and native
// metadata modes; the Loki-compatible profile rejects them with this error.
func DottedNameError(query string) string {
	if strings.IndexByte(query, '.') < 0 {
		return ""
	}
	d := dotScanner{src: query, line: 1, col: 1}
	d.parens = d.parensBuf[:0]
	return d.run()
}

type dotTokKind int

const (
	dotTokNone dotTokKind = iota
	dotTokIdent
	dotTokString
	dotTokNumber
	dotTokRange
	dotTokPunct
)

type dotTok struct {
	kind dotTokKind
	text string // lower-cased identifier or punctuation text
}

type dotParenKind int

const (
	parenOther       dotParenKind = iota
	parenGroup                    // by/without/on/ignoring/group_left/group_right label list
	parenConv                     // unwrap bytes(...) / duration(...) / duration_seconds(...)
	parenIP                       // ip("...")
	parenRangeAgg                 // rate( ... count_over_time( ...
	parenVector                   // vector(
	parenLabelFilter              // parenthesised label filter expression
)

type dotParen struct {
	kind dotParenKind
	// prefixGroup marks a grouping list written before the aggregation body
	// (sum by (a) (...)), after which Loki expects "(".
	prefixGroup bool
}

// dotStage is the pipeline stage the scanner is in.
type dotStage int

const (
	stageNone dotStage = iota
	stagePipe
	stageLineFilter
	stageLabelFilter
	stageExtraction // json / logfmt parameter list
	stageStringArg  // regexp / pattern / line_format
	stageKeepDrop
	stageLabelFormat
	stageUnwrap
	stagePlain // decolorize / unpack
)

type dotScanner struct {
	src       string
	pos       int
	line, col int

	prev, prevPrev dotTok
	braceDepth     int
	parens         []dotParen
	closedParen    dotParen // paren of the ")" just scanned
	closedValid    bool
	parensBuf      [8]dotParen
	stage          dotStage
	lfDst          bool // label_format: next identifier is a destination
	unwrapName     bool // unwrap: the label name was read
	identEnd       int  // byte offset just after the last identifier
}

var rangeAggNames = map[string]bool{
	"rate": true, "rate_counter": true, "count_over_time": true, "bytes_rate": true,
	"bytes_over_time": true, "avg_over_time": true, "sum_over_time": true,
	"min_over_time": true, "max_over_time": true, "stdvar_over_time": true,
	"stddev_over_time": true, "quantile_over_time": true, "first_over_time": true,
	"last_over_time": true, "absent_over_time": true,
}

var vectorAggNames = map[string]bool{
	"sum": true, "avg": true, "min": true, "max": true, "count": true,
	"stddev": true, "stdvar": true, "bottomk": true, "topk": true,
	"sort": true, "sort_desc": true, "approx_topk": true,
}

var groupKeywords = map[string]bool{
	"by": true, "without": true, "on": true, "ignoring": true,
	"group_left": true, "group_right": true,
}

var unwrapConversions = map[string]bool{
	"bytes": true, "duration": true, "duration_seconds": true,
}

func (d *dotScanner) peek() (rune, int) {
	if d.pos >= len(d.src) {
		return utf8.RuneError, 0
	}
	return utf8.DecodeRuneInString(d.src[d.pos:])
}

func (d *dotScanner) advance() rune {
	r, size := d.peek()
	if size == 0 {
		return r
	}
	d.pos += size
	if r == '\n' {
		d.line++
		d.col = 1
	} else {
		d.col++
	}
	return r
}

func (d *dotScanner) run() string {
	for {
		d.skipSpaceAndComments()
		r, size := d.peek()
		if size == 0 {
			return ""
		}
		line, col := d.line, d.col
		switch {
		case r == '.':
			unexpected := "."
			if next := d.peekAt(1); next >= '0' && next <= '9' {
				if d.prev.kind != dotTokIdent || d.identEnd != d.pos {
					d.scanNumber()
					d.push(dotTok{kind: dotTokNumber})
					continue
				}
				// name.5: Loki lexes ".5" as a NUMBER right after the name.
				unexpected = "NUMBER"
			}
			msg := "syntax error: unexpected " + unexpected
			if expecting := d.expecting(); expecting != "" {
				msg += ", expecting " + expecting
			}
			return fmt.Sprintf("parse error at line %d, col %d: %s", line, col, msg)
		case r == '"' || r == '`' || r == '\'':
			d.scanQuoted(r)
			d.push(dotTok{kind: dotTokString})
		case r == '[':
			for d.pos < len(d.src) {
				if d.advance() == ']' {
					break
				}
			}
			d.push(dotTok{kind: dotTokRange})
		case unicode.IsLetter(r) || r == '_':
			start := d.pos
			for {
				c, n := d.peek()
				if n == 0 || !isIdentRune(c) {
					break
				}
				d.advance()
			}
			d.ident(strings.ToLower(d.src[start:d.pos]))
			d.identEnd = d.pos
		case r >= '0' && r <= '9':
			d.scanNumber()
			d.push(dotTok{kind: dotTokNumber})
		default:
			d.punct(d.scanPunct())
		}
	}
}

// isIdentRune reports whether r continues a Go-style identifier, the
// identifiers Loki's text/scanner lexer reads.
func isIdentRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func (d *dotScanner) peekAt(offset int) rune {
	p := d.pos
	for i := 0; i < offset && p < len(d.src); i++ {
		_, n := utf8.DecodeRuneInString(d.src[p:])
		p += n
	}
	if p >= len(d.src) {
		return utf8.RuneError
	}
	r, _ := utf8.DecodeRuneInString(d.src[p:])
	return r
}

// skipSpaceAndComments skips whitespace and the comments Loki's lexer drops:
// "#" to the end of the line, and Go-style // and /* */ comments.
func (d *dotScanner) skipSpaceAndComments() {
	for {
		r, size := d.peek()
		switch {
		case size == 0:
			return
		case unicode.IsSpace(r):
			d.advance()
		case r == '#':
			for d.pos < len(d.src) && d.src[d.pos] != '\n' {
				d.advance()
			}
		case r == '/' && d.peekAt(1) == '/':
			for d.pos < len(d.src) && d.src[d.pos] != '\n' {
				d.advance()
			}
		case r == '/' && d.peekAt(1) == '*':
			d.advance()
			d.advance()
			for d.pos < len(d.src) && !strings.HasPrefix(d.src[d.pos:], "*/") {
				d.advance()
			}
			if d.pos < len(d.src) {
				d.advance()
				d.advance()
			}
		default:
			return
		}
	}
}

func (d *dotScanner) scanQuoted(quote rune) {
	d.advance()
	for d.pos < len(d.src) {
		r := d.advance()
		if r == '\\' && quote != '`' {
			d.advance()
			continue
		}
		if r == quote {
			return
		}
	}
}

// scanNumber consumes a number together with the duration or byte-size
// suffix Loki's lexer folds into it (1.5h, 10KB, 0.5).
func (d *dotScanner) scanNumber() {
	for {
		r, size := d.peek()
		if size == 0 || (!isIdentRune(r) && r != '.') {
			return
		}
		d.advance()
	}
}

func (d *dotScanner) scanPunct() string {
	start := d.pos
	d.advance()
	if d.pos+1 <= len(d.src) && d.pos == start+1 && d.pos < len(d.src) {
		switch two := d.src[start : d.pos+1]; two {
		case "|=", "|~", "|>", "!=", "!~", "!>", "=~", "==", ">=", "<=":
			d.advance()
			return two
		}
	}
	return d.src[start:d.pos]
}

func (d *dotScanner) push(t dotTok) {
	d.prevPrev = d.prev
	d.prev = t
	d.closedValid = false
}

func (d *dotScanner) ident(name string) {
	switch {
	case d.braceDepth > 0:
	case d.stage == stagePipe:
		switch name {
		case "json", "logfmt":
			d.stage = stageExtraction
		case "regexp", "pattern", "line_format":
			d.stage = stageStringArg
		case "keep", "drop":
			d.stage = stageKeepDrop
		case "label_format":
			d.stage = stageLabelFormat
			d.lfDst = true
		case "unwrap":
			d.stage = stageUnwrap
			d.unwrapName = false
		case "decolorize", "unpack":
			d.stage = stagePlain
		default:
			d.stage = stageLabelFilter
		}
	case d.stage == stageLabelFormat:
		if d.prev.kind == dotTokPunct && d.prev.text == "=" {
			d.lfDst = false
		}
	case d.stage == stageUnwrap && d.prev.kind == dotTokIdent && d.prev.text == "unwrap":
		d.unwrapName = !unwrapConversions[name]
	}
	d.push(dotTok{kind: dotTokIdent, text: name})
}

func (d *dotScanner) punct(p string) {
	var closed dotParen
	closedValid := false
	switch p {
	case "{":
		d.braceDepth++
	case "}":
		if d.braceDepth > 0 {
			d.braceDepth--
		}
		d.stage = stageNone
	case "|":
		if d.braceDepth == 0 {
			d.stage = stagePipe
		}
	case "|=", "|~", "|>", "!>":
		d.stage = stageLineFilter
	case "!=", "!~":
		if d.braceDepth == 0 && d.stage != stageLabelFilter && d.stage != stageKeepDrop && d.stage != stageExtraction {
			d.stage = stageLineFilter
		}
	case ",":
		if d.stage == stageLabelFormat {
			d.lfDst = true
		}
	case "(":
		d.parens = append(d.parens, d.openParen())
	case ")":
		if n := len(d.parens); n > 0 {
			top := d.parens[n-1]
			d.parens = d.parens[:n-1]
			closed, closedValid = top, true
			if top.kind == parenRangeAgg || top.kind == parenVector {
				d.stage = stageNone
			}
		}
	}
	d.push(dotTok{kind: dotTokPunct, text: p})
	d.closedParen, d.closedValid = closed, closedValid
}

func (d *dotScanner) openParen() dotParen {
	if d.prev.kind == dotTokIdent {
		name := d.prev.text
		switch {
		case groupKeywords[name]:
			prefix := (name == "by" || name == "without") && d.prevPrev.kind == dotTokIdent && vectorAggNames[d.prevPrev.text]
			return dotParen{kind: parenGroup, prefixGroup: prefix}
		case d.stage == stageUnwrap && unwrapConversions[name]:
			return dotParen{kind: parenConv}
		case name == "ip":
			return dotParen{kind: parenIP}
		case rangeAggNames[name] && d.braceDepth == 0:
			return dotParen{kind: parenRangeAgg}
		case name == "vector":
			return dotParen{kind: parenVector}
		}
	}
	if d.stage == stagePipe || d.stage == stageLabelFilter {
		d.stage = stageLabelFilter
		return dotParen{kind: parenLabelFilter}
	}
	return dotParen{kind: parenOther}
}

func (d *dotScanner) topParen() (dotParen, bool) {
	if len(d.parens) == 0 {
		return dotParen{}, false
	}
	return d.parens[len(d.parens)-1], true
}

func (d *dotScanner) prevIs(text string) bool {
	return d.prev.kind == dotTokPunct && d.prev.text == text
}

// expecting returns the expected-token list Loki's parser prints for a DOT
// read after the tokens scanned so far (observed against Loki v3.7.7).
//
//nolint:gocyclo // one case per parser state; a flat table is the clearest form.
func (d *dotScanner) expecting() string {
	prev := d.prev
	if d.braceDepth > 0 {
		switch {
		case d.prevIs("{"):
			return "IDENTIFIER or }"
		case d.prevIs(","):
			return "IDENTIFIER"
		case prev.kind == dotTokIdent:
			return "= or =~ or !~ or !="
		case d.prevIs("=") || d.prevIs("!=") || d.prevIs("=~") || d.prevIs("!~"):
			return "STRING"
		case prev.kind == dotTokString:
			return "} or ,"
		}
		return ""
	}
	if top, ok := d.topParen(); ok {
		switch top.kind {
		case parenGroup:
			switch {
			case d.prevIs("("):
				return "IDENTIFIER or )"
			case d.prevIs(","):
				return "IDENTIFIER"
			case prev.kind == dotTokIdent:
				return ", or )"
			}
		case parenConv:
			switch {
			case d.prevIs("("):
				return "IDENTIFIER"
			case prev.kind == dotTokIdent:
				return ")"
			}
		case parenIP:
			if d.prevIs("(") {
				return "STRING"
			}
		case parenRangeAgg:
			switch {
			case d.prevIs("("):
				return "NUMBER or { or ("
			case prev.kind == dotTokRange:
				return ")"
			}
		case parenVector:
			if d.prevIs("(") {
				return "NUMBER"
			}
		}
	}
	if d.closedValid && d.closedParen.kind == parenGroup && d.closedParen.prefixGroup {
		return "("
	}
	if prev.kind == dotTokIdent {
		switch prev.text {
		case "by", "without", "on", "ignoring":
			if d.stage == stageNone {
				return "("
			}
		case "offset":
			if d.stage == stageNone {
				return "DURATION"
			}
		}
	}
	switch d.stage {
	case stageLineFilter:
		if d.prevIs("|=") || d.prevIs("|~") || d.prevIs("|>") || d.prevIs("!=") || d.prevIs("!~") || d.prevIs("!>") ||
			(prev.kind == dotTokIdent && prev.text == "or") {
			return "STRING or ip"
		}
	case stageLabelFilter:
		if d.prevIs(",") || d.prevIs("(") || (prev.kind == dotTokIdent && (prev.text == "and" || prev.text == "or")) {
			return "IDENTIFIER or ("
		}
	case stageExtraction:
		switch {
		case d.prevIs(","):
			return "IDENTIFIER"
		case d.prevIs("="):
			return "STRING"
		}
	case stageStringArg:
		if prev.kind == dotTokIdent {
			return "STRING"
		}
	case stageKeepDrop:
		if d.prevIs(",") || (prev.kind == dotTokIdent && (prev.text == "keep" || prev.text == "drop") && d.prevPrev.kind == dotTokPunct && d.prevPrev.text == "|") {
			return "IDENTIFIER"
		}
	case stageLabelFormat:
		switch {
		case d.prevIs(","):
			return "IDENTIFIER"
		case d.prevIs("="):
			return "IDENTIFIER or STRING"
		case prev.kind == dotTokIdent && prev.text == "label_format" && d.prevPrev.kind == dotTokPunct && d.prevPrev.text == "|":
			return "IDENTIFIER"
		case prev.kind == dotTokIdent && d.lfDst:
			return "="
		}
	case stageUnwrap:
		switch {
		case prev.kind == dotTokIdent && prev.text == "unwrap":
			return "IDENTIFIER or BYTES_CONV or DURATION_CONV or DURATION_SECONDS_CONV"
		case prev.kind == dotTokIdent && d.unwrapName:
			return "RANGE or |"
		}
	}
	return ""
}
