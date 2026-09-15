package logql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"unicode/utf8"
)

// Stage validation mirrors the checks Loki runs when it builds a query
// pipeline (pkg/logql/log and pkg/logql/syntax, v3.7). Loki answers every one
// of these failures with HTTP 400, so the proxy rejects the same queries before
// they reach VictoriaLogs instead of returning a 200 that silently ignores the
// broken stage.

// lokiErrorLabel is logqlmodel.ErrorLabel.
const lokiErrorLabel = "__error__"

// lokiTemplateFunctions lists every function name Loki registers for
// line_format and label_format templates: functionMap, the sprig subset in
// templateFunctions, and __line__/__timestamp__ (pkg/logql/log/fmt.go).
// text/template only needs the names to parse, so each maps to a stub.
var lokiTemplateFunctions = func() template.FuncMap {
	names := []string{
		// functionMap
		"ToLower", "ToUpper", "Replace", "Trim", "TrimLeft", "TrimRight",
		"TrimPrefix", "TrimSuffix", "TrimSpace", "regexReplaceAll",
		"regexReplaceAllLiteral", "count", "urldecode", "urlencode", "bytes",
		"duration", "duration_seconds", "unixEpochMillis", "unixEpochNanos",
		"toDateInZone", "unixToTime", "alignLeft", "alignRight",
		// sprig templateFunctions
		"b64enc", "b64dec", "lower", "upper", "title", "trunc", "substr",
		"contains", "hasPrefix", "hasSuffix", "indent", "nindent", "replace",
		"repeat", "trim", "trimAll", "trimSuffix", "trimPrefix", "int",
		"float64", "add", "sub", "mul", "div", "mod", "addf", "subf", "mulf",
		"divf", "max", "min", "maxf", "minf", "ceil", "floor", "round",
		"fromJson", "date", "toDate", "now", "unixEpoch", "default",
		// line and timestamp accessors
		"__line__", "__timestamp__",
	}
	stub := func(...interface{}) interface{} { return nil }
	funcs := make(template.FuncMap, len(names))
	for _, name := range names {
		funcs[name] = stub
	}
	return funcs
}()

// validateTemplate parses tmpl the way Loki's formatters do; name is the
// template name Loki uses in the error ("line" or "label").
func validateTemplate(name, tmpl string) error {
	_, err := template.New(name).Option("missingkey=zero").Funcs(lokiTemplateFunctions).Parse(tmpl)
	return err
}

// stageError mirrors logqlmodel.NewStageError.
func stageError(stage string, err error) string {
	return newParseError(0, 0, fmt.Sprintf("stage '%s' : %s", stage, err)).Msg
}

// validateStageSemantics returns Loki's error for a pipeline stage that Loki
// refuses to build, or "".
func validateStageSemantics(stage Stage) string {
	switch s := stage.(type) {
	case *ParserStage:
		return validateParserStage(s)
	case *LabelFormatStage:
		return validateLabelFormatStage(s)
	case *LineFormatStage:
		if err := validateTemplate("line", s.Template); err != nil {
			return stageError("| line_format "+strconv.Quote(s.Template), fmt.Errorf("invalid line template: %w", err))
		}
	}
	return ""
}

func validateParserStage(s *ParserStage) string {
	switch s.Type {
	case ParserRegexp:
		// syntax.newLineParserExpr → log.NewRegexpParser (parse-time in Loki).
		if err := validateRegexpParser(s.Param); err != nil {
			return newParseError(0, 0, "invalid regexp parser: "+err.Error()).Msg
		}
	case ParserPattern:
		// syntax.newLineParserExpr → log.NewPatternParser (parse-time in Loki).
		if err := validatePatternParser(s.Param); err != nil {
			return newParseError(0, 0, "invalid pattern parser: "+err.Error()).Msg
		}
	case ParserJSON, ParserLogfmt:
		if len(s.Fields) == 0 {
			return ""
		}
		kind, validate := "json", validateJSONExpression
		if s.Type == ParserLogfmt {
			kind, validate = "logfmt", validateLogfmtExpression
		}
		for _, f := range s.Fields {
			if err := validate(f.Expression); err != nil {
				return stageError(extractionStageString(kind, s.Fields), fmt.Errorf("cannot parse expression [%s]: %w", f.Expression, err))
			}
		}
	}
	return ""
}

// extractionStageString renders the stage like Loki's
// JSONExpressionParserExpr/LogfmtExpressionParserExpr String().
func extractionStageString(kind string, fields []ExtractionField) string {
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, f.Name+"="+strconv.Quote(f.Expression))
	}
	return "| " + kind + " " + strings.Join(parts, ",")
}

// validateLabelFormatStage mirrors log.NewLabelsFormatter.
func validateLabelFormatStage(s *LabelFormatStage) string {
	parts := make([]string, 0, len(s.Formats))
	for _, f := range s.Formats {
		value := strconv.Quote(f.Value)
		if f.Rename {
			value = f.Value
		}
		parts = append(parts, f.Name+"="+value)
	}
	stage := "| label_format " + strings.Join(parts, ",")
	seen := make(map[string]struct{}, len(s.Formats))
	for _, f := range s.Formats {
		if f.Name == lokiErrorLabel {
			return stageError(stage, fmt.Errorf("%s cannot be formatted", f.Name))
		}
		if _, ok := seen[f.Name]; ok {
			return stageError(stage, fmt.Errorf("multiple label name '%s' not allowed in a single format operation", f.Name))
		}
		seen[f.Name] = struct{}{}
	}
	for _, f := range s.Formats {
		if f.Rename {
			continue
		}
		if err := validateTemplate("label", f.Value); err != nil {
			return stageError(stage, fmt.Errorf("invalid template for label '%s': %s", f.Name, err))
		}
	}
	return ""
}

var errMissingNamedCapture = errors.New("at least one named capture must be supplied")

// validateRegexpParser mirrors log.NewRegexpParser.
func validateRegexpParser(re string) error {
	compiled, err := regexp.Compile(re)
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, name := range compiled.SubexpNames() {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate extracted label name '%s'", name)
		}
		seen[name] = struct{}{}
	}
	if len(seen) == 0 {
		return errMissingNamedCapture
	}
	return nil
}

// validatePatternParser mirrors pattern.New: the lexer turns <name> (ASCII
// letter or underscore, then letters, digits, underscores) into captures and
// everything else into literals; <_> is an unnamed capture.
func validatePatternParser(pattern string) error {
	if pattern == "" {
		return errors.New("parse error at line 1, col 1: syntax error: unexpected $end, expecting IDENTIFIER or LITERAL")
	}
	var (
		captures     []string
		named        []string
		prevCapture  bool
		consecutives string
	)
	for i := 0; i < len(pattern); {
		if name, size := patternCapture(pattern[i:]); size > 0 {
			if prevCapture && consecutives == "" {
				consecutives = "<" + captures[len(captures)-1] + "><" + name + ">"
			}
			captures = append(captures, name)
			if name != "_" {
				named = append(named, name)
			}
			prevCapture = true
			i += size
			continue
		}
		_, size := utf8.DecodeRuneInString(pattern[i:])
		prevCapture = false
		i += size
	}
	if len(named) == 0 {
		return errors.New("at least one capture is required")
	}
	if consecutives != "" {
		return fmt.Errorf("found consecutive capture '%s': invalid expression", consecutives)
	}
	seen := make(map[string]struct{}, len(named))
	for _, name := range named {
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate capture name (%s): invalid expression", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// patternCapture reports the capture name and byte length when s starts with
// a pattern identifier token (<name>).
func patternCapture(s string) (string, int) {
	if len(s) < 3 || s[0] != '<' || (!isASCIILetter(s[1]) && s[1] != '_') {
		return "", 0
	}
	for j := 2; j < len(s); j++ {
		switch c := s[j]; {
		case c == '>':
			return s[1:j], j + 1
		case isASCIILetter(c) || c == '_' || (c >= '0' && c <= '9'):
		default:
			return "", 0
		}
	}
	return "", 0
}

func isASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

// Token names of Loki's jsonexpr and logfmt expression grammars.
const (
	exprEnd    = "$end"
	exprDot    = "DOT"
	exprLSB    = "LSB"
	exprRSB    = "RSB"
	exprString = "STRING"
	exprField  = "FIELD"
	exprIndex  = "INDEX"
	exprKey    = "KEY"
)

// exprLexer reproduces the rune reader shared by Loki's jsonexpr and logfmt
// expression scanners, including their EOF-as-NUL and unread semantics.
type exprLexer struct {
	runes  []rune
	pos    int
	lastOK bool
}

func (l *exprLexer) read() rune {
	if l.pos >= len(l.runes) {
		l.lastOK = false
		return 0
	}
	r := l.runes[l.pos]
	l.pos++
	l.lastOK = true
	return r
}

func (l *exprLexer) unread() {
	if l.lastOK {
		l.pos--
		l.lastOK = false
	}
}

func exprWhitespace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' }

func exprIdentStart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

func (l *exprLexer) scanField() {
	for {
		r := l.read()
		if (!exprIdentStart(r) && (r < '0' || r > '9')) || r == 0 {
			l.unread()
			return
		}
	}
}

// scanStr consumes a string started by '"'; like Loki it stops at '"', ']'
// (consuming it) or end of input.
func (l *exprLexer) scanStr() {
	l.read() // opening quote
	for {
		r := l.read()
		if r == 0 || r == '"' || r == ']' {
			return
		}
	}
}

// jsonToken returns the next jsonexpr token, or ("", err) for a lexer error
// (which Loki's lexer reports as end of input).
func (l *exprLexer) jsonToken() (string, error) {
	for {
		r := l.read()
		switch {
		case r == 0:
			return exprEnd, nil
		case exprWhitespace(r):
			continue
		case r >= '0' && r <= '9':
			l.unread()
			return l.scanIndex()
		case r == '[':
			return exprLSB, nil
		case r == ']':
			return exprRSB, nil
		case r == '.':
			return exprDot, nil
		case exprIdentStart(r):
			l.unread()
			l.scanField()
			return exprField, nil
		case r == '"':
			l.unread()
			l.scanStr()
			return exprString, nil
		default:
			return "", fmt.Errorf("unexpected char %c", r)
		}
	}
}

func (l *exprLexer) scanIndex() (string, error) {
	var digits []rune
	for {
		r := l.read()
		if r == '.' && len(digits) > 0 {
			return "", errors.New("cannot use float as array index")
		}
		if exprWhitespace(r) || r == '.' || r == ']' {
			l.unread()
			break
		}
		if r < '0' || r > '9' {
			return "", fmt.Errorf("non-integer value: %c", r)
		}
		digits = append(digits, r)
	}
	if _, err := strconv.Atoi(string(digits)); err != nil {
		return "", err
	}
	return exprIndex, nil
}

// validateJSONExpression mirrors jsonexpr.Parse acceptance:
//
//	values: field | [key] | [index] | values [key] | values [index] | values . field
func validateJSONExpression(expr string) error {
	const (
		stateStart = iota
		stateBracket
		stateCloseBracket
		stateDot
		stateComplete
	)
	l := &exprLexer{runes: []rune(expr)}
	state := stateStart
	for {
		tok, lexErr := l.jsonToken()
		if lexErr != nil {
			if state == stateComplete {
				return lexErr
			}
			tok = exprEnd
		}
		switch state {
		case stateStart:
			switch tok {
			case exprField:
				state = stateComplete
			case exprLSB:
				state = stateBracket
			default:
				return exprSyntaxError(tok, "LSB or FIELD")
			}
		case stateBracket:
			if tok != exprString && tok != exprIndex {
				return exprSyntaxError(tok, "STRING or INDEX")
			}
			state = stateCloseBracket
		case stateCloseBracket:
			if tok != exprRSB {
				return exprSyntaxError(tok, "RSB")
			}
			state = stateComplete
		case stateDot:
			if tok != exprField {
				return exprSyntaxError(tok, "FIELD")
			}
			state = stateComplete
		case stateComplete:
			switch tok {
			case exprEnd:
				return nil
			case exprDot:
				state = stateDot
			case exprLSB:
				state = stateBracket
			default:
				return exprSyntaxError(tok, "")
			}
		}
	}
}

// validateLogfmtExpression mirrors logfmt.Parse acceptance:
//
//	expressions: KEY | STRING | expressions STRING
func validateLogfmtExpression(expr string) error {
	l := &exprLexer{runes: []rune(expr)}
	started := false
	for {
		tok, lexErr := l.logfmtToken()
		if lexErr != nil {
			if started {
				return lexErr
			}
			tok = exprEnd
		}
		switch {
		case tok == exprEnd && started:
			return nil
		case tok == exprEnd:
			return exprSyntaxError(tok, "STRING or KEY")
		case tok == exprKey && started:
			return exprSyntaxError(tok, "")
		}
		started = true
	}
}

func (l *exprLexer) logfmtToken() (string, error) {
	for {
		r := l.read()
		switch {
		case r == 0:
			return exprEnd, nil
		case exprWhitespace(r):
			continue
		case exprIdentStart(r):
			l.unread()
			l.scanField()
			return exprKey, nil
		case r == '"':
			l.unread()
			l.scanStr()
			return exprString, nil
		default:
			return "", fmt.Errorf("unexpected char %c", r)
		}
	}
}

func exprSyntaxError(tok, expecting string) error {
	if expecting == "" {
		return fmt.Errorf("syntax error: unexpected %s", tok)
	}
	return fmt.Errorf("syntax error: unexpected %s, expecting %s", tok, expecting)
}
