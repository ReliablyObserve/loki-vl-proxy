package proxy

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/grafana/jsonparser"
	fj "github.com/valyala/fastjson"
)

// Settings of -logql-dotted-names and -label-browse-extensions.
const (
	CompatAuto   = "auto"
	CompatReject = "reject"
	CompatAccept = "accept"
	CompatOn     = "on"
	CompatOff    = "off"
)

// lokiProfile reports whether the label settings form the Loki-compatible
// profile: underscore label names and translated metadata fields, so every
// name a client sees is a valid Loki label name. "auto" settings follow it:
// Loki's grammar for LogQL names, Loki's label endpoint parameters. The hybrid
// and native metadata modes (and passthrough labels) expose dotted
// VictoriaLogs field names on purpose, so "auto" keeps their extensions on.
func lokiProfile(style LabelStyle, mode MetadataFieldMode) bool {
	return style == LabelStyleUnderscores && mode == MetadataFieldModeTranslated
}

// resolveDottedNames returns whether LogQL dotted names are rejected with
// Loki's parse error for -logql-dotted-names=setting.
func resolveDottedNames(setting string, style LabelStyle, mode MetadataFieldMode) (reject bool, err error) {
	switch strings.TrimSpace(setting) {
	case "", CompatAuto:
		return lokiProfile(style, mode), nil
	case CompatReject:
		return true, nil
	case CompatAccept:
		return false, nil
	}
	return false, fmt.Errorf("invalid -logql-dotted-names %q: want auto, reject or accept", setting)
}

// resolveLabelBrowse returns whether the label endpoints honour the proxy's
// browse parameters for -label-browse-extensions=setting.
func resolveLabelBrowse(setting string, style LabelStyle, mode MetadataFieldMode, indexedCache bool) (bool, error) {
	switch strings.TrimSpace(setting) {
	case "", CompatAuto:
		return !lokiProfile(style, mode) || indexedCache, nil
	case CompatOn:
		return true, nil
	case CompatOff:
		return false, nil
	}
	return false, fmt.Errorf("invalid -label-browse-extensions %q: want auto, on or off", setting)
}

// lokiNameError returns Loki's parse error for a LogQL name Loki's grammar
// rejects (a "." token), or "" when dotted names are accepted.
func (p *Proxy) lokiNameError(query string) string {
	if !p.rejectDottedNames {
		return ""
	}
	return logqlpkg.DottedNameError(query)
}

// lokiNameCheck returns lokiNameError for selector-parameter validation, or
// nil when dotted names are accepted.
func (p *Proxy) lokiNameCheck() func(string) string {
	if !p.rejectDottedNames {
		return nil
	}
	return logqlpkg.DottedNameError
}

// labelBrowseExtensions reports whether the label endpoints honour the
// proxy's browse parameters (search/q on /labels and /label/{name}/values,
// limit and offset on /label/{name}/values). Loki reads only start, end and
// query there (loghttp.ParseLabelQuery) and returns every name or value.
func (p *Proxy) labelBrowseExtensions() bool {
	return p.labelBrowse
}

// detectedFieldName is the detected_fields label of a key parsed from a JSON
// log line. Loki's json parser sanitizes keys (http.method -> http_method) and
// reports the original key in jsonPath; while dotted names are rejected in
// LogQL, the proxy does the same, so every field Drilldown offers is a name
// the proxy accepts. With dotted names accepted, the stored name is kept.
func (p *Proxy) detectedFieldName(key string) string {
	if !p.rejectDottedNames {
		return key
	}
	return lokiJSONKeyLabel(key)
}

// lokiJSONKeyLabel is Loki's sanitizeLabelKey for a top-level parsed key:
// surrounding space trimmed, a leading digit prefixed with an underscore, and
// every rune outside [A-Za-z0-9_] replaced by an underscore (no collapsing).
func lokiJSONKeyLabel(key string) string {
	return lokiJSONPathLabel([]string{key})
}

// lokiJSONPathLabel is the label Loki's json parser gives a nested key: the
// sanitized keys of its path joined by underscores, a leading digit of the
// first one prefixed with an underscore (buildSanitizedPrefixFromBuffer).
func lokiJSONPathLabel(path []string) string {
	var b strings.Builder
	for _, part := range path {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('_')
		} else if part[0] >= '0' && part[0] <= '9' {
			b.WriteByte('_')
		}
		for _, r := range part {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				b.WriteRune(r)
			} else {
				b.WriteByte('_')
			}
		}
	}
	return b.String()
}

// visitJSONLineLeaves calls fn for every string, number and boolean value of
// a JSON object, nested objects included, with its key path: the values
// Loki's json parser turns into labels (arrays and nulls give none).
func visitJSONLineLeaves(obj *fj.Object, path []string, fn func(path []string, v *fj.Value)) {
	obj.Visit(func(key []byte, v *fj.Value) {
		keyPath := append(path[:len(path):len(path)], string(key))
		switch v.Type() {
		case fj.TypeObject:
			if nested, err := v.Object(); err == nil {
				visitJSONLineLeaves(nested, keyPath, fn)
			}
		case fj.TypeString, fj.TypeNumber, fj.TypeTrue, fj.TypeFalse:
			fn(keyPath, v)
		}
	})
}

// jsonLineLabels returns the labels Loki's json parser reads from a JSON
// object line, or nil when the line is not one.
func jsonLineLabels(line []byte) map[string]bool {
	if len(line) < 2 || line[0] != '{' {
		return nil
	}
	var parser fj.Parser
	value, err := parser.ParseBytes(line)
	if err != nil {
		return nil
	}
	obj, err := value.Object()
	if err != nil {
		return nil
	}
	labels := make(map[string]bool, obj.Len())
	visitJSONLineLeaves(obj, nil, func(path []string, _ *fj.Value) {
		labels[lokiJSONPathLabel(path)] = true
	})
	return labels
}

// jsonLineMayHold reports whether a JSON line with these top-level keys may
// hold the label name: a key named so, or an object key the name continues
// (svc_name under {"svc":{...}}). It spares the full parse of lines that
// cannot.
func jsonLineMayHold(lineKeys map[string]bool, name string) bool {
	for key, object := range lineKeys {
		label := key
		if !isLokiLabelName(key) {
			label = lokiJSONKeyLabel(key)
		}
		if label == name || (object && strings.HasPrefix(name, label+"_")) {
			return true
		}
	}
	return false
}

// isLokiLabelName reports whether s is already a label name Loki's json
// parser keeps as is: [A-Za-z_][A-Za-z0-9_]*.
func isLokiLabelName(s string) bool {
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		default:
			return false
		}
	}
	return true
}

// logfmtLineHasKey reports whether a logfmt line holds key, tokenizing as
// parseLogfmtFields does (Loki's decoder ends a token at any whitespace byte
// outside quotes).
func logfmtLineHasKey(line []byte, key string) bool {
	return logfmtLineHasKeyFunc(line, func(k []byte) bool { return string(k) == key })
}

// logfmtLineHasLabel reports whether Loki's logfmt parser reads the label
// name from a line: a key that is the name, or sanitizes to it.
func logfmtLineHasLabel(line []byte, name string) bool {
	return logfmtLineHasKeyFunc(line, func(k []byte) bool {
		if string(k) == name {
			return true
		}
		return !isLokiLabelName(string(k)) && lokiJSONKeyLabel(string(k)) == name
	})
}

func logfmtLineHasKeyFunc(line []byte, match func([]byte) bool) bool {
	tokenMatches := func(tok []byte) bool {
		eq := bytes.IndexByte(tok, '=')
		return eq > 0 && match(bytes.TrimSpace(tok[:eq]))
	}
	start := 0
	inQuote := false
	for i, c := range line {
		if c == '"' {
			inQuote = !inQuote
			continue
		}
		if c > ' ' || inQuote {
			continue
		}
		if i > start && tokenMatches(line[start:i]) {
			return true
		}
		start = i + 1
	}
	// The last token ends with the line.
	return start < len(line) && tokenMatches(line[start:])
}

// labelSearchParam returns the browse search term (search, else q), or ""
// when the label endpoints ignore browse parameters.
func (p *Proxy) labelSearchParam(r *http.Request) string {
	if !p.labelBrowseExtensions() {
		return ""
	}
	if search := strings.TrimSpace(r.FormValue("search")); search != "" {
		return search
	}
	return strings.TrimSpace(r.FormValue("q"))
}

// labelLimitParam returns the client's limit on a label endpoint, or "" when
// the label endpoints ignore browse parameters.
func (p *Proxy) labelLimitParam(r *http.Request) string {
	if !p.labelBrowseExtensions() {
		return ""
	}
	return r.FormValue("limit")
}

// lineFieldExposure is what a log query's pipeline exposes, as Loki labels,
// of the fields VictoriaLogs unpacked from a JSON log line at ingest. Loki has
// no labels from the line until a stage adds them, and each stage adds only
// its own (pkg/logql/log: JSONParser, LogfmtParser, RegexpParser,
// PatternParser, UnpackParser, LabelsFormatter; LineFormatter adds none):
//   - | json and | logfmt without an extraction list: every key that parser
//     reads from the line (a logfmt parse of a JSON line reads none);
//   - | json a, b="x.y" and | logfmt a, b: only the named labels;
//   - | regexp and | pattern: their named captures;
//   - | label_format: its target labels;
//   - | unpack: the keys of a packed line (one holding an _entry key).
//
// Every exposed label is a parsed label; fields the line does not hold stay
// structured metadata. The Loki-compatible profile classifies every response
// row with it (profiles/parsed-fields-without-parser).
type lineFieldExposure struct {
	jsonAll   bool
	logfmtAll bool
	// logfmt is set by any | logfmt stage: VictoriaLogs then holds the keys of
	// a logfmt line as fields, which an extraction list does not expose.
	logfmt bool
	unpack bool
	names  map[string]bool
	// extractions are the entries of json and logfmt extraction lists. Loki
	// gives every one of them a label, empty when the line lacks the key.
	extractions []lineExtraction
	// regexps are the | regexp stages' expressions, which read a named group
	// of the line where VictoriaLogs leaves the stored field of that name.
	regexps []*regexp.Regexp
	// labelFormats are the label_format entries, each with what the stages
	// before it expose (fixLabelFormats).
	labelFormats []labelFormatTarget
	// filters are the pipeline's label filters on a single label, each with
	// what the stages before it expose (see dropsRow).
	filters []lineFieldFilter
}

// lineExtraction is one entry of a | json or | logfmt extraction list: the
// label and the key path it reads (Loki's jsonexpr path for | json, the key
// for | logfmt).
type lineExtraction struct {
	name   string
	path   []string
	logfmt bool
}

// labelFormatTarget is one label_format entry (newLabelFormatTarget).
type labelFormatTarget struct {
	name   string
	rename bool
	refs   []string
	parts  []string
	before lineFieldExposure
}

// lineFieldFilter is a label filter stage on one label.
type lineFieldFilter struct {
	name string
	// matchesEmpty is the filter's result on a label the entry does not have:
	// Loki evaluates a string matcher on "" and fails a numeric, duration or
	// bytes comparison (pkg/logql/log/label_filter.go).
	matchesEmpty bool
	before       lineFieldExposure
}

// lineFieldExposure returns the pipeline exposure of a log query in the
// Loki-compatible profile, or nil for another profile or a query the parser
// rejects (those keep every field, as before).
func (p *Proxy) lineFieldExposure(query string) *lineFieldExposure {
	if !p.lokiCompatibleProfile() {
		return nil
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return nil
	}
	exposure := pipelineAddsLabels(lq.Pipeline)
	return &exposure
}

// backendLogQuery returns the query sent to VictoriaLogs for a log query,
// and whether VictoriaLogs must also return the stored line. In the
// Loki-compatible profile a line_format stage whose output no later stage
// reads (no line filter, parser or decolorize follows it) is left out: the
// proxy renders line_format on every response that has one
// (applyLineFormatTemplate, from the labels the entry carries), and
// VictoriaLogs then returns the stored line, which tells the fields of a JSON
// line from structured metadata (lineFieldExposure). A line_format that stays
// (a later stage reads its output, or there are several) rewrites the line,
// so the query has VictoriaLogs copy the stored line aside first
// (translator.TranslateLogQueryKeepingLine). Another profile, or a query that
// is not a log query, is sent as is.
func (p *Proxy) backendLogQuery(query string) (string, bool) {
	if !strings.Contains(query, "line_format") || !p.lokiCompatibleProfile() {
		return query, false
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return query, false
	}
	formats := 0
	for _, stage := range lq.Pipeline {
		if _, ok := stage.(*logqlpkg.LineFormatStage); ok {
			formats++
		}
	}
	if formats == 0 {
		return query, false
	}
	if stripped, ok := stripUnreadLineFormat(query, lq.Pipeline); ok {
		return stripped, false
	}
	return query, true
}

// stripUnreadLineFormat returns query without its only line_format stage
// when no later stage reads the formatted line.
func stripUnreadLineFormat(query string, pipeline []logqlpkg.Stage) (string, bool) {
	seen := false
	kept := make([]string, 0, len(pipeline))
	for _, stage := range pipeline {
		if _, ok := stage.(*logqlpkg.LineFormatStage); !ok {
			kept = append(kept, stage.String())
		}
		switch s := stage.(type) {
		case *logqlpkg.LineFormatStage:
			if seen {
				return "", false
			}
			seen = true
		case *logqlpkg.LineFilterStage, *logqlpkg.ParserStage, *logqlpkg.DecolorizeStage:
			if seen {
				return "", false
			}
		case *logqlpkg.LabelFormatStage:
			if seen && strings.Contains(s.Raw, "__line__") {
				return "", false
			}
		}
	}
	loc := lineFormatTemplateRE.FindStringIndex(query)
	if !seen || loc == nil {
		return "", false
	}
	stripped := strings.TrimSpace(strings.TrimSpace(query[:loc[0]]) + " " + strings.TrimSpace(query[loc[1]:]))
	// The text removed must be the line_format stage and nothing else.
	check, err := logqlpkg.ParseLogQuery(stripped)
	if err != nil || len(check.Pipeline) != len(kept) {
		return "", false
	}
	for i, stage := range check.Pipeline {
		if stage.String() != kept[i] {
			return "", false
		}
	}
	return stripped, true
}

// lokiCompatibleProfile reports whether this proxy runs the Loki-compatible
// profile (see lokiProfile).
func (p *Proxy) lokiCompatibleProfile() bool {
	return p.labelTranslator != nil && lokiProfile(p.labelTranslator.style, p.metadataFieldMode)
}

// pipelineAddsLabels returns what the stages of a log pipeline expose (see
// lineFieldExposure).
func pipelineAddsLabels(pipeline []logqlpkg.Stage) lineFieldExposure {
	var exposure lineFieldExposure
	for i, stage := range pipeline {
		switch s := stage.(type) {
		case *logqlpkg.ParserStage:
			full := len(s.Fields) == 0
			for _, f := range s.Fields {
				x := lineExtraction{name: f.Name, path: []string{f.Expression}, logfmt: s.Type == logqlpkg.ParserLogfmt}
				if !x.logfmt {
					x.path = lokiJSONExpressionPath(f.Expression)
				}
				if x.path != nil {
					exposure.extractions = append(exposure.extractions, x)
				}
			}
			switch s.Type {
			case logqlpkg.ParserRegexp:
				if re, err := regexp.Compile(s.Param); err == nil {
					exposure.regexps = append(exposure.regexps, re)
				}
			case logqlpkg.ParserJSON:
				exposure.jsonAll = exposure.jsonAll || full
			case logqlpkg.ParserLogfmt:
				exposure.logfmtAll = exposure.logfmtAll || full
				exposure.logfmt = true
			case logqlpkg.ParserUnpack:
				exposure.unpack = true
			}
		case *logqlpkg.LabelFilterStage:
			exposure.filters = append(exposure.filters, labelFilterConditions(s.Raw, exposureBefore(pipeline[:i]))...)
		case *logqlpkg.LabelFormatStage:
			before := exposureBefore(pipeline[:i])
			for _, f := range s.Formats {
				exposure.labelFormats = append(exposure.labelFormats, newLabelFormatTarget(f, before))
			}
		}
	}
	exposure.names = pipelineLineFields(pipeline)
	return exposure
}

// exposureBefore is what the stages of a pipeline prefix expose, without
// the per-stage details only the whole pipeline uses.
func exposureBefore(prefix []logqlpkg.Stage) lineFieldExposure {
	before := pipelineAddsLabels(prefix)
	before.filters, before.labelFormats, before.extractions = nil, nil, nil
	return before
}

// labelFilterConditions returns the conditions of a label filter stage that
// names labels with plain matchers (a, b, ... are ANDed). A stage with "or",
// parentheses or another form returns none, so it is left to VictoriaLogs.
func labelFilterConditions(raw string, before lineFieldExposure) []lineFieldFilter {
	if m := numericLabelFilterRE.FindStringSubmatch(raw); m != nil {
		return []lineFieldFilter{{name: m[1], before: before}}
	}
	parsed, err := logqlpkg.ParseLogQuery("{" + raw + "}")
	if err != nil {
		return nil
	}
	conditions := make([]lineFieldFilter, 0, len(parsed.Selector.Matchers))
	for _, m := range parsed.Selector.Matchers {
		op := [...]string{"=", "!=", "=~", "!~"}[m.Op]
		condition, err := translator.NewDropCondition(m.Name, op, m.Value)
		if err != nil {
			return nil
		}
		conditions = append(conditions, lineFieldFilter{name: m.Name, matchesEmpty: condition.Matches(""), before: before})
	}
	return conditions
}

// numericLabelFilterRE matches a numeric, duration or bytes label filter
// (an unquoted value), e.g. status >= 500 or latency > 250ms.
var numericLabelFilterRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:==|!=|>=|<=|>|<)\s*-?[0-9][0-9A-Za-z.]*\s*$`)

// exposesJSONLine reports whether the stages expose every key of a JSON line
// with these top-level keys: a full | json, or | unpack on a packed line.
func (e *lineFieldExposure) exposesJSONLine(lineKeys map[string]bool) bool {
	if e.jsonAll {
		return true
	}
	_, packed := lineKeys["_entry"]
	return e.unpack && packed
}

// fieldCategory returns how a stored field of a row shows up in Loki: a
// parsed label, structured metadata, or nothing (hidden). lineKeys are the
// row's JSON line keys (jsonLineKeys), nil when the line is not a JSON
// object; line is the row's stored line. A key of a logfmt line is a field
// only after a | logfmt stage, and is parsed when that stage has no
// extraction list. A row whose line VictoriaLogs did not keep (missing _msg,
// the line rebuilt from its fields) cannot tell line keys from structured
// metadata and keeps the earlier rule: parsed after a | json or | logfmt,
// otherwise structured metadata.
func (e *lineFieldExposure) fieldCategory(key string, lineKeys map[string]bool, line []byte, lineMissing, classifyAsParsed bool) (parsed, hidden bool) {
	switch {
	case e.names[key]:
		return true, false
	case lineKeys != nil:
		if !isJSONLineField(key, lineKeys) {
			return false, false
		}
		if e.exposesJSONLine(lineKeys) {
			return true, false
		}
		return false, true
	case lineMissing:
		return classifyAsParsed, false
	case e.logfmt && logfmtLineHasKey(line, key):
		return e.logfmtAll, !e.logfmtAll
	}
	return false, false
}

// extractedSuffix is what Loki appends to the name of a parsed label or
// structured metadata key that shares its name with a stream label
// (pkg/logql/log/parser.go duplicateSuffix): the stream label keeps its value
// and the entry label is exposed as name_extracted.
const extractedSuffix = "_extracted"

// mayReadKey reports whether a stage may read key from the row's line: a
// full parser reads every key of its format, the other stages name theirs.
func (e *lineFieldExposure) mayReadKey(key string, lineKeys map[string]bool, line []byte) bool {
	switch {
	case e == nil:
		return false
	case e.names[key]:
		return true
	case lineKeys != nil:
		return e.exposesJSONLine(lineKeys) && isJSONLineField(key, lineKeys)
	}
	return e.logfmtAll && len(line) > 0 && logfmtLineHasKey(line, key)
}

// skipsStreamKeyField reports whether a stored field named like a stream label
// is no entry label: no stage reads the key from the line, and the field only
// repeats the stream label's value (stream) or is content of the JSON line no
// stage exposes (classifyRowField hides it).
func (e *lineFieldExposure) skipsStreamKeyField(key string, value []byte, stream string, lineKeys map[string]bool, line []byte) bool {
	return !e.mayReadKey(key, lineKeys, line) && (string(value) == stream || (lineKeys != nil && isJSONLineField(key, lineKeys)))
}

// readsLineKey reports whether a full parser stage reads key from the row's
// line (a JSON key, or a logfmt key after a | logfmt stage).
func (e *lineFieldExposure) readsLineKey(r *lineRow, key string) bool {
	switch {
	case r.keys != nil && e.exposesJSONLine(r.keys) && isJSONLineField(key, r.keys):
		return true
	case r.keys == nil && e.logfmtAll && !e.jsonAll && len(r.line) > 0 && logfmtLineHasKey(r.line, key):
		// Beside a | json stage VictoriaLogs does not unpack the logfmt of a
		// line, so the stored field is the stream label again.
		return true
	}
	for _, x := range e.extractions {
		if x.name == key && extractionReadsLine(x, r.line) {
			return true
		}
	}
	for _, re := range e.regexps {
		if i := re.SubexpIndex(key); i > 0 {
			if m := re.FindSubmatch(r.line); m != nil && len(m[i]) > 0 {
				return true
			}
		}
	}
	return false
}

// extractionReadsLine reports whether the line holds the key an extraction
// list entry reads.
func extractionReadsLine(x lineExtraction, line []byte) bool {
	if x.logfmt {
		return logfmtLineHasKey(line, x.path[0])
	}
	_, _, _, err := jsonparser.Get(line, x.path...)
	return err == nil
}

// repeatsLabel reports whether a stored field only repeats a label of the
// entry: a stream label's value, which no stage reads from the line, or any
// label outside the Loki-compatible profile (entryLabelName).
func (e *lineFieldExposure) repeatsLabel(key, value string, labels, streamOnly map[string]string, r *lineRow) bool {
	if _, exists := labels[key]; !exists {
		return false
	}
	stream, onStream := streamOnly[key]
	return e == nil || (key == "level" && !onStream) || (onStream && value == stream && !e.mayReadKey(key, r.keys, r.line))
}

// extractedName returns the name_extracted label of a colliding field, unless
// a stage of the query sets that label itself (a label_format target, a
// capture, an extraction-list name): the label the query sets wins, in either
// order, as the first parser does not overwrite a key an earlier stage
// extracted.
func (e *lineFieldExposure) extractedName(name string) (string, bool) {
	renamed := name + extractedSuffix
	return renamed, !e.names[renamed]
}

// entryLabelName returns the label a stored field is exposed as, and whether
// it is exposed at all. A field named like a stream label is not an entry
// label of its own when it only repeats the stream label (VictoriaLogs stores
// stream labels as fields); when it holds a parsed value or structured
// metadata of the entry, Loki keeps the stream label and names the entry
// label name_extracted. A parsed key is told from the repeated stream label by
// a full parser reading it from the line; another stage's capture (a regexp
// or pattern name) and structured metadata by a value that differs from the
// stream label's. A row stored without its line cannot tell structured
// metadata from line content (profiles/json-line-stored-without-line), so its
// differing fields are left out as before. streamOnly holds the labels of the
// stored _stream; final the stream labels the response carries (derived
// service_name included), which a field of another name may collide with
// (OTel service.name against service_name).
func (e *lineFieldExposure) entryLabelName(r *lineRow, key, value string, ex metadataFieldExposure, parsed, lineMissing bool, streamOnly, final map[string]string) (string, bool) {
	stream, onStream := streamOnly[key]
	if e == nil || (!onStream && (key == "level" || ex.name == "level")) || ex.name == detectedLevelLabel {
		_, exists := final[ex.name]
		return ex.name, !exists || ex.isAlias
	}
	if onStream {
		collides := !lineMissing && value != stream
		if parsed {
			collides = value != stream || (!lineMissing && e.readsLineKey(r, key))
		}
		if !collides {
			return "", false
		}
		return e.extractedName(ex.name)
	}
	if _, exists := final[ex.name]; exists {
		return e.extractedName(ex.name)
	}
	return ex.name, true
}

// classifyLineField returns whether a stored field of a row is a parsed
// label and whether it is left out: by the exposure in the Loki-compatible
// profile (fieldCategory), otherwise parsed when it holds content of a JSON
// line, or after a | json / | logfmt stage beside any other line.
func classifyLineField(e *lineFieldExposure, key string, lineKeys map[string]bool, line []byte, lineMissing, classifyAsParsed bool) (parsed, hidden bool) {
	if e != nil {
		return e.fieldCategory(key, lineKeys, line, lineMissing, classifyAsParsed)
	}
	if lineKeys != nil {
		return isJSONLineField(key, lineKeys), false
	}
	return classifyAsParsed, false
}

// lineRow is one response row as the exposure reads it: its stored line
// (the line before any line_format), the line's JSON keys (jsonLineKeys, nil
// when the line is not a JSON object) and its stream labels.
type lineRow struct {
	line   []byte
	keys   map[string]bool
	stream map[string]string
	labels map[string]bool // the JSON line's labels, walked once when a name needs it
}

// hiddenBefore reports whether name is a key of the row's line (a JSON key,
// or a logfmt key once a | logfmt stage made VictoriaLogs hold the line's
// keys as fields) that the stages before a point, described by before, do
// not expose: Loki's entry has no such label there.
func (r *lineRow) hiddenBefore(e, before *lineFieldExposure, name string) bool {
	jsonLine := r.keys != nil
	if before.names[name] {
		return false
	}
	if jsonLine && before.exposesJSONLine(r.keys) {
		return false
	}
	if !jsonLine && (!e.logfmt || before.logfmtAll) {
		return false
	}
	if _, stream := r.stream[name]; stream {
		return false
	}
	switch {
	case !jsonLine:
		return logfmtLineHasLabel(r.line, name)
	case r.keys[name]:
		// An object key names no label itself, only its nested keys.
		return false
	}
	if _, top := r.keys[name]; top {
		return true
	}
	if !jsonLineMayHold(r.keys, name) {
		return false
	}
	if r.labels == nil {
		r.labels = jsonLineLabels(r.line)
	}
	return r.labels[name]
}

// classifyRowField is classifyLineField for a stored field with its value:
// an array of the JSON line is left out where | json would expose it
// (hidesJSONArray).
func classifyRowField(e *lineFieldExposure, r *lineRow, key, value string, lineMissing, classifyAsParsed bool) (parsed, hidden bool) {
	parsed, hidden = classifyLineField(e, key, r.keys, r.line, lineMissing, classifyAsParsed)
	if e != nil && parsed && !hidden && !lineMissing && r.hidesJSONArray(key, value) {
		return false, true
	}
	return parsed, hidden
}

// finishRow gives a classified row the extraction-list values and the
// label_format corrections Loki's stages produce (fillExtractions,
// fixLabelFormats). A row stored without its line is left as it is.
func (e *lineFieldExposure) finishRow(r *lineRow, lineMissing bool, parsed, metadata map[string]string) {
	if e == nil || lineMissing {
		return
	}
	if len(e.extractions) > 0 {
		e.fillExtractions(r.line, parsed, metadata, r.stream)
	}
	if len(e.labelFormats) > 0 {
		e.fixLabelFormats(r, parsed, metadata)
	}
}

// dropsRow reports whether a label filter drops a row Loki would not see it
// match: the filter names a key of the row's line that no stage before the
// filter exposes (hiddenBefore), so Loki evaluates it on a label the entry
// does not have. VictoriaLogs matched the stored field; the row is kept when
// the filter matches an empty value. A row stored without its line is never
// dropped (fieldCategory).
func (e *lineFieldExposure) dropsRow(r *lineRow) bool {
	for i := range e.filters {
		f := &e.filters[i]
		if !f.matchesEmpty && r.hiddenBefore(e, &f.before, f.name) {
			return true
		}
	}
	return false
}

// fixLabelFormats corrects label_format targets that read a key of the line
// no stage before the label_format exposes, which VictoriaLogs read from its
// stored field: Loki renames nothing from a label the entry does not have
// (the target is left out) and renders a template with an empty value for
// it. Only templates made of {{.label}} placeholders are rendered here (the
// only form the LogsQL translation renders as well); values come from the
// row's labels.
func (e *lineFieldExposure) fixLabelFormats(r *lineRow, parsed, metadata map[string]string) {
	for i := range e.labelFormats {
		lf := &e.labelFormats[i]
		hidden := false
		for _, ref := range lf.refs {
			if r.hiddenBefore(e, &lf.before, ref) {
				hidden = true
				break
			}
		}
		if !hidden {
			continue
		}
		if lf.rename {
			delete(parsed, lf.name)
			continue
		}
		if lf.parts == nil {
			continue
		}
		var b strings.Builder
		for k, part := range lf.parts {
			if k%2 == 0 {
				b.WriteString(part)
				continue
			}
			if r.hiddenBefore(e, &lf.before, part) {
				continue
			}
			if v, ok := parsed[part]; ok {
				b.WriteString(v)
			} else if v, ok := metadata[part]; ok {
				b.WriteString(v)
			} else {
				b.WriteString(r.stream[part])
			}
		}
		parsed[lf.name] = b.String()
	}
}

// hidesJSONArray reports whether a stored field holds an array of the JSON
// line: Loki's json parser gives an array no label (visitJSONLineLeaves), so
// | json does not expose it.
func (r *lineRow) hidesJSONArray(key, value string) bool {
	if r.keys == nil || !strings.HasPrefix(value, "[") {
		return false
	}
	_, typ, _, err := jsonparser.Get(r.line, strings.Split(key, ".")...)
	return err == nil && typ == jsonparser.Array
}

// labelFormatTemplateRE matches a {{.label}} placeholder of a label_format
// template.
var labelFormatTemplateRE = regexp.MustCompile(`\{\{\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// newLabelFormatTarget describes one label_format entry: the labels it reads
// and, for a template of {{.label}} placeholders only, its parts (literal
// text at even indexes, label names at odd ones).
func newLabelFormatTarget(f logqlpkg.LabelFormat, before lineFieldExposure) labelFormatTarget {
	t := labelFormatTarget{name: f.Name, rename: f.Rename, before: before}
	if f.Rename {
		t.refs = []string{f.Value}
		return t
	}
	matches := labelFormatTemplateRE.FindAllStringSubmatchIndex(f.Value, -1)
	last := 0
	parts := make([]string, 0, 2*len(matches)+1)
	var literal strings.Builder
	for _, m := range matches {
		parts = append(parts, f.Value[last:m[0]], f.Value[m[2]:m[3]])
		literal.WriteString(f.Value[last:m[0]])
		t.refs = append(t.refs, f.Value[m[2]:m[3]])
		last = m[1]
	}
	parts = append(parts, f.Value[last:])
	literal.WriteString(f.Value[last:])
	if !strings.Contains(literal.String(), "{{") {
		t.parts = parts
	}
	return t
}

// fillExtractions gives every extraction-list label the row lacks Loki's
// value read from the line: the value at the key path (a nested object as
// its JSON text), or empty when the line lacks the key
// (JSONExpressionParser, LogfmtExpressionParser). VictoriaLogs leaves no
// field for a missing key, a path into an array or a nested object, and does
// not apply logfmt renames. | json gives no label for a line that does not
// start like JSON (Loki reports a parse error there). A label a stream label
// or structured metadata already holds is left as it is.
func (e *lineFieldExposure) fillExtractions(line []byte, parsed, metadata, streamLabels map[string]string) {
	for _, x := range e.extractions {
		if _, ok := parsed[x.name]; ok {
			continue
		}
		if _, ok := metadata[x.name]; ok {
			continue
		}
		if _, ok := streamLabels[x.name]; ok {
			continue
		}
		if x.logfmt {
			parsed[x.name] = logfmtLineValue(line, x.path[0])
			continue
		}
		if len(line) == 0 || (line[0] != '{' && line[0] != '[' && line[0] != '"') {
			continue
		}
		parsed[x.name] = jsonLineValue(line, x.path)
	}
}

// jsonLineValue returns the value Loki's json expression parser reads at a
// key path: a string unescaped, null as empty, any other value (an object
// included) as its JSON text, or empty when the path is missing.
func jsonLineValue(line []byte, path []string) string {
	value, typ, _, err := jsonparser.Get(line, path...)
	if err != nil {
		return ""
	}
	switch typ {
	case jsonparser.String:
		if s, err := jsonparser.ParseString(value); err == nil {
			return s
		}
	case jsonparser.Null:
		return ""
	}
	return string(value)
}

// lokiJSONExpressionPath turns a Loki json expression (`a.b`, `a[0]`,
// `["a b"].c`) into a key path, array indexes as "[n]"; nil when it cannot
// be read.
func lokiJSONExpressionPath(expr string) []string {
	var path []string
	for i := 0; i < len(expr); {
		switch c := expr[i]; c {
		case '.':
			i++
		case '[':
			end := strings.IndexByte(expr[i:], ']')
			if end < 0 {
				return nil
			}
			inner := strings.TrimSpace(expr[i+1 : i+end])
			if key, err := strconv.Unquote(inner); err == nil {
				path = append(path, key)
			} else if _, err := strconv.Atoi(inner); err == nil {
				path = append(path, "["+inner+"]")
			} else {
				return nil
			}
			i += end + 1
		default:
			j := i
			for j < len(expr) && expr[j] != '.' && expr[j] != '[' {
				j++
			}
			path = append(path, strings.TrimSpace(expr[i:j]))
			i = j
		}
	}
	return path
}

// logfmtLineValue returns the value of key in a logfmt line, its quotes
// removed as parseLogfmtFields does, or empty when the line lacks it.
func logfmtLineValue(line []byte, key string) string {
	return parseLogfmtFields(string(line))[key]
}

// filtersLineFields reports whether a label filter may drop a row (dropsRow).
func (e *lineFieldExposure) filtersLineFields() bool {
	if e == nil {
		return false
	}
	for _, f := range e.filters {
		if !f.matchesEmpty {
			return true
		}
	}
	return false
}

// needsClassification reports whether rows must be classified even when the
// response carries no categorized metadata: stage labels join the stream
// labels, or a label filter may drop rows.
func (e *lineFieldExposure) needsClassification() bool {
	return e != nil && (len(e.names) > 0 || e.filtersLineFields())
}

// streamLabelMerge returns which extracted labels join a log entry's stream
// labels in the Loki-compatible profile: every parsed label, stage labels
// included, exactly when parsed | json labels do (mergesParsedStreamLabels),
// so a categorize-labels response keys streams by the stream labels alone;
// and, without categorize-labels, structured metadata too (mergeMetadata), as
// Loki's legacy encoding merges every label of an entry into its stream.
// Another profile keeps mergeParsed and the regexp captures it merges.
func (e *lineFieldExposure) streamLabelMerge(mergeParsed bool, captureFields map[string]bool, categorizedLabels, emitStructuredMetadata bool) (merge bool, captures map[string]bool, mergeMetadata bool) {
	if e == nil {
		return mergeParsed, captureFields, false
	}
	stageLabels := e.unpack || len(e.names) > 0
	if !categorizedLabels {
		return true, nil, true
	}
	return mergeParsed || (stageLabels && !emitStructuredMetadata), nil, false
}

// mergeMetadataIntoParsed adds structured metadata to the parsed labels that
// join the stream labels (streamLabelMerge), returning the parsed map; buf
// is the parsed-label buffer used when there were none.
func mergeMetadataIntoParsed(metadata, parsed, buf map[string]string) map[string]string {
	if len(metadata) == 0 {
		return parsed
	}
	if parsed == nil {
		parsed = buf
	}
	for k, v := range metadata {
		if _, ok := parsed[k]; !ok {
			parsed[k] = v
		}
	}
	return parsed
}

// jsonLineKeys returns the top-level keys of a JSON-object log line, each
// true when its value is an object, or nil when the line is not one.
func jsonLineKeys(line []byte) map[string]bool {
	if len(line) < 2 || line[0] != '{' {
		return nil
	}
	var parser fj.Parser
	value, err := parser.ParseBytes(line)
	if err != nil {
		return nil
	}
	obj, err := value.Object()
	if err != nil {
		return nil
	}
	keys := make(map[string]bool, obj.Len())
	obj.Visit(func(key []byte, v *fj.Value) {
		keys[string(key)] = v.Type() == fj.TypeObject
	})
	return keys
}

// isJSONLineField reports whether a stored VictoriaLogs field holds content
// of the JSON log line: one of its top-level keys, or a dotted name under a
// top-level object (VictoriaLogs flattens nested objects into dotted names).
func isJSONLineField(field string, lineKeys map[string]bool) bool {
	if _, ok := lineKeys[field]; ok {
		return true
	}
	if i := strings.IndexByte(field, '.'); i > 0 {
		return lineKeys[field[:i]]
	}
	return false
}
