package lower

import (
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The typed builders of a filter on detected_level: the LogsQL nodes whose text is the string translator's
// (translate.BuildDetectedLevelFilter and translate.JoinDetectedLevelFilters over every level field), read from the
// same tables (translate.DetectedLevelSpellings and the others), so the plan holds no query text for them.
// TestDetectedLevelNodesPrintTheBuildersText holds each node to the builders' text. The chain that derives the value
// depends on its mode alone and is built once per mode.

// levelFilterNodes is the typed form of translate.DetectedLevelFilterPlan.
type levelFilterNodes struct {
	// Never is set when the matcher accepts no row.
	Never bool
	// Prefilter is the filter in front of the pipes (nil for none); PrefilterPipe the metric form's, a filter pipe
	// of its own right before the chain.
	Prefilter, PrefilterPipe logsql.FilterExpr
	// Match is the filter on the derived value.
	Match logsql.FilterExpr
	// Pipes are the pipes of the filter alone: the chain and the match (none when every row passes).
	Pipes []logsql.Pipe
}

// The scratch and result fields of the chain.
const (
	dlField      = "__dl"
	dlStored     = "__dl_s"
	dlHas        = "__dl_h"
	dlValue      = "__dl_v"
	dlKeyword    = "__dl_e"
	jsonPrefix   = "__j_"
	logfmtPrefix = "__l_"
	sevNumber    = "severity_number"
	sevText      = "severity_text"
	sevName      = "severity"
	unspecified  = "Unspecified"
)

// levelScratch are the scratch fields the chain deletes at its end; levelScratchAll the log form's, with __dl.
var (
	levelScratch    = []string{"__dl_*", "__j_*", "__l_*"}
	levelScratchAll = []string{"__dl", "__dl_*", "__j_*", "__l_*"}
)

func lvAny(f string) logsql.FilterExpr { return logsql.FieldFilter{Field: f, Op: logsql.FieldOpAny} }
func lvEmpty(f string) logsql.FilterExpr {
	return logsql.FieldFilter{Field: f, Op: logsql.FieldOpEmpty}
}
func lvExact(f, v string) logsql.FilterExpr {
	return logsql.FieldFilter{Field: f, Op: logsql.FieldOpExact, Value: v}
}

// lvRe is a regexp filter written as the builders write it (quoteLevelRegexp: in backquotes when it can).
func lvRe(f, re string) logsql.FilterExpr {
	return logsql.FieldFilter{Field: f, Op: logsql.FieldOpRegexp, Value: re, Backquoted: true}
}

// lvGoRe is a regexp filter written Go-quoted.
func lvGoRe(f, re string) logsql.FilterExpr {
	return logsql.FieldFilter{Field: f, Op: logsql.FieldOpRegexp, Value: re}
}

func lvBang(f logsql.FilterExpr) logsql.FilterExpr { return logsql.NotExpr{Expr: f, Bang: true} }
func lvExcl(f logsql.FilterExpr) logsql.FilterExpr { return logsql.Exclude{Expr: f} }
func lvGroup(f logsql.FilterExpr) logsql.FilterExpr {
	return logsql.Group{Expr: f}
}

// lvWords is `field:(i(w1) OR i(w2) ...)`: any of the words, as a word, in any case (wordAlternatives).
func lvWords(field string, words []string) logsql.FilterExpr {
	alts := make(logsql.AnyOf, len(words))
	for i, w := range words {
		alts[i] = logsql.AnyCasePhrase{Value: w, Word: true}
	}
	return logsql.FieldScope{Field: field, Expr: alts}
}

// lvOr is joinOr: the filters with OR, in parentheses when there are several, nil for none.
func lvOr(fs ...logsql.FilterExpr) logsql.FilterExpr {
	alts := make(logsql.AnyOf, 0, len(fs))
	for _, f := range fs {
		if f != nil {
			alts = append(alts, f)
		}
	}
	switch len(alts) {
	case 0:
		return nil
	case 1:
		return alts[0]
	}
	return lvGroup(alts)
}

// lvFormat is `format [if (cond)] "template" as field [keep_original_fields]`.
func lvFormat(cond logsql.FilterExpr, template, field string, keep bool) logsql.Pipe {
	return logsql.PipeFormat{If: cond, Template: template, ResultField: field, KeepOriginalFields: keep}
}

// levelSpellingRegexp is `(?i)^(s1|s2...)$` over the spellings.
func levelSpellingRegexp(spellings []string) string {
	return "(?i)^(" + strings.Join(spellings, "|") + ")$"
}

// allLevelSpellings are the spellings of every level, in the tables' order.
var allLevelSpellings = sync.OnceValue(func() []string {
	var out []string
	for _, s := range translate.DetectedLevelSpellings() {
		out = append(out, s.Spellings...)
	}
	return out
})

// knownLevelSpellingRegexp is levelSpellingRegexp over every spelling.
var knownLevelSpellingRegexp = sync.OnceValue(func() string { return levelSpellingRegexp(allLevelSpellings()) })

func isKnownLevelSpelling(v string) bool {
	for _, s := range allLevelSpellings() {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// levelTextFields are the level fields that hold a text: all but severity_number.
var levelTextFields = sync.OnceValue(func() []string {
	fields := translate.DetectedLevelFieldNames
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != sevNumber {
			out = append(out, f)
		}
	}
	return out
})

// synthesizedSeverityText is the builders' synthesizedSeverityText: a severity_text VictoriaLogs derived from a
// non-zero severity_number.
var synthesizedSeverityText = sync.OnceValue(func() logsql.FilterExpr {
	families := translate.SeverityFamilies()
	alts := make(logsql.AnyOf, 0, len(families)+1)
	for _, f := range families {
		nums := make([]string, 0, f.Hi-f.Lo+1)
		for n := f.Lo; n <= f.Hi; n++ {
			nums = append(nums, strconv.Itoa(n))
		}
		alts = append(alts, lvGroup(logsql.AllOf{lvRe(sevText, "^"+f.Stem+"[234]?$"), lvRe(sevNumber, "^("+strings.Join(nums, "|")+")$")}))
	}
	alts = append(alts, lvGroup(logsql.AllOf{lvExact(sevText, unspecified), lvRe(sevNumber, `^(-[1-9][0-9]*|2[5-9]|[3-9][0-9]|[1-9][0-9]{2,})$`)}))
	return lvGroup(alts)
})

// severityStems are the stems of the families of the levels given, all of them when none is.
func severityStems(levels ...string) []string {
	var stems []string
	for _, f := range translate.SeverityFamilies() {
		if len(levels) == 0 || slices.Contains(levels, strings.ToLower(f.Stem)) {
			stems = append(stems, f.Stem)
		}
	}
	return stems
}

// synthesizedSeverityName is the builders' synthesizedSeverityName: a severity name VictoriaLogs before v1.50
// derived from the OTLP number.
var synthesizedSeverityName = sync.OnceValue(func() logsql.FilterExpr {
	return lvGroup(logsql.AllOf{lvRe(sevName, "^("+unspecified+"|("+strings.Join(severityStems(), "|")+")[234]?)$"), lvExcl(lvAny(sevNumber))})
})

// severityNameClause is the builders' severityNameClause, nil when no target has a derived name.
func severityNameClause(targets []string) logsql.FilterExpr {
	stems := severityStems(targets...)
	if len(stems) == 0 || len(targets) == 0 {
		return nil
	}
	return lvGroup(logsql.AllOf{lvExcl(lvAny(sevNumber)), lvRe(sevName, "^("+strings.Join(stems, "|")+")[234]?$")})
}

// hasLevelFilter is the builders' BuildHasLevelFilter over every level field: a row whose level a stored field
// decides.
var hasLevelFilter = sync.OnceValue(func() logsql.FilterExpr {
	stored := translate.DetectedLevelFieldNames
	alts := make(logsql.AnyOf, len(stored))
	for i, f := range stored {
		alts[i] = lvAny(translate.LogsQLFieldName(f))
	}
	has := logsql.AllOf{lvGroup(alts)}
	// The rows whose only level text is the Unspecified VictoriaLogs synthesises (noLevelRows).
	text := emptyExcept(stored, sevText, sevNumber)
	text = append(text, lvExact(sevText, unspecified), lvExact(sevNumber, "0"))
	name := emptyExcept(stored, sevName, sevNumber)
	name = append(name, lvExact(sevName, unspecified), lvExcl(lvAny(sevNumber)))
	return append(has, lvBang(lvGroup(text)), lvBang(lvGroup(name)))
})

// emptyExcept are `f:""` filters for the stored fields other than skip.
func emptyExcept(stored []string, skip ...string) logsql.AllOf {
	out := make(logsql.AllOf, 0, len(stored)+2)
	for _, f := range stored {
		if !slices.Contains(skip, f) {
			out = append(out, lvEmpty(translate.LogsQLFieldName(f)))
		}
	}
	return out
}

// levelChainPipes are the pipes of translate.BuildDetectedLevelChain over every level field with no presence and no
// shared unpack, for the Group (log filters) and Grouping (metric rows) modes.
func levelChainPipes(mode translate.DetectedLevelChainMode) []logsql.Pipe {
	texts := levelTextFields()
	bodyKeys := translate.DetectedLevelBodyKeys()
	hasLevel := lvExcl(lvAny(dlHas))
	quoted := make([]string, len(bodyKeys))
	for i, n := range bodyKeys {
		quoted[i] = translate.LogsQLFieldName(n)
	}
	pipes := make([]logsql.Pipe, 0, 64)
	// The stored level fields in Loki's order, then the rows a stored level decides (storedLevelPicks).
	for _, field := range texts {
		name := translate.LogsQLFieldName(field)
		var cond logsql.FilterExpr = lvAny(name)
		switch field {
		case sevText:
			cond = logsql.AllOf{cond, lvBang(synthesizedSeverityText()), lvBang(lvGroup(logsql.AllOf{lvExact(sevText, unspecified), lvExact(sevNumber, "0")}))}
		case sevName:
			cond = logsql.AllOf{cond, lvBang(synthesizedSeverityName())}
		}
		pipes = append(pipes, lvFormat(cond, "<"+field+">", dlStored, true))
	}
	pipes = append(pipes,
		lvFormat(lvAny(dlStored), "1", dlHas, false),
		lvFormat(logsql.AllOf{hasLevel, lvAny(sevNumber), lvBang(lvGroup(logsql.AllOf{lvExact(sevNumber, "0"), lvExact(sevText, unspecified)}))}, "1", dlHas, false),
		lvFormat(logsql.AllOf{hasLevel, synthesizedSeverityName(), lvExcl(lvExact(sevName, unspecified))}, "1", dlHas, false),
		lvFormat(lvAny(dlStored), "<"+dlStored+">", dlValue, true),
		logsql.PipeUnpackJSON{If: hasLevel, From: "_msg", Fields: quoted, ResultPrefix: jsonPrefix},
		logsql.PipeUnpackLogfmt{If: logsql.AllOf{hasLevel, lvExcl(lvRe("_msg", translate.BracedLineRegexp))}, From: "_msg", Fields: quoted, ResultPrefix: logfmtPrefix},
	)
	for _, prefix := range [...]string{jsonPrefix, logfmtPrefix} {
		for _, n := range bodyKeys {
			pipes = append(pipes, lvFormat(lvAny(translate.LogsQLFieldName(prefix+n)), "<"+prefix+n+">", dlValue, true))
		}
	}
	for _, s := range translate.DetectedLevelSpellings() {
		pipes = append(pipes, lvFormat(lvRe(dlValue, levelSpellingRegexp(s.Spellings)), s.Level, dlField, true))
	}
	pipes = append(pipes,
		logsql.PipeExtractRegexp{If: logsql.AllOf{hasLevel, lvEmpty(dlField), lvWords("_msg", translate.LevelKeywords())}, Pattern: translate.LevelKeywordRegexp(), From: "_msg"},
		lvFormat(logsql.AllOf{lvEmpty(dlField), lvGoRe(dlKeyword, "(?i)^(error|err)$")}, "error", dlField, true),
		lvFormat(logsql.AllOf{lvEmpty(dlField), lvGoRe(dlKeyword, "(?i)^(warning|warn)$")}, "warn", dlField, true),
		lvFormat(logsql.AllOf{lvEmpty(dlField), lvAny(dlKeyword)}, "<lc:"+dlKeyword+">", dlField, true),
		lvFormat(logsql.AllOf{lvEmpty(dlField), lvAny(dlStored)}, "<"+dlStored+">", dlField, true),
	)
	// severity_number decides only when no level field did (severityNumberPipes).
	for _, level := range translate.DetectedLevelCanonical() {
		re, ok := translate.SeverityNumberRegexp(level)
		if !ok {
			continue
		}
		match := lvRe(sevNumber, re)
		if names := severityNameClause([]string{level}); names != nil {
			match = lvGroup(logsql.AnyOf{match, names})
		}
		pipes = append(pipes, lvFormat(logsql.AllOf{lvEmpty(dlField), lvExcl(lvAny(dlStored)), match}, level, dlField, true))
	}
	pipes = append(pipes,
		lvFormat(logsql.AllOf{lvEmpty(dlField), lvExcl(lvAny(dlStored)), lvAny(sevNumber)}, "info", dlField, true),
		lvFormat(nil, "unknown", dlField, true),
	)
	if mode == translate.DetectedLevelChainGrouping {
		return append(pipes, lvFormat(nil, "<"+dlField+">", translate.DetectedLevelLabel, false), logsql.PipeDelete{Labels: levelScratchAll})
	}
	return append(pipes, logsql.PipeDelete{Labels: levelScratch})
}

// The chains of the two modes the plan writes: Group without its closing delete (the log form filters between it
// and the delete of every scratch field), and Grouping.
var (
	levelChainLogs = sync.OnceValue(func() []logsql.Pipe {
		pipes := levelChainPipes(translate.DetectedLevelChainGroup)
		return pipes[: len(pipes)-1 : len(pipes)-1]
	})
	levelChainMetric = sync.OnceValue(func() []logsql.Pipe { return levelChainPipes(translate.DetectedLevelChainGrouping) })
)

// storedTextClause is the builders' storedTextClause: a text level field whose normalised value the positive matcher
// accepts. nil when none can.
func storedTextClause(m translate.DetectedLevelMatch, field string) logsql.FilterExpr {
	f := translate.LogsQLFieldName(field)
	var alts logsql.AnyOf
	var spellings []string
	for _, t := range m.Targets {
		for _, s := range translate.DetectedLevelSpellings() {
			if s.Level == t {
				spellings = append(spellings, s.Spellings...)
			}
		}
	}
	if len(spellings) > 0 {
		alts = append(alts, lvRe(f, levelSpellingRegexp(spellings)))
	}
	switch {
	case m.Regexp:
		alts = append(alts, lvGroup(logsql.AllOf{lvRe(f, translate.AnchorLabelFilterPattern(translate.LokiLabelFilterPattern(m.Value))), lvBang(lvRe(f, knownLevelSpellingRegexp()))}))
	case m.Value != "" && !isKnownLevelSpelling(m.Value):
		alts = append(alts, lvExact(f, m.Value))
	}
	return lvOr(alts...)
}

// severityNumberClause is the builders' severityNumberClause, nil for none.
func severityNumberClause(targets []string) logsql.FilterExpr {
	var alts logsql.AnyOf
	for _, t := range targets {
		if re, ok := translate.SeverityNumberRegexp(t); ok {
			alts = append(alts, lvRe(sevNumber, re))
		}
		if t == "info" {
			alts = append(alts, lvGroup(logsql.AllOf{lvAny(sevNumber), lvBang(lvRe(sevNumber, "^-?[0-9]+$"))}))
		}
	}
	return lvOr(alts...)
}

// storedFilter is the builders' storedFilter over every level field: the rows whose stored fields decide a value
// the positive matcher accepts, nil when none can.
func storedFilter(m translate.DetectedLevelMatch) logsql.FilterExpr {
	stored := translate.DetectedLevelFieldNames
	var clauses logsql.AnyOf
	for i, f := range stored {
		var match logsql.FilterExpr
		otelNumber := false
		switch f {
		case sevNumber:
			match = severityNumberClause(m.Targets)
			otelNumber = match != nil
		default:
			match = storedTextClause(m, f)
			if match != nil && f == sevText {
				match = logsql.AllOf{match, lvBang(synthesizedSeverityText()), lvBang(lvGroup(logsql.AllOf{lvExact(sevText, unspecified), lvExact(sevNumber, "0")}))}
			}
			if f == sevName {
				if match != nil {
					match = lvGroup(logsql.AllOf{match, lvBang(synthesizedSeverityName())})
				}
				match = lvOr(match, severityNameClause(m.Targets))
			}
		}
		if match == nil {
			continue
		}
		// The stored fields ranked before f are empty.
		prefix := make(logsql.AllOf, 0, i+1)
		for _, g := range stored[:i] {
			prefix = append(prefix, lvEmpty(translate.LogsQLFieldName(g)))
		}
		if otelNumber {
			// A synthesised non-zero severity_text defers to the number.
			texts := make(logsql.AllOf, 0, len(stored))
			for _, g := range stored {
				if g == sevText {
					break
				}
				texts = append(texts, lvEmpty(translate.LogsQLFieldName(g)))
			}
			texts = append(texts, synthesizedSeverityText())
			prefix = append(make(logsql.AllOf, 0, 2), lvGroup(logsql.AnyOf{prefix, lvGroup(texts)}))
		}
		clauses = append(clauses, append(prefix, match))
	}
	return lvOr(clauses...)
}

// storedEmpty reports whether storedFilter is nil without building it: no target, and a value that is a known
// spelling or empty (no raw word and no regexp).
func storedEmpty(m translate.DetectedLevelMatch) bool {
	return len(m.Targets) == 0 && !m.Regexp && (m.Value == "" || isKnownLevelSpelling(m.Value))
}

// levelPrefilter is the builders' prefilter over every level field: a filter every row the positive matcher accepts
// passes, nil when it would not narrow the rows.
func levelPrefilterNode(m translate.DetectedLevelMatch) logsql.FilterExpr {
	unknown := slices.Contains(m.Targets, "unknown")
	var words []string
	for _, s := range translate.DetectedLevelSpellings() {
		if slices.Contains(m.Targets, s.Level) {
			words = append(words, s.Spellings...)
		}
	}
	storedWords := words
	if unknown {
		storedWords = append(slices.Clone(words), "unknown")
	}
	raws, anyRaw := translate.DetectedLevelRawValues(m)
	var alts logsql.AnyOf
	for _, f := range levelTextFields() {
		name := translate.LogsQLFieldName(f)
		if anyRaw {
			alts = append(alts, lvAny(name))
			continue
		}
		if len(storedWords) > 0 {
			alts = append(alts, lvWords(name, storedWords))
		}
		for _, raw := range raws {
			alts = append(alts, lvExact(name, raw))
		}
	}
	if len(m.Targets) > 0 {
		alts = append(alts, lvAny(sevNumber))
		if !anyRaw {
			alts = append(alts, lvGroup(logsql.AllOf{lvAny(sevName), lvExcl(lvAny(sevNumber))}))
		}
		body := logsql.AllOf{lvBang(lvGroup(hasLevelFilter()))}
		if !unknown {
			body = append(body, lvWords("_msg", words))
		}
		alts = append(alts, lvGroup(body))
	}
	if len(alts) == 0 {
		return nil
	}
	return lvGroup(alts)
}

// levelMatchNode is the builders' matchFilter: the filter on the field holding the derived value.
func levelMatchNode(m translate.DetectedLevelMatch, field string) logsql.FilterExpr {
	if m.Regexp {
		return translate.WholeValueRegexpFilterExpr(field, translate.LokiLabelFilterPattern(m.Value), m.Negated, false)
	}
	return translate.BuildFieldFilter(field, logsql.FieldOpExact, m.Value, m.Negated)
}

// buildLevelFilterNodes is translate.BuildDetectedLevelFilter over every level field, typed.
func buildLevelFilterNodes(op, value string, mode translate.DetectedLevelFilterMode) (levelFilterNodes, error) {
	m, err := translate.MatchDetectedLevel(op, value)
	if err != nil {
		return levelFilterNodes{}, err
	}
	if !m.Regexp && value == "" {
		return levelFilterNodes{Never: !m.Negated}, nil
	}
	if mode == translate.DetectedLevelFilterStored {
		return storedOnlyLevelNodes(m), nil
	}
	if len(m.Targets) == 0 && storedEmpty(m) {
		return levelFilterNodes{Never: !m.Negated}, nil
	}
	var n levelFilterNodes
	if !m.Negated {
		n.Prefilter = levelPrefilterNode(m)
	}
	if mode == translate.DetectedLevelFilterMetric {
		n.PrefilterPipe, n.Prefilter = n.Prefilter, nil
		n.Match = levelMatchNode(m, translate.DetectedLevelLabel)
		n.Pipes = metricLevelPipes([]logsql.FilterExpr{n.PrefilterPipe}, []logsql.FilterExpr{n.Match})
		return n, nil
	}
	n.Match = levelMatchNode(m, dlField)
	n.Pipes = logsLevelPipes([]logsql.FilterExpr{n.Match})
	return n, nil
}

// storedOnlyLevelNodes is the builders' storedOnlyLevelFilter: the stored level fields alone decide.
func storedOnlyLevelNodes(m translate.DetectedLevelMatch) levelFilterNodes {
	positive := storedFilter(m)
	if slices.Contains(m.Targets, "unknown") {
		positive = lvOr(positive, lvBang(lvGroup(hasLevelFilter())))
	}
	if positive == nil {
		return levelFilterNodes{Never: !m.Negated}
	}
	if m.Negated {
		return levelFilterNodes{Prefilter: lvBang(lvGroup(positive))}
	}
	return levelFilterNodes{Prefilter: lvGroup(positive)}
}

// levelMatches is the builders' andFilters: one filter as it is, several each in parentheses, one after another.
func levelMatches(matches []logsql.FilterExpr) logsql.FilterExpr {
	if len(matches) == 1 {
		return matches[0]
	}
	all := make(logsql.AllOf, len(matches))
	for i, f := range matches {
		all[i] = lvGroup(f)
	}
	return all
}

// metricLevelPipes is the metric form: the prefilter pipes, the grouping chain and the matchers on detected_level,
// which let the rows the proxy's witness flags through.
func metricLevelPipes(prefilters, matches []logsql.FilterExpr) []logsql.Pipe {
	chain := levelChainMetric()
	pipes := make([]logsql.Pipe, 0, len(prefilters)+len(chain)+1)
	for _, p := range prefilters {
		if p != nil {
			pipes = append(pipes, logsql.PipeFilter{Expr: p})
		}
	}
	pipes = append(pipes, chain...)
	return append(pipes, logsql.PipeFilter{Expr: logsql.AnyOf{lvGroup(levelMatches(matches)), lvAny(translate.LevelWitnessField)}})
}

// logsLevelPipes is the log form: the chain into __dl, the matchers on it, and the delete of every scratch field.
func logsLevelPipes(matches []logsql.FilterExpr) []logsql.Pipe {
	chain := levelChainLogs()
	pipes := make([]logsql.Pipe, 0, len(chain)+2)
	pipes = append(pipes, chain...)
	return append(pipes, logsql.PipeFilter{Expr: levelMatches(matches)}, logsql.PipeDelete{Labels: levelScratchAll})
}

// joinLevelFilters is translate.JoinDetectedLevelFilters over every level field, typed: one chain and every matcher
// for consecutive filters; nil when none has pipes.
func joinLevelFilters(plans []*levelFilterNodes, mode translate.DetectedLevelFilterMode) []logsql.Pipe {
	var kept []*levelFilterNodes
	for _, p := range plans {
		if len(p.Pipes) > 0 {
			kept = append(kept, p)
		}
	}
	switch len(kept) {
	case 0:
		return nil
	case 1:
		return kept[0].Pipes
	}
	matches := make([]logsql.FilterExpr, len(kept))
	pre := make([]logsql.FilterExpr, len(kept))
	for i, p := range kept {
		matches[i], pre[i] = p.Match, p.PrefilterPipe
	}
	if mode == translate.DetectedLevelFilterMetric {
		return metricLevelPipes(pre, matches)
	}
	return logsLevelPipes(matches)
}
