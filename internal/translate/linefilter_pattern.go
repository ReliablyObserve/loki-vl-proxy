package translate

import (
	"html"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// PatternFilterScratch prefixes the fields the extract pipe of a pattern line
// filter writes; the filter deletes them once it has read them.
const PatternFilterScratch = "__lvp_pf"

// PatternLineFilterParts translates a pattern line filter (|> or !>) and its
// `or` alternatives into parts of the LogsQL query: the text the string
// translator writes, which PatternLineFilter builds as nodes.
//
// Loki keeps a line when pattern.Matcher.Test accepts it
// (pkg/logql/log/pattern/pattern.go:124-165): each literal is searched once,
// at its first occurrence after the previous one (a leading literal may start
// anywhere in the line), every capture must be non-empty, and a pattern that
// ends on a literal must end the line there. A regexp backtracks to later
// occurrences, so it is exact only for `<_>` and `literal<_>`. The other
// patterns with one literal are exact as a pair of regexp filters
// (exactSingleLiteralPatternFilter) and need no pipe. VictoriaLogs'
// extract pipe searches each text at its first occurrence as Test does
// (lib/logstorage/pattern.go apply), so the remaining patterns extract the line
// into scratch fields, one per capture plus the text after a trailing
// literal, and keep it when every capture is non-empty and that text is
// empty. Under |> the regexp, a superset of Loki's lines, stays in front as a
// prefilter. seq numbers the alternatives that extract, so their fields stay
// apart across the query.
func PatternLineFilterParts(values []string, negative bool, seq *int) []string {
	p := planPatternLineFilter(values, seq)
	if p.exact {
		if negative {
			return []string{"NOT ~" + strconv.Quote(p.combined)}
		}
		return []string{"~" + strconv.Quote(p.combined)}
	}

	if p.pipeFree {
		// Every alternative is a filter on the line: no extract is needed.
		terms := make([]string, len(p.alts))
		for i, alt := range p.alts {
			terms[i] = alt.filter().String()
		}
		keep := "(" + terms[0] + ")"
		if len(terms) > 1 {
			keep = "((" + strings.Join(terms, ") or (") + "))"
		}
		if negative {
			return []string{"NOT " + keep}
		}
		return []string{keep}
	}

	var pipes, conds []string
	for _, alt := range p.alts {
		if alt.extract == "" {
			conds = append(conds, alt.filter().String())
			continue
		}
		pipes = append(pipes, logsql.PipeExtract{Pattern: alt.extract, From: "_msg"}.String())
		conds = append(conds, alt.cond.String())
	}
	keep := conds[0]
	if len(conds) > 1 {
		keep = "(" + strings.Join(conds, ") or (") + ")"
	}
	if negative {
		keep = "NOT (" + keep + ")"
	}
	pipes = append(pipes, "| filter "+keep, patternScratchDelete.String())
	pipe := strings.Join(pipes, " ")
	if negative {
		return []string{pipe}
	}
	var out []string
	switch groups := patternLiteralPrefilter(p.alts); len(groups) {
	case 0:
	case 1:
		for _, f := range groups[0] {
			out = append(out, f.String())
		}
	default:
		terms := make([]string, len(groups))
		for i, g := range groups {
			terms[i] = g.String()
		}
		out = append(out, "(("+strings.Join(terms, ") or (")+"))")
	}
	return append(out, "~"+strconv.Quote(p.combined), pipe)
}

// PatternLineFilter is the pattern line filter PatternLineFilterParts writes,
// built as nodes: the filters on the line, in order, then the pipes (none when
// every alternative is a filter): the extract pipe of each alternative that
// needs one, the filter that keeps Loki's lines and the delete of the scratch
// fields. seq numbers the alternatives that extract, as PatternLineFilterParts
// does.
func PatternLineFilter(values []string, negative bool, seq *int) (filters []logsql.FilterExpr, pipes []logsql.Pipe) {
	p := planPatternLineFilter(values, seq)
	if p.exact {
		var f logsql.FilterExpr = logsql.Regexp{Pattern: p.combined}
		if negative {
			f = logsql.NotExpr{Expr: f}
		}
		return []logsql.FilterExpr{f}, nil
	}

	if p.pipeFree {
		terms := make([]logsql.FilterExpr, len(p.alts))
		for i, alt := range p.alts {
			terms[i] = alt.filter()
		}
		var keep logsql.FilterExpr = logsql.Group{Expr: anyOfGroups(terms)}
		if negative {
			keep = logsql.NotExpr{Expr: keep}
		}
		return []logsql.FilterExpr{keep}, nil
	}

	var conds []logsql.FilterExpr
	for _, alt := range p.alts {
		if alt.extract == "" {
			conds = append(conds, alt.filter())
			continue
		}
		pipes = append(pipes, logsql.PipeExtract{Pattern: alt.extract, From: "_msg"})
		conds = append(conds, alt.cond)
	}
	keep := anyOfGroups(conds)
	if negative {
		keep = logsql.NotExpr{Expr: logsql.Group{Expr: keep}}
	}
	pipes = append(pipes, logsql.PipeFilter{Expr: keep}, patternScratchDelete)
	if negative {
		return nil, pipes
	}
	switch groups := patternLiteralPrefilter(p.alts); len(groups) {
	case 0:
	case 1:
		filters = append(filters, groups[0]...)
	default:
		terms := make([]logsql.FilterExpr, len(groups))
		for i, g := range groups {
			terms[i] = g
		}
		filters = append(filters, logsql.Group{Expr: anyOfGroups(terms)})
	}
	return append(filters, logsql.Regexp{Pattern: p.combined}), pipes
}

// patternScratchDelete deletes the scratch fields of the pattern line filters.
var patternScratchDelete = logsql.PipeDelete{Labels: []string{PatternFilterScratch + "*"}}

// anyOfGroups is the OR of terms, each in parentheses; one term alone as it is.
func anyOfGroups(terms []logsql.FilterExpr) logsql.FilterExpr {
	if len(terms) == 1 {
		return terms[0]
	}
	out := make(logsql.AnyOf, len(terms))
	for i, t := range terms {
		out[i] = logsql.Group{Expr: t}
	}
	return out
}

// patternLineFilterPlan is the plan of a pattern line filter: its alternatives,
// combined, the regexp of all of them, which accepts every line Loki accepts,
// and whether that regexp is exact or no alternative needs the extract pipe.
type patternLineFilterPlan struct {
	alts            []patternLineFilterAlt
	combined        string
	exact, pipeFree bool
}

func planPatternLineFilter(values []string, seq *int) patternLineFilterPlan {
	p := patternLineFilterPlan{alts: make([]patternLineFilterAlt, len(values)), exact: true, pipeFree: true}
	regexes := make([]string, len(values))
	for i, value := range values {
		p.alts[i] = planPatternLineFilterAlt(value, *seq)
		regexes[i] = p.alts[i].re
		if p.alts[i].extract != "" {
			p.exact, p.pipeFree = false, false
			*seq++
		} else if p.alts[i].exact != nil {
			p.exact = false
		}
	}
	p.combined = regexes[0]
	if len(regexes) > 1 {
		p.combined = "(?:" + strings.Join(regexes, ")|(?:") + ")"
	}
	return p
}

// patternLiteralPrefilter returns plain regexp filters for the literals of the
// alternatives, to run before the anchored regexp: a line that matches an
// alternative holds all its literals, so these are necessary conditions, and
// VictoriaLogs answers a plain regexp as a substring search that skips the
// blocks without the text, where the anchored regexp runs on every row. It is
// one group of filters per alternative, which the caller ORs when there are
// several, and none when an alternative has no literal (<_>), which any line
// can match.
func patternLiteralPrefilter(alts []patternLineFilterAlt) []logsql.AllOf {
	groups := make([]logsql.AllOf, len(alts))
	for i, alt := range alts {
		if len(alt.lits) == 0 {
			return nil
		}
		for _, lit := range alt.lits {
			groups[i] = append(groups[i], msgRegexp(regexp.QuoteMeta(lit)))
		}
	}
	return groups
}

// msgRegexp is the regexp filter re on the message: _msg:~"re".
func msgRegexp(re string) logsql.FieldFilter {
	return logsql.FieldFilter{Field: "_msg", Op: logsql.FieldOpRegexp, Value: re}
}

// patternLineFilterAlt is one pattern alternative: re is a regexp on the line
// that accepts every line Loki accepts and is exact when extract is empty;
// otherwise extract is the extract pipe pattern and cond the filter on its
// fields that keeps exactly Loki's lines.
type patternLineFilterAlt struct {
	re, extract string
	cond        logsql.FilterExpr
	// lits are the distinct literals of the pattern: a line that holds none of
	// them cannot match, which the prefilter in front of extract says with
	// plain filters.
	lits []string
	// exact, when set, is a LogsQL filter (no pipe) that keeps exactly Loki's
	// lines; re is then only the superset regexp, and extract is empty.
	exact logsql.FilterExpr
}

// filter is the filter of an alternative that needs no extract pipe: exact, or
// its regexp on the message.
func (a patternLineFilterAlt) filter() logsql.FilterExpr {
	if a.exact != nil {
		return a.exact
	}
	return msgRegexp(a.re)
}

func planPatternLineFilterAlt(value string, id int) patternLineFilterAlt {
	if value == "" {
		// Loki's empty pattern is a matcher with no expression, which accepts
		// only an empty line (pkg/logql/log/pattern ParseLineFilter, Test).
		return patternLineFilterAlt{re: "^$"}
	}
	nodes := logql.LinePatternNodes(value)
	var lits []string
	for _, n := range nodes {
		if !n.Capture && !slices.Contains(lits, n.Text) {
			lits = append(lits, n.Text)
		}
	}
	var re, ext strings.Builder
	var cond logsql.AllOf
	captures := 0
	field := func(suffix string) string {
		return PatternFilterScratch + strconv.Itoa(id) + "_" + suffix
	}
	for i, n := range nodes {
		if !n.Capture {
			re.WriteString(regexp.QuoteMeta(n.Text))
			// extract reads `<` as the start of a field and html-unescapes
			// its texts; plain: keeps a quoted capture as it is in the line.
			ext.WriteString(html.EscapeString(n.Text))
			continue
		}
		if i == 0 {
			re.WriteString("^")
		}
		re.WriteString(".+")
		captures++
		f := field(strconv.Itoa(captures))
		ext.WriteString("<plain:" + f + ">")
		cond = append(cond, logsql.FieldFilter{Field: f, Op: logsql.FieldOpAny})
	}
	if !nodes[len(nodes)-1].Capture {
		re.WriteString("$")
		f := field("r")
		ext.WriteString("<plain:" + f + ">")
		cond = append(cond, logsql.FieldFilter{Field: f, Op: logsql.FieldOpEmpty})
	}
	alt := patternLineFilterAlt{re: re.String(), lits: lits}
	if captures > 0 {
		alt.re = "(?s)" + alt.re
	}
	if (len(nodes) == 1 && nodes[0].Capture) || (len(nodes) == 2 && !nodes[0].Capture && nodes[1].Capture) {
		return alt
	}
	if exact := exactSingleLiteralPatternFilter(nodes); exact != nil {
		alt.exact = exact
		return alt
	}
	if captures == 0 {
		// A literal alone leaves the trailing field empty when the line does
		// not hold it at all; the regexp says it does.
		cond = append(logsql.AllOf{msgRegexp(alt.re)}, cond...)
	}
	alt.extract = ext.String()
	alt.cond = cond
	return alt
}

// exactSingleLiteralPatternFilter returns a LogsQL filter, with no pipe, that
// keeps exactly the lines Loki's Matcher.Test accepts for a pattern with one
// literal L, or nil for any other pattern. L is searched once, at its first
// occurrence, and a later occurrence ends later, so "some occurrence is followed
// by a character" is the same as "the first one is". With "after" = (?s)L. (an
// occurrence followed by a character) and "start" = ^L (one at the start, which
// is then the first):
//
//	<_>L<_>  not start, and after (no occurrence is at the start, so one
//	         followed by a character needs no character before it)
//	<_>L     the line ends with L, not after (so that is the only occurrence,
//	         and it is the first), and the line is not L alone: ^L$
//	L        the line ends with L and not after
//
// <_> and L<_> are plain regexps and are not handled here. The plain regexp
// for L comes first: VictoriaLogs answers it as a substring search and prunes
// the blocks that do not hold L. The regexps are written without a leading
// `.`, which keeps VictoriaLogs from reading the literal ahead (a count over
// 518,500 rows: 64 ms for the plain filter, 67 ms for these, 134 ms with `.L.`
// and 130 ms for the extract form).
func exactSingleLiteralPatternFilter(nodes []logql.LinePatternNode) logsql.FilterExpr {
	var lit string
	var lead, trail bool
	switch len(nodes) {
	case 1:
		lit = nodes[0].Text
	case 2:
		lit, lead = nodes[0].Text, nodes[0].Capture
		if lead {
			lit = nodes[1].Text
		}
	case 3:
		if !nodes[0].Capture || nodes[1].Capture || !nodes[2].Capture {
			return nil
		}
		lit, lead, trail = nodes[1].Text, true, true
	default:
		return nil
	}
	if lit == "" {
		return nil
	}
	q := regexp.QuoteMeta(lit)
	not := func(re string) logsql.FilterExpr { return logsql.NotExpr{Expr: msgRegexp(re)} }
	switch {
	case lead && trail:
		return logsql.AllOf{msgRegexp(q), msgRegexp("(?s)" + q + "."), not("^" + q)}
	case lead:
		return logsql.AllOf{msgRegexp(q), msgRegexp("(?s)" + q + "$"), not("(?s)" + q + "."), not("^" + q + "$")}
	default:
		return logsql.AllOf{msgRegexp(q), msgRegexp("(?s)" + q + "$"), not("(?s)" + q + ".")}
	}
}
