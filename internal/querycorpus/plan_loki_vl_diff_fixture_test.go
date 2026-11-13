package querycorpus_test

import (
	"encoding/json"
	"net/netip"
	"regexp"
	"regexp/syntax"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The data each log query of the Loki differential runs over (plan_loki_vl_diff_test.go): a few streams and the
// lines of each, built from the query's own plan so that its selector and stages have something to choose between.
//
//   - Streams: two the selector selects (each matcher given a value it accepts, the rest service_name, a cluster,
//     a level-like stream label and a known OpenTelemetry name), and one it does not when a value can be found.
//   - Lines: a fixed catalogue (json and logfmt lines with numbers, durations, sizes, nested keys, arrays,
//     booleans, empty values, unicode and escaped quotes; lines json and logfmt fail on; plain text with levels,
//     addresses and colours), then lines that hold the query's own literals: each line filter's text, a string
//     each regexp and pattern accepts, and each label filter's value with its neighbours, in json and logfmt.
//   - Structured metadata: on every third line, and for each label filter name a line that holds a value the
//     filter keeps and one it drops (never an empty value, which VictoriaLogs does not store, never a name
//     VictoriaLogs keeps for itself, and never a stream label's name, which VictoriaLogs holds as one field).
//
// Everything is a function of the plan, so the data and its hash are stable from run to run.

// diffFixtureVersion changes when the generator's output changes on purpose; it is part of the recorded hash.
const diffFixtureVersion = "5"

// diffMaxHarvested bounds the lines taken from one query's literals.
const diffMaxHarvested = 40

type diffEntry struct {
	Stream   int               `json:"stream"`
	Line     string            `json:"line"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type diffFixture struct {
	Streams []map[string]string `json:"streams"`
	Entries []diffEntry         `json:"entries"`
}

// diffCatalogue are the lines every query runs over.
var diffCatalogue = []string{
	`{"level":"error","msg":"request failed","status":500,"duration":"1.5s","bytes":"10KB","user":{"id":42,"name":"ann"},"tags":["a","b"],"ok":false}`,
	`{"level":"info","msg":"request ok","status":200,"duration":"120ms","bytes":"512B","path":"/api/users","method":"GET"}`,
	`{"level":"warn","msg":"slow","status":"404","latency_ms":1234.5,"ratio":0.25,"empty":"","nil":null}`,
	`{"msg":"unicode ünïcødé 日本 é","quote":"say \"hi\"","backslash":"a\\b","level":"debug"}`,
	`{"nested":{"deep":{"value":"x1"}},"arr":[{"k":"v"}],"n":-3,"big":1e21,"bool":true}`,
	`{"service_name":"from-line","app":"line-app","cluster":"line-cluster","level":"ERROR"}`,
	`{"status":503,"method":"POST","path":"/api/orders","duration":"2m3s","size":"1.5MiB","user":"bob"}`,
	`{"k8s.pod.name":"pod-dotted","http.status":"201","key-with-dash":"d","msg":"dotted keys"}`,
	`{"level":"info","ip":"10.0.0.5","msg":"login","user":"carol","status":401}`,
	`level=error msg="db timeout" status=500 duration=2s bytes=1KB user=bob`,
	`level=info msg=ok status=200 duration=15ms path=/healthz method=GET`,
	`level=warn msg="retry \"quoted\"" attempt=3 latency=0.5s empty= flag`,
	`ts=2026-01-01T00:00:00Z lvl=debug caller=main.go:42 msg="unicode ü"`,
	`status=404 method=DELETE path=/api/items/7 size=20KB`,
	`msg="multi word value" level=info count=007`,
	`{"level":"error","msg":"truncated`,
	`{not json at all}`,
	`level=error msg="unterminated`,
	`=novalue key==x`,
	`ERROR 2026-01-01 request failed for user 42`,
	`INFO GET /api/users 200 120ms`,
	`WARN disk usage 91%`,
	`debug: cache miss key=abc`,
	`panic: runtime error: index out of range`,
	`GET /api/orders HTTP/1.1 500 1234`,
	`timeout while calling upstream service`,
	`127.0.0.1 - - "GET /index.html" 200`,
	"tab\tseparated\tvalues",
	"\x1b[31mcolored error\x1b[0m",
	`   leading and trailing spaces   `,
}

// diffStreamBase are the labels of the two selected streams before the selector's values are set.
var diffStreamBase = []map[string]string{
	{"service_name": "checkout", "cluster": "c1"},
	{"service_name": "checkout", "cluster": "c2", "level": "warn", "k8s_namespace_name": "ns-1"},
}

// buildDiffFixture is the data of a log query.
func buildDiffFixture(sel *queryplan.LogSelect) diffFixture {
	var f diffFixture
	values := selectorValues(sel.Matchers)
	for i, base := range diffStreamBase {
		s := map[string]string{}
		for k, v := range base {
			s[k] = v
		}
		for name, vs := range values {
			v := vs[0]
			if i < len(vs) {
				v = vs[i]
			}
			if v == "" {
				delete(s, name)
			} else {
				s[name] = v
			}
		}
		if !matchesAll(sel.Matchers, s) {
			// A label the selector does not name may still be read by it (a matcher on an empty value): fall back to
			// the selector's values alone.
			s = map[string]string{"service_name": diffStreamBase[0]["service_name"]}
			for name, vs := range values {
				if vs[0] != "" {
					s[name] = vs[0]
				}
			}
			if !matchesAll(sel.Matchers, s) {
				continue
			}
		}
		f.Streams = append(f.Streams, s)
	}
	selected := len(f.Streams)
	if un := unselectedStream(sel.Matchers, f.Streams); un != nil {
		f.Streams = append(f.Streams, un)
	}
	if selected == 0 {
		return f
	}
	lines := append(append([]string{}, diffCatalogue...), harvestLines(sel)...)
	for i, line := range lines {
		e := diffEntry{Stream: i % selected, Line: line}
		if i%3 == 1 {
			e.Metadata = map[string]string{"trace_id": "tr-" + strconv.Itoa(i), "user_id": "u" + strconv.Itoa(i%4)}
		}
		f.Entries = append(f.Entries, e)
	}
	for _, sm := range harvestMetadata(sel, f.Streams[:selected]) {
		f.Entries = append(f.Entries, diffEntry{Stream: len(f.Entries) % selected, Line: "metadata line " + strconv.Itoa(len(f.Entries)), Metadata: sm})
	}
	if len(f.Streams) > selected {
		for i := 0; i < 4; i++ {
			f.Entries = append(f.Entries, diffEntry{Stream: selected, Line: diffCatalogue[i]})
		}
	}
	return f
}

// hash is the fixture's identity in the recorded answers.
func (f diffFixture) hash() string {
	b, _ := json.Marshal(f)
	return shortHash(diffFixtureVersion + "\x00" + string(b))
}

// selectorValues are, per label the selector names, the values that satisfy every matcher on it, best first; ""
// is the label left out (a matcher reads a missing label as the empty value).
func selectorValues(ms []queryplan.Matcher) map[string][]string {
	byName := map[string][]queryplan.Matcher{}
	var names []string
	for _, m := range ms {
		if _, ok := byName[m.Name]; !ok {
			names = append(names, m.Name)
		}
		byName[m.Name] = append(byName[m.Name], m)
	}
	out := map[string][]string{}
	for _, name := range names {
		var cands []string
		for _, m := range byName[name] {
			switch m.Op {
			case logql.MatchEq:
				cands = append(cands, m.Value)
			case logql.MatchRe:
				cands = append(cands, regexpSamples(m.Value)...)
			}
		}
		cands = append(cands, "other-1", "other-2", "a")
		if name != "service_name" {
			// Every stream has a service_name: Loki gives a stream without one a name at ingest, which the proxy
			// derives on read, and the process oracle has no ingest.
			cands = append(cands, "")
		}
		var ok []string
		seen := map[string]bool{}
		for _, c := range cands {
			if seen[c] {
				continue
			}
			seen[c] = true
			good := true
			for _, m := range byName[name] {
				if !matcherAccepts(m, c) {
					good = false
					break
				}
			}
			if good {
				ok = append(ok, c)
			}
		}
		if len(ok) == 0 {
			ok = []string{cands[0]}
		}
		out[name] = ok
	}
	return out
}

// matcherAccepts is whether a stream matcher accepts value (a regexp is anchored at both ends, as Loki's are).
func matcherAccepts(m queryplan.Matcher, value string) bool {
	switch m.Op {
	case logql.MatchEq:
		return value == m.Value
	case logql.MatchNeq:
		return value != m.Value
	}
	re, err := regexp.Compile("^(?:" + m.Value + ")$")
	if err != nil {
		return false
	}
	if m.Op == logql.MatchRe {
		return re.MatchString(value)
	}
	return !re.MatchString(value)
}

func matchesAll(ms []queryplan.Matcher, labels map[string]string) bool {
	for _, m := range ms {
		if !matcherAccepts(m, labels[m.Name]) {
			return false
		}
	}
	return true
}

// unselectedStream is the first selected stream with one matcher's label given a value it rejects, nil when no
// such value is found.
func unselectedStream(ms []queryplan.Matcher, streams []map[string]string) map[string]string {
	if len(streams) == 0 {
		return nil
	}
	for _, m := range ms {
		for _, v := range []string{"zz-unselected", "", "other-1"} {
			if v == "" && m.Name == "service_name" {
				continue
			}
			s := map[string]string{}
			for k, x := range streams[0] {
				s[k] = x
			}
			if v == "" {
				delete(s, m.Name)
			} else {
				s[m.Name] = v
			}
			if !matchesAll(ms, s) {
				return s
			}
		}
	}
	return nil
}

// harvestLines are lines that hold the query's literals, at most diffMaxHarvested.
func harvestLines(sel *queryplan.LogSelect) []string {
	var out []string
	seen := map[string]bool{}
	add := func(l string) {
		if l != "" && !seen[l] && len(out) < diffMaxHarvested {
			seen[l] = true
			out = append(out, l)
		}
	}
	// One json and one logfmt line with the first sample of every label filter of the pipeline, for the filters that
	// must hold together.
	together := map[string]string{}
	var order []string
	numeric := map[string]bool{}
	for _, st := range sel.Stages {
		if lf, ok := st.(*queryplan.LabelFilter); ok {
			for _, leaf := range labelFilterLeaves(lf.Expr) {
				if vs := labelFilterSamples(leaf); len(vs) > 0 && leaf.Name != "" {
					if _, dup := together[leaf.Name]; !dup {
						order = append(order, leaf.Name)
					}
					together[leaf.Name] = vs[0]
					numeric[leaf.Name] = leaf.Kind == logql.LabelFilterValueNumber
				}
			}
		}
	}
	if len(order) > 1 {
		var j, f strings.Builder
		j.WriteByte('{')
		for i, name := range order {
			k, _ := json.Marshal(name)
			v, _ := json.Marshal(together[name])
			if numeric[name] {
				if _, err := strconv.ParseFloat(together[name], 64); err == nil {
					v = []byte(together[name])
				}
			}
			if i > 0 {
				j.WriteByte(',')
				f.WriteByte(' ')
			}
			j.Write(k)
			j.WriteByte(':')
			j.Write(v)
			f.WriteString(strings.TrimSuffix(logfmtLine(name, together[name]), " msg=harvested"))
		}
		j.WriteByte('}')
		add(j.String())
		add(f.String())
	}
	for _, st := range sel.Stages {
		switch s := st.(type) {
		case *queryplan.LineFilter:
			alts := append([]logql.LineFilterAlt{{Value: s.Value, IP: s.IP}}, s.Or...)
			for _, a := range alts {
				for _, v := range lineFilterSamples(s.Op, a) {
					// A pattern filter matches the whole line: the sample alone, then inside a longer line.
					add(v)
					add("found " + v + " here")
					b, _ := json.Marshal(map[string]string{"msg": v, "level": "info"})
					add(string(b))
				}
			}
		case *queryplan.Parse:
			switch s.Type {
			case logql.ParserRegexp:
				for _, v := range regexpSamples(s.Param) {
					add(v)
				}
			case logql.ParserPattern:
				add(patternSample(s.Param))
			case logql.ParserJSON, logql.ParserLogfmt:
				for _, fld := range s.Fields {
					add(jsonPathLine(fld.Expression))
				}
			}
		case *queryplan.LabelFilter:
			for _, leaf := range labelFilterLeaves(s.Expr) {
				for _, v := range labelFilterSamples(leaf) {
					add(jsonLine(leaf.Name, v, leaf.Kind == logql.LabelFilterValueNumber))
					add(logfmtLine(leaf.Name, v))
				}
			}
		case *queryplan.Drop:
			for _, m := range s.Matchers {
				add(jsonLine(m.Name, m.Value, false))
				add(logfmtLine(m.Name, m.Value))
			}
		case *queryplan.Keep:
			for _, m := range s.Matchers {
				add(jsonLine(m.Name, m.Value, false))
				add(logfmtLine(m.Name, m.Value))
			}
		}
	}
	return out
}

// harvestMetadata are the structured metadata sets of the label filters' names: for each leaf on a name that is
// not a stream label, one set the filter keeps and one it drops. A name a stream already has is left out:
// VictoriaLogs holds a stream label and a field of one name as one field.
func harvestMetadata(sel *queryplan.LogSelect, streams []map[string]string) []map[string]string {
	var out []map[string]string
	seen := map[string]bool{}
	for _, st := range sel.Stages {
		lf, ok := st.(*queryplan.LabelFilter)
		if !ok {
			continue
		}
		for _, leaf := range labelFilterLeaves(lf.Expr) {
			if leaf.Name == "" || strings.HasPrefix(leaf.Name, "__") || leaf.Name == "detected_level" || vlReservedName(leaf.Name) || seen[leaf.Name] {
				continue
			}
			isStream := false
			for _, s := range streams {
				if _, ok := s[leaf.Name]; ok {
					isStream = true
				}
			}
			if isStream {
				continue
			}
			seen[leaf.Name] = true
			for _, v := range labelFilterSamples(leaf) {
				// VictoriaLogs stores no empty field, so an empty value is no structured metadata it can hold.
				if v != "" {
					out = append(out, map[string]string{leaf.Name: v})
				}
			}
			if len(out) >= 12 {
				return out
			}
		}
	}
	return out
}

// vlReservedName is a name VictoriaLogs keeps for itself (any case): structured metadata of that name cannot be stored
// as a field of its own, so the fixtures hold none.
func vlReservedName(name string) bool {
	switch strings.ToLower(name) {
	case "_msg", "_time", "_stream", "_stream_id":
		return true
	}
	return false
}

func labelFilterLeaves(e *logql.LabelFilterExpr) []*logql.LabelFilterExpr {
	if e == nil {
		return nil
	}
	if e.Op == logql.LabelFilterLeaf {
		return []*logql.LabelFilterExpr{e}
	}
	return append(labelFilterLeaves(e.Left), labelFilterLeaves(e.Right)...)
}

// labelFilterSamples are values around a label filter's literal: the literal and one on each side of it.
func labelFilterSamples(leaf *logql.LabelFilterExpr) []string {
	v := leaf.Value
	switch leaf.Kind {
	case logql.LabelFilterValueNumber:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return []string{v}
		}
		return []string{v, strconv.FormatFloat(f+1, 'f', -1, 64), strconv.FormatFloat(f-1, 'f', -1, 64)}
	case logql.LabelFilterValueDuration:
		return []string{v, "1h", "1ms"}
	case logql.LabelFilterValueBytes:
		return []string{v, "1GB", "1B"}
	case logql.LabelFilterValueIP:
		return []string{ipSample(v), "203.0.113.9"}
	case logql.LabelFilterValueString:
		if leaf.Cmp == "=~" || leaf.Cmp == "!~" {
			return append(regexpSamples(v), "zz-nomatch")
		}
		return []string{v, v + "z"}
	}
	return nil
}

// lineFilterSamples are strings a line filter's operand finds: its text, a match of its regexp or pattern, or an
// address in its range.
func lineFilterSamples(op logql.LineFilterOp, a logql.LineFilterAlt) []string {
	if a.IP {
		return []string{ipSample(a.Value)}
	}
	switch op {
	case logql.LineFilterMatchRe, logql.LineFilterExcludeRe:
		return regexpSamples(a.Value)
	case logql.LineFilterContainsPat, logql.LineFilterExcludePat:
		return []string{patternSample(a.Value)}
	}
	return []string{a.Value}
}

// regexpSamples are up to two strings the regexp accepts (the first and the second alternative of its top level).
func regexpSamples(src string) []string {
	re, err := syntax.Parse(src, syntax.Perl)
	if err != nil {
		return nil
	}
	re = re.Simplify()
	var out []string
	for alt := 0; alt < 2; alt++ {
		s := genRegexp(re, alt, 0)
		if len(s) > 200 {
			s = s[:200]
		}
		if !contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// genRegexp writes a string re accepts; alt picks the alternative at the top level.
func genRegexp(re *syntax.Regexp, alt, depth int) string {
	if depth > 20 {
		return ""
	}
	switch re.Op {
	case syntax.OpLiteral:
		return string(re.Rune)
	case syntax.OpCharClass:
		if len(re.Rune) == 0 {
			return ""
		}
		lo, hi := re.Rune[0], re.Rune[1]
		for i := 0; i+1 < len(re.Rune); i += 2 {
			if re.Rune[i] <= 'a' && 'a' <= re.Rune[i+1] {
				return "a"
			}
		}
		if lo < ' ' && hi >= ' ' {
			return " "
		}
		return string(lo)
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		return "a"
	case syntax.OpCapture:
		return genRegexp(re.Sub[0], alt, depth+1)
	case syntax.OpPlus:
		return genRegexp(re.Sub[0], 0, depth+1)
	case syntax.OpRepeat:
		var b strings.Builder
		for i := 0; i < re.Min && i < 8; i++ {
			b.WriteString(genRegexp(re.Sub[0], 0, depth+1))
		}
		return b.String()
	case syntax.OpConcat:
		var b strings.Builder
		for _, s := range re.Sub {
			b.WriteString(genRegexp(s, 0, depth+1))
		}
		return b.String()
	case syntax.OpAlternate:
		return genRegexp(re.Sub[alt%len(re.Sub)], 0, depth+1)
	}
	return ""
}

// patternSample fills a pattern's captures (<name> and <_>) with a word.
func patternSample(p string) string {
	var b strings.Builder
	for {
		i := strings.IndexByte(p, '<')
		if i < 0 {
			b.WriteString(p)
			return b.String()
		}
		j := strings.IndexByte(p[i:], '>')
		if j < 0 {
			b.WriteString(p)
			return b.String()
		}
		b.WriteString(p[:i])
		b.WriteString("v1")
		p = p[i+j+1:]
	}
}

// ipSample is an address inside an ip() operand: the address, the first of a range or of a prefix.
func ipSample(v string) string {
	if a, err := netip.ParseAddr(v); err == nil {
		return a.String()
	}
	if p, err := netip.ParsePrefix(v); err == nil {
		return p.Addr().String()
	}
	if lo, _, ok := strings.Cut(v, "-"); ok {
		if a, err := netip.ParseAddr(strings.TrimSpace(lo)); err == nil {
			return a.String()
		}
	}
	return v
}

// jsonLine is a json line holding name: value (a number unquoted when number is set).
func jsonLine(name, value string, number bool) string {
	if name == "" {
		return ""
	}
	if number {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			k, _ := json.Marshal(name)
			return `{` + string(k) + `:` + value + `,"msg":"harvested"}`
		}
	}
	b, _ := json.Marshal(map[string]string{name: value, "msg": "harvested"})
	return string(b)
}

// logfmtLine is a logfmt line holding name=value, quoted when it must be.
func logfmtLine(name, value string) string {
	if name == "" {
		return ""
	}
	if value == "" || strings.ContainsAny(value, " \"=\\\t") || strings.ContainsFunc(value, func(r rune) bool { return r > 126 || r < 32 }) {
		value = strconv.Quote(value)
	}
	return name + "=" + value + " msg=harvested"
}

// jsonPathLine is a json line with a value at a json parser's extraction path (a.b, a[0], a["b"]).
func jsonPathLine(expr string) string {
	var path []any
	for _, part := range strings.FieldsFunc(expr, func(r rune) bool { return r == '.' || r == '[' || r == ']' }) {
		part = strings.Trim(part, `"`)
		if n, err := strconv.Atoi(part); err == nil {
			path = append(path, n)
		} else if part != "" {
			path = append(path, part)
		}
	}
	if len(path) == 0 {
		return ""
	}
	var v any = "x1"
	for i := len(path) - 1; i >= 0; i-- {
		switch k := path[i].(type) {
		case int:
			arr := make([]any, k+1)
			for j := range arr {
				arr[j] = "x0"
			}
			arr[k] = v
			v = arr
		case string:
			v = map[string]any{k: v}
		}
	}
	if _, ok := v.(map[string]any); !ok {
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// sortedKeys are a map's keys, sorted.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
