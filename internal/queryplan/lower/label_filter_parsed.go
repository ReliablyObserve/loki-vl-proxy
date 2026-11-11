package lower

import (
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A label filter after a json or logfmt parser. The string translator writes it as a filter pipe of its own
// (`| unpack_json | filter status:>=500`) and matches the label against every key the parser can have stored it
// under: Loki sanitizes http.method and http-method to http_method, VictoriaLogs' unpack_* keeps the key, so
// `http_status="5"` becomes `(http_status:="5" OR "http.status":="5" OR "http-status":="5")`
// (translate.ParsedKeysOf). The leaf builders are the ones of a filter without a parser; only the keys and the
// filter pipe differ (logSelect places the pipe).
//
// The lowering leaves to the string translator the label filters whose text it reads in ways the typed filter
// does not carry, or writes in ways that are defects the lowering must not copy:
//   - a numeric, duration or bytes value compared with =, == or != (the translator writes the string form);
//   - a json extraction label whose name is written both ways the translator tells apart by text (`| json a_b`
//     and `| json a_b="a_b"`), and an expression the rewrite below cannot write;
//   - a name_extracted label in a query whose json or logfmt parsers have flags or an extraction list (the
//     string translator reads those parsers' text as naming the labels they extract, flags included);
//   - a filter whose keys the translator joins with a space, when a key's filter holds an alternation.

// checkParsedLabelFilterExpr names the construct of the first leaf of a label filter after a parser that the
// lowering does not write, "" when it writes all. It adds to checkLabelFilterExpr, which names the rest.
func checkParsedLabelFilterExpr(e *logql.LabelFilterExpr, sel *queryplan.LogSelect) string {
	if !e.IsLeaf() {
		if c := checkParsedLabelFilterExpr(e.Left, sel); c != "" {
			return c
		}
		return checkParsedLabelFilterExpr(e.Right, sel)
	}
	switch e.Kind {
	case logql.LabelFilterValueNumber, logql.LabelFilterValueDuration, logql.LabelFilterValueBytes:
		if e.Cmp == "=" || e.Cmp == "==" || e.Cmp == "!=" {
			return "LogSelect label filter numeric equality after parser"
		}
	}
	label := translate.SanitizeFieldIdentifier(e.Name)
	if namesJSONExtraction(sel, e.Name, label) {
		return "LogSelect label filter json extraction label"
	}
	return ""
}

// namesJSONExtraction reports whether a json stage's extraction list holds the label in the one form the string
// translator tells from another by the text of the stage: `| json a_b` extracts the key a_b (the filter then
// names the keys a_b can be stored under) and `| json a_b="a_b"` defines it (it names that field alone).
func namesJSONExtraction(sel *queryplan.LogSelect, raw, label string) bool {
	if !strings.Contains(label, "_") {
		return false // no keys to name either way
	}
	for _, st := range sel.Stages {
		p, ok := st.(*queryplan.Parse)
		if !ok || p.Type != logql.ParserJSON {
			continue
		}
		for _, f := range p.Fields {
			if f.Expression == f.Name && (f.Name == raw || f.Name == label) {
				return true
			}
		}
	}
	return false
}

// jsonAlias is a label a json stage defines from another key (`| json a="b.c"`).
type jsonAlias struct{ name, key string }

// addJSONAliases records the labels a json stage defines, a later stage's overriding an earlier one's (the string
// translator keeps them in a map).
func (lb *leafBuilder) addJSONAliases(p *queryplan.Parse) {
	if p.Type != logql.ParserJSON {
		return
	}
	for _, f := range p.Fields {
		if f.Expression == f.Name || f.Expression == "" {
			continue
		}
		replaced := false
		for i := range lb.aliases {
			if lb.aliases[i].name == f.Name {
				lb.aliases[i].key = f.Expression
				replaced = true
			}
		}
		if !replaced {
			lb.aliases = append(lb.aliases, jsonAlias{f.Name, f.Expression})
		}
	}
}

// aliased reports whether the label is one a json stage defines or reads (alias or key): the filter then names
// that field alone, not the keys a sanitized label can be stored under.
func (lb *leafBuilder) aliased(label string) bool {
	for _, a := range lb.aliases {
		if a.name == label || a.key == label {
			return true
		}
	}
	return false
}

// rewrite is the key a leaf names through a json alias: the string translator rewrites a filter written as
// `alias<operator>...` (the alias directly followed by the operator) to the key the alias reads, so the filter
// reads the key in the line and not the copy.
func (lb *leafBuilder) rewrite(leaf string) (key string, ok bool) {
	for _, a := range lb.aliases {
		if len(leaf) > len(a.name) && strings.HasPrefix(leaf, a.name) {
			switch leaf[len(a.name)] {
			case '=', '!', '<', '>', '~':
				return a.key, true
			}
		}
	}
	return "", false
}

// parsedLabelFilterLeaf is the filter of one leaf after a parser: the filter of a leaf without a parser when
// the label cannot be a sanitized key, else the filter on each key the label can be stored under, combined
// (translate.ParsedKeyFilter). A leaf that names a json alias is rewritten to its key first. c names the
// construct the lowering does not write, "" when it writes it.
func (lb *leafBuilder) parsedLeaf(e *logql.LabelFilterExpr) (f logsql.FilterExpr, c string) {
	if len(lb.sv) > 0 {
		name := e.Name
		key, rewritten := lb.rewrite(e.Leaf)
		if rewritten {
			name = key
		}
		if label := translate.SanitizeFieldIdentifier(name); slices.Contains(lb.sv, label) {
			if rewritten || lb.aliased(label) {
				return logsql.FieldFilter{}, "LogSelect stream label after parser" // the alias rules come first
			}
			se := *e
			se.Name = translate.StreamValueScratch + label
			return labelFilterLeaf(&se, nil, lb.caps), ""
		}
	}
	if key, ok := lb.rewrite(e.Leaf); ok {
		// The rewritten text is a filter on the key: any key the lowering would read differently from the string
		// translator's reading of that text stays with it.
		if translate.SanitizeFieldIdentifier(key) != key || !plainKey(key) {
			return logsql.FieldFilter{}, "LogSelect label filter json extraction label"
		}
		switch key {
		case "service_name", "detected_level", "extract":
			return logsql.FieldFilter{}, "LogSelect label filter json extraction label"
		}
		re := *e
		re.Name = key
		if lb.label != nil {
			if t := lb.label(key); strings.Contains(t, translate.ParsedKeySep) || translate.SanitizeFieldIdentifier(t) == "" {
				return logsql.FieldFilter{}, "LogSelect label filter json extraction label"
			}
		}
		return labelFilterLeaf(&re, lb.label, lb.caps), ""
	}
	if !lb.keyParsed {
		return labelFilterLeaf(e, lb.label, lb.caps), "" // a regexp or pattern names its labels itself
	}
	label := translate.SanitizeFieldIdentifier(e.Name)
	translated := label
	if lb.label != nil {
		translated = lb.label(label)
	}
	var keys []string
	if !lb.aliased(label) && !lb.names.exact(label) {
		keys = translate.ParsedKeysOf(label, translated)
	}
	if keys == nil {
		return labelFilterLeaf(e, lb.label, lb.caps), ""
	}
	op, _ := translate.LookupLabelFilterOp(e.Cmp) // the operator is known
	isIP := e.Kind == logql.LabelFilterValueIP
	if translate.ParsedKeysKeepsFirst(e.Value, op, isIP) {
		keys = keys[:1]
	}
	filters := make([]logsql.FilterExpr, len(keys))
	for i, key := range keys {
		field := translate.SanitizeFieldIdentifier(key)
		if strings.ContainsAny(field, ".-") {
			field = `"` + field + `"`
		} else {
			field = logsql.QuoteKeywordField(field) // a key called not, in or the like
		}
		filters[i], _ = translate.LabelFilterLeaf(field, e.Cmp, e.Value, isIP, lb.caps)
	}
	f, ok := translate.ParsedKeyFilter(filters, translate.ParsedKeyMatchesEmpty(e.Value, op, isIP))
	if !ok {
		return logsql.FieldFilter{}, "LogSelect label filter parsed keys"
	}
	if base, collision := translate.ExtractedBase(label); collision && lb.keyParsers > 0 {
		// A parsed label Loki renamed name_extracted where the stream has the label: its value is in a scratch
		// field the parsers unpack the key into.
		parsed, _ := translate.LabelFilterLeaf(translate.ExtractedScratch+"_"+base, e.Cmp, e.Value, isIP, lb.caps)
		if f, ok = translate.ExtractedFilter(base, parsed, f); !ok {
			return logsql.FieldFilter{}, "LogSelect label filter _extracted"
		}
		lb.noteBase(base)
	}
	return f, ""
}

// noteBase records a label whose name_extracted rename the query's filters read, for the query and for the stage.
func (lb *leafBuilder) noteBase(base string) {
	if !slices.Contains(lb.bases, base) {
		lb.bases = append(lb.bases, base)
	}
	if !slices.Contains(lb.stageBases, base) {
		lb.stageBases = append(lb.stageBases, base)
	}
}

// plainKey reports whether a json key is letters, digits, underscores, dots and hyphens that starts with a letter
// or an underscore: a name the string translator reads back as the label of the rewritten filter.
func plainKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || c == '.' || c == '-'):
		default:
			return false
		}
	}
	return key != ""
}
