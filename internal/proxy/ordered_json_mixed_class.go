package proxy

import (
	"regexp"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

const (
	mixedClassOriginalField  = "__vlmc_original"
	mixedClassStreamField    = "__vlmc_stream"
	mixedClassBodyField      = "__vlmc_body"
	mixedClassValidField     = "__vlmc_valid"
	mixedClassDupField       = "__vlmc_dup"
	mixedClassTargetPrefix   = "__vlmc_target_"
	mixedClassMaxBytes       = "4096"
	mixedClassInvalidBodyKey = `"(?:[ !#-~]*[\x20-\x21\x23-\x2f\x3a-\x40\x5b-\x5e\x60\x7b-\x7e][ !#-~]*|[0-9][ !#-~]*|)"\s*:`
)

// The three guards that read the stream descriptor's label names sit at this place of the class's base guards, after
// the three message guards and the target's value run: the descriptor's grammar, its aliased names and the label
// itself as a stream label. A certified class (certifiedFor) leaves them out, as the window's stored field names
// settle them; TestMixedJSONRowClassStreamNameGuardsAreTheDroppedOnes pins which guards they are.
const (
	mixedClassStreamNameGuardsAt = 4
	mixedClassStreamNameGuards   = 3
)

// mixedJSONRowClass describes a conservative backend row partition. It does
// not authorize a route: the caller must separately prove the original typed
// pipeline, source profile and alias configuration eligible. Prepared fields
// and predicates are immutable after construction.
type mixedJSONRowClass struct {
	label string
	// prepare is the complete preparation, with the packed original envelope the exact
	// complement restores; nativePrepare is what the native count needs (the same, unless the
	// class was derived for a window whose field names were checked: see certified).
	prepare       []logsql.Pipe
	nativePrepare []logsql.Pipe
	safe          logsql.FilterExpr
	// base is the guards before the five that read the packed envelope.
	base          logsql.AllOf
	originalField string
	// explicit is the class of `json k="[\"k\"]" | logfmt`, which reads only the named path (see
	// newMixedJSONRowClassFor); the memo key carries it, so a class of one shape is never served to the other.
	explicit bool
	// keysChecked marks the class that lacks those guards because the window's JSON key names are checked instead
	// (see collectKeyChecked); its certificates are kept apart from the full class's.
	keysChecked bool
}

// memoMode names the guard set for the certificate memo key.
func (c *mixedJSONRowClass) memoMode() string {
	switch {
	case c.explicit:
		return "explicit"
	case c.keysChecked:
		return "keys"
	}
	return "full"
}

// withoutKeyText is the class of the bare order without the three key-text guards, for a window whose JSON key
// names have been checked. It is the explicit order's guard set.
func (c *mixedJSONRowClass) withoutKeyText() *mixedJSONRowClass {
	derived, _ := newMixedJSONRowClassFor(c.label, true)
	derived.explicit = false
	derived.keysChecked = true
	return derived
}

// conformance: semantics/two-parsers-keep-one
func newMixedJSONRowClass(label string) (*mixedJSONRowClass, bool) {
	return newMixedJSONRowClassFor(label, false)
}

// newMixedJSONRowClassFor is the class for one of the two parser orders. The bare `logfmt | json` runs Loki's full
// JSON parse, which flattens nested objects, trims and sanitizes every key and so can read another spelling as the
// extracted key; three guards on the message's key text keep those rows out of the native count. The explicit
// `json k="[\"k\"]" | logfmt` reads only the named path: what is in the other keys cannot reach the label, so a row
// is no less safe for a trimmed, nested, dotted or long other key, and the native count takes it. The recorded Loki
// answers for those rows, both orders, through real VictoriaLogs 1.49 and 1.54, are
// TestOrderedMixedStatsShapeKeysMatchRecordedLoki; the duplicate-key, hazard and target guards are the same for both.
func newMixedJSONRowClassFor(label string, explicit bool) (*mixedJSONRowClass, bool) {
	if len(label) > 128 || !orderedJSONPushdownLabelOK(label) || !isValidLabelName(label) || strings.HasPrefix(label, "_") ||
		strings.HasSuffix(label, "_extracted") || label == "level" {
		return nil, false
	}
	name := regexp.QuoteMeta(label)
	// Finite ASCII ranges avoid VL's expensive formatting of wide negated
	// Unicode classes. Escapes are allowed only in packed source values; the
	// ORIGINAL message and stream values have stricter separate predicates.
	const valueRun = `[ !#-\[\]-~]*`
	const packedPair = `"[A-Za-z_][A-Za-z0-9_]{0,127}":"(?:[ !#-\[\]-~]|\\.)*"`
	const streamPair = `[A-Za-z_][A-Za-z0-9_]{0,127}="` + valueRun + `"`
	// The physical-key grammar is bounded above, but valid ASCII alone is
	// not an identity proof under LabelStyleUnderscores: double underscores
	// collapse and trailing underscores disappear. Match only actual keys,
	// preserving canonical internal names such as _time and _stream_id.
	const aliasedName = `(?:[A-Za-z0-9_]*__[A-Za-z0-9_]*|[A-Za-z0-9_]*_)`
	const scratchKey = `"__vlmc_[A-Za-z0-9_]*":`
	marker := `"` + name + `"\s*:`
	regex := func(field, pattern string, negate bool) logsql.FilterExpr {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: pattern, Negate: negate}
	}
	// VictoriaLogs evaluates an AND in written order and stops at the first
	// guard that rejects a row, so the order is a cost order: the cheap
	// message-only checks first (a row that is not one flat printable JSON
	// object with the target key never reaches a long regexp), then the
	// stream descriptor (constant per stream and block), the extracted target,
	// the longer message regexps, and last the guards that read the packed
	// envelope. Every conjunct is unchanged; only their order differs.
	safe := logsql.AllOf{
		regex("_msg", `^\{[\x20-\x7e]*\}$`, false),
		regex("_msg", `[\\=]`, true),
		logsql.LenRange{Field: "_msg", Min: "0", Max: mixedClassMaxBytes},
		regex("_msg", marker+`\s*"`+valueRun+`"`, false),
		regex(mixedClassStreamField, `^\{(?:`+streamPair+`(?:,`+streamPair+`)*)?\}$`, false),
		mixedClassLiteralGuard(mixedClassStreamField, `(?:^\{|,)`+aliasedName+`=`, "__", "_="),
		mixedClassLiteralGuard(mixedClassStreamField, `(?:^\{|,)`+name+`=`, label+"="),
		logsql.FieldFilter{Field: mixedClassTargetPrefix + mixedClassValidField, Op: logsql.FieldOpAny},
		logsql.FieldFilter{Field: mixedClassTargetPrefix + label, Op: logsql.FieldOpAny},
		// VL stats grouping may canonicalize numeric-looking STRING values
		// (notably "-0" to "0"). Admit categorical values only; the exact
		// original mixed evaluator keeps every other value in the complement.
		regex(mixedClassTargetPrefix+label, `^[A-Za-z_][A-Za-z0-9_-]*$`, false),
		regex(mixedClassTargetPrefix+label, `(?i)^(?:nan|inf|infinity|true|false|null)$`, true),
		// The quoted name twice in the message (the key twice, or the key and a value of the same name): the copy has
		// its first occurrence removed (see prepare), so a second one is still there. Stricter than two keys, which
		// only sends such a row to the exact complement, and a literal search instead of a lazy regexp between two
		// matches.
		regex(mixedClassDupField, regexp.QuoteMeta(`"`+label+`"`), true),
		mixedClassLiteralGuard("_msg", `"__vlmc_[A-Za-z0-9_]*"\s*:`, "__vlmc_"),
		// This admission bound is deliberately stricter than Loki's accepted
		// keys; longer names retain the original exact mixed execution. The
		// pattern is a cheaper superset of "a key of 129 or more name
		// characters": any quoted run that long, so a row with a long value
		// is rejected too and takes the exact complement, which only costs
		// that row its native count.
	}
	if !explicit {
		safe = append(safe,
			regex("_msg", `"[A-Za-z0-9_]{129}`, true),
			regex("_msg", mixedClassBodyAliasPattern(label), true))
	}
	// Reuse the existing closed JSON disagreement predicate as typed
	// nodes, preserving the legacy spelling without a deferred-text site.
	safe = append(safe, logsql.NotExpr{Expr: jsonLokiRejectHazardFilter()})
	if !explicit {
		parts := strings.Split(label, "_")
		for i := 1; i < len(parts); i++ {
			prefix := regexp.QuoteMeta(strings.Join(parts[:i], "_"))
			safe = append(safe, regex("_msg", `"`+prefix+`"\s*:\s*\{`, true))
		}
	}
	// The guards that read the packed envelope force pack_json for the block and
	// are the most expensive, so they come last, after every message, stream and
	// target guard (including the nested-prefix guards above).
	base := append(logsql.AllOf(nil), safe...)
	safe = append(safe,
		logsql.LenRange{Field: mixedClassOriginalField, Min: "0", Max: mixedClassMaxBytes},
		regex(mixedClassOriginalField, `^\{(?:`+packedPair+`(?:,`+packedPair+`)*)?\}$`, false),
		mixedClassLiteralGuard(mixedClassOriginalField, `(?:^\{|,)"`+aliasedName+`":`, "__", `_":`),
		mixedClassLiteralGuard(mixedClassOriginalField, `(?:^\{|,)"`+name+`":`, `"`+label+`":`),
		mixedClassLiteralGuard(mixedClassOriginalField, `(?:^\{|,)`+scratchKey, "__vlmc_"),
	)
	prepare := []logsql.Pipe{
		// Snapshot first, including any preexisting field with this name.
		// The raw consumer restores this envelope instead of deleting
		// scratch fields and losing original collision/provenance data.
		logsql.PipePackJSON{ResultField: mixedClassOriginalField},
		logsql.PipeCopy{Pairs: [][2]string{{"_stream", mixedClassStreamField}, {"_msg", mixedClassBodyField}, {"_msg", mixedClassDupField}}},
		logsql.PipeReplace{Field: mixedClassDupField, Old: `"` + label + `"`, New: "", Limit: 1},
		// The same complete-object sentinel splice as jsonValidityPipes,
		// expressed in the existing AST and applied to the message COPY.
		logsql.PipeReplaceRegexp{Field: mixedClassBodyField, Regex: `^\{`, Replacement: `{"` + mixedClassValidField + `":1,`},
		// One parse of the spliced body gives both the sentinel (present only when the whole text parsed) and the
		// target: the spliced body holds the message's keys and one more.
		logsql.PipeUnpackJSON{From: mixedClassBodyField, Fields: []string{mixedClassValidField, label}, ResultPrefix: mixedClassTargetPrefix},
	}
	return &mixedJSONRowClass{
		label:         label,
		originalField: mixedClassOriginalField,
		prepare:       prepare,
		nativePrepare: prepare,
		base:          base,
		safe:          safe,
		explicit:      explicit,
	}, true
}

// Required-label hints discard unrelated body names before parser budgets.
// Retain all spellings that could alias the requested name or its nested
// prefixes. Blank keys must remain excluded: a chain of empty object names
// can reach the depth bound without growing its relevant prefix. The 4 KiB
// body bound and nonempty relevant names retain the field/value/depth bounds.
func mixedClassBodyAliasPattern(label string) string {
	// Bound span enumeration before expanding it; larger names retain the
	// conservative key grammar and its unchanged exact raw complement.
	if strings.Count(label, "_") > maxOrderedJSONAliasUnderscores {
		return mixedClassInvalidBodyKey
	}
	pattern := orderedJSONKeyAliasPattern([]string{label})
	if pattern == "" {
		// The shared helper handles underscored names. With printable ASCII
		// and no escapes already established, other names only gain aliases
		// through surrounding spaces; keep blank keys excluded as well.
		name := regexp.QuoteMeta(label)
		pattern = `(?:"(?: +` + name + ` *|` + name + ` +)"\s*:|" *"\s*:)`
	}
	if len(pattern) > vlDefaultMaxQueryLen/4 {
		// Many underscore spans can expand the alias expression. Preserve
		// the conservative all-key guard instead of growing backend SQL.
		return mixedClassInvalidBodyKey
	}
	return pattern
}

// Each positive pattern necessarily contains one of its literal prefilters.
// NOT(P AND R) therefore preserves NOT(R) for all inputs while letting VL's
// literal regexp fast path avoid evaluating R on unrelated source rows.
func mixedClassLiteralGuard(field, pattern string, literals ...string) logsql.FilterExpr {
	prefilter := make(logsql.AnyOf, 0, len(literals))
	for _, literal := range literals {
		prefilter = append(prefilter, logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: regexp.QuoteMeta(literal)})
	}
	original := logsql.FieldFilter{Field: field, Op: logsql.FieldOpRegexp, Value: pattern}
	return logsql.NotExpr{Expr: logsql.AllOf{prefilter, original}}
}

func (c *mixedJSONRowClass) nativePipes() []logsql.Pipe {
	return filteredMixedClassPipes(c.nativePrepare, c.safe)
}

func (c *mixedJSONRowClass) rawPipes() []logsql.Pipe {
	return filteredMixedClassPipes(c.prepare, logsql.NotExpr{Expr: c.safe})
}

func filteredMixedClassPipes(prepare []logsql.Pipe, filter logsql.FilterExpr) []logsql.Pipe {
	pipes := make([]logsql.Pipe, len(prepare)+1)
	for i, pipe := range prepare {
		switch node := pipe.(type) {
		case logsql.PipeCopy:
			node.Pairs = append([][2]string(nil), node.Pairs...)
			pipes[i] = node
		case logsql.PipeUnpackJSON:
			node.Fields = append([]string(nil), node.Fields...)
			pipes[i] = node
		default:
			pipes[i] = pipe
		}
	}
	pipes[len(prepare)] = logsql.PipeFilter{Expr: cloneMixedClassFilter(filter)}
	return pipes
}

// The constructor's mutable filter children are AND/OR slices. Recurse
// through NOT/group wrappers as well, so callers own the entire returned
// tree rather than a shallow copy of its outer node. Other constructor nodes
// contain only scalar values and immutable strings.
func cloneMixedClassFilter(expr logsql.FilterExpr) logsql.FilterExpr {
	switch node := expr.(type) {
	case logsql.AllOf:
		out := make(logsql.AllOf, len(node))
		for i, child := range node {
			out[i] = cloneMixedClassFilter(child)
		}
		return out
	case logsql.AnyOfLower:
		out := make(logsql.AnyOfLower, len(node))
		for i, child := range node {
			out[i] = cloneMixedClassFilter(child)
		}
		return out
	case logsql.AnyOf:
		out := make(logsql.AnyOf, len(node))
		for i, child := range node {
			out[i] = cloneMixedClassFilter(child)
		}
		return out
	case logsql.NotExpr:
		return logsql.NotExpr{Expr: cloneMixedClassFilter(node.Expr)}
	case logsql.Group:
		return logsql.Group{Expr: cloneMixedClassFilter(node.Expr)}
	default:
		return expr
	}
}
