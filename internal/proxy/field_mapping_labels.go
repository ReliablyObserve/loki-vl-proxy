package proxy

import (
	"context"
	"fmt"
	"slices"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// FieldMappingLabelMode controls which names of a -field-mapping field the
// answers show: the label listings (/labels, detected_labels, the volume target
// labels), stream and series labels, structured metadata and detected fields.
// Queries and /label/{name}/values accept every name of a mapping in every mode.
type FieldMappingLabelMode string

const (
	// FieldMappingLabelModeUnset is the flag left empty: one name per field in
	// every answer, the loki_label of the field's last mapping (the behaviour of
	// builds without the flag).
	FieldMappingLabelModeUnset FieldMappingLabelMode = ""
	// FieldMappingLabelModeOriginal shows a mapped field under its own name as
	// stored, formatted only by -label-style (labels) and -metadata-field-mode
	// (structured metadata and detected fields), with no alias.
	FieldMappingLabelModeOriginal FieldMappingLabelMode = "original"
	// FieldMappingLabelModeAlias shows a mapped field under its aliases only (the
	// loki_labels that differ from the name -label-style gives the field), and a
	// field without an alias under its own name.
	FieldMappingLabelModeAlias FieldMappingLabelMode = "alias"
	// FieldMappingLabelModeBoth shows the field's own names and its aliases.
	FieldMappingLabelModeBoth FieldMappingLabelMode = "both"
)

// ParseFieldMappingLabelMode reads a -field-mapping-label-mode value. Empty is
// FieldMappingLabelModeUnset.
func ParseFieldMappingLabelMode(value string) (FieldMappingLabelMode, error) {
	switch mode := FieldMappingLabelMode(strings.TrimSpace(value)); mode {
	case FieldMappingLabelModeUnset, FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid -field-mapping-label-mode: %q (must be 'original', 'alias', or 'both', or empty)", value)
	}
}

// SetFieldMappingLabelMode applies mode to the mappings. It is called once,
// before the translator is shared. The unset mode changes nothing.
//
// In the original, alias and both modes, for each mapped field:
//   - its base name is what -label-style gives it without a mapping;
//   - its aliases are the loki_labels of its mappings that differ from the base
//     name and resolve back to it (a loki_label given to two fields belongs to
//     the later mapping), in -field-mapping order;
//   - answers (stream and series labels) carry the base name in the original
//     mode, else the last alias, or the base name when there is none;
//   - listings show the base name (original), the aliases (alias) or the
//     aliases and the base name (both), or the base name when there is no alias;
//   - the base name becomes a query name of the field, unless a mapping or
//     another mapped field already owns it; the aliases stay query names.
func (lt *LabelTranslator) SetFieldMappingLabelMode(mode FieldMappingLabelMode) {
	if mode != FieldMappingLabelModeOriginal && mode != FieldMappingLabelModeAlias && mode != FieldMappingLabelModeBoth {
		return
	}
	lt.labelMode = mode
	lt.listing = make(map[string][]string, len(lt.vlToLoki))
	lt.aliases = make(map[string][]string, len(lt.vlToLoki))
	// Aliases are read before any base name is registered as a query name.
	aliases := make(map[string][]string, len(lt.vlToLoki))
	var fields []string
	for _, m := range lt.mappings {
		if m.VLField == "" || m.LokiLabel == "" {
			continue
		}
		if !slices.Contains(fields, m.VLField) {
			fields = append(fields, m.VLField)
		}
		if m.LokiLabel == lt.baseName(m.VLField) || lt.lokiToVL[m.LokiLabel] != m.VLField {
			continue
		}
		// A repeated mapping moves the alias to its later place.
		list := slices.DeleteFunc(aliases[m.VLField], func(a string) bool { return a == m.LokiLabel })
		aliases[m.VLField] = append(list, m.LokiLabel)
	}
	for _, field := range fields {
		base := lt.baseName(field)
		owner, owned := lt.lokiToVL[base]
		// Registered even when the base name is the stored name: -label-style
		// could otherwise read it as a dotted OTel name. A base name that is
		// itself another mapped field keeps reading that field.
		if _, mappedField := lt.vlToLoki[base]; !owned && (base == field || !mappedField) {
			lt.lokiToVL[base] = field
			owner, owned = field, true
		}
		own := aliases[field]
		lt.aliases[field] = own
		ownsBase := owned && owner == field
		switch {
		case len(own) == 0:
			lt.vlToLoki[field] = base
			lt.listing[field] = []string{base}
		case mode == FieldMappingLabelModeOriginal && ownsBase:
			lt.vlToLoki[field] = base
			lt.listing[field] = []string{base}
		case mode == FieldMappingLabelModeOriginal:
			// Another field reads the base name: the field keeps the name it
			// has without the flag, the last mapping's.
			lt.listing[field] = []string{lt.vlToLoki[field]}
		default:
			lt.vlToLoki[field] = own[len(own)-1]
			listed := slices.Clone(own)
			if mode == FieldMappingLabelModeBoth && ownsBase {
				listed = append(listed, base)
			}
			lt.listing[field] = listed
		}
	}
}

// mappedMetadataExposures returns the names a mapped field has in structured
// metadata and detected fields in the original, alias and both modes. Its own
// names are the ones the metadata field mode gives the field (native: the
// stored name; translated: the base name; hybrid: both), the base name only
// while it reads the field. The original mode shows the own names, the alias
// mode the aliases (the own names for a field without one), the both mode
// both. false for any other field and in the unset mode.
func (lt *LabelTranslator) mappedMetadataExposures(vlField string, mode MetadataFieldMode) ([]metadataFieldExposure, bool) {
	if lt == nil || lt.labelMode == FieldMappingLabelModeUnset {
		return nil, false
	}
	aliases, mapped := lt.aliases[vlField]
	if !mapped {
		return nil, false
	}
	result := make([]metadataFieldExposure, 0, 2+len(aliases))
	add := func(name string) {
		if name == "" || slices.ContainsFunc(result, func(e metadataFieldExposure) bool { return e.name == name }) {
			return
		}
		result = append(result, metadataFieldExposure{name: name, isAlias: name != vlField})
	}
	if lt.labelMode != FieldMappingLabelModeAlias || len(aliases) == 0 {
		base := lt.baseName(vlField)
		ownsBase := lt.lokiToVL[base] == vlField
		if mode != MetadataFieldModeTranslated || !ownsBase {
			add(vlField)
		}
		if mode != MetadataFieldModeNative && ownsBase {
			add(base)
		}
	}
	if lt.labelMode != FieldMappingLabelModeOriginal {
		for _, alias := range aliases {
			add(alias)
		}
	}
	return result, true
}

// keepsMappedValue reports whether a structured metadata name already set for
// an entry keeps its value against vlField: in the original, alias and both
// modes, a name a mapped field claims as a query name (an alias, or a base name
// the mode made one) keeps the mapped field's value against a stored field of
// that name, whatever order the entry's fields come in, as queries for the name
// read the mapped field. Parsed labels are not concerned.
func (lt *LabelTranslator) keepsMappedValue(name, vlField string) bool {
	if lt == nil || lt.labelMode == FieldMappingLabelModeUnset {
		return false
	}
	owner := lt.lokiToVL[name]
	_, mapped := lt.aliases[owner]
	return mapped && owner != vlField
}

// setMetadata sets an entry's structured metadata name to the value of
// vlField, unless the name already holds the value of the mapped field that
// claims it (keepsMappedValue).
func (lt *LabelTranslator) setMetadata(sm map[string]string, name, vlField, value string) {
	if lt.keepsMappedValue(name, vlField) && sm[name] != "" {
		return
	}
	sm[name] = value
}

// namesMappedField reports an alias a mapped field shows in structured
// metadata and detected fields (mappedMetadataExposures, alias and both
// modes). Detected fields list it even when it is a name they leave out for
// other fields (namespace, cluster, app).
func (lt *LabelTranslator) namesMappedField(name, vlField string) bool {
	if lt == nil || (lt.labelMode != FieldMappingLabelModeAlias && lt.labelMode != FieldMappingLabelModeBoth) {
		return false
	}
	return slices.Contains(lt.aliases[vlField], name)
}

// requestedGroupNames maps a mapped field that a by() grouping of query names
// under another of its names than the one answers carry to that name, in the
// listing modes: Loki answers a grouping under the label it names. The names
// come from the typed plan (the plan memo when the handlers planned it). nil in
// the default mode and when no grouping names such a field.
func (p *Proxy) requestedGroupNames(ctx context.Context, query string) map[string]string {
	lt := p.labelTranslator
	if !lt.listsAliases() || query == "" {
		return nil
	}
	plan, err := loweringPlan(ctx, logqlpkg.CanonicalizeLogRanges(query))
	if err != nil || plan == nil {
		return nil
	}
	var out map[string]string
	add := func(g *queryplan.Grouping) {
		if g == nil || g.Without {
			return
		}
		for _, label := range g.Labels {
			field, ok := lt.lokiToVL[label]
			if _, mapped := lt.aliases[field]; !ok || !mapped || lt.ToLoki(field) == label {
				continue
			}
			if _, named := out[field]; named {
				continue // the first name a query gives the field
			}
			if out == nil {
				out = make(map[string]string, 2)
			}
			out[field] = label
		}
	}
	var walk func(queryplan.Node)
	walk = func(n queryplan.Node) {
		switch n := n.(type) {
		case *queryplan.VectorAgg:
			add(n.By)
			walk(n.In)
		case *queryplan.Rank:
			add(n.By)
			walk(n.In)
		case *queryplan.RangeAgg:
			add(n.By)
		case *queryplan.BinOp:
			walk(n.L)
			walk(n.R)
		case *queryplan.LabelReplace:
			walk(n.In)
		}
	}
	walk(plan.Root)
	return out
}

// baseName is the name -label-style gives a field without a mapping.
func (lt *LabelTranslator) baseName(vlField string) string {
	if lt.style == LabelStyleUnderscores {
		return SanitizeLabelName(vlField)
	}
	return vlField
}

// listsAliases reports a mode whose listings can show more than one name per field.
func (lt *LabelTranslator) listsAliases() bool {
	return lt != nil && lt.listing != nil
}

// listedNames returns the names a label listing shows for a VictoriaLogs field.
// one backs the result for a field with a single name, so nothing is allocated.
func (lt *LabelTranslator) listedNames(vlField string, one *[1]string) []string {
	if names, ok := lt.listing[vlField]; ok {
		return names
	}
	one[0] = lt.ToLoki(vlField)
	return one[:]
}

// aliasAnswer returns the alias a VictoriaLogs field carries in answers in the
// alias and both modes: a stream field of that field is the stream label of
// the alias, as in a Loki that stored the alias, so a parsed key of the same
// name is renamed name_extracted beside it. false for a field without an alias
// and in the original mode.
func (lt *LabelTranslator) aliasAnswer(vlField string) (string, bool) {
	if !lt.listsAliases() || lt.labelMode == FieldMappingLabelModeOriginal {
		return "", false
	}
	if _, mapped := lt.listing[vlField]; !mapped {
		return "", false
	}
	answer := lt.vlToLoki[vlField]
	return answer, answer != "" && answer != lt.baseName(vlField)
}

// isAliasLabel reports a label that is the alias a mapped field carries in
// answers (aliasAnswer): its stream label lives in that stored field, which no
// parser overwrites.
func (lt *LabelTranslator) isAliasLabel(label string) bool {
	return lt.aliasField(label) != ""
}

// aliasField returns the stored field an alias label is the stream label of
// (isAliasLabel), "" for any other label and in the original mode. In the both
// mode the field's -label-style name is such a label too (bothBaseName).
func (lt *LabelTranslator) aliasField(label string) string {
	if !lt.listsAliases() {
		return ""
	}
	field, ok := lt.lokiToVL[label]
	if !ok {
		return ""
	}
	answer, ok := lt.aliasAnswer(field)
	if !ok {
		return ""
	}
	if answer != label {
		if base, listed := lt.bothBaseName(field); !listed || base != label {
			return ""
		}
	}
	return field
}

// bothBaseName returns the -label-style name of a field with an alias in the
// both mode, where stream labels carry it beside the alias
// (field_mapping_both_answers.go): a parsed key of that name is renamed
// name_extracted beside it, as Loki renames a key named like a stream label.
// false in the other modes and for a field the both mode lists no base name of.
func (lt *LabelTranslator) bothBaseName(field string) (string, bool) {
	if lt.labelMode != FieldMappingLabelModeBoth {
		return "", false
	}
	base := lt.baseName(field)
	if _, ok := lt.aliasAnswer(field); !ok || !slices.Contains(lt.listing[field], base) {
		return "", false
	}
	return base, true
}

// fieldAlias is the alias a stored field carries as its stream label in the
// alias and both modes, "" for any other field (lower.Profile FieldAlias).
func (lt *LabelTranslator) fieldAlias(field string) string {
	if alias, ok := lt.aliasAnswer(field); ok {
		return alias
	}
	return ""
}

// withAliases gives a lowering profile the alias functions in the alias and
// both modes; the unset and original modes leave them nil and their text
// unchanged.
func (lt *LabelTranslator) withAliases(prof lower.Profile) lower.Profile {
	if lt.listsAliases() && lt.labelMode != FieldMappingLabelModeOriginal {
		prof.AliasField, prof.FieldAlias = lt.aliasField, lt.fieldAlias
	}
	return prof
}

// answerName is the name answers carry for the field a mapped label name reads.
func (lt *LabelTranslator) answerName(label string) (string, bool) {
	if !lt.listsAliases() {
		return "", false
	}
	field, ok := lt.lokiToVL[label]
	if !ok {
		return "", false
	}
	return lt.ToLoki(field), true
}

// addRequestedNames gives labels, keyed by answer names, a value under each of
// names that is another name of one of its fields (volume target labels).
func (lt *LabelTranslator) addRequestedNames(labels map[string]string, names []string) {
	if !lt.listsAliases() {
		return
	}
	for _, name := range names {
		if labels[name] != "" {
			continue
		}
		if answer, ok := lt.answerName(name); ok && labels[answer] != "" {
			labels[name] = labels[answer]
		}
	}
}

// otherListedNames returns the listed names of the fields in labels (keyed by
// answer names) that labels does not carry.
func (lt *LabelTranslator) otherListedNames(labels map[string]string) []string {
	if !lt.listsAliases() {
		return nil
	}
	var out []string
	for field, names := range lt.listing {
		if labels[lt.ToLoki(field)] == "" {
			continue
		}
		for _, name := range names {
			if _, ok := labels[name]; !ok {
				out = appendUniqueString(out, name)
			}
		}
	}
	return out
}

// lokiLabelListing is the /labels answer for a VictoriaLogs name listing.
func (p *Proxy) lokiLabelListing(listed []string) []string {
	filtered := make([]string, 0, len(listed))
	for _, v := range listed {
		// Filter VL internal fields only (before translation). A detected_level
		// stream field is an index label, as in Loki.
		if isVLInternalField(v) {
			continue
		}
		filtered = append(filtered, v)
	}
	// Apply label name translation (e.g., dots → underscores)
	labels := p.labelTranslator.TranslateLabelsList(filtered)
	if len(listed) > 0 && p.labelTranslator.listsAliases() {
		labels = appendUniqueStrings(labels, p.declaredListingNames(listed)...)
	}
	return appendSyntheticLabels(labels)
}

// declaredListingNames returns the names the declared label fields add to
// listed, VictoriaLogs' own listing, in the alias and both modes:
//   - a declared name that is a mapping's loki_label is listed as declared;
//   - another declared field that listed does not hold is listed under its
//     -label-style name when it is mapped (an alias is listed for a field with
//     data only), and as in the default mode when it is not.
func (p *Proxy) declaredListingNames(listed []string) []string {
	if len(p.declaredLabelNames) == 0 {
		return nil
	}
	lt := p.labelTranslator
	// The default mode learns these from the listing it appends them to.
	lt.LearnFieldAliases(p.declaredLabelFields)
	have := make(map[string]struct{}, len(listed))
	for _, field := range listed {
		have[field] = struct{}{}
	}
	var out []string
	for _, declared := range p.declaredLabelNames {
		field := strings.TrimSpace(lt.ToVL(declared))
		if field == "" {
			field = declared
		}
		if isVLInternalField(field) {
			continue
		}
		// The original mode shows no alias: a declared alias lists its field
		// under the base name below.
		if lt.isMappingName(declared, field) && lt.labelMode != FieldMappingLabelModeOriginal {
			out = appendUniqueString(out, declared)
			continue
		}
		if _, ok := have[field]; ok {
			continue
		}
		if _, mapped := lt.listing[field]; mapped {
			out = appendUniqueString(out, lt.baseName(field))
		} else {
			out = appendUniqueString(out, lt.ToLoki(field))
		}
	}
	return out
}

// isMappingName reports a configured loki_label that reads field.
func (lt *LabelTranslator) isMappingName(name, field string) bool {
	return lt.lokiToVL[name] == field && slices.ContainsFunc(lt.mappings, func(m FieldMapping) bool {
		return m.LokiLabel == name && m.VLField == field
	})
}

// declaredLabelNamesOf keeps the -stream-fields and -extra-label-fields names
// as the operator spelled them.
func declaredLabelNamesOf(streamFields, extraLabelFields []string) []string {
	var out []string
	for _, name := range slices.Concat(streamFields, extraLabelFields) {
		out = appendUniqueString(out, name)
	}
	return out
}
