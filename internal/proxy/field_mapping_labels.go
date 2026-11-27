package proxy

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// FieldMappingLabelMode controls which names of a -field-mapping field the
// label listings show (/labels, detected_labels, the volume target labels).
// Queries and /label/{name}/values accept every name of a mapping in every
// mode, and stream and series labels always carry one name per field.
type FieldMappingLabelMode string

const (
	// FieldMappingLabelModeOriginal is the default: one name per field in every
	// answer, the loki_label of the field's last mapping.
	FieldMappingLabelModeOriginal FieldMappingLabelMode = "original"
	// FieldMappingLabelModeAlias lists a mapped field under its aliases (the
	// loki_labels that differ from the name -label-style gives the field), and a
	// field without an alias under that name.
	FieldMappingLabelModeAlias FieldMappingLabelMode = "alias"
	// FieldMappingLabelModeBoth lists the aliases and the -label-style name.
	FieldMappingLabelModeBoth FieldMappingLabelMode = "both"
)

// ParseFieldMappingLabelMode reads a -field-mapping-label-mode value. Empty is
// the default.
func ParseFieldMappingLabelMode(value string) (FieldMappingLabelMode, error) {
	switch mode := FieldMappingLabelMode(strings.TrimSpace(value)); mode {
	case "":
		return FieldMappingLabelModeOriginal, nil
	case FieldMappingLabelModeOriginal, FieldMappingLabelModeAlias, FieldMappingLabelModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid -field-mapping-label-mode: %q (must be 'original', 'alias', or 'both')", value)
	}
}

// SetFieldMappingLabelMode applies mode to the mappings. It is called once,
// before the translator is shared. The original mode changes nothing.
//
// In the alias and both modes, for each mapped field:
//   - its base name is what -label-style gives it without a mapping;
//   - its aliases are the loki_labels of its mappings that differ from the base
//     name and resolve back to it (a loki_label given to two fields belongs to
//     the later mapping), in -field-mapping order;
//   - answers (stream and series labels) carry the last alias, or the base name
//     when there is none;
//   - listings show the aliases (alias mode) or the aliases and the base name
//     (both mode), or the base name when there is no alias;
//   - the base name becomes a query name of the field, unless a mapping or
//     another mapped field already owns it.
func (lt *LabelTranslator) SetFieldMappingLabelMode(mode FieldMappingLabelMode) {
	if mode != FieldMappingLabelModeAlias && mode != FieldMappingLabelModeBoth {
		return
	}
	lt.labelMode = mode
	lt.listing = make(map[string][]string, len(lt.vlToLoki))
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
		if len(own) == 0 {
			lt.vlToLoki[field] = base
			lt.listing[field] = []string{base}
			continue
		}
		lt.vlToLoki[field] = own[len(own)-1]
		listed := slices.Clone(own)
		if mode == FieldMappingLabelModeBoth && owned && owner == field {
			listed = append(listed, base)
		}
		lt.listing[field] = listed
	}
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
	if !lt.listsAliases() {
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
// (isAliasLabel), "" for any other label and in the original mode.
func (lt *LabelTranslator) aliasField(label string) string {
	if !lt.listsAliases() {
		return ""
	}
	field, ok := lt.lokiToVL[label]
	if !ok {
		return ""
	}
	if answer, ok := lt.aliasAnswer(field); !ok || answer != label {
		return ""
	}
	return field
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
// both modes; the original mode leaves them nil and its text unchanged.
func (lt *LabelTranslator) withAliases(prof lower.Profile) lower.Profile {
	if lt.listsAliases() {
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
		if lt.isMappingName(declared, field) {
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
