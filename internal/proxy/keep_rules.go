package proxy

import (
	"slices"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// keepRule is one | keep stage of a log query. Loki's keep leaves a label when
// the stage names it bare, or when one of the stage's matchers on that name
// matches its value, and removes every other label of the entry, stream label,
// structured metadata and parsed alike; the error labels are never removed.
type keepRule struct {
	// stage is the query's keep stage: its bare names, and its matchers when each is = or != (read from the query,
	// no copy); conds are the matchers' conditions when one is a regexp (regexp set).
	stage  *logqlpkg.KeepStage
	conds  []translate.DropCondition
	regexp bool
	// parserAfter says a parser stage follows, whose labels the keep does not
	// see; adderAfter says a parser or a label_format follows, so a label of
	// an entry that the keep does not name may have been added after it.
	parserAfter, adderAfter bool
}

// keepRules are the | keep stages of a log query, in stage order.
type keepRules []keepRule

// keepRulesOf reads the keep stages of a pipeline; nil without one.
func keepRulesOf(pipeline []logqlpkg.Stage) keepRules {
	var rules keepRules
	for _, stage := range pipeline {
		switch s := stage.(type) {
		case *logqlpkg.KeepStage:
			rule := keepRule{stage: s}
			if slices.ContainsFunc(s.Matchers, func(m logqlpkg.DropMatcher) bool { return m.Op != "=" && m.Op != "!=" }) {
				rule.regexp = true
				for _, m := range s.Matchers {
					if dc, err := translate.NewDropCondition(m.Name, m.Op, m.Value); err == nil {
						rule.conds = append(rule.conds, dc)
					}
				}
			}
			rules = append(rules, rule)
		case *logqlpkg.ParserStage:
			for i := range rules {
				rules[i].parserAfter, rules[i].adderAfter = true, true
			}
		case *logqlpkg.LabelFormatStage:
			for i := range rules {
				rules[i].adderAfter = true
			}
		}
	}
	return rules
}

// names reports whether the stage names name, bare or with a matcher.
func (k *keepRule) names(name string) bool {
	if slices.Contains(k.bare(), name) {
		return true
	}
	for _, m := range k.eq() {
		if m.Name == name {
			return true
		}
	}
	for i := range k.conds {
		if k.conds[i].Field == name {
			return true
		}
	}
	return false
}

// matched reports whether the stage has matchers.
func (k *keepRule) matched() bool { return len(k.eq()) > 0 || len(k.conds) > 0 }

// bare are the names the stage lists bare.
func (k *keepRule) bare() []string {
	if k.stage == nil {
		return nil
	}
	return k.stage.Labels
}

// eq are the stage's matchers when each is = or != (nil when conds holds them).
func (k *keepRule) eq() []logqlpkg.DropMatcher {
	if k.stage == nil || k.regexp {
		return nil
	}
	return k.stage.Matchers
}

// keeps reports whether the stage leaves the label name with value.
func (k *keepRule) keeps(name, value string) bool {
	if isSpecialKeepLabel(name) || slices.Contains(k.bare(), name) {
		return true
	}
	for _, m := range k.eq() {
		if m.Name == name && (m.Value == value) == (m.Op == "=") {
			return true
		}
	}
	for i := range k.conds {
		if k.conds[i].Field == name && k.conds[i].Matches(value) {
			return true
		}
	}
	return false
}

// rejects reports whether a matcher of the stage names name and none matches
// value, with name not listed bare.
func (k *keepRule) rejects(name, value string) bool {
	return !k.keeps(name, value) && k.names(name)
}

// keepsSpelled is keeps for a label with two spellings, the stored one and the
// one the response carries: a stage that names either decides by it.
func (k *keepRule) keepsSpelled(name, other, value string) bool {
	return k.keeps(name, value) || other != name && k.keeps(other, value)
}

// removesLevel reports a stored level, no stream label, that a keep stage
// removes: it is no stream label of the entry. A parser after the stage reads
// the line's level as a label of its own, which the entry's parsed labels hold.
func (rules keepRules) removesLevel(value []byte) bool {
	for i := range rules {
		if !rules[i].keeps("level", string(value)) {
			return true
		}
	}
	return false
}

// hidesRowLevel reports a stored level, no stream label, that is no stream
// label of a classified entry: in the Loki-compatible profile (a non-nil
// exposure) a keep stage removes it, or the pipeline does not expose the
// line's key (hidesStoredLevelOf).
func (e *lineFieldExposure) hidesRowLevel(rules keepRules, value string, storedLine []byte) bool {
	return e != nil && rules.removesLevel([]byte(value)) || e.hidesStoredLevelOf(storedLine)
}

// entry applies the keep stages to an entry's structured metadata and parsed
// labels. VictoriaLogs projects the fields a stage does not name away, which
// is all a bare stage does here. A stage with matchers and no parser after it
// decides every parsed label; one with a parser after it saw none of them.
// Where a later stage may have added a label, only a rejected matcher removes
// it.
func (rules keepRules) entry(sm, pf map[string]string, lt *LabelTranslator) {
	for i := range rules {
		rule := &rules[i]
		if rule.adderAfter || !rule.matched() {
			// The stored level and name_extracted base fields the projection
			// retains for the proxy are no labels the stage keeps.
			keepEntryFields(rule.bare(), sm, pf)
		}
		if !rule.matched() {
			// The projection removed every other field of a bare keep.
			continue
		}
		rule.filter(sm, !rule.adderAfter, lt)
		if !rule.parserAfter {
			rule.filter(pf, !rule.adderAfter, lt)
		}
	}
}

func (k *keepRule) filter(fields map[string]string, final bool, lt *LabelTranslator) {
	for name, value := range fields {
		other := name
		if lt != nil && !lt.IsPassthrough() {
			other = lt.ToLoki(name)
		}
		if final {
			if !k.keepsSpelled(name, other, value) {
				delete(fields, name)
			}
		} else if k.rejects(name, value) || other != name && k.rejects(other, value) {
			delete(fields, name)
		}
	}
}
