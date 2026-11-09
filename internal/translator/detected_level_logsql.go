package translator

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"

// The builders of Loki's detected_level over VictoriaLogs rows (the chain of __dl_*, __j_* and __l_* pipes, the
// stored-level filters and the filters on the derived value) live in internal/translate. The string translator
// forwards to them under the names the proxy has always called, so its output is unchanged.

// BracedLineRegexp matches the start of a line Loki's ingest reads as JSON.
const BracedLineRegexp = translate.BracedLineRegexp

// DetectedLevelFieldNames lists the stored fields that decide detected_level, in priority order.
var DetectedLevelFieldNames = translate.DetectedLevelFieldNames

// DetectedLevelChainMode selects how BuildDetectedLevelChain ends.
type DetectedLevelChainMode = translate.DetectedLevelChainMode

const (
	// DetectedLevelChainGroup keeps the result in __dl for stats/hits grouping.
	DetectedLevelChainGroup = translate.DetectedLevelChainGroup
	// DetectedLevelChainLogs writes the result to detected_level for rows without a stored level.
	DetectedLevelChainLogs = translate.DetectedLevelChainLogs
	// DetectedLevelChainGrouping writes Loki's value of every row into detected_level.
	DetectedLevelChainGrouping = translate.DetectedLevelChainGrouping
)

// DetectedLevelPresence names the level fields and body keys a window holds.
type DetectedLevelPresence = translate.DetectedLevelPresence

// SharedUnpack names the JSON keys a stats plan unpacks itself.
type SharedUnpack = translate.SharedUnpack

// DetectedLevelFilterPlan is the LogsQL form of `| detected_level <op> "<value>"`.
type DetectedLevelFilterPlan = translate.DetectedLevelFilterPlan

// DetectedLevelFilterMode selects where BuildDetectedLevelFilter keeps the value it filters on.
type DetectedLevelFilterMode = translate.DetectedLevelFilterMode

const (
	DetectedLevelFilterLogs   = translate.DetectedLevelFilterLogs
	DetectedLevelFilterMetric = translate.DetectedLevelFilterMetric
	DetectedLevelFilterStored = translate.DetectedLevelFilterStored
)

// LevelWitnessField is the field the proxy sets on a row holding a level field or body key a narrowed chain does
// not read.
const LevelWitnessField = translate.LevelWitnessField

const (
	groupChainDelete = translate.GroupChainDelete
	levelChainEnd    = translate.LevelChainEnd
)

// LevelKeywordGate returns the filter in front of the keyword regexp.
func LevelKeywordGate() string { return translate.LevelKeywordGate() }

// DetectedLevelStoredFields returns the level-deciding fields present in existingFields, in priority order.
func DetectedLevelStoredFields(existingFields []string) []string {
	return translate.DetectedLevelStoredFields(existingFields)
}

// DetectedLevelBodyKeys lists the body keys Loki reads a level from, in the order the chain reads them.
func DetectedLevelBodyKeys() []string { return translate.DetectedLevelBodyKeys() }

// BuildDetectedLevelChain returns the body chain (starting with "| ").
func BuildDetectedLevelChain(existingFields []string, mode DetectedLevelChainMode, present *DetectedLevelPresence) string {
	return translate.BuildDetectedLevelChain(existingFields, mode, present)
}

// BuildDetectedLevelChainShared is BuildDetectedLevelChain for a plan that unpacks Keys itself.
func BuildDetectedLevelChainShared(existingFields []string, mode DetectedLevelChainMode, present *DetectedLevelPresence, shared *SharedUnpack) string {
	return translate.BuildDetectedLevelChainShared(existingFields, mode, present, shared)
}

// NormalizeStoredLevel is the detected_level of a row whose first stored level field holds value.
func NormalizeStoredLevel(value string) string { return translate.NormalizeStoredLevel(value) }

// BuildStoredLevelCopyFilter returns the pipes that keep the rows a filter on detected_level can match.
func BuildStoredLevelCopyFilter(stored, meta string, filters []DropCondition) string {
	return translate.BuildStoredLevelCopyFilter(stored, meta, filters)
}

// BuildHasLevelFilter is the filter of the rows that hold a stored level field.
func BuildHasLevelFilter(existingFields []string) string {
	return translate.BuildHasLevelFilter(existingFields)
}

// BuildStoredLevelFilter is the filter on the stored level fields for `| detected_level <op> "<value>"`.
func BuildStoredLevelFilter(op, value string, existingFields []string) (string, error) {
	return translate.BuildStoredLevelFilter(op, value, existingFields)
}

// BuildDetectedLevelFilter plans `| detected_level <op> "<value>"`.
func BuildDetectedLevelFilter(op, value string, existingFields []string, mode DetectedLevelFilterMode) (DetectedLevelFilterPlan, error) {
	return translate.BuildDetectedLevelFilter(op, value, existingFields, mode)
}

// JoinDetectedLevelFilters returns the pipes of consecutive detected_level filters built for mode.
func JoinDetectedLevelFilters(plans []DetectedLevelFilterPlan, existingFields []string, mode DetectedLevelFilterMode) string {
	return translate.JoinDetectedLevelFilters(plans, existingFields, mode)
}

// DetectedLevelProbeBase returns the rows a probe of the level fields of base may read.
func DetectedLevelProbeBase(base string) string { return translate.DetectedLevelProbeBase(base) }

// Operator and Operand are the condition as translate.LevelCondition reads it.
func (dc DropCondition) Operator() string { return dc.Op }

// Operand is the value the condition compares with.
func (dc DropCondition) Operand() string { return dc.Value }

func logsqlFieldName(name string) string { return translate.LogsQLFieldName(name) }
