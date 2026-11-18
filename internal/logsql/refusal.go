package logsql

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Refusal prediction: what VictoriaLogs answers a query with before it reads any data, decided from the query text
// as the proxy is about to send it, the endpoint and the running VictoriaLogs release, so the proxy can answer the
// refusal without the call. Every rule is VictoriaLogs' own (lib/logstorage and app/vlselect of the releases named
// in each gate) and is held to VictoriaLogs' answer by an oracle test against v1.49.0 and v1.53.0
// (internal/proxy vl_refusal_oracle_test.go). A rule only fires on a text the proxy's parser reads and on pipes
// it knows: anything else is sent and VictoriaLogs' answer classified, so a prediction never refuses a query
// VictoriaLogs would run.

// Version is a VictoriaLogs release.
type Version struct {
	Major, Minor, Patch int
	// Known is false for a release the proxy has not detected: nothing is predicted.
	Known bool
}

// ParseVersion reads a VictoriaLogs semver ("v1.53.0", "1.52.1"); an unreadable one is not Known.
func ParseVersion(s string) Version {
	maj, min, patch, ok := parseSemver(s)
	if !ok {
		return Version{}
	}
	return Version{Major: maj, Minor: min, Patch: patch, Known: true}
}

// String is the release as VictoriaLogs names it ("v1.53.0"), or "unknown".
func (v Version) String() string {
	if !v.Known {
		return "unknown"
	}
	return "v" + strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// Gate is a VictoriaLogs behaviour by release line: it holds from From on, and on an older release line from the
// first patch Lines names for it (an LTS line that got a fix in a patch release; v1.52.1 carries a fix v1.53.0
// also has, while v1.52.0 does not). Gating by line, not only by a minimum version, keeps a patch release of an
// older line from being read as the line's first release. No gate has a line entry today: the LTS patches that
// name fixes (v1.52.1, v1.52.2) are enterprise-only images the proxy's checks have not run against.
type Gate struct {
	From  [3]int
	Lines map[[2]int]int
}

// In reports whether release v has the behaviour.
func (g Gate) In(v Version) bool {
	if !v.Known {
		return false
	}
	if cmp3(v.Major, v.Minor, v.Patch, g.From) >= 0 {
		return true
	}
	if patch, ok := g.Lines[[2]int{v.Major, v.Minor}]; ok {
		return v.Patch >= patch
	}
	return false
}

func cmp3(maj, min, patch int, w [3]int) int {
	switch {
	case maj != w[0]:
		return maj - w[0]
	case min != w[1]:
		return min - w[1]
	}
	return patch - w[2]
}

// The release gates of the predicted refusals (VictoriaLogs CHANGELOG and lib/logstorage of each release).
var (
	// GatePipeCoalesce: the coalesce pipe (v1.51.0, #690). Before it `| coalesce (...)` is refused as an
	// unexpected pipe.
	GatePipeCoalesce = Gate{From: [3]int{1, 51, 0}}
	// GatePipeJSONArrayConcat: the json_array_concat pipe (v1.52.0, #712). v1.49 and v1.50 read
	// `| json_array_concat ...` as a filter pipe and answer it; it is never predicted.
	GatePipeJSONArrayConcat = Gate{From: [3]int{1, 52, 0}}
	// GateStatsLimitAfterStats: limit and offset pipes after the last stats pipe of a stats_query (v1.51.0,
	// #1296). Before it they are refused as pipes that cannot be put after stats.
	GateStatsLimitAfterStats = Gate{From: [3]int{1, 51, 0}}
	// GateRangeTimeWriters: on stats_query_range, a pipe in front of the last stats pipe that writes _time (math
	// ... as _time, replace ... at _time, unpack_json/unpack_logfmt of _time, unroll of _time, extract of
	// <_time>, collapse_nums at _time) is refused (v1.53.0, its canReturnLastNResults checks). v1.49 to v1.52.x
	// answer such a query, bucketing by the overwritten _time.
	GateRangeTimeWriters = Gate{From: [3]int{1, 53, 0}}
	// GateMathQuotedConstantClone: a math pipe holding a quoted number ("2025-01-01T00:00:00Z") keeps its quotes
	// when VictoriaLogs prints the query (v1.53.0). Before it, a query that VictoriaLogs clones (the limit query
	// arg of /select/logsql/query, live tailing, a cluster's remote query) fails its own reparse and VictoriaLogs
	// panics: the connection drops without an answer and the VictoriaLogs process exits (probed on the published v1.49.0 and
	// v1.52.0 images). The v1.52.1 and v1.52.2 LTS releases, whose notes name the fix, exist only as enterprise
	// images and their tags' source lacks it: the whole v1.52 line is guarded, as a backend crash costs more than
	// refusing that one query on an LTS build that has the fix.
	GateMathQuotedConstantClone = Gate{From: [3]int{1, 53, 0}}
	// GateExtraFiltersLen: -search.maxQueryLen also bounds each extra_filters and extra_stream_filters arg (v1.54.0,
	// #1923; app/vlselect/logsql parseExtraFilters; probed on the published v1.54.0 image, v1.52.0 and v1.53.0 answer
	// such a call). The v1.52.2 LTS release that names the same fix is enterprise-only and not gated in.
	GateExtraFiltersLen = Gate{From: [3]int{1, 54, 0}}
)

// The VictoriaLogs query endpoints a refusal is predicted on.
const (
	PathQuery           = "/select/logsql/query"
	PathStatsQuery      = "/select/logsql/stats_query"
	PathStatsQueryRange = "/select/logsql/stats_query_range"
	PathTail            = "/select/logsql/tail"
)

// RefusalInput is a call about to be sent to VictoriaLogs.
type RefusalInput struct {
	Path  string
	Query string
	// Limit: a /select/logsql/query call with a limit query arg above zero (VictoriaLogs clones such a query).
	Limit bool
	// MaxQueryLen is VictoriaLogs' -search.maxQueryLen in bytes; zero when the proxy does not know it.
	MaxQueryLen int
	// ExtraFilters and ExtraStreamFilters are the call's extra_filters and extra_stream_filters args (the
	// proxy's tenant and exclusion scoping).
	ExtraFilters, ExtraStreamFilters []string
}

// Refusal is VictoriaLogs' predicted answer to a call: the registry item of its rule, the status VictoriaLogs
// answers with, and a message its error classification reads as VictoriaLogs' own. Crash marks a call VictoriaLogs
// does not answer (its handler panics and the connection drops): no status is written then.
type Refusal struct {
	Rule    string
	Status  int
	Message string
	Crash   bool
}

// The registry items (conformance/registry/vl/capabilities) of the predicted refusals.
const (
	RuleQueryTooLong        = "vl-query-too-long"
	RuleExtraFiltersTooLong = "vl-extra-filters-too-long"
	RulePipeBeforeRelease   = "vl-pipe-before-release"
	RuleStatsMissingStats   = "vl-stats-missing-stats-pipe"
	RuleRangePipeBeforeStat = "vl-stats-range-pipe-before-stats"
	RuleStatsPipeAfterStats = "vl-stats-pipe-after-stats"
	RuleMathQuotedClone     = "vl-math-quoted-constant-clone-crash"
)

// RefusalRules lists every rule, for metrics.
func RefusalRules() []string {
	return []string{RuleQueryTooLong, RuleExtraFiltersTooLong, RulePipeBeforeRelease, RuleStatsMissingStats, RuleRangePipeBeforeStat, RuleStatsPipeAfterStats, RuleMathQuotedClone}
}

// PredictRefusal returns VictoriaLogs' answer to in on release v when VictoriaLogs refuses it before reading any
// data. It reads the text with the proxy's LogsQL parser only after an allocation-free screen finds a construct a
// rule can fire on; a text the parser does not read and a pipe no rule knows predict nothing, and so does an
// unknown release, except for a call its handler would crash on (guarded until the release is known to be fixed).
//
// TODO(capability layer): take the layer's gate set instead of a Version once it lands; the gates below move into
// its table unchanged.
func PredictRefusal(in RefusalInput, v Version) (Refusal, bool) {
	status := refusalStatus(in.Path)
	if status == 0 {
		return Refusal{}, false
	}
	if !v.Known {
		// Nothing is predicted for an unknown release but a crash: a release that may lack the fix is guarded,
		// as a crash takes VictoriaLogs down.
		return predictCloneCrash(in, v)
	}
	if in.MaxQueryLen > 0 && len(in.Query) > in.MaxQueryLen && in.Path != PathTail {
		// app/vlselect/logsql parseQueryFromRequest checks the length before parsing.
		return Refusal{Rule: RuleQueryTooLong, Status: status, Message: fmt.Sprintf(
			"the `query` arg length cannot exceed -search.maxQueryLen=%d bytes; the current query length is %d bytes", in.MaxQueryLen, len(in.Query))}, true
	}
	screen := screenRefusal(in, v)
	if screen == 0 {
		return extraFiltersTooLong(in, v, status)
	}
	q, err := Parse(in.Query)
	if err != nil {
		return Refusal{}, false
	}
	if screen&screenPipes != 0 {
		for _, p := range q.Pipes {
			if _, ok := p.(PipeCoalesce); ok && !GatePipeCoalesce.In(v) {
				return Refusal{Rule: RulePipeBeforeRelease, Status: status, Message: `cannot parse ` + "`query`" + ` arg: unexpected pipe "coalesce"`}, true
			}
		}
	}
	if r, ok := extraFiltersTooLong(in, v, status); ok {
		return r, true
	}
	if screen&screenStats != 0 {
		if r, ok := predictStatsRefusal(q, in.Path == PathStatsQueryRange, v); ok {
			return r, true
		}
	}
	if screen&screenClone != 0 {
		return cloneCrashIn(q)
	}
	return Refusal{}, false
}

// predictCloneCrash is the clone crash rule alone (GateMathQuotedConstantClone).
func predictCloneCrash(in RefusalInput, v Version) (Refusal, bool) {
	if screenRefusal(in, v)&screenClone == 0 {
		return Refusal{}, false
	}
	q, err := Parse(in.Query)
	if err != nil {
		return Refusal{}, false
	}
	return cloneCrashIn(q)
}

// cloneCrashIn finds a math pipe of q holding a quoted constant VictoriaLogs cannot read back.
func cloneCrashIn(q *Query) (Refusal, bool) {
	for _, p := range q.Pipes {
		if m, ok := p.(PipeMath); ok && mathHasQuotedNumber(m.Expr) {
			return Refusal{Rule: RuleMathQuotedClone, Crash: true}, true
		}
	}
	return Refusal{}, false
}

// extraFiltersTooLong is VictoriaLogs' refusal of an extra_filters or extra_stream_filters arg longer than its
// -search.maxQueryLen (checked after the query parses, in that order).
func extraFiltersTooLong(in RefusalInput, v Version, status int) (Refusal, bool) {
	if in.MaxQueryLen <= 0 || in.Path == PathTail || !GateExtraFiltersLen.In(v) {
		return Refusal{}, false
	}
	for _, arg := range [...]struct {
		name   string
		values []string
	}{{"extra_filters", in.ExtraFilters}, {"extra_stream_filters", in.ExtraStreamFilters}} {
		for _, s := range arg.values {
			if len(s) > in.MaxQueryLen {
				return Refusal{Rule: RuleExtraFiltersTooLong, Status: status, Message: fmt.Sprintf(
					"the `%s` arg length cannot exceed -search.maxQueryLen=%d bytes; the current length is %d bytes", arg.name, in.MaxQueryLen, len(s))}, true
			}
		}
	}
	return Refusal{}, false
}

// refusalStatus is the status VictoriaLogs refuses a query with on path: 400 from httpserver.Errorf on the log
// query endpoints, 422 from httpserver.SendPrometheusError on the stats ones; zero for a path no rule covers.
func refusalStatus(path string) int {
	switch path {
	case PathQuery, PathTail:
		return http.StatusBadRequest
	case PathStatsQuery, PathStatsQueryRange:
		return http.StatusUnprocessableEntity
	}
	return 0
}

// clonesQuery reports a call whose query VictoriaLogs may clone (print and parse again) before running it: the limit
// arg and a query ending in the last-N shape on /select/logsql/query, live tailing, the time filter clone of
// stats_query_range and hits, and a cluster's remote query, which clones every query. Every query path counts, so
// the guard holds whatever VictoriaLogs runs behind the proxy (the oracle's roundtrip command clones the same way).
func clonesQuery(in RefusalInput) bool {
	return refusalStatus(in.Path) != 0
}

// What screenRefusal found worth parsing for.
const (
	screenPipes = 1 << iota
	screenStats
	screenClone
)

// screenRefusal is the cheap test of every rule but the length: which rules may fire on in, read from its pipe
// names alone, without allocating.
func screenRefusal(in RefusalInput, v Version) int {
	var screen int
	if !GatePipeCoalesce.In(v) && strings.Contains(in.Query, "coalesce") {
		screen |= screenPipes
	}
	if in.Path == PathStatsQuery || in.Path == PathStatsQueryRange {
		if statsShapeMayRefuse(in.Query, in.Path == PathStatsQueryRange, v) {
			screen |= screenStats
		}
	}
	if clonesQuery(in) && !GateMathQuotedConstantClone.In(v) && strings.Contains(in.Query, "math") && strings.ContainsAny(in.Query, "\"`'") {
		screen |= screenClone
	}
	return screen
}

// statsShapeMayRefuse reads the pipe names of query (each pipe's first word, outside quotes and brackets) and
// reports a shape GetStatsLabelsAddGroupingByTime may refuse: no stats pipe; on a range query a pipe ahead of the
// last stats pipe that is never allowed there, or one that may write or drop _time; a pipe after it that is never
// allowed there.
func statsShapeMayRefuse(query string, rangeQuery bool, v Version) bool {
	var names [64]pipeWord
	n, ok := pipeWords(query, names[:])
	if !ok {
		return false // more pipes than the screen keeps: the parser decides
	}
	last := -1
	for i := 0; i < n; i++ {
		if names[i].name == "stats" {
			last = i
		}
	}
	if last < 0 {
		return true
	}
	if rangeQuery {
		for i := 0; i < last; i++ {
			w := names[i]
			switch frontRule(w.name, v) {
			case frontRefused:
				return true
			case frontTime:
				if strings.Contains(w.text, "_time") {
					return true
				}
			case frontFields:
				if !strings.Contains(w.text, "_time") && !strings.Contains(w.text, "*") {
					return true
				}
			}
		}
	}
	for i := last + 1; i < n; i++ {
		if !afterStatsAllowedName(names[i].name, rangeQuery, v) {
			return true
		}
	}
	return false
}

// pipeWord is one pipe of a query: its first word and its whole text.
type pipeWord struct{ name, text string }

// pipeWords splits query at its pipe bars outside quotes, brackets and parentheses into out, and reports how many
// it wrote; ok is false when out is too short. The query's filter part is not a pipe.
func pipeWords(query string, out []pipeWord) (int, bool) {
	n, depth := 0, 0
	start := -1
	var quote byte
	flush := func(end int) bool {
		if start < 0 {
			return true
		}
		if n == len(out) {
			return false
		}
		text := strings.TrimSpace(query[start:end])
		j := 0
		for j < len(text) && (text[j] == '_' || text[j] >= 'a' && text[j] <= 'z' || text[j] >= 'A' && text[j] <= 'Z' || text[j] >= '0' && text[j] <= '9') {
			j++
		}
		out[n] = pipeWord{name: strings.ToLower(text[:j]), text: text}
		n++
		return true
	}
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case quote != 0:
			if c == '\\' && quote != '`' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case (c == ')' || c == ']' || c == '}') && depth > 0:
			depth--
		case c == '|' && depth == 0:
			if !flush(i) {
				return 0, false
			}
			start = i + 1
		}
	}
	if !flush(len(query)) {
		return 0, false
	}
	return n, true
}

// How a pipe ahead of the last stats pipe of a range query is read.
const (
	frontAllowed = iota
	frontRefused // never allowed there
	frontTime    // refused when it writes or drops _time
	frontFields  // the fields pipe: refused unless it keeps _time
)

// frontRule is how VictoriaLogs v reads a pipe named name ahead of the last stats pipe of a range query (its
// canReturnLastNResults).
func frontRule(name string, v Version) int {
	switch name {
	case "total_stats", "running_stats", "sort", "order", "limit", "head", "offset", "skip", "first", "last", "top", "uniq",
		"sample", "field_names", "field_values", "facets", "join", "union", "stream_context", "generate_sequence",
		"query_stats", "block_stats", "blocks_count":
		return frontRefused
	case "fields", "keep":
		return frontFields
	case "copy", "cp", "delete", "del", "rm", "drop", "rename", "mv", "format", "hash", "len", "json_array_len",
		"pack_json", "pack_logfmt", "split", "unpack_words", "coalesce", "json_array_concat":
		return frontTime
	case "math", "eval", "replace", "replace_regexp", "unpack_json", "unpack_logfmt", "unroll", "extract",
		"extract_regexp", "collapse_nums":
		if GateRangeTimeWriters.In(v) {
			return frontTime
		}
	}
	return frontAllowed
}

// afterStatsAllowedName reports a pipe name GetStatsLabels lets follow the last stats pipe on v (a pipe the screen
// does not name is a filter pipe written without its keyword, which is allowed).
func afterStatsAllowedName(name string, rangeQuery bool, v Version) bool {
	switch name {
	case "limit", "head", "offset", "skip":
		return !rangeQuery && GateStatsLimitAfterStats.In(v)
	case "unpack_logfmt", "extract", "extract_regexp", "replace", "replace_regexp", "hash", "len", "json_array_len",
		"pack_json", "pack_logfmt", "split", "unpack_words", "coalesce", "json_array_concat", "top", "uniq", "sample",
		"field_names", "field_values", "facets", "join", "union", "stream_context", "unroll", "collapse_nums",
		"decolorize", "drop_empty_fields", "set_stream_fields", "time_add", "unpack_syslog", "stats":
		return false
	}
	return true
}

// predictStatsRefusal applies GetStatsLabelsAddGroupingByTime's checks to q, in its order: a stats pipe, then on a
// range query the pipes ahead of the last stats pipe, then the pipes after it.
func predictStatsRefusal(q *Query, rangeQuery bool, v Version) (Refusal, bool) {
	if mergesLimits(q.Pipes) {
		return Refusal{}, false
	}
	last := -1
	for i, p := range q.Pipes {
		if _, ok := p.(PipeStats); ok {
			last = i
		}
	}
	if last < 0 {
		return Refusal{Rule: RuleStatsMissingStats, Status: http.StatusUnprocessableEntity, Message: "missing `| stats ...` pipe in the query"}, true
	}
	stats := q.Pipes[last]
	if rangeQuery {
		for _, p := range q.Pipes[:last] {
			if _, ok := p.(PipeStats); ok {
				continue
			}
			refused, known := frontPipeRefused(p, v)
			if !known {
				return Refusal{}, false
			}
			if refused {
				return Refusal{Rule: RuleRangePipeBeforeStat, Status: http.StatusUnprocessableEntity, Message: fmt.Sprintf(
					"the pipe `| %q` cannot be put in front of `| %q`, since it may modify or delete `_time` field", pipeBody(p), pipeBody(stats))}, true
			}
		}
	}
	for _, p := range q.Pipes[last+1:] {
		allowed, known := afterStatsPipeAllowed(p, rangeQuery, v)
		if !known {
			return Refusal{}, false
		}
		if allowed {
			continue
		}
		msg := fmt.Sprintf("the %q pipe cannot be put after %q pipe in the query", pipeBody(p), pipeBody(stats))
		if rangeQuery && GateStatsLimitAfterStats.In(v) && isLimitOrOffset(p) {
			msg = fmt.Sprintf("the %s pipe isn't allowed in range queries, since it cannot be applied individually for each step", pipeBody(p))
		}
		return Refusal{Rule: RuleStatsPipeAfterStats, Status: http.StatusUnprocessableEntity, Message: msg}, true
	}
	return Refusal{}, false
}

// mergesLimits reports pipes VictoriaLogs' optimizer rewrites before it checks a stats query
// (lib/logstorage optimizeOffsetLimitPipes, optimizeSortLimitPipes, optimizeSortOffsetPipes, optimizeUniqLimitPipes):
// a limit or offset next to a sort, uniq, limit or offset (merged into it, swapped or dropped), or a limit or offset
// of zero. The shape VictoriaLogs checks is then not the one written, so no stats rule predicts on it.
func mergesLimits(pipes []Pipe) bool {
	merges := func(p Pipe) bool {
		switch p.(type) {
		case PipeSort, PipeUniq, PipeLimit, PipeOffset:
			return true
		}
		return false
	}
	for i, p := range pipes {
		switch t := p.(type) {
		case PipeLimit:
			if t.N == 0 {
				return true
			}
		case PipeOffset:
			if t.N == 0 {
				return true
			}
		default:
			continue
		}
		if i > 0 && merges(pipes[i-1]) || i+1 < len(pipes) && merges(pipes[i+1]) {
			return true
		}
	}
	return false
}

func isLimitOrOffset(p Pipe) bool {
	switch p.(type) {
	case PipeLimit, PipeOffset:
		return true
	}
	return false
}

// pipeBody is a pipe's text without its leading bar.
func pipeBody(p Pipe) string {
	return strings.TrimSpace(strings.TrimPrefix(p.String(), "|"))
}

// frontPipeRefused reports whether VictoriaLogs v refuses p ahead of the last stats pipe of a range query (its
// canReturnLastNResults is false); known is false for a pipe no rule reads, which predicts nothing.
func frontPipeRefused(p Pipe, v Version) (refused, known bool) {
	switch p.(type) {
	case PipeFilter, PipeDecolorize, PipeDropEmptyFields, PipeSetStreamFields, PipeTimeAdd, PipeUnpackSyslog:
		return false, true
	case PipeTotalStats, PipeRunningStats, PipeSort, PipeLimit, PipeOffset, PipeFirst, PipeLast, PipeTop, PipeUniq,
		PipeSample, PipeFieldNames, PipeFieldValues, PipeFacets, PipeJoin, PipeUnion, PipeStreamContext,
		PipeGenerateSequence, PipeQueryStats:
		return true, true
	}
	if field, ok := frontResultField(p); ok {
		// A pipe writing one field: refused when it writes _time; a written field the pipe defaults (an empty one)
		// is not read.
		if field.gated && !GateRangeTimeWriters.In(v) {
			return false, true
		}
		return unquoteName(field.name) == "_time", field.name != "" || !field.optional
	}
	return frontListPipeRefused(p, v)
}

// resultField is the field a pipe writes: gated when only GateRangeTimeWriters releases refuse it, optional when
// an empty name means a default the rule does not read.
type resultField struct {
	name            string
	gated, optional bool
}

// frontResultField is the one field p writes, for the pipes that write one.
func frontResultField(p Pipe) (resultField, bool) {
	switch t := p.(type) {
	case PipeFormat:
		return resultField{name: t.ResultField}, true
	case PipeHash:
		return resultField{name: t.Result}, true
	case PipeLen:
		return resultField{name: t.Result}, true
	case PipeJSONArrayLen:
		return resultField{name: t.Result}, true
	case PipePackJSON:
		return resultField{name: t.ResultField}, true
	case PipePackLogfmt:
		return resultField{name: t.ResultField}, true
	case PipeSplit:
		return resultField{name: t.As, optional: true}, true
	case PipeUnpackWords:
		return resultField{name: t.As, optional: true}, true
	case PipeCoalesce:
		return resultField{name: t.Result}, true
	case PipeJSONArrayConcat:
		return resultField{name: t.As, optional: true}, true
	case PipeMath:
		return resultField{name: t.Alias, gated: true}, true
	case PipeReplace:
		return resultField{name: t.Field, gated: true}, true
	case PipeReplaceRegexp:
		return resultField{name: t.Field, gated: true}, true
	case PipeCollapseNums:
		return resultField{name: t.Field, gated: true}, true
	}
	return resultField{}, false
}

// frontListPipeRefused is frontPipeRefused for the pipes that name several fields.
func frontListPipeRefused(p Pipe, v Version) (refused, known bool) {
	writers := GateRangeTimeWriters.In(v)
	switch t := p.(type) {
	case PipeFields:
		for _, f := range t.Labels {
			if f = unquoteName(f); f == "_time" || strings.HasSuffix(f, "*") {
				return false, true
			}
		}
		return true, true
	case PipeDelete:
		return namesTime(t.Labels), !namesWildcard(t.Labels)
	case PipeCopy:
		return pairsWriteTime(t.Pairs, false)
	case PipeRename:
		return pairsWriteTime(t.Pairs, true)
	case PipeUnroll:
		return writers && namesTime(t.Fields), !namesWildcard(t.Fields)
	case PipeUnpackJSON:
		return writers && unpacksTime(t.Fields, t.ResultPrefix, t.KeepOriginalFields), true
	case PipeUnpackLogfmt:
		return writers && unpacksTime(t.Fields, t.ResultPrefix, t.KeepOriginalFields), true
	case PipeExtract:
		return writers && !t.KeepOriginalFields && strings.Contains(t.Pattern, "<_time>"), true
	case PipeExtractRegexp:
		return writers && !t.KeepOriginalFields && strings.Contains(t.Pattern, "(?P<_time>"), true
	}
	return false, false
}

// pairsWriteTime reports copy (or, with source, rename) pairs that write _time (or read it); a wildcard pair is
// not read.
func pairsWriteTime(pairs [][2]string, source bool) (refused, known bool) {
	for _, pair := range pairs {
		if strings.HasSuffix(pair[0], "*") || strings.HasSuffix(pair[1], "*") {
			return false, false
		}
		if unquoteName(pair[1]) == "_time" || source && unquoteName(pair[0]) == "_time" {
			return true, true
		}
	}
	return false, true
}

// unpacksTime is VictoriaLogs v1.53's canReturnLastNResultsAfterUnpack, negated: an unpack that writes _time from
// a field it names (a wildcard is not read, as VictoriaLogs does not).
func unpacksTime(fields []string, prefix string, keep bool) bool {
	if keep || !strings.HasPrefix("_time", prefix) {
		return false
	}
	for _, f := range fields {
		if prefix+unquoteName(f) == "_time" {
			return true
		}
	}
	return false
}

func namesTime(names []string) bool {
	for _, n := range names {
		if unquoteName(n) == "_time" {
			return true
		}
	}
	return false
}

func namesWildcard(names []string) bool {
	for _, n := range names {
		if strings.HasSuffix(n, "*") {
			return true
		}
	}
	return false
}

// afterStatsPipeAllowed reports whether VictoriaLogs v lets p follow the last stats pipe (GetStatsLabels' switch
// over the pipes after it); known is false for a pipe no rule reads.
func afterStatsPipeAllowed(p Pipe, rangeQuery bool, v Version) (allowed, known bool) {
	switch p.(type) {
	case PipeFilter, PipeFirst, PipeLast, PipeSort, PipeRunningStats, PipeTotalStats, PipeMath, PipeFields,
		PipeDelete, PipeCopy, PipeRename, PipeFormat, PipeUnpackJSON:
		return true, true
	case PipeLimit, PipeOffset:
		return !rangeQuery && GateStatsLimitAfterStats.In(v), true
	case PipeUnpackLogfmt, PipeExtract, PipeExtractRegexp, PipeReplace, PipeReplaceRegexp, PipeHash, PipeLen,
		PipeJSONArrayLen, PipePackJSON, PipePackLogfmt, PipeSplit, PipeUnpackWords, PipeCoalesce, PipeJSONArrayConcat,
		PipeTop, PipeUniq, PipeSample, PipeFieldNames, PipeFieldValues, PipeFacets, PipeJoin, PipeUnion,
		PipeStreamContext, PipeUnroll, PipeCollapseNums, PipeDecolorize, PipeDropEmptyFields, PipeSetStreamFields,
		PipeTimeAdd, PipeUnpackSyslog:
		return false, true
	}
	return false, false
}

// mathHasQuotedNumber reports a math expression holding a quoted token VictoriaLogs reads as a number and whose
// unquoted text its lexer splits (an RFC3339 time in double quotes, backticks or single quotes). VictoriaLogs
// before GateMathQuotedConstantClone prints such a token without its quotes, and the reprint does not parse; a
// quoted plain number ("1.5") reprints as itself and runs.
func mathHasQuotedNumber(expr string) bool {
	for i := 0; i < len(expr); i++ {
		q := expr[i]
		if q != '"' && q != '`' && q != '\'' {
			continue
		}
		j := i + 1
		for j < len(expr) && expr[j] != q {
			if expr[j] == '\\' && q != '`' {
				j++
			}
			j++
		}
		if j >= len(expr) {
			return false
		}
		tok := expr[i : j+1]
		s, err := strconv.Unquote(tok)
		if q == '\'' {
			s, err = expr[i+1:j], nil
		}
		if err == nil && quotedNumber(s) && splitsUnquoted(s) {
			return true
		}
		i = j
	}
	return false
}

// splitsUnquoted reports a number text VictoriaLogs' lexer would read as several tokens once printed without its
// quotes (an RFC3339 time: 2025-01-01T00:00:00Z reads as 2025 - 01 - 01T00:00:00Z); a plain number or duration
// reads back as itself, so its reprint parses and the clone succeeds.
func splitsUnquoted(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '.' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return true
		}
	}
	return false
}

// quotedNumber reports text VictoriaLogs' math pipe reads as a number constant: a decimal number, a
// duration or an RFC3339 timestamp (lib/logstorage pipe_math.go tryParseNumber, the forms the proxy can tell
// apart without VictoriaLogs' own parser; other forms predict nothing).
func quotedNumber(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	if _, err := time.ParseDuration(s); err == nil {
		return true
	}
	if _, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return true
	}
	return false
}
