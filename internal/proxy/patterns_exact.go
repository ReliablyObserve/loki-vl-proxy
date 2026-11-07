package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// DefaultPatternsExactMaxLines is -patterns-exact-max-lines: the most lines of a request's range the proxy reads
// from VictoriaLogs to mine Loki's patterns exactly. 0 turns the exact tier off.
const (
	DefaultPatternsExactMaxLines = 200_000
)

// patternExactCountQuery counts the lines of a stream selector's filter over the range.
func patternExactCountQuery(filter logsql.FilterExpr) *logsql.Query {
	return logsql.NewQuery(filter, logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}}})
}

// patternExactLinesQuery reads the lines of a stream selector's filter with what the miner needs of each, a stream
// after another and the lines of a stream oldest first, the order Loki's pattern ingester reads them in. Lines of a
// stream that share a time (a push's lines often do) come in their text's order: VictoriaLogs returns such ties in
// any order, and the order a stream's lines are mined in decides which cluster a line joins, so every read, rebuild
// and peer mines them alike.
func patternExactLinesQuery(filter logsql.FilterExpr) *logsql.Query {
	fields := []string{"_time", "_msg", patternExactStreamField}
	for _, field := range patternCountsLevelFields {
		if strings.Contains(field, ".") {
			field = logsql.QuoteValue(field)
		}
		fields = append(fields, field)
	}
	return logsql.NewQuery(filter,
		logsql.PipeFields{Labels: fields},
		logsql.PipeSort{By: []logsql.SortField{{Field: patternExactStreamField}, {Field: "_time"}, {Field: "_msg"}}},
	)
}

// patternExactStreamField names the stream of a line: the exact tier mines each stream apart, as Loki does.
const patternExactStreamField = "_stream_id"

// patternsFromLines answers /patterns by reading the lines of the range and mining them as Loki's pattern
// ingester does (exact tier): each stream at each level apart, oldest line first, the samples counted from the
// same lines as Loki counts them (10 s samples inside the range, summed into steps) with the steps between a
// cluster's first and last sample as 0. It reads them only when the range holds at most -patterns-exact-max-lines
// lines (one count query first).
//
// answered is false when the tier is off, the request is one it does not answer, the range holds more lines, or
// VictoriaLogs fails or answers something it cannot use (the sort of a very large range can exceed its memory): the
// caller then counts the lines, or samples them. A failed backend is never answered from. Once the lines were read,
// what they mined is the answer even if empty, as Loki's is; read is true then, and false for a range without lines
// (an answer that is not kept, as lines may be on their way).
func (p *Proxy) patternsFromLines(r *http.Request, query, startParam, endParam, stepParam string, patternLimit int) (entries []patternResultEntry, answered, read bool) {
	maxLines := p.patternsExactMaxLines
	if maxLines <= 0 {
		return nil, false, false
	}
	scope, ok := p.patternScope(query, startParam, endParam, stepParam)
	if !ok {
		return nil, false, false
	}
	if p.patternState != nil {
		// One peer holds a selector's state, so every peer answers it with the same patterns.
		if entries, ok := p.patternsFromOwner(r, scope); ok {
			return entries, true, true
		}
	}
	entries, fromNs, lines, ok := p.patternsFromState(r, scope, maxLines, patternLimit)
	if ok && (!p.patternsFullRange || len(entries) > 0 || scope.startNs >= fromNs) {
		// (With -patterns-full-range, a range the state has no pattern for, such as one before its samples or a
		// service quiet since, is mined from the range's own lines below, so its chart is not empty.)
		if p.patternsFullRange && scope.startNs < fromNs {
			// The patterns are Loki's, those with 30 lines in the state's part of the range; the part before it
			// is counted onto them.
			entries = dropPatternsBelowMinLines(entries, fromNs, scope.endNs)
			history := scope
			history.endNs = fromNs
			entries = p.addPatternHistory(r, history, entries)
		}
		return entries, true, true
	}
	ctx := r.Context()
	if lines < 0 { // not counted by the state
		var err error
		if lines, err = p.countPatternLines(ctx, scope, scope.startNs, scope.endNs); err != nil {
			return nil, false, false
		}
	}
	if lines > maxLines {
		return nil, false, false
	}
	if lines == 0 {
		return nil, true, false
	}
	miner, err := p.minePatternLines(ctx, scope, maxLines)
	if err != nil {
		return nil, false, false
	}
	return patternResultEntriesFromMaps(buildPatternResponse(miner, patternLimit)), true, true
}

// minePatternLines reads the lines of the scope, sorted by stream and time, and mines them as they come; no more
// than a line is held at a time, so memory is that of the clusters.
func (p *Proxy) minePatternLines(ctx context.Context, scope patternQueryScope, maxLines int) (*patternMiner, error) {
	miner := newPatternMiner()
	miner.oneStream = p.patternsSelectorScope
	miner.shortWholeLine = p.patternsSelectorScope
	miner.fillStep = int64(scope.step / time.Second)
	miner.sampleFromNs, miner.sampleToNs = scope.startNs, scope.endNs
	read := newPatternLineReader(miner, p.newLogRowLevels())
	err := p.readPatternCountRows(ctx, patternExactLinesQuery(scope.filter), scope.startNs, scope.endNs, maxLines, read)
	return miner, err
}

// newPatternLineReader returns the function that mines one row of the lines query into the miner.
func newPatternLineReader(miner *patternMiner, levels logRowLevels) func(*fj.Value) bool {
	step := miner.fillStep
	return func(v *fj.Value) bool {
		msg := v.GetStringBytes("_msg")
		if len(msg) == 0 {
			return true // an empty line has no tokens
		}
		if isVLMissingMsgBytes(msg, levels.defaultMsg) {
			// A row without a message is built from all of its fields, which the query does not keep.
			return false
		}
		timeNs, ok := parseLokiTimeToUnixNano(string(v.GetStringBytes("_time")))
		if !ok {
			return false
		}
		miner.observeStreamLine(string(v.GetStringBytes(patternExactStreamField)), patternLevelFromFJ(v, &levels), string(msg), patternBucketStart(timeNs, step), miner.sampleCount(timeNs))
		return true
	}
}

// readPatternLines mines the rows of a lines query's answer, for callers that hold the answer as a stream.
func readPatternLines(r io.Reader, miner *patternMiner, levels logRowLevels, maxLines int) error {
	return scanPatternRows(r, maxLines, newPatternLineReader(miner, levels))
}
