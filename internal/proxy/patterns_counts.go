package proxy

import (
	"bufio"
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// patternCountsMinStep is the resolution Loki keeps pattern samples at (pkg/pattern/drain TimeResolution): a
// smaller step is answered at it.
const patternCountsMinStep = 10 * time.Second

// patternCountsLevelFields are the fields the level of a row is read from (levelFieldSlot), grouped by so that
// each counted group keeps the level its rows have. log.level is quoted as a LogsQL field name.
var patternCountsLevelFields = []string{
	"detected_level", "level", "LEVEL", "Level", `"log.level"`,
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT", "severity_number",
}

// patternCountsStep is the step Loki answers /patterns at (pkg/loghttp ParsePatternsQuery, then the pattern
// samples' resolution): the request's step, or the range over 250 points in whole seconds, and no less than
// patternCountsMinStep.
func patternCountsStep(startNs, endNs int64, stepParam string) (time.Duration, bool) {
	var step time.Duration
	if strings.TrimSpace(stepParam) != "" {
		d, ok := parsePositiveStepDuration(stepParam)
		if !ok {
			return 0, false
		}
		step = d
	} else {
		step = time.Duration((endNs-startNs)/250) / time.Second * time.Second
		if step < time.Second {
			step = time.Second
		}
	}
	return max(step, patternCountsMinStep).Truncate(time.Second), true
}

// patternCountsQuery is the counts query for a bare stream selector, built as LogsQL nodes: the selector's
// filter, numbers collapsed in the message, then a count per step, level field and collapsed message. Stream labels
// are fields to VictoriaLogs, so a level stored as a stream label is grouped by too.
// The last pipe asks for one row more than maxRows, so a query that has more is told apart.
func patternCountsQuery(filter logsql.FilterExpr, step time.Duration, maxRows int) *logsql.Query {
	by := make([]logsql.GroupKey, 0, len(patternCountsLevelFields)+3)
	by = append(by, logsql.GroupKey{Field: "_time", Bucket: strconv.FormatInt(int64(step/time.Second), 10) + "s"})
	for _, field := range patternCountsLevelFields {
		by = append(by, logsql.GroupKey{Field: field})
	}
	by = append(by, logsql.GroupKey{Field: "_msg"})
	return logsql.NewQuery(filter,
		logsql.PipeCollapseNums{},
		logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}}},
		logsql.PipeLimit{N: maxRows + 1},
	)
}

// patternsFromCounts answers /patterns from VictoriaLogs' counts of every line in the range rather than from a
// sample of lines: one stats query, mined per distinct collapsed message with its count. ok is false when the
// request is one it does not answer (a query that is not a bare stream selector, bounds it cannot read, more
// groups than the cap, a row without a message) or VictoriaLogs fails; the caller then samples lines as before.
func (p *Proxy) patternsFromCounts(ctx context.Context, query, startParam, endParam, stepParam string, patternLimit int) ([]patternResultEntry, bool) {
	if !p.patternsCounts {
		return nil, false
	}
	lq, err := logqlpkg.ParseLogQuery(strings.TrimSpace(query))
	if err != nil || len(lq.Pipeline) > 0 {
		return nil, false
	}
	startNs, endNs, ok := parseLokiTimeRangeToUnixNano(startParam, endParam)
	if !ok || endNs <= startNs {
		return nil, false
	}
	step, ok := patternCountsStep(startNs, endNs, stepParam)
	if !ok {
		return nil, false
	}
	p.configMu.RLock()
	opts := logqlpkg.TranslateOptions{StreamFields: p.streamFieldsMap}
	if p.labelTranslator != nil {
		opts.LabelFn = p.labelTranslator.ToVL
	}
	p.configMu.RUnlock()
	filter, err := logqlpkg.SelectorFilter(lq.Selector, opts)
	if err != nil {
		return nil, false
	}
	maxRows := p.limits().PatternsCountsRows
	params := url.Values{}
	params.Set("query", patternCountsQuery(filter, step, maxRows).String())
	params.Set("start", strconv.FormatInt(startNs, 10))
	params.Set("end", strconv.FormatInt(endNs, 10))
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}

	miner := newPatternMiner()
	levels := p.newLogRowLevels()
	var parser fj.Parser
	rows := 0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), maxPatternCountsRowBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		rows++
		if rows > maxRows {
			return nil, false
		}
		v, err := parser.ParseBytes(line)
		if err != nil {
			return nil, false
		}
		msg := v.GetStringBytes("_msg")
		if isVLMissingMsgBytes(msg, levels.defaultMsg) {
			// The line of a row without a message is built from all of its fields, which a count does not keep.
			return nil, false
		}
		at, err := time.Parse(time.RFC3339Nano, string(v.GetStringBytes("_time")))
		if err != nil {
			return nil, false
		}
		count, err := strconv.Atoi(string(v.GetStringBytes("c")))
		if err != nil || count <= 0 {
			// Not a row of the counts query.
			return nil, false
		}
		miner.ObserveCollapsed(patternLevelFromFJ(v, &levels), string(msg), at.Unix(), count)
	}
	if sc.Err() != nil || rows == 0 {
		return nil, false
	}
	return patternResultEntriesFromMaps(buildPatternResponse(miner, patternLimit)), true
}

// maxPatternCountsRowBytes bounds one row of the counts query: a collapsed message with its stream and fields.
const maxPatternCountsRowBytes = 4 << 20
