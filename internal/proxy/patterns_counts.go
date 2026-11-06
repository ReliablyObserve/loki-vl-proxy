package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// patternCountsMinStep is the resolution Loki keeps pattern samples at (pkg/pattern/drain TimeResolution): a
// smaller step is answered at it.
const patternCountsMinStep = 10 * time.Second

// patternCountsLevelFields are the fields the level of a row is read from (levelFieldSlot), grouped by so that
// each counted group keeps the level its rows have. Stream labels are fields to VictoriaLogs, so a level stored
// as a stream label is grouped by too.
var patternCountsLevelFields = []string{
	"detected_level", "level", "LEVEL", "Level", "log.level",
	"severity", "SEVERITY", "Severity", "SeverityText",
	"lvl", "LVL", "Lvl",
	"severity_text", "Severity_Text", "SEVERITY_TEXT", "severity_number",
}

// patternCountsGroupField holds a row's message with its numbers collapsed (collapse_nums): rows that differ only
// in numbers form one group.
const patternCountsGroupField = "_lvp_collapsed"

// patternCountsMaxField is a group's largest message; the smallest is returned as _msg.
const patternCountsMaxField = "_lvp_max"

// maxPatternCountsRowBytes bounds one row of a counts query: a group's messages with its fields.
const maxPatternCountsRowBytes = 4 << 20

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

// patternCountsGroupBy is the group of a counted row: the level fields, then the collapsed message.
func patternCountsGroupBy() []logsql.GroupKey {
	by := make([]logsql.GroupKey, 0, len(patternCountsLevelFields)+1)
	for _, field := range patternCountsLevelFields {
		if strings.Contains(field, ".") {
			field = logsql.QuoteValue(field)
		}
		by = append(by, logsql.GroupKey{Field: field})
	}
	return append(by, logsql.GroupKey{Field: patternCountsGroupField})
}

// patternCountsQuery is a counts query for a stream selector's filter, built as LogsQL nodes: the message copied
// with its numbers collapsed, a stats pipe, and one row more than maxRows, so a query that has more is told apart.
func patternCountsQuery(filter logsql.FilterExpr, stats logsql.PipeStats, maxRows int) *logsql.Query {
	return logsql.NewQuery(filter,
		logsql.PipeCopy{Pairs: [][2]string{{"_msg", patternCountsGroupField}}},
		logsql.PipeCollapseNums{Field: patternCountsGroupField},
		stats,
		logsql.PipeLimit{N: maxRows + 1},
	)
}

// patternGroupsQuery counts every group over the range and keeps its smallest and largest message: two real lines
// that differ where the group's numbers vary, so the miner keeps the numbers that do not as they are.
func patternGroupsQuery(filter logsql.FilterExpr, maxRows int) *logsql.Query {
	return patternCountsQuery(filter, logsql.PipeStats{By: patternCountsGroupBy(), Funcs: []logsql.StatsFuncAlias{
		{Func: logsql.Count{}, Alias: "c"},
		{Func: logsql.Min{Field: "_msg"}, Alias: "_msg"},
		{Func: logsql.Max{Field: "_msg"}, Alias: patternCountsMaxField},
	}}, maxRows)
}

// patternTimelineQuery counts every group per step.
func patternTimelineQuery(filter logsql.FilterExpr, step time.Duration, maxRows int) *logsql.Query {
	by := append([]logsql.GroupKey{{Field: "_time", Bucket: strconv.FormatInt(int64(step/time.Second), 10) + "s"}}, patternCountsGroupBy()...)
	return patternCountsQuery(filter, logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}}}, maxRows)
}

// patternGroupKey identifies a counted row's group: its level fields and collapsed message.
func patternGroupKey(v *fj.Value) string {
	var b strings.Builder
	for _, field := range patternCountsLevelFields {
		b.Write(v.GetStringBytes(field))
		b.WriteByte(0)
	}
	b.Write(v.GetStringBytes(patternCountsGroupField))
	return b.String()
}

// patternGroup is one row of the groups query.
type patternGroup struct {
	key, level, lo, hi string
	count              int
}

// patternTimelineCount is one row of the timeline query, its group interned.
type patternTimelineCount struct {
	group  int
	bucket int64
	count  int
}

// patternCountsWindow is the shortest window a range longer than it is counted in; patternCountsMaxWindows bounds
// the windows, which double in length until it holds. Windows are aligned to their length, so a refresh that moves
// the range counts only its ends: a settled window's patterns come from the cache.
const (
	patternCountsWindow     = time.Hour
	patternCountsMaxWindows = 16
	patternCountsParallel   = 8
)

func patternCountsWindows(startNs, endNs int64) []queryRangeWindow {
	span := time.Duration(endNs - startNs)
	if span <= patternCountsWindow {
		return []queryRangeWindow{{startNs: startNs, endNs: endNs}}
	}
	window := patternCountsWindow
	for span/window > patternCountsMaxWindows {
		window *= 2
	}
	return splitQueryRangeWindowsWithOptions(startNs, endNs, window, "backward", true)
}

// patternsFromCounts answers /patterns from VictoriaLogs' counts of every line in the range rather than from a
// sample of lines (countPatternsWindow), window by window over a long range, the windows' patterns merged as the
// sampled windows' are. ok is false when the request is one it does not answer (a query that is not a bare stream
// selector, bounds it cannot read, more rows than -patterns-max-counts-rows, a row without a message) or
// VictoriaLogs fails; the caller then samples lines.
func (p *Proxy) patternsFromCounts(r *http.Request, query, startParam, endParam, stepParam string, patternLimit int) ([]patternResultEntry, bool) {
	if !p.patternsCounts {
		return nil, false
	}
	lq, err := parsedLogQuery(strings.TrimSpace(query))
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
	windows := patternCountsWindows(startNs, endNs)
	if len(windows) == 1 {
		entries, ok := p.countPatternsWindow(r.Context(), filter, step, startNs, endNs, patternLimit)
		return entries, ok && len(entries) > 0
	}
	type windowResult struct {
		entries []patternResultEntry
		ok      bool
	}
	results := make([]windowResult, len(windows))
	sem := make(chan struct{}, patternCountsParallel)
	var wg sync.WaitGroup
	for i, w := range windows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i].entries, results[i].ok = p.countPatternsWindowCached(r, filter, step, w, i > 0 && i < len(windows)-1)
		}()
	}
	wg.Wait()
	var merged []patternResultEntry
	for _, res := range results {
		if !res.ok {
			return nil, false
		}
		merged = mergePatternResultEntries(merged, res.entries)
	}
	return merged, len(merged) > 0
}

// countPatternsWindowCached is countPatternsWindow for one window of a long range, its patterns cached when the
// window is interior and settled (queryRangeWindowTTL), as the sampled windows' are.
func (p *Proxy) countPatternsWindowCached(r *http.Request, filter logsql.FilterExpr, step time.Duration, w queryRangeWindow, interior bool) ([]patternResultEntry, bool) {
	ctx := r.Context()
	ttl := p.queryRangeWindowTTL(w.endNs)
	cacheKey := ""
	if interior && ttl > 0 {
		stepParam := strconv.FormatInt(int64(step/time.Second), 10) + "s"
		cacheKey = p.patternWindowCacheKey(r, "counts "+filter.String(), stepParam, maxPatternResponseLimit, p.limits().PatternsCountsRows, w.startNs, w.endNs)
		lookup := time.Now()
		if cached, ok := p.cache.Get(cacheKey); ok {
			var value patternWindowCacheValue
			if json.Unmarshal(cached, &value) == nil {
				p.observeInternalOperation(ctx, "patterns_window_cache", "hit", time.Since(lookup))
				return value.Entries, true
			}
		}
		p.observeInternalOperation(ctx, "patterns_window_cache", "miss", time.Since(lookup))
	}
	entries, ok := p.countPatternsWindow(ctx, filter, step, w.startNs, w.endNs, maxPatternResponseLimit)
	if ok && cacheKey != "" && ctx.Err() == nil {
		if encoded, err := json.Marshal(patternWindowCacheValue{Entries: entries, PatternCount: len(entries)}); err == nil {
			p.cache.SetWithTTL(cacheKey, encoded, ttl)
		}
	}
	return entries, ok
}

// countPatternsWindow counts every line of [startNs, endNs]: two typed stats queries run at once, the groups,
// each with its smallest and largest real message, which are mined; and their counts per step, which become the
// patterns' samples.
func (p *Proxy) countPatternsWindow(ctx context.Context, filter logsql.FilterExpr, step time.Duration, startNs, endNs int64, patternLimit int) ([]patternResultEntry, bool) {
	maxRows := p.limits().PatternsCountsRows
	levels := p.newLogRowLevels()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var groups []patternGroup
	var timeline []patternTimelineCount
	var timelineKeys []string
	var groupsErr, timelineErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		groupsErr = p.readPatternCountRows(ctx, patternGroupsQuery(filter, maxRows), startNs, endNs, maxRows, func(v *fj.Value) bool {
			lo, hi := v.GetStringBytes("_msg"), v.GetStringBytes(patternCountsMaxField)
			count, err := strconv.Atoi(string(v.GetStringBytes("c")))
			if err != nil || count <= 0 || isVLMissingMsgBytes(lo, levels.defaultMsg) {
				// Not a row of the groups query, or a row without a message, whose line is built from all of its
				// fields, which a group does not keep.
				return false
			}
			groups = append(groups, patternGroup{key: patternGroupKey(v), level: patternLevelFromFJ(v, &levels), lo: string(lo), hi: string(hi), count: count})
			return true
		})
		if groupsErr != nil {
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		interned := map[string]int{}
		timelineErr = p.readPatternCountRows(ctx, patternTimelineQuery(filter, step, maxRows), startNs, endNs, maxRows, func(v *fj.Value) bool {
			at, err := time.Parse(time.RFC3339Nano, string(v.GetStringBytes("_time")))
			if err != nil {
				return false
			}
			count, err := strconv.Atoi(string(v.GetStringBytes("c")))
			if err != nil || count <= 0 {
				return false
			}
			key := patternGroupKey(v)
			group, seen := interned[key]
			if !seen {
				group = len(timelineKeys)
				interned[key] = group
				timelineKeys = append(timelineKeys, key)
			}
			timeline = append(timeline, patternTimelineCount{group: group, bucket: at.Unix(), count: count})
			return true
		})
		if timelineErr != nil {
			cancel()
		}
	}()
	wg.Wait()
	if groupsErr != nil || timelineErr != nil {
		return nil, false
	}
	if len(groups) == 0 {
		// No line in the window: an answer, with no patterns.
		return nil, true
	}

	// The miner's clusters depend on the order it sees lines in: the groups go in by count, then key, so an
	// answer does not depend on the order VictoriaLogs returned them in.
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].count != groups[j].count {
			return groups[i].count > groups[j].count
		}
		return groups[i].key < groups[j].key
	})
	miner := newPatternMiner()
	clusters := make(map[string]*patternBucket, len(groups))
	for _, g := range groups {
		if cluster := miner.observeGroup(g.level, g.lo, g.hi); cluster != nil {
			clusters[g.key] = cluster
		}
	}
	for _, t := range timeline {
		if cluster := clusters[timelineKeys[t.group]]; cluster != nil {
			cluster.buckets[t.bucket] += t.count
			cluster.total += t.count
		}
	}
	return patternResultEntriesFromMaps(buildPatternResponse(miner, patternLimit)), true
}

// errPatternCounts marks a counts query whose answer the flow does not use.
type errPatternCounts struct{ reason string }

func (e errPatternCounts) Error() string { return "patterns counts: " + e.reason }

// readPatternCountRows runs a counts query over [startNs, endNs] and hands each row to read, which returns false
// for a row it cannot use. More than maxRows rows, a row read rejects or a failed request is an error.
func (p *Proxy) readPatternCountRows(ctx context.Context, q *logsql.Query, startNs, endNs int64, maxRows int, read func(*fj.Value) bool) error {
	params := url.Values{}
	params.Set("query", q.String())
	params.Set("start", strconv.FormatInt(startNs, 10))
	params.Set("end", strconv.FormatInt(endNs, 10))
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errPatternCounts{"status " + strconv.Itoa(resp.StatusCode)}
	}
	var parser fj.Parser
	rows := 0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), maxPatternCountsRowBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if rows++; rows > maxRows {
			return errPatternCounts{"more rows than -patterns-max-counts-rows"}
		}
		v, err := parser.ParseBytes(line)
		if err != nil {
			return err
		}
		if !read(v) {
			return errPatternCounts{"unexpected row"}
		}
	}
	return sc.Err()
}
