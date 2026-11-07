package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
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
// in numbers form one group. VictoriaLogs' collapse_nums takes digit-free hex such as aaabafcb for a number, which
// Loki's tokenizer does not, so a range counted this way mines such ids as one group; the exact tier does not (a
// replace_regexp of the tokens that hold a digit would, but costs three to five times the VictoriaLogs time on a
// large range).
const patternCountsGroupField = "_lvp_collapsed"

// patternCountsGroupHash is the collapsed message's hash: the timeline names a group by it, so a row of it carries
// a few bytes rather than the message.
const patternCountsGroupHash = "_lvp_group"

// patternCountsMaxField is a group's largest message; the smallest is returned as _msg.
const patternCountsMaxField = "_lvp_max"

// patternCountsFirstField is a group's earliest time: a stream's groups are mined in the order they first appear.
const patternCountsFirstField = "_lvp_first"

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

// patternCountsGroupBy is the group of a counted row: the level fields, then the collapsed message. Streams are
// not part of it: a service that runs thousands of short-lived pods would count thousands of groups for each
// message, so the counts flow mines the lines of all of a level's streams together, unlike Loki, which mines each
// stream apart (the exact tier does too).
func patternCountsGroupBy() []logsql.GroupKey {
	by := make([]logsql.GroupKey, 0, len(patternCountsLevelFields)+1)
	for _, field := range patternCountsLevelFields {
		if strings.Contains(field, ".") {
			field = logsql.QuoteValue(field)
		}
		by = append(by, logsql.GroupKey{Field: field})
	}
	return append(by, logsql.GroupKey{Field: patternCountsGroupHash})
}

// patternCountsQuery is a counts query for a stream selector's filter, built as LogsQL nodes: the message copied
// with its numbers collapsed, a stats pipe, and one row more than maxRows, so a query that has more is told apart.
func patternCountsQuery(filter logsql.FilterExpr, stats logsql.PipeStats, maxRows int) *logsql.Query {
	return logsql.NewQuery(filter,
		logsql.PipeCopy{Pairs: [][2]string{{"_msg", patternCountsGroupField}}},
		logsql.PipeCollapseNums{Field: patternCountsGroupField},
		logsql.PipeHash{Fields: []string{patternCountsGroupField}, Result: patternCountsGroupHash},
		stats,
		logsql.PipeLimit{N: maxRows + 1},
	)
}

// patternGroupsQuery counts every group over the range and keeps its smallest and largest message: two real lines
// that differ where the group's numbers vary, which the miner trains in that order (observeGroup).
func patternGroupsQuery(filter logsql.FilterExpr, maxRows int) *logsql.Query {
	return patternCountsQuery(filter, logsql.PipeStats{By: patternCountsGroupBy(), Funcs: []logsql.StatsFuncAlias{
		{Func: logsql.Count{}, Alias: "c"},
		{Func: logsql.Min{Field: "_msg"}, Alias: "_msg"},
		{Func: logsql.Max{Field: "_msg"}, Alias: patternCountsMaxField},
		{Func: logsql.Min{Field: "_time"}, Alias: patternCountsFirstField},
	}}, maxRows)
}

// patternTimelineQuery counts every group per step. VictoriaLogs truncates a line's time to the step from the
// epoch directly; Loki counts a line into its 10 s sample first and the sample into the step (patternBucketStart,
// used by the exact tier and the sampled read). The two agree when the step is a multiple of 10 s, which is the
// default step's case, and differ by the lines near a step's edge otherwise: a documented deviation of the counts
// flow, which cannot see the samples.
func patternTimelineQuery(filter logsql.FilterExpr, step time.Duration, maxRows int) *logsql.Query {
	by := append([]logsql.GroupKey{{Field: "_time", Bucket: strconv.FormatInt(int64(step/time.Second), 10) + "s"}}, patternCountsGroupBy()...)
	return patternCountsQuery(filter, logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}}}, maxRows)
}

// patternGroupKey identifies a counted row's group: its level fields and the hash of its collapsed message.
func patternGroupKey(v *fj.Value) string {
	var b strings.Builder
	for _, field := range patternCountsLevelFields {
		b.Write(v.GetStringBytes(field))
		b.WriteByte(0)
	}
	b.Write(v.GetStringBytes(patternCountsGroupHash))
	return b.String()
}

// patternGroupInput is one row of the groups query: a mining input of the counts flow.
type patternGroupInput struct {
	Key   string `json:"k"`
	Level string `json:"v"`
	Lo    string `json:"lo"`
	Hi    string `json:"hi"`
	Count int    `json:"c"`
	// FirstNs is the time of the group's earliest line, in unix nanoseconds, 0 when the row has none.
	FirstNs int64 `json:"t"`
}

// patternTimelineInput is one row of the timeline query, its group named by index into the window's keys.
type patternTimelineInput struct {
	Group  int   `json:"g"`
	Bucket int64 `json:"b"`
	Count  int   `json:"c"`
}

// patternWindowInputs are what the counts flow reads from VictoriaLogs for one window: the inputs of mining, not
// its result, so that a long range is mined in one pass over the lines of all its windows rather than window by
// window, and cached as such.
type patternWindowInputs struct {
	Keys     []string               `json:"keys"`
	Groups   []patternGroupInput    `json:"groups"`
	Timeline []patternTimelineInput `json:"timeline"`
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

// patternQueryScope is a /patterns request the counts and exact flows answer: the stream selector's filter, the
// range and Loki's step.
// patternMinableLines keeps the lines a pattern can be mined from: it leaves out a JSON line (one starting with {
// and ending with }) whose text holds none of the message fields' keys, quoted, which no pattern is mined from
// (patternJSONMessage reads the raw text for a top-level key the same way), so a service that writes only such lines
// reads nothing and counts nothing. A line holding a key's quoted name anywhere is kept, the miner telling a key from
// a value: phrases VictoriaLogs finds through its token index, where a regexp of the key and its colon scans every
// line (env=production over 3h: 95 ms against 230 ms, the same lines kept).
var patternMinableLines = logsql.Exclude{Expr: logsql.Group{Expr: logsql.AllOf{
	// An exact prefix: the whole line starts with {, where a prefix filter matches a word starting with { anywhere.
	logsql.FieldFilter{Field: "_msg", Op: logsql.FieldOpExactPrefix, Value: "{"},
	logsql.FieldFilter{Field: "_msg", Op: logsql.FieldOpRegexp, Value: `\}$`},
	logsql.Exclude{Expr: logsql.Group{Expr: patternMessageKeyPhrases()}},
}}}

// patternMessageKeyPhrases matches a line holding the quoted name of one of patternJSONMessageFields.
func patternMessageKeyPhrases() logsql.AnyOf {
	keys := make(logsql.AnyOf, len(patternJSONMessageFields))
	for i, key := range patternJSONMessageFields {
		keys[i] = logsql.Phrase{Value: `"` + key + `"`}
	}
	return keys
}

type patternQueryScope struct {
	filter         logsql.FilterExpr
	startNs, endNs int64
	step           time.Duration
}

// patternScope reads a request into a patternQueryScope. ok is false for a request they do not answer: a query that
// is not a bare stream selector, or bounds or a step it cannot read.
func (p *Proxy) patternScope(query, startParam, endParam, stepParam string) (patternQueryScope, bool) {
	lq, err := parsedLogQuery(strings.TrimSpace(query))
	if err != nil || len(lq.Pipeline) > 0 {
		return patternQueryScope{}, false
	}
	startNs, endNs, ok := parseLokiTimeRangeToUnixNano(startParam, endParam)
	if !ok || endNs <= startNs {
		return patternQueryScope{}, false
	}
	step, ok := patternCountsStep(startNs, endNs, stepParam)
	if !ok {
		return patternQueryScope{}, false
	}
	p.configMu.RLock()
	opts := logqlpkg.TranslateOptions{StreamFields: p.streamFieldsMap}
	if p.labelTranslator != nil {
		opts.LabelFn = p.labelTranslator.ToVL
	}
	p.configMu.RUnlock()
	filter, err := logqlpkg.SelectorFilter(lq.Selector, opts)
	if err != nil {
		return patternQueryScope{}, false
	}
	filter = logsql.AllOf{filter, patternMinableLines}
	return patternQueryScope{filter: filter, startNs: startNs, endNs: endNs, step: step}, true
}

// patternsFromCounts answers /patterns from VictoriaLogs' counts of every line in the range rather than from a
// sample of lines, window by window over a long range, the windows' inputs mined together in one pass in the order
// the groups first appeared in, as the lines of a short range are. ok is false when the request is one it does not
// answer (a query that is not a bare stream selector, bounds it cannot read, more rows than
// -patterns-max-counts-rows, a row without a message) or VictoriaLogs fails; the caller then samples lines.
func (p *Proxy) patternsFromCounts(r *http.Request, query, startParam, endParam, stepParam string, patternLimit int) ([]patternResultEntry, bool) {
	if !p.patternsCounts {
		return nil, false
	}
	scope, ok := p.patternScope(query, startParam, endParam, stepParam)
	if !ok {
		return nil, false
	}
	windows := patternCountsWindows(scope.startNs, scope.endNs)
	inputs := make([]patternWindowInputs, len(windows))
	oks := make([]bool, len(windows))
	if len(windows) == 1 {
		inputs[0], oks[0] = p.readPatternWindowInputs(r.Context(), scope.filter, scope.step, scope.startNs, scope.endNs)
	} else {
		sem := make(chan struct{}, patternCountsParallel)
		var wg sync.WaitGroup
		for i, w := range windows {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				inputs[i], oks[i] = p.patternWindowInputsCached(r, scope.filter, scope.step, w, i > 0 && i < len(windows)-1)
			}()
		}
		wg.Wait()
	}
	for _, ok := range oks {
		if !ok {
			return nil, false
		}
	}
	entries := minePatternWindowInputs(inputs, patternLimit)
	return entries, len(entries) > 0
}

// patternWindowInputsCached is readPatternWindowInputs for one window of a long range, its inputs cached when the
// window is interior and settled (queryRangeWindowTTL), as the sampled windows' patterns are.
func (p *Proxy) patternWindowInputsCached(r *http.Request, filter logsql.FilterExpr, step time.Duration, w queryRangeWindow, interior bool) (patternWindowInputs, bool) {
	ctx := r.Context()
	ttl := p.queryRangeWindowTTL(w.endNs)
	cacheKey := ""
	if interior && ttl > 0 {
		cacheKey = p.patternWindowInputsKey(r, filter, step, w)
		lookup := time.Now()
		if cached, ok := p.cache.Get(cacheKey); ok {
			var value patternWindowInputs
			if json.Unmarshal(cached, &value) == nil {
				p.observeInternalOperation(ctx, "patterns_window_cache", "hit", time.Since(lookup))
				return value, true
			}
		}
		p.observeInternalOperation(ctx, "patterns_window_cache", "miss", time.Since(lookup))
	}
	inputs, ok := p.readPatternWindowInputs(ctx, filter, step, w.startNs, w.endNs)
	if ok && cacheKey != "" && ctx.Err() == nil {
		if encoded, err := json.Marshal(inputs); err == nil {
			p.cache.SetWithTTL(cacheKey, encoded, ttl)
		}
	}
	return inputs, ok
}

// patternWindowInputsKey names a window's counted inputs in the cache.
func (p *Proxy) patternWindowInputsKey(r *http.Request, filter logsql.FilterExpr, step time.Duration, w queryRangeWindow) string {
	stepParam := strconv.FormatInt(int64(step/time.Second), 10) + "s"
	return p.patternWindowCacheKey(r, "counts "+filter.String(), stepParam, maxPatternResponseLimit, p.limits().PatternsCountsRows, w.startNs, w.endNs)
}

// readPatternWindowInputs counts every line of [startNs, endNs]: two typed stats queries run at once, the groups,
// each with its smallest and largest real message and its earliest time, which are mined; and their counts per
// step, which become the patterns' samples.
func (p *Proxy) readPatternWindowInputs(ctx context.Context, filter logsql.FilterExpr, step time.Duration, startNs, endNs int64) (patternWindowInputs, bool) {
	maxRows := p.limits().PatternsCountsRows
	levels := p.newLogRowLevels()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var inputs patternWindowInputs
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
			firstNs, _ := parseLokiTimeToUnixNano(string(v.GetStringBytes(patternCountsFirstField)))
			inputs.Groups = append(inputs.Groups, patternGroupInput{Key: patternGroupKey(v), Level: patternLevelFromFJ(v, &levels), Lo: string(lo), Hi: string(hi), Count: count, FirstNs: firstNs})
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
				group = len(inputs.Keys)
				interned[key] = group
				inputs.Keys = append(inputs.Keys, key)
			}
			inputs.Timeline = append(inputs.Timeline, patternTimelineInput{Group: group, Bucket: at.Unix(), Count: count})
			return true
		})
		if timelineErr != nil {
			cancel()
		}
	}()
	wg.Wait()
	if groupsErr != nil || timelineErr != nil {
		return patternWindowInputs{}, false
	}
	return inputs, true
}

// minePatternWindowInputs mines the windows' groups in one pass, in the order their earliest lines came in (then by
// key, so an answer does not depend on the order VictoriaLogs returned them in), and adds the timelines' counts to
// the clusters their groups ended in. A group the windows have more than once is counted in the cluster of its
// first.
func minePatternWindowInputs(windows []patternWindowInputs, patternLimit int) []patternResultEntry {
	var groups []patternGroupInput
	for _, w := range windows {
		groups = append(groups, w.Groups...)
	}
	if len(groups) == 0 {
		return nil
	}
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].FirstNs != groups[j].FirstNs {
			return groups[i].FirstNs < groups[j].FirstNs
		}
		return groups[i].Key < groups[j].Key
	})
	miner := newPatternMiner()
	clusters := make(map[string]*patternBucket, len(groups))
	for _, g := range groups {
		cluster := miner.observeGroup(g.Level, g.Lo, g.Hi)
		if _, seen := clusters[g.Key]; !seen && cluster != nil {
			clusters[g.Key] = cluster
		}
	}
	for _, w := range windows {
		for _, t := range w.Timeline {
			if cluster := clusters[w.Keys[t.Group]]; cluster != nil {
				cluster.buckets[t.Bucket] += t.Count
				cluster.total += t.Count
			}
		}
	}
	return patternResultEntriesFromMaps(buildPatternResponse(miner, patternLimit))
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
	return scanPatternRows(resp.Body, maxRows, read)
}

// scanPatternRows hands each JSON row of r to read, one at a time: the row is valid until read returns, and no
// more than one row is held. More than maxRows rows or a row read rejects is an error.
func scanPatternRows(r io.Reader, maxRows int, read func(*fj.Value) bool) error {
	var parser fj.Parser
	rows := 0
	sc := bufio.NewScanner(r)
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
