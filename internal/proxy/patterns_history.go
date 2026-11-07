package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// addPatternHistory counts the lines of the scope's range (the part of a request before the pattern state's
// samples) onto entries, the patterns the state answered, so a long range's chart covers the whole range with
// Loki's patterns: VictoriaLogs counts the range's groups per step a window at a time (patternWindowInputsCached,
// settled windows cached and shared with peers), and each group's counts go to the most specific pattern of its
// level its line matches as Drilldown's pattern filter (|>) matches it. A group no pattern matches is not counted:
// the patterns are the state's. When VictoriaLogs fails, entries are returned as they are.
func (p *Proxy) addPatternHistory(r *http.Request, scope patternQueryScope, entries []patternResultEntry) []patternResultEntry {
	if len(entries) == 0 || scope.startNs >= scope.endNs {
		return entries
	}
	inputs, ok := p.patternHistoryInputs(r, scope)
	if !ok {
		p.observeInternalOperation(r.Context(), "patterns_history", "error", 0)
		return entries
	}

	// The patterns of each level, most specific first: the most literal text, then the most lines.
	type candidate struct {
		at      int
		pattern string
	}
	byLevel := map[string][]candidate{}
	for i, e := range entries {
		byLevel[e.Level] = append(byLevel[e.Level], candidate{i, e.Pattern})
	}
	for _, list := range byLevel {
		sort.SliceStable(list, func(i, j int) bool {
			return len(strings.ReplaceAll(list[i].pattern, patternVarPlaceholder, "")) > len(strings.ReplaceAll(list[j].pattern, patternVarPlaceholder, ""))
		})
	}
	matched := map[string]int{} // group key → entry, -1 when none matches
	match := func(key, level, line string) int {
		if at, ok := matched[key]; ok {
			return at
		}
		at := -1
		for _, c := range byLevel[level] {
			if patternMatchesLine(c.pattern, line) {
				at = c.at
				break
			}
		}
		matched[key] = at
		return at
	}
	added := make([]map[int64]int, len(entries))
	for _, w := range inputs {
		groupOf := make(map[string]int, len(w.Groups))
		for _, g := range w.Groups {
			groupOf[g.Key] = match(g.Key, g.Level, g.Lo)
		}
		for _, row := range w.Timeline {
			if row.Group < 0 || row.Group >= len(w.Keys) {
				continue
			}
			at, ok := groupOf[w.Keys[row.Group]]
			if !ok || at < 0 {
				continue
			}
			if added[at] == nil {
				added[at] = map[int64]int{}
			}
			added[at][row.Bucket] += row.Count
		}
	}
	for i, extra := range added {
		if len(extra) == 0 {
			continue
		}
		samples := make(map[int64]int, len(entries[i].Samples)+len(extra))
		for _, s := range entries[i].Samples {
			if len(s) < 2 {
				continue
			}
			at, okAt := numberToInt64(s[0])
			n, okN := numberToInt(s[1])
			if okAt && okN {
				samples[at] += n
			}
		}
		for at, n := range extra {
			samples[at] += n
		}
		times := make([]int64, 0, len(samples))
		for at := range samples {
			times = append(times, at)
		}
		sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
		out := make([][]interface{}, len(times))
		for j, at := range times {
			out[j] = []interface{}{at, samples[at]}
		}
		entries[i].Samples = out
	}
	p.observeInternalOperation(r.Context(), "patterns_history", "counted", 0)
	return entries
}

// patternHistoryRanges are the ranges whose history the holder of a selector's state counts ahead, Logs
// Drilldown's longer time pickers: Drilldown asks /patterns without a step, so a range's step follows from its
// length (patternCountsStep) and the windows counted ahead are the ones its request reads.
var patternHistoryRanges = []time.Duration{6 * time.Hour, 12 * time.Hour, 24 * time.Hour}

// patternHistoryPrewarmEvery is how often at most the holder counts a selector's history ahead: a settled window
// is added every hour, the others are cached.
const patternHistoryPrewarmEvery = 30 * time.Minute

// prewarmPatternHistory counts, in the background, the history of patternHistoryRanges before the state's samples
// (retainFromNs), so the first request for such a range reads only the window its start cuts. One runs at a time;
// it reads with the request's identity, outlives it and stops with the store.
func (p *Proxy) prewarmPatternHistory(r *http.Request, filter logsql.FilterExpr, now time.Time, retainFromNs int64) {
	store := p.patternState
	select {
	case store.prewarms <- struct{}{}:
	default:
		return
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	stop := context.AfterFunc(store.ctx, cancel)
	req := r.Clone(ctx)
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		defer func() { <-store.prewarms }()
		defer stop()
		defer cancel()
		for _, span := range patternHistoryRanges {
			startNs := now.Add(-span).UnixNano()
			if startNs >= retainFromNs || ctx.Err() != nil {
				continue
			}
			step, _ := patternCountsStep(startNs, now.UnixNano(), "")
			if _, ok := p.patternHistoryInputs(req, patternQueryScope{filter: filter, startNs: startNs, endNs: retainFromNs, step: step}); !ok {
				p.observeInternalOperation(ctx, "patterns_history", "prewarm_failed", 0)
				return
			}
		}
		p.observeInternalOperation(ctx, "patterns_history", "prewarmed", 0)
	}()
}

// patternHistoryInputs is the counted inputs of the history's windows: the settled ones from the cache (memory,
// disk, the peers), and those missing counted together in one VictoriaLogs read (a groups and a timeline query)
// over their span, split into windows and cached, so a range's first request reads VictoriaLogs twice whatever
// its length and later ones reuse the windows. The window the range's moving start cuts is not cached.
func (p *Proxy) patternHistoryInputs(r *http.Request, scope patternQueryScope) ([]patternWindowInputs, bool) {
	windows := patternCountsWindows(scope.startNs, scope.endNs)
	// The window the range's start cuts is counted whole, from its aligned start, and cached as the others are, so
	// a refresh, whose start has moved, reads it from the cache too: its steps that start before the range are
	// left out of the answer (as Loki answers only the steps inside the range).
	size := int64(0)
	for _, w := range windows {
		size = max(size, w.endNs-w.startNs+1)
	}
	for i, w := range windows {
		if w.startNs == scope.startNs && len(windows) > 1 && w.endNs+1-size < w.startNs {
			windows[i].startNs = w.endNs + 1 - size
		}
	}
	inputs := make([]patternWindowInputs, len(windows))
	keys := make([]string, len(windows))
	var missing []int
	defer func() {
		for i := range inputs {
			inputs[i] = inputs[i].from(scope.startNs)
		}
	}()
	for i, w := range windows {
		if (w.startNs != scope.startNs || len(windows) > 1) && p.queryRangeWindowTTL(w.endNs) > 0 {
			keys[i] = p.patternWindowInputsKey(r, scope.filter, scope.step, w)
			if cached, ok := p.cache.Get(keys[i]); ok && json.Unmarshal(cached, &inputs[i]) == nil {
				continue
			}
		}
		missing = append(missing, i)
	}
	if len(missing) == 0 {
		return inputs, true
	}
	// Each run of adjacent missing windows is read once over its span: a step that starts before the run counts,
	// in this read, only the lines inside it, which belong to the run's first window; a run never covers a cached
	// window, whose lines its own read counted.
	sort.Slice(missing, func(a, b int) bool { return windows[missing[a]].startNs < windows[missing[b]].startNs })
	for len(missing) > 0 {
		run := 1
		// Windows are inclusive of their end (the next starts a nanosecond later) or end where the next starts.
		for run < len(missing) && windows[missing[run]].startNs-windows[missing[run-1]].endNs <= 1 {
			run++
		}
		if !p.readPatternHistoryRun(r, scope, windows, missing[:run], inputs, keys) {
			return nil, false
		}
		missing = missing[run:]
	}
	p.observeInternalOperation(r.Context(), "patterns_history", "read", 0)
	return inputs, true
}

// from is the window's inputs with the steps that start before startNs left out; the inputs are not changed.
func (w patternWindowInputs) from(startNs int64) patternWindowInputs {
	keep := w.Timeline[:0:0]
	for _, row := range w.Timeline {
		if row.Bucket*int64(time.Second) >= startNs {
			keep = append(keep, row)
		}
	}
	if len(keep) == len(w.Timeline) {
		return w
	}
	w.Timeline = keep
	return w
}

// readPatternHistoryRun reads the run of adjacent windows (indexes into windows, oldest first) in one groups and
// one timeline query, splits the counts into the windows (a step into the window it starts in, or the run's first
// window when it starts before the run) and caches the windows that have a key.
func (p *Proxy) readPatternHistoryRun(r *http.Request, scope patternQueryScope, windows []queryRangeWindow, run []int, inputs []patternWindowInputs, keys []string) bool {
	// A step belongs to the window it starts in, whole: the read starts at the run's first step and ends where its
	// last step does (the history's end at most), so a step that crosses into the next window is counted here in
	// full and the next window's read, starting at its own first step, does not count it again.
	step := int64(scope.step)
	from := ceilToStep(windows[run[0]].startNs, step)
	to := ceilToStep(windows[run[len(run)-1]].endNs+1, step)
	if to > scope.endNs {
		to = scope.endNs
	}
	if from >= to {
		return true
	}
	all, ok := p.readPatternWindowInputs(r.Context(), scope.filter, scope.step, from, to)
	if !ok {
		return false
	}
	groupOf := make(map[string]patternGroupInput, len(all.Groups))
	for _, g := range all.Groups {
		groupOf[g.Key] = g
	}
	parts := make([]patternWindowInputs, len(run))
	index := make([]map[int]int, len(run))
	for k := range index {
		index[k] = map[int]int{}
	}
	for _, row := range all.Timeline {
		if row.Group < 0 || row.Group >= len(all.Keys) {
			continue
		}
		at := row.Bucket * int64(time.Second)
		k := 0
		for k+1 < len(run) && at >= windows[run[k+1]].startNs {
			k++
		}
		local, seen := index[k][row.Group]
		if !seen {
			local = len(parts[k].Keys)
			index[k][row.Group] = local
			key := all.Keys[row.Group]
			parts[k].Keys = append(parts[k].Keys, key)
			if g, ok := groupOf[key]; ok {
				parts[k].Groups = append(parts[k].Groups, g)
			}
		}
		parts[k].Timeline = append(parts[k].Timeline, patternTimelineInput{Group: local, Bucket: row.Bucket, Count: row.Count})
	}
	for k, i := range run {
		inputs[i] = parts[k]
		if keys[i] != "" {
			if encoded, err := json.Marshal(parts[k]); err == nil {
				p.cache.SetWithTTL(keys[i], encoded, p.queryRangeWindowTTL(windows[i].endNs))
			}
		}
	}
	return true
}

// ceilToStep is ns rounded up to a multiple of step from the epoch.
func ceilToStep(ns, step int64) int64 {
	if step <= 0 {
		return ns
	}
	return (ns + step - 1) / step * step
}

// patternMatchesLine reports whether a pattern matches a line as Loki's pattern line filter (|>) does, the filter
// Logs Drilldown adds when a pattern is included: each literal is found at its first occurrence after the text
// before it (a leading literal anywhere in the line), each variable holds at least one character, and a pattern
// that ends on a literal ends the line there.
func patternMatchesLine(pattern, line string) bool {
	pos, variable := 0, false
	for pattern != "" {
		if strings.HasPrefix(pattern, patternVarPlaceholder) {
			variable = true
			pattern = pattern[len(patternVarPlaceholder):]
			continue
		}
		next := strings.Index(pattern, patternVarPlaceholder)
		if next < 0 {
			next = len(pattern)
		}
		literal := pattern[:next]
		pattern = pattern[next:]
		i := strings.Index(line[pos:], literal)
		if i < 0 || variable && i == 0 {
			return false
		}
		pos += i + len(literal)
		variable = false
	}
	if variable {
		return pos < len(line)
	}
	return pos == len(line)
}
