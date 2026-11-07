package proxy

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
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

// patternHistoryInputs is the counted inputs of the history's windows: the settled ones from the cache (memory,
// disk, the peers), and those missing counted together in one VictoriaLogs read (a groups and a timeline query)
// over their span, split into windows and cached, so a range's first request reads VictoriaLogs twice whatever
// its length and later ones reuse the windows. The window the range's moving start cuts is not cached.
func (p *Proxy) patternHistoryInputs(r *http.Request, scope patternQueryScope) ([]patternWindowInputs, bool) {
	windows := patternCountsWindows(scope.startNs, scope.endNs)
	inputs := make([]patternWindowInputs, len(windows))
	keys := make([]string, len(windows))
	var missing []int
	for i, w := range windows {
		if w.startNs != scope.startNs && p.queryRangeWindowTTL(w.endNs) > 0 {
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
	from, to := windows[missing[0]].startNs, windows[missing[0]].endNs
	for _, i := range missing[1:] {
		from, to = min64(from, windows[i].startNs), max64(to, windows[i].endNs)
	}
	all, ok := p.readPatternWindowInputs(r.Context(), scope.filter, scope.step, from, to)
	if !ok {
		return nil, false
	}
	groupOf := make(map[string]patternGroupInput, len(all.Groups))
	for _, g := range all.Groups {
		groupOf[g.Key] = g
	}
	for _, i := range missing {
		w := windows[i]
		var part patternWindowInputs
		index := map[int]int{}
		for _, row := range all.Timeline {
			if at := row.Bucket * int64(time.Second); at < w.startNs || at >= w.endNs || row.Group < 0 || row.Group >= len(all.Keys) {
				continue
			}
			local, seen := index[row.Group]
			if !seen {
				local = len(part.Keys)
				index[row.Group] = local
				key := all.Keys[row.Group]
				part.Keys = append(part.Keys, key)
				if g, ok := groupOf[key]; ok {
					part.Groups = append(part.Groups, g)
				}
			}
			part.Timeline = append(part.Timeline, patternTimelineInput{Group: local, Bucket: row.Bucket, Count: row.Count})
		}
		inputs[i] = part
		if keys[i] != "" {
			if encoded, err := json.Marshal(part); err == nil {
				p.cache.SetWithTTL(keys[i], encoded, p.queryRangeWindowTTL(w.endNs))
			}
		}
	}
	p.observeInternalOperation(r.Context(), "patterns_history", "read", 0)
	return inputs, true
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
