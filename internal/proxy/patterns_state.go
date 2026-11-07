package proxy

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"
)

const (
	// DefaultPatternsStateMaxBytes is -patterns-state-max-bytes: the memory the pattern state of all tenants may
	// hold before the least recently used tenant's is dropped. 0 turns the state off.
	DefaultPatternsStateMaxBytes = 256 << 20
	// DefaultPatternsStateStreamIdle is -patterns-state-stream-idle: how long a stream may read no line before its
	// clusters are kept as their patterns and samples alone.
	DefaultPatternsStateStreamIdle = 5 * time.Minute
	// DefaultPatternsStateMaxBuildLines is -patterns-state-max-build-lines: the most lines over Loki's retention a
	// selector's state is built from in the background, when they are more than -patterns-exact-max-lines.
	DefaultPatternsStateMaxBuildLines = 2_000_000
)

// patternStateBuildWindow is the span of lines a background build reads at a time, holding the tenant's state
// only while it trains them; a window VictoriaLogs cannot sort is read in quarters.
const patternStateBuildWindow = time.Hour

// patternStateAgeEvery is how often a request ages its tenant's state (folds idle streams, drops samples older
// than the retention, applies the store's bounds); a read from the retention's start and a new retention hour age
// it at once. Aging walks every cluster's samples, a fifth of a busy selector's warm answer when done each time.
const patternStateAgeEvery = 15 * time.Second

// patternStateFutureSteps is how many steps after now a range may end and still be answered from the state.
const patternStateFutureSteps = 3

// patternStateBuildWait is how long a background build waits at most for the request that started it to be
// answered before it reads: until then its reads would slow that request's own.
var patternStateBuildWait = 30 * time.Second

// lokiPatternRetention is how far back Loki's pattern ingester holds patterns (pattern_ingester retain_for,
// default 3h): the pattern state is built from then, and a range that starts earlier is answered, as Loki answers
// it, with the samples of the retention alone.
const lokiPatternRetention = 3 * time.Hour

// patternStateOverlap is how far before the end of what a selector's state read a refresh reads again, so a line
// VictoriaLogs made visible just after the last read is not missed. A line older than its stream's newest line read
// is not mined, as Loki skips such a line; lines at the newest time are mined unless read already (several lines
// of a stream often share a time).
const patternStateOverlap = 10 * time.Second

// patternStateStore keeps, per tenant, the state Loki's pattern ingester keeps: each stream's clusters at each
// level, trained on every line of the stream since the state was built, with samples every 10 s. A /patterns
// request whose range lies inside Loki's retention is answered from it: a refresh reads only the lines after
// what the state read and trains them on, and the answer is the samples inside the range. One state serves every
// selector of a tenant, a line being mined once whichever selector read it.
type patternStateStore struct {
	mu       sync.Mutex
	tenants  map[string]*patternTenantState
	maxBytes int64
	idle     time.Duration
	clock    uint64
	// maxBuildLines bounds the selectors built in the background (DefaultPatternsStateMaxBuildLines); 0 builds
	// none. One build runs at a time (builds), each stopped with the store (ctx, wg).
	maxBuildLines int
	builds        chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	// now is the clock the state ages by; time.Now outside tests.
	now func() time.Time
}

// newPatternStateStoreFor is the store for the configured bytes, nil when 0 turns the state off.
func newPatternStateStoreFor(maxBytes int64, idle time.Duration, maxBuildLines int) *patternStateStore {
	if maxBytes <= 0 {
		return nil
	}
	st := newPatternStateStore(maxBytes, idle)
	st.maxBuildLines = maxBuildLines
	return st
}

func newPatternStateStore(maxBytes int64, idle time.Duration) *patternStateStore {
	if idle <= 0 {
		idle = DefaultPatternsStateStreamIdle
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &patternStateStore{
		tenants: make(map[string]*patternTenantState), maxBytes: maxBytes, idle: idle, now: time.Now,
		builds: make(chan struct{}, 1), ctx: ctx, cancel: cancel,
	}
}

// close stops the store's background builds and waits for them, at most until ctx is done.
func (st *patternStateStore) close(ctx context.Context) {
	if st == nil {
		return
	}
	st.cancel()
	done := make(chan struct{})
	go func() {
		st.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// patternTenantState is one tenant's streams. Its lock is held while a request reads lines into it, so requests
// of a tenant are answered one at a time and a range's lines are read once.
type patternTenantState struct {
	mu sync.Mutex
	// miner holds each stream's clusterer at each level; its samples are keyed by the start of their 10 s sample.
	miner *patternMiner
	// newest is each stream's newest line read, in unix nanoseconds, and atNewest the lines read at that time, by
	// the hash of their text: a refresh reads the newest lines again and trains only those not read yet.
	newest   map[string]int64
	atNewest map[string]map[uint64]int
	// folded is the patterns of the streams that went idle, their clusterers freed.
	folded    map[string][]patternFoldedCluster
	selectors map[string]*patternSelectorState
	bytes     int64
	used      uint64
	// agedAt and agedFrom are when the state was last aged and the retention's start then.
	agedAt   time.Time
	agedFrom int64
}

// patternSelectorState is what the state read for a selector: its lines from fromNs up to toNs, and the streams
// they belong to.
type patternSelectorState struct {
	fromNs, toNs int64
	streams      map[string]struct{}
	// building is true while a background build reads the selector's lines; requests are answered without the
	// state until it ends.
	building bool
}

// patternFoldedCluster is a cluster of a stream that went idle: a finished stream trains nothing more, so its
// pattern and samples are all an answer needs.
type patternFoldedCluster struct {
	level, pattern string
	samples        map[int64]int
}

// patternsFromState answers a request from the tenant's pattern state, building the state for the selector from
// Loki's retention when it has none (if that holds at most maxLines lines) and reading the lines after what it
// read when the range ends later. A range that starts before the retention is answered with the retention's
// samples alone, as Loki's pattern ingester answers it (it holds no older ones), and one that ends before it with
// none. answered is false when the state cannot be built within maxLines (or is being built in the background),
// the range ends more than patternStateFutureSteps steps after now, or VictoriaLogs fails: the caller answers without the state.
//
// fromNs is where the answer's samples start: Loki's retention (3h back), or with -patterns-full-range the hour
// the state's samples start at, the range before it left to the caller (addPatternHistory). lines is the range's
// count of lines when the state counted it (a range inside the retention, on the request that finds no state for
// the selector, counted along with the retention's), -1 otherwise.
func (p *Proxy) patternsFromState(r *http.Request, scope patternQueryScope, maxLines, patternLimit int) (entries []patternResultEntry, fromNs int64, lines int, ok bool) {
	store := p.patternState
	if store == nil {
		return nil, 0, -1, false
	}
	now := store.now()
	retainFrom := now.Add(-lokiPatternRetention).Truncate(time.Hour).UnixNano()
	// A range may end a few steps after now (Drilldown's moving window); one that ends later is answered from its
	// own lines (the caller), not from a state read up to now.
	if scope.endNs > now.Add(max(time.Minute, patternStateFutureSteps*scope.step)).UnixNano() {
		return nil, 0, -1, false
	}
	ctx := r.Context()
	t := store.tenant(p.patternStateTenantKey(r))
	t.mu.Lock()
	defer t.mu.Unlock()

	selector := scope.filter.String()
	s := t.selectors[selector]
	if s != nil && s.building {
		p.observeInternalOperation(ctx, "patterns_state", "building", 0)
		return nil, 0, -1, false
	}
	lines = -1
	if s == nil {
		// A range inside the retention is counted with it, for the caller to answer the request without the
		// state when the retention holds too many; a longer range costs more to count than the retention and is
		// counted only then, by the caller.
		var retained int
		var retainedErr, rangeErr error
		var wg sync.WaitGroup
		if scope.startNs >= retainFrom {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lines, rangeErr = p.countPatternLines(ctx, scope, scope.startNs, scope.endNs)
			}()
		}
		retained, retainedErr = p.countPatternLines(ctx, scope, retainFrom, scope.endNs)
		wg.Wait()
		if rangeErr != nil {
			lines = -1
		}
		if retainedErr != nil {
			return nil, 0, lines, false
		}
		if retained > maxLines {
			if retained <= store.maxBuildLines && p.buildPatternStateInBackground(r, t, selector, scope, retainFrom, now) {
				p.observeInternalOperation(ctx, "patterns_state", "build_started", 0)
			} else {
				p.observeInternalOperation(ctx, "patterns_state", "over_cap", 0)
			}
			return nil, 0, lines, false
		}
		s = &patternSelectorState{fromNs: retainFrom, toNs: retainFrom, streams: make(map[string]struct{})}
		t.selectors[selector] = s
	}
	readTo := scope.endNs
	if nowNs := now.UnixNano(); readTo > nowNs {
		readTo = nowNs
	}
	built := false
	if readTo > s.toNs {
		started := time.Now()
		built = s.toNs == s.fromNs
		readFrom := s.toNs - int64(patternStateOverlap)
		if readFrom < s.fromNs {
			readFrom = s.fromNs
		}
		if err := p.readPatternStateLines(ctx, t, s, scope, readFrom, readTo, maxLines); err != nil {
			delete(t.selectors, selector)
			p.observeInternalOperation(ctx, "patterns_state", "error", time.Since(started))
			return nil, 0, lines, false
		}
		s.toNs = readTo
		p.observeInternalOperation(ctx, "patterns_state", "read", time.Since(started))
	} else {
		p.observeInternalOperation(ctx, "patterns_state", "hit", 0)
	}
	fromNs = now.Add(-lokiPatternRetention).UnixNano()
	if p.patternsFullRange {
		fromNs = retainFrom
	}
	answered := scope
	if answered.startNs < fromNs {
		answered.startNs = fromNs
	}
	if answered.startNs < answered.endNs {
		entries = t.answer(s, answered, patternLimit)
	}
	if built || retainFrom != t.agedFrom || now.Sub(t.agedAt) >= patternStateAgeEvery {
		store.age(t, now, now, retainFrom)
		t.agedAt, t.agedFrom = now, retainFrom
	}
	return entries, fromNs, lines, true
}

// buildPatternStateInBackground builds the selector's state from retainFromNs to now in the background, a window
// at a time, so the tenant's other requests are answered between windows; false when another build runs. The
// build reads with the request's identity, outlives the request and stops with the store.
func (p *Proxy) buildPatternStateInBackground(r *http.Request, t *patternTenantState, selector string, scope patternQueryScope, retainFromNs int64, now time.Time) bool {
	store := p.patternState
	select {
	case store.builds <- struct{}{}:
	default:
		return false
	}
	s := &patternSelectorState{fromNs: retainFromNs, toNs: retainFromNs, streams: make(map[string]struct{}), building: true}
	t.selectors[selector] = s
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	stop := context.AfterFunc(store.ctx, cancel)
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		defer func() { <-store.builds }()
		defer stop()
		defer cancel()
		// The build reads once the request that started it is answered (its context ends with the answer), so
		// that request's own reads are not slowed down by the build's.
		select {
		case <-r.Context().Done():
		case <-ctx.Done():
		case <-time.After(patternStateBuildWait):
		}
		started := time.Now()
		ok := p.readPatternStateWindows(ctx, t, selector, s, scope, retainFromNs, now.UnixNano())
		t.mu.Lock()
		defer t.mu.Unlock()
		if !ok {
			delete(t.selectors, selector)
			p.observeInternalOperation(ctx, "patterns_state", "build_failed", time.Since(started))
			return
		}
		s.building = false
		p.observeInternalOperation(ctx, "patterns_state", "built", time.Since(started))
	}()
	return true
}

// readPatternStateWindows reads the selector's lines in [fromNs, toNs) a window at a time, oldest first, so each
// stream's lines are trained in time order, and folds the streams idle by the end of each window.
func (p *Proxy) readPatternStateWindows(ctx context.Context, t *patternTenantState, selector string, s *patternSelectorState, scope patternQueryScope, fromNs, toNs int64) bool {
	store := p.patternState
	window := int64(patternStateBuildWindow)
	for from := fromNs; from < toNs; {
		to := from + window
		if to > toNs {
			to = toNs
		}
		if ctx.Err() != nil {
			return false
		}
		t.mu.Lock()
		err := p.readPatternStateLines(ctx, t, s, scope, from, to, store.maxBuildLines)
		dropped := false
		if err == nil {
			s.toNs = to
			store.age(t, store.now(), time.Unix(0, to), fromNs)
			// The tenant alone outgrew -patterns-state-max-bytes and was started again: stop building.
			dropped = t.selectors[selector] != s
		}
		t.mu.Unlock()
		if dropped {
			return false
		}
		if err != nil {
			if window <= int64(patternStateBuildWindow)/4 {
				return false
			}
			window /= 4 // VictoriaLogs could not sort as many lines: read less at a time
			continue
		}
		from = to
	}
	return true
}

// countPatternLines is how many lines the scope's filter holds in [fromNs, toNs).
func (p *Proxy) countPatternLines(ctx context.Context, scope patternQueryScope, fromNs, toNs int64) (int, error) {
	lines := -1
	err := p.readPatternCountRows(ctx, patternExactCountQuery(scope.filter), fromNs, toNs, 1, func(v *fj.Value) bool {
		var err error
		lines, err = strconv.Atoi(string(v.GetStringBytes("c")))
		return err == nil
	})
	if err == nil && lines < 0 {
		err = errPatternCounts{"no line count"}
	}
	return lines, err
}

// readPatternStateLines reads the selector's lines in [fromNs, toNs), a stream after another and each oldest
// first, and trains each stream's clusterers on the lines newer than the newest it read.
func (p *Proxy) readPatternStateLines(ctx context.Context, t *patternTenantState, s *patternSelectorState, scope patternQueryScope, fromNs, toNs int64, maxLines int) error {
	levels := p.newLogRowLevels()
	// unread is, per stream, the lines at its newest time this read has not met again yet.
	unread := map[string]map[uint64]int{}
	// With -patterns-scope=selector the selector's lines are mined by one miner per level, whatever their stream;
	// the lines read are still tracked per stream, apart from the other selectors' (each mines them itself).
	selector := ""
	if p.patternsSelectorScope {
		selector = patternSelectorStream(scope.filter.String())
		t.miner.shortWholeLine = true
	}
	return p.readPatternCountRows(ctx, patternExactLinesQuery(scope.filter), fromNs, toNs, maxLines, func(v *fj.Value) bool {
		msg := v.GetStringBytes("_msg")
		if isVLMissingMsgBytes(msg, levels.defaultMsg) {
			return false
		}
		timeNs, ok := parseLokiTimeToUnixNano(string(v.GetStringBytes("_time")))
		if !ok {
			return false
		}
		stream := string(v.GetStringBytes(patternExactStreamField))
		s.streams[stream] = struct{}{}
		minedAs := stream
		if selector != "" {
			minedAs, stream = selector, selector+stream
		}
		newest, seen := t.newest[stream]
		switch {
		case seen && timeNs < newest:
			return true // read already, or older than a line read: Loki skips it
		case seen && timeNs == newest:
			left := unread[stream]
			if left == nil {
				left = maps.Clone(t.atNewest[stream])
				if left == nil {
					left = map[uint64]int{}
				}
				unread[stream] = left
			}
			h := patternLineHash(msg)
			if left[h] > 0 {
				left[h]--
				return true // read already
			}
			t.atNewest[stream][h]++
		default:
			t.newest[stream] = timeNs
			t.atNewest[stream] = map[uint64]int{patternLineHash(msg): 1}
			unread[stream] = map[uint64]int{}
		}
		if len(msg) > 0 {
			t.miner.observeStreamLine(minedAs, patternLevelFromFJ(v, &levels), string(msg), patternSampleStart(timeNs), 1)
		}
		return true
	})
}

// patternSelectorStream is the miner key of a selector's lines under -patterns-scope=selector: a name no stream
// has, the selector's filter after a NUL.
func patternSelectorStream(selector string) string { return "\x00" + selector }

// patternStateTenantKey names a tenant's state: the scope and the identity a request is read with, so tenants
// and their stream filters never share a state.
func (p *Proxy) patternStateTenantKey(r *http.Request) string {
	identity, _ := json.Marshal([]any{
		p.scopeFingerprint(r.Context(), r.Header.Get("X-Scope-OrgID")),
		p.fingerprintFromCtx(r.Context(), r),
		p.backend.String(), p.detectedLevelCacheKey(), p.defaultMsgValue(),
	})
	return string(identity)
}

func (st *patternStateStore) tenant(key string) *patternTenantState {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.clock++
	t := st.tenants[key]
	if t == nil {
		t = &patternTenantState{
			miner:     newPatternMiner(),
			newest:    make(map[string]int64),
			atNewest:  make(map[string]map[uint64]int),
			folded:    make(map[string][]patternFoldedCluster),
			selectors: make(map[string]*patternSelectorState),
		}
		st.tenants[key] = t
	}
	t.used = st.clock
	return t
}

// answer is the selector's patterns in the scope's range: each stream's 10 s samples of a pattern inside the range
// summed into the scope's steps, the steps between its first and last sample 0, and equal patterns of a level
// summed across streams, the most lines first, at most patternLimit. A stream's samples of a pattern are zero-filled
// together, whether one cluster or several hold them (one folded when the stream went idle and one it started when
// it wrote again), as Loki's one cluster would be.
//
// Counts are kept in slices indexed by step (a range has a few hundred steps), a stream's patterns gathered in a
// buffer reused across streams, and a live cluster's pattern written again only when its template changed.
func (t *patternTenantState) answer(s *patternSelectorState, scope patternQueryScope, patternLimit int) []patternResultEntry {
	g := newPatternStepGrid(scope)
	byStream := make(map[string][]*patternClusterer, len(t.miner.miners))
	selector := patternSelectorStream(scope.filter.String())
	for mk, c := range t.miner.miners {
		if _, ok := s.streams[mk.stream]; ok || mk.stream == selector {
			byStream[mk.stream] = append(byStream[mk.stream], c)
		}
	}
	streams := make([]string, 0, len(s.streams)+1)
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	if byStream[selector] != nil {
		streams = append(streams, selector) // -patterns-scope=selector: one miner per level for all the streams
	}
	for _, stream := range streams {
		for _, cluster := range t.folded[stream] {
			g.add(cluster.level, cluster.pattern, cluster.samples)
		}
		for _, c := range byStream[stream] {
			for _, cluster := range c.live {
				if cluster.text == "" {
					cluster.text = t.miner.joinPatternTokens(cluster.tokens, cluster.spacesAfter, cluster.format)
				}
				g.add(c.level, cluster.text, cluster.buckets)
			}
		}
		g.endStream()
	}
	return g.entries(patternLimit)
}

// patternStepKey is a pattern of a level, an entry of an answer.
type patternStepKey struct{ level, pattern string }

// patternStepCounts is a pattern's lines per step over the range, and the steps with a sample (zero-filled ones
// included).
type patternStepCounts struct {
	counts  []int
	present []bool
	total   int
}

// patternStreamGroup is one pattern of the stream being added: its lines per step and the steps it spans.
type patternStreamGroup struct {
	key         patternStepKey
	counts      []int
	first, last int
}

// patternStepGrid adds up the samples of an answer's range by step: the steps from the one the range's start
// falls in to the one its end does.
type patternStepGrid struct {
	scope      patternQueryScope
	step, base int64
	steps      int
	sums       map[patternStepKey]*patternStepCounts
	groups     []patternStreamGroup
	spare      [][]int
}

func newPatternStepGrid(scope patternQueryScope) *patternStepGrid {
	step := int64(scope.step / time.Second)
	if step <= 0 {
		step = 1
	}
	startSec, endSec := scope.startNs/int64(time.Second), (scope.endNs-1)/int64(time.Second)
	base := startSec / step * step
	return &patternStepGrid{scope: scope, step: step, base: base, steps: int((endSec/step*step-base)/step) + 1, sums: map[patternStepKey]*patternStepCounts{}}
}

// add counts a cluster's 10 s samples inside the range into the stream's pattern.
func (g *patternStepGrid) add(level, pattern string, samples map[int64]int) {
	for at, n := range samples {
		if ns := at * int64(time.Second); ns < g.scope.startNs || ns >= g.scope.endNs || n == 0 {
			continue
		}
		idx := int((at/g.step*g.step - g.base) / g.step)
		if idx < 0 || idx >= g.steps {
			continue
		}
		grp := g.group(patternStepKey{level, pattern}, idx)
		grp.counts[idx] += n
		grp.first, grp.last = min(grp.first, idx), max(grp.last, idx)
	}
}

// group is the stream's pattern, started at step idx when the stream has none of it yet.
func (g *patternStepGrid) group(k patternStepKey, idx int) *patternStreamGroup {
	for i := range g.groups {
		if g.groups[i].key == k {
			return &g.groups[i]
		}
	}
	var counts []int
	if n := len(g.spare); n > 0 {
		counts, g.spare = g.spare[n-1], g.spare[:n-1]
	} else {
		counts = make([]int, g.steps)
	}
	g.groups = append(g.groups, patternStreamGroup{key: k, counts: counts, first: idx, last: idx})
	return &g.groups[len(g.groups)-1]
}

// endStream adds the stream's patterns to the answer, each zero-filled between its first and last step.
func (g *patternStepGrid) endStream() {
	for _, grp := range g.groups {
		a := g.sums[grp.key]
		if a == nil {
			a = &patternStepCounts{counts: make([]int, g.steps), present: make([]bool, g.steps)}
			g.sums[grp.key] = a
		}
		for idx := grp.first; idx <= grp.last; idx++ {
			a.counts[idx] += grp.counts[idx]
			a.total += grp.counts[idx]
			a.present[idx] = true
			grp.counts[idx] = 0
		}
		g.spare = append(g.spare, grp.counts)
	}
	g.groups = g.groups[:0]
}

// entries is the answer: the patterns with at least lokiPatternMinLines lines in the range (the others the
// answer's 30-line rule would drop), the most lines first, at most patternLimit.
func (g *patternStepGrid) entries(patternLimit int) []patternResultEntry {
	type ranked struct {
		key patternStepKey
		acc *patternStepCounts
	}
	items := make([]ranked, 0, len(g.sums))
	for k, a := range g.sums {
		if a.total >= lokiPatternMinLines {
			items = append(items, ranked{k, a})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].acc.total != items[j].acc.total {
			return items[i].acc.total > items[j].acc.total
		}
		if items[i].key.level != items[j].key.level {
			return items[i].key.level < items[j].key.level
		}
		return items[i].key.pattern < items[j].key.pattern
	})
	if patternLimit <= 0 || patternLimit > maxPatternResponseLimit {
		patternLimit = maxPatternResponseLimit
	}
	if len(items) > patternLimit {
		items = items[:patternLimit]
	}
	entries := make([]patternResultEntry, 0, len(items))
	for _, item := range items {
		samples := make([][]interface{}, 0, g.steps)
		for idx, present := range item.acc.present {
			if present {
				samples = append(samples, []interface{}{g.base + int64(idx)*g.step, item.acc.counts[idx]})
			}
		}
		entries = append(entries, patternResultEntry{Pattern: item.key.pattern, Level: item.key.level, Samples: samples})
	}
	return entries
}

// age folds the tenant's streams that read no line for the store's idle time before idleAt (now, or the end of
// the window a background build read), drops the samples and selectors older than the retention, and drops the
// least recently used tenants' states while all of them hold more than the store's bytes.
func (st *patternStateStore) age(t *patternTenantState, now, idleAt time.Time, retainFromNs int64) {
	idleBefore := idleAt.Add(-st.idle).UnixNano()
	retainFrom := retainFromNs / int64(time.Second)
	byStream := map[string][]patternMinerKey{}
	for mk := range t.miner.miners {
		byStream[mk.stream] = append(byStream[mk.stream], mk)
	}
	for stream, keys := range byStream {
		if t.newest[stream] >= idleBefore || strings.HasPrefix(stream, "\x00") {
			// A selector's miner (-patterns-scope=selector) stays with its selector; its samples age out below.
			continue
		}
		for _, mk := range keys {
			c := t.miner.miners[mk]
			for _, cluster := range c.live {
				if cluster.total > 0 {
					t.folded[stream] = append(t.folded[stream], patternFoldedCluster{
						level:   c.level,
						pattern: t.miner.joinPatternTokens(cluster.tokens, cluster.spacesAfter, cluster.format),
						samples: cluster.buckets,
					})
				}
			}
			delete(t.miner.miners, mk)
		}
	}
	kept := make(map[*patternClusterer]bool, len(t.miner.miners))
	for _, c := range t.miner.miners {
		kept[c] = true
		for _, cluster := range c.live {
			for at := range cluster.buckets {
				if at < retainFrom {
					delete(cluster.buckets, at)
				}
			}
		}
	}
	order := t.miner.order[:0]
	for _, c := range t.miner.order {
		if kept[c] {
			order = append(order, c)
		}
	}
	t.miner.order = order
	for stream, clusters := range t.folded {
		kept := clusters[:0]
		for _, cluster := range clusters {
			for at := range cluster.samples {
				if at < retainFrom {
					delete(cluster.samples, at)
				}
			}
			if len(cluster.samples) > 0 {
				kept = append(kept, cluster)
			}
		}
		if len(kept) == 0 {
			delete(t.folded, stream)
			delete(t.newest, stream)
			delete(t.atNewest, stream)
		} else {
			t.folded[stream] = kept
		}
	}
	t.ageSelectors(retainFromNs, idleBefore)
	t.bytes = t.size()

	st.mu.Lock()
	defer st.mu.Unlock()
	var total int64
	for _, other := range st.tenants {
		total += other.bytes
	}
	for total > st.maxBytes && len(st.tenants) > 1 {
		oldestKey := ""
		for key, other := range st.tenants {
			if other == t {
				continue
			}
			if oldestKey == "" || other.used < st.tenants[oldestKey].used {
				oldestKey = key
			}
		}
		total -= st.tenants[oldestKey].bytes
		delete(st.tenants, oldestKey)
	}
	if total > st.maxBytes {
		// The tenant alone holds more than the store may: start it again on its next request.
		t.miner = newPatternMiner()
		t.newest = make(map[string]int64)
		t.atNewest = make(map[string]map[uint64]int)
		t.folded = make(map[string][]patternFoldedCluster)
		t.selectors = make(map[string]*patternSelectorState)
		t.bytes = 0
	}
}

// ageSelectors drops the selectors whose lines all fell out of the retention, with their miners under
// -patterns-scope=selector, and a selector's per-stream read marks once the stream went idle.
func (t *patternTenantState) ageSelectors(retainFromNs, idleBefore int64) {
	for selector, s := range t.selectors {
		if s.toNs < retainFromNs && !s.building {
			delete(t.selectors, selector)
			sel := patternSelectorStream(selector)
			for mk := range t.miner.miners {
				if mk.stream == sel {
					delete(t.miner.miners, mk)
				}
			}
		}
	}
	for key, newest := range t.newest {
		// A selector's per-stream marks (-patterns-scope=selector) are dropped once the stream went idle: its lines
		// are mined, and nothing was folded under them.
		if strings.HasPrefix(key, "\x00") && newest < idleBefore {
			delete(t.newest, key)
			delete(t.atNewest, key)
		}
	}
}

// size is an estimate of the memory the tenant's state holds.
func (t *patternTenantState) size() int64 {
	var n int64
	for mk, c := range t.miner.miners {
		n += int64(64 + len(mk.stream))
		for _, cluster := range c.live {
			n += 160 + int64(48*len(cluster.buckets)) + int64(24*len(cluster.tokens))
			for _, token := range cluster.tokens {
				n += int64(len(token))
			}
		}
	}
	for stream, clusters := range t.folded {
		n += int64(64 + len(stream))
		for _, cluster := range clusters {
			n += 64 + int64(len(cluster.pattern)) + int64(48*len(cluster.samples))
		}
	}
	for _, s := range t.selectors {
		n += int64(64 + 48*len(s.streams))
	}
	for _, lines := range t.atNewest {
		n += int64(48 + 24*len(lines))
	}
	return n + int64(48*len(t.newest))
}

// patternLineHash names a line's text among the lines of a stream at one time.
func patternLineHash(msg []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(msg)
	return h.Sum64()
}
