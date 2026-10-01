package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Time-bucketed metadata inventory.
//
// A VictoriaLogs metadata listing (stream_field_names, stream_field_values,
// field_names, field_values) over [start, end) costs time and memory in
// proportion to the rows and distinct streams in the range: on the e2e stack
// a stream_field_names call takes 0.08 s over 5 minutes, 0.6 s over an hour,
// 5.5 s over 6 hours and 35 s over 7 days, and the 7-day one holds 0.8 GiB.
// Loki answers the same /labels from its index in milliseconds. Grafana asks
// again for a window shifted by a few seconds on every refresh, and every
// shifted window used to be a new full-range scan.
//
// The listing of a range is the union of the listings of any partition of
// that range, and its hits are the sum (VictoriaLogs treats end as exclusive,
// so [a, b) and [b, c) never count a row twice). The inventory therefore
// splits a window into UTC-aligned day, hour, 5-minute and minute buckets
// (the largest aligned bucket that fits, from either side), caches each
// sealed bucket in the shared read cache (memory, disk and the peer fleet),
// and asks VictoriaLogs only for what is not cached: the unaligned edges, the
// last minute (never cached), and buckets whose entry is missing or due for
// revalidation. A missing hour or day is merged from its cached children
// before any scan. The merged answer is ordered as VictoriaLogs orders it
// (hits descending, then natural order), so a caller sees exactly the answer
// of a single full-range call.
//
// Freshness. A sealed bucket changes only if rows are written with old
// timestamps. Each entry is revalidated after a delay that grows with the
// bucket's age (the labels TTL for the last hour, up to one hour for buckets
// older than a day, as the endpoint caches already scale by window). Hour and
// day buckets of a query without pipes carry the bucket's row count, read
// before the scan; on revalidation a count (0.03-0.2 s per day for * on the
// e2e stack, against 5-20 s for the scan) that still matches proves the
// bucket unchanged, so its scan is not repeated. Empty buckets of such a query
// that ended within max-metadata-cache-freshness are confirmed on every
// listing by one count per contiguous run (revalidateEmptyRuns), as Loki reads
// that window live.
// Rows in VictoriaLogs are append-only; retention drops whole days.

const (
	// metadataInventoryKeyVersion versions the bucket cache keys.
	metadataInventoryKeyVersion = "vlinv-v1"
	// metadataInventorySealLag keeps the most recent minute out of the
	// cache: rows arriving a few seconds late still land in an uncached
	// segment that every request reads from VictoriaLogs.
	metadataInventorySealLag = time.Minute
	// metadataInventoryEntryTTL is how long a bucket entry is kept at all.
	// Within it, the entry is revalidated on the schedule of
	// metadataInventoryRevalidateAfter.
	metadataInventoryEntryTTL = 24 * time.Hour
	// metadataInventoryMaxSegments bounds the plan of one request; a window
	// that would need more (years at day granularity) is asked in one call.
	metadataInventoryMaxSegments = 1000
	// metadataInventoryMinWindow is the smallest window worth splitting.
	metadataInventoryMinWindow = 10 * time.Minute
	// metadataInventoryMinStartNs keeps bucket bounds in the range
	// VictoriaLogs reads as nanoseconds (it reads shorter integers as
	// seconds, milliseconds or microseconds, and negative ones as relative).
	metadataInventoryMinStartNs = int64(1e14)
	// DefaultMetadataInventoryParallelism is how many bucket listings one
	// request may have in flight. VictoriaLogs already spreads one query
	// over its cores, so more buys little: three concurrent 7-day scans took
	// 1.8x as long as one on the e2e stack.
	DefaultMetadataInventoryParallelism = 4
)

// metadataInventoryLevels are the bucket sizes, largest first; each divides
// the previous one, and all are aligned to the Unix epoch (UTC).
var metadataInventoryLevels = []time.Duration{24 * time.Hour, time.Hour, 5 * time.Minute, time.Minute}

// metadataInventoryPaths are the VictoriaLogs listings whose answers can be
// merged across time buckets.
var metadataInventoryPaths = map[string]bool{
	"/select/logsql/stream_field_names":  true,
	"/select/logsql/stream_field_values": true,
	"/select/logsql/field_names":         true,
	"/select/logsql/field_values":        true,
}

// inventorySegment is one piece of a planned window: [start, end) in Unix
// nanoseconds. level indexes metadataInventoryLevels; -1 is an uncached edge.
type inventorySegment struct {
	start, end int64
	level      int
}

// planInventorySegments splits [start, end) into aligned buckets and
// uncached edges. Only [start, sealBefore) may be bucketed; the rest is one
// edge. From the left, every step takes the largest level aligned at the
// cursor that still fits, or an edge up to the next minute boundary.
func planInventorySegments(start, end, sealBefore int64) []inventorySegment {
	if end <= start {
		return nil
	}
	var out []inventorySegment
	addEdge := func(a, b int64) {
		if b <= a {
			return
		}
		if n := len(out); n > 0 && out[n-1].level < 0 && out[n-1].end == a {
			out[n-1].end = b
			return
		}
		out = append(out, inventorySegment{start: a, end: b, level: -1})
	}
	limit := end
	if sealBefore < limit {
		limit = sealBefore
	}
	finest := int64(metadataInventoryLevels[len(metadataInventoryLevels)-1])
	cur := start
	for cur < limit {
		placed := false
		for li, lvl := range metadataInventoryLevels {
			size := int64(lvl)
			if cur%size == 0 && cur+size <= limit {
				out = append(out, inventorySegment{start: cur, end: cur + size, level: li})
				cur += size
				placed = true
				break
			}
		}
		if placed {
			continue
		}
		next := (cur/finest + 1) * finest
		if cur < 0 && cur%finest != 0 {
			next = (cur / finest) * finest
		}
		if next > limit {
			next = limit
		}
		addEdge(cur, next)
		cur = next
	}
	addEdge(cur, end)
	return out
}

// inventoryEntry is a cached bucket listing.
type inventoryEntry struct {
	Values  []string `json:"v"`
	Hits    []int64  `json:"h"`
	Rows    int64    `json:"r"` // rows in the bucket read before the scan; -1 when unknown
	Checked int64    `json:"c"` // Unix ns of the scan or of the last count that confirmed it
}

// vlValueHits is one entry of a VictoriaLogs listing.
type vlValueHits struct {
	Value string `json:"value"`
	Hits  int64  `json:"hits"`
}

func decodeVLValueHits(body []byte) ([]vlValueHits, error) {
	var resp struct {
		Values []vlValueHits `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return resp.Values, nil
}

// sortVLValueHits orders a listing as VictoriaLogs does: hits descending,
// then natural order of the value.
func sortVLValueHits(items []vlValueHits) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Hits != items[j].Hits {
			return items[i].Hits > items[j].Hits
		}
		return lessNatural(items[i].Value, items[j].Value)
	})
}

// mergeVLValueHits sums the hits of equal values across bucket listings and
// orders the union like VictoriaLogs, which merges the per-partition
// listings of one call the same way (MergeValuesWithHits): when any part
// carries a zero hit count (VictoriaLogs zeroes the hits of a listing it
// truncated), every hit of the union is zeroed.
func mergeVLValueHits(parts ...[]vlValueHits) []vlValueHits {
	total := 0
	zeroed := false
	for _, part := range parts {
		total += len(part)
		for _, item := range part {
			zeroed = zeroed || item.Hits == 0
		}
	}
	index := make(map[string]int, total)
	merged := make([]vlValueHits, 0, total)
	for _, part := range parts {
		for _, item := range part {
			if i, ok := index[item.Value]; ok {
				merged[i].Hits += item.Hits
				continue
			}
			index[item.Value] = len(merged)
			merged = append(merged, item)
		}
	}
	if zeroed {
		for i := range merged {
			merged[i].Hits = 0
		}
	}
	sortVLValueHits(merged)
	return merged
}

// lessNatural compares strings the way VictoriaLogs orders listings with
// equal hits: runs of decimal digits compare by numeric value (then by run
// length), everything else bytewise, and a digit sorts before a non-digit.
func lessNatural(a, b string) bool {
	reverse := false
	for {
		if len(a) > len(b) {
			a, b = b, a
			reverse = !reverse
		}
		i := 0
		for i < len(a) {
			ca, cb := a[i], b[i]
			da, db := isASCIIDigit(ca), isASCIIDigit(cb)
			if da && db {
				break
			}
			if da {
				return !reverse
			}
			if db {
				return reverse
			}
			if ca != cb {
				return (ca < cb) != reverse
			}
			i++
		}
		a, b = a[i:], b[i:]
		if len(a) == 0 {
			return !reverse && len(b) > 0
		}
		na, la, okA := leadingNumber(a)
		nb, lb, okB := leadingNumber(b)
		if !okA || !okB {
			if reverse {
				return b < a
			}
			return a < b
		}
		if na != nb {
			return (na < nb) != reverse
		}
		if la != lb {
			return (la < lb) != reverse
		}
		a, b = a[la:], b[lb:]
	}
}

func isASCIIDigit(c byte) bool { return c >= '0' && c <= '9' }

// leadingNumber parses the run of digits at the start of s. ok is false when
// the run overflows, and the caller falls back to a bytewise comparison.
func leadingNumber(s string) (n uint64, length int, ok bool) {
	n = uint64(s[0] - '0')
	length = 1
	for length < len(s) && isASCIIDigit(s[length]) {
		if n > (math.MaxUint64-9)/10 {
			return 0, 0, false
		}
		n = n*10 + uint64(s[length]-'0')
		length++
	}
	return n, length, true
}

// inventoryQueryShape tells whether a LogsQL metadata query can be answered
// bucket by bucket, and whether a row count proves a bucket unchanged.
//
// Bucketable: every pipe works on one row at a time, there is no subquery
// (which VictoriaLogs would evaluate over the whole range), no _time filter
// and no query options. Countable: the query is *, stream filters and field
// filters only. Counting the rows such a query matches reads block headers
// (for *), the stream index or the filtered fields' columns, less than the
// listing itself, and rows are append-only, so an unchanged count means an
// unchanged set of matching rows and an unchanged listing. Word and phrase
// filters are not counted: their count reads every message, and these counts
// are not admission-limited; nor are pipes, whose count would redo their
// work.
func inventoryQueryShape(query string) (bucketable, countable bool) {
	query = strings.TrimSpace(query)
	if query == "" {
		return false, false
	}
	var (
		unquoted strings.Builder
		pipes    []int
		depth    int
		quote    byte
	)
	for i := 0; i < len(query); i++ {
		c := query[i]
		if quote != 0 {
			if c == '\\' && quote != '`' {
				i++
				continue
			}
			if c == quote {
				quote = 0
				unquoted.WriteByte('"')
			}
			continue
		}
		switch c {
		case '"', '\'', '`':
			quote = c
			unquoted.WriteByte('"')
			continue
		case '(':
			depth++
		case ')':
			depth--
		case '|':
			if depth != 0 {
				return false, false // a subquery
			}
			pipes = append(pipes, unquoted.Len())
		}
		unquoted.WriteByte(c)
	}
	if quote != 0 || depth != 0 {
		return false, false
	}
	text := unquoted.String()
	lower := strings.ToLower(text)
	if strings.Contains(lower, "_time") || strings.Contains(lower, "options(") {
		return false, false
	}
	for idx, at := range pipes {
		stageEnd := len(text)
		if idx+1 < len(pipes) {
			stageEnd = pipes[idx+1]
		}
		stage := strings.TrimSpace(text[at+1 : stageEnd])
		name := stage
		if cut := strings.IndexAny(stage, " (\t"); cut >= 0 {
			name = stage[:cut]
		}
		if !rowLocalLogsQLPipes[strings.ToLower(name)] {
			return false, false
		}
	}
	return true, len(pipes) == 0 && countableFilters(text)
}

// streamFilterGroup matches a stream filter's braces once quoted strings have
// been collapsed (inventoryQueryShape).
var streamFilterGroup = regexp.MustCompile(`\{[^{}]*\}`)

// countableFilters reports whether a pipe-free LogsQL filter, with its quoted
// strings collapsed to a single quote, consists only of *, stream filters
// ({...} or _stream:{...}) and field filters (name:...) other than on _msg,
// joined by and/or/not, - and parentheses: filters whose count reads block
// headers, the stream index or single columns, never every message.
func countableFilters(text string) bool {
	text = streamFilterGroup.ReplaceAllString(text, "{}")
	text = strings.NewReplacer("(", " ", ")", " ").Replace(text)
	for _, token := range strings.Fields(text) {
		token = strings.ToLower(strings.TrimLeft(token, "!-"))
		switch {
		case token == "" || token == "*" || token == "and" || token == "or" || token == "not":
		case token == "{}" || strings.HasPrefix(token, "_stream:"):
		case strings.HasPrefix(token, "_msg:") || strings.HasPrefix(token, `"`):
			return false
		case strings.Index(token, ":") > 0:
		default:
			return false
		}
	}
	return true
}

// rowLocalLogsQLPipes are the pipes whose output for a row depends on that
// row only, so a listing over a range is the union of listings over its parts.
var rowLocalLogsQLPipes = map[string]bool{
	"filter": true, "where": true, "format": true, "extract": true, "extract_regexp": true,
	"unpack_json": true, "unpack_logfmt": true, "unpack_syslog": true, "unpack_words": true,
	"unroll": true, "copy": true, "cp": true, "rename": true, "mv": true, "delete": true,
	"del": true, "rm": true, "drop": true, "fields": true, "keep": true, "replace": true,
	"replace_regexp": true, "math": true, "eval": true, "drop_empty_fields": true,
	"pack_json": true, "pack_logfmt": true, "len": true, "decolorize": true,
	"collapse_nums": true, "coalesce": true, "split": true,
}

// metadataInventoryRevalidateAfter is how long a bucket entry is served
// before it is revalidated: the base TTL for buckets that ended within the
// last hour, then 3x, 10x and 20x as the bucket ages past 6 h and a day, at
// most an hour (or the base, when the base is longer).
func metadataInventoryRevalidateAfter(bucketEnd, now int64, base time.Duration) time.Duration {
	if base <= 0 {
		base = CacheTTLs["labels"]
	}
	age := time.Duration(now - bucketEnd)
	after := base
	switch {
	case age <= time.Hour:
	case age <= 6*time.Hour:
		after = 3 * base
	case age <= 24*time.Hour:
		after = 10 * base
	default:
		after = 20 * base
	}
	ceiling := time.Hour
	if base > ceiling {
		ceiling = base
	}
	if after > ceiling {
		after = ceiling
	}
	return after
}

// resolveMetadataInventoryParallelism maps 0 to the default and a negative
// value to "inventory off".
func resolveMetadataInventoryParallelism(configured int) int {
	switch {
	case configured < 0:
		return 0
	case configured == 0:
		return DefaultMetadataInventoryParallelism
	default:
		return configured
	}
}

// metadataInventoryEnabled reports whether listings may be bucketed at all.
func (p *Proxy) metadataInventoryEnabled() bool {
	return p != nil && p.metadataInventoryParallelism > 0 && p.cache != nil && p.cache.MaxEntrySizeBytes() > 0
}

// inventoryPlan decides whether a listing is answered from buckets and
// returns the plan. The plan must hold at least one cacheable bucket.
func (p *Proxy) inventoryPlan(path string, params url.Values, now time.Time) ([]inventorySegment, bool) {
	if !p.metadataInventoryEnabled() || !metadataInventoryPaths[path] {
		return nil, false
	}
	if limit := strings.TrimSpace(params.Get("limit")); limit != "" && limit != "0" {
		// A truncated listing of a range is not the union of truncated
		// listings of its parts.
		return nil, false
	}
	if bucketable, _ := inventoryQueryShape(params.Get("query")); !bucketable {
		return nil, false
	}
	startNs, okStart := parseLokiTimeToUnixNano(params.Get("start"))
	endNs, okEnd := parseLokiTimeToUnixNano(params.Get("end"))
	if !okStart || !okEnd || endNs-startNs < int64(metadataInventoryMinWindow) || startNs < metadataInventoryMinStartNs {
		return nil, false
	}
	plan := planInventorySegments(startNs, endNs, now.Add(-metadataInventorySealLag).UnixNano())
	if len(plan) == 0 || len(plan) > metadataInventoryMaxSegments {
		return nil, false
	}
	for _, seg := range plan {
		if seg.level >= 0 {
			return plan, true
		}
	}
	return nil, false
}

// inventoryKeyBase identifies the listing a bucket belongs to: tenant, auth
// scope, endpoint and every parameter but the time range.
func (p *Proxy) inventoryKeyBase(ctx context.Context, path string, params url.Values) string {
	rest := url.Values{}
	for k, vs := range params {
		if k == "start" || k == "end" {
			continue
		}
		rest[k] = vs
	}
	return p.metadataFieldNamesCacheKey(ctx, metadataInventoryKeyVersion+":"+path+":", rest)
}

func inventoryBucketKey(base string, seg inventorySegment) string {
	return base + ":" + metadataInventoryLevels[seg.level].String() + ":" + strconv.FormatInt(seg.start, 10)
}

// fetchVLListing returns a VictoriaLogs metadata listing with hits, in
// VictoriaLogs' order, from the bucket inventory when the listing allows it.
func (p *Proxy) fetchVLListing(ctx context.Context, path string, params url.Values) ([]vlValueHits, error) {
	now := time.Now()
	plan, ok := p.inventoryPlan(path, params, now)
	if !ok {
		return p.fetchVLListingOnce(ctx, path, params)
	}
	_, countable := inventoryQueryShape(params.Get("query"))
	base := p.inventoryKeyBase(ctx, path, params)
	if _, oversize := p.cache.Get(base + ":oversize"); oversize {
		return p.fetchVLListingOnce(ctx, path, params)
	}
	// The day bucket scans of a long-range listing go through the
	// metadata-scan limiter one scan at a time (inventoryScanKey). Hour and
	// shorter buckets are short scans, like any short listing, and edges and
	// minute buckets, the whole cost of a refresh, never wait.
	longListing := p.metadataScanLimiter != nil && isMetadataScanRequest(path, params, p.backendHeavyQueryMinRange)
	parts := make([][]vlValueHits, len(plan))
	// A bucket that fails (most often a 429 from the admission limiter) stops
	// new buckets from starting, but does not cancel the scans already
	// running: they finish and are cached, so the retry that follows a 429
	// continues where this request stopped instead of starting over. Only
	// the request's own context cancels them.
	if countable {
		p.revalidateEmptyRuns(ctx, path, params, base, plan)
	}
	var group errgroup.Group
	group.SetLimit(p.metadataInventoryParallelism)
	var failed atomic.Bool
	var scanned, reused, revalidated int
	var statsMu sync.Mutex
	for i, seg := range plan {
		group.Go(func() error {
			if failed.Load() {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			items, outcome, err := p.fetchInventorySegment(ctx, path, params, base, seg, countable, longListing)
			if err != nil {
				failed.Store(true)
				return err
			}
			parts[i] = items
			statsMu.Lock()
			switch outcome {
			case "scanned":
				scanned++
			case "revalidated":
				revalidated++
			default:
				reused++
			}
			statsMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		if errors.Is(err, errInventoryBucketTooLarge) && ctx.Err() == nil {
			// Remembered for an hour, so later listings of the same kind go
			// straight to one call.
			p.cache.SetWithTTL(base+":oversize", []byte("1"), time.Hour)
			return p.fetchVLListingOnce(ctx, path, params)
		}
		return nil, err
	}
	p.observeInternalOperation(ctx, "metadata_inventory", inventoryOutcome(scanned, revalidated, reused), time.Since(now))
	merged := mergeVLValueHits(parts...)
	if err := p.checkMergedListingCap(ctx, path, merged); err != nil {
		return nil, err
	}
	return merged, nil
}

// vlListingEncodedSize is the size of the response VictoriaLogs sends for a
// listing of items from one call: {"values":[...]} with one
// {"hits":N,"value":"..."} per item, the items comma separated, and a final
// newline. It is the size the response cap sees on the one-call path.
func vlListingEncodedSize(items []vlValueHits) int64 {
	size := int64(len(`{"values":[]}`)) + 1
	for i, item := range items {
		quoted, _ := json.Marshal(item.Value)
		size += int64(len(`{"hits":,"value":}`)) + int64(len(quoted)) + int64(len(strconv.FormatInt(item.Hits, 10)))
		if i > 0 {
			size++
		}
	}
	return size
}

// checkMergedListingCap applies the label values response cap to the merged
// inventory listing, so the cap bounds the answer the client would get from
// one VictoriaLogs call over the whole range and not each bucket read, and
// buckets another request filled without a cap count too.
func (p *Proxy) checkMergedListingCap(ctx context.Context, path string, merged []vlValueHits) error {
	limit, ok := ctx.Value(metadataResponseCapKey{}).(int64)
	if !ok || limit <= 0 {
		return nil
	}
	size := vlListingEncodedSize(merged)
	if size <= limit {
		return nil
	}
	p.observeInternalOperation(ctx, "label_values_response_cap", "rejected", 0)
	p.log.Warn("label values response exceeds -label-values-max-response-bytes",
		"backend.route", path, "bytes_read", size, "limit", limit, "limit_flag", "-label-values-max-response-bytes")
	return &labelValuesResponseTooLargeError{read: size, limit: limit}
}

func inventoryOutcome(scanned, revalidated, reused int) string {
	switch {
	case scanned == 0 && revalidated == 0:
		return "cached"
	case scanned == 0:
		return "revalidated"
	case reused == 0 && revalidated == 0:
		return "scanned"
	default:
		return "partial"
	}
}

// fetchInventorySegment answers one planned segment: an edge from
// VictoriaLogs, a bucket from the cache, its cached children, a confirming
// row count, or a scan.
func (p *Proxy) fetchInventorySegment(ctx context.Context, path string, params url.Values, base string, seg inventorySegment, countable, longListing bool) ([]vlValueHits, string, error) {
	if seg.level < 0 {
		items, err := p.fetchVLListingOnce(ctx, path, withTimeRange(params, seg.start, seg.end))
		return items, "scanned", err
	}
	key := inventoryBucketKey(base, seg)
	entry, ok := p.loadInventoryEntry(key)
	if ok && p.inventoryEntryFresh(path, seg, entry) {
		return entry.items(), "cached", nil
	}
	flightKey := key
	if isBackgroundInventory(ctx) {
		flightKey += ":bg"
	}
	// A capped fill fails where an uncapped one succeeds, so the two never
	// share a flight.
	flightKey += responseCapKeySuffix(ctx)
	type result struct {
		items   []vlValueHits
		outcome string
	}
	fill := func() (interface{}, error) {
		if ok {
			if confirmed := p.revalidateInventoryEntry(ctx, key, entry, seg, params, countable); confirmed {
				return result{entry.items(), "revalidated"}, nil
			}
		} else if merged, composed := p.composeInventoryEntry(path, base, seg); composed {
			if err := p.storeInventoryEntry(key, merged); err != nil {
				return nil, err
			}
			return result{merged.items(), "cached"}, nil
		}
		// An empty 5m or 1m bucket that an empty-run count found rows in is
		// counted too (the run's count is cached), so it leaves the run.
		fresh, err := p.scanInventoryBucket(ctx, path, params, seg, countable && (seg.level <= 1 || ok && len(entry.Values) == 0), longListing && seg.level == 0)
		if err != nil {
			return nil, err
		}
		if err := p.storeInventoryEntry(key, fresh); err != nil {
			return nil, err
		}
		return result{fresh.items(), "scanned"}, nil
	}
	// A fill is shared by every request that needs the bucket at that moment
	// and runs under its leader's context. When the leader goes away (Grafana
	// cancels the previous refresh) or is refused, a follower whose own
	// request is still alive fills the bucket itself instead of inheriting
	// that outcome.
	for attempt := 0; ; attempt++ {
		led := false // singleflight reports shared to the leader too
		v, err, _ := p.inventoryGroup.Do(flightKey, func() (interface{}, error) {
			led = true
			return fill()
		})
		if err == nil {
			r := v.(result)
			return r.items, r.outcome, nil
		}
		if attempt >= 2 || ctx.Err() != nil || !inheritedFailure(err, !led) || (led && errors.Is(err, context.DeadlineExceeded)) {
			return nil, "", err
		}
	}
}

// inheritedFailure reports errors that belong to another request than the
// one retrying: a cancellation or an expired deadline while this request is
// alive (the leader of a shared inventory fill, or of the VictoriaLogs call it
// coalesced with, went away or ran out of its own time), or an admission
// refusal of a fill this request only followed. The caller retries only while
// its own context is alive.
func inheritedFailure(err error, followed bool) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (followed && isHeavyQueryQueueFull(err))
}

// inventoryLiveWindow is how far back an empty, count-checkable bucket is
// revalidated at the negative TTL: Loki's max_metadata_cache_freshness, or an
// hour when it is off.
func (p *Proxy) inventoryLiveWindow() time.Duration {
	if p.metadataCacheFreshness > 0 {
		return p.metadataCacheFreshness
	}
	return time.Hour
}

// emptyRunBucket is an empty bucket of a countable plan: the entry that holds
// its confirmation and the bucket itself.
type emptyRunBucket struct {
	key   string
	seg   inventorySegment
	entry inventoryEntry
}

// revalidateEmptyRuns confirms the empty buckets of a plan that ended within
// the live window, with one row count over each contiguous run of such
// buckets instead of one call per bucket, on every listing: Loki reads the
// last max_metadata_cache_freshness live, and rows written with old timestamps
// (a shipper outage, a replay) land in buckets cached empty a moment before.
// A count over an empty range reads no data. A run that counts zero rows is
// unchanged: every bucket in it is confirmed. A run that counts rows is halved
// until the buckets that received rows are found; those are made due, so the
// per-bucket path rescans them now, and the empty ones are confirmed. Buckets
// of every size take part: an empty 5m or 1m bucket carries no row count from
// its scan, and one that holds rows its listing does not show (a values
// listing of a field those rows lack) is rescanned once with its count and
// leaves the run. The count must be taken during this listing; an entry is
// rewritten only when its own schedule made it due, so a refresh adds one
// count per run and no cache writes.
func (p *Proxy) revalidateEmptyRuns(ctx context.Context, path string, params url.Values, base string, plan []inventorySegment) {
	now := cache.Now().UnixNano()
	var run []emptyRunBucket
	flush := func() {
		if len(run) > 0 {
			p.confirmEmptyRun(ctx, path, params.Get("query"), run, now)
		}
		run = nil
	}
	for _, seg := range plan {
		var bucket emptyRunBucket
		ok := false
		if seg.level >= 0 {
			key := inventoryBucketKey(base, seg)
			if e, found := p.loadInventoryEntry(key); found && len(e.Values) == 0 && e.Rows <= 0 && seg.end > now-int64(p.inventoryLiveWindow()) {
				bucket, ok = emptyRunBucket{key: key, seg: seg, entry: e}, true
			}
		}
		if !ok || (len(run) > 0 && run[len(run)-1].seg.end != seg.start) {
			flush()
		}
		if ok {
			run = append(run, bucket)
		}
	}
	flush()
}

// confirmEmptyRun counts the rows of a contiguous run of empty buckets with a
// count taken at or after notBefore (the start of the listing) and confirms
// the buckets that are still empty, see revalidateEmptyRuns.
func (p *Proxy) confirmEmptyRun(ctx context.Context, path, query string, run []emptyRunBucket, notBefore int64) {
	span := inventorySegment{start: run[0].seg.start, end: run[len(run)-1].seg.end, level: -1}
	rows, at, err := p.countInventoryRows(ctx, query, span, notBefore)
	if err != nil {
		return // the per-bucket path counts or scans each bucket
	}
	if rows == 0 {
		for _, b := range run {
			if b.entry.Rows == 0 && p.inventoryEntryFresh(path, b.seg, b.entry) {
				continue // served as cached: nothing to rewrite
			}
			b.entry.Rows, b.entry.Checked = 0, at
			_ = p.storeInventoryEntry(b.key, b.entry)
		}
		return
	}
	if len(run) == 1 {
		// This bucket received rows: due now, so the per-bucket path rescans
		// it even if it was confirmed a moment ago.
		b := run[0]
		b.entry.Checked = 0
		_ = p.storeInventoryEntry(b.key, b.entry)
		return
	}
	mid := len(run) / 2
	p.confirmEmptyRun(ctx, path, query, run[:mid], notBefore)
	p.confirmEmptyRun(ctx, path, query, run[mid:], notBefore)
}

// inventoryEntryFresh reports whether a bucket entry may be served without
// revalidation. Ages are read on the cache clock, like the entry's expiry.
// An empty listing of a bucket that ended within the last hour is revalidated
// after the negative TTL, like every other empty metadata answer: VictoriaLogs
// answers empty while data it has not yet made searchable is on its way.
func (p *Proxy) inventoryEntryFresh(path string, seg inventorySegment, e inventoryEntry) bool {
	now := cache.Now().UnixNano()
	base := p.inventoryBaseTTL(path)
	after := metadataInventoryRevalidateAfter(seg.end, now, base)
	if e.Rows >= 0 && after > 3*base {
		// A counted bucket is confirmed by a count, which is cheap: rows
		// written late into old hours show up within three labels TTLs.
		after = 3 * base
	}
	// An empty bucket is revalidated after the negative TTL while it is recent:
	// rows may not be searchable yet, or may be backfilled with old timestamps
	// (a shipper outage). Loki reads the last max-metadata-cache-freshness
	// live: for a countable query every empty bucket in that window is
	// confirmed on each listing by one count over its run
	// (revalidateEmptyRuns), so this schedule applies only when that count
	// fails. An hour or day bucket that carries a row count then keeps the
	// negative TTL for the whole window; other empty buckets (5m and 1m
	// buckets, word, phrase or pipe filters) keep it for the last hour only;
	// older ones follow the age schedule.
	if len(e.Values) == 0 {
		window := time.Hour
		if e.Rows >= 0 && seg.level <= 1 {
			if live := p.inventoryLiveWindow(); live > window {
				window = live
			}
		}
		if seg.end > now-int64(window) {
			if negative := p.metadataNegativeTTL(); negative < after {
				after = negative
			}
		}
	}
	return now-e.Checked < int64(after)
}

func (e inventoryEntry) items() []vlValueHits {
	out := make([]vlValueHits, len(e.Values))
	for i, v := range e.Values {
		out[i] = vlValueHits{Value: v}
		if i < len(e.Hits) {
			out[i].Hits = e.Hits[i]
		}
	}
	return out
}

func newInventoryEntry(items []vlValueHits, rows int64, checked time.Time) inventoryEntry {
	e := inventoryEntry{Values: make([]string, len(items)), Hits: make([]int64, len(items)), Rows: rows, Checked: checked.UnixNano()}
	for i, item := range items {
		e.Values[i], e.Hits[i] = item.Value, item.Hits
	}
	return e
}

func (p *Proxy) inventoryBaseTTL(path string) time.Duration {
	if strings.HasSuffix(path, "_values") {
		if p.cacheTTLLabelValues > 0 {
			return p.cacheTTLLabelValues
		}
		return CacheTTLs["label_values"]
	}
	if p.cacheTTLLabels > 0 {
		return p.cacheTTLLabels
	}
	return CacheTTLs["labels"]
}

func (p *Proxy) loadInventoryEntry(key string) (inventoryEntry, bool) {
	raw, ok := p.cache.Get(key)
	if !ok {
		return inventoryEntry{}, false
	}
	var e inventoryEntry
	if json.Unmarshal(raw, &e) != nil || len(e.Hits) != len(e.Values) {
		return inventoryEntry{}, false
	}
	return e, true
}

// errInventoryBucketTooLarge reports a bucket listing larger than the read
// cache keeps: the listing is then one full-range call, which costs what it
// cost before the inventory.
var errInventoryBucketTooLarge = errors.New("metadata inventory bucket exceeds the cache entry size")

func (p *Proxy) storeInventoryEntry(key string, e inventoryEntry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(raw) > p.cache.MaxEntrySizeBytes() {
		return errInventoryBucketTooLarge
	}
	p.cache.SetWithTTL(key, raw, metadataInventoryEntryTTL)
	return nil
}

// composeInventoryEntry builds a bucket from its children at the next finer
// level when every child is cached locally and none is due for revalidation.
// Its row count is the sum of the children's, when all of them carry one.
func (p *Proxy) composeInventoryEntry(path, base string, seg inventorySegment) (inventoryEntry, bool) {
	child := seg.level + 1
	if child >= len(metadataInventoryLevels) {
		return inventoryEntry{}, false
	}
	size := int64(metadataInventoryLevels[child])
	parts := make([][]vlValueHits, 0, (seg.end-seg.start)/size)
	rows := int64(0)
	checked := cache.Now().UnixNano()
	for start := seg.start; start < seg.end; start += size {
		c := inventorySegment{start: start, end: start + size, level: child}
		raw, _, ok := p.cache.GetWithTTL(inventoryBucketKey(base, c))
		if !ok {
			return inventoryEntry{}, false
		}
		var e inventoryEntry
		if json.Unmarshal(raw, &e) != nil || len(e.Hits) != len(e.Values) {
			return inventoryEntry{}, false
		}
		// A child is only as good as its own revalidation schedule.
		if !p.inventoryEntryFresh(path, c, e) {
			return inventoryEntry{}, false
		}
		if e.Checked < checked {
			checked = e.Checked
		}
		if rows >= 0 && e.Rows >= 0 {
			rows += e.Rows
		} else {
			rows = -1
		}
		parts = append(parts, e.items())
	}
	merged := newInventoryEntry(mergeVLValueHits(parts...), rows, time.Unix(0, checked))
	return merged, true
}

// scanInventoryBucket lists one bucket from VictoriaLogs. When the bucket can
// be counted, the count is read first: a row written between the count and
// the scan then makes the next count differ, which forces a rescan, while
// counting after the scan could confirm a listing that missed that row.
func (p *Proxy) scanInventoryBucket(ctx context.Context, path string, params url.Values, seg inventorySegment, count, admitted bool) (inventoryEntry, error) {
	rows := int64(-1)
	if count {
		// Any count taken before the scan will do: a count lower than the
		// rows the scan saw only forces an earlier rescan.
		if n, _, err := p.countInventoryRows(ctx, params.Get("query"), seg, 0); err == nil {
			rows = n
		}
	}
	checked := cache.Now()
	scanCtx := withScanWork(ctx, rows)
	if admitted {
		scanCtx = context.WithValue(scanCtx, inventoryScanKey{}, true)
	}
	items, err := p.fetchVLListingOnce(scanCtx, path, withTimeRange(params, seg.start, seg.end))
	if err != nil {
		return inventoryEntry{}, err
	}
	return newInventoryEntry(items, rows, checked), nil
}

// revalidateInventoryEntry confirms a cached bucket by its row count and
// refreshes its schedule. It reports false when the bucket must be scanned.
func (p *Proxy) revalidateInventoryEntry(ctx context.Context, key string, entry inventoryEntry, seg inventorySegment, params url.Values, countable bool) bool {
	if !countable || entry.Rows < 0 {
		return false
	}
	// The count must be newer than the entry's last confirmation.
	rows, at, err := p.countInventoryRows(ctx, params.Get("query"), seg, entry.Checked+1)
	if err != nil || rows != entry.Rows {
		return false
	}
	entry.Checked = at
	return p.storeInventoryEntry(key, entry) == nil
}

// inventoryCountKey marks the row counts that revalidate inventory buckets:
// they read less than the listing they stand in for (block headers for *),
// so they bypass the heavy-query admission their time range would select.
type inventoryCountKey struct{}

func isInventoryCount(ctx context.Context) bool { return ctx.Value(inventoryCountKey{}) != nil }

// inventoryCount is a cached row count of one bucket for one query.
type inventoryCount struct {
	Rows int64 `json:"r"`
	At   int64 `json:"a"` // Unix ns on the cache clock, taken before the count
}

// countInventoryRows counts the rows a query without pipes matches in a
// bucket, reusing a count taken at or after notBefore: a count depends on the
// tenant, auth scope, query and bucket only, so the names listing and every
// field's values listing of the same query share it for one labels TTL. It
// returns the count and when it was taken.
func (p *Proxy) countInventoryRows(ctx context.Context, query string, seg inventorySegment, notBefore int64) (int64, int64, error) {
	key := p.metadataFieldNamesCacheKey(ctx, metadataInventoryKeyVersion+":count:", url.Values{"query": {strings.TrimSpace(query)}}) +
		":" + strconv.FormatInt(seg.start, 10) + ":" + strconv.FormatInt(seg.end, 10)
	if raw, ok := p.cache.Get(key); ok {
		var c inventoryCount
		if json.Unmarshal(raw, &c) == nil && c.At >= notBefore {
			return c.Rows, c.At, nil
		}
	}
	at := cache.Now().UnixNano()
	rows, err := p.countInventoryRowsOnce(ctx, query, seg)
	if err != nil {
		return 0, 0, err
	}
	if raw, err := json.Marshal(inventoryCount{Rows: rows, At: at}); err == nil {
		p.cache.SetLocalOnlyWithTTL(key, raw, p.inventoryBaseTTL("/select/logsql/stream_field_names"))
	}
	return rows, at, nil
}

// countInventoryRowsOnce asks VictoriaLogs for the count.
func (p *Proxy) countInventoryRowsOnce(ctx context.Context, query string, seg inventorySegment) (int64, error) {
	params := url.Values{}
	// A newline ends a trailing "# comment" in the query.
	params.Set("query", strings.TrimSpace(query)+"\n| count() as rows")
	params.Set("start", strconv.FormatInt(seg.start, 10))
	params.Set("end", strconv.FormatInt(seg.end, 10))
	if !p.breaker.Allow() {
		return 0, errors.New("circuit breaker open")
	}
	resp, err := p.vlGetInner(context.WithValue(ctx, inventoryCountKey{}, true), "/select/logsql/query", params)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, 64<<10)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("inventory row count: status %d", resp.StatusCode)
	}
	// VictoriaLogs answers a count over any range, empty or not, with one
	// row; anything else leaves the bucket unconfirmed.
	var row struct {
		Rows string `json:"rows"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &row); err != nil {
		return 0, err
	}
	return strconv.ParseInt(row.Rows, 10, 64)
}

// fetchVLListingOnce is one VictoriaLogs listing call over the params' range.
func (p *Proxy) fetchVLListingOnce(ctx context.Context, path string, params url.Values) ([]vlValueHits, error) {
	status, body, err := p.vlGetMetadataCoalesced(ctx, path, params)
	for attempt := 0; err != nil && attempt < 2 && ctx.Err() == nil && errors.Is(err, context.Canceled); attempt++ {
		// The call was coalesced with another request's identical call and
		// that request went away; this one is still alive.
		status, body, err = p.vlGetMetadataCoalesced(ctx, path, params)
	}
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, p.redactedBackendStatusError("", status, body)
	}
	return decodeVLValueHits(body)
}

func withTimeRange(params url.Values, start, end int64) url.Values {
	out := make(url.Values, len(params)+2)
	for k, vs := range params {
		out[k] = vs
	}
	out.Set("start", strconv.FormatInt(start, 10))
	out.Set("end", strconv.FormatInt(end, 10))
	return out
}

func listingValues(items []vlValueHits) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.Value
	}
	return out
}
