package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"
	"golang.org/x/sync/errgroup"
)

// Segmented instant volume.
//
// An instant /index/volume over [start, end) is a VictoriaLogs stats query,
// sum_len(_msg) grouped by the plan's fields. That sum is additive over
// disjoint time ranges for every group (VictoriaLogs reads `end` as exclusive
// since v1.36.0, so [a, b) and [b, c) never count a row twice), and the Loki
// name of a stats row (volumeNamesForRow) depends on the row alone. Loki sums
// its own time splits the same way and applies the limit once to the merged
// volumes (pkg/storage/stores/index/seriesvolume/volume.go:57-98, Merge and
// MapToVolumeResponse). So the volume of a window is computed from the rows of
// any partition of it, summed per row, then named, limited and ordered by the
// unchanged volumeResult.
//
// The window is split like the metadata inventory (planInventorySegments):
// UTC-aligned day, hour, 5-minute and minute segments, plus edges. A segment
// that ended before now minus -max-stats-cache-freshness (Loki's
// max_stats_cache_freshness, pkg/querier/queryrange/volume_cache.go:85-96) is
// sealed: its rows are kept in the shared read cache (memory, disk and the
// owner peer). The edges, which include everything newer than the freshness
// window, are read from VictoriaLogs on every request. A Logs Drilldown refresh
// moves the window by seconds, so it reads only its two edges.
//
// Missing sealed segments of one level that follow each other are filled by
// one stats_query_range call whose step is the level (VictoriaLogs' buckets are
// epoch-aligned [T, T+step), exactly the segments), so a cold window costs a
// handful of calls rather than one per segment. Requests whose rows
// VictoriaLogs would limit (plan.pushLimit: it keeps its own top N, with its
// own tie order), short windows and queries that are not row-local keep the
// single whole-window call. A rejected optimised query, a response or segment
// over its size limit, segment rows beyond -backend-max-buffered-response-bytes
// for the request, or an unexpected bucket also sends the whole request to
// that call: segment rows are never mixed with rows of another query form.
// Rows are folded into one sum as they are read, so a request holds one row
// per group plus the segments in flight.

const (
	// volumeSegmentKeyVersion versions the segment cache keys.
	volumeSegmentKeyVersion = "vlvolseg-v1"
	// DefaultMaxStatsCacheFreshness is Loki's max_stats_cache_freshness
	// default (pkg/validation/limits.go:413 in v3.7.9).
	DefaultMaxStatsCacheFreshness = 10 * time.Minute
)

var (
	// errVolumeSegmentTooLarge is a segment whose rows exceed the read cache's
	// entry size, or segment rows of one request beyond the bytes one
	// whole-window answer may have: the request is answered by one
	// whole-window call, and so are its kind's for an hour.
	errVolumeSegmentTooLarge = errors.New("volume segment exceeds the cache entry size")
	// errVolumeSegmentBucket is a stats_query_range bucket that is not one of
	// the requested segments.
	errVolumeSegmentBucket = errors.New("volume segment fill returned an unexpected bucket")
)

// volumeSegmentLookupParallelism bounds the cache lookups of one request
// that run side by side.
const volumeSegmentLookupParallelism = 16

// volumeSegmentFJPool parses segment answers and cached segments; every
// string kept from a parse is a copy.
var volumeSegmentFJPool fj.ParserPool

// volumeSegmentRow is one VictoriaLogs stats row of a segment: its labels as
// alternating name and value, sorted by name, and its bytes.
type volumeSegmentRow struct {
	kv    []string
	bytes uint64
}

// volumeSegmentEntry is the cached form of a segment.
type volumeSegmentEntry struct {
	Rows  [][]string `json:"r"`
	Bytes []uint64   `json:"b"`
}

// volumeSegmentsSealBefore is the end of what may be cached: Loki does not
// cache a volume ending within max_stats_cache_freshness of now, and the
// newest minute is never cached whatever the setting.
func (p *Proxy) volumeSegmentsSealBefore(now time.Time) int64 {
	return now.Add(-max(p.maxStatsCacheFreshness, metadataInventorySealLag)).UnixNano()
}

// volumeSegmentPlan returns the segments of an instant volume request, or
// false when it is answered by one whole-window call.
func (p *Proxy) volumeSegmentPlan(req volumeRequest, plan volumePlan, statsQuery string, now time.Time) ([]inventorySegment, bool) {
	if req.stepNs != 0 || plan.pushLimit || !p.metadataInventoryEnabled() {
		return nil, false
	}
	if req.endNs-req.startNs < int64(metadataInventoryMinWindow) || req.startNs < metadataInventoryMinStartNs {
		return nil, false
	}
	cut := strings.LastIndex(statsQuery, " | stats by (")
	if cut < 0 {
		return nil, false
	}
	if bucketable, _ := inventoryQueryShape(statsQuery[:cut]); !bucketable {
		return nil, false
	}
	segs := planInventorySegments(req.startNs, req.endNs, p.volumeSegmentsSealBefore(now))
	if len(segs) == 0 || len(segs) > metadataInventoryMaxSegments {
		return nil, false
	}
	for _, seg := range segs {
		if seg.level >= 0 {
			return segs, true
		}
	}
	return nil, false
}

// volumeSegmentKeyBase identifies the stats query a segment belongs to:
// tenant, routing scope, auth scope and the rendered query, which carries the
// translated selector, tenant filters and every grouping field. It has no
// limit: segments hold every row.
func (p *Proxy) volumeSegmentKeyBase(ctx context.Context, statsQuery string) string {
	orgID := getOrgID(ctx)
	sum := sha256.Sum256([]byte(statsQuery))
	return volumeSegmentKeyVersion + ":" + orgID + ":routing=" + p.scopeFingerprint(ctx, orgID) +
		":auth=" + p.contextScopeFingerprint(ctx) + ":" + hex.EncodeToString(sum[:])
}

func volumeSegmentKey(base string, seg inventorySegment) string {
	return base + ":" + metadataInventoryLevels[seg.level].String() + ":" + strconv.FormatInt(seg.start, 10)
}

// volumeSegmentTTL is how long a sealed segment is kept: the metadata
// inventory's schedule over the labels TTL, which grows with the segment's age
// (rows written late into an old segment show up within it).
func (p *Proxy) volumeSegmentTTL(seg inventorySegment, now int64) time.Duration {
	return metadataInventoryRevalidateAfter(seg.end, now, p.inventoryBaseTTL("/select/logsql/stream_field_names"))
}

// computeVolumeSegmented answers an instant volume from segments. handled is
// false when the request is not segmented or must be computed whole; the
// caller then makes the whole-window call.
func (p *Proxy) computeVolumeSegmented(ctx context.Context, req volumeRequest, logsqlQuery string, plan volumePlan) (map[string]interface{}, bool, error) {
	started := time.Now()
	statsQuery := volumeStatsQuery(volumeRequest{limit: req.limit}, logsqlQuery, plan, true)
	segs, ok := p.volumeSegmentPlan(req, plan, statsQuery, started)
	if !ok {
		return nil, false, nil
	}
	base := p.volumeSegmentKeyBase(ctx, statsQuery)
	if _, oversize := p.cache.Get(base + ":oversize"); oversize {
		return nil, false, nil
	}
	// Every segment's rows are folded into one sum as soon as they are read,
	// within the bytes one whole-window answer may have.
	merge := newVolumeSegmentMerge(int64(p.limits().BufferedBackendBodyBytes))
	// The kept segments are looked up side by side: a miss may ask the disk
	// tier and a peer.
	cached := make([]bool, len(segs))
	var lookups errgroup.Group
	lookups.SetLimit(volumeSegmentLookupParallelism)
	for i, seg := range segs {
		if seg.level < 0 {
			continue
		}
		lookups.Go(func() error {
			rows, ok := p.loadVolumeSegment(volumeSegmentKey(base, seg))
			if !ok {
				return nil
			}
			cached[i] = true
			return merge.add(rows)
		})
	}
	err := lookups.Wait()
	var missing []int
	reused, scanned := 0, 0
	for i := range segs {
		if cached[i] {
			reused++
		} else {
			missing = append(missing, i)
		}
	}
	if err == nil {
		var group errgroup.Group
		group.SetLimit(p.metadataInventoryParallelism)
		for _, job := range volumeSegmentJobs(segs, missing) {
			if segs[job[0]].level >= 0 {
				scanned += len(job)
			}
			group.Go(func() error {
				rows, err := p.fillVolumeSegments(ctx, logsqlQuery, plan, statsQuery, base, segs, job)
				if err != nil {
					return err
				}
				for _, r := range rows {
					if err := merge.add(r); err != nil {
						return err
					}
				}
				return nil
			})
		}
		err = group.Wait()
	}
	if err != nil {
		if ctx.Err() == nil && (isUpstreamQueryRejected(err) || errors.Is(err, errBodyTooLarge) || errors.Is(err, errVolumeSegmentTooLarge) || errors.Is(err, errVolumeSegmentBucket)) {
			if errors.Is(err, errVolumeSegmentTooLarge) {
				// Remembered for an hour, so later requests of the same kind
				// go straight to one call.
				p.cache.SetWithTTL(base+":oversize", []byte("1"), time.Hour)
			}
			p.observeInternalOperation(ctx, "volume_segments", "whole_window", time.Since(started))
			return nil, false, nil
		}
		return nil, true, err
	}
	p.observeInternalOperation(ctx, "volume_segments", inventoryOutcome(scanned, 0, reused), time.Since(started))
	return p.volumeSegmentsAnswer(req, plan, merge.rows), true, nil
}

// volumeSegmentJobs groups the missing segments into VictoriaLogs calls: an
// edge alone, and runs of adjacent sealed segments of one level together.
func volumeSegmentJobs(segs []inventorySegment, missing []int) [][]int {
	var jobs [][]int
	for _, idx := range missing {
		if n := len(jobs); n > 0 && segs[idx].level >= 0 {
			last := jobs[n-1][len(jobs[n-1])-1]
			if last == idx-1 && segs[last].level == segs[idx].level {
				jobs[n-1] = append(jobs[n-1], idx)
				continue
			}
		}
		jobs = append(jobs, []int{idx})
	}
	return jobs
}

// fillVolumeSegments reads the segments of one job from VictoriaLogs and keeps
// the sealed ones. A run of sealed segments is shared by every request that
// needs it at that moment; a follower whose leader went away fills it itself.
func (p *Proxy) fillVolumeSegments(ctx context.Context, logsqlQuery string, plan volumePlan, statsQuery, base string, segs []inventorySegment, job []int) ([][]volumeSegmentRow, error) {
	first, last := segs[job[0]], segs[job[len(job)-1]]
	fill := func() (interface{}, error) {
		rows, err := p.fetchVolumeSegmentRows(ctx, logsqlQuery, plan, statsQuery, segs, job)
		if err != nil || first.level < 0 {
			return rows, err
		}
		now := time.Now().UnixNano()
		for i, idx := range job {
			if err := p.storeVolumeSegment(volumeSegmentKey(base, segs[idx]), rows[i], p.volumeSegmentTTL(segs[idx], now)); err != nil {
				return nil, err
			}
		}
		return rows, nil
	}
	if first.level < 0 {
		v, err := fill()
		if err != nil {
			return nil, err
		}
		return v.([][]volumeSegmentRow), nil
	}
	flightKey := base + ":fill:" + metadataInventoryLevels[first.level].String() + ":" + strconv.FormatInt(first.start, 10) + ":" + strconv.FormatInt(last.end, 10)
	for attempt := 0; ; attempt++ {
		led := false
		v, err, _ := p.inventoryGroup.Do(flightKey, func() (interface{}, error) {
			led = true
			return fill()
		})
		if err == nil {
			return v.([][]volumeSegmentRow), nil
		}
		if attempt >= 2 || ctx.Err() != nil || !inheritedFailure(err, !led) || (led && errors.Is(err, context.DeadlineExceeded)) {
			return nil, err
		}
	}
}

// fetchVolumeSegmentRows reads the rows of one job: one stats_query over a
// single segment, or one stats_query_range over a run of same-level segments
// (step = level, the run clipped to [start, end) in the query as the
// volume_range path does), whose buckets are the segments.
func (p *Proxy) fetchVolumeSegmentRows(ctx context.Context, logsqlQuery string, plan volumePlan, statsQuery string, segs []inventorySegment, job []int) ([][]volumeSegmentRow, error) {
	first, last := segs[job[0]], segs[job[len(job)-1]]
	if len(job) == 1 {
		body, err := p.fetchVolumeStats(ctx, volumeRequest{startNs: first.start, endNs: first.end}, statsQuery)
		if err != nil {
			return nil, err
		}
		rows, err := parseVolumeSegmentRows(body, nil)
		if err != nil {
			return nil, err
		}
		return [][]volumeSegmentRow{rows[0]}, nil
	}
	runReq := volumeRequest{startNs: first.start, endNs: last.end, stepNs: int64(metadataInventoryLevels[first.level])}
	// The run's bounds are step-aligned, so VictoriaLogs' buckets are the
	// segments and its range is the run: no bucket past the end is read.
	params := url.Values{}
	params.Set("query", volumeStatsQuery(runReq, logsqlQuery, plan, true))
	params.Set("start", strconv.FormatInt(runReq.startNs, 10))
	params.Set("end", strconv.FormatInt(runReq.endNs, 10))
	params.Set("step", exactVLDuration(time.Duration(runReq.stepNs)))
	body, err := p.postVolumeStats(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, err
	}
	index := make(map[int64]int, len(job))
	for i, idx := range job {
		index[segs[idx].start] = i
	}
	return parseVolumeSegmentRows(body, index)
}

// parseVolumeSegmentRows reads a VictoriaLogs stats answer. Without index it
// is one stats_query answer: one segment. With index it is a
// stats_query_range answer whose bucket start (Unix ns) selects the segment.
func parseVolumeSegmentRows(body []byte, index map[int64]int) ([][]volumeSegmentRow, error) {
	n := 1
	if index != nil {
		n = len(index)
	}
	out := make([][]volumeSegmentRow, n)
	parser := volumeSegmentFJPool.Get()
	defer releaseFJParser(&volumeSegmentFJPool, parser, len(body))
	v, err := parser.ParseBytes(body)
	if err != nil {
		// The whole-window path ignores an unreadable body (an empty answer);
		// so does a segment.
		return out, nil
	}
	for _, item := range v.GetArray("data", "result") {
		var kv []string
		if obj := item.GetObject("metric"); obj != nil {
			kv = make([]string, 0, 2*obj.Len())
			obj.Visit(func(key []byte, val *fj.Value) {
				if string(key) == "__name__" {
					return
				}
				kv = append(kv, string(key), string(val.GetStringBytes()))
			})
		}
		sortVolumeLabelPairs(kv)
		add := func(point *fj.Value) error {
			arr := point.GetArray()
			if len(arr) < 2 {
				return nil
			}
			ts, err := arr[0].Float64()
			if err != nil {
				return nil
			}
			bytes, err := strconv.ParseUint(string(arr[1].GetStringBytes()), 10, 64)
			if err != nil {
				return nil
			}
			slot := 0
			if index != nil {
				i, ok := index[int64(math.Round(ts))*int64(time.Second)]
				if !ok {
					return errVolumeSegmentBucket
				}
				slot = i
			}
			out[slot] = append(out[slot], volumeSegmentRow{kv: kv, bytes: bytes})
			return nil
		}
		if index == nil {
			if value := item.Get("value"); value != nil {
				if err := add(value); err != nil {
					return nil, err
				}
			}
			continue
		}
		for _, point := range item.GetArray("values") {
			if err := add(point); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// sortVolumeLabelPairs sorts alternating name, value pairs by name, in
// place (an insertion sort: a row has a few grouping fields).
func sortVolumeLabelPairs(kv []string) {
	for i := 2; i+1 < len(kv); i += 2 {
		for j := i; j >= 2 && kv[j] < kv[j-2]; j -= 2 {
			kv[j], kv[j-2] = kv[j-2], kv[j]
			kv[j+1], kv[j-1] = kv[j-1], kv[j+1]
		}
	}
}

func (p *Proxy) loadVolumeSegment(key string) ([]volumeSegmentRow, bool) {
	raw, _, _, ok := p.cache.GetSharedWithTTL(key)
	if !ok {
		return nil, false
	}
	return decodeVolumeSegment(raw)
}

// decodeVolumeSegment reads a cached segment (volumeSegmentEntry).
func decodeVolumeSegment(raw []byte) ([]volumeSegmentRow, bool) {
	parser := volumeSegmentFJPool.Get()
	defer releaseFJParser(&volumeSegmentFJPool, parser, len(raw))
	v, err := parser.ParseBytes(raw)
	if err != nil {
		return nil, false
	}
	labels, bytes := v.GetArray("r"), v.GetArray("b")
	if len(labels) != len(bytes) || (len(labels) == 0 && (v.Get("r") == nil || v.Get("b") == nil)) {
		return nil, false
	}
	rows := make([]volumeSegmentRow, len(labels))
	for i, item := range labels {
		arr, err := item.Array()
		if err != nil || len(arr)%2 != 0 {
			return nil, false
		}
		kv := make([]string, len(arr))
		for j, s := range arr {
			b, err := s.StringBytes()
			if err != nil {
				return nil, false
			}
			kv[j] = string(b)
		}
		n, err := bytes[i].Uint64()
		if err != nil {
			return nil, false
		}
		rows[i] = volumeSegmentRow{kv: kv, bytes: n}
	}
	return rows, true
}

func (p *Proxy) storeVolumeSegment(key string, rows []volumeSegmentRow, ttl time.Duration) error {
	e := volumeSegmentEntry{Rows: make([][]string, len(rows)), Bytes: make([]uint64, len(rows))}
	for i, row := range rows {
		e.Rows[i], e.Bytes[i] = row.kv, row.bytes
		if e.Rows[i] == nil {
			e.Rows[i] = []string{}
		}
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(raw) > p.cache.MaxEntrySizeBytes() {
		return errVolumeSegmentTooLarge
	}
	p.cache.SetWithTTL(key, raw, ttl)
	return nil
}

// volumeSegmentMerge sums segment rows per row as they arrive. The rows read
// for one request may hold at most budget bytes (strings plus a word per
// string and per row): past it the request is answered by one whole-window
// call, which -backend-max-buffered-response-bytes bounds the same way.
type volumeSegmentMerge struct {
	mu     sync.Mutex
	index  map[string]int
	rows   []volumeSegmentRow
	key    []byte
	size   int64
	budget int64
}

func newVolumeSegmentMerge(budget int64) *volumeSegmentMerge {
	return &volumeSegmentMerge{index: map[string]int{}, budget: budget}
}

func (m *volumeSegmentMerge) add(rows []volumeSegmentRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range rows {
		m.size += 8
		for _, s := range row.kv {
			m.size += int64(len(s)) + 8
		}
		if m.budget > 0 && m.size > m.budget {
			return errVolumeSegmentTooLarge
		}
		m.key = m.key[:0]
		for _, s := range row.kv { // length-prefixed: no value can fake a boundary
			m.key = append(binary.AppendUvarint(m.key, uint64(len(s))), s...)
		}
		if i, ok := m.index[string(m.key)]; ok {
			m.rows[i].bytes += row.bytes
			continue
		}
		m.index[string(m.key)] = len(m.rows)
		m.rows = append(m.rows, row)
	}
	return nil
}

// mergeVolumeSegments sums the rows of every segment per row and answers like
// the whole-window path.
func (p *Proxy) mergeVolumeSegments(req volumeRequest, plan volumePlan, parts [][]volumeSegmentRow) map[string]interface{} {
	merge := newVolumeSegmentMerge(0)
	for _, part := range parts {
		_ = merge.add(part)
	}
	return p.volumeSegmentsAnswer(req, plan, merge.rows)
}

// volumeSegmentsAnswer names, limits and orders summed rows with the
// whole-window path's code.
func (p *Proxy) volumeSegmentsAnswer(req volumeRequest, plan volumePlan, merged []volumeSegmentRow) map[string]interface{} {
	buckets := map[int64]map[string]uint64{}
	metrics := map[string]map[string]string{}
	ms := volumeBucketMillis(req, 0)
	row := map[string]string{}
	for _, r := range merged {
		clear(row)
		for i := 0; i+1 < len(r.kv); i += 2 {
			row[r.kv[i]] = r.kv[i+1]
		}
		names := p.volumeNamesForRow(req, plan, row, metrics)
		if len(names) == 0 {
			continue
		}
		bucket := buckets[ms]
		if bucket == nil {
			bucket = map[string]uint64{}
			buckets[ms] = bucket
		}
		for _, name := range names {
			bucket[name] += r.bytes
		}
	}
	return volumeResult(req, buckets, metrics)
}
