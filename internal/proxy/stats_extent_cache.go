package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The stats extent cache answers the stats_query_range calls of a metric
// query_range request from bucket data an earlier request already read, and
// asks VictoriaLogs only for the buckets it does not hold: the newest ones on
// a dashboard refresh, the oldest ones when a window moves back. Loki's
// results cache does the same with its query_range answers (split by
// -querier.split-queries-by-interval, extents per split, nothing newer than
// max_cache_freshness_per_query: pkg/storage/chunk/cache/resultscache/cache.go
// Do, handleHit and filterRecentExtents; defaults in pkg/validation/limits.go).
//
// It works below the proxy's own evaluation, on VictoriaLogs' answers, which
// are made of whole buckets: with a step, VictoriaLogs widens start and end to
// the bucket edges k*step-offset (alignStartEndToStep in
// app/vlselect/logsql/logsql.go, v1.49.0 through v1.53.0) and every point of
// its answer is one bucket [edge, edge+step) computed from that bucket's rows
// alone. Its answer for a range is therefore the answer for each of its
// buckets, put together in its own series order (ProcessStatsQueryRangeRequest
// sorts series by metric column, name and labels, and points by time), and the
// cache rebuilds exactly that body. The proxy's evaluation then runs on the
// same bytes as without the cache, so its answer is the same.
//
// Only buckets that ended before now - -query-range-freshness are stored, in
// the proxy's response cache (memory, the disk tier and the peer fleet when
// configured) under the tenant's scope, for -query-range-extent-cache-ttl
// counted from the oldest read the entry holds (Loki keeps results cache
// entries for its default validity, 1h: pkg/storage/chunk/cache/cache.go:50). A query whose buckets depend on
// more than their own rows (a subquery, a relative time filter, time_add,
// running or total stats), an unwrap checked for conversion errors, and an
// answer the cache cannot read back exactly go to VictoriaLogs as before.

// statsExtentRequestKey marks a query_range request whose stats_query_range
// calls the extent cache may answer.
type statsExtentRequestKey struct{}

// statsExtentFetchKey marks the calls the extent cache sends itself.
type statsExtentFetchKey struct{}

const statsQueryRangePath = "/select/logsql/stats_query_range"

const (
	// statsExtentSplit is the time one cache entry covers at most, Loki's
	// default split_queries_by_interval (pkg/validation/limits.go).
	statsExtentSplit = time.Hour
	// statsExtentMinChunkBuckets keeps coarse steps from spreading a range
	// over many small entries.
	statsExtentMinChunkBuckets = 128
	// statsExtentMaxGaps is the number of missing ranges read apart; more
	// are read as one range.
	statsExtentMaxGaps = 3
	// statsExtentKeyPrefix versions the stored entries.
	statsExtentKeyPrefix = "stats-extent:v1:"
)

// withStatsExtentCache lets the stats_query_range calls of r use the extent
// cache.
func (p *Proxy) withStatsExtentCache(r *http.Request) *http.Request {
	if !p.queryRangeExtentCache || p.cache == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), statsExtentRequestKey{}, true))
}

// statsExtentRequest is a stats_query_range call the cache can answer: its
// bucket grid and the buckets VictoriaLogs answers for it.
type statsExtentRequest struct {
	params       url.Values
	step, offset int64
	first, last  int64 // bucket indexes, inclusive
	chunk        int64 // buckets per cache entry
	key          string
}

func (r *statsExtentRequest) edge(k int64) int64 { return k*r.step - r.offset }

func (r *statsExtentRequest) chunkKey(j int64) string {
	return r.key + ":" + strconv.FormatInt(j, 10)
}

// statsExtentPost answers a stats_query_range call through the extent cache,
// or reports false when the call goes to VictoriaLogs unchanged.
func (p *Proxy) statsExtentPost(ctx context.Context, path string, params url.Values) (*http.Response, error, bool) {
	if path != statsQueryRangePath || p.cache == nil {
		return nil, nil, false
	}
	if on, _ := ctx.Value(statsExtentRequestKey{}).(bool); !on {
		return nil, nil, false
	}
	if fetch, _ := ctx.Value(statsExtentFetchKey{}).(bool); fetch {
		return nil, nil, false
	}
	if unwrapCheckFrom(ctx) != nil {
		// The unwrap check counts the conversion errors of every bucket it reads.
		return nil, nil, false
	}
	req, ok := p.statsExtentRequestFor(ctx, params)
	if !ok {
		return nil, nil, false
	}
	horizon := cache.Now().Add(-p.queryRangeFreshness).UnixNano()
	lastStored := statsExtentFloorDiv(horizon+req.offset, req.step) - 1
	if lastStored < req.first || p.queryRangeExtentCacheTTL <= 0 || p.cache.MaxEntrySizeBytes() <= 0 {
		return nil, nil, false // nothing old enough to keep, or nowhere to keep it
	}
	started := time.Now()
	resp, err, outcome := p.statsExtentAnswer(ctx, req, lastStored)
	p.observeInternalOperation(ctx, "stats_extent_cache", outcome, time.Since(started))
	return resp, err, true
}

// statsExtentAnswer assembles the answer of req from stored buckets and
// VictoriaLogs calls for the missing ones, and stores what it read.
func (p *Proxy) statsExtentAnswer(ctx context.Context, req *statsExtentRequest, lastStored int64) (*http.Response, error, string) {
	fetchCtx := context.WithValue(ctx, statsExtentFetchKey{}, true)
	limit := int64(p.cache.MaxEntrySizeBytes())
	chunks := p.loadStatsExtentChunks(req, lastStored)
	var covered [][2]int64
	for _, c := range chunks {
		if from, to := max(c.from, req.first), statsExtentMin(c.to, req.last); from <= to {
			covered = append(covered, [2]int64{from, to})
		}
	}
	gaps := statsExtentGaps(req.first, req.last, covered)
	if len(gaps) > statsExtentMaxGaps {
		gaps = [][2]int64{{gaps[0][0], gaps[len(gaps)-1][1]}}
	}
	if len(gaps) == 1 && gaps[0] == [2]int64{req.first, req.last} {
		// Nothing stored: the call as it came, read once and kept.
		return p.statsExtentMiss(fetchCtx, req, lastStored, chunks, limit)
	}
	fragments := make([]*statsExtentFragment, len(gaps))
	errs := make([]error, len(gaps))
	resps := make([]*http.Response, len(gaps))
	var wg sync.WaitGroup
	for i, gap := range gaps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fragments[i], resps[i], errs[i] = p.statsExtentFetch(fetchCtx, req, gap, limit)
		}()
	}
	wg.Wait()
	for i := range gaps {
		if errs[i] != nil {
			return nil, errs[i], "error"
		}
		if resps[i] != nil {
			// VictoriaLogs refused the range: its answer stands for the call.
			for k, other := range resps {
				if k != i && other != nil {
					_ = other.Body.Close()
				}
			}
			return resps[i], nil, "error"
		}
	}
	for _, f := range fragments {
		if f == nil {
			// An answer the cache cannot take apart exactly: the call goes as it came.
			return p.statsExtentPassThrough(fetchCtx, req)
		}
	}
	pieces := make([]*statsExtentFragment, 0, len(chunks)+len(fragments))
	for _, c := range chunks {
		// The stored buckets the reads do not hold (all but the ones a
		// merged read of several gaps took again).
		for _, r := range statsExtentGaps(max(c.from, req.first), statsExtentMin(c.to, req.last), append([][2]int64(nil), gaps...)) {
			pieces = append(pieces, c.slice(r[0], r[1]))
		}
	}
	pieces = append(pieces, fragments...)
	body, ok := renderStatsExtentPieces(pieces)
	if !ok {
		return p.statsExtentPassThrough(fetchCtx, req)
	}
	p.storeStatsExtentChunks(req, lastStored, chunks, fragments)
	outcome := "partial"
	if len(fragments) == 0 {
		outcome = "hit"
	}
	return statsExtentResponse(body), nil, outcome
}

// statsExtentMiss sends the call unchanged and keeps its buckets.
func (p *Proxy) statsExtentMiss(ctx context.Context, req *statsExtentRequest, lastStored int64, chunks map[int64]*statsExtentChunk, limit int64) (*http.Response, error, string) {
	resp, err := p.vlPostHTTP(ctx, statsQueryRangePath, req.params)
	if err != nil {
		return nil, err, "error"
	}
	if resp.StatusCode != http.StatusOK {
		return resp, nil, "error"
	}
	head, whole, err := readStatsExtentBody(resp.Body, limit)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err, "error"
	}
	if !whole {
		// Larger than one cache entry may be: handed on as it streams.
		resp.Body = &statsExtentPrefixedBody{Reader: io.MultiReader(bytes.NewReader(head), resp.Body), Closer: resp.Body}
		resp.ContentLength = -1
		return resp, nil, "too_large"
	}
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(head))
	resp.ContentLength = int64(len(head))
	if f, ok := parseStatsExtentFragment(head, req, req.first, req.last); ok {
		p.storeStatsExtentChunks(req, lastStored, chunks, []*statsExtentFragment{f})
		return resp, nil, "miss"
	}
	return resp, nil, "unreadable"
}

// statsExtentPassThrough sends the call unchanged and hands its answer on.
func (p *Proxy) statsExtentPassThrough(ctx context.Context, req *statsExtentRequest) (*http.Response, error, string) {
	resp, err := p.vlPostHTTP(ctx, statsQueryRangePath, req.params)
	if err != nil {
		return nil, err, "error"
	}
	return resp, nil, "unreadable"
}

// statsExtentFetch reads the buckets of gap from VictoriaLogs: a non-nil
// response is VictoriaLogs' refusal, a nil fragment without one an answer the
// cache cannot use.
func (p *Proxy) statsExtentFetch(ctx context.Context, req *statsExtentRequest, gap [2]int64, limit int64) (*statsExtentFragment, *http.Response, error) {
	params := cloneURLValues(req.params)
	if gap[0] != req.first {
		params.Set("start", time.Unix(0, req.edge(gap[0])).UTC().Format(time.RFC3339Nano))
	}
	if gap[1] != req.last {
		// The end is exclusive: the last bucket asked for ends there.
		params.Set("end", time.Unix(0, req.edge(gap[1]+1)).UTC().Format(time.RFC3339Nano))
	}
	resp, err := p.vlPostHTTP(ctx, statsQueryRangePath, params)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		return nil, resp, nil
	}
	body, whole, err := readStatsExtentBody(resp.Body, limit)
	_ = resp.Body.Close()
	if err != nil {
		return nil, nil, err
	}
	if !whole {
		return nil, nil, nil
	}
	f, ok := parseStatsExtentFragment(body, req, gap[0], gap[1])
	if !ok {
		return nil, nil, nil
	}
	return f, nil, nil
}

// readStatsExtentBody reads body up to limit bytes; whole is false when it
// holds more, and everything read is returned either way.
func readStatsExtentBody(body io.Reader, limit int64) (data []byte, whole bool, err error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(body, limit+1)); err != nil {
		return nil, false, err
	}
	return buf.Bytes(), int64(buf.Len()) <= limit, nil
}

// loadStatsExtentChunks reads the stored splits of req, side by side: a
// miss may ask the disk tier and a peer.
func (p *Proxy) loadStatsExtentChunks(req *statsExtentRequest, lastStored int64) map[int64]*statsExtentChunk {
	firstChunk := statsExtentFloorDiv(req.first, req.chunk)
	lastChunk := statsExtentFloorDiv(statsExtentMin(req.last, lastStored), req.chunk)
	found := make([]*statsExtentChunk, lastChunk-firstChunk+1)
	var wg sync.WaitGroup
	for j := firstChunk; j <= lastChunk; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, ok := p.cache.Get(req.chunkKey(j))
			if !ok {
				return
			}
			c, err := decodeStatsExtentChunk(raw)
			if err != nil || c.from < j*req.chunk || c.to >= (j+1)*req.chunk || c.from > c.to {
				return
			}
			found[j-firstChunk] = c
		}()
	}
	wg.Wait()
	chunks := make(map[int64]*statsExtentChunk, len(found))
	for i, c := range found {
		if c != nil {
			chunks[firstChunk+int64(i)] = c
		}
	}
	return chunks
}

type statsExtentPrefixedBody struct {
	io.Reader
	io.Closer
}

func statsExtentResponse(body []byte) *http.Response {
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// statsExtentRequestFor reads the bucket grid of a stats_query_range call, or
// reports false when its buckets are not the cache's to answer.
func (p *Proxy) statsExtentRequestFor(ctx context.Context, params url.Values) (*statsExtentRequest, bool) {
	if !statsExtentQueryCacheable(params.Get("query")) || len(params["start"]) != 1 || len(params["end"]) != 1 || len(params["step"]) != 1 || len(params["offset"]) > 1 {
		return nil, false
	}
	step, ok := parseVLWholeDuration(params.Get("step"))
	if !ok || step < int64(time.Millisecond) {
		return nil, false
	}
	var offset int64
	if raw := params.Get("offset"); raw != "" {
		if offset, ok = parseVLWholeDuration(raw); !ok {
			return nil, false
		}
	}
	start, okStart := parseVLStatsTime(params.Get("start"))
	end, okEnd := parseVLStatsTime(params.Get("end"))
	if !okStart || !okEnd || start+offset < 0 || end-1+offset <= 0 {
		return nil, false
	}
	req := &statsExtentRequest{
		params: params,
		step:   step,
		offset: offset,
		first:  statsExtentFloorDiv(start+offset, step),
		last:   statsExtentFloorDiv(end-1+offset, step),
	}
	if req.first > req.last {
		return nil, false
	}
	req.chunk = max(int64(statsExtentSplit)/step, statsExtentMinChunkBuckets)
	// The key holds every argument but the range, and what the call may read:
	// the tenant, the routing and the forwarded identity.
	names := make([]string, 0, len(params))
	for name := range params {
		switch name {
		case "start", "end", "timeout":
		default:
			names = append(names, name)
		}
	}
	sort.Strings(names)
	h := sha256.New()
	_, _ = io.WriteString(h, p.contextScopeFingerprint(ctx))
	for _, name := range names {
		_, _ = io.WriteString(h, "\x00"+name)
		for _, v := range params[name] {
			_, _ = io.WriteString(h, "\x01"+v)
		}
	}
	_, _ = io.WriteString(h, "\x00chunk="+strconv.FormatInt(req.chunk, 10))
	req.key = statsExtentKeyPrefix + hex.EncodeToString(h.Sum(nil))
	return req, true
}

// statsExtentQueryCacheable reports whether every bucket of the query's answer
// depends on that bucket's rows alone. VictoriaLogs runs the stats of a range
// query per bucket, with sort, first and last partitioned by bucket and limit
// and offset refused after the stats (GetStatsLabelsAddGroupingByTime and
// addPartitionByTime in lib/logstorage/parser.go). What remains to refuse:
// a subquery (in(...), join, union), which reads the whole range; a time
// filter, which may be relative to the query's end; time_add, which moves
// rows across buckets; and running_stats or total_stats, which carry values
// across buckets.
func statsExtentQueryCacheable(query string) bool {
	if query == "" || strings.Contains(query, "_time:") {
		return false
	}
	for _, pipe := range logsQLPipeNameRE.FindAllStringSubmatch(query, -1) {
		switch pipe[1] {
		case "join", "union", "time_add", "running_stats", "total_stats":
			return false
		}
	}
	// A pipe inside parentheses is a subquery.
	depth := 0
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '"', '\'', '`':
			end := statsExtentQuotedEnd(query, i)
			if end < 0 {
				return false
			}
			i = end
		case '(':
			depth++
		case ')':
			depth--
		case '|':
			if depth > 0 {
				return false
			}
		}
	}
	return depth == 0
}

// statsExtentQuotedEnd returns the index of the quote closing the quoted
// LogsQL string at i, or -1.
func statsExtentQuotedEnd(s string, i int) int {
	quote := s[i]
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			if quote != '`' {
				j++
			}
		case quote:
			return j
		}
	}
	return -1
}

// parseVLWholeDuration reads a duration of one whole number and one unit, the
// forms the proxy sends (10s, 60000000000ns, -1ns), as VictoriaLogs reads it
// (tryParseDuration in lib/logstorage/values_encoder.go).
func parseVLWholeDuration(raw string) (int64, bool) {
	s := raw
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	digits := 0
	for digits < len(s) && s[digits] >= '0' && s[digits] <= '9' {
		digits++
	}
	if digits == 0 || digits > 15 {
		return 0, false
	}
	n, err := strconv.ParseInt(s[:digits], 10, 64)
	if err != nil {
		return 0, false
	}
	var unit int64
	switch s[digits:] {
	case "ns":
		unit = 1
	case "us", "µs":
		unit = int64(time.Microsecond)
	case "ms":
		unit = int64(time.Millisecond)
	case "s":
		unit = int64(time.Second)
	case "m":
		unit = int64(time.Minute)
	case "h":
		unit = int64(time.Hour)
	case "d":
		unit = 24 * int64(time.Hour)
	case "w":
		unit = 7 * 24 * int64(time.Hour)
	default:
		return 0, false
	}
	if n > math.MaxInt64/unit {
		return 0, false
	}
	if negative {
		return -n * unit, true
	}
	return n * unit, true
}

// parseVLStatsTime reads a start or end argument in the forms whose meaning is
// fixed, as VictoriaLogs reads them (timeutil.ParseTimeAt): an RFC3339 time in
// UTC, or a whole Unix timestamp whose size tells seconds, milliseconds,
// microseconds or nanoseconds (getUnixTimestampNanoseconds).
func parseVLStatsTime(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}
	if strings.HasSuffix(raw, "Z") {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return 0, false
		}
		return t.UnixNano(), true
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	switch {
	case n < 1<<31:
		return n * 1e9, true
	case n < 1e3*(1<<31):
		return n * 1e6, true
	case n < 1e6*(1<<31):
		return n * 1e3, true
	}
	return n, true
}

// statsExtentMin is the smallest of values (the package's tests declare their
// own min).
func statsExtentMin(first int64, rest ...int64) int64 {
	for _, v := range rest {
		if v < first {
			first = v
		}
	}
	return first
}

func statsExtentFloorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// statsExtentGaps returns the ranges of [first, last] that covered does not
// hold.
func statsExtentGaps(first, last int64, covered [][2]int64) [][2]int64 {
	sort.Slice(covered, func(i, j int) bool { return covered[i][0] < covered[j][0] })
	var gaps [][2]int64
	next := first
	for _, c := range covered {
		if c[0] > next {
			gaps = append(gaps, [2]int64{next, c[0] - 1})
		}
		next = max(next, c[1]+1)
	}
	if next <= last {
		gaps = append(gaps, [2]int64{next, last})
	}
	return gaps
}

// statsExtentSeries is one series of a stats_query_range answer as
// VictoriaLogs wrote it: its metric object, the part of its sort key after the
// name (the non-empty labels, see MarshalFieldsToJSON), and its points.
type statsExtentSeries struct {
	name     string
	metric   []byte
	labelKey []byte
	points   []statsExtentPoint
}

type statsExtentPoint struct {
	k   int64  // bucket index
	raw []byte // [timestamp,"value"] as written
}

// statsExtentFragment holds the buckets [from, to] of an answer.
type statsExtentFragment struct {
	from, to int64
	names    []string // the result names in VictoriaLogs' column order
	series   []*statsExtentSeries
	readAt   int64 // when VictoriaLogs answered, unix ns
}

const statsExtentBodyHead = `{"status":"success","data":{"resultType":"matrix","result":[`
const statsExtentBodyTail = `]}}`

// parseStatsExtentFragment takes a stats_query_range answer apart, or reports
// false when it is not exactly the form VictoriaLogs writes
// (stats_query_range_response.qtpl), its points are not on the request's grid
// inside [from, to], or its series are not in VictoriaLogs' order: the cache
// then leaves the call alone.
func parseStatsExtentFragment(body []byte, req *statsExtentRequest, from, to int64) (*statsExtentFragment, bool) {
	if !bytes.HasPrefix(body, []byte(statsExtentBodyHead)) || !bytes.HasSuffix(body, []byte(statsExtentBodyTail)) {
		return nil, false
	}
	f := &statsExtentFragment{from: from, to: to, readAt: cache.Now().UnixNano()}
	s := &statsExtentScanner{b: body, i: len(statsExtentBodyHead)}
	seen := map[string]bool{}
	for !s.at(']') {
		if len(f.series) > 0 && !s.eat(",") {
			return nil, false
		}
		series, ok := s.series(req, from, to)
		if !ok || seen[string(series.metric)] {
			return nil, false
		}
		seen[string(series.metric)] = true
		if n := len(f.names); n == 0 || f.names[n-1] != series.name {
			for _, name := range f.names {
				if name == series.name {
					return nil, false // a name's series are together
				}
			}
			f.names = append(f.names, series.name)
		} else if prev := f.series[len(f.series)-1]; bytes.Compare(prev.labelKey, series.labelKey) >= 0 {
			return nil, false
		}
		f.series = append(f.series, series)
	}
	if s.i != len(body)-len(statsExtentBodyTail) {
		return nil, false
	}
	// What is rebuilt from the parts must be the answer itself.
	rebuilt, ok := renderStatsExtentPieces([]*statsExtentFragment{f})
	return f, ok && bytes.Equal(rebuilt, body)
}

type statsExtentScanner struct {
	b []byte
	i int
}

func (s *statsExtentScanner) at(c byte) bool { return s.i < len(s.b) && s.b[s.i] == c }

func (s *statsExtentScanner) eat(lit string) bool {
	if !bytes.HasPrefix(s.b[s.i:], []byte(lit)) {
		return false
	}
	s.i += len(lit)
	return true
}

// str scans a JSON string and returns it as written, quotes included.
func (s *statsExtentScanner) str() ([]byte, bool) {
	if !s.at('"') {
		return nil, false
	}
	start := s.i
	for j := s.i + 1; j < len(s.b); j++ {
		switch s.b[j] {
		case '\\':
			j++
		case '"':
			s.i = j + 1
			return s.b[start:s.i], true
		}
	}
	return nil, false
}

func (s *statsExtentScanner) series(req *statsExtentRequest, from, to int64) (*statsExtentSeries, bool) {
	if !s.eat(`{"metric":{"__name__":`) {
		return nil, false
	}
	metricStart := s.i - len(`{"__name__":`)
	name, ok := s.str()
	if !ok {
		return nil, false
	}
	labelKey := []byte{'{'}
	for s.at(',') {
		s.i++
		pairStart := s.i
		key, ok := s.str()
		if !ok || len(key) == 2 || !s.eat(":") {
			return nil, false // an empty label name is keyed as _msg
		}
		value, ok := s.str()
		if !ok {
			return nil, false
		}
		if len(value) > 2 {
			if len(labelKey) > 1 {
				labelKey = append(labelKey, ',')
			}
			labelKey = append(labelKey, s.b[pairStart:s.i]...)
		}
	}
	if !s.eat("}") {
		return nil, false
	}
	series := &statsExtentSeries{name: string(name), metric: s.b[metricStart:s.i], labelKey: append(labelKey, '}')}
	if !s.eat(`,"values":[`) {
		return nil, false
	}
	for !s.at(']') {
		if len(series.points) > 0 && !s.eat(",") {
			return nil, false
		}
		pointStart := s.i
		if !s.eat("[") {
			return nil, false
		}
		numStart := s.i
		for s.i < len(s.b) && strings.IndexByte("0123456789.eE+-", s.b[s.i]) >= 0 {
			s.i++
		}
		seconds, err := strconv.ParseFloat(string(s.b[numStart:s.i]), 64)
		if err != nil || !s.eat(",") {
			return nil, false
		}
		if _, ok := s.str(); !ok || !s.eat("]") {
			return nil, false
		}
		ns := int64(math.Round(seconds * 1e9))
		k := statsExtentFloorDiv(ns+req.offset+req.step/2, req.step)
		if d := ns - req.edge(k); d > req.step/4 || d < -req.step/4 || k < from || k > to {
			return nil, false
		}
		if n := len(series.points); n > 0 && series.points[n-1].k >= k {
			return nil, false
		}
		series.points = append(series.points, statsExtentPoint{k: k, raw: s.b[pointStart:s.i]})
	}
	if !s.eat("]}") || len(series.points) == 0 {
		return nil, false
	}
	return series, true
}

// renderStatsExtentPieces writes the answer made of the pieces' buckets in
// VictoriaLogs' form and order, or reports false when the pieces disagree on
// the order of the result names.
func renderStatsExtentPieces(pieces []*statsExtentFragment) ([]byte, bool) {
	var names []string
	merged := map[string]*statsExtentSeries{}
	var order []*statsExtentSeries
	size := len(statsExtentBodyHead) + len(statsExtentBodyTail)
	for _, piece := range pieces {
		if len(piece.series) == 0 {
			continue
		}
		if names == nil {
			names = piece.names
		} else if !slices.Equal(names, piece.names) {
			return nil, false
		}
		for _, series := range piece.series {
			m := merged[string(series.metric)]
			if m == nil {
				m = &statsExtentSeries{name: series.name, metric: series.metric, labelKey: series.labelKey}
				merged[string(series.metric)] = m
				order = append(order, m)
				size += len(series.metric) + len(`{"metric":,"values":[]},`)
			}
			m.points = append(m.points, series.points...)
			for _, point := range series.points {
				size += len(point.raw) + 1
			}
		}
	}
	rank := make(map[string]int, len(names))
	for i, name := range names {
		rank[name] = i
	}
	less := func(a, b *statsExtentSeries) int {
		if ra, rb := rank[a.name], rank[b.name]; ra != rb {
			return ra - rb
		}
		return bytes.Compare(a.labelKey, b.labelKey)
	}
	sort.Slice(order, func(i, j int) bool { return less(order[i], order[j]) < 0 })
	for i := 1; i < len(order); i++ {
		if less(order[i-1], order[i]) == 0 {
			return nil, false // an order VictoriaLogs' sort leaves open
		}
	}
	out := make([]byte, 0, size)
	out = append(out, statsExtentBodyHead...)
	for i, series := range order {
		if i > 0 {
			out = append(out, ',')
		}
		sort.Slice(series.points, func(a, b int) bool { return series.points[a].k < series.points[b].k })
		for k := 1; k < len(series.points); k++ {
			if series.points[k-1].k == series.points[k].k {
				return nil, false
			}
		}
		out = append(out, `{"metric":`...)
		out = append(out, series.metric...)
		out = append(out, `,"values":[`...)
		for k, point := range series.points {
			if k > 0 {
				out = append(out, ',')
			}
			out = append(out, point.raw...)
		}
		out = append(out, "]}"...)
	}
	out = append(out, statsExtentBodyTail...)
	return out, true
}

// statsExtentChunk is one cache entry: the buckets [from, to] of one split
// and when the oldest of them was read.
type statsExtentChunk = statsExtentFragment

// slice returns the buckets of c inside [from, to].
func (c *statsExtentChunk) slice(from, to int64) *statsExtentFragment {
	out := &statsExtentFragment{from: max(c.from, from), to: statsExtentMin(c.to, to), names: c.names, readAt: c.readAt}
	for _, series := range c.series {
		var points []statsExtentPoint
		for _, point := range series.points {
			if point.k >= out.from && point.k <= out.to {
				points = append(points, point)
			}
		}
		if len(points) > 0 {
			out.series = append(out.series, &statsExtentSeries{name: series.name, metric: series.metric, labelKey: series.labelKey, points: points})
		}
	}
	return out
}

// storeStatsExtentChunks keeps the buckets read for req that ended before the
// freshness horizon, one entry per split: the stored range of a split grows
// by the buckets read next to it; a range read apart from it replaces it when
// longer.
func (p *Proxy) storeStatsExtentChunks(req *statsExtentRequest, lastStored int64, chunks map[int64]*statsExtentChunk, fragments []*statsExtentFragment) {
	ttl := p.queryRangeExtentCacheTTL
	now := cache.Now().UnixNano()
	for _, f := range fragments {
		for j := statsExtentFloorDiv(f.from, req.chunk); j <= statsExtentFloorDiv(statsExtentMin(f.to, lastStored), req.chunk); j++ {
			lo, hi := max(f.from, j*req.chunk), statsExtentMin(f.to, (j+1)*req.chunk-1, lastStored)
			if lo > hi {
				continue
			}
			read := f.slice(lo, hi)
			next := read
			if old := chunks[j]; old != nil && now-old.readAt < int64(ttl) && (len(old.series) == 0 || len(read.series) == 0 || slices.Equal(old.names, read.names)) {
				switch {
				case old.from <= lo && hi <= old.to:
					continue // already held
				case old.from <= hi+1 && lo <= old.to+1:
					// Adjacent or overlapping: one range, the new read wins where both hold a bucket.
					next = mergeStatsExtentChunks(old, read)
				case old.to-old.from >= hi-lo:
					continue
				}
			}
			chunks[j] = next
			entryTTL := ttl - time.Duration(now-next.readAt)
			if entryTTL <= 0 {
				continue
			}
			if raw, err := encodeStatsExtentChunk(next); err == nil {
				p.cache.SetWithTTL(req.chunkKey(j), raw, entryTTL)
			}
		}
	}
}

// mergeStatsExtentChunks joins two adjacent or overlapping ranges of one
// split, taking the buckets of read where both hold them.
func mergeStatsExtentChunks(old, read *statsExtentChunk) *statsExtentChunk {
	out := &statsExtentFragment{from: statsExtentMin(old.from, read.from), to: max(old.to, read.to), names: read.names, readAt: statsExtentMin(old.readAt, read.readAt)}
	if len(read.series) == 0 {
		out.names = old.names
	}
	bySeries := map[string]*statsExtentSeries{}
	add := func(series *statsExtentSeries, keep func(int64) bool) {
		m := bySeries[string(series.metric)]
		if m == nil {
			m = &statsExtentSeries{name: series.name, metric: series.metric, labelKey: series.labelKey}
			bySeries[string(series.metric)] = m
			out.series = append(out.series, m)
		}
		for _, point := range series.points {
			if keep(point.k) {
				m.points = append(m.points, point)
			}
		}
	}
	for _, series := range old.series {
		add(series, func(k int64) bool { return k < read.from || k > read.to })
	}
	for _, series := range read.series {
		add(series, func(int64) bool { return true })
	}
	kept := out.series[:0]
	for _, series := range out.series {
		if len(series.points) > 0 {
			sort.Slice(series.points, func(a, b int) bool { return series.points[a].k < series.points[b].k })
			kept = append(kept, series)
		}
	}
	out.series = kept
	return out
}

// encodeStatsExtentChunk writes an entry: its range, read time, names and
// series with their points as VictoriaLogs wrote them, s2-compressed.
func encodeStatsExtentChunk(c *statsExtentChunk) ([]byte, error) {
	var b []byte
	b = binary.AppendVarint(b, c.from)
	b = binary.AppendVarint(b, c.to)
	b = binary.AppendVarint(b, c.readAt)
	b = binary.AppendUvarint(b, uint64(len(c.names)))
	index := map[string]int{}
	for i, name := range c.names {
		index[name] = i
		b = appendStatsExtentBytes(b, []byte(name))
	}
	b = binary.AppendUvarint(b, uint64(len(c.series)))
	for _, series := range c.series {
		i, ok := index[series.name]
		if !ok {
			return nil, errors.New("stats extent: series name not listed")
		}
		b = binary.AppendUvarint(b, uint64(i))
		b = appendStatsExtentBytes(b, series.metric)
		b = appendStatsExtentBytes(b, series.labelKey)
		b = binary.AppendUvarint(b, uint64(len(series.points)))
		prev := c.from
		for _, point := range series.points {
			b = binary.AppendUvarint(b, uint64(point.k-prev))
			prev = point.k
			b = appendStatsExtentBytes(b, point.raw)
		}
	}
	return s2.Encode(nil, b), nil
}

func appendStatsExtentBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

var errStatsExtentCorrupt = errors.New("stats extent: corrupt entry")

func decodeStatsExtentChunk(raw []byte) (*statsExtentChunk, error) {
	b, err := s2.Decode(nil, raw)
	if err != nil {
		return nil, err
	}
	d := &statsExtentDecoder{b: b}
	c := &statsExtentFragment{from: d.varint(), to: d.varint(), readAt: d.varint()}
	names := d.uvarint()
	if names > uint64(len(b)) {
		return nil, errStatsExtentCorrupt
	}
	for range names {
		c.names = append(c.names, string(d.bytes()))
	}
	count := d.uvarint()
	if count > uint64(len(b)) {
		return nil, errStatsExtentCorrupt
	}
	for range count {
		i := d.uvarint()
		series := &statsExtentSeries{metric: d.bytes(), labelKey: d.bytes()}
		if i >= uint64(len(c.names)) {
			return nil, errStatsExtentCorrupt
		}
		series.name = c.names[i]
		points := d.uvarint()
		if points > uint64(len(b)) {
			return nil, errStatsExtentCorrupt
		}
		k := c.from
		for range points {
			k += int64(d.uvarint())
			series.points = append(series.points, statsExtentPoint{k: k, raw: d.bytes()})
		}
		c.series = append(c.series, series)
	}
	if d.err || d.i != len(b) {
		return nil, errStatsExtentCorrupt
	}
	return c, nil
}

type statsExtentDecoder struct {
	b   []byte
	i   int
	err bool
}

func (d *statsExtentDecoder) varint() int64 {
	v, n := binary.Varint(d.b[d.i:])
	if n <= 0 {
		d.err, d.i = true, len(d.b)
		return 0
	}
	d.i += n
	return v
}

func (d *statsExtentDecoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b[d.i:])
	if n <= 0 {
		d.err, d.i = true, len(d.b)
		return 0
	}
	d.i += n
	return v
}

func (d *statsExtentDecoder) bytes() []byte {
	n := d.uvarint()
	if n > uint64(len(d.b)-d.i) {
		d.err, d.i = true, len(d.b)
		return nil
	}
	v := d.b[d.i : d.i+int(n)]
	d.i += int(n)
	return v
}
