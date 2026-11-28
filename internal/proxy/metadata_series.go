package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// flagMetadataSeriesMaxReadRows bounds the rows of one rebuilt VictoriaLogs read.
const flagMetadataSeriesMaxReadRows = "-structured-metadata-series-max-read-rows"

// Loki names a metric series by the stream labels and the entry's structured metadata, so a stream whose lines carry
// a per-line trace_id is a series per line; the proxy's series is the stream (metric-identity/structured-metadata-
// splits-series). VictoriaLogs stores structured metadata as a row's other fields, and nothing in a row tells such a
// field from one an ingestion route parsed out of the line. With -structured-metadata-series-identity the operator
// declares that every stored field of a row that is not a stream field is its structured metadata: VictoriaLogs is
// fed through the Loki push API or OTLP with the line kept as it is (disable_message_parsing, no JSON or logfmt
// parsing at ingest), and no other writer adds fields. A metric request (query, query_range) then reads Loki's
// series: every VictoriaLogs stats read of it that reads a series per stream (logsql.ReadsStreamIdentity) gets the
// rows' _stream rebuilt from the stream fields and the metadata fields under Loki's names (logsql.WithSeriesMetadata,
// the names from VictoriaLogs' field_names and stream_field_names over the read's window, cached), so every route
// that groups, folds, windows, chunks or names series by _stream reads Loki's series. A read that groups by labels
// only (sum by, max by over max_over_time) reads what it read before: its answer does not depend on the split.
//
// Where the declaration is false (a field parsed out of the line at ingest, a jsonline or other writer's field), that
// field splits the proxy's series where Loki's would not; an answer that sums the series is unchanged.

// metadataSeriesKey marks a metric request whose reads take Loki's structured-metadata series identity; its value is
// the request's memo of the identities it listed.
type metadataSeriesKey struct{}

// metadataSeriesMemo holds the identities a request listed, by the filter its reads share: a read whose window lies in
// a listed window takes that identity (a superset of its rows' fields), so the time chunks and the operands of one
// request list the fields once. It holds a few entries per request and dies with it.
type metadataSeriesMemo struct {
	mu      sync.Mutex
	entries []metadataSeriesEntry
	hists   map[string][]rowHistogram
	// rebuilt are the reads the request rebuilt, by their rebuilt text.
	rebuilt map[string]logsql.SeriesIdentityRead
}

// noteRebuilt keeps the read a text was rebuilt from (a few per request: the request's reads share their texts).
func (m *metadataSeriesMemo) noteRebuilt(text string, read logsql.SeriesIdentityRead) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rebuilt == nil {
		m.rebuilt = map[string]logsql.SeriesIdentityRead{}
	}
	if len(m.rebuilt) < 16 {
		m.rebuilt[text] = read
	}
}

// rebuiltRead is the read text was rebuilt from, when this request rebuilt it.
func (m *metadataSeriesMemo) rebuiltRead(text string) (logsql.SeriesIdentityRead, bool) {
	if m == nil {
		return logsql.SeriesIdentityRead{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rebuilt[text]
	return r, ok
}

func (m *metadataSeriesMemo) histogram(filter string, from, to int64) (rowHistogram, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range m.hists[filter] {
		if h.lo <= from && to < h.hi {
			return h, true
		}
	}
	return rowHistogram{}, false
}

func (m *metadataSeriesMemo) addHistogram(filter string, h rowHistogram) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hists == nil {
		m.hists = map[string][]rowHistogram{}
	}
	if len(m.hists) < 16 && len(m.hists[filter]) < 4 {
		m.hists[filter] = append(m.hists[filter], h)
	}
}

type metadataSeriesEntry struct {
	filter string
	lo, hi int64
	spec   logsql.SeriesMetadata
	ok     bool
}

func (m *metadataSeriesMemo) find(filter string, from, to int64) (metadataSeriesEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		if e.filter == filter && e.lo <= from && to <= e.hi {
			return e, true
		}
	}
	return metadataSeriesEntry{}, false
}

func (m *metadataSeriesMemo) add(e metadataSeriesEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) < 16 {
		m.entries = append(m.entries, e)
	}
}

// withMetadataSeries marks ctx's reads for the identity when the operator declared it and the split can change the
// answer of query (metadataSplitChanges): a sum by labels, and every other shape the split cannot change, reads as
// without the flag.
func (p *Proxy) withMetadataSeries(ctx context.Context, query string) context.Context {
	if !p.structuredMetadataSeries || !metadataSplitChanges(query) {
		return ctx
	}
	if _, ok := ctx.Value(metadataSeriesKey{}).(*metadataSeriesMemo); ok {
		return ctx
	}
	return context.WithValue(ctx, metadataSeriesKey{}, &metadataSeriesMemo{})
}

func metadataSeriesOf(ctx context.Context) *metadataSeriesMemo {
	m, _ := ctx.Value(metadataSeriesKey{}).(*metadataSeriesMemo)
	return m
}

// metadataSeriesDiscoveryAlign widens a read's window to whole hours for the field listings: a superset of the
// fields of the read's rows gives the same answer, and reads of one hour share one listing and one query text, so
// the bucket caches keep reusing their stored buckets as a window moves.
const metadataSeriesDiscoveryAlign = time.Hour

// metadataSeriesParams returns params with the read's query rebuilt for Loki's series identity
// (-structured-metadata-series-identity), or params as they are: the flag is off, the request is not a metric
// request, the read is not a stats read of a series per stream, or its window holds no structured metadata. A read
// whose fields cannot be listed is sent as it is and counted.
func (p *Proxy) metadataSeriesParams(ctx context.Context, path string, params url.Values) url.Values {
	memo := metadataSeriesOf(ctx)
	if memo == nil {
		return params
	}
	switch path {
	case "/select/logsql/stats_query_range", "/select/logsql/stats_query", "/select/logsql/query":
	default:
		return params
	}
	text := params.Get("query")
	if text == "" {
		return params
	}
	started := time.Now()
	read, ok, err := logsql.ReadSeriesIdentity(text)
	if err != nil {
		p.observeInternalOperation(ctx, "metadata_series_identity", "unread", time.Since(started))
		return params
	}
	if !ok {
		return params
	}
	from, okFrom := parseFlexibleUnixNanos(params.Get("start"))
	to, okTo := parseFlexibleUnixNanos(params.Get("end"))
	if !okFrom || !okTo || to < from {
		p.observeInternalOperation(ctx, "metadata_series_identity", "no_window", time.Since(started))
		return params
	}
	spec, ok, err := p.seriesMetadataFor(ctx, memo, read.Filter(), from, to)
	if err != nil {
		p.observeInternalOperation(ctx, "metadata_series_identity", "listing_failed", time.Since(started))
		return params
	}
	if !ok {
		p.observeInternalOperation(ctx, "metadata_series_identity", "no_metadata", time.Since(started))
		return params
	}
	rebuilt, changed := read.With(spec)
	if !changed {
		return params
	}
	// The slicer finds the read it rebuilt by its text (slicedSeriesMetadataRead), with what it counts its rows by.
	memo.noteRebuilt(rebuilt, read)
	out := make(url.Values, len(params))
	for k, v := range params {
		out[k] = v
	}
	out["query"] = []string{rebuilt}
	p.observeInternalOperation(ctx, "metadata_series_identity", "rebuilt", time.Since(started))
	return out
}

// seriesMetadataFor is the series identity of the rows q's filters read in [from, to], widened to whole hours: the
// stream fields and all fields VictoriaLogs lists for them there (field_names and stream_field_names, both cached),
// or the identity the request listed already over a window that holds this one.
func (p *Proxy) seriesMetadataFor(ctx context.Context, memo *metadataSeriesMemo, filter string, from, to int64) (logsql.SeriesMetadata, bool, error) {
	if e, ok := memo.find(filter, from, to); ok {
		return e.spec, e.ok, nil
	}
	align := metadataSeriesDiscoveryAlign.Nanoseconds()
	lo := from / align * align
	hi := (to + align - 1) / align * align
	params := url.Values{}
	params.Set("query", filter)
	params.Set("start", strconv.FormatInt(lo, 10))
	params.Set("end", strconv.FormatInt(hi, 10))
	// The two listings are read side by side: neither needs the other.
	var stream []string
	var streamErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream, streamErr = p.seriesMetadataListing(ctx, "/select/logsql/stream_field_names", "smsfn:", params)
	}()
	fields, err := p.seriesMetadataListing(ctx, "/select/logsql/field_names", "smfn:", params)
	<-done
	if err == nil {
		err = streamErr
	}
	if err != nil {
		return logsql.SeriesMetadata{}, false, err
	}
	spec, ok := logsql.NewSeriesMetadata(stream, fields, SanitizeLabelName)
	memo.add(metadataSeriesEntry{filter: filter, lo: lo, hi: hi, spec: spec, ok: ok})
	return spec, ok, nil
}

// statsReadSeriesLimit is the series a stats read may hold: the tenant's series limit (resolvedMaxStatsQuerySeries),
// except for a read of a request whose answer the bucket matrix builder folds into its groups (matrixFold) while it
// takes Loki's structured-metadata series. Such a read holds a series per stream and metadata set, often one per
// line, which Loki folds before its limit applies (to the folded answer: the chunk fold and capSeriesToLimit hold
// it there); the read is then bounded by -manual-range-metric-row-limit, as the proxy's other folded reads are, and
// by the stats body bound, past which it is read again in time chunks.
func (p *Proxy) statsReadSeriesLimit(ctx context.Context) int {
	limit := p.resolvedMaxStatsQuerySeries(ctx)
	if metadataSeriesOf(ctx) == nil || matrixFoldFrom(ctx) == nil {
		return limit
	}
	if rows, err := p.manualMetricRowBudget(); err == nil && (limit <= 0 || rows > limit) {
		return rows
	}
	return limit
}

// A rebuilt read holds a VictoriaLogs stats group per series and bucket, and with a per-line trace_id a series per
// line: one stats_query_range read of a day of such lines held them all at once and ran VictoriaLogs out of memory.
// Such a read is sent in slices of the bucket grid (the stats extent cache's: statsExtentRequestFor, gapParams,
// statsExtentFetch, renderStatsExtentPieces), one after another, each over at most -structured-metadata-series-
// max-read-rows rows by a count of the read's rows: every bucket of an answer depends only on its own rows
// (statsExtentQueryCacheable), so the joined slices are the whole read's answer. VictoriaLogs holds the groups of one
// slice; the proxy holds the joined answer, which the stats body bound bounds as it bounds the whole read
// (-backend-max-buffered-response-bytes, past which the time-chunked window evaluator takes over as before).

// slicedSeriesMetadataRead answers a rebuilt stats_query_range read over more rows than the bound in slices, and
// reports whether it did. A read within the bound is sent as it is (false). A read over it whose buckets cannot be
// read apart, an instant stats read and a read of rows over it answer Loki's limit error naming -structured-metadata-series-max-read-rows, never an unbounded read.
func (p *Proxy) slicedSeriesMetadataRead(ctx context.Context, path string, params url.Values) (*http.Response, bool, error) {
	switch path {
	case statsQueryRangePath, "/select/logsql/stats_query", "/select/logsql/query":
	default:
		return nil, false, nil
	}
	if p.seriesMetadataMaxReadRows <= 0 || metadataSeriesOf(ctx) == nil || params.Get("start") == "" || params.Get("end") == "" {
		return nil, false, nil
	}
	if fetch, _ := ctx.Value(statsExtentFetchKey{}).(bool); fetch {
		return nil, false, nil
	}
	read, ok := metadataSeriesOf(ctx).rebuiltRead(params.Get("query"))
	if !ok {
		return nil, false, nil
	}
	started := time.Now()
	rows, err := p.seriesMetadataReadRows(ctx, read, params)
	if err != nil {
		p.observeInternalOperation(ctx, "metadata_series_identity", "count_failed", time.Since(started))
		return nil, true, err
	}
	limit := int64(p.seriesMetadataMaxReadRows)
	bound := int64(p.limits().BufferedBackendBodyBytes)
	if seriesFoldFrom(ctx) != nil {
		noteStatsReadRows(ctx, rows, limit)
		// A folded read (series_fold_stream.go) groups a series across the buckets of its read, so it is never cut into
		// slices; VictoriaLogs holds a group per series of the read, so one over the bound is refused as past its
		// bound: the time chunks read it again in smaller chunks.
		if rows <= limit {
			return nil, false, nil
		}
		p.observeInternalOperation(ctx, "metadata_series_identity", "folded_over_rows", time.Since(started))
		return nil, true, lokierr.LimitReached(flagMetadataSeriesMaxReadRows, limit, &metadataReadRowsError{rowLimitError{limit: int(limit), unit: "rows in one VictoriaLogs read", matches: rows}})
	}
	if path == statsQueryRangePath && matrixFoldFrom(ctx) != nil && bound > 0 && rows > bound/metadataSeriesRowBytes {
		// A read the window evaluator folds in time chunks once it passes the stats body bound
		// (rereadOverflowedInChunks): with a series per row about metadataSeriesRowBytes each, this one would pass it,
		// so it is not sent; the chunks are read instead, each counted the same way.
		p.observeInternalOperation(ctx, "metadata_series_identity", "predicted_over_body", time.Since(started))
		return nil, true, newBackendBodyTooLarge(bound)
	}
	if rows <= limit {
		return nil, false, nil
	}
	// An instant read and a read of rows (the series fold, the unwrap identity) hold their window's groups at once.
	req, ok := (*statsExtentRequest)(nil), false
	if path == statsQueryRangePath {
		req, ok = p.statsExtentRequestFor(ctx, params)
	}
	if !ok {
		p.observeInternalOperation(ctx, "metadata_series_identity", "over_rows_unsliced", time.Since(started))
		return nil, true, lokierr.LimitReached(flagMetadataSeriesMaxReadRows, limit, &rowLimitError{limit: int(limit), unit: "rows in one VictoriaLogs read", matches: rows})
	}
	gaps := metadataSeriesSlices(req.first, req.last, rows, limit)
	fetchCtx := context.WithValue(context.WithValue(ctx, statsExtentFetchKey{}, true), statsExtentHeavyKey{}, true)
	if bound <= 0 {
		bound = math.MaxInt64
	}
	var used int64
	pieces := make([]*statsExtentFragment, 0, len(gaps))
	for _, gap := range gaps {
		f, refused, err := p.statsExtentFetch(fetchCtx, req, gap, bound-used)
		switch {
		case err != nil:
			return nil, true, err
		case refused != nil:
			return refused, true, nil
		case f == nil:
			// Past the body bound (or an answer that cannot be taken apart): what the whole read would answer.
			p.observeInternalOperation(ctx, "metadata_series_identity", "sliced_over_body", time.Since(started))
			return nil, true, newBackendBodyTooLarge(bound)
		}
		used += statsExtentFragmentBytes(f)
		pieces = append(pieces, f)
	}
	body, ok := renderStatsExtentPieces(pieces)
	if !ok || int64(len(body)) > bound {
		return nil, true, newBackendBodyTooLarge(bound)
	}
	p.observeInternalOperation(ctx, "metadata_series_identity", "sliced", time.Since(started))
	return statsExtentResponse(body), true, nil
}

// metadataSeriesSlices cuts the buckets [first, last] into slices of about half the bound's rows each, assuming the
// read's rows spread evenly over its buckets; each slice holds at least one bucket.
func metadataSeriesSlices(first, last, rows, limit int64) [][2]int64 {
	buckets := last - first + 1
	n := min64(buckets, (2*rows+limit-1)/limit)
	if n < 1 {
		n = 1
	}
	out := make([][2]int64, 0, n)
	for i := int64(0); i < n; i++ {
		from, to := first+i*buckets/n, first+(i+1)*buckets/n-1
		out = append(out, [2]int64{from, to})
	}
	return out
}

// statsExtentFragmentBytes is about the bytes f takes in an answer.
func statsExtentFragmentBytes(f *statsExtentFragment) int64 {
	var n int64
	for _, s := range f.series {
		n += int64(len(s.metric)) + 16
		for _, pt := range s.points {
			n += int64(len(pt.raw)) + 1
		}
	}
	return n
}

// seriesMetadataReadRows is the rows filter matches in the read's window (params' start and end): one count, kept for
// the request (its time chunks and operands share the read's filter).
func (p *Proxy) seriesMetadataReadRows(ctx context.Context, read logsql.SeriesIdentityRead, params url.Values) (int64, error) {
	memo := metadataSeriesOf(ctx)
	from, okFrom := parseFlexibleUnixNanos(params.Get("start"))
	to, okTo := parseFlexibleUnixNanos(params.Get("end"))
	if !okFrom || !okTo || to < from {
		return 0, errors.New("metadata series read without a window")
	}
	filter := read.Filter()
	hist, ok := memo.histogram(filter, from, to)
	if !ok {
		var err error
		if hist, err = p.seriesMetadataRowsCached(ctx, read, from, to); err != nil {
			return 0, err
		}
		memo.addHistogram(filter, hist)
	}
	return hist.rows(from, to), nil
}

// seriesMetadataRowsCached is the minute row counts of [from, to] (seriesMetadataRowHistogram), kept per hour across
// requests: an hour older than -query-range-freshness for an hour, a younger one for the freshness window. A request
// whose window moves reads only the hours no request read before, in one read.
func (p *Proxy) seriesMetadataRowsCached(ctx context.Context, read logsql.SeriesIdentityRead, from, to int64) (rowHistogram, error) {
	hour, minute := metadataSeriesDiscoveryAlign.Nanoseconds(), metadataSeriesRowBucket.Nanoseconds()
	lo, hi := from/hour*hour, (to/hour+1)*hour
	out := rowHistogram{lo: from / minute * minute, hi: (to/minute + 1) * minute}
	out.hours = make([]int64, (out.hi-out.lo)/minute)
	key := func(h int64) string {
		return "smrows:" + p.contextScopeFingerprint(ctx) + ":" + read.Filter() + ":" + strconv.FormatInt(h, 10)
	}
	put := func(h int64, counts []int64) {
		for i, c := range counts {
			if j := (h + int64(i)*minute - out.lo) / minute; j >= 0 && j < int64(len(out.hours)) {
				out.hours[j] = c
			}
		}
	}
	missLo, missHi := int64(-1), int64(-1)
	for h := lo; h < hi; h += hour {
		if raw, ok := p.cacheGet(ctx, p.streamFieldNamesCache, key(h)); ok {
			if counts, ok := decodeMinuteCounts(raw); ok {
				put(h, counts)
				continue
			}
		}
		if missLo < 0 {
			missLo = h
		}
		missHi = h + hour
	}
	if missLo < 0 {
		return out, nil
	}
	fetched, err := p.seriesMetadataRowHistogram(ctx, read, missLo, missHi-1)
	if err != nil {
		return rowHistogram{}, err
	}
	settled := time.Now().Add(-p.queryRangeFreshness).UnixNano()
	for h := missLo; h < missHi; h += hour {
		counts := make([]int64, hour/minute)
		for i := range counts {
			if j := (h + int64(i)*minute - fetched.lo) / minute; j >= 0 && j < int64(len(fetched.hours)) {
				counts[i] = fetched.hours[j]
			}
		}
		put(h, counts)
		ttl := p.queryRangeFreshness
		if h+hour <= settled {
			ttl = time.Hour
		}
		if ttl > 0 {
			p.streamFieldNamesCache.SetWithTTL(key(h), encodeMinuteCounts(counts), ttl)
		}
	}
	return out, nil
}

func encodeMinuteCounts(counts []int64) []byte {
	b := make([]byte, 0, len(counts)*4)
	for i, c := range counts {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, c, 10)
	}
	return b
}

func decodeMinuteCounts(raw []byte) ([]int64, bool) {
	parts := strings.Split(string(raw), ",")
	counts := make([]int64, len(parts))
	for i, part := range parts {
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, false
		}
		counts[i] = n
	}
	return counts, true
}

// metadataSeriesRowBucket is the grain of a request's row counts: a read's rows are those of every minute it touches,
// at most two minutes more than its own.
const metadataSeriesRowBucket = time.Minute

// rowHistogram is the rows a filter matches per whole minute of [lo, hi).
type rowHistogram struct {
	lo, hi int64
	hours  []int64
}

// rows is the rows of every minute [from, to] touches: at least the rows of [from, to].
func (h rowHistogram) rows(from, to int64) int64 {
	align := metadataSeriesRowBucket.Nanoseconds()
	var n int64
	for i, c := range h.hours {
		start := h.lo + int64(i)*align
		if start <= to && start+align > from {
			n += c
		}
	}
	return n
}

// seriesMetadataRowHistogram reads the rows read's filters match per minute of [from, to] widened to whole minutes, in
// one stats_query_range read: the request's reads (its time chunks, its operands) are bounded from it without a read
// each.
func (p *Proxy) seriesMetadataRowHistogram(ctx context.Context, read logsql.SeriesIdentityRead, from, to int64) (rowHistogram, error) {
	align := metadataSeriesRowBucket.Nanoseconds()
	h := rowHistogram{lo: from / align * align, hi: (to/align + 1) * align}
	h.hours = make([]int64, (h.hi-h.lo)/align)
	count := url.Values{}
	count.Set("query", read.RowCount())
	count.Set("start", strconv.FormatInt(h.lo, 10))
	count.Set("end", strconv.FormatInt(h.hi-1, 10))
	count.Set("step", "60s")
	// The count is a read of its own: not the request's folded read, not one its chunk's meter counts.
	countCtx := context.WithValue(context.WithValue(ctx, seriesFoldKey{}, (*seriesFoldRead)(nil)), statsReadMeterKey{}, (*statsReadMeter)(nil))
	resp, err := p.vlPost(countCtx, statsQueryRangePath, count)
	if err != nil {
		return rowHistogram{}, err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	if err != nil {
		return rowHistogram{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return rowHistogram{}, p.redactedBackendStatusError("stats_query_range backend", resp.StatusCode, body)
	}
	var answer struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return rowHistogram{}, err
	}
	for _, r := range answer.Data.Result {
		for _, v := range r.Values {
			if len(v) != 2 {
				continue
			}
			ts, _ := v[0].(float64)
			c, _ := v[1].(string)
			n, _ := strconv.ParseInt(c, 10, 64)
			if i := (int64(ts*1e9) - h.lo) / align; i >= 0 && i < int64(len(h.hours)) {
				h.hours[i] += n
			}
		}
	}
	return h, nil
}

// metadataSeriesRowBytes is the fewest bytes a series of one row takes in a rebuilt read's answer (its _stream with
// the stream labels and the metadata, the bucket and value): a read of more rows than the stats body bound divided by
// it passes the bound when its rows are series of their own.
const metadataSeriesRowBytes = 200

// defaultSeriesMetadataMaxReadRows is -structured-metadata-series-max-read-rows' default: a read of 6 h of the bench
// generator (about 700,000 groups) fit VictoriaLogs' 4 GiB, a day of it did not; a quarter of a million rows per read
// leaves room for reads beside it.
const defaultSeriesMetadataMaxReadRows = 250_000

func seriesMetadataReadRowsOrDefault(n int) int {
	switch {
	case n == 0:
		return defaultSeriesMetadataMaxReadRows
	case n < 0:
		return 0
	}
	return n
}

func isMetadataReadRowsError(err error) bool {
	var e *metadataReadRowsError
	return errors.As(err, &e)
}

// metadataReadRowsError is a folded read over -structured-metadata-series-max-read-rows: the time chunks read it
// again in smaller chunks (noteStatsReadOverflow), and a single step over it is the chunks' limit error.
type metadataReadRowsError struct{ rowLimitError }

// metadataSplitChanges reports whether Loki's structured-metadata series can change query's answer, from its plan:
// a log query, a sum by labels (or of everything) over a function whose value adds up over series (count_over_time,
// rate, bytes_over_time, bytes_rate, sum_over_time), a max by over max_over_time, a min by over min_over_time and an
// absent_over_time answer the same over a series per stream, and everything above such an aggregation reads its
// groups. A query whose plan is unknown, and every other shape, is answered with the split.
func metadataSplitChanges(query string) bool {
	plan := resolvedPlan(query)
	if plan == nil {
		return true
	}
	return splitChangesNode(plan.Root)
}

func splitChangesNode(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.LogSelect, *queryplan.Scalar, *queryplan.VectorLit:
		return false
	case *queryplan.RangeAgg:
		return n.Op != logql.RangeAbsentOverTime
	case *queryplan.VectorAgg:
		if ra, ok := n.In.(*queryplan.RangeAgg); ok {
			return (n.By != nil && n.By.Without) || !splitFreeAggregation(n.Op, ra)
		}
		// An aggregation over groups the split does not change reads those groups.
		return splitChangesNode(n.In)
	case *queryplan.BinOp:
		return splitChangesNode(n.L) || splitChangesNode(n.R)
	case *queryplan.LabelReplace:
		return splitChangesNode(n.In)
	case *queryplan.Rank:
		// A ranking of the series of n.In: of groups the split does not change, the same ranking.
		return splitChangesNode(n.In)
	}
	return true
}

// splitFreeAggregation reports a vector aggregation by labels over ra whose value per group is the same whether the
// group's samples come from a series per stream or a series per stream and metadata set.
func splitFreeAggregation(op logql.VectorOp, ra *queryplan.RangeAgg) bool {
	if ra.By != nil {
		return false
	}
	switch op {
	case logql.VectorSum:
		switch ra.Op {
		case logql.RangeCountOverTime, logql.RangeRate, logql.RangeBytesOverTime, logql.RangeBytesRate, logql.RangeSumOverTime:
			return true
		}
	case logql.VectorMax:
		return ra.Op == logql.RangeMaxOverTime
	case logql.VectorMin:
		return ra.Op == logql.RangeMinOverTime
	}
	return false
}

// seriesMetadataListing is a field listing of params' window in one VictoriaLogs call (cached as the label listings
// are): the identity's listings need no per-day or per-hour pieces of the window, which a cold request would read one
// by one.
func (p *Proxy) seriesMetadataListing(ctx context.Context, path, prefix string, params url.Values) ([]string, error) {
	key := p.metadataFieldNamesCacheKey(ctx, prefix, params)
	if fields, ok := p.cachedFieldNames(ctx, key); ok {
		return fields, nil
	}
	items, err := p.fetchVLListingOnce(ctx, path, params)
	if err != nil {
		return nil, err
	}
	fields := listingValues(items)
	p.storeFieldNames(key, fields)
	return fields, nil
}
