package proxy

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
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
	counts  map[string]int64
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

func (m *metadataSeriesMemo) rows(key string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.counts[key]
	return n, ok
}

func (m *metadataSeriesMemo) setRows(key string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int64{}
	}
	if len(m.counts) < 64 {
		m.counts[key] = n
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

// withMetadataSeries marks ctx's reads for the identity when the operator declared it.
func (p *Proxy) withMetadataSeries(ctx context.Context) context.Context {
	if !p.structuredMetadataSeries {
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
	fields, err := p.fetchAllFieldNamesCached(ctx, params)
	if err != nil {
		return logsql.SeriesMetadata{}, false, err
	}
	stream, err := p.fetchStreamFieldNamesCached(ctx, params)
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
	bound := int64(p.limits().BufferedBackendBodyBytes)
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
	key := read.Filter() + "\x00" + params.Get("start") + "\x00" + params.Get("end")
	if rows, ok := memo.rows(key); ok {
		return rows, nil
	}
	count := url.Values{}
	count.Set("query", read.RowCount())
	count.Set("start", params.Get("start"))
	count.Set("end", params.Get("end"))
	count.Set("time", params.Get("end"))
	resp, err := p.vlPost(ctx, "/select/logsql/stats_query", count)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, p.redactedBackendStatusError("stats_query backend", resp.StatusCode, body)
	}
	var answer struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return 0, err
	}
	var rows int64
	for _, r := range answer.Data.Result {
		if len(r.Value) == 2 {
			if s, ok := r.Value[1].(string); ok {
				n, _ := strconv.ParseInt(s, 10, 64)
				rows += n
			}
		}
	}
	memo.setRows(key, rows)
	return rows, nil
}

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
