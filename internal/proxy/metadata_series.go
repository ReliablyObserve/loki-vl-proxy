package proxy

import (
	"context"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

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
	q, err := logsql.Parse(text)
	if err != nil {
		p.observeInternalOperation(ctx, "metadata_series_identity", "unread", time.Since(started))
		return params
	}
	if !logsql.ReadsStreamIdentity(*q) {
		return params
	}
	from, okFrom := parseFlexibleUnixNanos(params.Get("start"))
	to, okTo := parseFlexibleUnixNanos(params.Get("end"))
	if !okFrom || !okTo || to < from {
		p.observeInternalOperation(ctx, "metadata_series_identity", "no_window", time.Since(started))
		return params
	}
	spec, ok, err := p.seriesMetadataFor(ctx, memo, *q, from, to)
	if err != nil {
		p.observeInternalOperation(ctx, "metadata_series_identity", "listing_failed", time.Since(started))
		return params
	}
	if !ok {
		p.observeInternalOperation(ctx, "metadata_series_identity", "no_metadata", time.Since(started))
		return params
	}
	rebuilt, changed := logsql.WithSeriesMetadata(*q, spec)
	if !changed {
		return params
	}
	out := make(url.Values, len(params))
	for k, v := range params {
		out[k] = v
	}
	out["query"] = []string{rebuilt.String()}
	p.observeInternalOperation(ctx, "metadata_series_identity", "rebuilt", time.Since(started))
	return out
}

// seriesMetadataFor is the series identity of the rows q's filters read in [from, to], widened to whole hours: the
// stream fields and all fields VictoriaLogs lists for them there (field_names and stream_field_names, both cached),
// or the identity the request listed already over a window that holds this one.
func (p *Proxy) seriesMetadataFor(ctx context.Context, memo *metadataSeriesMemo, q logsql.Query, from, to int64) (logsql.SeriesMetadata, bool, error) {
	filter := logsql.Query{Filter: q.Filter}.String()
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
