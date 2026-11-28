package proxy

import (
	"context"
	"net/url"
	"strconv"
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

// metadataSeriesKey marks a metric request whose reads take Loki's structured-metadata series identity.
type metadataSeriesKey struct{}

// withMetadataSeries marks ctx's reads for the identity when the operator declared it.
func (p *Proxy) withMetadataSeries(ctx context.Context) context.Context {
	if !p.structuredMetadataSeries {
		return ctx
	}
	return context.WithValue(ctx, metadataSeriesKey{}, true)
}

func metadataSeriesOn(ctx context.Context) bool {
	on, _ := ctx.Value(metadataSeriesKey{}).(bool)
	return on
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
	if !metadataSeriesOn(ctx) {
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
	spec, ok, err := p.seriesMetadataFor(ctx, *q, from, to)
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
// stream fields and all fields VictoriaLogs lists for them there (field_names and stream_field_names, both cached).
func (p *Proxy) seriesMetadataFor(ctx context.Context, q logsql.Query, from, to int64) (logsql.SeriesMetadata, bool, error) {
	align := metadataSeriesDiscoveryAlign.Nanoseconds()
	lo := from / align * align
	hi := (to + align - 1) / align * align
	filter := logsql.Query{Filter: q.Filter}
	params := url.Values{}
	params.Set("query", filter.String())
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
	return spec, ok, nil
}
