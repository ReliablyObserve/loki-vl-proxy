// Package workload defines the query workloads used in benchmarks.
package workload

import (
	"fmt"
	"net/url"
	"time"
)

// Query is a single HTTP request definition.
type Query struct {
	Name   string
	Method string // GET or POST
	Path   string // URL path
	Params url.Values
}

func (q Query) URL(base string) string {
	u := base + q.Path
	if len(q.Params) > 0 {
		u += "?" + q.Params.Encode()
	}
	return u
}

// Workload is a named collection of queries.
type Workload struct {
	Name    string
	Queries []Query
}

// Small: metadata + short log selects (≤5 min window).
// Exercises Grafana Explore label browser, small panel refreshes, metadata cache (T0/L1).
func Small(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start1m := ns(now.Add(-1 * time.Minute))
	end := ns(now)

	return Workload{Name: "small", Queries: []Query{
		{
			Name:   "labels",
			Path:   "/loki/api/v1/labels",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		{
			Name:   "label_values_app",
			Path:   "/loki/api/v1/label/app/values",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		{
			Name:   "label_values_namespace",
			Path:   "/loki/api/v1/label/namespace/values",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		{
			Name:   "label_values_level",
			Path:   "/loki/api/v1/label/level/values",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		{
			Name: "series",
			Path: "/loki/api/v1/series",
			Params: url.Values{
				"match[]": {`{app=~".+"}`},
				"start":   {start5m},
				"end":     {end},
			},
		},
		{
			Name: "query_range_simple_1m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start1m},
				"end":   {end},
				"limit": {"200"},
			},
		},
		{
			Name: "query_range_simple_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m},
				"end":   {end},
				"limit": {"500"},
			},
		},
		{
			Name: "query_range_filter_1m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} |= "error"`},
				"start": {start1m},
				"end":   {end},
				"limit": {"100"},
			},
		},
		{
			Name: "query_instant_count",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`count_over_time({app="api-gateway"}[5m])`},
				"time":  {end},
			},
		},
		{
			Name: "query_instant_rate",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[1m]))`},
				"time":  {end},
			},
		},
		{
			Name: "detected_fields_small",
			Path: "/loki/api/v1/detected_fields",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start5m},
				"end":   {end},
			},
		},
		{
			Name: "index_stats",
			Path: "/loki/api/v1/index/stats",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m},
				"end":   {end},
			},
		},
	}}
}

// Heavy: complex pipelines, metric aggregations, full-volume log returns.
// Exercises proxy translation overhead, VL field indexing, metric shaping.
func Heavy(now time.Time) Workload {
	start15m := ns(now.Add(-15 * time.Minute))
	start30m := ns(now.Add(-30 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	start2h := ns(now.Add(-2 * time.Hour))
	end := ns(now)

	return Workload{Name: "heavy", Queries: []Query{
		// JSON parse + filter — exercises proxy | json translation + VL field search.
		{
			Name: "json_parse_filter_status",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | status >= 400`},
				"start": {start30m},
				"end":   {end},
				"limit": {"1000"},
			},
		},
		{
			Name: "json_parse_multi_field",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | status >= 200 | status < 500 | latency_ms > 100`},
				"start": {start30m},
				"end":   {end},
				"limit": {"500"},
			},
		},
		{
			Name: "json_line_format",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | line_format "{{.method}} {{.path}} {{.status}} {{.latency_ms}}ms"`},
				"start": {start15m},
				"end":   {end},
				"limit": {"200"},
			},
		},
		// Logfmt parse + filter.
		{
			Name: "logfmt_parse_error",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="payment-service"} | logfmt | level="error"`},
				"start": {start30m},
				"end":   {end},
				"limit": {"500"},
			},
		},
		{
			Name: "logfmt_latency_filter",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="worker-service"} | logfmt | duration_ms > 5000`},
				"start": {start1h},
				"end":   {end},
				"limit": {"200"},
			},
		},
		// Regex line filter.
		{
			Name: "regex_filter",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |~ "status=(4|5)[0-9][0-9]"`},
				"start": {start30m},
				"end":   {end},
				"limit": {"500"},
			},
		},
		// Metric aggregations — rate/count/bytes over various windows.
		{
			Name: "metric_rate_by_app",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[5m]))`},
				"start": {start1h},
				"end":   {end},
				"step":  {"60"},
			},
		},
		{
			Name: "metric_rate_by_status",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m]))`},
				"start": {start1h},
				"end":   {end},
				"step":  {"60"},
			},
		},
		{
			Name: "metric_count_by_level",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (level) (count_over_time({namespace="prod"}[5m]))`},
				"start": {start2h},
				"end":   {end},
				"step":  {"60"},
			},
		},
		{
			Name: "metric_bytes_rate",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (bytes_rate({namespace="prod"}[5m]))`},
				"start": {start1h},
				"end":   {end},
				"step":  {"60"},
			},
		},
		// Topk + quantile — complex aggregation shapes.
		{
			Name: "topk_apps",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`topk(5, sum by (app) (rate({namespace="prod"}[5m])))`},
				"start": {start1h},
				"end":   {end},
				"step":  {"60"},
			},
		},
		// Full detected_fields over all services.
		{
			Name: "detected_fields_all",
			Path: "/loki/api/v1/detected_fields",
			Params: url.Values{
				"query": {`{namespace="prod"} | json`},
				"start": {start30m},
				"end":   {end},
			},
		},
		// Patterns — proxy clustering.
		{
			Name: "patterns_prod",
			Path: "/loki/api/v1/patterns",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start1h},
				"end":   {end},
			},
		},
		// Full volume log return — large response payload.
		{
			Name: "full_volume_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start1h},
				"end":   {end},
				"limit": {"5000"},
			},
		},
		// Volume endpoint.
		{
			Name: "volume_range",
			Path: "/loki/api/v1/index/volume_range",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start1h},
				"end":   {end},
				"step":  {"60"},
			},
		},
	}}
}

// LongRange: 6h, 24h, 48h, 72h windows.
// Exercises proxy query-range windowing, prefilter, adaptive parallelism, historical cache.
func LongRange(now time.Time) Workload {
	start6h := ns(now.Add(-6 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	start48h := ns(now.Add(-48 * time.Hour))
	start72h := ns(now.Add(-72 * time.Hour))
	end := ns(now)

	return Workload{Name: "long_range", Queries: []Query{
		// Simple log selects over long windows — tests windowing + cache.
		{
			Name: "log_select_6h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start6h},
				"end":   {end},
				"limit": {"2000"},
			},
		},
		{
			Name: "log_select_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start24h},
				"end":   {end},
				"limit": {"2000"},
			},
		},
		{
			Name: "log_select_error_48h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |= "error"`},
				"start": {start48h},
				"end":   {end},
				"limit": {"1000"},
			},
		},
		// Metric rate over long windows — many windows × step points.
		{
			Name: "rate_by_app_6h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[5m]))`},
				"start": {start6h},
				"end":   {end},
				"step":  {"300"},
			},
		},
		{
			Name: "rate_by_app_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[5m]))`},
				"start": {start24h},
				"end":   {end},
				"step":  {"300"},
			},
		},
		{
			Name: "count_by_level_48h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (level) (count_over_time({namespace="prod"}[1h]))`},
				"start": {start48h},
				"end":   {end},
				"step":  {"3600"},
			},
		},
		{
			Name: "bytes_rate_72h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(bytes_rate({namespace="prod"}[1h]))`},
				"start": {start72h},
				"end":   {end},
				"step":  {"3600"},
			},
		},
		// Metadata over long windows.
		{
			Name:   "labels_24h",
			Path:   "/loki/api/v1/labels",
			Params: url.Values{"start": {start24h}, "end": {end}},
		},
		{
			Name: "series_24h",
			Path: "/loki/api/v1/series",
			Params: url.Values{
				"match[]": {`{namespace="prod"}`},
				"start":   {start24h},
				"end":     {end},
			},
		},
		// Full volume — large response, stresses windowing + merge.
		{
			Name: "full_volume_json_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | status >= 400`},
				"start": {start24h},
				"end":   {end},
				"limit": {"5000"},
			},
		},
	}}
}

// Compute: CPU-intensive metric processing — multi-level aggregations, math operations,
// parse pipelines, unwrap aggregations. Exercises proxy translation overhead and
// actual query engine CPU for rate/quantile/division computations.
func Compute(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start15m := ns(now.Add(-15 * time.Minute))
	start30m := ns(now.Add(-30 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	end := ns(now)

	return Workload{Name: "compute", Queries: []Query{
		// Arithmetic on aggregated rates: per-minute count from rate
		{
			Name: "rate_x60_per_min",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(rate({namespace="prod"}[5m])) * 60`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Division: HTTP error rate as percentage (requires 2-level aggregation + div)
		{
			Name: "error_rate_pct",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(rate({app="api-gateway"} | json | status >= 400 [5m])) / sum(rate({app="api-gateway"} | json [5m])) * 100`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// 5xx-only error rate
		{
			Name: "5xx_rate_pct",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(rate({app="api-gateway"} | json | status >= 500 [5m])) / sum(rate({app="api-gateway"}[5m])) * 100`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// topk: top-3 apps by rate
		{
			Name: "topk3_rate_by_app",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`topk(3, sum by (app) (rate({namespace="prod"}[5m])))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Multi-label grouping: sum by app + region
		{
			Name: "rate_by_app_region",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app, region) (rate({namespace="prod"}[5m]))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Unwrap avg latency (requires JSON parse → unwrap → avg_over_time)
		{
			Name: "avg_latency_unwrap",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`avg_over_time({app="api-gateway"} | json | unwrap latency_ms [5m]) by (app)`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// p99 latency via quantile_over_time (most expensive unwrap aggregation)
		{
			Name: "p99_latency_unwrap",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`quantile_over_time(0.99, {app="api-gateway"} | json | unwrap latency_ms [5m]) by (app)`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// bytes_rate: throughput in bytes/sec
		{
			Name: "bytes_rate_by_app",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (bytes_rate({namespace="prod"}[5m]))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Multi-stage parse pipeline with line_format (parse → filter → format)
		{
			Name: "json_pipeline_format",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} | json | status >= 400 | level="error" | line_format "{{.app}} ERR {{.status}} {{.latency_ms}}ms"`},
				"start": {start15m}, "end": {end},
				"limit": {"500"},
			},
		},
		// Nested label_replace on metric result (post-processing math)
		{
			Name: "count_div_3600",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (count_over_time({namespace="prod"}[1h])) / 3600`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Multi-filter with logfmt: latency p99 via quantile_over_time
		{
			Name: "logfmt_p99_latency",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`quantile_over_time(0.99, {app="worker-service"} | logfmt | unwrap duration_ms [5m]) by (app)`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Subtraction: error count minus warn count
		{
			Name: "error_minus_warn",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(count_over_time({namespace="prod"} | logfmt | level="error" [5m])) - sum(count_over_time({namespace="prod"} | logfmt | level="warn" [5m]))`},
				"start": {start30m}, "end": {end}, "step": {"60"},
			},
		},
		// Rate with complex multi-stage parse (JSON + field filter + regex)
		{
			Name: "rate_json_regex_filter",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"} | json | status >= 400 | method=~"POST|PUT|DELETE" [5m]))`},
				"start": {start5m}, "end": {end}, "step": {"60"},
			},
		},
		// Instant: sum of rates across all apps (scalar aggregation at query time)
		{
			Name: "instant_sum_rates",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`sum(rate({namespace="prod"}[5m]))`},
				"time":  {end},
			},
		},
		// Instant: topk error apps
		{
			Name: "instant_topk_errors",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`topk(5, sum by (app) (rate({namespace="prod"} | json | status >= 400 [5m])))`},
				"time":  {end},
			},
		},
	}}
}

// UnindexedScan: content-search queries that expose the fundamental indexing gap between
// Loki (no content index → full chunk scan) and VictoriaLogs (inverted token index →
// targeted block lookup). Use this workload with a large dataset to show the O(total_bytes)
// vs O(matching_blocks) scaling difference.
//
// Loki must scan every chunk belonging to the matched streams to find substring/regex
// matches in log content. VictoriaLogs maintains a word-level inverted index, so most
// token searches skip non-matching blocks entirely.
func UnindexedScan(now time.Time) Workload {
	start1h := ns(now.Add(-1 * time.Hour))
	start6h := ns(now.Add(-6 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	end := ns(now)

	return Workload{Name: "unindexed_scan", Queries: []Query{
		// Word match — Loki: full chunk scan. VL: inverted index lookup.
		{
			Name: "word_timeout_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |= "timeout"`},
				"start": {start1h}, "end": {end}, "limit": {"1000"},
			},
		},
		{
			Name: "word_declined_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |= "declined"`},
				"start": {start1h}, "end": {end}, "limit": {"1000"},
			},
		},
		// Regex match — Loki: regex scan of all chunks. VL: regex on candidate blocks.
		{
			Name: "regex_user_id_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |~ "usr_[0-9]{5}"`},
				"start": {start1h}, "end": {end}, "limit": {"500"},
			},
		},
		{
			Name: "regex_status_codes_6h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |~ "status=(4|5)[0-9][0-9]"`},
				"start": {start6h}, "end": {end}, "limit": {"500"},
			},
		},
		// Negation filter — Loki must still scan all chunks to verify absence.
		{
			Name: "exclude_info_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} != "info"`},
				"start": {start1h}, "end": {end}, "limit": {"500"},
			},
		},
		// Rate metric over content-filtered stream — full scan per rate window.
		{
			Name: "rate_error_content_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum(rate({namespace="prod"} |= "error" [5m]))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		{
			Name: "rate_timeout_content_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"} |= "timeout" [5m]))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Long-window content scan — most expensive case for Loki (24h × all streams).
		{
			Name: "word_error_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |= "error"`},
				"start": {start24h}, "end": {end}, "limit": {"1000"},
			},
		},
		{
			Name: "regex_5xx_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |~ "5[0-9][0-9]"`},
				"start": {start24h}, "end": {end}, "limit": {"500"},
			},
		},
		// Count over content filter — aggregation forces full scan.
		{
			Name: "count_errors_24h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (count_over_time({namespace="prod"} |= "error" [1h]))`},
				"start": {start24h}, "end": {end}, "step": {"3600"},
			},
		},
	}}
}

// HighCardinality: queries over high-cardinality label streams. These expose Loki's
// O(streams × retention) memory model — Loki ingesters must track every unique label
// combination as a separate stream, holding chunk metadata for each in RAM. VictoriaLogs
// uses columnar storage with a single stream-level index, so cardinality has minimal
// memory impact beyond index size.
//
// Requires seeding with --high-cardinality flag (adds pod as a stream label with
// --pods-per-service unique pods, creating N×services unique Loki streams).
func HighCardinality(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	end := ns(now)

	return Workload{Name: "high_cardinality", Queries: []Query{
		// Series count — returns one entry per unique label combination.
		// At high cardinality, this response is huge and Loki must materialize all stream IDs.
		{
			Name: "series_all_1h",
			Path: "/loki/api/v1/series",
			Params: url.Values{
				"match[]": {`{namespace="prod"}`},
				"start":   {start1h}, "end": {end},
			},
		},
		{
			Name: "series_all_24h",
			Path: "/loki/api/v1/series",
			Params: url.Values{
				"match[]": {`{namespace="prod"}`},
				"start":   {start24h}, "end": {end},
			},
		},
		// Label values for a high-cardinality label (pod).
		{
			Name: "label_values_pod_1h",
			Path: "/loki/api/v1/label/pod/values",
			Params: url.Values{"start": {start1h}, "end": {end}},
		},
		{
			Name: "label_values_pod_24h",
			Path: "/loki/api/v1/label/pod/values",
			Params: url.Values{"start": {start24h}, "end": {end}},
		},
		// Per-pod selection — fan-out across many streams at query time.
		{
			Name: "query_range_by_pod_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod",app="api-gateway"}`},
				"start": {start1h}, "end": {end}, "limit": {"1000"},
			},
		},
		// Aggregation across all high-cardinality streams.
		{
			Name: "rate_by_pod_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (pod) (rate({namespace="prod"}[5m]))`},
				"start": {start1h}, "end": {end}, "step": {"60"},
			},
		},
		// Count distinct pods (cardinality measurement).
		{
			Name: "count_distinct_pods_1h",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`count(sum by (pod) (count_over_time({namespace="prod"}[1m])))`},
				"start": {start1h}, "end": {end}, "step": {"300"},
			},
		},
		// Index stats — at high cardinality, Loki must enumerate all matching streams.
		{
			Name: "index_stats_prod_1h",
			Path: "/loki/api/v1/index/stats",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start1h}, "end": {end},
			},
		},
		{
			Name: "index_stats_prod_24h",
			Path: "/loki/api/v1/index/stats",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start24h}, "end": {end},
			},
		},
		// Detected fields — must scan all streams to discover fields across high-cardinality data.
		{
			Name: "detected_fields_prod_5m",
			Path: "/loki/api/v1/detected_fields",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m}, "end": {end},
			},
		},
	}}
}

// Machinery: a curated mix of representative proxy operations designed to
// measure raw proxy overhead — LogQL→LogsQL translation, HTTP proxying,
// response shaping — without the coalescer or cache short-circuiting results.
//
// Always run with --unique-windows so every worker sends a distinct URL and
// against a proxy started with -cache-disabled -label-values-indexed-cache=false
// to bypass both the L1 response cache and the in-process label-values index.
//
// Query variety covers every distinct proxy translation/shaping code path and
// uses label values that exist in the e2e-compat stack (api-gateway, auth-service,
// batch-etl, cache-redis, frontend-ssr, ml-serving, nginx-ingress, payment-service,
// worker-service; namespaces: batch, data, ingress-nginx, ml, prod).
func Machinery(now time.Time) Workload {
	start1m := ns(now.Add(-1 * time.Minute))
	start5m := ns(now.Add(-5 * time.Minute))
	start15m := ns(now.Add(-15 * time.Minute))
	end := ns(now)

	return Workload{Name: "machinery", Queries: []Query{
		// ── Log selects (VL NDJSON → Loki streams conversion) ─────────────────
		// Simple stream select — baseline log proxy path.
		{
			Name: "log_select_api_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start5m}, "end": {end}, "limit": {"200"},
			},
		},
		// Different service — exercises different stream data paths.
		{
			Name: "log_select_frontend_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="frontend-ssr"}`},
				"start": {start5m}, "end": {end}, "limit": {"200"},
			},
		},
		// Namespace-scoped select across all services.
		{
			Name: "log_select_namespace_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m}, "end": {end}, "limit": {"300"},
			},
		},
		// Content filter — exercises |= word filter path (not just stream select).
		{
			Name: "log_filter_error_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{namespace="prod"} |= "error"`},
				"start": {start5m}, "end": {end}, "limit": {"200"},
			},
		},
		// Regex filter — exercises |~ regex path.
		{
			Name: "log_regex_status_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} |~ "5[0-9][0-9]"`},
				"start": {start5m}, "end": {end}, "limit": {"200"},
			},
		},

		// ── JSON parse pipelines (| json → | unpack_json translation) ─────────
		// JSON parse + numeric field filter.
		{
			Name: "json_parse_status_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | status >= 400`},
				"start": {start5m}, "end": {end}, "limit": {"200"},
			},
		},
		// JSON parse + multiple field filters.
		{
			Name: "json_parse_multi_field_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="api-gateway"} | json | status >= 200 | status < 500`},
				"start": {start5m}, "end": {end}, "limit": {"100"},
			},
		},
		// JSON parse on different service.
		{
			Name: "json_parse_ml_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="ml-serving"} | json`},
				"start": {start5m}, "end": {end}, "limit": {"100"},
			},
		},

		// ── Logfmt parse (| logfmt → | unpack_logfmt translation) ────────────
		// Logfmt parse + level filter.
		{
			Name: "logfmt_level_filter_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="auth-service"} | logfmt | level="error"`},
				"start": {start5m}, "end": {end}, "limit": {"100"},
			},
		},
		// Logfmt on a different service.
		{
			Name: "logfmt_cache_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`{app="cache-redis"} | logfmt`},
				"start": {start5m}, "end": {end}, "limit": {"100"},
			},
		},

		// ── Metric aggregations (rate/count, metric shaping) ──────────────────
		// rate grouped by app — most common metric query shape.
		{
			Name: "rate_by_app_15m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[1m]))`},
				"start": {start15m}, "end": {end}, "step": {"60"},
			},
		},
		// rate grouped by level — different grouping dimension.
		{
			Name: "rate_by_level_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (level) (rate({namespace="prod"}[1m]))`},
				"start": {start5m}, "end": {end}, "step": {"60"},
			},
		},
		// count_over_time — exercises count aggregation shaping.
		{
			Name: "count_by_app_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (count_over_time({namespace="prod"}[1m]))`},
				"start": {start5m}, "end": {end}, "step": {"60"},
			},
		},
		// bytes_rate — exercises bytes metric translation.
		{
			Name: "bytes_rate_prod_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (bytes_rate({namespace="prod"}[1m]))`},
				"start": {start5m}, "end": {end}, "step": {"60"},
			},
		},
		// rate on filtered stream (json parse + filter + aggregation chain).
		{
			Name: "rate_error_json_5m",
			Path: "/loki/api/v1/query_range",
			Params: url.Values{
				"query": {`sum by (app) (rate({app="api-gateway"} | json | status >= 500 [1m]))`},
				"start": {start5m}, "end": {end}, "step": {"60"},
			},
		},

		// ── Detected fields (OTel field detection + VL metadata) ──────────────
		// detected_fields for api-gateway — json fields.
		{
			Name: "detected_fields_api_5m",
			Path: "/loki/api/v1/detected_fields",
			Params: url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {start5m}, "end": {end},
			},
		},
		// detected_fields across prod namespace — broader field scan.
		{
			Name: "detected_fields_prod_5m",
			Path: "/loki/api/v1/detected_fields",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m}, "end": {end},
			},
		},

		// ── Metadata endpoints (labels/series via VL — no label index) ─────────
		// labels with query scope (different per timestamp).
		{
			Name:   "labels_scoped_1m",
			Path:   "/loki/api/v1/labels",
			Params: url.Values{"query": {`{app="api-gateway"}`}, "start": {start1m}, "end": {end}},
		},
		// label_values for app — cardinality query.
		{
			Name:   "label_values_app_5m",
			Path:   "/loki/api/v1/label/app/values",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		// label_values for level — low-cardinality dimension.
		{
			Name:   "label_values_level_5m",
			Path:   "/loki/api/v1/label/level/values",
			Params: url.Values{"start": {start5m}, "end": {end}},
		},
		// series — stream label enumeration.
		{
			Name: "series_prod_5m",
			Path: "/loki/api/v1/series",
			Params: url.Values{
				"match[]": {`{namespace="prod"}`},
				"start":   {start5m}, "end": {end},
			},
		},

		// ── Instant queries (/query path, not /query_range) ───────────────────
		// Instant rate — exercises /loki/api/v1/query path.
		{
			Name: "instant_rate_prod_1m",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`sum by (app) (rate({namespace="prod"}[1m]))`},
				"time":  {end},
			},
		},
		// Instant count — different aggregation on the same path.
		{
			Name: "instant_count_api_1m",
			Path: "/loki/api/v1/query",
			Params: url.Values{
				"query": {`sum(count_over_time({app="api-gateway"}[1m]))`},
				"time":  {end},
			},
		},

		// ── Index stats + volume ───────────────────────────────────────────────
		// index_stats — exercises stats endpoint translation.
		{
			Name: "index_stats_prod_5m",
			Path: "/loki/api/v1/index/stats",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m}, "end": {end},
			},
		},
		// volume — exercises volume endpoint shaping.
		{
			Name: "volume_prod_5m",
			Path: "/loki/api/v1/index/volume",
			Params: url.Values{
				"query": {`{namespace="prod"}`},
				"start": {start5m}, "end": {end},
			},
		},
	}}
}

// All returns all standard workloads for the given time reference.
func All(now time.Time) []Workload {
	return []Workload{Small(now), Heavy(now), LongRange(now), Compute(now)}
}

// AllEdgeCases returns edge-case workloads that expose architectural trade-offs.
// These require specific data shapes (unindexed_scan: any data; high_cardinality:
// seed with --high-cardinality flag) and are not included in the default run.
func AllEdgeCases(now time.Time) []Workload {
	return []Workload{UnindexedScan(now), HighCardinality(now)}
}

// ByName returns the named workloads, including edge-case and machinery workloads.
func ByName(names []string, now time.Time) []Workload {
	all := append(All(now), AllEdgeCases(now)...)
	all = append(all, Machinery(now))
	if len(names) == 0 {
		return All(now)
	}
	m := make(map[string]Workload, len(all))
	for _, w := range all {
		m[w.Name] = w
	}
	var result []Workload
	for _, n := range names {
		if w, ok := m[n]; ok {
			result = append(result, w)
		}
	}
	return result
}

func ns(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixNano())
}
