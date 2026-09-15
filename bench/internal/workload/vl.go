// Package workload defines VictoriaLogs native LogsQL workloads for benchmarking.
// These mirror the Loki workloads but use VL's native API endpoints
// (/select/logsql/query, /select/logsql/hits, /select/logsql/field_names, etc.)
// with LogsQL syntax instead of LogQL.
//
// Tested against VictoriaLogs v1.50.0:
//   - range stats use /select/logsql/stats_query_range (start/end/step); the
//     instant /select/logsql/stats_query takes only `time`, so start/end/step
//     would be ignored and the query would scan
//     all stored data
//   - count() without by() clause works in stats_query_range
//   - grouped counts over time use /select/logsql/hits with field=<name> (the
//     equivalent of LogQL `sum by (detected_level) (count_over_time(...))`)
//   - rate() and bytes_rate() pipes are NOT supported
//   - hits endpoint uses step= (not granularity=)
//   - parsing uses the unpack_json / unpack_logfmt pipes: the seed stores the
//     raw line in _msg (no ingest-time parsing), and `| json` / `| logfmt` are
//     not LogsQL parser pipes (they return an empty result).
package workload

import (
	"net/url"
	"time"
)

// VLSmall mirrors Small using VictoriaLogs native LogsQL API (≤5 min windows).
func VLSmall(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start1m := ns(now.Add(-1 * time.Minute))
	end := ns(now)

	return Workload{Name: "small", Queries: []Query{
		{
			Name:   "field_names",
			Path:   "/select/logsql/field_names",
			Params: url.Values{"query": {"*"}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "field_values_app",
			Path:   "/select/logsql/field_values",
			Params: url.Values{"field": {"app"}, "query": {"*"}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "field_values_namespace",
			Path:   "/select/logsql/field_values",
			Params: url.Values{"field": {"namespace"}, "query": {"*"}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "field_values_cluster",
			Path:   "/select/logsql/field_values",
			Params: url.Values{"field": {"cluster"}, "query": {"*"}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "stream_ids",
			Path:   "/select/logsql/stream_ids",
			Params: url.Values{"query": {`app:*`}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "log_simple_1m",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway"`}, "start": {start1m}, "end": {end}, "limit": {"200"}},
		},
		{
			Name:   "log_simple_5m",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start5m}, "end": {end}, "limit": {"500"}},
		},
		{
			Name:   "log_filter_error_1m",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway" "error"`}, "start": {start1m}, "end": {end}, "limit": {"100"}},
		},
		{
			// stats_query_range count() without by() is supported in VL v1.50.0
			Name:   "stats_count_5m",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`app:"api-gateway" | count()`}, "start": {start5m}, "end": {end}, "step": {"5m"}},
		},
		{
			// hits endpoint is the equivalent of Loki's metric range query for time-series counts
			Name:   "hits_by_1m",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start5m}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "hits_app_1m",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`app:"api-gateway"`}, "start": {start5m}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "count_uniq_app",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`* | count_uniq(app)`}, "start": {start5m}, "end": {end}, "step": {"5m"}},
		},
	}}
}

// VLHeavy mirrors Heavy using VictoriaLogs native LogsQL (15min–2h windows, complex filters).
func VLHeavy(now time.Time) Workload {
	start15m := ns(now.Add(-15 * time.Minute))
	start30m := ns(now.Add(-30 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	start2h := ns(now.Add(-2 * time.Hour))
	end := ns(now)

	return Workload{Name: "heavy", Queries: []Query{
		{
			Name:   "log_json_status_filter",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400`}, "start": {start30m}, "end": {end}, "limit": {"1000"}},
		},
		{
			Name:   "log_json_multi_field",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=200 | status:<500 | latency_ms:>100`}, "start": {start30m}, "end": {end}, "limit": {"500"}},
		},
		{
			Name:   "log_json_extract",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json`}, "start": {start15m}, "end": {end}, "limit": {"200"}},
		},
		{
			Name:   "log_logfmt_error",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"payment-service" | unpack_logfmt | level:"error"`}, "start": {start30m}, "end": {end}, "limit": {"500"}},
		},
		{
			Name:   "log_logfmt_latency",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"worker-service" | unpack_logfmt | duration_ms:>5000`}, "start": {start1h}, "end": {end}, "limit": {"200"}},
		},
		{
			Name:   "log_regex_filter",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" ~"status.:(4|5)[0-9][0-9]"`}, "start": {start30m}, "end": {end}, "limit": {"500"}},
		},
		{
			Name:   "hits_prod_1h_1m",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "hits_errors_1h_1m",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "hits_level_2h_1m",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "field": {"level"}, "start": {start2h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "stats_count_errors",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400 | count()`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "stats_count_prod",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" | count()`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "field_names_json",
			Path:   "/select/logsql/field_names",
			Params: url.Values{"query": {`namespace:"prod" | unpack_json`}, "start": {start30m}, "end": {end}},
		},
		{
			Name:   "log_full_volume_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway"`}, "start": {start1h}, "end": {end}, "limit": {"5000"}},
		},
		{
			Name:   "hits_volume_1h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}, "step": {"5m"}},
		},
		{
			Name:   "stream_ids_prod",
			Path:   "/select/logsql/stream_ids",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}},
		},
	}}
}

// VLLongRange mirrors LongRange using VictoriaLogs native LogsQL (6h–72h windows).
func VLLongRange(now time.Time) Workload {
	start6h := ns(now.Add(-6 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	start48h := ns(now.Add(-48 * time.Hour))
	start72h := ns(now.Add(-72 * time.Hour))
	end := ns(now)

	return Workload{Name: "long_range", Queries: []Query{
		{
			Name:   "log_select_6h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway"`}, "start": {start6h}, "end": {end}, "limit": {"2000"}},
		},
		{
			Name:   "log_select_24h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}, "limit": {"2000"}},
		},
		{
			Name:   "log_error_48h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" "error"`}, "start": {start48h}, "end": {end}, "limit": {"1000"}},
		},
		{
			Name:   "hits_prod_6h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start6h}, "end": {end}, "step": {"5m"}},
		},
		{
			Name:   "hits_prod_24h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}, "step": {"5m"}},
		},
		{
			Name:   "hits_level_48h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "field": {"level"}, "start": {start48h}, "end": {end}, "step": {"1h"}},
		},
		{
			Name:   "hits_prod_72h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start72h}, "end": {end}, "step": {"1h"}},
		},
		{
			Name:   "field_names_24h",
			Path:   "/select/logsql/field_names",
			Params: url.Values{"query": {"*"}, "start": {start24h}, "end": {end}},
		},
		{
			Name:   "stream_ids_24h",
			Path:   "/select/logsql/stream_ids",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}},
		},
		{
			Name:   "log_json_errors_24h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400`}, "start": {start24h}, "end": {end}, "limit": {"5000"}},
		},
	}}
}

// VLCompute mirrors Compute using VictoriaLogs native LogsQL.
// Uses hits (time-series), stats_query_range count(), multi-stage parse pipelines,
// and count_uniq for cardinality — the VL equivalents of multi-layer LogQL aggregations.
func VLCompute(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start15m := ns(now.Add(-15 * time.Minute))
	start30m := ns(now.Add(-30 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	end := ns(now)

	return Workload{Name: "compute", Queries: []Query{
		// hits: per-minute rate of all prod logs (equivalent to rate × 60)
		{
			Name:   "hits_prod_1m_step",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// hits: per-minute rate of error logs only
		{
			Name:   "hits_errors_1m_step",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// hits: 5xx only
		{
			Name:   "hits_5xx_1m_step",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=500`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// stats_query count() on prod logs (equivalent of sum rate)
		{
			Name:   "stats_count_prod_1h",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" | count()`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// stats_query count() on errors (equivalent of error rate numerator)
		{
			Name:   "stats_count_errors_1h",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`app:"api-gateway" | unpack_json | status:>=400 | count()`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// count_uniq: cardinality of unique apps (equivalent of topk cardinality)
		{
			Name:   "count_uniq_apps",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" | count_uniq(app)`}, "start": {start1h}, "end": {end}, "step": {"5m"}},
		},
		// count_uniq: unique (app, region) combinations
		{
			Name:   "count_uniq_app_region",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" | count_uniq(app, region)`}, "start": {start1h}, "end": {end}, "step": {"5m"}},
		},
		// Multi-stage parse: JSON + multi-field filter (latency > threshold + error status)
		{
			Name: "json_multi_stage_filter",
			Path: "/select/logsql/query",
			Params: url.Values{
				"query": {`namespace:"prod" | unpack_json | status:>=400 | level:"error" | latency_ms:>100`},
				"start": {start15m}, "end": {end}, "limit": {"500"},
			},
		},
		// Logfmt parse + numeric filter + field extraction
		{
			Name: "logfmt_latency_filter",
			Path: "/select/logsql/query",
			Params: url.Values{
				"query": {`namespace:"prod" | unpack_logfmt | duration_ms:>1000 | level:"error"`},
				"start": {start30m}, "end": {end}, "limit": {"200"},
			},
		},
		// Regex + JSON multi-stage (most expensive parse chain)
		{
			Name: "regex_then_json_filter",
			Path: "/select/logsql/query",
			Params: url.Values{
				"query": {`namespace:"prod" ~"status.:(4|5)[0-9][0-9]" | unpack_json | latency_ms:>500`},
				"start": {start15m}, "end": {end}, "limit": {"200"},
			},
		},
		// hits with fine-grained step over long window (many buckets = more processing)
		{
			Name:   "hits_fine_grained_1h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}, "step": {"30s"}},
		},
		// stats_query with logfmt parse (logfmt + field filter + count)
		{
			Name: "stats_logfmt_errors",
			Path: "/select/logsql/stats_query_range",
			Params: url.Values{
				"query": {`app:"payment-service" | unpack_logfmt | level:"error" | count()`},
				"start": {start1h}, "end": {end}, "step": {"1m"},
			},
		},
		// Full JSON parse pipeline with multiple field filters
		{
			Name: "json_full_pipeline",
			Path: "/select/logsql/query",
			Params: url.Values{
				"query": {`app:"api-gateway" | unpack_json | status:>=200 | status:<500 | method:"POST" | latency_ms:>100`},
				"start": {start5m}, "end": {end}, "limit": {"500"},
			},
		},
		// hits: error logs at high frequency step (measures VL bucket computation)
		{
			Name: "hits_errors_30s_step",
			Path: "/select/logsql/hits",
			Params: url.Values{
				"query": {`namespace:"prod" | unpack_json | status:>=400`},
				"start": {start30m}, "end": {end}, "step": {"30s"},
			},
		},
		// Field names on filtered/parsed stream (complex metadata query)
		{
			Name: "field_names_json_filtered",
			Path: "/select/logsql/field_names",
			Params: url.Values{
				"query": {`app:"api-gateway" | unpack_json | status:>=400`},
				"start": {start1h}, "end": {end},
			},
		},
	}}
}

// VLUnindexedScan mirrors UnindexedScan using VictoriaLogs native LogsQL.
// The same content-search queries that force full chunk scans in Loki instead
// use VL's word-level inverted index — expected to be dramatically faster at scale.
func VLUnindexedScan(now time.Time) Workload {
	start1h := ns(now.Add(-1 * time.Hour))
	start6h := ns(now.Add(-6 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	end := ns(now)

	return Workload{Name: "unindexed_scan", Queries: []Query{
		// Token index lookup — VL has inverted index over all words.
		{
			Name:   "word_timeout_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" "timeout"`}, "start": {start1h}, "end": {end}, "limit": {"1000"}},
		},
		{
			Name:   "word_declined_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" "declined"`}, "start": {start1h}, "end": {end}, "limit": {"1000"}},
		},
		// Regex — VL narrows via block-level min/max filter before regex application.
		{
			Name:   "regex_user_id_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" ~"usr_[0-9]{5}"`}, "start": {start1h}, "end": {end}, "limit": {"500"}},
		},
		{
			Name:   "regex_status_codes_6h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" ~"status.:(4|5)[0-9][0-9]"`}, "start": {start6h}, "end": {end}, "limit": {"500"}},
		},
		// Negation — VL can skip blocks whose inverted index lacks the word.
		{
			Name:   "exclude_info_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" !"info"`}, "start": {start1h}, "end": {end}, "limit": {"500"}},
		},
		// Hits (time-series counts) on content-filtered stream.
		{
			Name:   "hits_error_content_1h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod" "error"`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		{
			Name:   "hits_timeout_content_1h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod" "timeout"`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// Long-window word search — VL uses inverted index across all 24h blocks.
		{
			Name:   "word_error_24h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" "error"`}, "start": {start24h}, "end": {end}, "limit": {"1000"}},
		},
		{
			Name:   "regex_5xx_24h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" ~"5[0-9][0-9]"`}, "start": {start24h}, "end": {end}, "limit": {"500"}},
		},
		// Stats count on content filter — equivalent of count_over_time.
		{
			Name:   "stats_count_errors_24h",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" "error" | count()`}, "start": {start24h}, "end": {end}, "step": {"1h"}},
		},
	}}
}

// VLHighCardinality mirrors HighCardinality using VictoriaLogs native LogsQL.
// VL's columnar model is not stream-count-sensitive — cardinality lives in the
// column index rather than in-memory stream tracker like Loki's ingester.
func VLHighCardinality(now time.Time) Workload {
	start5m := ns(now.Add(-5 * time.Minute))
	start1h := ns(now.Add(-1 * time.Hour))
	start24h := ns(now.Add(-24 * time.Hour))
	end := ns(now)

	return Workload{Name: "high_cardinality", Queries: []Query{
		// stream_ids — VL equivalent of Loki series.
		{
			Name:   "stream_ids_1h",
			Path:   "/select/logsql/stream_ids",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}},
		},
		{
			Name:   "stream_ids_24h",
			Path:   "/select/logsql/stream_ids",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}},
		},
		// Field values for pod — VL column index lookup.
		{
			Name:   "field_values_pod_1h",
			Path:   "/select/logsql/field_values",
			Params: url.Values{"field": {"pod"}, "query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}},
		},
		{
			Name:   "field_values_pod_24h",
			Path:   "/select/logsql/field_values",
			Params: url.Values{"field": {"pod"}, "query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}},
		},
		// Log select per pod — VL uses stream filter.
		{
			Name:   "log_by_pod_1h",
			Path:   "/select/logsql/query",
			Params: url.Values{"query": {`namespace:"prod" app:"api-gateway"`}, "start": {start1h}, "end": {end}, "limit": {"1000"}},
		},
		// Hits grouped — VL aggregates per bucket without materializing all stream IDs.
		{
			Name:   "hits_prod_1m_step",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}, "step": {"1m"}},
		},
		// count_uniq(pod) — count distinct pod values.
		{
			Name:   "count_uniq_pod_1h",
			Path:   "/select/logsql/stats_query_range",
			Params: url.Values{"query": {`namespace:"prod" | count_uniq(pod)`}, "start": {start1h}, "end": {end}, "step": {"5m"}},
		},
		// field_names — VL scans column headers, not all log lines.
		{
			Name:   "field_names_5m",
			Path:   "/select/logsql/field_names",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start5m}, "end": {end}},
		},
		{
			Name:   "field_names_1h",
			Path:   "/select/logsql/field_names",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start1h}, "end": {end}},
		},
		// Hits over 24h — tests VL performance at large retention + high cardinality.
		{
			Name:   "hits_prod_24h",
			Path:   "/select/logsql/hits",
			Params: url.Values{"query": {`namespace:"prod"`}, "start": {start24h}, "end": {end}, "step": {"1h"}},
		},
	}}
}

// VLAll returns all standard VL-native workloads for the given time reference.
func VLAll(now time.Time) []Workload {
	return []Workload{VLSmall(now), VLHeavy(now), VLLongRange(now), VLCompute(now)}
}

// VLAllEdgeCases returns VL-native edge-case workloads.
func VLAllEdgeCases(now time.Time) []Workload {
	return []Workload{VLUnindexedScan(now), VLHighCardinality(now)}
}

// VLByName returns the named VL-native workloads, including edge-case workloads.
func VLByName(names []string, now time.Time) []Workload {
	all := append(VLAll(now), VLAllEdgeCases(now)...)
	return AlignToStep(selectByName(all, names, VLAll(now)))
}
