# Complete Fields results and cache policy isolation

The reported 15-minute Fields request returned 1000 partial `batch_id` frames
through the playground's patterns datasource and 3157 complete frames through
Loki. The running patterns proxy had an old 1000-series override. Compose on
main already selected 1,000,000, matching the reference Loki.

There was also a response-cache defect: effective `max_query_series` was absent
from the Loki response identity. Increasing a global or tenant limit could
replay an older partial answer; decreasing it could replay an unrestricted
answer. The response keys now include the resolved limit, including federated
tenant minima. VL buckets and metadata retain their existing identities.

## Correctness: main versus change

Base product: `eef311db6845170474d6d200d818a58e811edc59`. Candidate product
files and the separate playground build are identified in
[provenance.json](provenance.json).

| Contract | Main | Change | Evidence |
|---|---|---|---|
| Raise global/tenant limit, same request | Replays partial answer | Complete answer under the raised limit | `TestOrderedJSONResultCacheSeparatesSeriesLimits`; retained failing-before-fix log |
| Lower global/tenant limit, same request | Replays unrestricted answer | Correct partial answer and warning | Same regression, both transition directions |
| Plain API limit error, then raise/lower | Response identity omits limit | Held 400 and complete 200 use their own limits | Extended `TestErrorFlights_DataFailureHeldForDataTTL` |
| SSD reopen with changed limit | Identity omits limit | Old-policy entry is not served; remains reusable under its original policy | Extended `TestResponseCache_ReadRoutesUseDiskAfterRestart` |
| Replica/federated identity | Effective limit omitted | Same effective limits share; different effective limits separate | `TestResponseCache_ReplicasResolveSeriesLimitIdentity` |
| VL bucket identity and metadata | Reusable | Still reusable across series-policy changes | Same identity regression, including the actual stats-extent key |
| Unchanged-limit exact repeat | Reusable | Reusable, zero request-attributed VL calls in focused regressions | Cache/flight tests above |
| Existing plan routes and backend reads | Recorded baseline | 2590 rows, 25,900 executions, zero route mismatches; recorded route/read digests unchanged | `TestPlanRouteParity` |

These identity checks cover peer-compatible keys; they do not run peer transport
or claim a new fleet throughput result. Shifted-window and near-now refresh
regressions run in the existing cache suite. New fleet-loss, pressure, physical
IOPS and device-throughput measurements are outside this change.

## Real browser and Loki

The identical saved 15-minute Grafana request, with a 2-second range window and
step, returned **3157 frames and 3157 points from both proxy and Loki**, with no
notices. Labels, timestamps and values normalize to the same digest; an exact
proxy repeat gives the same answer. See [replay-summary.json](replay-summary.json)
and the compressed [proxy](replayed-proxy.json.gz) and
[Loki](replayed-loki.json.gz) request/response pairs.

Extending the same Grafana request to **3 hours with a 20-second window/step**
returned **37,830 series and 37,870 points** from each side, with the same
normalized answer and no notices. Individual observations were 1.398s for
proxy, 6.377s for Loki and 0.734s for a proxy repeat. These are single requests
with different cache histories, not a controlled cold comparison or p50/p95.
See [three-hour-summary.json](three-hour-summary.json) and its compressed
request/response pairs. All 123 existing live matrix contracts also passed
against the same Loki/VL stack; the retained matrix log names every case.

The original browser capture shows that **Show all 1000 made no new request**:
it expands received frames, rather than fetching omitted server results. See
[browser-before.json](browser-before.json) and
[the original screen](browser-before.png).

The existing `drilldown-field-bounds.spec.ts` suite, selected by the existing
`@drilldown-core` CI shard, passed six scenarios:

| Mode | Ranges | Frames per field | Identity/value assertions |
|---|---|---:|---|
| Deliberately bounded vmauth | 24h, 48h | 1000 | Populated observations and visible partial-result warnings |
| Matching patterns/Loki limit | 15m, 3h, 24h, 48h | 1010 | Exact distinct fixture identities for `batch_id`, `trace_id`, `total`; finite positive observations; no partial notices |

See [browser-summary.json](browser-summary.json) and
[all captured identities](browser-identities.json.gz). The fixture check is
about complete identities and populated observations; the separate real-Loki
replay above compares exact values. The existing typed JSON/range-function/
aggregation path links the focused cache regressions through the same registry.

The playground retains PR #1705 plus this cache patch. All 13 proxy variants
were recreated in the existing `e2e-compat` project; backend/Grafana container
IDs and all cache volumes were preserved. Only the old patterns series limit
changed. The built-in product limit and deliberate vmauth limit remain 1000.
This was a single patterns-proxy measurement, not a multi-peer fleet benchmark.

## Performance and resource scope

Apple M5 Pro, arm64, Go 1.27.4, `GOMAXPROCS=2`, `GOMEMLIMIT=2GiB`. Eight samples
per side, 300ms per sample, interleaved main/change/change/main, with the same
committed benchmark file on each product version. The fixture has 240 rows
and populated metric answers. Cache-hit benchmarks below are handler costs,
not whole-proxy RSS, CPU utilisation or Grafana render time.

| Path | Main | Change | Result |
|---|---:|---:|---|
| Native metric, cold | 90.79 µs | 90.30 µs | No significant timing change |
| JSON metric, cold | 157.1 µs | 158.0 µs | No significant timing change |
| Drilldown JSON, cold | 160.1 µs | 158.9 µs | No significant timing change |
| Drilldown JSON, repeat | 7.009 µs | 6.871 µs | 1.96% lower, p=0.047; a small microbenchmark gain |
| Primary query-range cache hit | 1.422 µs, 496 B, 11 allocations | 1.410 µs, 464 B, 10 allocations | Timing stable; one temporary allocation removed |
| Compat query-range cache hit | 4.095 µs, 4448 B, 54 allocations | 4.129 µs, 4448 B, 53 allocations | Timing stable; one temporary allocation removed |

All eight measured paths have stable or lower timing and fewer allocations.
The builder appends the profile to its final key instead of allocating an
intermediate profile string. Full samples and statistical comparisons are in
`bench-main.txt`, `bench-pr.txt` and `benchstat.txt` beside this file.

Query keys retain an extra `/ms-<limit>`: 8 bytes at 1000, 11 at 1,000,000.
`cache.entrySize` charges key bytes as well as payload bytes. Old/new policy
entries can coexist until expiry/eviction within the same RAM/SSD budgets.
Each previously cached query misses its new response identity once after
deployment; reusable VL bucket data is not invalidated. SSD compression, batching, expiry, replica
placement and admission limits do not change.

Whole-process CPU/RSS, simultaneous fleet peaks, SSD occupancy deltas,
physical IOPS/throughput, peer-loss latency and sustained Grafana rendering
cost were not measured here. The individual real-API timings are retained as
observations, not p50/p95 claims or proof that every warm query beats Loki.

Reproduce the request-path comparison on both product versions with the same
`internal/proxy/ordered_json_result_cache_bench_test.go`:

```sh
GOMAXPROCS=2 GOMEMLIMIT=2GiB go test -run '^$' -p 1 -benchmem \
  -bench '^(BenchmarkProxy_QueryRange_CacheHit|BenchmarkProxy_QueryRange_CompatCacheHit_DelayedBackend|BenchmarkOrderedJSONResultCache)$' \
  -benchtime=300ms -count=8 ./internal/proxy
```

## Verification limits

The full unsharded local package run reached Go's default 10-minute package
timeout while running `TestStatsJSONSeriesLargeAnswerMatchesReference`. It is
not a passing check. Final local validation uses the complete focused
cache/limits/flight suite, its race checks, and the full route-parity gate;
the ordinary CI shards run the full package suite. No timeout, threshold,
expected error, recorded route or backend-call baseline was weakened.
