---
sidebar_label: Cache troubleshooting
description: Diagnose Drilldown cache reuse and backend work in production.
---

# Diagnose slow Drilldown refreshes

Start with one reproducible request: keep the selector, tenant, forwarded auth,
client profile, step and evaluation phase fixed. Record the requested time
bounds and status. Compare a cold request, an identical repeat, then a window
moved by exactly one step. Compare successful answers against Loki where a
reference is available; an empty or partial response is not a successful speedup.

## Distinguish response hits, fragments and exclusions

For a slow Logs Drilldown page, check `/patterns` separately from `query_range`.
A shifted Grafana `now-6h` range uses new absolute bounds, so the exact pattern
response key misses even if the previous page load was warm. Settled aligned
pattern source windows can be reused through the Loki layer; the moving edge
and recent windows still read VL. Use
`loki_vl_proxy_internal_operation_total{operation="patterns_window_cache"}`
to distinguish `hit`, `miss`, `edge`, `recent_disabled` and `unreadable` before
attributing a long page load to the general query cache.

An exact response hit can return before evaluation-point lookups. A moved
window cannot use that exact response; eligible requests reuse older settled
points and evaluate missing ranges. The Loki-facing response/point cache and
the VictoriaLogs-facing backend cache have separate identities and counters.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_cache"}[5m])
)
```

Request outcomes are `hit`, `partial`, `miss`, `fallback` and `error`.
`stored` and `unreadable` describe background admission, not extra requests;
exclude them from request hit-rate denominators. `partial` means missing
evaluation ranges were recomputed, not that a partial answer was returned.
These counts do not include requests already served by an exact response cache.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_eligibility"}[5m])
)
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="query_steps_admission"}[5m])
)
```

Eligibility exclusions are `disabled`, `drilldown_profile`, `mixed_parsers`,
`shape`, `bounds` and `phase`. An exclusion is not a lookup miss. Admission
rejections are `canceled`, `error`, `oversize`, `headers` or `evaluator`.
`evaluator` means the route did not certify a complete per-step answer: native
stats/field routes and truncated Drilldown results can use other caches, but
must not be treated as complete retained points. These fixed labels are
exported through the existing Prometheus and OTLP internal-operation families.

For parser-dependent metrics, inspect
`loki_vl_proxy_parser_metric_evaluations_total{evaluator,reason}`. `raw_rows`
with `probe` means a parse-risk check required exact row evaluation;
`ineligible` means the pipeline cannot use stats pushdown. A moving raw query
with settled-point reuse should transfer only missing edges, while an exact
response hit should avoid evaluator/backend calls entirely. Request counts
alone cannot distinguish a one-minute read from a two-day scan.

## Explain an individual decision

Temporarily enable the existing debug log level for a controlled investigation.
The structured `query_steps_cache` event contains `outcome`, `total_steps`,
`missing_steps`, `missing_ranges` and `response_bytes`. Outcomes include
`miss`, `hit`, `partial`, `gap_not_mergeable` and `merge_rejected`. A
`merge_rejected` event means the whole-window route is used to preserve its
series ordering, limits, envelope or evaluator contract. It is not permission
to raise limits without measuring the cost.

`backend_body_read` records each actual decoded upstream body at caller
close, before response transformations or coalesced replay. Its existing
internal-operation counter/histogram use fixed outcomes `eof`, `closed`,
`error`, `canceled` and `timeout`. The structured event adds `backend.route`,
`decoded_bytes`, `read_span_seconds` when both requested bounds are known,
`elapsed_seconds` and `eof_observed`. Normal completions are debug events;
read/close failures and deadlines are warnings without raw error or query text.
`loki_vl_proxy_backend_read_bytes_total{kind}` exposes consumed decoded bytes
at body close in both Prometheus and OTLP. The fixed kinds are `raw`, `stats`,
`metadata` and `other`; these describe VL HTTP reads, not cache occupancy.
Use `sum by (kind) (rate(loki_vl_proxy_backend_read_bytes_total[5m]))` to
verify reduced transfer without enabling debug logs. In-flight bodies have
not yet contributed their closing snapshot.

```promql
sum by (outcome) (
  rate(loki_vl_proxy_internal_operation_total{operation="backend_body_read"}[5m])
)
histogram_quantile(0.95, sum by (le, outcome) (
  rate(loki_vl_proxy_internal_operation_duration_seconds_bucket{operation="backend_body_read"}[5m])
))
```

Compare byte rates, completion outcomes and duration with request volume.
Lower transfer with more `error`/`timeout` completions is not a successful
optimization. A stable backend request rate can still hide a large reduction
in the requested range or bytes; inspect these signals together.

The span describes requested backend bounds, including any lookback, rather
than rows actually scanned. Decoded bytes are consumed application bytes,
not compressed network traffic. Duration runs from HTTP start until caller
close and includes consumer processing/backpressure. `closed` only means
the consumer did not observe EOF; it does not imply a bad API answer. On a
concurrent abort, bytes are a snapshot at close: an interrupted read can
finish afterward. Existing HTTP/header observations and request-level error
metrics remain separate.

These events contain no query, selector, cache key, tenant or forwarded
credentials. Use normal request/trace correlation and the existing
`upstream_request` events to inspect backend latency/status. Expected cache
misses and exclusions remain debug diagnostics; failed backend requests retain
the existing error handling. Do not turn on raw-query logging to diagnose reuse.

## Measure cost by semantic layer and storage tier

Use the [layer metric definitions and caveats](observability.md#semantic-cache-layers)
alongside backend request/duration metrics and process CPU/RSS. Useful queries:

```promql
sum by (layer) (loki_vl_proxy_cache_layer_bytes{tier="l1_memory"})
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_read_bytes_total[5m]))
sum by (layer) (rate(loki_vl_proxy_cache_layer_disk_encoded_write_bytes_total[5m]))
sum by (layer, tier) (rate(loki_vl_proxy_cache_layer_evictions_total[5m]))
```

RAM occupancy is a logical cache charge, not process RSS. Encoded disk bytes
are not device IOPS, throughput or a physical bbolt file-size bound. Background
flush/replication may complete after the request. Keep priming, measured reads
and delayed writes separate; use filesystem/device telemetry for actual I/O.
Increasing a cache hit ratio without reducing backend ranges/bytes or latency
does not prove an improvement.

On Linux, process memory uses `/proc` RSS. When `/proc` is unavailable (including
macOS), the legacy `loki_vl_proxy_process_resident_memory_bytes` gauge falls
back to Go `MemStats.Sys`, despite its name. Use container/kernel memory
telemetry for RSS in that environment; do not relabel the fallback as RSS.

## Safety boundaries

Raw JSON `count_over_time` and `rate` can retain complete settled answers on
their original evaluation grid, including range equal to step. This includes
eligible direct grouped metrics with ordered JSON and logfmt stages. Their
fragments carry the evaluator's label-order provenance and a distinct order
version, so merging disjoint groups preserves the cold reply's series order.
Native Drilldown histograms and outer transforms retain their separate
admission rules. A hit on a Loki answer or settled point does not establish
reuse of raw VictoriaLogs input between different field queries. Large
local-only exact replies also do not establish SSD or peer reuse; eligible
shared fragments retain the existing tier budgets and placement rules.
The certified categorical mixed-parser count/rate route shares native VL
count buckets between the supported forward and reverse parser orders. Its
original-row complement is evaluated separately; a bucket hit does not mean
those raw rows were cached. After restart, a query can still need a stream-field
inventory read even when all metric buckets or settled points came from SSD.
Distinguish that metadata call from a query or stats scan when diagnosing reuse.
Any truncated source/gap, changed route or merged
series/byte/sample limit causes full evaluation rather than merging unsafe
points. New raw JSON step keys are separated from older binaries' step keys.

Points newer than `now - query-range-freshness` are recomputed. Reusing older
points preserves their original read age and extent TTL; it does not make late
backfills immediately visible after the settle horizon. This is not distributed
deletion invalidation. Check restart and peer-loss behavior separately from
ordinary warm requests; RF=1 and RF=2 have different recovery costs.

## Size the cache from its eviction signals

A cache that uses a small part of its budget and still evicts all the time is
usually limited by TTLs, not by size. The reason counters say which. All series
carry only `layer`, `tier` and `reason` (or `from`/`to`, `class`); see the
[metric definitions](observability.md#why-entries-leave-or-never-enter).

```promql
# Why entries leave L1 and L2.
sum by (tier, reason) (rate(loki_vl_proxy_cache_evictions_total[1h]))
# Why values never reach a tier.
sum by (tier, reason) (rate(loki_vl_proxy_cache_admission_rejections_total[1h]))
# How old an entry was, and how often it had been read, when it was evicted.
histogram_quantile(0.5, sum by (le, tier, reason) (rate(loki_vl_proxy_cache_entry_age_at_eviction_seconds_bucket[1h])))
sum by (tier, reason) (rate(loki_vl_proxy_cache_entry_hits_at_eviction_sum[1h]))
  / sum by (tier, reason) (rate(loki_vl_proxy_cache_entry_hits_at_eviction_count[1h]))
# The TTL each answer class is stored with.
loki_vl_proxy_cache_ttl_seconds
```

| What you see | What it means | What to change |
|---|---|---|
| `evictions{reason="capacity"}` is a large share and `entry_hits_at_eviction{reason="capacity"}` is above 0 | Entries that were being reused are pushed out for room | Raise `-cache-max-bytes` (or `-cache-max` when the entry count is the limit) for L1, `-disk-cache-max-bytes` or `-disk-cache-max-entries` for L2 |
| `evictions{reason="expired"}` dominates, the age at eviction sits at the class TTL and hits at eviction are above 0 | Reused answers expire before their next use | Compare with `cache_ttl_seconds{class}`; raise that class's flag where it has one (`-query-range-history-cache-ttl`, `-query-range-recent-cache-ttl`, `-query-range-extent-cache-ttl`, `-labels-cache-ttl`, `-parse-risk-probe-cache-ttl`). The Loki answer classes (`query_range`, `query`, `series`, `volume`, detected fields and labels) have fixed TTLs that keep answers fresh |
| `evictions{reason="expired"}` dominates with 0 hits at eviction | One-time answers: a bigger cache or a longer TTL would not be reused | Nothing |
| `admission_rejections{tier="l2_disk",reason="ttl_below_minimum"}` is high | Short-lived answers (TTL under `-disk-cache-min-ttl`, 30s by default; `volume` and `index_stats` answers live 10s) stay in memory only | Usually nothing: lowering `-disk-cache-min-ttl` writes answers to disk that expire before most rereads |
| `admission_rejections{reason="single_entry_budget"}` (L1) or `{reason="too_large"}` (L2) is high | Answers above a tenth of `-cache-max-bytes` (the L2 entry limit is the same tenth) are never cached | Raise `-cache-max-bytes` |
| `admission_rejections{tier="l2_disk",reason="queue_full"}` | The disk write queue is full between flushes | Raise `-disk-cache-max-buffer-bytes` or shorten `-disk-cache-flush-interval` |
| `admission_rejections{tier="l2_disk",reason="memory_only"}` or `not_replica` | Placement, not refusal: memory-only answers and non-owner shadow copies are kept off the disk by design | Nothing |
| `promotions{from="l2_disk"}` is low while L1 evicts on `expired` | A memory and a disk copy are written together with the same TTL, so they expire together; the disk answers after a restart, an L1 capacity eviction, or a promotion of a still-valid copy | Raise memory first; the disk does not extend a TTL |
| `cache_l2_read_seconds` p99 is in milliseconds, or `cache_l2_read_bytes` is mostly under 4 KiB | The disk is slow, or many small records are read per request | Check the device; see cache lookups per request below |

Cache lookups per client request are
`loki_vl_proxy_request_fanout{kind="cache_lookup"}` (by route). The split of those
lookups by tier is not measured. Requests that are never cached (the settled
steps cache's `canceled`, `error`, `oversize`, `headers` and `evaluator`
admission reasons above) are counted by the internal-operation metrics, not by
these families. Eviction histograms observe only expiry and capacity evictions;
an L2 record found by the start-up scan has no known admission time and is not
observed.


## Reuse-aware retention

The memory caches (L1 and Tier0) keep a protected share of their budgets for
entries read again since they were stored (`-cache-reuse-protection-percent`,
20 by default; 0 is plain LRU). A capacity eviction takes an entry stored but
never read again first, then the least recently used protected one; when the
protected share is full its least recently read entry goes back to the
unprotected part. `-cache-demote-on-evict` (off by default) also hands a
capacity-evicted entry that was read at least once to L2: L2 keeps its copy
newest in its recency order, or writes it with the TTL it has left.

Neither changes a TTL, a key or which answer is served, and neither acts while
the cache is below its budgets: an entry still leaves when its TTL ends, so
`evictions{reason="expired"}` and everything an expiry-bound cache answers are
the same with any setting. They matter when `evictions{reason="capacity"}` is
a real share of the evictions. On the production shape #1767 reported (budgets
far above the working set, evictions by expiry), they change nothing; raise
the budgets or the TTLs as the table above says.

Measured on deterministic traces replayed through a real memory cache over a
real disk cache (`TestRetentionTraceReplay` in `internal/cache`; seeded
traces, simulated clock, both tiers smaller than the distinct answers except
`ttl-bound`):

<!-- retention-trace-table:begin (TestRetentionTraceReplay; LVP_RETENTION_TABLE_UPDATE=1 rewrites it) -->
| Trace | Policy | Hit ratio | Byte hit ratio | L1 hit ratio | After the event: hit ratio | After the event: L1 hit ratio |
|---|---|---:|---:|---:|---:|---:|
| `drilldown-zipf` | `lru` | 67.80% | 69.21% | 54.46% | 68.77% | 55.27% |
| `drilldown-zipf` | `protect20` | 67.83% | 69.23% | 55.38% | 68.78% | 56.16% |
| `drilldown-zipf` | `protect50` | 67.93% | 69.34% | 56.63% | 68.93% | 57.16% |
| `drilldown-zipf` | `protect80` | 67.98% | 69.38% | 56.85% | 69.00% | 57.36% |
| `drilldown-zipf` | `demote` | 67.85% | 69.26% | 54.32% | 68.85% | 55.20% |
| `drilldown-zipf` | `protect20+demote` | 67.86% | 69.27% | 55.23% | 68.89% | 56.11% |
| `dashboard-refresh` | `lru` | 88.56% | 86.76% | 0.70% | 89.26% | 0.72% |
| `dashboard-refresh` | `protect20` | 88.56% | 86.76% | 0.84% | 89.26% | 0.85% |
| `dashboard-refresh` | `protect50` | 88.56% | 86.76% | 0.84% | 89.26% | 0.85% |
| `dashboard-refresh` | `protect80` | 88.56% | 86.76% | 0.84% | 89.26% | 0.85% |
| `dashboard-refresh` | `demote` | 88.56% | 86.76% | 0.70% | 89.26% | 0.72% |
| `dashboard-refresh` | `protect20+demote` | 88.56% | 86.76% | 0.84% | 89.26% | 0.85% |
| `one-scan-flood` | `lru` | 22.92% | 19.37% | 22.50% | 84.12% | 83.46% |
| `one-scan-flood` | `protect20` | 23.67% | 20.24% | 23.46% | 85.00% | 84.54% |
| `one-scan-flood` | `protect50` | 23.86% | 20.42% | 23.64% | 84.96% | 84.50% |
| `one-scan-flood` | `protect80` | 23.87% | 20.42% | 23.64% | 84.96% | 84.50% |
| `one-scan-flood` | `demote` | 23.13% | 19.60% | 22.50% | 84.33% | 83.46% |
| `one-scan-flood` | `protect20+demote` | 23.67% | 20.23% | 23.45% | 84.96% | 84.50% |
| `working-set-shift` | `lru` | 92.14% | 94.12% | 84.78% | 92.27% | 85.33% |
| `working-set-shift` | `protect20` | 92.18% | 94.14% | 84.94% | 92.30% | 85.45% |
| `working-set-shift` | `protect50` | 92.22% | 94.17% | 85.11% | 92.31% | 85.11% |
| `working-set-shift` | `protect80` | 92.26% | 94.20% | 83.53% | 92.31% | 81.94% |
| `working-set-shift` | `demote` | 92.10% | 94.08% | 84.78% | 92.04% | 85.33% |
| `working-set-shift` | `protect20+demote` | 92.13% | 94.11% | 84.94% | 92.04% | 85.45% |
| `ttl-bound` | `lru` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
| `ttl-bound` | `protect20` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
| `ttl-bound` | `protect50` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
| `ttl-bound` | `protect80` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
| `ttl-bound` | `demote` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
| `ttl-bound` | `protect20+demote` | 38.22% | 45.30% | 38.22% | 38.78% | 38.78% |
<!-- retention-trace-table:end -->

- `drilldown-zipf`: Logs Drilldown service pages picked by a Zipf law, eight
  panels per visit; L1 10% and L2 25% of the distinct bytes.
- `dashboard-refresh`: twelve dashboards of ten panels refreshed every minute,
  each panel reading twelve 5m buckets (settled buckets 2h, the open one 15s),
  beside Explore queries; L1 15%, L2 35%.
- `one-scan-flood`: a hot set read twice a second and, from 30m to 40m, 18000
  answers read once; L1 3%, L2 6%. "After the event" is the 20 minutes after
  the flood.
- `working-set-shift`: one set of 800 queries for 40 minutes, then another;
  L1 25%, L2 45%. "After the event" is the 40 minutes after the shift.
- `ttl-bound`: budgets four times the distinct bytes, 60s TTL: every eviction
  is an expiry, and every policy gives the same answers and hits.

The default share (20) is the largest of those measured that is not below
plain LRU on any trace and ratio; larger shares protect more against a flood
but hold the old set longer after a shift. Demotion is off by default: it
raised the hit ratio of the Zipf and flood traces a little, and lowered it
after the shift, where it keeps the old set on disk. Count demotions with
`loki_vl_proxy_cache_demotions_total{result}`.
