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

### Sizing procedure

Turn on `-cache-sizing-signals` on one replica (about 50 ns per lookup), let it
see a day of traffic, then read the signals in this order:

```promql
# 1. Is the cache bound by size or by TTL? (from the eviction reasons above)
sum by (tier, reason) (rate(loki_vl_proxy_cache_evictions_total[1h]))
# 2. What would more room, or longer TTLs, have answered? Compare with the misses.
sum by (tier, scenario) (rate(loki_vl_proxy_cache_ghost_hits_total[1h]))
sum by (tier) (rate(loki_vl_proxy_cache_tier_misses_total{tier=~"l1_memory|l2_disk"}[1h]))
# 3. How large is what users ask for?
max by (window) (loki_vl_proxy_cache_working_set_bytes)
max by (window) (loki_vl_proxy_cache_working_set_keys)
# 4. How long until a key is asked again? Compare with loki_vl_proxy_cache_ttl_seconds.
histogram_quantile(0.5, sum by (le) (rate(loki_vl_proxy_cache_reuse_interval_seconds_bucket[1h])))
# 5. Which answer classes are served from the cache, by bytes?
sum by (class) (rate(loki_vl_proxy_cache_hit_bytes_total[1h]))
  / sum by (class) (rate(loki_vl_proxy_cache_requested_bytes_total[1h]))
```

| What you see | What to change |
|---|---|
| `ghost_hits{tier="l1_memory",scenario="2x_bytes"}` is a large share of the L1 misses | Raise `-cache-max-bytes` (and `-cache-max` when `working_set_keys` is above it); up to `working_set_bytes{window="1h"}` is where more memory still helps |
| `ghost_hits{tier="l2_disk",scenario="2x_bytes"}` is a large share of the L2 misses | Raise `-disk-cache-max-bytes` or `-disk-cache-max-entries`, toward `working_set_bytes{window="24h"}` |
| `ghost_hits{scenario="2x_ttl"}` is high and `2x_bytes` low, with the reuse interval just above the class TTL | The cache is TTL-bound: raise the TTL of that class where it has a flag (see the table above); the Loki answer classes keep fixed TTLs that keep answers fresh, and a longer TTL serves staler answers |
| Both ghosts are low | Neither more room nor longer TTLs would help: the misses are first asks or answers that are never reused |
| `working_set_bytes{window="5m"}` is above `-cache-max-bytes` | Even the last five minutes do not fit in memory: raise it, or rely on L2 and the peers |
| The byte hit ratio of a class is far below the request hit ratio (`cache_tier_hits_total / cache_tier_requests_total`) | Large answers of that class miss while small ones hit: check `admission_rejections{reason="single_entry_budget"}` and raise `-cache-max-bytes` |
| `sizing_dropped_events_total` above 0 | The sampled figures are a lower bound for that period |

The ghost caches are LRU simulations of a 1-in-16 sample of the keys with the
tier's budgets doubled (or its TTLs doubled), fed by the same lookups and
stores; with `-cache-reuse-protection-percent` the real memory tier is not
plain LRU, so its ghosts approximate it.

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
off by default; 20 recommended where it helps, below). A capacity eviction takes an entry stored but
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

[//]: # 'retention-trace-table:begin (TestRetentionTraceReplay; LVP_RETENTION_TABLE_UPDATE=1 rewrites it)'
| Trace | Policy | Hit ratio | Byte hit ratio | L1 hit ratio | After the event: hit ratio | After the event: L1 hit ratio |
|---|---|---:|---:|---:|---:|---:|
| `drilldown-zipf` | `lru` | 63.59% | 63.23% | 48.66% | 64.11% | 48.06% |
| `drilldown-zipf` | `protect20` | 63.82% | 63.45% | 50.01% | 64.44% | 49.73% |
| `drilldown-zipf` | `protect50` | 64.10% | 63.91% | 51.37% | 64.82% | 50.73% |
| `drilldown-zipf` | `protect80` | 64.32% | 64.13% | 51.66% | 64.98% | 50.57% |
| `drilldown-zipf` | `demote` | 63.93% | 63.59% | 48.51% | 64.58% | 48.01% |
| `drilldown-zipf` | `protect20+demote` | 63.95% | 63.61% | 49.61% | 64.54% | 49.10% |
| `dashboard-refresh` | `lru` | 88.41% | 85.83% | 0.59% | 89.10% | 0.62% |
| `dashboard-refresh` | `protect20` | 88.41% | 85.83% | 0.69% | 89.10% | 0.72% |
| `dashboard-refresh` | `protect50` | 88.41% | 85.83% | 0.69% | 89.10% | 0.72% |
| `dashboard-refresh` | `protect80` | 88.41% | 85.83% | 0.69% | 89.10% | 0.72% |
| `dashboard-refresh` | `demote` | 88.41% | 85.83% | 0.59% | 89.10% | 0.62% |
| `dashboard-refresh` | `protect20+demote` | 88.41% | 85.83% | 0.69% | 89.10% | 0.72% |
| `one-scan-flood` | `lru` | 22.62% | 22.32% | 21.83% | 83.71% | 82.25% |
| `one-scan-flood` | `protect20` | 23.32% | 23.16% | 22.72% | 85.00% | 83.67% |
| `one-scan-flood` | `protect50` | 23.56% | 23.48% | 22.93% | 85.00% | 83.83% |
| `one-scan-flood` | `protect80` | 23.66% | 23.61% | 22.98% | 84.83% | 83.54% |
| `one-scan-flood` | `demote` | 22.84% | 22.54% | 21.83% | 83.79% | 82.25% |
| `one-scan-flood` | `protect20+demote` | 23.35% | 23.20% | 22.71% | 84.96% | 83.62% |
| `working-set-shift` | `lru` | 91.83% | 91.16% | 82.88% | 91.98% | 83.20% |
| `working-set-shift` | `protect20` | 91.86% | 91.20% | 83.06% | 91.97% | 83.40% |
| `working-set-shift` | `protect50` | 92.01% | 91.40% | 83.30% | 92.00% | 83.28% |
| `working-set-shift` | `protect80` | 92.06% | 91.46% | 81.97% | 92.03% | 80.00% |
| `working-set-shift` | `demote` | 91.80% | 91.19% | 82.88% | 91.65% | 83.20% |
| `working-set-shift` | `protect20+demote` | 91.81% | 91.20% | 83.06% | 91.62% | 83.40% |
| `working-set-shift-wide` | `lru` | 85.82% | 81.67% | 76.97% | 85.81% | 77.19% |
| `working-set-shift-wide` | `protect20` | 85.99% | 81.93% | 77.61% | 86.05% | 77.79% |
| `working-set-shift-wide` | `protect50` | 86.73% | 82.82% | 78.56% | 86.68% | 78.41% |
| `working-set-shift-wide` | `protect80` | 87.78% | 84.01% | 77.28% | 87.48% | 74.68% |
| `working-set-shift-wide` | `demote` | 86.55% | 82.41% | 76.97% | 86.48% | 77.19% |
| `working-set-shift-wide` | `protect20+demote` | 86.72% | 82.58% | 77.61% | 86.64% | 77.79% |
| `ttl-bound` | `lru` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |
| `ttl-bound` | `protect20` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |
| `ttl-bound` | `protect50` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |
| `ttl-bound` | `protect80` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |
| `ttl-bound` | `demote` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |
| `ttl-bound` | `protect20+demote` | 29.11% | 21.13% | 29.11% | 29.44% | 29.44% |

Over 5 seeds of every trace, `protect20`'s largest loss against `lru` on any of these ratios is 0.10 points (seed 3 working-set-shift window memory hit ratio).
[//]: # 'retention-trace-table:end'

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
  `working-set-shift-wide`: the same with 2400 queries per set, eight lookups
  a second, L1 15%, L2 30%.
- `ttl-bound`: budgets four times the distinct bytes, 60s TTL: every eviction
  is an expiry, and every policy gives the same answers and hits.

Both are off by default. A protected share raised the hit ratio after the
flood on every seed, and the memory hit ratio on the Zipf traces by about a
point, but no share was at or above plain LRU on every ratio of every trace
and seed (the line under the table gives `protect20`'s largest loss); larger
shares protect more against a flood but hold the old set longer after a
shift. Turn it on (20) where `evictions{reason="capacity"}` is a real share and
`entry_hits_at_eviction{reason="capacity"}` is above 0, or where one-time
floods push out what users read again. Demotion raised the hit ratio of the
Zipf and flood traces a little and lowered it after the shift, where it keeps
the old set on disk. Count demotions with
`loki_vl_proxy_cache_demotions_total{result}`.
