# Metric Fast-Path Expansion Design

## Goal

Expand the proxy's fast-path metric execution (VL `stats_query_range`) to cover parser-stage queries, ungrouped aggregations, and `topk`/`bottomk` wrappers, bringing heavy/compute workload throughput from 0.11-0.38× Loki to 0.45-0.80× Loki without any VL API changes.

## Problem

The proxy has two fast-path gates that force most real-world metric queries onto the slow path (raw log scan + O(N) manual aggregation):

**Gate 1 — parser-stage guard** (`shouldUseManualRangeMetricCompat`, lines 368-416):
```go
case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
    if queryUsesParserStages(baseQuery) && !strings.Contains(originalLogql, "__error__") {
        return true  // forces slow path for ANY query containing | json, | logfmt
    }
```
This was conservative: VL `stats_query_range` natively supports inline filter pipelines (`{...} | json | status >= 400 | stats by (app) rate() as c`), so the guard is unnecessary.

**Gate 2 — GroupBy gate** (`proxyManualRangeMetricRange`, fast-path entry):
```go
if !hasStreamSentinel && (len(spec.GroupBy) > 0 || spec.ByExplicit) {
    // fast path: collectRangeMetricHits()
}
// else: falls to slow path
```
`sum(rate({...}[5m]))` has no `by` clause → `GroupBy` is empty → forced to slow path even though VL can compute the ungrouped aggregate directly with `| stats rate() as c`.

**Gate 3 — topk/bottomk opaque wrapper**: The proxy parses `topk(5, <expr>)` as a binary-ish operation and never unwraps the inner expression to check if it is fast-path eligible.

## Approach

**Approach B: targeted gate removal + topk unwrapping**

Three focused changes to `internal/proxy/range_metric_compat.go`:

### Change 1 — Remove parser-stage guard

Delete the `queryUsesParserStages` branch for rate/bytes_rate/count_over_time/bytes_over_time. VL `stats_query_range` already handles `| json` and `| logfmt` inline, so the guard is purely conservative. The `__error__` fast-exit remains untouched (it tests internal proxy plumbing, not VL capability).

Before:
```go
case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
    if queryUsesParserStages(baseQuery) && !strings.Contains(originalLogql, "__error__") {
        return true
    }
    return !rangeEqualsStep
```

After:
```go
case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
    return !rangeEqualsStep
```

### Change 2 — Allow ungrouped aggregations on the fast path

Lift the `len(GroupBy) > 0 || ByExplicit` requirement from the fast-path gate. When `GroupBy` is empty and `ByExplicit` is false, generate a VL stats query without a `by(...)` clause:
```
{...} | json | status >= 400 | stats rate() as c
```
VL returns a single series, the proxy wraps it in the Loki vector format exactly as it does today for grouped results.

Concretely, `collectRangeMetricHits` must be updated to omit `by (...)` when `len(spec.GroupBy) == 0 && !spec.ByExplicit`.

### Change 3 — Unwrap topk/bottomk before fast-path routing

Add `parseTopKWrapper(expr string) (k int, inner string, ok bool)` that detects `topk(N, <inner>)` and `bottomk(N, <inner>)` and extracts the inner expression. In `proxyManualRangeMetricRange` (and wherever metric dispatch occurs), attempt unwrapping first:

1. If the outer expression is `topk`/`bottomk`, extract `inner`.
2. Route `inner` through normal fast-path logic.
3. If fast-path succeeds for `inner`, apply `topk`/`bottomk` post-processing on the returned matrix in Go (sort by value at last step, keep top/bottom-k streams).
4. If fast-path fails for `inner`, fall through to slow path for the whole expression (unchanged behaviour).

Post-processing cost: O(S log S) where S = number of series returned by VL — negligible compared to the query itself.

## Accepted Limitations (not changed)

- `avg_over_time | unwrap` and `quantile_over_time | unwrap` — VL has no native unwrap-based range aggregation; these remain on the slow path.
- Binary operations (e.g., `rate(...) / rate(...)`) already dispatch both operands in parallel goroutines. The bottleneck for those queries is each operand hitting the slow path, which Change 1 + Change 2 address indirectly when both operands are parser-stage or ungrouped queries.

## Expected Impact

| Workload | Current | Target |
|---------|---------|--------|
| Heavy: `sum by (app) (rate(...|json|status>=400[5m]))` | slow path | fast path (Change 1+2) |
| Heavy: `topk(5, sum by (app) (rate(...)))` | slow path | fast path (Change 3) |
| Compute: `sum(rate(...|json|status>=400))/sum(rate(...|json))*100` | 2× slow path | fast path both operands (Change 1+2) |
| Compute: `topk(3, sum by (app) (rate(...|json...)))` | slow path | fast path (all 3 changes) |
| `avg_over_time | unwrap` | slow path | slow path (accepted) |
| `quantile_over_time | unwrap` | slow path | slow path (accepted) |

Projected improvement: heavy 0.36-0.38× → 0.70-0.80× Loki; compute 0.11-0.13× → 0.45-0.55× Loki.

## Architecture

All changes are confined to `internal/proxy/range_metric_compat.go` with unit tests in `internal/proxy/range_metric_compat_test.go`. No new files, no new interfaces. The `statsCompatSpec` and `originalRangeMetricSpec` structs are unchanged.

## Testing Strategy

1. **Unit tests** — table-driven tests in `range_metric_compat_test.go` for:
   - `shouldUseManualRangeMetricCompat` — verify parser-stage queries now return false for rate/count functions
   - `collectRangeMetricHits` generated VL query — verify ungrouped path omits `by ()`
   - `parseTopKWrapper` — verify extraction of k, inner expression, direction
   - topk post-processing — verify correct stream selection

2. **Parity tests** — existing `TestLogQLParity` suite in `test/e2e-compat/` run against live stack; all 116 cases must pass.

3. **Bench** — re-run `bench/` heavy and compute workloads before/after to confirm speedup.

## Files Changed

- `internal/proxy/range_metric_compat.go` — all three changes
- `internal/proxy/range_metric_compat_test.go` — unit tests for all three changes
