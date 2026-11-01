# Nested JSON metric regression evidence

The retained matrix contains 132 query cases, each sent to pinned Loki 3.7.9
and the proxy against equivalent, separately isolated five-row fixtures in
VictoriaLogs 1.52.0. Each response retains its exact query, time axis, HTTP status
and complete metric result or error text. Loki's timing-dependent execution stats
are omitted. The manifest identifies runtime/harness commits, backend image IDs,
fixtures, successful assertions, failing cases and compressed-file checksums.

`2026-11-01-exact-fence` is the latest capture, after integration with the
settled-window changes and targeted nested-response cache fencing: measured main
`3de2a9b`, candidate `4ba6146`. The table below describes this capture.
`2026-11-01-integrated-953` preserves the preceding integrated capture at candidate
`06b13d2`; `2026-11-01` preserves the earlier pre-integration capture. Their actual
runtime and harness identities are retained as historical evidence, without
relabelling them as newer sources. All three captures produce the same targeted
pass/fail counts.

| Targeted contract | Main | Candidate |
|---|---:|---:|
| Loki reference meets independently specified expectations | 132/132 | 132/132 |
| Proxy meets the same expectations | 68/132 | 132/132 |
| Proxy differences | 64 | 0 |

This is targeted parser/nesting evidence, not an overall API compatibility score.
The 132 cases comprise 15 vector shapes × 2 range functions (`rate`,
`count_over_time`) × 2 error-filter states × 2 API modes, plus 12 cases isolating
valid JSON, truncated objects and plain text. Shapes cover direct grouped sums,
outer min/max/avg/count/topk/bottomk, `without`, three vector levels, aggregate-all
parser hints and invalid ranking parameters. Expected successful values, labels,
timestamps and point counts are asserted independently on both servers. Runtime
errors compare their category; their first failing row and error envelope can differ.

Verify the retained records without running services:

```sh
python3 test/e2e-compat/testdata/parser-error-matrix/verify.py
```

Run the live regression against the regular compatibility stack:

```sh
go test -race -tags=e2e ./test/e2e-compat -count=1 -timeout=5m -v \
  -run '^(TestChaining_ParserErrorNestedAggregations|TestHardeningLive_ParserError.*)$'
```

`TestChaining_ParserErrorNestedAggregations` is selected by the existing core
compatibility shard. The four existing `TestHardeningLive_ParserError*` suites
remain selected by the runtime security regression job. The new unit regressions
also check final series-limit behavior, request counters/durations, query tracking
and the nested Drilldown histogram route. CI logs emit structured `matrix_response`
records for refreshing this evidence after validating against the pinned reference.

The matrix declares the existing `semantics/pipeline-error-text-parity`,
`semantics/nested-aggregation-outer-by-native-routes`,
`semantics/non-sum-vector-aggregation-unreduced` and
`semantics/json-error-filter-metric-split-pushdown` contracts. It adds no duplicate
registry gap. Positive parsed-field filters without an error-label filter belong
to separate coverage and are not claimed by these 132 cases.

No performance or fleet-cache gain is claimed here. Nested expressions retain the
ordered evaluator's row, body and sample bounds; their outer result receives the
tenant's series limit. Inner native-handler rewrites are skipped on the new nested
route to retain correct parser behavior and final request accounting. Aggregate-all
nests can consequently read raw rows where the previous native path counted them;
that cost needs separate workload measurements. These nested expressions bypass
generic settled-step fragments, including entries produced before the parser fix;
the direct JSON point-cache compiler remains unchanged. Exact nested query and
query_range replies use a targeted version segment, including Tier0, uniform
offsets and parent expressions that evaluate a nested operand, so old
peer-prefetched L1 copies cannot bypass the corrected evaluator. Unit tests seed
the earlier exact key, check the corrected answer, tenant/auth isolation and reuse
of the new reply where the existing admission policy permits it. Unaffected
response key identities, admission and TTL policies remain unchanged.
Moving-window, disk-restart and peer-loss cache
costs for the nested route are not measured by this parity matrix.
Logfmt, unwrap functions, binary
joins, offsets, unsupported vector operators and broader high-cardinality workloads
are outside this bounded matrix.
