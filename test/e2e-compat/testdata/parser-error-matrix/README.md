# Nested JSON metric regression evidence

The retained matrix contains 132 query cases, each sent to pinned Loki 3.7.9
and the proxy against equivalent, separately isolated five-row fixtures in
VictoriaLogs 1.52.0. Each response retains its exact query, time axis, HTTP status
and complete metric result or error text. Loki's timing-dependent execution stats
are omitted. The manifest identifies runtime/harness commits, backend image IDs,
fixtures, successful assertions, failing cases and compressed-file checksums.

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

No performance or fleet-cache gain is claimed here. Nested expressions retain the
ordered evaluator's row, body and sample bounds; their outer result receives the
tenant's series limit. Inner native-handler rewrites are skipped on the new nested
route to retain correct parser behavior and final request accounting. Aggregate-all
nests can consequently read raw rows where the previous native path counted them;
that cost needs separate workload measurements. Logfmt, unwrap functions, binary
joins, offsets, unsupported vector operators and broader high-cardinality workloads
are outside this bounded matrix.
