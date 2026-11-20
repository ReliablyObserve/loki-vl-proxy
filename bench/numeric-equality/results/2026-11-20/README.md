# Numeric equality proof — 2026-11-20

The existing Loki pipeline oracle, VictoriaLogs store oracles and live API tests
share the numeric-equality fixtures. These are bounded contracts.

| Current check | Result |
|---|---:|
| Storage grid, VL v1.49.0/v1.54.0 | 81 queries, 7,286 row decisions |
| Plan differences from Loki | 126: present-empty 66, overflow 28, sanitized/extracted keys 32 |
| Frozen translator differences | 2,244 |
| Error rows restored against earlier PR text | 1,160; no formerly correct rows removed |
| Recorded pipeline replay | 23 cases; two explicit adjacent-filter gaps |
| Live numeric API comparisons | 298: 292 Loki-equivalent, six exact accepted identity projections |
| Existing semantic matrix | 126 live case subtests |

`candidate-e2e-final.txt.gz` records the successful live run on Loki 3.7.10 and
VL 1.54.0: log entries, timestamps, metadata, sample values/labels and complete
pipeline-error bodies. The shared wrappers cover count, rate, sum, grouped sum,
topk, sort, scalar multiplication, set union, label_replace, offsets and both
orders of binary operands. Source error/preserve keys, generated-error filters
and gaps between evaluation windows are checked too. When two operands fail
with different parser hints, either complete Loki operand error is valid.

The six exact default projections belong to the existing
`parsed-identity/dynamic-key-parsers` policy: topk, sort and label_replace
keep stream identity on these stats routes. They are not counted as exact Loki
matches. The change preserves existing routes instead of forcing all clean
queries through a new raw-row fallback. Empty/overflow/key-collision, adjacent
error-filter and broader operator/stage combinations remain registered gaps.

All 13 proxies were rebuilt in the existing e2e-compat project, preserving other
container IDs and named cache volumes. The image is marked as an uncommitted
candidate; verification.json identifies the tested source hashes.

## VictoriaLogs filter cost

`TestNumericEqualityVictoriaLogsCost`: 600,000 deterministic JSON rows over 24h,
nine alternating samples per query/window. Control: the current error-retention
implementation with the unfactored regexps from 8dfb84. Candidate: factored
regexps. Every timed request must return an identical count vector.
This is neither main-versus-PR nor proxy-versus-Loki latency. Times include the
serve oracle's local HTTP round trip.

| Backend | Filter/window | Control p50 | Candidate p50 | Delta |
|---|---|---:|---:|---:|
| v1.54.0 | size=1KB / 1h | 21.900 ms | 16.043 ms | -26.7% |
| v1.54.0 | size=1KB / 6h | 120.614 ms | 84.703 ms | -29.8% |
| v1.54.0 | size=1KB / 24h | 284.689 ms | 200.670 ms | -29.5% |
| v1.49.0 | size=1KB / 1h | 23.132 ms | 16.451 ms | -28.9% |
| v1.49.0 | size=1KB / 6h | 118.791 ms | 84.139 ms | -29.2% |
| v1.49.0 | size=1KB / 24h | 475.354 ms | 328.326 ms | -30.9% |
| v1.54.0 | d=5s / 24h | 230.463 ms | 223.604 ms | -3.0% |
| v1.49.0 | d=5s / 24h | 374.778 ms | 361.869 ms | -3.4% |
| v1.54.0 | number=200 / 24h | 186.682 ms | 180.451 ms | -3.3% |
| v1.49.0 | number=200 / 24h | 180.105 ms | 184.819 ms | +2.6% |

`vl-cost-correctness-control.txt` retains all results, including an unchanged
number-inequality control at 6h on v1.54.0: 55.404 → 64.573 ms (+16.6%) despite
identical query text. This is variability, not evidence of a code regression.
Small number/duration changes are not claimed significant. The separate bare
range pair returns different counts (24h size: 300,001 vs 450,004); its latency
ratio compares different work. CPU time, peak RSS, disk IOPS, network bytes and
end-to-end main/Loki resource costs were not collected by this filter benchmark.

Reproduce:
```sh
GOMEMLIMIT=2GiB GOMAXPROCS=2 LVP_NUMEQ_VLCOST=1 \
  LVP_NUMEQ_BASELINE_FILE=bench/numeric-equality/results/2026-11-20/correctness-control-queries.json \
  go test -count=1 -p 1 -timeout=20m -v \
  -run '^TestNumericEqualityVictoriaLogsCost$' ./internal/querycorpus
```

## Earlier discovery evidence

`vl-cost.txt`, `baseline-queries.json`, `candidate-e2e.txt.gz` and
`live-grid.jsonl.gz` predate the production error-retention fix. The earlier
candidate failed all 63 selected API comparisons and lost 1,286 storage
decisions. These remain red evidence. Its regex-only size gain was 20–24%.

That earlier live grid dual-wrote 357 lines and replayed 81 queries. Loki HTTP
matched its direct pipeline oracle on 77/81; VL HTTP matched the store oracle on
81/81. Four 1.5MiB cases differ because printing and reparsing Loki's public AST
changes 1,572,864 bytes into 1.6MB (1,600,000 bytes). Original, printed and
reparsed results remain separate. This is consistent with the HTTP observation
without claiming which server hop rewrites it. Oracle expectations were not
changed. The historical 21/81 plan/Loki row-set agreement is not current API
coverage. Current live-grid artifacts, when present, are separately named
`live-grid-final*`.
