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
| v1.54.0 | size=1KB / 1h | 21.672ms | 15.72ms | -27.5% |
| v1.54.0 | size=1KB / 6h | 120.346ms | 85.455ms | -29.0% |
| v1.54.0 | size=1KB / 24h | 476.638ms | 331.596ms | -30.4% |
| v1.49.0 | size=1KB / 1h | 21.945ms | 16.2ms | -26.2% |
| v1.49.0 | size=1KB / 6h | 116.592ms | 83.638ms | -28.3% |
| v1.49.0 | size=1KB / 24h | 461.365ms | 325.413ms | -29.5% |
| v1.54.0 | number=200 / 24h | 173.139ms | 169.307ms | -2.2% |
| v1.54.0 | d=5s / 24h | 376.887ms | 364.969ms | -3.2% |
| v1.49.0 | number=200 / 24h | 171.627ms | 163.908ms | -4.5% |
| v1.49.0 | d=5s / 24h | 367.077ms | 354.386ms | -3.5% |

`vl-cost-correctness-control.txt` retains every current result. The prior
measurement, `vl-cost-before-compact.txt`, includes an unchanged-query
number-inequality outlier of +16.6%; timing variability is not hidden.
Small number/duration changes are not claimed significant. The separate bare
range pair returns different counts (24h size: 300,001 vs 450,004); its latency
ratio compares different work. CPU time, peak RSS, disk IOPS, network bytes and
end-to-end main/Loki resource costs were not collected by this filter benchmark.

Reproduce:
```sh
GOMEMLIMIT=2GiB GOMAXPROCS=2 LVP_NUMEQ_VLCOST=1 \
  LVP_NUMEQ_BASELINE_FILE="$PWD/bench/numeric-equality/results/2026-11-20/correctness-control-queries.json" \
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

## Corpus and allocation checks

`route-call-comparison.json` compares main eef311db6 with this change on the
existing route-parity harness. All 25,900 common request executions keep their
HTTP status and VictoriaLogs call counts, both cold and on the exact repeat.
Three added matrix queries contribute 30 new executions; no request was removed.
This uses the harness's deterministic backend, not live latency measurements.

The unchanged ranking-allocation gate passes after sharing immutable error
predicates, omitting unused predicate history and precomputing grammar strings.
The 208 query-text construction sites remain unchanged: the error-counting and
lookup pipes are encoded once from typed LogsQL nodes. Log-query oracle answers
were recorded again against both backends; the added JSON numeric matrix query
links its remaining parser-error/empty-label differences to the existing cases.

## Translation misses against main

Six samples per shape, 100ms each, Apple M5 Pro arm64, GOMAXPROCS=2.
The same committed benchmark runs on main eef311db6 and the candidate. These
rows include request validation and a different translation-cache key on each
iteration. Raw samples and all eight shapes are retained in
`translation-main.txt`, `translation-candidate.txt`, `translation-benchstat.txt`
and `translation-comparison.json`.

| Shape | Main / candidate time | Main / candidate B/op | Main / candidate allocs/op |
|---|---:|---:|---:|
| JSON numeric log filter | 20.98 / 20.56 us | 2,369 / 4,510 | 41 / 48 |
| Logfmt numeric metric | 22.37 / 21.11 us | 3,622 / 5,510 | 56 / 59 |
| JSON `==` log filter | 22.46 / 20.88 us | 3,604 / 4,511 | 47 / 48 |
| JSON vector aggregation | 22.15 / 22.53 us | 3,506 / 6,254.5 | 53 / 56 |

The longer encoded query and error-classification AST cost additional memory
on a translation miss. This remains a resource tradeoff requiring review before
merge; passing the ranking gate does not prove memory parity with main. The
small timing changes do not establish end-to-end speed or CPU-time savings.
Peak RSS, disk IOPS and network bytes were not measured by this microbenchmark.

`allocation-diff.txt` is the sampled allocation-space profile difference for
40,000 validated numeric log-filter misses on each build (65,536-byte sampling).
The largest positive sites are query encoding (+38.68 MiB) and translation
cache storage (+38.31 MiB): the longer query string and its cache payload.
The profiles include equal benchmark setup and are not process RSS. Raw
`translation-main.allocs.pprof` and `translation-candidate.allocs.pprof` retain
the sampling evidence; timing tables above come from the separate unprofiled
six-sample run.

## Grafana capture

The existing Playwright capture ran both numeric-equality pages on Grafana
13.3.1 in the existing playground: two tests, four screenshots, no page/API
errors or empty panels. The three query payloads (log entries, their histogram,
and the count metric) match Loki, including timestamps, labels and values.
`visual-comparison.json` also retains the ancillary index/stats and tenant-wide
label-catalog differences; this is not a claim that every resource call matches.

| Page | Proxy | Loki |
|---|---|---|
| Log entries and histogram | [Screenshot](explore-numeric-equality-logs-pr.png) | [Screenshot](explore-numeric-equality-logs-loki.png) |
| Count metric | [Screenshot](explore-numeric-equality-count-pr.png) | [Screenshot](explore-numeric-equality-count-loki.png) |

The selector excludes `query_semantics_run_id` fixtures: the local Loki reference
exposes those test streams together, whereas the proxy isolates their numeric
tenants from tenant 0. The first unscoped capture saw four Loki-only fixture
timestamps; their marker and original line confirmed the different input data.
The final capture uses the same shared dataset, without relaxing comparison.
The two pages are linked to the existing cases in `bench/visual/spec.json`.
Local captures cover proxy/Loki; the CI harness captures main/PR/Loki.
