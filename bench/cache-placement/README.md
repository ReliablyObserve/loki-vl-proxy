# Real HTTP cache placement proof

This opt-in harness compares two proxy executables against the same immutable
Loki/VictoriaLogs fixture. It runs real proxy processes, HTTP requests, peer
traffic and retained bbolt files. Normal tests skip the process/Compose matrix;
the small parser, response bound, topology and observer regression tests run in
the existing nested `bench` module test job.

The measured comparison uses baseline
`7c59fafa50fcfbb5379871e77eb4fe14b154339e` and candidate production code
`55d3f7fb08a33125d9f4b0b3eb1950c334de3639`. The report records executable
SHA-256 hashes, operator-supplied source refs, fixture identity, OS, architecture
and Go version. This comparison includes both the disk-cache changes and peer
placement changes between those versions; it does not isolate a single change.

## Reproduce

Use Go matching the repository, Docker Compose and absolute paths. Reserve the
proxy port range 55200–55288 and backend ports 54101/54428, or change all three
port flags consistently. Setup and cleanup affect only the selected Compose
project and artifact directory. Do not reuse a fixture directory after
recreating its backend volumes.

From the repository root, build each revision in a separate checkout:

```sh
REPO="$(pwd)"
TASK_DIR="$(mktemp -d /tmp/cache-placement-proof.XXXXXX)"
BASE_REF=7c59fafa50fcfbb5379871e77eb4fe14b154339e
CANDIDATE_REF=55d3f7fb08a33125d9f4b0b3eb1950c334de3639
BASE_TREE="$TASK_DIR/baseline"
CANDIDATE_TREE="$TASK_DIR/candidate"
BASE_BIN="$TASK_DIR/proxy-baseline"
CANDIDATE_BIN="$TASK_DIR/proxy-candidate"
OUT="$TASK_DIR/results"
rtk git fetch origin "$BASE_REF" "$CANDIDATE_REF"
rtk git worktree add --detach "$BASE_TREE" "$BASE_REF"
rtk git worktree add --detach "$CANDIDATE_TREE" "$CANDIDATE_REF"
rtk go build -C "$BASE_TREE" -o "$BASE_BIN" ./cmd/proxy
rtk go build -C "$CANDIDATE_TREE" -o "$CANDIDATE_BIN" ./cmd/proxy
```

Run the harness from the nested module. The function keeps identical executable,
backend, output and port arguments across operations:

```sh
proof() {
  rtk go test -C "$REPO/bench" ./cache-placement -count=1 -timeout=30m -args \
    -perf-root="$REPO" -perf-output="$OUT" \
    -perf-baseline="$BASE_BIN" -perf-candidate="$CANDIDATE_BIN" \
    -perf-baseline-ref="$BASE_REF" -perf-candidate-ref="$CANDIDATE_REF" \
    -perf-project=cacheplacementproof \
    -perf-loki-port=54101 -perf-vl-port=54428 -perf-proxy-port-base=55200 \
    "$@"
}
proof -perf=setup
proof -perf=probe
```

A probe runs two source blocks with two repeats; the default small fit probe
uses two shards. It verifies responses and wire accounting without making a
quiet-host latency claim. Explicit `-perf-profile=fit|pooled|pressure` selects
the full corresponding shard count.

Stop other benchmark, compatibility and race workloads before timing. Run a
same-source ABBA control first; then run the actual ABBA comparison. The
control substitutes the baseline executable for both source labels. The order
is baseline, candidate, candidate, baseline, with fresh cache files for each
block. Both versions use the same proxy addresses, hash-ring addresses and
single backend URL, so backend identity and ownership are comparable.

```sh
proof -perf=measure -perf-quiet=true -perf-control=true -perf-profile=fit
proof -perf=measure -perf-quiet=true -perf-profile=fit
proof -perf=measure -perf-quiet=true -perf-profile=pooled
proof -perf=probe -perf-profile=pressure
proof -perf=scaling-probe
proof -perf=scaling-counts -perf-scaling-profile=pool-pressure
proof -perf=scaling -perf-quiet=true -perf-control=true
proof -perf=scaling -perf-quiet=true
proof -perf=cleanup
```

Use `-perf-rf=1` for owner-only placement; the default is RF=2. The cleanup
removes backend volumes but retains reports. Worktree cleanup is separate.
Helper checks need no running stack:

```sh
rtk go test -C "$REPO/bench" -race ./cache-placement -count=1
```

## What each mode measures

Startup label warming and pattern processing are disabled. The indexed-label
cache remains disabled, matching its default. Peer hot read-ahead remains disabled by default. Health
and version traffic during startup is recorded separately. A zero-backend
request phase therefore describes requested work against a prepared fixture;
default production startup warmers can still read VictoriaLogs.

| Mode | Initial state and request |
|---|---|
| cold-fill | Fresh process/cache files; fill through node 0. |
| exact | Same fixed historical windows, round-robin through three frontends; includes first visits to other frontends. |
| frontend-prime | Three rounds through all frontends. |
| warm-repeat | Replay only after that explicit frontend priming. |
| shifted | Historical start/end move backward 60 seconds per repeat, matching the moving-window aspect of daily A/B. |
| refresh | Historical start/end advance into previously unrequested windows. The initial historical end is 30 minutes before seeded history ends; refresh advances +60, +120, … seconds inside that history. |
| recent-refresh | Advances windows in the immutable setup-time recent fixture. Each response records end age and eligibility for the default 120-second recent-tail and 24-hour metadata freshness windows. It is not a new-ingestion proof or necessarily near-now. |
| restart | Restart every proxy with disk retained, then request the original fixed windows. |
| owner-loss | Restart surviving frontends with retained disk, stop node 0, then request fixed windows. Only keys whose placement involves node 0 exercise its loss. |

Phases within a block share cache history. Forward refresh windows are unseen,
but this is not an independent cache-state experiment for every mode. Older
preparation results whose refresh reversed earlier shifted windows describe
historical-window replay, not unseen forward refresh.

The fit profile has 256 L1 entries, 16 MiB L1 and 16 MiB logical disk per node,
with eight shards. The pooled profile uses the same L1 budget, 2 MiB logical
disk and 90 shards. The tiny-budget pressure profile has 64 L1 entries,
2 MiB L1 and 32 KiB logical disk per node with 90 shards. Tier0 remains enabled
in all profiles. Each source uses the common 1-second flush interval and
1000-entry flush threshold; other defaults can differ between the revisions.

The six workload families cover DLV1 JSON Drilldown volume, DLV2 JSON error
filter, DLV3 JSON volume, a count metric, ordered log rows and per-shard label
values. DLV1–DLV3 still exercise the ordered JSON/raw query path; this harness
does not assume that placement changes add shifted-window reuse to that path.

## Correctness and measurement boundaries

Every measured request is compared with a direct Loki request using the same
path and headers. Canonical comparison ignores execution statistics and
unordered result-series/metadata-list ordering; it retains log-entry order and
values and normalizes numeric sample spelling. Parsing rejects trailing JSON
or oversized responses. Mismatch bodies are retained under a unique run/event
path. Non-200 parity is not successful throughput: scaling reports
`SuccessfulCorrect`, `HTTPErrors` and `ValidThroughput`; any failed or
incorrect scaling response invalidates a throughput claim.

Reports keep p50/p95, individual response times, per-phase backend request
counts/ranges and peer contacts. Whole-phase counters are authoritative:
concurrent individual request intervals overlap. Counts include a bounded
quiet drain for asynchronous replica pushes. Direct Loki cache state is not
reset per request, so these are not cold-Loki measurements.

The raw JSONL wire ledger records method, path, ranges/forms, status, encoded
request-body length and response-body bytes. Peer GET response bytes and
replicated SET request bytes are separate. The connection observer additionally
counts client-to-observer HTTP message bytes, including headers, framing and
encoded bodies. It excludes TCP/IP headers, retransmissions and the additional
observer-to-target hop. Current phase snapshots wait for active HTTP responses
and in-flight writes to finish before pairing ledger and byte counters; a
bounded timeout fails the measurement explicitly. Archived 2026-10-30 reports
precede this completion barrier and may omit trailing HTTP writes. Their
message-byte totals are observed counts, not exact transfer totals; old phase
ledger boundaries could also omit or misattribute a late handler entry despite
the quiet drain. Earlier
preparation results with payload-only counters cannot be treated as full wire
byte counts. Instrumentation is present for
both sources and adds overhead; timings describe this instrumented loopback
setup.

CPU counters cover proxy processes, excluding the observer, Loki and
VictoriaLogs. Metrics samples record expected/alive/sampled nodes and errors;
incomplete samples invalidate CPU/memory claims. Intentionally stopped owners
are excluded. On macOS the process memory metric is Go MemStats.Sys, not RSS;
kernel child `Maxrss` is collected at process exit and labelled separately.
Physical bbolt file length and allocated filesystem blocks are high-water
storage measures, not logical admitted cache bytes.

A practical latency difference requires both 25% and 50 ms. Small transport
changes should be reported as absolute time, allocations or request/byte
counts instead. A same-source control does not establish statistical
significance; report control variation and raw matched block values.

## Reader scaling scope

The optional scaling matrix keeps three cache owners fixed and starts 1, 3
or 6 cold **nonmember** reader proxies. The test verifies that each reader's
ring excludes itself and does not configure reader SSD. Concurrent cold
requests are grouped into workload waves, so fleet backend counts can be
attributed to each wave. Warm throughput uses one concurrent request stream
per reader over the representative workload mix.

This measures independent-request throughput and request latency on one host,
where CPU, SSD and loopback are shared. It does not distribute a single query's
execution across replicas, and it does not demonstrate production linear
scaling or increased SSD capacity from adding cache owners. Membership
growth, remapping, migration of existing records and convergence to N/R
persistent copies are separate properties.


The optional `scaling-counts` mode with
`-perf-scaling-profile=pool-pressure` disables Tier0 and uses 90 historical
metadata identities, exceeding the 25-slot nonowner shadow budget. Each source
primes three owners, probes two identities on cold readers, and performs three
fixed passes over all 90 identities per reader. Aggregate wire and backend
counts describe this finite pass; it makes no throughput claim. This profile
also differs from the fit workload mix, so their timings are not comparable.
