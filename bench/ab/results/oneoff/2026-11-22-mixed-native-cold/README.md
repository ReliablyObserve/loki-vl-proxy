# Mixed-parser metrics: cold and moving-window API evidence

Measured **2026-11-22**, Apple M5 Pro, Go 1.27.4, Loki 3.7.10 and VictoriaLogs
1.54.0, on the existing dual-written normal-generator dataset. These are dated
measurements of an immutable, uncommitted source tree, **not a passing current-head
CI or whole-Drilldown claim**. `source-and-method.json` identifies every product
Go file, the complete build-source digest, base commit and limits.

Main is `895c23078f4afa12fe0a1200f8138a32f0b7f76a`. Previous and candidate source
digests start `a3e398ffafb9` and `ff5dc9f57a0e`; both already contain the exact
native row partition and raw complement. The latter captures the request's
cancellation channel once, polls it at every evaluated point, and reserves the
known response size for the unchanged settled-steps roundtrip check.

## Complete answers and latency

Six rounds rotate the order of main, previous and candidate; Loki is the fourth
reference. Each query/range starts with the proxy's L0/L1/L2 caches flushed and
has one first request followed by two 60-second forward updates. Peers are
disabled, and each measured proxy has a separate disk cache configured for
128 MiB of logical cache capacity. This is not a physical file-size bound:
bbolt metadata, allocation and free pages can make the file larger. Loki's
results cache and backend/OS caches are **not** cleared; its timings below are
reference observations, not established cold Loki timings.

All **432 responses returned 200**. The retained complete bodies were compared by
their canonical labels, timestamps and values: **108/108 four-way comparisons
match**, including every update. `answer-digests.json` contains those request
identities, full-answer digests and series/point counts; large response bodies are
not committed. `api-rounds.json` preserves all existing collector samples and
cache/resource observations. `summary.json` contains the derived medians.

| Parser order / range | First request main / previous / candidate | Forward update main / previous / candidate |
|---|---:|---:|
| JSON → logfmt, 1 h | 28 / 43 / 38.5 ms | 20 / 14 / 14 ms |
| JSON → logfmt, 6 h | 82 / 162 / 159 ms | 55 / 34 / 35 ms |
| JSON → logfmt, 24 h | 143 / 227 / 219 ms | 68 / 34 / 34.5 ms |
| logfmt → JSON, 1 h | 26.5 / 40.5 / 35 ms | 19.5 / 14 / 14 ms |
| logfmt → JSON, 6 h | 83.5 / 157.5 / 156 ms | 52.5 / 33 / 33 ms |
| logfmt → JSON, 24 h | 140 / 227.5 / 222 ms | 67.5 / 34 / 33.5 ms |

**First requests remain slower than main.** This evidence does not clear that
performance blocker. Small differences at 6 h and 24 h are not statistically
established improvements. Updates retain their advantage over main in this
scoped workload.

## Resource costs and measurement limits

For JSON → logfmt, median first-request process CPU deltas, main / previous /
candidate, are **13.4 / 29.0 / 22.1 ms** at 1 h, **41.1 / 96.7 / 75.4 ms** at
6 h, and **42.0 / 63.6 / 59.5 ms** at 24 h. Process deltas can include background
work; request-scoped backend-call headers are retained separately. The candidate
usually makes one scoped VL call versus main's three; initial discovery calls
also appear in the raw samples. These counts are not inferred from global
counter deltas.

Maximum sampled process RSS, main / previous / candidate, is **165.8 / 160.5 /
180.2 MiB**, **153.8 / 186.3 / 187.7 MiB**, and **155.2 / 186.8 / 226.8 MiB**
respectively. These are sparse samples across requests, not exact peaks or
retained heap. They **do not demonstrate a memory reduction**. Decoded VL bytes
are unchanged between previous and candidate; they are not encoded network
traffic or physical SSD throughput. Per-layer occupancy, physical IOPS,
evictions, fleet pressure and per-query backend CPU were not measured here.

`reducer-before.txt` and `reducer-after.txt` isolate the cancellation change using
the committed `BenchmarkRawRowContextCheck/window-reducer`, six samples per
function, 200 ms per sample, `GOMAXPROCS=2`, `GOMEMLIMIT=3GiB`, `-p=1`. The prior
source uses the same benchmark through a test-file overlay. Count median is
18.800 → 15.905 ms and rate 19.178 → 15.549 ms; measured bytes and allocation
counts are unchanged. This benchmark reuses an already-created context. The
first `Done()` call on a fresh cancellable context can allocate its channel;
zero extra allocations are not claimed for every context or HTTP request.

## Reproduction and scope

Use the existing `bench/ab/perf_matrix.py` collector, `shapes.json` and
`runtime-plan.json` from this directory, with the existing stack's normal
generator and identical proxy profiles:

```sh
python3 bench/ab/perf_matrix.py --set mixed-parser-metrics \
  --shapes-file bench/ab/results/oneoff/2026-11-22-mixed-native-cold/shapes.json \
  --range-plan bench/ab/results/oneoff/2026-11-22-mixed-native-cold/runtime-plan.json \
  --runs 3 --ranges 1h,6h,24h --end 1795378740 --refresh-s 60 \
  --proxy-metrics --timeout 90 --quiet-cap-s 0 \
  --target main=http://127.0.0.1:13102 \
  --target previous=http://127.0.0.1:13151 \
  --target pr=http://127.0.0.1:13150 \
  --target loki=http://127.0.0.1:13101 \
  --flush-before-run0 http://127.0.0.1:13102/admin/cache/flush \
  --flush-before-run0 http://127.0.0.1:13151/admin/cache/flush \
  --flush-before-run0 http://127.0.0.1:13150/admin/cache/flush \
  --out /tmp/mixed-native-cold.json
```

The admin credential is supplied by the operator through `AB_ADMIN_TOKEN`.
Historical bounds require the original retained data; moving them is a new
measurement. Run six times with rotated target order. The source records the
actual collector command and no synthetic fixture is substituted for the normal
generator. The two queries are derived from the existing typed matrix case IDs;
the additional `workload_scope="scope-01"` selector limits this measurement.

The unscoped workload, complete Fields/Labels pages, 12 h / 18 h / 48 h UX,
browser rendering, exact-repeat, SSD restart, RF1/RF2 owner loss and pressure
acceptance are **not established by this run**. Existing whole-stack volumes and
the generator were preserved; all three measured proxy images, commands, users
and mounts were restored and verified after the run. Earlier broader functional
and SSD evidence remains separately dated in
[`../2026-11-21-mixed-parser-metrics/`](../2026-11-21-mixed-parser-metrics/).
