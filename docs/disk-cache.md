---
sidebar_label: Disk Cache
description: L2 cache codecs, LRU capacity, write batching and persisted-record compatibility.
---

# Disk cache

Set `-disk-cache-path` to enable L2. L1 misses can recover values from disk;
peers can serve their disk copies without calling VictoriaLogs. TTLs and full
cache keys, including their tenant and authentication identity, remain intact.

Historical log `query_range` fragments use the same tiered path as settled
pattern source windows, metadata replies and VictoriaLogs metric extents.
Completed `query`, `query_range` and `series` replies up to 128 KiB also enter
the Loki layer's disk and peer tiers; larger replies stay in memory on the
replica that computed them: in the edge cache, or, when a reply is larger than
one edge cache entry (a tenth of its budget), in the primary memory cache with
the edge cache's TTL and freshness rule, up to the primary cache's own entry
bound (a tenth of `-cache-max-bytes`). Those answers share primary L1 with
VictoriaLogs extents and settled steps under its LRU: at the default 256 MiB,
ten answers of the largest admitted size fill it. Known limit: such an answer
is never written to disk or pushed to peers, so another replica computes it
again (`TestFleet_OversizedReplyStaysOnItsReplica`). The exact-response edge cache
remains memory-only. Shared reply keys include the routing configuration fingerprint,
so a tenant or field-mapping reload cannot reuse an older disk or peer answer.
The fragment key also includes routing scope, direction, row budget, aligned
window bounds, parser shape and response profile, so a fragment fetched for a
smaller limit or before a tenant-map reload cannot answer a later request. The
local e2e stack tests these read paths
on one bounded disk volume: it restarts the patterns datasource and requires
identical answers and L2 hits for a shifted pattern request, label values,
metric range, short and windowed log ranges, series and instant metric. Live
tail is a streaming read and does not have a reusable completed answer.

See the [cache storage verification](benchmarks/cache-storage-2026-10-30.md) for
matched benchmarks, allocation profiles and restart/peer regression evidence.
See [fleet operations](fleet-cache-operations.md) for replica capacity, combined
RAM budgets and how cache reuse reduces VictoriaLogs work.

## Capacity and memory

L2 evicts the least recently used persisted entries when admitting a new record
would exceed any configured capacity. Reads update recency in RAM, without a
database write. Expired records are removed during bounded maintenance. Startup
rebuilds the recency index and enforces the configured limits; values survive a
restart. Their previous recency order survives only a graceful shutdown with the
restart hand-over on (below); otherwise the index starts in key order.

| Budget | Default | What it bounds |
|---|---:|---|
| `-disk-cache-max-bytes` | `0` | Encoded record plus key bytes; zero disables this byte limit |
| `-disk-cache-max-entries` | `100000` | Persisted entries |
| `-disk-cache-max-index-bytes` | `16777216` (16 MiB) | Estimated recency metadata, charged at 128 bytes plus key length per entry |
| `-disk-cache-max-buffer-bytes` | `16777216` (16 MiB) | Pending and currently flushing values combined, including keys and queue metadata |

The index and write queue have separate budgets. These accounting limits are
not an RSS ceiling: Go allocator overhead, compression state and in-flight reads
also use memory. Decoded values are limited to the L1 single-entry budget (10%
of `-cache-max-bytes`), with an additional hard limit of 256 MiB.

The logical byte limit is not a filesystem quota. bbolt retains freed pages for
reuse and does not shrink its file on eviction. Monitor the filesystem as well
as cache metrics. For example, add `-disk-cache-max-bytes=1073741824` for a 1 GiB
logical cache budget. LRU capacity does not invalidate cached copies when data
is deleted directly in VictoriaLogs; TTL expiry or an explicit cache purge is
still required.

## Writes and codecs

A single background flusher batches writes. Its defaults are 1000 pending
entries, 4 MiB of queued bytes, or 5 seconds, whichever triggers first. Use
`-disk-cache-flush-size`, `-disk-cache-flush-bytes` and
`-disk-cache-flush-interval` to tune those triggers. The write queue remains
bounded even while storage is slow; cache admission may be rejected under
pressure. Rejected replacements must not expose an older disk value.

`-disk-cache-compress=true` uses `-disk-cache-codec=s2` by default. The codec can
also be `zstd`, `gzip` or `raw`; setting compression to false writes raw records.
Incompressible values are stored raw when compression would make them larger.
Compression uses the already pinned compression dependency; the framing,
capacity accounting and LRU implementation introduce no new modules.

New records contain a version, codec, expiry, decoded length and CRC32C covering
both metadata and decoded contents. Readers validate size limits before
allocating and reject malformed records. The checksum detects corruption; it
is not authentication or encryption. Protect the cache directory and use the
storage platform’s encryption controls where required.

## Restart hand-over

Off by default. With `-cache-shutdown-flush-timeout` (a time budget) a graceful
shutdown (SIGTERM), after the HTTP server drained and every background writer
stopped, walks the memory cache (L1) from the most to the least recently used
entry and, within the budget:

- gives every entry the disk tier would hold the disk recency of its rank and
  writes the ones the disk lacks (a write the queue refused under load, a
  record the disk evicted while memory kept the entry), hottest first;
- keeps each entry's rank and memory hit count in a `cache_handover_v1` bucket
  of the same bbolt file;
- keeps there, up to `-cache-startup-warm-bytes`, the completed `query`,
  `query_range` and `series` replies that stay in memory only because they are
  larger than one edge cache entry (keyed by tenant, routing scope and request).

Other memory-only entries (helper markers, probes, the exact-response edge
cache, non-owner shadows of peer writes) and patterns entries are never kept.
The disk tier's close then writes a manifest as the file's last transaction:
it holds that transaction's id, so any later write to the file (an older binary,
which does not know the bucket, a purge, a process that crashed after it
started) invalidates the hand-over, and the next start drops it whole. A flush
that fails or a process killed before the disk tier closes leaves no manifest.
The bucket is never read by lookups, stale lookups, peer reads or the index
rebuild, and a purge deletes it.

At start the index rebuild orders ranked records by their rank, so the hottest
are newest and evicted last. With `-cache-startup-warm-bytes` (a byte budget,
at most half of `-cache-max-bytes`) a warm-up in the background then loads
ranked entries back into L1, hottest first, until the budget or
`-cache-startup-warm-timeout` (default `120s`, which the Helm chart also sets) is
reached. The replica reports ready when the warm-up finishes, or after the
timeout, whichever comes first: `/ready` answers 503 `response cache warming`
meanwhile and never longer than the timeout.

The warm-up restores in batches of at most 256 entries or about 4 MiB of
decoded values (a larger entry is a batch of its own):

- one read transaction per batch, with at most `min(GOMAXPROCS, 4)` goroutines
  reading and decoding its records;
- a read-ahead of the next records into the page cache (plain reads in file
  order, at most 64 MiB ahead, eight 512 KiB buffers), so a cold disk is read
  in parallel;
- one short hold of the memory cache's lock per batch to admit it. Beside the
  restored entries the warm-up holds about one batch and the read-ahead buffers;
- the deadline is checked before every batch (it stops at most one batch late),
  and a purge that starts at any time after the warm-up began stops it.

- a disk record exactly as a disk hit would promote it: its own expiry, an
  unknown age (freshness rules derive it from the TTL), never over a fresher
  memory entry, and not after a purge started;
- a kept memory-only reply only when the writer's scope equals this process's
  (the build's version and revision and a hash of every flag value and
  environment override except the pod identity and credentials), and it stays
  memory-only: peers never copy it to their disks;
- nothing when this process's clock reads earlier than the flush started, so
  a clock that moved back cannot lengthen an entry; a clock that moved forward
  only expires entries sooner.

The hand-over bucket is outside `-disk-cache-max-bytes`: it holds up to
`-cache-startup-warm-bytes` of memory-only replies (encoded with the disk
codec) plus about 30 bytes and the key per ranked record, so the bbolt file can
grow by that much at shutdown; bbolt keeps the freed pages for reuse after the
next start deletes the bucket.

Restored entries go in behind what live requests stored and never evict them;
the warm-up reads the disk without counting disk hits and leaves the disk
recency as the ranks set it. Disk records are restored by any build, as a disk
hit would serve them, because their keys carry the versions of what decides the
answer; a version rollout therefore restores the disk records and drops the
memory-only replies. The Helm chart sets the flags, a grace period covering the
HTTP drain and the flush, a preStop sleep, a startup probe for the index
rebuild and a one-replica-at-a-time rollout with `cacheHandover.enabled=true`
(chart README, "Rolling restarts with a warm cache"). The metrics are listed in
[Observability](observability.md) (`loki_vl_proxy_cache_restart_*` and
`loki_vl_proxy_cache_startup_l1_*`).

### Limits on how much is restored

All of these apply; the first one reached ends the restore or skips the entry
(`loki_vl_proxy_cache_restart_warm_skipped_entries{reason}`):

| Limit | Default | Effect | For a 2 GiB restore |
|---|---|---|---|
| `-cache-startup-warm-bytes` | `0` (off) | decoded bytes restored | `2147483648` |
| half of `-cache-max-bytes` | 128 MiB (of 256 MiB) | caps the warm budget | `-cache-max-bytes` at least `4294967296` |
| `-cache-max` | 10,000 | memory entries; the rest is `memory_full` | at least the entry count (2 GiB at 100 KiB an entry is about 21,000) |
| a tenth of `-cache-max-bytes` | 25.6 MiB | larger entries are `too_large` | not a limit at 4 GiB (410 MiB) |
| `-disk-cache-max-entries` | 100,000 | records the disk keeps, so ranked ones to restore | at least the entry count |
| `-disk-cache-max-index-bytes` | 16 MiB | 128 bytes plus the key per record: about 50,000 records at 200-byte keys, 37,000 at 320 | `67108864` holds about 140,000 at 320-byte keys |
| `-disk-cache-max-bytes` | `0` (no cap) | stored record bytes the disk keeps | at least the stored size of the hot set, with room |

Set the pod's memory limit and `GOMEMLIMIT` for the memory cache you configure,
not only for the warm budget.

### Estimating the restore time

Before a restart, the time is about the larger of:

- reading: the stored bytes of the hot set (at most the warm budget) divided by
  the disk's sequential read throughput. A page cache that is still warm (a
  container restart on the same node) removes most of it;
- decoding and admitting: the decoded bytes divided by the CPU rate, which
  `BenchmarkRestoreWarm` measures on your hardware (`zstd` decoding runs on one
  core at a time).

Add the index rebuild, which runs before the proxy listens and grows with the
number of records (one read per record on a cold disk), and give the startup
probe room for both. A warm-up that cannot finish in time is not lost work: the
hottest entries are restored first.

While it runs, the log has a `response cache warm-up started` line (the plan,
with an estimate at 125 MB/s of disk throughput, slightly
below gp3's included 125 MiB/s), a `response cache warm-up
progress` line every 2 seconds (percent, bytes per second, estimated time and
time left), a `will not finish before its timeout` warning, and a final
`response cache warmed from the restart hand-over` line with the outcome, rate
and skipped entries by reason (a warning when the deadline cut it). The same
figures are metrics:

```promql
# share done (the further of stored bytes processed and budget filled)
loki_vl_proxy_cache_restart_warm_processed_bytes
  / loki_vl_proxy_cache_restart_warm_planned_bytes
# seconds left, and whether that fits the timeout
loki_vl_proxy_cache_restart_warm_remaining_seconds
loki_vl_proxy_cache_restart_warm_timeout_seconds
  - loki_vl_proxy_cache_restart_warm_duration_seconds
# the last restart's rate, to size the timeout
loki_vl_proxy_cache_restart_warm_throughput_bytes_per_second
# a restart the deadline cut
loki_vl_proxy_cache_restart_warm_outcome{outcome="deadline"} == 1
```

`BenchmarkRestoreWarm`, `BenchmarkRestoreShutdownFlush` and
`BenchmarkRestoreIndexRebuild` (`internal/cache/restore_bench_test.go`) measure
this on your disk: `LVP_RESTORE_BENCH_MB` sets the decoded size,
`LVP_RESTORE_COLD=1` drops the file from the page cache first, and
`LVP_RESTORE_GATE=1 go test -run TestRestoreWarm_ThroughputGate ./internal/cache/`
fails below 400 MiB/s (2 GiB in about 5 s).

### Lowest read time on EBS

Lowest possible time to read the stored data from an EBS volume (stored size
divided by throughput; computed, not measured on EBS). Index rebuild, I/O
latency, request limits, decompression and filling memory come on top.

| Restore into memory (read from disk) | 125 MiB/s | 250 MiB/s | 500 MiB/s |
|---|---|---|---|
| 2 GiB, compressed (721 MiB) | 5.8 s ❌ | 2.9 s | 1.4 s |
| 2 GiB, uncompressed (2 GiB) | 16.4 s ❌ | 8.2 s ❌ | 4.1 s |
| 4 GiB, compressed (1.41 GiB) | 11.6 s ❌ | 5.8 s | 2.9 s |
| 4 GiB, uncompressed (4 GiB) | 32.8 s ❌ | 16.4 s ❌ | 8.2 s |

❌ = the target (2 GiB under 5 s, 4 GiB under 10 s) cannot be met at that speed.
gp3 includes 125 MiB/s and 3,000 IOPS by default and can be provisioned higher.
The compressed rows assume the benchmark data's `s2` ratio of about 2.8x. The
instance's EBS bandwidth can cap the volume, and a volume created from a
snapshot is slower until it is initialised.

### Timeouts

Set from the computed figures above, not from measurements on EBS:

| Setting | Value | Covers | Basis |
|---|---|---|---|
| `-cache-startup-warm-timeout` | `120s` (flag and chart) | the warm-up; the replica is ready as soon as it finishes | computed: the slowest cell above is 32.8 s to read |
| chart `cacheHandover.shutdownFlushTimeoutSeconds` | `60` (the flag stays `0`, off) | writing the records the disk lacks at shutdown | computed: 4 GiB uncompressed at 125 MiB/s is about 33 s |
| chart `terminationGracePeriodSeconds` with the hand-over on | 130 s with the defaults | preStop 5 s, HTTP drain 30 + 30 s, the flush, 5 s to close | follows the two settings; a larger value of yours wins |
| chart startup probe with the hand-over on | 60 failures, 5 s apart after 5 s (305 s) | the index rebuild, before the listener opens | see below |

The index rebuild reads the disk one record at a time: about one 4 KiB read per
record, one after another, because the cache file is mapped without read-ahead.
On an emulated EBS volume (a local VM with the disk throttled, 2026-11-26; not
EBS, and without added latency) it took 32 s for 94,000 records (a 10 GiB file)
at 3,000 IOPS, and the number of IOPS set it, not the throughput. On a volume
with real latency each read also waits that latency, which more IOPS do not
shorten: at 1 to 3 ms a read, 96,000 reads are an estimated 1.5 to 5 minutes.
Watch `loki_vl_proxy_cache_restart_index_rebuild_duration_seconds` and raise
`probe.startup.failureThreshold` for a large disk cache. Without the startup
probe (a disk cache without `cacheHandover`), the chart's liveness probe
restarts a pod whose listener is not open after about 35 to 50 s.

## Upgrades, restarts and purge

Readers continue to accept the previous raw and gzip records from the legacy
bucket, using the previous `-disk-cache-compress` setting for those records.
Keep that setting unchanged until old records expire or are purged. New records
identify their own codec, so changing codecs does not require a cold cache.
New writes replace legacy copies of the same key atomically. A corrupt new
record is a miss and cannot fall back to an older legacy value.

Old binaries cannot read the new bucket. They also ignore the restart
hand-over bucket and leave it in the file; a later start of a current binary
finds its manifest outdated and deletes it. A downgrade can reuse legacy entries
that remain, but loses access to values written in the new format. The disk tier
is disposable: bbolt uses asynchronous syncing, so a crash can lose recent
cache writes. A miss recomputes the answer from the backend.

`POST /admin/cache/flush` purges local memory and disk caches. Add `?peers=1` to
purge the fleet; inspect each peer’s result and retry failures. Purge is ordered
with pending writes so a captured pre-purge batch cannot restore deleted data.
See [Fleet Cache Architecture](fleet-cache.md#ring-wide-cache-flush).
