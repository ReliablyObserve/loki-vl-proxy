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

### Restore from peers

A replica that starts with nothing local to warm from (no valid hand-over, and
a disk cache that held no record at start, such as a new volume or an
`emptyDir`, or no disk cache at all) and has a peer cache pulls the entries it
owns or replicates from its peers instead, before `/ready` answers 200. It is
on whenever `-cache-startup-warm-bytes` is set (`-cache-startup-peer-restore`,
default `true`; the Helm chart's `cacheHandover.peerRestore`), and it runs
inside the same budget and the same `-cache-startup-warm-timeout`, so the
readiness, the startup probe and the chart's `progressDeadlineSeconds` are
unchanged. The disk warm-up and the peer restore never both run: a replica with
a hand-over warms from its disk, one without and with records on its disk
serves them from disk as before.

It is a separate transfer from the peer reads of single keys and from the
hot-key read-ahead (`POST /_cache/bulk`, the same peer token as every
`/_cache` route; with `-peer-insecure-ip-allowlist` instead of a token, a peer
that has not discovered the new replica yet answers 403 and is skipped, so the
restore needs the peer token):

1. every peer is probed once (8 at a time; one that does not answer within
   `-peer-timeout` three times, 300-900 ms apart, is skipped). A peer of an
   older build answers without the protocol header and is skipped at once, as
   is one with serving off, one that restores from peers itself (it answers
   `restoring`, so replicas that all start together never wait on each other)
   and one that refuses the token;
2. the restoring replica sends each peer its own ring (its peer list, itself
   and its replication factor), so a peer that has not discovered it yet still
   decides ownership the same way. A peer sends the keys whose replicas include
   the restoring replica and for which it is the first of the other replicas
   among the peers asked: each key crosses the network once, also at a
   replication factor of 3 or more. At a replication factor of 1 a key has no
   other copy (a non-owner keeps only a short shadow, which is never sent), so
   a replica that lost its disk finds nothing to restore;
3. at most `-cache-startup-peer-restore-concurrency` (default 4) streams run at
   once. A peer streams its memory hot set in recency order, then the disk
   records it holds beyond it, newest first, and stops at the requester's
   remaining budget. A busy peer (429) is asked again after its `Retry-After`,
   jittered, until the timeout.

Each record travels as the serving disk stores it (a native disk record with
its CRC32C over the header and the decoded value; values held only in memory
are encoded once with the disk codec) in a frame whose header and payload carry
their own CRC32C. The restoring replica:

- skips a record whose payload or record checksum fails and goes on; a frame
  header that fails ends that stream (the boundaries cannot be trusted), and a
  stream that ends without its end frame is truncated: what arrived before
  stays, record by record;
- admits batches of at most 256 entries or about 4 MiB through the same
  admission as the disk warm-up: within the budget, never over an entry it
  already holds (a live write wins), behind what live requests stored, never
  evicting it, and nothing once a purge started after the restore began;
- keeps each entry for the TTL the peer had left when it sent it (clocks are not
  compared), skips ones with less than 5 s left, and never lengthens one;
- queues the admitted values to its own disk, as a replica keeps a pushed copy,
  while its memory still holds them and the disk has nothing for the key; the
  hottest end newest on disk. A full write queue is committed once and tried
  again; what still does not fit is written by the next graceful shutdown's
  hand-over;
- receives a memory-only answer only from a peer of the same build and
  configuration (the hand-over scope) and keeps it memory-only, never on disk.
  Entries computed with a user's forwarded credentials never leave their
  replica, as with pushes; non-owner shadows and patterns entries are never
  sent.

On the serving side, every bulk stream together is bounded whatever the number
of replicas that start: at most `-peer-bulk-serve-max-streams` (default 2) at
once, a request beyond them answered 429 before any work, at most
`-peer-bulk-serve-max-bytes-per-second` (default 64 MiB/s) for all of them, one
batch (256 entries or about 4 MiB) and a 64 KiB write buffer in memory per
stream, plus the stream's plan (one entry per key it holds). Disk records are
read in batches of one read transaction, copied out by at most
`min(GOMAXPROCS, 4)` goroutines, and sent as stored: nothing is decoded, and
memory values are encoded once. Serving counts no lookup, moves neither its
memory LRU nor its disk recency and touches no breaker. Its streams end when its
HTTP server starts shutting down, so they never hold the drain.

While it runs, the log has `response cache restore from peers started`, the
same progress line every 2 seconds, and `response cache restored from peers`
with the outcome, the skips by reason, the disk writes and each peer's outcome
(`done`, `budget`, `truncated`, `corrupt`, `busy`, `restoring`, `unsupported`,
`unreachable`, `unauthorized`, ...). The metrics are the warm-up's with
`source="peer"` (`loki_vl_proxy_cache_restart_warm_*{source="peer"}`), and the
serving side's `loki_vl_proxy_peer_cache_bulk_serve_*`
([Observability](observability.md)).

Measured over loopback (one process, both ends on an Apple M5 Pro laptop SSD,
`GOMAXPROCS=4`, no network limit, no rate limit, 2026-11-26, a laptop shared
with other work, so the page cache was under pressure at 4 GiB;
`BenchmarkPeerBulkRestore` in `internal/cache/restore_peer_bench_test.go`, the
mixed-size fixture of `BenchmarkRestoreWarm`, served from the serving replica's
disk; mean of 3 runs). "Disk on" also writes every entry to the restoring
replica's new disk:

| Restored (decoded) | Codec, bytes on the wire | Disk off | Disk on |
|---|---|---|---|
| 2 GiB, 19,085 entries | `s2`, 755 MB | 0.80 s | 1.97 s |
| 2 GiB | `raw`, 2,148 MB | 0.78 s | 2.52 s |
| 4 GiB, 37,701 entries | `s2`, 1,511 MB | 2.04 s | 3.61 s |
| 4 GiB | `raw`, 4,296 MB | 3.80 s | 4.41 s |

An earlier run of the same code gave 4 GiB `raw` in 1.42 s and 2.77 s: with the
serving file out of the page cache, its reads decide.

That is the cost of the two ends, not what a pod network carries. On a real
fleet the time is about the stored bytes divided by the smallest of: the
serving rate times the peers streaming at once (64 MiB/s each by default, up
to 4 peers), the restoring node's network bandwidth, and the restoring
replica's disk write rate when its disk is on. Computed, not measured:

| Restore | 1 peer at 64 MiB/s (2 replicas) | 2 peers (3 replicas) | 4 peers (5+ replicas) | 4 peers, no rate limit, 10 Gbit/s node |
|---|---|---|---|---|
| 2 GiB `s2` (720 MiB stored) | 11 s | 5.6 s | 2.8 s | under 1 s (CPU-bound, as loopback) |
| 2 GiB `raw` (2 GiB) | 32 s | 16 s | 8 s | about 2 s |
| 4 GiB `s2` (1.41 GiB) | 23 s | 11 s | 5.6 s | about 2 s |
| 4 GiB `raw` (4 GiB) | 64 s | 32 s | 16 s | about 4 s |

All fit the 120 s warm-up timeout. A node's baseline network bandwidth can be
far below what it bursts to (check your instance type's baseline): keep the
serving rate below that baseline, so
the serving replicas keep bandwidth for their clients and VictoriaLogs. With the
restoring replica's disk on, a gp3 volume's 125 MiB/s of writes bounds it at
about 6 s for 2 GiB `s2` and 12 s for 4 GiB `s2` (the stored bytes): about the default
rate of two peers, so with more peers streaming the volume is the limit. A serving
replica whose disk is not in its page cache reads one record after another per
copy goroutine: on a network volume its read latency then decides (about one
read per record), like the index rebuild (see the targets below).

When many replicas start at once (a scale-out, a node loss, a whole fleet on new
volumes), each serving replica still sends at most 64 MiB/s in at most two
streams; the others are answered 429 at once and ask again. K restoring
replicas holding about D bytes each, served by N replicas, take about
K x D / (N x 64 MiB/s): ten new replicas of 720 MiB from twenty serving ones
about 6 s. Replicas that restore refuse to serve, so a fleet that starts empty
everywhere finishes at once with nothing restored and fills from VictoriaLogs
as before. A peer stream costs the serving replica one plan per request (its
keys, with a hash per key to decide ownership: about 50 ms of CPU for 100,000
keys); no VictoriaLogs call is made on either side.

`LVP_PEER_RESTORE_GATE=1 go test -run TestPeerRestore_ThroughputGate ./internal/cache/`
fails below 200 MiB/s over loopback for 2 GiB (with and without the receiver's
disk); `LVP_PEER_RESTORE_RATE_MB` sets a serving rate for the benchmark.

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
fails below 400 MiB/s of warm-up (2 GiB in about 5 s: the whole local-SSD ready
target below, spent on the warm-up alone).

### Restore time targets by storage

A restarted replica is ready after two phases, one after the other:

1. **index rebuild**, before the listener opens: one read of about 4 KiB per
   record on disk, one at a time. Its time follows the number of records and the
   volume's read latency. The startup probe covers it.
2. **warm-up**, bounded by `-cache-startup-warm-timeout`: reading and decoding
   the hot set. Its time follows throughput, IOPS and, about eight times less
   than the rebuild, latency (eight reads in flight).

The targets are for the benchmark's mix of answer sizes (about 19,000 records
for 2 GiB restored and 38,000 for 4 GiB; `s2` compresses it about 2.8x), with a
cache file that holds little beyond the hot set.

| Storage | Restore | Index rebuild target | Warm-up target | Ready target (both) | Based on |
|---|---|---:|---:|---:|---|
| local SSD | 2 GiB, `s2` or `raw` | 3 s | 2 s | **5 s** | measured |
| local SSD | 4 GiB, `s2` or `raw` | 6 s | 4 s | **10 s** | measured |
| EBS, 375 MiB/s and 6,500 IOPS or more | 2 GiB, `s2` | 90 s | 10 s | **100 s** | emulated + estimated latency |
| EBS, 375 MiB/s and 6,500 IOPS or more | 4 GiB, `s2` | 180 s | 20 s | **200 s** | emulated + estimated latency |
| EBS, 375 MiB/s and 6,500 IOPS or more | 2 GiB, `raw` | 90 s | 20 s | **110 s** | computed + estimated latency |
| EBS, 375 MiB/s and 6,500 IOPS or more | 4 GiB, `raw` | 180 s | 40 s | **220 s** | computed + estimated latency |
| gp3 default, 125 MiB/s and 3,000 IOPS | 2 GiB, `s2` | 90 s | 15 s | **105 s** | emulated + estimated latency |
| gp3 default, 125 MiB/s and 3,000 IOPS | 4 GiB, `s2` | 180 s | 30 s | **210 s** | emulated + estimated latency |
| gp3 default, 125 MiB/s and 3,000 IOPS | 2 GiB, `raw` | 90 s | 35 s | **125 s** | emulated + estimated latency |
| gp3 default, 125 MiB/s and 3,000 IOPS | 4 GiB, `raw` | 180 s | 75 s | **255 s** | emulated + estimated latency |

No EBS volume has been measured. The EBS targets leave room above the estimate
at 3 ms a read (below): about 1.4 times for the rebuild of `raw` records and 1.7
for `s2`, and at least 1.4 times for the warm-up. A volume created from a
snapshot that is not yet initialised, or an instance whose EBS bandwidth is
below the volume's throughput, can miss them.

**Local SSD, measured** (2026-11-26, Apple M5 Pro internal SSD,
`GOMAXPROCS=4`, page cache dropped, `BenchmarkRestoreWarm` and
`BenchmarkRestoreIndexRebuild`; a laptop, not a server): warm-up 0.47 s (`s2`)
and 0.54 s (`raw`) for 2 GiB, 1.03 s and 1.25 s for 4 GiB; index rebuild 1.0 s
for 19,000 records and 2.1 s for 38,000.

**EBS profiles, emulated** (2026-11-26; a local Linux VM with the disk throttled
per container, one run per cell, **no latency added**; the restore code of
commit `52205cd63` plus timeout edits that were not yet committed). Index
rebuild + warm-up:

| Restore | 125 MiB/s, 3,000 IOPS | 375 MiB/s, 6,500 IOPS |
|---|---:|---:|
| 2 GiB, `s2` | 5.9 + 5.5 = 11.4 s | 2.7 + 1.9 = 4.6 s |
| 4 GiB, `s2` | 11.8 + 11.0 = 22.8 s | 5.4 + 3.8 = 9.2 s |
| 2 GiB, `raw` | 7.2 + 16.0 = 23.2 s | not run |
| 4 GiB, `raw` | 14.4 + 33.8 = 48.2 s | not run |

**With volume latency, estimated.** The rebuild waits the latency on every read
(17,900 to 43,300 reads for these sets), so it takes about reads times latency
however many IOPS are provisioned. The warm-up is taken as its time above plus
its reads (11,800 to 43,500, counted in the 125 MiB/s runs) divided by eight,
times the latency. For `raw` at
375 MiB/s, which was not run, the warm-up starts from the computed read time of
the stored size (5.6 s and 11.1 s).

| Restore, storage | Ready at 1 ms (rebuild + warm-up) | Ready at 3 ms (rebuild + warm-up) |
|---|---:|---:|
| 2 GiB `s2`, 375 MiB/s | 18 + 3 = 21 s | 54 + 6 = 60 s |
| 4 GiB `s2`, 375 MiB/s | 36 + 7 = 43 s | 107 + 13 = 120 s |
| 2 GiB `raw`, 375 MiB/s | 22 + 8 = 30 s | 66 + 12 = 78 s |
| 4 GiB `raw`, 375 MiB/s | 43 + 17 = 60 s | 130 + 27 = 157 s |
| 2 GiB `s2`, gp3 default | 18 + 7 = 25 s | 54 + 10 = 64 s |
| 4 GiB `s2`, gp3 default | 36 + 14 = 50 s | 107 + 20 = 127 s |
| 2 GiB `raw`, gp3 default | 22 + 18 = 40 s | 66 + 23 = 89 s |
| 4 GiB `raw`, gp3 default | 43 + 39 = 82 s | 130 + 50 = 180 s |

### Sizing

| What to set | 2 GiB restored | 4 GiB restored |
|---|---|---|
| ready within 5 s / 10 s | local SSD | local SSD |
| ready within 100 s / 200 s (`s2`; `raw` 110 s / 220 s) | EBS with 375 MiB/s and 6,500 IOPS | the same |
| ready within 105 s / 210 s (`s2`; `raw` 125 s / 255 s) | gp3 as included | the same |
| `cacheHandover.startupWarmBytes` (`-cache-startup-warm-bytes`) | `2147483648` | `4294967296` |
| `-cache-max-bytes`, at least | `4294967296` | `8589934592` |
| `-cache-max`, at least the entry count (the benchmark mix) | `20000` | `40000` |
| `-disk-cache-max-index-bytes` | the default 16 MiB holds about 37,000 records at 320-byte keys | `67108864` |
| cache file holding only that hot set, benchmark mix (`s2` / `raw`) | 0.74 / 2.0 GiB | 1.48 / 4.1 GiB |
| pod memory limit / `GOMEMLIMIT` used in the emulated runs (restore only, no traffic) | 6 GiB / `5GiB` | 12 GiB / `10GiB` |

The volume must be larger than the cache file, which holds everything the disk
tier keeps, not only the hot set. Throughput and IOPS shorten the warm-up. The
index rebuild gets shorter only with fewer records (`-disk-cache-max-entries`,
`-disk-cache-max-bytes`) or a volume with lower read latency: in the emulation a
10 GiB file of 94,000 records, 1.9 GiB of them hot, rebuilt in 32 s at 3,000
IOPS and in an estimated 288 s at 3 ms a read.

### Lowest read time on EBS

Lowest possible time to read the stored data from an EBS volume (stored size
divided by throughput; computed, not measured on EBS). Index rebuild, I/O
latency, request limits, decompression and filling memory come on top.

| Restore into memory (read from disk) | 125 MiB/s | 250 MiB/s | 500 MiB/s |
|---|---|---|---|
| 2 GiB, compressed (721 MiB) | 5.8 s | 2.9 s | 1.4 s |
| 2 GiB, uncompressed (2 GiB) | 16.4 s | 8.2 s | 4.1 s |
| 4 GiB, compressed (1.41 GiB) | 11.6 s | 5.8 s | 2.9 s |
| 4 GiB, uncompressed (4 GiB) | 32.8 s | 16.4 s | 8.2 s |

Each figure fits inside the EBS ready target for its restore above; six of the twelve
exceed the local-SSD target. gp3 includes 125 MiB/s and 3,000
IOPS by default and can be provisioned higher.
The compressed rows assume the benchmark data's `s2` ratio of about 2.8x. The
instance's EBS bandwidth can cap the volume, and a volume created from a
snapshot is slower until it is initialised.

### Timeouts

Set from computed, emulated and estimated figures, not from measurements on EBS:

| Setting | Value | Covers | Basis |
|---|---|---|---|
| `-cache-startup-warm-timeout` | `120s` (flag and chart) | the warm-up only (it starts after the index rebuild); the replica is ready as soon as it finishes | estimated: the slowest warm-up above is about 50 s (4 GiB `raw`, gp3 default, 3 ms) |
| chart `cacheHandover.shutdownFlushTimeoutSeconds` | `60` (the flag stays `0`, off) | writing the records the disk lacks at shutdown | computed: 4 GiB uncompressed at 125 MiB/s is about 33 s |
| chart `terminationGracePeriodSeconds` with the hand-over on | 130 s with the defaults | preStop 5 s, HTTP drain 30 + 30 s, the flush, 5 s to close | follows the two settings; a larger value of yours wins |
| chart startup probe with a disk cache, with or without the hand-over | 120 failures, 5 s apart after 5 s (605 s) | the index rebuild, before the listener opens | estimated: see below |
| chart Deployment `progressDeadlineSeconds` with a disk cache | 935 s; 1170 s with the hand-over (defaults) | one replica's replacement | grace period + startup window + (hand-over) warm-up timeout and `minReadySeconds` + 300 s |

**Startup probe.** The chart renders it on `/health` whenever the proxy has a
disk cache (`persistence.enabled` or a non-empty `extraArgs.disk-cache-path`);
without one the chart renders no startup probe unless you enable it. 605 s is
2.1 times the estimated 288 s to rebuild 94,000 records at 3 ms a read, and just
under twice the estimate for the default `-disk-cache-max-entries` of 100,000
(about 306 s). With the default 16 MiB `-disk-cache-max-index-bytes` and keys of
200 bytes or more the index holds about 50,000 records at most. Your
`probe.startup.*` values win. Watch
`loki_vl_proxy_cache_restart_index_rebuild_duration_seconds` and raise
`probe.startup.failureThreshold` for a slower volume. The cost: a pod that hangs
before it listens is restarted only after the whole window.

When a Deployment's replacement pod shares the old pod's volume and node (a
claim with `maxSurge: 0`, as the chart sets with the hand-over), it can wait for
the old pod's lock on the cache file while the old pod terminates, up to the
grace period (130 s) of the startup window, which leaves 475 s.

Without the hand-over the chart sets no Deployment `strategy`, and the
Kubernetes default (25% surge and unavailable, so for one to three replicas one
surge pod and none unavailable) keeps the old pod until the new one is ready.
With a disk cache on one `ReadWriteOnce` claim the surge pod then waits for the
old pod's file lock (on the same node) or for the volume (on another), and the
rollout does not finish. The longer window reports this later: the pod is
restarted after 605 s (before: about 50 s) and the deadline passes at 935 s
(before: 600 s). Use a StatefulSet, the hand-over, or a
`workload.deploymentStrategy` with `maxSurge: 0`.

**Progress deadline.** The chart renders it only on a Deployment with a disk
cache and a startup probe, and only above the Kubernetes default of 600 s;
`workload.progressDeadlineSeconds` replaces it. It restarts nothing: past it,
the Deployment is reported `ProgressDeadlineExceeded` (and `helm --wait` or a
deployment tool may fail), so a rollout that is stuck is reported later too. A
warm-up timeout set through `extraArgs` is not counted. A StatefulSet has no such
field: its rolling update waits for each pod to become ready without a deadline.

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
