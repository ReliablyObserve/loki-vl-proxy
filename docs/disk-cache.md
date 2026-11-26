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
`-cache-startup-warm-timeout` (default `10s`) is reached; `/ready` answers
503 `response cache warming` meanwhile and never longer than the timeout:

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
the warm-up reads the disk without counting disk hits. Disk records are
restored by any build, as a disk hit would serve them, because their keys carry
the versions of what decides the answer; a version rollout therefore restores
the disk records and drops the memory-only replies. The metrics are listed in
[Observability](observability.md) (`loki_vl_proxy_cache_restart_*` and
`loki_vl_proxy_cache_startup_l1_*`).

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
