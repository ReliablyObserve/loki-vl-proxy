---
sidebar_label: Disk Cache
description: L2 cache codecs, LRU capacity, write batching and persisted-record compatibility.
---

# Disk cache

Set `-disk-cache-path` to enable L2. L1 misses can recover values from disk;
peers can serve their disk copies without calling VictoriaLogs. TTLs and full
cache keys, including their tenant and authentication identity, remain intact.

See the [cache storage verification](benchmarks/cache-storage-2026-10-30.md) for
matched benchmarks, allocation profiles and restart/peer regression evidence.
See [fleet operations](fleet-cache-operations.md) for replica capacity, combined
RAM budgets and how cache reuse reduces VictoriaLogs work.

## Capacity and memory

L2 evicts the least recently used persisted entries when admitting a new record
would exceed any configured capacity. Reads update recency in RAM, without a
database write. Expired records are removed during bounded maintenance. Startup
rebuilds the recency index and enforces the configured limits; values survive a
restart, but their previous in-memory recency order does not.

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

## Upgrades, restarts and purge

Readers continue to accept the previous raw and gzip records from the legacy
bucket, using the previous `-disk-cache-compress` setting for those records.
Keep that setting unchanged until old records expire or are purged. New records
identify their own codec, so changing codecs does not require a cold cache.
New writes replace legacy copies of the same key atomically. A corrupt new
record is a miss and cannot fall back to an older legacy value.

Old binaries cannot read the new bucket. A downgrade can reuse legacy entries
that remain, but loses access to values written in the new format. The disk tier
is disposable: bbolt uses asynchronous syncing, so a crash can lose recent
cache writes. A miss recomputes the answer from the backend.

`POST /admin/cache/flush` purges local memory and disk caches. Add `?peers=1` to
purge the fleet; inspect each peer’s result and retry failures. Purge is ordered
with pending writes so a captured pre-purge batch cannot restore deleted data.
See [Fleet Cache Architecture](fleet-cache.md#ring-wide-cache-flush).
