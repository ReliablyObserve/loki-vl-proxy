---
sidebar_label: Cost Model (AWS)
description: Illustrative AWS EC2 cost model for Loki versus VictoriaLogs plus Loki-VL-proxy across three user and ingestion scenarios, with explicit assumptions and formulas.
---

# Cost Model

This page is an **illustrative** AWS cost worksheet, not a product benchmark.
The AWS numbers are pure calculations used to express the comparison in `$$`,
not observed cloud bills.

It exists to make the cost discussion concrete for three paired scenarios:

- `100` active users with `100k` log lines per second
- `1,000` active users with `500k` log lines per second
- `10,000` active users with `1M` log lines per second

The numbers below are useful because they are explicit. They are also limited
because they depend on assumptions. Use them as a planning aid, not as a
guaranteed savings promise.

## Assumptions

### Traffic and retention

- `7d` retention
- average raw log line size: `250 B`
- active read demand: `0.1` Grafana or API read requests per second per active
  user

That gives these paired scenarios:

| Scenario | Active users | Read demand | Log ingest |
|---|---:|---:|---:|
| Small | `100` | `10 rps` | `100k lines/s` |
| Medium | `1,000` | `100 rps` | `500k lines/s` |
| Large | `10,000` | `1,000 rps` | `1M lines/s` |

### AWS price assumptions

Illustrative `us-east-1` on-demand list prices used only to convert resource
shapes into explicit dollar figures:

| Resource | Assumed monthly price |
|---|---:|
| `c7i.large` | `$62.05` |
| `c7i.xlarge` | `$124.10` |
| `c7i.2xlarge` | `$248.20` |
| `c7i.4xlarge` | `$496.40` |
| `gp3` EBS | `$0.08 / GB-month` |

### Storage assumptions

The storage model uses two sourced inputs:

- VictoriaLogs docs say logs usually compress by `10x` or more.
- TrueFoundry reported VictoriaLogs storage at about `37%` less than Loki on
  its side-by-side workload.

So this worksheet uses:

- `VictoriaLogs stored bytes = raw retained bytes / 10`
- `Loki stored bytes = VictoriaLogs stored bytes / 0.63`

This is intentionally conservative and transparent. Change the factors if a
target environment has better measurements.

### Real-life VictoriaLogs compression note

Some operators see much higher VictoriaLogs compression ratios in practice,
including `50-60x` on the VictoriaLogs compression-ratio metric.

That number is useful, but it is **not** identical to full billed retained
storage:

- it is a ratio between original data size and compressed data blocks on disk
- it explicitly excludes `indexdb` size
- it moves over time as background merges and retention policies reshape the
  stored dataset

For that reason, the main table below keeps the more conservative `10x`
VictoriaLogs storage factor for budget planning, and then shows the `50-60x`
data-only view separately as an optimistic lower-bound.

### Compute-topology assumptions

The Loki side now uses Grafana's own published distributed sizing guide as the
starting point.

Grafana's Loki sizing docs publish these base cluster requests by ingest tier:

- `<3 TB/day`: `38 vCPU`, `59 Gi`
- `3-30 TB/day`: `431 vCPU`, `857 Gi`
- `~30 TB/day`: `1221 vCPU`, `2235 Gi`

That is a **base request floor**, not a hard cap. The same page also warns that
unoptimized queries can require `10x` the suggested querier resources.

This worksheet converts those published Loki CPU/memory requests into the
closest simple `c7i.4xlarge` node floor (`16 vCPU`, `32 Gi` each), then keeps a
separate conservative reference pack for `VictoriaLogs + Loki-VL-proxy`.

Reference compute packs used in the table below:

| Scenario | Raw ingest/day | Loki docs tier | Loki published base request | Loki EC2 compute floor | VictoriaLogs + proxy reference compute |
|---|---:|---|---|---|---|
| Small | `2.16 TB/day` | `<3 TB/day` | `38 vCPU / 59 Gi` | `3 x c7i.4xlarge` | `2 x c7i.large` proxy + `2 x c7i.large` backend |
| Medium | `10.8 TB/day` | `3-30 TB/day` | `431 vCPU / 857 Gi` | `27 x c7i.4xlarge` | `2 x c7i.large` proxy + `3 x c7i.xlarge` backend |
| Large | `21.6 TB/day` | `3-30 TB/day` | `431 vCPU / 857 Gi` | `27 x c7i.4xlarge` | `3 x c7i.large` proxy + `4 x c7i.2xlarge` backend |

There is no equally detailed official VictoriaLogs ingest-tier sizing matrix
for a direct like-for-like conversion here. The `VictoriaLogs + proxy` side
therefore remains a conservative project-side reference pack rather than a
vendor-published floor.

## Formulas

### Raw retained data

```text
raw_bytes_per_second = lines_per_second * 250
raw_gb_per_day = raw_bytes_per_second * 86400 / 1e9
raw_gb_for_7_days = raw_gb_per_day * 7
```

### Stored data

```text
victorialogs_gb = raw_gb_for_7_days / 10
loki_gb = victorialogs_gb / 0.63
```

### Monthly total

```text
monthly_total = compute_monthly + storage_gb * 0.08
```

## Real-Life Tested VictoriaLogs Baseline

The table below uses a real-life tested VictoriaLogs setup instead of the
generic `250 B` line-size assumption above:

| Metric | Observed value |
|---|---:|
| Total log entries | `800 M` |
| Ingested logs `24h` | `112 M` |
| Ingested bytes `24h` | `310 GiB` |
| Insert requests per second | `1.20 K` |
| Read requests per second | `0` |
| Compression ratio | `54.9` |
| Disk space usage | `40.5 GiB` |
| Available CPU | `43` |
| Available memory | `43 GiB` |

Derived observations:

- retained window from `800 M / 112 M per day` is about `7.14 days`
- average raw event size is about `2.9 KiB`
- compressed data blocks per day are about `5.65 GiB` at the observed
  `54.9x` ratio
- projected `7d` compressed data blocks are about `39.5 GiB`, which lines up
  closely with the observed `40.5 GiB` disk usage

Important:

- this is a **write-heavy** snapshot because read traffic is `0 rps`
- `available CPU` and `available memory` are capacity figures, not proven
  consumed usage, so they should **not** be turned into direct CPU or RAM
  savings claims by themselves

### Real-Life Tested Compute Envelope

Using the measured component footprint from the same real-life tested
VictoriaLogs setup:

- `vlstorage`: about `1.0` core and `5.0 GiB`
- `vlinsert`: about `0.1` core and `0.6 GiB`
- `vlselect`: about `0.1` core and `0.25 GiB`

This gives a practical VictoriaLogs service envelope of:

- `1.2` cores total
- `5.85 GiB` total

That envelope is the only CPU/memory baseline used below. The dashboard fields
`available CPU = 43` and `available memory = 43 GiB` are treated as cluster
headroom, not as service consumption.

| Scale | Raw ingest/day | Scaled VL envelope | Illustrative VL EC2 floor | Loki published compute floor | Loki / VL CPU ratio | Loki / VL memory ratio |
|---|---:|---:|---|---:|---:|---:|
| `1x` | `0.333 TB/day` | `1.2 cores / 5.85 GiB` | `1 x c7i.xlarge` | `$1,489.20 / month` | `31.7x` | `10.1x` |
| `10x` | `3.33 TB/day` | `12 cores / 58.5 GiB` | `4 x c7i.2xlarge` | `$13,402.80 / month` | `35.9x` | `14.6x` |
| `30x` | `9.99 TB/day` | `36 cores / 175.5 GiB` | `6 x c7i.4xlarge` | `$13,402.80 / month` | `12.0x` | `4.9x` |
| `100x` | `33.29 TB/day` | `120 cores / 585 GiB` | `19 x c7i.4xlarge` | `$38,222.80 / month` | `10.2x` | `3.8x` |

Illustrative monthly VictoriaLogs compute floors for those rows are:

- `1x`: `$124.10 / month`
- `10x`: `$992.80 / month`
- `30x`: `$2,978.40 / month`
- `100x`: `$9,431.60 / month`

### Real-Life Tested Steady-State High-Load Envelope

The real-life tested setup also includes a higher steady-state envelope:

- ingest throughput: about `2.5k` events per second
- raw ingest bandwidth: about `6.5 MB/s`
- component footprint:
  - `vlstorage`: about `1.0` core and `5.0 GiB`
  - `vlinsert`: about `0.1` core and `0.6 GiB`
  - `vlselect`: about `0.1` core and `0.25 GiB`

Converted into the same worksheet shape, that envelope becomes:

| Scenario | Raw ingest/day | VictoriaLogs retained `~7.1d` | Estimated Loki retained `~7.1d` | Scaled VL envelope | Illustrative VL EC2 floor | Loki published tier | Loki compute floor | Loki cross-AZ write payload/day | Effective inter-AZ monthly cost |
|---|---:|---:|---:|---:|---|---|---:|---:|---:|
| Real-life tested steady-state high load | `0.56 TB/day` | `68.3 GiB` | `108.4 GiB` | `2.0 cores / 9.9 GiB` | `1 x c7i.2xlarge` | `<3 TB/day` | `$1,489.20 / month` | `1,046 GiB/day` | `$627.60 / month` |

What this extra row means:

- it is materially above the daily average snapshot, so it should be treated as
  a sustained higher-load envelope rather than as the primary daily baseline
- it still lands well below Loki's first published distributed throughput floor
- even at that higher steady-state envelope, Loki's published compute and
  replication floor remain much larger than the tested VictoriaLogs shape

### 3-AZ VictoriaLogs Topology Note

The resource tables above describe measured process envelope and use combined
compute for cost comparison. A normal multi-AZ production layout is different
because `vlinsert`, `vlselect`, and `vlstorage` are usually spread across
separate nodes or pools.

For a 3-AZ VictoriaLogs cluster with:

- `1 x vlinsert` per AZ
- `1 x vlselect` per AZ
- `3-4 x vlstorage` pods per AZ

the minimum pod topology is:

- `3 x vlinsert`
- `3 x vlselect`
- `9-12 x vlstorage`

For the worksheet, those topologies still use the combined compute rows above:

| Topology | Minimum pod shape | Cost-model treatment |
|---|---|---|
| `3 x vlstorage` per AZ | `3 x vlinsert`, `3 x vlselect`, `9 x vlstorage` | keep the combined compute envelope used in the main tables |
| `4 x vlstorage` per AZ | `3 x vlinsert`, `3 x vlselect`, `12 x vlstorage` | keep the combined compute envelope used in the main tables |

This avoids overstating VictoriaLogs cost by multiplying the measured service
envelope per pod. The measured `5.0 GiB` and `1.0` core figures apply to the
tested `vlstorage` service envelope as a whole, not per storage pod.

## Scaling The Real-Life Tested Baseline To Loki Floors

The table below keeps the real-life tested VictoriaLogs baseline for storage
and retention, then maps scaled ingest to Loki's published throughput tiers.
For the Loki storage column, it uses the same conservative cross-system
assumption as the rest of this page: `VictoriaLogs retained bytes = 63% of Loki
retained bytes`.

| Scale | Raw ingest/day | VictoriaLogs retained `~7.1d` | Estimated Loki retained `~7.1d` | VictoriaLogs gp3 | Loki gp3 | Loki published tier | Loki compute floor |
|---|---:|---:|---:|---:|---:|---|---:|
| `1x` | `0.333 TB/day` | `40.5 GiB` | `64.3 GiB` | `$3.24` | `$5.14` | `<3 TB/day` | `$1,489.20 / month` |
| `10x` | `3.33 TB/day` | `405 GiB` | `642.9 GiB` | `$32.40` | `$51.43` | `3-30 TB/day` | `$13,402.80 / month` |
| `30x` | `9.99 TB/day` | `1,215 GiB` | `1,928.6 GiB` | `$97.20` | `$154.29` | `3-30 TB/day` | `$13,402.80 / month` |
| `100x` | `33.29 TB/day` | `4,050 GiB` | `6,428.6 GiB` | `$324.00` | `$514.29` | `~30 TB/day` | `$38,222.80 / month` |

What this real baseline says:

- the observed VictoriaLogs storage footprint is extremely small for the raw
  bytes ingested because the measured compression ratio is high
- the storage delta versus Loki is real, but at larger scales the **published
  Loki compute floor** dominates the monthly bill much more than gp3 storage
  does
- because the observed snapshot has `0` read requests per second, it is useful
  for **storage and ingest-tier calibration**, but not for proving read-path
  savings from the proxy cache stack

## Inter-AZ Network Cost Model

AWS EC2 pricing states that data transferred across Availability Zones in the
same Region is charged at `$0.01/GB` **in** and `$0.01/GB` **out**. For a
payload that crosses an AZ boundary once, this worksheet therefore models an
effective inter-AZ transport price of:

```text
$0.02 / GB crossed once
```

### Loki 3-AZ write-path floor

Loki docs state that the distributor forwards each stream to a
`replication_factor` of ingesters and that the replication factor is generally
`3`.

In a 3-AZ spread with replication factor `3`, a simple write-path floor is:

- one replica written to an ingester in the local AZ
- two replicas written to ingesters in remote AZs

That means the **minimum** cross-AZ write payload is approximately:

```text
2 x raw ingest volume
```

This ignores extra cross-AZ traffic from:

- query fanout
- querier to ingester reads
- index/object-store fetches
- compactor, ruler, or other component chatter

So this is a floor, not a ceiling.

| Scale | Raw ingest/day | Cross-AZ Loki write payload/day | Effective inter-AZ monthly cost |
|---|---:|---:|---:|
| `1x` | `310 GiB` | `620 GiB` | `$372.00` |
| `10x` | `3,100 GiB` | `6,200 GiB` | `$3,720.00` |
| `30x` | `9,300 GiB` | `18,600 GiB` | `$11,160.00` |
| `100x` | `31,000 GiB` | `62,000 GiB` | `$37,200.00` |

### Why Loki read-path traffic is harder to cap

Loki docs say:

- queriers query all ingesters for in-memory data before falling back to the
  backend store
- query frontends split larger queries into multiple smaller queries and execute
  them in parallel on downstream queriers

Loki's zone-aware replication design docs are also explicit that:

- **minimizing cross-zone traffic costs is a non-goal**
- a remaining open question is how to make queriers and ingesters zone-aware so
  that each querier only queries ingesters in the same zone

That is why this worksheet does **not** attach a hard cross-AZ read bill to the
observed system: with `0 rps` reads in the snapshot, any numeric read-path
network bill would be invented rather than measured.

### VictoriaLogs + proxy in 3 AZ

VictoriaLogs cluster docs explicitly support:

- independent VictoriaLogs instances or clusters in separate availability zones
- advanced multi-level cluster setups
- HA patterns where copies of logs are sent to independent clusters and queries
  can return full responses from the remaining AZ when one AZ is unavailable

This matters because it enables a different traffic shape:

- local clients can be pinned to local proxy and local VictoriaLogs paths for
  normal reads
- global fanout can be reserved for explicit global or failover queries instead
  of being the default shape for every read
- Loki-VL-proxy adds `zstd`/`gzip` compression on the read path it controls,
  which reduces client and peer-cache transport bytes on repeated reads

Important constraint:

- the docs do **not** publish a stable per-hop VictoriaLogs replication
  compression ratio for cross-AZ HA traffic
- because of that, this worksheet does not invent a hard dollar figure for
  VictoriaLogs cross-AZ write replication

That omission is intentional. It keeps the model honest.

## Storage Model Output

| Scenario | Raw 7d retained data | VictoriaLogs stored data | Loki stored data |
|---|---:|---:|---:|
| Small | `15,120 GB` | `1,512 GB` | `2,400 GB` |
| Medium | `75,600 GB` | `7,560 GB` | `12,000 GB` |
| Large | `151,200 GB` | `15,120 GB` | `24,000 GB` |

## VictoriaLogs Data-Only Compression View

This table uses the real-life `50-60x` VictoriaLogs compression ratio **only**
for the compressed data blocks, excluding `indexdb`.

| Scenario | Raw 7d retained data | VictoriaLogs data blocks at `50x` | VictoriaLogs data blocks at `60x` | Data-block storage cost range |
|---|---:|---:|---:|---:|
| Small | `15,120 GB` | `302.4 GB` | `252.0 GB` | `$24.19 -> $20.16` |
| Medium | `75,600 GB` | `1,512 GB` | `1,260 GB` | `$120.96 -> $100.80` |
| Large | `151,200 GB` | `3,024 GB` | `2,520 GB` | `$241.92 -> $201.60` |

Use that range as:

- an optimistic real-life storage floor for VictoriaLogs data blocks
- a sanity check when the actual retained-disk bill is much higher or lower

Do **not** treat it as a full retained-storage number until you add:

- `indexdb`
- filesystem overhead
- snapshots and backups
- retention-window skew
- temporary variance during background merges

## Monthly Cost Scenarios

| Scenario | Loki compute | Loki storage | Loki total | Proxy compute | VictoriaLogs compute | VictoriaLogs storage | Proxy + VL total | Monthly delta | Savings |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Small | `$1,489.20` | `$192.00` | `$1,681.20` | `$124.10` | `$124.10` | `$120.96` | `$369.16` | `$1,312.04` | `78.0%` |
| Medium | `$13,402.80` | `$960.00` | `$14,362.80` | `$124.10` | `$372.30` | `$604.80` | `$1,101.20` | `$13,261.60` | `92.3%` |
| Large | `$13,402.80` | `$1,920.00` | `$15,322.80` | `$186.15` | `$992.80` | `$1,209.60` | `$2,388.55` | `$12,934.25` | `84.4%` |

## Loki Sizing Guide Converted To EC2 Cost

This is the raw conversion of Grafana's published Loki tier requests into a
simple `c7i.4xlarge` floor before storage and before any query spikes.

| Loki docs ingest tier | Published base request | `c7i.4xlarge` nodes required | Monthly compute floor |
|---|---:|---:|---:|
| `<3 TB/day` | `38 vCPU / 59 Gi` | `3` | `$1,489.20` |
| `3-30 TB/day` | `431 vCPU / 857 Gi` | `27` | `$13,402.80` |
| `~30 TB/day` | `1221 vCPU / 2235 Gi` | `77` | `$38,222.80` |

Those numbers are important because they come from Loki's own distributed
throughput guidance, not from a marketing claim made by this project.

## Compression Between Components

Compression affects both the retained-storage bill and the network bill, but
they are different layers:

- Loki docs describe compressed chunk blocks and compressed structured metadata
  inside chunk storage
- VictoriaLogs docs describe `10x` or better usual log compression and expose a
  data-block compression ratio that some operators observe at `50-60x`,
  excluding `indexdb`
- Loki-VL-proxy adds compressed transport on the read path it controls:
  `gzip` client responses, `zstd`/`gzip` peer-cache transfers, gzip
  disk-cache values, and negotiated upstream compression with safe decode

That means:

- backend storage savings are mostly a VictoriaLogs-versus-Loki question
- repeated-read network savings are partly a proxy question, because cache hits
  and compressed peer/client hops reduce bytes moved for the same user action

## Why VictoriaLogs Compression Ratios And Loki Customer Reports Differ

When a VictoriaLogs operator sees `50-60x`, that is usually a **data-block**
number. When a Loki customer publishes a side-by-side retained-storage delta,
that is a **whole-system footprint** number.

Example:

- VictoriaLogs data blocks might compress `50-60x`
- the same VictoriaLogs deployment can still have a higher total retained
  footprint once `indexdb` and filesystem overhead are included
- a Loki side-by-side report such as TrueFoundry's `≈40%` storage reduction is
  comparing full retained bytes across two different systems, not just one
  internal compression metric

Use the `50-60x` number as a lower-bound signal for VictoriaLogs data blocks.
Use cross-system retained-byte comparisons to talk about the actual bill.

## What These Numbers Mean

### Why the delta grows with scale

The model assumes three things compound together as ingest grows:

- VictoriaLogs stores less data than Loki on the same retained raw workload
- VictoriaLogs backend compute pack stays smaller than Loki's reference pack
- proxy cache layers remove repeated read work instead of sending every repeated
  dashboard refresh upstream

That is why the gap widens in the medium and large scenarios.

If the actual VictoriaLogs dataset stays closer to the observed `50-60x`
data-only compression range, the storage part of the `Proxy + VL` column can
drop further than the conservative table shows. The compute and proxy sections
of the model stay the same.

### Why the proxy does not erase the savings

The proxy is not free, but it is intentionally small.

The reference proxy pool in this worksheet costs:

- `$124.10 / month` in the small and medium scenarios
- `$186.15 / month` in the large scenario

That extra layer is still substantially cheaper than forcing the backend to do
the same repeated read work on every refresh.

## What This Model Does Not Include

The table above does **not** include:

- cross-AZ traffic
- load balancers
- object storage requests
- backup systems
- snapshots
- EKS control-plane cost
- operational labor
- alerting, dashboards, or query burst headroom beyond the reference packs

If you need a finance-grade model, this page is the starting point, not the
final calculator.

## How To Recalibrate This Model For Another Environment

Replace the assumptions with measured values from the target cluster:

1. Measure actual average raw bytes per log line.
2. Measure real active-user read request rates from Grafana and API clients.
3. Measure full retained Loki bytes for the same retention window, including
   chunk and index/object-store footprint.
4. Measure VictoriaLogs data-block bytes, `indexdb` bytes, and total retained
   bytes separately.
5. Replace the `10x` VictoriaLogs storage factor with the real total retained
   compression result, and keep the data-block ratio as a separate note.
6. Replace the `37%` storage delta with a real side-by-side retained-bytes
   measurement.
7. Measure actual bytes moved between Grafana, proxy, peers, and VictoriaLogs
   with compression enabled.
8. Replace the reference compute packs with observed CPU and memory saturation
   under load.

## Signals To Collect Before Claiming Savings

- `loki_vl_proxy_requests_total`
- `loki_vl_proxy_request_duration_seconds`
- `loki_vl_proxy_backend_duration_seconds`
- `loki_vl_proxy_cache_hits_by_endpoint`
- `loki_vl_proxy_cache_misses_by_endpoint`
- `loki_vl_proxy_process_cpu_usage_ratio`
- `loki_vl_proxy_process_resident_memory_bytes`
- `loki_vl_proxy_process_network_receive_bytes_total`
- `loki_vl_proxy_process_network_transmit_bytes_total`
- Loki retained chunk/index or object-store bytes for the same retention window
- VictoriaLogs data-block bytes, `indexdb` bytes, and total retained bytes
- VictoriaLogs compression-ratio metric and real disk bytes used at the same time
- EC2 node counts and saturation for queriers, ingesters, proxy pods, and backend pods

Pair those with storage consumption and VictoriaLogs runtime metrics before
turning the worksheet into an internal budget claim.

## Related Docs

- [Comparison Matrix](cost-and-comparison.md)
- [Performance](performance.md)
- [Benchmarks](benchmarks.md)
- [Fleet Cache](fleet-cache.md)
- [Observability](observability.md)
